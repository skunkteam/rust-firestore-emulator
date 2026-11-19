import { DocumentReference, Firestore, Timestamp } from '@google-cloud/firestore';
import { setTimeout as time } from 'timers/promises';
import { callRawUnaryRpc, editions, readData, writeData } from './utils';

interface BeginTransactionResponse {
    transaction: Uint8Array;
}

function databasePath(firestore: Firestore): string {
    return (firestore as unknown as { formattedName: string }).formattedName;
}

function documentPath(firestore: Firestore, ref: DocumentReference): string {
    return `${databasePath(firestore)}/documents/${ref.path}`;
}

function rawFieldsWithTtl(fields: Record<string, unknown>): Record<string, unknown> {
    const ttlSeconds = Math.floor(Date.now() / 1_000) + 3_600;
    return {
        ...fields,
        ttl: { timestampValue: { seconds: ttlSeconds, nanos: 0 } },
    };
}

async function expectGrpcCode(promise: Promise<unknown>, expectedCode: number): Promise<void> {
    try {
        await promise;
    } catch (err: unknown) {
        const code = typeof err === 'object' && err !== null ? (err as { code?: unknown }).code : undefined;
        expect(code).toBe(expectedCode);
        return;
    }
    throw new Error(`Expected gRPC status code ${expectedCode}, but RPC succeeded`);
}

async function beginTxn(firestore: Firestore, options: Record<string, unknown>): Promise<Uint8Array> {
    const res = await callRawUnaryRpc<Record<string, unknown>, BeginTransactionResponse>(firestore, 'beginTransaction', {
        database: databasePath(firestore),
        options,
    });
    return res.transaction;
}

describe.each(editions)('$description - precondition edge cases', fs => {
    let docRef: DocumentReference;
    beforeEach(async () => {
        docRef = fs.collection.doc();
    });

    test('updating a non-existing document rejects with 5 NOT_FOUND', async () => {
        await expect(docRef.update({ foo: 'bar' })).rejects.toThrow('5 NOT_FOUND');
    });

    test('deleting a non-existing document with exists=true rejects with 5 NOT_FOUND', async () => {
        await expect(docRef.delete({ exists: true })).rejects.toThrow('5 NOT_FOUND');
    });

    test('updating or deleting an existing document with mismatched lastUpdateTime rejects with 9 FAILED_PRECONDITION', async () => {
        await docRef.set(writeData({ foo: 'initial' }));
        const wrongTime = Timestamp.fromMillis(1_000_000);

        await expect(docRef.update({ foo: 'updated' }, { lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
        await expect(docRef.delete({ lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
    });

    test('updating or deleting a non-existing document with lastUpdateTime rejects with 9 FAILED_PRECONDITION', async () => {
        const wrongTime = Timestamp.fromMillis(1_000_000);

        await expect(docRef.update({ foo: 'updated' }, { lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
        await expect(docRef.delete({ lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
    });

    test('updating or deleting a deleted document with its delete writeTime rejects with 9 FAILED_PRECONDITION', async () => {
        await docRef.set(writeData({ foo: 'initial' }));
        const { writeTime: deleteTime } = await docRef.delete();

        await expect(docRef.update({ foo: 'after-delete' }, { lastUpdateTime: deleteTime })).rejects.toThrow('9 FAILED_PRECONDITION');
        await expect(docRef.delete({ lastUpdateTime: deleteTime })).rejects.toThrow('9 FAILED_PRECONDITION');
    });

    test('failed multi-write batch leaves earlier document untouched', async () => {
        const missingRef = fs.collection.doc();
        const batch = fs.firestore.batch();
        batch.set(docRef, writeData({ committed: true }));
        batch.update(missingRef, { missing: true });

        await expect(batch.commit()).rejects.toThrow('5 NOT_FOUND');
        expect(await docRef.get()).toHaveProperty('exists', false);
    });
});

describe.each(editions)('$description - transaction commit and rollback lifecycle', fs => {
    let docRef: DocumentReference;
    beforeEach(async () => {
        docRef = fs.collection.doc();
        await docRef.set(writeData({ v: 1 }));
    });

    test('rollback is idempotent after explicit rollback and after failed commits (ALREADY_EXISTS, NOT_FOUND, FAILED_PRECONDITION)', async () => {
        const db = databasePath(fs.firestore);
        const existingPath = documentPath(fs.firestore, docRef);
        const missingPath = documentPath(fs.firestore, fs.collection.doc());

        // Double rollback on an active read-write transaction.
        const txRb = await beginTxn(fs.firestore, { readWrite: {} });
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRb });
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRb });

        // Failed commit with 6 ALREADY_EXISTS closes the transaction and accepts idempotent Rollback.
        const txExists = await beginTxn(fs.firestore, { readWrite: {} });
        await expectGrpcCode(
            callRawUnaryRpc(fs.firestore, 'commit', {
                database: db,
                transaction: txExists,
                writes: [
                    {
                        update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '2' } }) },
                        currentDocument: { exists: false },
                    },
                ],
            }),
            6,
        );
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'getDocument', { name: existingPath, transaction: txExists }), 3);
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txExists });
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txExists });

        // Failed commit with 5 NOT_FOUND accepts idempotent Rollback.
        const txNotFound = await beginTxn(fs.firestore, { readWrite: {} });
        await expectGrpcCode(
            callRawUnaryRpc(fs.firestore, 'commit', {
                database: db,
                transaction: txNotFound,
                writes: [
                    {
                        update: { name: missingPath, fields: rawFieldsWithTtl({ v: { integerValue: '1' } }) },
                        currentDocument: { exists: true },
                    },
                ],
            }),
            5,
        );
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txNotFound });

        // Failed commit with 9 FAILED_PRECONDITION closes the transaction and accepts idempotent Rollback.
        const txPrecond = await beginTxn(fs.firestore, { readWrite: {} });
        await expectGrpcCode(
            callRawUnaryRpc(fs.firestore, 'commit', {
                database: db,
                transaction: txPrecond,
                writes: [
                    {
                        update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '3' } }) },
                        currentDocument: { updateTime: { seconds: 1_000, nanos: 0 } },
                    },
                ],
            }),
            9,
        );
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'getDocument', { name: existingPath, transaction: txPrecond }), 3);
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txPrecond });
    });

    test('rollback rejects with 10 ABORTED after a succeeded commit and rejects malformed tokens with 3 INVALID_ARGUMENT', async () => {
        const db = databasePath(fs.firestore);
        const existingPath = documentPath(fs.firestore, docRef);

        // Succeeded RW commit with writes -> Rollback returns 10 ABORTED (even on repeat).
        const txRwWrite = await beginTxn(fs.firestore, { readWrite: {} });
        await callRawUnaryRpc(fs.firestore, 'commit', {
            database: db,
            transaction: txRwWrite,
            writes: [{ update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '2' } }) } }],
        });
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRwWrite }), 10);
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRwWrite }), 10);

        // Succeeded RW commit with empty writes -> Rollback returns 10 ABORTED.
        const txRwEmpty = await beginTxn(fs.firestore, { readWrite: {} });
        await callRawUnaryRpc(fs.firestore, 'commit', { database: db, transaction: txRwEmpty, writes: [] });
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRwEmpty }), 10);

        // Succeeded RO commit with empty writes -> Rollback returns 10 ABORTED.
        const txRoEmpty = await beginTxn(fs.firestore, { readOnly: {} });
        await callRawUnaryRpc(fs.firestore, 'commit', { database: db, transaction: txRoEmpty, writes: [] });
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRoEmpty }), 10);

        // Empty and malformed tokens -> 3 INVALID_ARGUMENT.
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: Buffer.alloc(0) }), 3);
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: Buffer.from('not-a-valid-token') }), 3);
    });

    test('commit request validation error preserves active read-write and read-only transactions', async () => {
        const db = databasePath(fs.firestore);
        const existingPath = documentPath(fs.firestore, docRef);
        const invalidWrites: Array<Record<string, unknown>> = [
            { update: { name: existingPath, fields: rawFieldsWithTtl({ __bad__: { integerValue: '1' } }) } },
            {
                update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '9' } }) },
                updateTransforms: [{ fieldPath: 'ts', setToServerValue: 'SERVER_VALUE_UNSPECIFIED' }],
            },
            {
                update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '9' } }) },
                currentDocument: { updateTime: { seconds: -1, nanos: 0 } },
            },
            {
                update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '9' } }) },
                currentDocument: { updateTime: { seconds: 10, nanos: -1 } },
            },
        ];

        // RW transaction stays active after request validation errors and can still commit.
        const txRw = await beginTxn(fs.firestore, { readWrite: {} });
        for (const badWrite of invalidWrites) {
            await expectGrpcCode(
                callRawUnaryRpc(fs.firestore, 'commit', {
                    database: db,
                    transaction: txRw,
                    writes: [badWrite],
                }),
                3,
            );
            await callRawUnaryRpc(fs.firestore, 'getDocument', { name: existingPath, transaction: txRw });
        }
        await callRawUnaryRpc(fs.firestore, 'commit', {
            database: db,
            transaction: txRw,
            writes: [{ update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '4' } }) } }],
        });
        expect(readData((await docRef.get()).data())).toEqual({ v: 4 });

        // RO transaction also stays active after request validation errors.
        const txRo = await beginTxn(fs.firestore, { readOnly: {} });
        for (const badWrite of invalidWrites) {
            await expectGrpcCode(
                callRawUnaryRpc(fs.firestore, 'commit', {
                    database: db,
                    transaction: txRo,
                    writes: [badWrite],
                }),
                3,
            );
            await callRawUnaryRpc(fs.firestore, 'getDocument', { name: existingPath, transaction: txRo });
        }
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRo });
    });

    test('commit with valid write on read-only transaction closes it, allows idempotent rollback, and rejects retry', async () => {
        const db = databasePath(fs.firestore);
        const existingPath = documentPath(fs.firestore, docRef);

        const txRo = await beginTxn(fs.firestore, { readOnly: {} });
        await expectGrpcCode(
            callRawUnaryRpc(fs.firestore, 'beginTransaction', {
                database: db,
                options: { readWrite: { retryTransaction: txRo } },
            }),
            3,
        );
        await expectGrpcCode(
            callRawUnaryRpc(fs.firestore, 'commit', {
                database: db,
                transaction: txRo,
                writes: [{ update: { name: existingPath, fields: rawFieldsWithTtl({ v: { integerValue: '5' } }) } }],
            }),
            3,
        );
        await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'getDocument', { name: existingPath, transaction: txRo }), 3);
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRo });
        await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: txRo });
        await expectGrpcCode(
            callRawUnaryRpc(fs.firestore, 'beginTransaction', {
                database: db,
                options: { readWrite: { retryTransaction: txRo } },
            }),
            3,
        );
    });

    test('SDK runTransaction failing commit with ALREADY_EXISTS releases locks and allows subsequent writes', async () => {
        await expect(
            fs.firestore.runTransaction(async txn => {
                const snap = await txn.get(docRef);
                expect(snap.exists).toBeTrue();
                txn.create(docRef, writeData({ v: 99 }));
            }),
        ).rejects.toThrow('6 ALREADY_EXISTS');

        await docRef.update({ v: 2 });
        expect(readData((await docRef.get()).data())).toEqual({ v: 2 });
    });
});

const optimisticEditions = editions.filter(e => e.concurrencyMode === 'optimistic');
if (optimisticEditions.length > 0) {
    describe.each(optimisticEditions)('$description - optimistic aborted commit rollback', fs => {
        test('optimistic ABORTED commit closes the transaction and allows idempotent rollback', async () => {
            const docRef = fs.collection.doc();
            await docRef.set(writeData({ v: 1 }));
            const db = databasePath(fs.firestore);
            const path = documentPath(fs.firestore, docRef);

            const tx = await beginTxn(fs.firestore, { readWrite: {} });
            await callRawUnaryRpc(fs.firestore, 'getDocument', { name: path, transaction: tx });
            await docRef.set(writeData({ v: 2 }));

            await expectGrpcCode(
                callRawUnaryRpc(fs.firestore, 'commit', {
                    database: db,
                    transaction: tx,
                    writes: [{ update: { name: path, fields: rawFieldsWithTtl({ v: { integerValue: '3' } }) } }],
                }),
                10,
            );
            await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'getDocument', { name: path, transaction: tx }), 3);
            await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: tx });
            await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: tx });
        });
    });
}

describe.each(editions.filter(e => e.concurrencyMode === 'pessimistic'))('$description - concurrent rollback during blocked commit', fs => {
    test('rollback while commit is blocked waiting on locks succeeds without aborting the commit', async () => {
        const docRef = fs.collection.doc();
        await docRef.set(writeData({ v: 1 }));
        const db = databasePath(fs.firestore);
        const path = documentPath(fs.firestore, docRef);

        const blocker = await beginTxn(fs.firestore, { readWrite: {} });
        await callRawUnaryRpc(fs.firestore, 'getDocument', { name: path, transaction: blocker });

        const committer = await beginTxn(fs.firestore, { readWrite: {} });
        let commitSettled = false;
        const commitPromise = callRawUnaryRpc(fs.firestore, 'commit', {
            database: db,
            transaction: committer,
            writes: [{ update: { name: path, fields: rawFieldsWithTtl({ v: { integerValue: '42' } }) } }],
        }).finally(() => {
            commitSettled = true;
        });

        let blockerReleased = false;
        try {
            await time(250);
            expect(commitSettled).toBeFalse();

            // Rollback while Commit is blocked waiting on locks returns OK immediately without aborting Commit.
            await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: committer });
            await time(150);
            expect(commitSettled).toBeFalse();

            blockerReleased = true;
            await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: blocker });
            await commitPromise;

            expect(readData((await docRef.get()).data())).toEqual({ v: 42 });
            await expectGrpcCode(callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: committer }), 10);
        } finally {
            if (!blockerReleased) {
                await callRawUnaryRpc(fs.firestore, 'rollback', { database: db, transaction: blocker }).catch(() => {});
            }
            await commitPromise.catch(() => {});
        }
    });
});
