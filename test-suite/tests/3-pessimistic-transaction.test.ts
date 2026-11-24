import { DocumentReference, DocumentSnapshot, FieldPath, FieldValue, Transaction } from '@google-cloud/firestore';
import assert from 'assert';
import { setTimeout as time } from 'timers/promises';
import {
    ConcurrentTest,
    callRawStreamRpc,
    callRawUnaryRpc,
    connection,
    editions,
    notImplementedInJava,
    readData,
    readDataRef,
    writeData,
} from './utils';

interface RawProtoValue {
    stringValue?: string;
    booleanValue?: boolean;
    timestampValue?: { seconds: number; nanos: number };
}

interface RawDocument {
    name?: string;
    fields?: Record<string, RawProtoValue>;
}

interface RawBeginTransactionResponse {
    transaction: Uint8Array;
}

interface RawBatchGetChunk {
    found?: RawDocument;
    missing?: string;
    transaction?: Uint8Array;
}

interface RawCommitResponse {
    commitTime?: { seconds?: string | number | { toString(): string }; nanos?: number };
}

function commitTimeNs(res: RawCommitResponse): bigint {
    const secs = res.commitTime?.seconds ?? 0;
    const secsBig = typeof secs === 'string' || typeof secs === 'number' ? BigInt(secs) : BigInt(secs.toString());
    return secsBig * 1_000_000_000n + BigInt(res.commitTime?.nanos ?? 0);
}

type RpcOutcome<T> = { ok: true; value: T } | { ok: false; code: number | undefined; message: string };

async function captureRpcOutcome<T>(fn: () => Promise<T>): Promise<RpcOutcome<T>> {
    try {
        const value = await fn();
        return { ok: true, value };
    } catch (err: unknown) {
        const code = typeof err === 'object' && err !== null ? (err as { code?: unknown }).code : undefined;
        const message = err instanceof Error ? err.message : String(err);
        return { ok: false, code: typeof code === 'number' ? code : undefined, message };
    }
}

async function waitOrTimeout(promise: Promise<unknown>, ms: number): Promise<boolean> {
    const ac = new AbortController();
    const timeoutPromise = time(ms, false as const, { signal: ac.signal }).catch(() => false as const);
    const targetPromise = promise.then(
        () => true as const,
        () => false as const,
    );
    try {
        return await Promise.race([targetPromise, timeoutPromise]);
    } finally {
        ac.abort();
    }
}

function rawTtlValue(): RawProtoValue {
    return { timestampValue: { seconds: Math.floor((Date.now() + 3_600_000) / 1_000), nanos: 0 } };
}

describe.each(editions.filter(e => e.concurrencyMode === 'pessimistic'))('$description - concurrent tests', fs => {
    const database = (fs.firestore as unknown as { formattedName: string }).formattedName;

    function docResourceName(ref: DocumentReference): string {
        return `${database}/documents/${ref.path}`;
    }

    function orderedRefs(order: 'ref1 < ref2' | 'ref2 < ref1'): readonly [DocumentReference, DocumentReference] {
        let [a, b] = refs();
        while (a.id === b.id) {
            [a, b] = refs();
        }
        const [smaller, larger] = a.id < b.id ? [a, b] : [b, a];
        return order === 'ref1 < ref2' ? [smaller, larger] : [larger, smaller];
    }

    async function rawBeginTxn(retryTransaction?: Uint8Array): Promise<Uint8Array> {
        const res = await callRawUnaryRpc<object, RawBeginTransactionResponse>(fs.firestore, 'beginTransaction', {
            database,
            options: { readWrite: retryTransaction ? { retryTransaction } : {} },
        });
        assert(res.transaction.length > 0, 'expected non-empty transaction token');
        return res.transaction;
    }

    async function rawReadDoc(ref: DocumentReference, transaction: Uint8Array): Promise<RawBatchGetChunk> {
        const chunks = await callRawStreamRpc<object, RawBatchGetChunk>(fs.firestore, 'batchGetDocuments', {
            database,
            documents: [docResourceName(ref)],
            transaction,
        });
        expect(chunks).toHaveLength(1);
        return chunks[0];
    }

    async function rawCommit(writes: object[], transaction?: Uint8Array): Promise<RawCommitResponse> {
        return callRawUnaryRpc<object, RawCommitResponse>(
            fs.firestore,
            'commit',
            transaction ? { database, transaction, writes } : { database, writes },
        );
    }

    async function rawRollback(transaction: Uint8Array): Promise<void> {
        await callRawUnaryRpc(fs.firestore, 'rollback', { database, transaction });
    }

    function rawUpdateWrite(ref: DocumentReference, fields: Record<string, RawProtoValue>, maskFields?: string[]): object {
        return {
            update: { name: docResourceName(ref), fields },
            ...(maskFields ? { updateMask: { fieldPaths: maskFields } } : {}),
        };
    }
    test.concurrent('simple txn', async () => {
        const [docRef1] = refs();

        await fs.firestore.runTransaction(async txn => {
            expect(await txn.get(docRef1)).toHaveProperty('exists', false);

            txn.set(docRef1, writeData({ foo: 'bar' }));

            expect(() => txn.get(docRef1)).toThrow('Firestore transactions require all reads to be executed before all writes.');
        });
        expect(await getData(docRef1.get())).toEqual({ foo: 'bar' });
    });

    test.concurrent('updating same doc multiple times', async () => {
        const [docRef1] = refs();

        await fs.firestore.runTransaction(async txn => {
            expect(await txn.get(docRef1)).toHaveProperty('exists', false);

            txn.set(docRef1, writeData({ foo: FieldValue.increment(1) }));
            txn.update(docRef1, { foo: FieldValue.increment(1) });
        });
        expect(await getData(docRef1.get())).toEqual({ foo: 2 });
    });

    test.concurrent('using txn.getAll', async () => {
        const [docRef1, docRef2] = refs();

        await docRef1.set(writeData({ some: 'data' }));

        await fs.firestore.runTransaction(async txn => {
            const [snap1, snap2] = await txn.getAll(docRef1, docRef2);

            expect(await getData(snap1)).toEqual({ some: 'data' });
            expect(snap2.exists).toBeFalse();

            txn.set(docRef2, writeData({ foo: 'bar' }));
        });
        expect(await getData(docRef1.get())).toEqual({ some: 'data' });
        expect(await getData(docRef2.get())).toEqual({ foo: 'bar' });
    });

    test.concurrent('aborting transaction', async () => {
        const [docRef1] = refs();

        await expect(
            fs.firestore.runTransaction(async txn => {
                expect(await txn.get(docRef1)).toHaveProperty('exists', false);

                txn.set(docRef1, writeData({ foo: 'bar' }));

                throw new Error('I quit!');
            }),
        ).rejects.toThrow('I quit!');

        expect(await docRef1.get()).toHaveProperty('exists', false);
    });

    describe('locks', () => {
        // In Java emulator, either leads to:
        // - 10 ABORTED: Transaction lock timeout
        // - inconsistent number of `tries`
        notImplementedInJava ||
            test.concurrent('retry if document is locked', async () => {
                const [docRef1] = refs();

                await docRef1.set(writeData({ some: 'data' }));

                await runTxn('outer', [docRef1], async () => {
                    const { innerTxnCompleted } = await innerTxn('inner', [docRef1]);

                    return { awaitAfterTxn: innerTxnCompleted };
                });

                expect(await getData(docRef1.get())).toEqual({
                    some: 'data',
                    outer: { tries: 1 },
                    inner: { tries: expect.toBeOneOf([2, 3, 4]) },
                });
            });

        notImplementedInJava ||
            test.concurrent('lock on non-existing document', async () => {
                const [docRef1] = refs();

                await runTxn('outer', [docRef1], async () => {
                    const { innerTxnCompleted } = await innerTxn('inner', [docRef1]);

                    return { awaitAfterTxn: innerTxnCompleted };
                });

                expect(await getData(docRef1.get())).toEqual({
                    outer: { tries: 1 },
                    inner: { tries: expect.toBeOneOf([2, 3, 4]) },
                });
            });

        test.concurrent('no lock if getting separate documents', async () => {
            const [docRef1, docRef2] = refs();

            await runTxn('outer', [docRef1], async () => {
                const { innerTxnCompleted } = await innerTxn('inner', [docRef2]);
                return { awaitAfterTxn: innerTxnCompleted };
            });
            expect(await getData(docRef1.get())).toEqual({ outer: { tries: 1 } });
            expect(await getData(docRef2.get())).toEqual({ inner: { tries: 1 } });
        });

        // Note: Very slow on Cloud Firestore!!
        notImplementedInJava ||
            test.concurrent(
                'chaos',
                async () => {
                    const [docRef1, docRef2] = refs();

                    await runTxn('outer', [docRef1], async () => {
                        // Will need a retry because of `docRef1`,
                        const { innerTxnCompleted: first } = await innerTxn('innerFirst', [docRef2, docRef1]);
                        // Somehow often only needs one try, even though `docRef2` could have been locked by 'innerFirst'
                        const { innerTxnCompleted: second } = await innerTxn('innerSecond', [docRef2]);

                        return { awaitAfterTxn: Promise.all([first, second]) };
                    });
                    expect(await getData(docRef1.get())).toEqual({
                        outer: { tries: 1 },
                        innerFirst: { tries: expect.toBeOneOf([2, 3, 4]) },
                    });
                    expect(await getData(docRef2.get())).toEqual({
                        innerFirst: { tries: expect.toBeOneOf([2, 3, 4]) },
                        innerSecond: { tries: expect.toBeOneOf([1, 2]) },
                    });
                },
                45_000,
            );

        test.concurrent('only read locked document', async () => {
            const [docRef1, docRef2] = refs();

            await docRef1.create(writeData());

            await runTxn('outer', [docRef1], async () => {
                // Doesn't need a retry because it reads `docRef1`, it only writes `docRef2` so both txn got the latest version
                // of `docRef1`.
                const { innerTxnCompleted } = await innerTxn('inner', { read: [docRef1, docRef2], write: [docRef2] });

                return { awaitAfterTxn: innerTxnCompleted };
            });
            expect(await getData(docRef1.get())).toEqual({
                outer: { tries: 1 },
            });
            expect(await getData(docRef2.get())).toEqual({
                inner: { tries: expect.toBeOneOf([1, 2]) }, // sometimes this will need a retry
            });
        });

        test.concurrent('regular `set` waits on transaction', async () => {
            const [docRef1] = refs();

            await docRef1.set(writeData({ some: 'data' }));

            let setDone = false;
            await runTxn('outer', [docRef1], async () => {
                const updateDone = docRef1.update({ extraProp: 'foo' }).then(() => (setDone = true));
                await time(250);
                expect(setDone).toBeFalse();
                return { awaitAfterTxn: updateDone };
            });
            expect(setDone).toBeTrue();
        });

        describe('tests with synchronized processes', () => {
            test.concurrent('reading the same doc from different txns', async () => {
                const test = new ConcurrentTest();
                // Scenario:
                // Process 1 - create doc
                // Process 1 - start txn A
                // Process 1 - in txn A: read doc
                // Process 2 - start txn B
                // Process 2 - in txn B: read doc
                // Process 2 - end txn B
                // Process 1 - end txn A

                const [ref] = refs();

                await test.run(
                    // Process 1
                    async () => {
                        await ref.create(writeData({ value: 'original value' }));
                        test.event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn A: doc read');

                            await test.when('txn B ended');
                            return value;
                        });
                        test.event('read: ' + result);
                        test.event('txn A ended');
                    },

                    // Process 2
                    async () => {
                        await test.when('in txn A: doc read');
                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn B started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn B: doc read');

                            return value;
                        });
                        test.event('read: ' + result);
                        test.event('txn B ended');
                    },
                );

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: in txn A: doc read',
                    ' <<1>> | EVENT: doc created',
                    ' <<1>> | EVENT: txn A started',
                    ' <<1>> | EVENT: in txn A: doc read',
                    ' <<1>> | WAITING UNTIL: txn B ended',
                    '       | <<2>> | EVENT: txn B started',
                    '       | <<2>> | EVENT: in txn B: doc read',
                    '       | <<2>> | EVENT: read: original value',
                    '       | <<2>> | EVENT: txn B ended',
                    ' <<1>> | EVENT: read: original value',
                    ' <<1>> | EVENT: txn A ended',
                ]);
            });

            test.concurrent('reading the same doc from different txns, try to write in second txn', async () => {
                const test = new ConcurrentTest();
                // Scenario:
                // Process 1 - create doc
                // Process 1 - start txn A
                // Process 1 - in txn A: read doc
                // Process 2 - start txn B
                // Process 2 - in txn B: read doc
                // Process 2 - in txn B: try to write to doc   <<--- the only difference with the previous test
                // Process 2 - try to end txn B, will stall until txn A is completed
                // Process 1 - end txn A
                // Process 2 - only now txn B ends

                const [ref] = refs();

                await test.run(
                    // Process 1
                    async () => {
                        await ref.create(writeData({ value: 'original value' }));
                        test.event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn A: doc read');

                            await test.when('in txn B: update requested in txn');
                            await time(250);
                            expect(test.lastEvent$.get()).toBe('in txn B: update requested in txn');
                            test.event('waited 250ms, txn B still pending');
                            return value;
                        });
                        test.event('txn A ended');
                        test.event('read: ' + result);
                    },

                    // Process 2
                    async () => {
                        await test.when('in txn A: doc read');
                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn B started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn B: doc read');

                            txn.update(ref, { value: 'changed by txn B' });
                            test.event('in txn B: update requested in txn');
                            return value;
                        });
                        test.event('txn B ended');
                        test.event('read: ' + result);
                    },
                );

                expect(test.log.slice(0, 9)).toEqual([
                    '       | <<2>> | WAITING UNTIL: in txn A: doc read',
                    ' <<1>> | EVENT: doc created',
                    ' <<1>> | EVENT: txn A started',
                    ' <<1>> | EVENT: in txn A: doc read',
                    ' <<1>> | WAITING UNTIL: in txn B: update requested in txn',
                    '       | <<2>> | EVENT: txn B started',
                    '       | <<2>> | EVENT: in txn B: doc read',
                    '       | <<2>> | EVENT: in txn B: update requested in txn',
                    ' <<1>> | EVENT: waited 250ms, txn B still pending',
                ]);
                const txnACompletion = [' <<1>> | EVENT: txn A ended', ' <<1>> | EVENT: read: original value'];
                const txnBCompletion = ['       | <<2>> | EVENT: txn B ended', '       | <<2>> | EVENT: read: original value'];
                expect(test.log.slice(9)).toBeOneOf([
                    [...txnACompletion, ...txnBCompletion],
                    [...txnBCompletion, ...txnACompletion],
                ]);
                expect(await getData(ref.get())).toEqual({ value: 'changed by txn B' });
            });

            test.concurrent('reading the same doc from different txns, try to write in both txns', async () => {
                const test = new ConcurrentTest();
                // Scenario:
                // Process 1 - create doc
                // Process 1 - start txn A
                // Process 1 - in txn A: read doc
                // Process 2 - start txn B
                // Process 2 - in txn B: read doc
                // Process 2 - in txn B: try to write to doc
                // Process 2 - try to end txn B, will stall until txn A is completed
                // Process 1 - in txn A: try to write to doc    <<--- the only difference with the previous test
                // Process 1 - end txn A
                // Process 2 - txn B retries

                // Apparently, even though txn B tries to write first, txn A gets priority (because it started first or because
                // it read first?)

                const [ref] = refs();

                await test.run(
                    // Process 1
                    async () => {
                        await ref.create(writeData({ value: 'original value' }));
                        test.event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn A: doc read');

                            await test.when('in txn B: update requested in txn');
                            await time(250);
                            expect(test.lastEvent$.get()).toBe('in txn B: update requested in txn');
                            test.event('waited 250ms, txn B still pending');
                            txn.update(ref, { value: 'changed by txn A' });
                            return value;
                        });
                        test.event('txn A ended');
                        test.event('read: ' + result);
                    },

                    // Process 2
                    async () => {
                        await test.when('in txn A: doc read');
                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn B started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn B: doc read');

                            txn.update(ref, { value: 'changed by txn B' });
                            test.event('in txn B: update requested in txn');
                            return value;
                        });
                        test.event('txn B ended');
                        test.event('read: ' + result);
                    },
                );

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: in txn A: doc read',
                    ' <<1>> | EVENT: doc created',
                    ' <<1>> | EVENT: txn A started',
                    ' <<1>> | EVENT: in txn A: doc read',
                    ' <<1>> | WAITING UNTIL: in txn B: update requested in txn',
                    '       | <<2>> | EVENT: txn B started',
                    '       | <<2>> | EVENT: in txn B: doc read',
                    '       | <<2>> | EVENT: in txn B: update requested in txn',
                    // Now txn B is stalled, waiting to get the verdict on the lock acquisition
                    ' <<1>> | EVENT: waited 250ms, txn B still pending',
                    ' <<1>> | EVENT: txn A ended',
                    // txn A succeeded and was allowed to write to doc, it read the original value of doc:
                    ' <<1>> | EVENT: read: original value',
                    // Retry of txn B:
                    '       | <<2>> | EVENT: txn B started',
                    '       | <<2>> | EVENT: in txn B: doc read',
                    '       | <<2>> | EVENT: in txn B: update requested in txn',
                    '       | <<2>> | EVENT: txn B ended',
                    '       | <<2>> | EVENT: read: changed by txn A',
                ]);
            });

            test.concurrent('regular writes also wait until all txns-locks are released', async () => {
                const test = new ConcurrentTest();
                // Scenario:
                // Process 1 - create outside txn (doc 1), completes immediately.
                // Process 2 - start txn A
                // Process 2 - in txn A: read doc 1
                // Process 3 - start txn B
                // Process 3 - in txn B: read doc 1
                // Process 1 - try to update outside txn (doc 1), stalls
                // Process 4 - try to update outside txn (doc 1), stalls
                // Process 3 - in txn B: write doc 2
                // Process 3 - end txn B
                // Process 2 - in txn A: update doc 1
                // Process 2 - end txn A
                // Process 1 - only now the outside update to doc 1 completes.
                // Process 4 - only now the outside update to doc 1 completes, this update wins.

                const [ref1, ref2] = refs();

                await test.run(
                    // Process 1
                    async () => {
                        await ref1.create(writeData({ log: ['created outside txn'] }));
                        test.event('create outside txn succeeded');

                        await test.when('in txn B: read doc 1');
                        const firstUpdate = ref1
                            .update({ log: FieldValue.arrayUnion('updated outside txn once') })
                            .then(() => test.event('finished first update outside txn'));

                        await time(0); // Make sure the update is sent

                        const secondUpdate = ref1
                            .update({ log: FieldValue.arrayUnion('updated outside txn again') })
                            .then(() => test.event('finished second update outside txn'));

                        test.event('started updates outside of txn');

                        await Promise.all([firstUpdate, secondUpdate]);
                    },
                    // Process 2
                    async () => {
                        await test.when('create outside txn succeeded');
                        await fs.firestore.runTransaction(async txn => {
                            await txn.get(ref1);
                            test.event('in txn A: read doc 1');

                            await test.when('end txn B');
                            txn.update(ref1, { log: FieldValue.arrayUnion('updated inside txn A') });
                        });
                        test.event('end txn A');
                    },
                    // Process 3
                    async () => {
                        await test.when('in txn A: read doc 1');
                        await fs.firestore.runTransaction(async txn => {
                            await txn.get(ref1);
                            test.event('in txn B: read doc 1');

                            await test.when('started updates outside of txn');
                            // Only write to doc2, so the lock on 1 is lifted without any update
                            txn.create(ref2, writeData({ from: 'txn B' }));
                        });
                        test.event('end txn B');
                    },
                );

                expect(test.log.slice(0, -3)).toEqual([
                    //<<1>> | <<2>> | <<3>> |
                    '       | <<2>> | WAITING UNTIL: create outside txn succeeded',
                    '       |       | <<3>> | WAITING UNTIL: in txn A: read doc 1',
                    ' <<1>> | EVENT: create outside txn succeeded',
                    ' <<1>> | WAITING UNTIL: in txn B: read doc 1',
                    '       | <<2>> | EVENT: in txn A: read doc 1',
                    '       | <<2>> | WAITING UNTIL: end txn B',
                    '       |       | <<3>> | EVENT: in txn B: read doc 1',
                    '       |       | <<3>> | WAITING UNTIL: started updates outside of txn',
                    ' <<1>> | EVENT: started updates outside of txn',
                    '       |       | <<3>> | EVENT: end txn B',
                ]);
                // Regular updates suffer from contention (in the current version of the SDK), so
                // they need to be retried and then the order cannot be guaranteed anymore.
                // These last two (or three on non-Cloud emulators) might come back in different order...
                if (connection !== 'CLOUD FIRESTORE') {
                    expect(test.log.slice(-3)).toIncludeSameMembers([
                        '       | <<2>> | EVENT: end txn A',
                        ' <<1>> | EVENT: finished first update outside txn',
                        ' <<1>> | EVENT: finished second update outside txn',
                    ]);
                } else {
                    expect(test.log.at(-3)).toBe('       | <<2>> | EVENT: end txn A');
                    expect(test.log.slice(-2)).toIncludeSameMembers([
                        ' <<1>> | EVENT: finished first update outside txn',
                        ' <<1>> | EVENT: finished second update outside txn',
                    ]);
                }
                const { log } = await getData(ref1.get());
                assert(Array.isArray(log));
                expect(log.slice(0, -2)).toEqual(['created outside txn', 'updated inside txn A']);
                // Even though the writes were done before the transaction finished, they were written afterwards
                expect(log.slice(-2)).toIncludeSameMembers(['updated outside txn once', 'updated outside txn again']);
            });

            test.concurrent('write using a batch write to a document that is locked by a transaction', async () => {
                const test = new ConcurrentTest();
                // Scenario:
                // Process 1 - create doc
                // Process 1 - start txn A
                // Process 1 - in txn A: read doc
                // Process 2 - start batch write, writing to doc
                // wait a few millisecs
                // Process 1 - end txn A, writing to doc
                // Process 2 - finish batch write, writing to doc

                const [ref] = refs();

                await test.run(
                    async () => {
                        await ref.create(writeData({ value: 'original value' }));
                        await fs.firestore.runTransaction(async txn => {
                            await txn.get(ref);
                            test.event('txn A got lock on doc');
                            await test.when('batch write waiting on lock');
                            txn.update(ref, { value: 'changed by txn' });
                        });
                        test.event('transaction completed');
                    },
                    async () => {
                        await test.when('txn A got lock on doc');
                        const batchPromise = fs.firestore
                            .batch()
                            .update(ref, { value: 'changed in batch' })
                            .commit()
                            .then(() => test.event('batch write finished'));
                        await time(50);
                        test.event('batch write waiting on lock');
                        await batchPromise;
                    },
                );

                expect(test.log.slice(0, 4)).toEqual([
                    '       | <<2>> | WAITING UNTIL: txn A got lock on doc',
                    ' <<1>> | EVENT: txn A got lock on doc',
                    ' <<1>> | WAITING UNTIL: batch write waiting on lock',
                    '       | <<2>> | EVENT: batch write waiting on lock',
                ]);
                const completionEvents = [' <<1>> | EVENT: transaction completed', '       | <<2>> | EVENT: batch write finished'];
                connection === 'JAVA EMULATOR'
                    ? expect(test.log.slice(4)).toIncludeSameMembers(completionEvents)
                    : expect(test.log.slice(4)).toEqual(completionEvents);

                const finalSnap = await ref.get();
                expect(readData(finalSnap.data())).toEqual({
                    value: 'changed in batch',
                });
            });

            notImplementedInJava ||
                test.concurrent('read waiting on write-lock that is waiting on a read-lock', async () => {
                    const [docRef] = refs();
                    await docRef.create(writeData({ some: 'data' }));

                    const test = new ConcurrentTest();
                    await test.run(
                        // Transaction 1 reads the document, but keeps the
                        // transaction alive until the very end.
                        async () => {
                            await fs.firestore.runTransaction(async txn => {
                                test.event('txn 1: started');
                                const snap = await txn.get(docRef);
                                expect(readData(snap.data())).toEqual({ some: 'data' });
                                test.event('txn 1: read doc');
                                await test.when('txn 3: started');
                                await time(100);
                                test.event('txn 1: stopped');
                            });
                        },
                        // Transaction 2 tries to write to the document after txn 1
                        // read it, so this means that a write lock is requested,
                        // but not granted until txn 1 completes.
                        async () => {
                            await test.when('txn 1: read doc');
                            await fs.firestore.runTransaction(async txn => {
                                test.event('txn 2: started');
                                txn.update(docRef, { other: 'data' });
                                test.event('txn 2: updating doc');
                                // We return from this handler to allow the
                                // Firestore SDK to commit to the backend, this
                                // requests a write lock that is only granted when
                                // txn 1 ends (because it needs to check whether txn
                                // 1 will write to the doc or not).
                            });
                            test.event('txn 2: updated doc');
                        },
                        // Transaction 3
                        async () => {
                            await test.when('txn 2: updating doc');
                            await time(50); // to be sure
                            await fs.firestore.runTransaction(async txn => {
                                test.event('txn 3: started');
                                // Txn 2 has requested a write lock, so, to make
                                // sure that write locks don't suffer from
                                // starvation, this read should wait until the
                                // write-lock was either granted or rejected.
                                const snap = await txn.get(docRef);
                                expect(readData(snap.data())).toEqual({ some: 'data', other: 'data' });
                                await time(50); // to be sure
                                test.event('txn 3: read doc');
                            });
                        },
                    );

                    expect(test.log).toEqual([
                        '       | <<2>> | WAITING UNTIL: txn 1: read doc',
                        '       |       | <<3>> | WAITING UNTIL: txn 2: updating doc',
                        ' <<1>> | EVENT: txn 1: started',
                        ' <<1>> | EVENT: txn 1: read doc',
                        ' <<1>> | WAITING UNTIL: txn 3: started',
                        '       | <<2>> | EVENT: txn 2: started',
                        '       | <<2>> | EVENT: txn 2: updating doc',
                        '       |       | <<3>> | EVENT: txn 3: started',
                        ' <<1>> | EVENT: txn 1: stopped',
                        '       | <<2>> | EVENT: txn 2: updated doc',
                        '       |       | <<3>> | EVENT: txn 3: read doc',
                    ]);
                });

            async function runDeadlockTest(order: 'ref1 < ref2' | 'ref2 < ref1') {
                const test = new ConcurrentTest({ timeout: 40_000 });

                const [ref1, ref2] = orderedRefs(order);

                let txnBFailedInFirstGet = false;

                await test.run(
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn A attempt ${++attempt} started`);

                            await txn.get(ref1);
                            test.event('in txn A: read doc 1');

                            if (attempt === 1) await test.when('in txn B: read doc 2');
                            await txn.get(ref2);
                            test.event('in txn A: read doc 2');

                            txn.set(ref1, writeData({ txnA: 'written' }));
                            txn.set(ref2, writeData({ txnA: 'written' }));
                        });
                    },
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn B attempt ${++attempt} started`);

                            if (attempt === 1) await test.when('in txn A: read doc 1');
                            await txn.get(ref2);
                            test.event('in txn B: read doc 2');

                            if (attempt === 1) await test.when('in txn A: read doc 2');
                            await txn.get(ref1).catch(e => {
                                // Sometimes the get fails, sometimes it succeeds and the txn fails;
                                // we can detect that here.
                                txnBFailedInFirstGet ||= attempt === 1;
                                throw e;
                            });

                            test.event('in txn B: read doc 1');

                            txn.set(ref1, writeData({ txnB: 'written' }));
                            txn.set(ref2, writeData({ txnB: 'written' }));
                        });
                    },
                );

                const retry = test.log.splice(-3, 3);
                // If `txn.get(ref1)` did not fail, then txn B tried to read doc 1 in attempt 1,
                // otherwise it did not.
                const secondReadInFirstAttempt = !txnBFailedInFirstGet && test.log.pop();

                expect(test.log).toEqual([
                    ' <<1>> | EVENT: txn A attempt 1 started',
                    '       | <<2>> | EVENT: txn B attempt 1 started',
                    '       | <<2>> | WAITING UNTIL: in txn A: read doc 1',
                    ' <<1>> | EVENT: in txn A: read doc 1',
                    ' <<1>> | WAITING UNTIL: in txn B: read doc 2',
                    '       | <<2>> | EVENT: in txn B: read doc 2',
                    '       | <<2>> | WAITING UNTIL: in txn A: read doc 2',
                    ' <<1>> | EVENT: in txn A: read doc 2',
                ]);

                if (secondReadInFirstAttempt) expect(secondReadInFirstAttempt).toBe('       | <<2>> | EVENT: in txn B: read doc 1');

                // Wound-Wait deterministically wounds younger txn B while older txn A completes cleanly.
                expect(retry).toEqual([
                    '       | <<2>> | EVENT: txn B attempt 2 started',
                    '       | <<2>> | EVENT: in txn B: read doc 2',
                    '       | <<2>> | EVENT: in txn B: read doc 1',
                ]);
            }

            test.concurrent('deadlock (ref1 < ref2)', async () => runDeadlockTest('ref1 < ref2'), 45_000);

            test.concurrent('deadlock (ref2 < ref1)', async () => runDeadlockTest('ref2 < ref1'), 45_000);

            // Skip these tests because queries seem to behave differently in Enterprise Edition
            // (as if they automatically opt into optimistic concurrency).
            if (!fs.enterprise)
                describe('queries', () => {
                    async function setupQueryTest(testName: string) {
                        const [docRef1, docRef2] = refs();

                        const testData = writeData({ testName });
                        await fs.firestore.batch().create(docRef1, testData).create(docRef2, testData).commit();

                        const baseQuery = fs.collection.where('testName', '==', testName);

                        return { docRef1, docRef2, testName, queryDocsInTest };

                        async function queryDocsInTest(txn: Transaction, opts: { exclude?: DocumentReference } = {}) {
                            const excludeId = opts.exclude?.id;
                            const query = excludeId ? baseQuery.where(FieldPath.documentId(), '!=', excludeId) : baseQuery;

                            const snaps = await txn.get(query);
                            // Sanity check
                            if (excludeId) expect(snaps.docs).not.toPartiallyContain({ id: excludeId });
                            return snaps;
                        }
                    }

                    test.concurrent('query waiting on write-lock that is waiting on a read-lock', async () => {
                        const [docRef] = refs();
                        await docRef.create(writeData({ some: 'data' }));

                        const test = new ConcurrentTest();
                        await test.run(
                            // Transaction 1 reads the document, but keeps the
                            // transaction alive until the very end.
                            async () => {
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('txn 1: started');
                                    const snap = await txn.get(docRef);
                                    expect(readData(snap.data())).toEqual({ some: 'data' });
                                    test.event('txn 1: read doc');
                                    await test.when('txn 3: started');
                                    await time(100);
                                    test.event('txn 1: stopped');
                                });
                            },
                            // Transaction 2 tries to write to the document after
                            // txn 1 read it, so this means that a write lock is
                            // requested, but not granted until txn 1 completes.
                            async () => {
                                await test.when('txn 1: read doc');
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('txn 2: started');
                                    txn.update(docRef, { other: 'data' });
                                    test.event('txn 2: updating doc');
                                    // We return from this handler to allow the
                                    // Firestore SDK to commit to the backend, this
                                    // requests a write lock that is only granted
                                    // when txn 1 ends (because it needs to check
                                    // whether txn 1 will write to the doc or not).
                                });
                                test.event('txn 2: updated doc');
                            },
                            // Transaction 3
                            async () => {
                                await test.when('txn 2: updating doc');
                                await time(50); // to be sure
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('txn 3: started');
                                    // Txn 2 has requested a write lock, so, to make
                                    // sure that write locks don't suffer from
                                    // starvation, this query should wait until the
                                    // write-lock was either granted or rejected.
                                    const snap = await txn.get(fs.collection.where(FieldPath.documentId(), '==', docRef.id));
                                    expect(snap.docs).toHaveLength(1);
                                    expect(readData(snap.docs[0].data())).toEqual({ some: 'data', other: 'data' });
                                    await time(50); // to be sure
                                    test.event('txn 3: read doc');
                                });
                            },
                        );

                        expect(test.log).toEqual([
                            '       | <<2>> | WAITING UNTIL: txn 1: read doc',
                            '       |       | <<3>> | WAITING UNTIL: txn 2: updating doc',
                            ' <<1>> | EVENT: txn 1: started',
                            ' <<1>> | EVENT: txn 1: read doc',
                            ' <<1>> | WAITING UNTIL: txn 3: started',
                            '       | <<2>> | EVENT: txn 2: started',
                            '       | <<2>> | EVENT: txn 2: updating doc',
                            '       |       | <<3>> | EVENT: txn 3: started',
                            ' <<1>> | EVENT: txn 1: stopped',
                            '       | <<2>> | EVENT: txn 2: updated doc',
                            '       |       | <<3>> | EVENT: txn 3: read doc',
                        ]);
                    });

                    test.concurrent('update after reading a query', async () => {
                        const { docRef1, docRef2, queryDocsInTest } = await setupQueryTest('update');

                        const test = new ConcurrentTest();

                        await test.run(
                            async () => {
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('transaction started');

                                    const snaps = await queryDocsInTest(txn);
                                    expect(snaps.size).toBe(2);
                                    test.event('transaction locked the query');
                                    await time(250);
                                    expect(test.lastEvent$.get()).toBe('transaction locked the query');

                                    txn.update(docRef1, { other: 'data' });
                                });
                                test.event('transaction completed');
                            },
                            async () => {
                                await test.when('transaction locked the query');
                                // Note that both these `update`s wait for the transaction above to be completed
                                await Promise.all([
                                    docRef1.update({ simple: 'also update docRef1' }),
                                    docRef2.update({ simple: 'this is the only update for docRef2' }),
                                ]);
                                test.event('both docs updated outside the query');
                            },
                        );

                        expect(test.log.slice(0, 3)).toEqual([
                            '       | <<2>> | WAITING UNTIL: transaction locked the query',
                            ' <<1>> | EVENT: transaction started',
                            ' <<1>> | EVENT: transaction locked the query',
                        ]);
                        const completionEvents = [
                            ' <<1>> | EVENT: transaction completed',
                            '       | <<2>> | EVENT: both docs updated outside the query',
                        ];
                        connection === 'JAVA EMULATOR'
                            ? expect(test.log.slice(3)).toIncludeSameMembers(completionEvents)
                            : expect(test.log.slice(3)).toEqual(completionEvents);
                    });

                    test.concurrent('delete after reading a query', async () => {
                        const { docRef1, queryDocsInTest } = await setupQueryTest('delete');

                        const test = new ConcurrentTest();

                        await test.run(
                            async () => {
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('transaction started');

                                    const snaps = await queryDocsInTest(txn);
                                    expect(snaps.size).toBe(2);
                                    test.event('transaction locked the query');
                                    await time(250);
                                    expect(test.lastEvent$.get()).toBe('transaction locked the query');

                                    txn.update(docRef1, { other: 'data' });
                                });
                                test.event('transaction completed');
                            },
                            async () => {
                                await test.when('transaction locked the query');
                                await docRef1.delete(); // Note that this `delete` waits for the transaction above to be completed
                                test.event('docRef1 deleted outside the query');
                            },
                        );

                        expect(test.log.slice(0, 3)).toEqual([
                            '       | <<2>> | WAITING UNTIL: transaction locked the query',
                            ' <<1>> | EVENT: transaction started',
                            ' <<1>> | EVENT: transaction locked the query',
                        ]);
                        const completionEvents = [
                            ' <<1>> | EVENT: transaction completed',
                            '       | <<2>> | EVENT: docRef1 deleted outside the query',
                        ];
                        connection === 'JAVA EMULATOR'
                            ? expect(test.log.slice(3)).toIncludeSameMembers(completionEvents)
                            : expect(test.log.slice(3)).toEqual(completionEvents);
                    });

                    notImplementedInJava ||
                        test.concurrent(
                            'conditional write based on query (contested doc is read first in the secondary transaction)',
                            async () => {
                                const { docRef1, docRef2, testName, queryDocsInTest } = await setupQueryTest('multi');

                                const test = new ConcurrentTest();

                                await test.run(
                                    async () => {
                                        await test.when('txn 2: read own document');
                                        await fs.firestore.runTransaction(async txn => {
                                            test.event('txn 1: started');

                                            // First Transaction Try:
                                            //   `snaps` should contain `docRef2` which will include the document in the transaction
                                            // Second Transaction Try:
                                            //   `docRef2` will have changed it's `testName`, so it won't be in the result anymore
                                            const snaps = await queryDocsInTest(txn, { exclude: docRef1 });
                                            if (snaps.size) {
                                                test.event('txn 1: found contested document');
                                                // in the time between these events, `docRef2` is updated, which will make the transaction retry
                                                await test.when('txn 2: completed');
                                                txn.update(docRef1, { testName: 'other name' });
                                            } else {
                                                test.event('txn 1: did not find any document');
                                            }
                                        });
                                        test.event('txn 1: completed');
                                    },
                                    async () => {
                                        await fs.firestore.runTransaction(async txn => {
                                            test.event('txn 2: started');
                                            // Read docRef2 before Txn1 queries this document, to get 'first rights' on this document
                                            await txn.get(docRef2);
                                            test.event('txn 2: read own document');

                                            await test.when('txn 1: found contested document');
                                            // Update `docRef2` after it is 'found' in the query in the first transaction
                                            txn.update(docRef2, { testName: 'some other name' });
                                            test.event('txn 2: updated contested document');
                                        });
                                        test.event('txn 2: completed');
                                    },
                                );
                                // the `update` of `docRef1` did not go through, because `docRef2` was altered before the transaction was committed
                                expect(await readDataRef(docRef1)).toEqual({ testName });
                                expect(await readDataRef(docRef2)).toEqual({ testName: 'some other name' });

                                expect(test.log).toEqual([
                                    ' <<1>> | WAITING UNTIL: txn 2: read own document',
                                    '       | <<2>> | EVENT: txn 2: started',
                                    '       | <<2>> | EVENT: txn 2: read own document',
                                    '       | <<2>> | WAITING UNTIL: txn 1: found contested document',
                                    ' <<1>> | EVENT: txn 1: started',
                                    ' <<1>> | EVENT: txn 1: found contested document',
                                    ' <<1>> | WAITING UNTIL: txn 2: completed',
                                    '       | <<2>> | EVENT: txn 2: updated contested document',
                                    '       | <<2>> | EVENT: txn 2: completed',
                                    // txn 1 is retried, because the document in the query changed
                                    ' <<1>> | EVENT: txn 1: started',
                                    ' <<1>> | EVENT: txn 1: did not find any document',
                                    ' <<1>> | EVENT: txn 1: completed',
                                ]);
                            },
                        );

                    test.concurrent('conditional write based on query (contested doc is read first in the query)', async () => {
                        const { docRef1, docRef2, queryDocsInTest } = await setupQueryTest('multi2');

                        const test = new ConcurrentTest();

                        await test.run(
                            async () => {
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('txn 1: started');

                                    // This query is the first to lock `docRef2`
                                    const snaps = await queryDocsInTest(txn, { exclude: docRef1 });
                                    assert(snaps.size, '`docRef2` should be found in the query');

                                    test.event('txn 1: found contested document');
                                    await test.when('txn 2: called `update` on contested document');

                                    txn.update(docRef1, { testName: 'other name' });
                                });
                                test.event('txn 1: completed');
                            },
                            async () => {
                                // Transaction 2 will start after Transaction 1 has already locked `docRef2`
                                await test.when('txn 1: found contested document');
                                await fs.firestore.runTransaction(async txn => {
                                    test.event('txn 2: started');
                                    await txn.get(docRef2);
                                    test.event('txn 2: read own document');

                                    txn.update(docRef2, { testName: 'some other name' });
                                    // Note that this does not mean that the `update` is committed yet!!
                                    test.event('txn 2: called `update` on contested document');
                                    await test.when('txn 1: completed');
                                });
                                test.event('txn 2: completed');
                            },
                        );
                        expect(await readDataRef(docRef1)).toEqual({ testName: 'other name' });
                        expect(await readDataRef(docRef2)).toEqual({ testName: 'some other name' });

                        expect(test.log).toEqual([
                            '       | <<2>> | WAITING UNTIL: txn 1: found contested document',
                            ' <<1>> | EVENT: txn 1: started',
                            ' <<1>> | EVENT: txn 1: found contested document',
                            ' <<1>> | WAITING UNTIL: txn 2: called `update` on contested document',
                            '       | <<2>> | EVENT: txn 2: started',
                            '       | <<2>> | EVENT: txn 2: read own document',
                            '       | <<2>> | EVENT: txn 2: called `update` on contested document',
                            '       | <<2>> | WAITING UNTIL: txn 1: completed',
                            ' <<1>> | EVENT: txn 1: completed',
                            '       | <<2>> | EVENT: txn 2: completed',
                        ]);
                    });

                    notImplementedInJava || // Timeout
                        test.concurrent('create after reading a query', async () => {
                            const testName = 'create';

                            const [docRef1, docRef2] = refs();

                            await docRef1.create(writeData({ testName }));

                            const query = fs.collection.where('testName', '==', testName);

                            const test = new ConcurrentTest();

                            await test.run(
                                async () => {
                                    await fs.firestore.runTransaction(async txn => {
                                        test.event('transaction started');

                                        const snaps = await txn.get(query);
                                        expect(snaps.size).toBe(1);
                                        test.event('transaction locked the query');

                                        await test.when('docRef2 created outside the query');

                                        txn.update(docRef1, { other: 'data' });
                                        txn.update(docRef2, { other: 'data' });
                                    });
                                    test.event('transaction completed');
                                },
                                async () => {
                                    await test.when('transaction locked the query');
                                    await docRef2.create(writeData({ first: 'data' }));
                                    test.event('docRef2 created outside the query');
                                },
                            );

                            expect(test.log).toEqual([
                                '       | <<2>> | WAITING UNTIL: transaction locked the query',
                                ' <<1>> | EVENT: transaction started',
                                ' <<1>> | EVENT: transaction locked the query',
                                ' <<1>> | WAITING UNTIL: docRef2 created outside the query',
                                '       | <<2>> | EVENT: docRef2 created outside the query',
                                ' <<1>> | EVENT: transaction completed',
                            ]);
                        });
                });
        });

        describe('cloud wound-wait validation probes', () => {
            test.concurrent(
                'probe A1: idle beginTransaction does not preempt earlier lock holder',
                async () => {
                    const [docA] = refs();
                    await docA.set(writeData({ value: 'initial' }));

                    const earlyBegun = Promise.withResolvers<void>();
                    const lateReadDocA = Promise.withResolvers<void>();
                    const earlySettled = Promise.withResolvers<void>();

                    let txnEarly: Uint8Array | undefined;
                    let txnLate: Uint8Array | undefined;
                    let earlyCommitOutcome: RpcOutcome<RawCommitResponse> | undefined;
                    let lateCommitOutcome: RpcOutcome<RawCommitResponse> | undefined;
                    let earlySettledWhileLateOpen = false;

                    try {
                        await Promise.all([
                            (async () => {
                                txnEarly = await rawBeginTxn();
                                await time(50);
                                earlyBegun.resolve();

                                await lateReadDocA.promise;
                                await rawReadDoc(docA, txnEarly);
                                earlyCommitOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from early' } }, ['value'])], txnEarly),
                                );
                                earlySettled.resolve();
                            })(),
                            (async () => {
                                await earlyBegun.promise;
                                txnLate = await rawBeginTxn();
                                await rawReadDoc(docA, txnLate);
                                lateReadDocA.resolve();

                                earlySettledWhileLateOpen = await waitOrTimeout(
                                    earlySettled.promise,
                                    connection === 'RUST EMULATOR' ? 750 : 1_500,
                                );
                                lateCommitOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from late' } }, ['value'])], txnLate),
                                );
                            })(),
                        ]);

                        assert(earlyCommitOutcome && lateCommitOutcome);
                        expect({
                            earlySettledWhileLateOpen,
                            earlyOk: earlyCommitOutcome.ok,
                            earlyCode: earlyCommitOutcome.ok ? undefined : earlyCommitOutcome.code,
                            lateOk: lateCommitOutcome.ok,
                            lateCode: lateCommitOutcome.ok ? undefined : lateCommitOutcome.code,
                        }).toEqual({
                            earlySettledWhileLateOpen: false,
                            earlyOk: false,
                            earlyCode: 10,
                            lateOk: true,
                            lateCode: undefined,
                        });
                        expect(await getData(docA.get())).toEqual({ value: 'from late' });
                    } finally {
                        if (txnEarly && !earlyCommitOutcome?.ok) await rawRollback(txnEarly).catch(() => undefined);
                        if (txnLate && !lateCommitOutcome?.ok) await rawRollback(txnLate).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe A2: priority assigned lazily on first lock acquisition across documents',
                async () => {
                    const [docSeed, docA] = refs();
                    await Promise.all([docSeed.set(writeData({ value: 'initial' })), docA.set(writeData({ value: 'initial' }))]);

                    const earlyLockedSeed = Promise.withResolvers<void>();
                    const lateReadDocA = Promise.withResolvers<void>();
                    const earlySettled = Promise.withResolvers<void>();

                    let txnEarly: Uint8Array | undefined;
                    let txnLate: Uint8Array | undefined;
                    let earlyCommitOutcome: RpcOutcome<RawCommitResponse> | undefined;
                    let lateCommitOutcome: RpcOutcome<RawCommitResponse> | undefined;
                    let earlySettledWhileLateOpen = false;

                    try {
                        await Promise.all([
                            (async () => {
                                txnEarly = await rawBeginTxn();
                                await rawReadDoc(docSeed, txnEarly);
                                await time(50);
                                earlyLockedSeed.resolve();

                                await lateReadDocA.promise;
                                await rawReadDoc(docA, txnEarly);
                                earlyCommitOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from early' } }, ['value'])], txnEarly),
                                );
                                earlySettled.resolve();
                            })(),
                            (async () => {
                                await earlyLockedSeed.promise;
                                txnLate = await rawBeginTxn();
                                await rawReadDoc(docA, txnLate);
                                lateReadDocA.resolve();

                                earlySettledWhileLateOpen = await waitOrTimeout(earlySettled.promise, 4_000);
                                lateCommitOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from late' } }, ['value'])], txnLate),
                                );
                            })(),
                        ]);

                        assert(earlyCommitOutcome && lateCommitOutcome);
                        expect({
                            earlySettledWhileLateOpen,
                            earlyOk: earlyCommitOutcome.ok,
                            earlyCode: earlyCommitOutcome.ok ? undefined : earlyCommitOutcome.code,
                            lateOk: lateCommitOutcome.ok,
                            lateCode: lateCommitOutcome.ok ? undefined : lateCommitOutcome.code,
                        }).toEqual({
                            earlySettledWhileLateOpen: true,
                            earlyOk: true,
                            earlyCode: undefined,
                            lateOk: false,
                            lateCode: 10,
                        });
                        expect(await getData(docA.get())).toEqual({ value: 'from early' });
                    } finally {
                        if (txnEarly && !earlyCommitOutcome?.ok) await rawRollback(txnEarly).catch(() => undefined);
                        if (txnLate && !lateCommitOutcome?.ok) await rawRollback(txnLate).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe B1: retryTransaction token wire bytes and inherited original priority',
                async () => {
                    const [docSeedOldest, docSeedMid] = refs();
                    const [docA, docB] = refs();
                    await Promise.all([
                        docSeedOldest.set(writeData({ value: 'initial' })),
                        docSeedMid.set(writeData({ value: 'initial' })),
                        docA.set(writeData({ value: 'initial' })),
                        docB.set(writeData({ value: 'initial' })),
                    ]);

                    let txnOldest: Uint8Array | undefined;
                    let txnTarget1: Uint8Array | undefined;
                    let txnTarget2: Uint8Array | undefined;
                    let txnMid: Uint8Array | undefined;
                    let oldestCommitted = false;
                    let target2Committed = false;
                    let midCommitted = false;

                    try {
                        txnOldest = await rawBeginTxn();
                        await rawReadDoc(docSeedOldest, txnOldest);
                        await time(50);

                        txnTarget1 = await rawBeginTxn();
                        await rawReadDoc(docA, txnTarget1);
                        await time(50);

                        txnMid = await rawBeginTxn();
                        await rawReadDoc(docSeedMid, txnMid);

                        await rawReadDoc(docA, txnOldest);
                        const oldestOutcome = await captureRpcOutcome(() =>
                            rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from oldest' } }, ['value'])], txnOldest),
                        );
                        expect(oldestOutcome.ok).toBe(true);
                        oldestCommitted = oldestOutcome.ok;

                        const attempt1Outcome = await captureRpcOutcome(() =>
                            rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from target1' } }, ['value'])], txnTarget1),
                        );
                        const attempt1Code = attempt1Outcome.ok ? undefined : attempt1Outcome.code;

                        txnTarget2 = await rawBeginTxn(txnTarget1);
                        const sameTokenBytes = Buffer.from(txnTarget2).equals(Buffer.from(txnTarget1));
                        const activeTarget2 = txnTarget2;
                        const activeMid = txnMid;

                        const midReadDocB = Promise.withResolvers<void>();
                        const target2Settled = Promise.withResolvers<void>();
                        let midOutcome: RpcOutcome<RawCommitResponse> | undefined;
                        let target2Outcome: RpcOutcome<RawCommitResponse> | undefined;
                        let target2SettledWhileMidOpen = false;

                        await Promise.all([
                            (async () => {
                                await rawReadDoc(docB, activeMid);
                                midReadDocB.resolve();
                                target2SettledWhileMidOpen = await waitOrTimeout(target2Settled.promise, 4_000);
                                midOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docB, { value: { stringValue: 'from mid' } }, ['value'])], activeMid),
                                );
                                midCommitted = midOutcome.ok;
                            })(),
                            (async () => {
                                await midReadDocB.promise;
                                await rawReadDoc(docB, activeTarget2);
                                target2Outcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docB, { value: { stringValue: 'from target2' } }, ['value'])], activeTarget2),
                                );
                                target2Committed = target2Outcome.ok;
                                target2Settled.resolve();
                            })(),
                        ]);

                        assert(midOutcome && target2Outcome);
                        expect({
                            sameTokenBytes,
                            attempt1Code,
                            target2SettledWhileMidOpen,
                            target2Ok: target2Outcome.ok,
                            target2Code: target2Outcome.ok ? undefined : target2Outcome.code,
                            midOk: midOutcome.ok,
                            midCode: midOutcome.ok ? undefined : midOutcome.code,
                        }).toEqual({
                            sameTokenBytes: false,
                            attempt1Code: 10,
                            target2SettledWhileMidOpen: true,
                            target2Ok: true,
                            target2Code: undefined,
                            midOk: false,
                            midCode: 10,
                        });
                        expect(await getData(docB.get())).toEqual({ value: 'from target2' });
                    } finally {
                        if (txnOldest && !oldestCommitted) await rawRollback(txnOldest).catch(() => undefined);
                        if (txnTarget1 && !txnTarget2) await rawRollback(txnTarget1).catch(() => undefined);
                        if (txnTarget2 && !target2Committed) await rawRollback(txnTarget2).catch(() => undefined);
                        if (txnMid && !midCommitted) await rawRollback(txnMid).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe B2: delayed rollback(attempt_1) after retryTransaction attempt_2 is active',
                async () => {
                    const [docA, docB] = refs();
                    await Promise.all([docA.set(writeData({ value: 'initial' })), docB.set(writeData({ value: 'initial' }))]);

                    let txnOldest: Uint8Array | undefined;
                    let attempt1: Uint8Array | undefined;
                    let attempt2: Uint8Array | undefined;
                    let oldestCommitted = false;
                    let attempt2Committed = false;

                    try {
                        txnOldest = await rawBeginTxn();
                        await rawReadDoc(docA, txnOldest);
                        await time(50);

                        attempt1 = await rawBeginTxn();
                        await rawReadDoc(docA, attempt1);

                        const oldestOutcome = await captureRpcOutcome(() =>
                            rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from oldest' } }, ['value'])], txnOldest),
                        );
                        expect(oldestOutcome.ok).toBe(true);
                        oldestCommitted = oldestOutcome.ok;

                        const attempt1Outcome = await captureRpcOutcome(() =>
                            rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from attempt1' } }, ['value'])], attempt1),
                        );
                        expect(attempt1Outcome.ok ? undefined : attempt1Outcome.code).toBe(10);

                        attempt2 = await rawBeginTxn(attempt1);
                        const sameTokenBytes = Buffer.from(attempt2).equals(Buffer.from(attempt1));
                        await rawReadDoc(docB, attempt2);

                        const delayedRollbackTarget = attempt1;
                        const delayedRollbackOutcome = await captureRpcOutcome(() => rawRollback(delayedRollbackTarget));
                        const activeAttempt2 = attempt2;
                        const attempt2CommitOutcome = await captureRpcOutcome(() =>
                            rawCommit([rawUpdateWrite(docB, { value: { stringValue: 'from attempt2' } }, ['value'])], activeAttempt2),
                        );
                        attempt2Committed = attempt2CommitOutcome.ok;

                        expect({
                            sameTokenBytes,
                            delayedRollbackOk: delayedRollbackOutcome.ok,
                            delayedRollbackCode: delayedRollbackOutcome.ok ? undefined : delayedRollbackOutcome.code,
                            attempt2CommitOk: attempt2CommitOutcome.ok,
                            attempt2CommitCode: attempt2CommitOutcome.ok ? undefined : attempt2CommitOutcome.code,
                        }).toEqual({
                            sameTokenBytes: false,
                            delayedRollbackOk: true,
                            delayedRollbackCode: undefined,
                            attempt2CommitOk: true,
                            attempt2CommitCode: undefined,
                        });
                        expect(await getData(docB.get())).toEqual({ value: 'from attempt2' });
                    } finally {
                        if (txnOldest && !oldestCommitted) await rawRollback(txnOldest).catch(() => undefined);
                        if (attempt1 && !attempt2) await rawRollback(attempt1).catch(() => undefined);
                        if (attempt2 && !attempt2Committed) await rawRollback(attempt2).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe C1: older writer wounds younger granted holder without wounding conflicting blind write waiter',
                async () => {
                    const [docSeed, docA] = refs();
                    await Promise.all([docSeed.set(writeData({ value: 'initial' })), docA.set(writeData({ value: 'initial' }))]);

                    let txnOld: Uint8Array | undefined;
                    let txnMid: Uint8Array | undefined;
                    let txnYoung: Uint8Array | undefined;
                    let oldCommitted = false;
                    let midCommitted = false;
                    let youngCommitted = false;

                    try {
                        txnOld = await rawBeginTxn();
                        await rawReadDoc(docSeed, txnOld);
                        await time(50);

                        txnMid = await rawBeginTxn();
                        await time(50);
                        txnYoung = await rawBeginTxn();

                        const activeOld = txnOld;
                        const activeMid = txnMid;
                        const activeYoung = txnYoung;

                        const midReadDocA = Promise.withResolvers<void>();
                        const youngQueued = Promise.withResolvers<void>();
                        const oldSettled = Promise.withResolvers<void>();

                        let oldOutcome: RpcOutcome<RawCommitResponse> | undefined;
                        let midOutcome: RpcOutcome<RawCommitResponse> | undefined;
                        let youngOutcome: RpcOutcome<RawCommitResponse> | undefined;
                        let youngBlockedInitially = false;
                        let oldSettledWhileMidOpen = false;

                        await Promise.all([
                            (async () => {
                                await rawReadDoc(docA, activeMid);
                                midReadDocA.resolve();
                                oldSettledWhileMidOpen = await waitOrTimeout(oldSettled.promise, 4_000);
                                midOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { fromMid: { booleanValue: true } }, ['fromMid'])], activeMid),
                                );
                                midCommitted = midOutcome.ok;
                            })(),
                            (async () => {
                                await midReadDocA.promise;
                                let youngDone = false;
                                const youngPromise = captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { fromYoung: { booleanValue: true } }, ['fromYoung'])], activeYoung),
                                ).then(res => {
                                    youngDone = true;
                                    return res;
                                });
                                await time(250);
                                youngBlockedInitially = !youngDone;
                                youngQueued.resolve();
                                youngOutcome = await youngPromise;
                                youngCommitted = youngOutcome.ok;
                            })(),
                            (async () => {
                                await youngQueued.promise;
                                oldOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { fromOld: { booleanValue: true } }, ['fromOld'])], activeOld),
                                );
                                oldCommitted = oldOutcome.ok;
                                oldSettled.resolve();
                            })(),
                        ]);

                        assert(oldOutcome && midOutcome && youngOutcome);
                        expect({
                            youngBlockedInitially,
                            oldSettledWhileMidOpen,
                            oldOk: oldOutcome.ok,
                            oldCode: oldOutcome.ok ? undefined : oldOutcome.code,
                            midOk: midOutcome.ok,
                            midCode: midOutcome.ok ? undefined : midOutcome.code,
                            youngOk: youngOutcome.ok,
                            youngCode: youngOutcome.ok ? undefined : youngOutcome.code,
                        }).toEqual({
                            youngBlockedInitially: true,
                            oldSettledWhileMidOpen: true,
                            oldOk: true,
                            oldCode: undefined,
                            midOk: false,
                            midCode: 10,
                            youngOk: true,
                            youngCode: undefined,
                        });
                        assert(oldOutcome.ok && youngOutcome.ok);
                        expect(commitTimeNs(oldOutcome.value)).toBeLessThan(commitTimeNs(youngOutcome.value));
                        expect(await getData(docA.get())).toEqual({ value: 'initial', fromOld: true, fromYoung: true });
                    } finally {
                        if (txnOld && !oldCommitted) await rawRollback(txnOld).catch(() => undefined);
                        if (txnMid && !midCommitted) await rawRollback(txnMid).catch(() => undefined);
                        if (txnYoung && !youngCommitted) await rawRollback(txnYoung).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe C2a: intermediate reader vs younger queued write waiter',
                async () => {
                    const [docSeed, docA] = refs();
                    await Promise.all([docSeed.set(writeData({ value: 'initial' })), docA.set(writeData({ value: 'initial' }))]);

                    let t0: Uint8Array | undefined;
                    let t1: Uint8Array | undefined;
                    let t2: Uint8Array | undefined;
                    let t0Committed = false;
                    let t1Committed = false;
                    let t2Committed = false;

                    try {
                        t0 = await rawBeginTxn();
                        await rawReadDoc(docA, t0);
                        await time(50);

                        t1 = await rawBeginTxn();
                        await rawReadDoc(docSeed, t1);
                        await time(50);

                        t2 = await rawBeginTxn();

                        const activeT0 = t0;
                        const activeT1 = t1;
                        const activeT2 = t2;

                        const t2Queued = Promise.withResolvers<void>();
                        const t1ReadStarted = Promise.withResolvers<void>();
                        const t1ReadFinished = Promise.withResolvers<void>();
                        const t0Released = Promise.withResolvers<void>();

                        let t1BypassedT2WhileT0Open = false;
                        let t2BlockedInitially = false;
                        let t1ReadOutcome: RpcOutcome<RawBatchGetChunk> | undefined;
                        let t2Outcome: RpcOutcome<RawCommitResponse> | undefined;

                        await Promise.all([
                            (async () => {
                                await t1ReadStarted.promise;
                                t1BypassedT2WhileT0Open = await waitOrTimeout(t1ReadFinished.promise, 1_500);
                                const t0Commit = await captureRpcOutcome(() => rawCommit([], activeT0));
                                t0Committed = t0Commit.ok;
                                t0Released.resolve();
                            })(),
                            (async () => {
                                let t2Done = false;
                                const t2Promise = captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from t2' } }, ['value'])], activeT2),
                                ).then(res => {
                                    t2Done = true;
                                    return res;
                                });
                                await time(250);
                                t2BlockedInitially = !t2Done;
                                t2Queued.resolve();
                                t2Outcome = await t2Promise;
                                t2Committed = t2Outcome.ok;
                            })(),
                            (async () => {
                                await t2Queued.promise;
                                t1ReadStarted.resolve();
                                t1ReadOutcome = await captureRpcOutcome(() => rawReadDoc(docA, activeT1));
                                t1ReadFinished.resolve();
                                await t0Released.promise;
                                if (t1ReadOutcome.ok) {
                                    const t1Commit = await captureRpcOutcome(() => rawCommit([], activeT1));
                                    t1Committed = t1Commit.ok;
                                }
                            })(),
                        ]);

                        assert(t1ReadOutcome && t2Outcome);
                        const t1ReadValue = t1ReadOutcome.ok ? t1ReadOutcome.value.found?.fields?.value?.stringValue : undefined;
                        expect({
                            t2BlockedInitially,
                            t1BypassedT2WhileT0Open,
                            t1ReadOk: t1ReadOutcome.ok,
                            t1ReadCode: t1ReadOutcome.ok ? undefined : t1ReadOutcome.code,
                            t1ReadValue,
                            t2Ok: t2Outcome.ok,
                            t2Code: t2Outcome.ok ? undefined : t2Outcome.code,
                        }).toEqual({
                            t2BlockedInitially: true,
                            t1BypassedT2WhileT0Open: true,
                            t1ReadOk: true,
                            t1ReadCode: undefined,
                            t1ReadValue: 'initial',
                            t2Ok: true,
                            t2Code: undefined,
                        });
                        expect(await getData(docA.get())).toEqual({ value: 'from t2' });
                    } finally {
                        if (t0 && !t0Committed) await rawRollback(t0).catch(() => undefined);
                        if (t1 && !t1Committed) await rawRollback(t1).catch(() => undefined);
                        if (t2 && !t2Committed) await rawRollback(t2).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe C2b: younger reader behind older queued write waiter',
                async () => {
                    const [docA] = refs();
                    await docA.set(writeData({ value: 'initial' }));

                    let t0: Uint8Array | undefined;
                    let t1: Uint8Array | undefined;
                    let t2: Uint8Array | undefined;
                    let t0Committed = false;
                    let t1Committed = false;
                    let t2Committed = false;

                    try {
                        t0 = await rawBeginTxn();
                        await time(50);
                        t1 = await rawBeginTxn();
                        await time(50);
                        t2 = await rawBeginTxn();

                        const activeT0 = t0;
                        const activeT1 = t1;
                        const activeT2 = t2;

                        const ct = new ConcurrentTest({ timeout: 20_000 });
                        let t1BlockedInitially = false;
                        let t2ReadDone = false;
                        let t2BlockedWhileT0Open = false;
                        let t1Outcome: RpcOutcome<RawCommitResponse> | undefined;
                        let t2ReadOutcome: RpcOutcome<RawBatchGetChunk> | undefined;

                        await ct.run(
                            async () => {
                                await rawReadDoc(docA, activeT0);
                                ct.event('t0 read docA');
                                await ct.when('t2 read started');
                                await time(300);
                                t2BlockedWhileT0Open = !t2ReadDone;
                                const t0Commit = await captureRpcOutcome(() => rawCommit([], activeT0));
                                t0Committed = t0Commit.ok;
                                ct.event('t0 released');
                            },
                            async () => {
                                await ct.when('t0 read docA');
                                let t1Done = false;
                                const t1Promise = captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(docA, { value: { stringValue: 'from t1' } }, ['value'])], activeT1),
                                ).then(res => {
                                    t1Done = true;
                                    return res;
                                });
                                await time(250);
                                t1BlockedInitially = !t1Done;
                                ct.event('t1 queued');
                                t1Outcome = await t1Promise;
                                t1Committed = t1Outcome.ok;
                            },
                            async () => {
                                await ct.when('t1 queued');
                                ct.event('t2 read started');
                                t2ReadOutcome = await captureRpcOutcome(() => rawReadDoc(docA, activeT2));
                                t2ReadDone = true;
                                if (t2ReadOutcome.ok) {
                                    const t2Commit = await captureRpcOutcome(() => rawCommit([], activeT2));
                                    t2Committed = t2Commit.ok;
                                }
                            },
                        );

                        assert(t1Outcome && t2ReadOutcome);
                        const t2ReadValue = t2ReadOutcome.ok ? t2ReadOutcome.value.found?.fields?.value?.stringValue : undefined;
                        expect({
                            t1BlockedInitially,
                            t2BlockedWhileT0Open,
                            t1Ok: t1Outcome.ok,
                            t2ReadOk: t2ReadOutcome.ok,
                            t2ReadValue,
                        }).toEqual({
                            t1BlockedInitially: true,
                            t2BlockedWhileT0Open: true,
                            t1Ok: true,
                            t2ReadOk: true,
                            t2ReadValue: 'from t1',
                        });
                    } finally {
                        if (t0 && !t0Committed) await rawRollback(t0).catch(() => undefined);
                        if (t1 && !t1Committed) await rawRollback(t1).catch(() => undefined);
                        if (t2 && !t2Committed) await rawRollback(t2).catch(() => undefined);
                    }
                },
                30_000,
            );

            async function runProbeD1(order: 'ref1 < ref2' | 'ref2 < ref1') {
                const [doc1, doc2] = orderedRefs(order);
                await Promise.all([doc1.set(writeData({ value: 'initial' })), doc2.set(writeData({ value: 'initial' }))]);

                const ct = new ConcurrentTest({ timeout: 20_000 });
                let txnOlder: Uint8Array | undefined;
                let txnOlderCommitted = false;
                let batchBlockedInitially = false;
                let txnOlderOutcome: RpcOutcome<RawCommitResponse> | undefined;
                let batchOutcome: RpcOutcome<RawCommitResponse> | undefined;

                try {
                    await ct.run(
                        async () => {
                            txnOlder = await rawBeginTxn();
                            await rawReadDoc(doc1, txnOlder);
                            ct.event('txn_older read doc1');
                            await ct.when('batch queued');
                            txnOlderOutcome = await captureRpcOutcome(() =>
                                rawCommit(
                                    [
                                        rawUpdateWrite(doc1, { fromTxn: { booleanValue: true } }, ['fromTxn']),
                                        rawUpdateWrite(doc2, { fromTxn: { booleanValue: true } }, ['fromTxn']),
                                    ],
                                    txnOlder,
                                ),
                            );
                            txnOlderCommitted = txnOlderOutcome.ok;
                            ct.event('txn_older committed');
                        },
                        async () => {
                            await ct.when('txn_older read doc1');
                            let batchDone = false;
                            const batchPromise = captureRpcOutcome(() =>
                                rawCommit([
                                    rawUpdateWrite(doc1, { fromBatch: { booleanValue: true } }, ['fromBatch']),
                                    rawUpdateWrite(doc2, { fromBatch: { booleanValue: true } }, ['fromBatch']),
                                ]),
                            ).then(res => {
                                batchDone = true;
                                return res;
                            });
                            await time(250);
                            batchBlockedInitially = !batchDone;
                            ct.event('batch queued');
                            batchOutcome = await batchPromise;
                        },
                    );

                    assert(txnOlderOutcome && batchOutcome);
                    expect({
                        order,
                        batchBlockedInitially,
                        txnOlderOk: txnOlderOutcome.ok,
                        txnOlderCode: txnOlderOutcome.ok ? undefined : txnOlderOutcome.code,
                        batchOk: batchOutcome.ok,
                        batchCode: batchOutcome.ok ? undefined : batchOutcome.code,
                    }).toEqual({
                        order,
                        batchBlockedInitially: true,
                        txnOlderOk: true,
                        txnOlderCode: undefined,
                        batchOk: true,
                        batchCode: undefined,
                    });
                    assert(txnOlderOutcome.ok && batchOutcome.ok);
                    expect(commitTimeNs(txnOlderOutcome.value)).toBeLessThan(commitTimeNs(batchOutcome.value));
                    expect(await getData(doc1.get())).toEqual({ value: 'initial', fromTxn: true, fromBatch: true });
                    expect(await getData(doc2.get())).toEqual({ value: 'initial', fromTxn: true, fromBatch: true });
                } finally {
                    if (txnOlder && !txnOlderCommitted) await rawRollback(txnOlder).catch(() => undefined);
                }
            }

            test.concurrent(
                'probe D1: non-transactional 2-doc WriteBatch colliding with older RW txn (ref1 < ref2)',
                async () => runProbeD1('ref1 < ref2'),
                30_000,
            );

            test.concurrent(
                'probe D1: non-transactional 2-doc WriteBatch colliding with older RW txn (ref2 < ref1)',
                async () => runProbeD1('ref2 < ref1'),
                30_000,
            );

            async function runProbeD2(order: 'ref1 < ref2' | 'ref2 < ref1') {
                const [doc1, doc2] = orderedRefs(order);
                await Promise.all([doc1.set(writeData({ value: 'initial' })), doc2.set(writeData({ value: 'initial' }))]);

                const blockerRead = Promise.withResolvers<void>();
                const batchQueued = Promise.withResolvers<void>();
                const youngerReadStarted = Promise.withResolvers<void>();
                const youngerReadFinished = Promise.withResolvers<void>();
                const blockerReleased = Promise.withResolvers<void>();
                const batchSettled = Promise.withResolvers<void>();

                let txnBlocker: Uint8Array | undefined;
                let txnYounger: Uint8Array | undefined;
                let blockerCommitted = false;
                let youngerCommitted = false;
                let batchBlockedInitially = false;
                let youngerReadDoc2WhileBlockerOpen = false;
                let batchSettledWhileYoungerOpen = false;
                let youngerReadOutcome: RpcOutcome<RawBatchGetChunk> | undefined;
                let youngerCommitOutcome: RpcOutcome<RawCommitResponse> | undefined;
                let batchOutcome: RpcOutcome<RawCommitResponse> | undefined;

                try {
                    await Promise.all([
                        (async () => {
                            txnBlocker = await rawBeginTxn();
                            await rawReadDoc(doc1, txnBlocker);
                            blockerRead.resolve();
                            await youngerReadStarted.promise;
                            youngerReadDoc2WhileBlockerOpen = await waitOrTimeout(
                                youngerReadFinished.promise,
                                connection === 'RUST EMULATOR' ? 750 : 1_500,
                            );
                            const blockerCommit = await captureRpcOutcome(() => rawCommit([], txnBlocker));
                            blockerCommitted = blockerCommit.ok;
                            blockerReleased.resolve();
                        })(),
                        (async () => {
                            await blockerRead.promise;
                            let batchDone = false;
                            const batchPromise = captureRpcOutcome(() =>
                                rawCommit([
                                    rawUpdateWrite(doc1, { fromBatch: { booleanValue: true } }, ['fromBatch']),
                                    rawUpdateWrite(doc2, { fromBatch: { booleanValue: true } }, ['fromBatch']),
                                ]),
                            ).then(res => {
                                batchDone = true;
                                return res;
                            });
                            await time(250);
                            batchBlockedInitially = !batchDone;
                            batchQueued.resolve();
                            batchOutcome = await batchPromise;
                            batchSettled.resolve();
                        })(),
                        (async () => {
                            await batchQueued.promise;
                            const activeYounger = await rawBeginTxn();
                            txnYounger = activeYounger;
                            youngerReadStarted.resolve();
                            youngerReadOutcome = await captureRpcOutcome(() => rawReadDoc(doc2, activeYounger));
                            youngerReadFinished.resolve();
                            await blockerReleased.promise;
                            batchSettledWhileYoungerOpen = await waitOrTimeout(batchSettled.promise, 500);
                            if (youngerReadOutcome.ok) {
                                youngerCommitOutcome = await captureRpcOutcome(() =>
                                    rawCommit(
                                        [
                                            rawUpdateWrite(doc1, { fromYounger: { booleanValue: true } }, ['fromYounger']),
                                            rawUpdateWrite(doc2, { fromYounger: { booleanValue: true } }, ['fromYounger']),
                                        ],
                                        activeYounger,
                                    ),
                                );
                                youngerCommitted = youngerCommitOutcome.ok;
                            }
                        })(),
                    ]);

                    assert(youngerReadOutcome && batchOutcome);
                    const youngerReadSawFromBatch = youngerReadOutcome.ok
                        ? youngerReadOutcome.value.found?.fields?.fromBatch?.booleanValue
                        : undefined;
                    expect({
                        order,
                        batchBlockedInitially,
                        youngerReadDoc2WhileBlockerOpen,
                        youngerReadSawFromBatch,
                        batchSettledWhileYoungerOpen,
                        youngerReadOk: youngerReadOutcome.ok,
                        youngerReadCode: youngerReadOutcome.ok ? undefined : youngerReadOutcome.code,
                        youngerCommitOk: youngerCommitOutcome?.ok ?? false,
                        youngerCommitCode: youngerCommitOutcome && !youngerCommitOutcome.ok ? youngerCommitOutcome.code : undefined,
                        batchOk: batchOutcome.ok,
                        batchCode: batchOutcome.ok ? undefined : batchOutcome.code,
                    }).toEqual({
                        order,
                        batchBlockedInitially: true,
                        youngerReadDoc2WhileBlockerOpen: false,
                        youngerReadSawFromBatch: true,
                        batchSettledWhileYoungerOpen: true,
                        youngerReadOk: true,
                        youngerReadCode: undefined,
                        youngerCommitOk: true,
                        youngerCommitCode: undefined,
                        batchOk: true,
                        batchCode: undefined,
                    });
                    assert(batchOutcome.ok && youngerCommitOutcome?.ok);
                    expect(commitTimeNs(batchOutcome.value)).toBeLessThan(commitTimeNs(youngerCommitOutcome.value));
                    expect(await getData(doc1.get())).toEqual({ value: 'initial', fromYounger: true, fromBatch: true });
                    expect(await getData(doc2.get())).toEqual({ value: 'initial', fromYounger: true, fromBatch: true });
                } finally {
                    if (txnBlocker && !blockerCommitted) await rawRollback(txnBlocker).catch(() => undefined);
                    if (txnYounger && !youngerCommitted) await rawRollback(txnYounger).catch(() => undefined);
                }
            }

            test.concurrent(
                'probe D2: non-transactional 2-doc WriteBatch entering before younger RW txn locks doc2 (ref1 < ref2)',
                async () => runProbeD2('ref1 < ref2'),
                30_000,
            );

            test.concurrent(
                'probe D2: non-transactional 2-doc WriteBatch entering before younger RW txn locks doc2 (ref2 < ref1)',
                async () => runProbeD2('ref2 < ref1'),
                30_000,
            );

            test.concurrent(
                'probe D3: single-document non-transactional commit waits behind RW txn',
                async () => {
                    const [doc1] = refs();
                    await doc1.set(writeData({ value: 'initial' }));

                    const ct = new ConcurrentTest({ timeout: 20_000 });
                    let txn: Uint8Array | undefined;
                    let txnCommitted = false;
                    let singleBlockedInitially = false;
                    let txnOutcome: RpcOutcome<RawCommitResponse> | undefined;
                    let singleOutcome: RpcOutcome<RawCommitResponse> | undefined;

                    try {
                        await ct.run(
                            async () => {
                                txn = await rawBeginTxn();
                                await rawReadDoc(doc1, txn);
                                ct.event('txn read doc1');
                                await ct.when('single write queued');
                                txnOutcome = await captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(doc1, { fromTxn: { booleanValue: true } }, ['fromTxn'])], txn),
                                );
                                txnCommitted = txnOutcome.ok;
                                ct.event('txn committed');
                            },
                            async () => {
                                await ct.when('txn read doc1');
                                let singleDone = false;
                                const singlePromise = captureRpcOutcome(() =>
                                    rawCommit([rawUpdateWrite(doc1, { fromSingle: { booleanValue: true } }, ['fromSingle'])]),
                                ).then(res => {
                                    singleDone = true;
                                    return res;
                                });
                                await time(250);
                                singleBlockedInitially = !singleDone;
                                ct.event('single write queued');
                                singleOutcome = await singlePromise;
                                ct.event('single write finished');
                            },
                        );

                        assert(txnOutcome && singleOutcome);
                        expect({
                            singleBlockedInitially,
                            txnOk: txnOutcome.ok,
                            singleOk: singleOutcome.ok,
                        }).toEqual({
                            singleBlockedInitially: true,
                            txnOk: true,
                            singleOk: true,
                        });
                        assert(txnOutcome.ok && singleOutcome.ok);
                        expect(commitTimeNs(txnOutcome.value)).toBeLessThan(commitTimeNs(singleOutcome.value));
                        expect(await getData(doc1.get())).toEqual({ value: 'initial', fromTxn: true, fromSingle: true });
                    } finally {
                        if (txn && !txnCommitted) await rawRollback(txn).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe D4: pessimistic read/write locking on missing document path',
                async () => {
                    const [missingRef] = refs();

                    let txnA: Uint8Array | undefined;
                    let txnB: Uint8Array | undefined;
                    let txnACommitted = false;
                    let txnBCommitted = false;

                    try {
                        txnA = await rawBeginTxn();
                        await time(50);
                        txnB = await rawBeginTxn();

                        const activeA = txnA;
                        const activeB = txnB;

                        const ct = new ConcurrentTest({ timeout: 20_000 });
                        let txnBCommitDone = false;
                        let txnBBlockedInitially = false;
                        let txnBBlockedWhileTxnAOpen = false;
                        let txnAOutcome: RpcOutcome<RawCommitResponse> | undefined;
                        let txnBOutcome: RpcOutcome<RawCommitResponse> | undefined;

                        await ct.run(
                            async () => {
                                const chunkA = await rawReadDoc(missingRef, activeA);
                                expect(chunkA.missing).toBeDefined();
                                ct.event('txnA read missing');
                                await ct.when('txnB commit queued');
                                await time(300);
                                txnBBlockedWhileTxnAOpen = !txnBCommitDone;
                                txnAOutcome = await captureRpcOutcome(() =>
                                    rawCommit(
                                        [rawUpdateWrite(missingRef, { value: { stringValue: 'from A' }, ttl: rawTtlValue() })],
                                        activeA,
                                    ),
                                );
                                txnACommitted = txnAOutcome.ok;
                                ct.event('txnA committed');
                            },
                            async () => {
                                await ct.when('txnA read missing');
                                const chunkB = await rawReadDoc(missingRef, activeB);
                                expect(chunkB.missing).toBeDefined();
                                const txnBPromise = captureRpcOutcome(() =>
                                    rawCommit(
                                        [rawUpdateWrite(missingRef, { value: { stringValue: 'from B' }, ttl: rawTtlValue() })],
                                        activeB,
                                    ),
                                ).then(res => {
                                    txnBCommitDone = true;
                                    return res;
                                });
                                await time(250);
                                txnBBlockedInitially = !txnBCommitDone;
                                ct.event('txnB commit queued');
                                txnBOutcome = await txnBPromise;
                                txnBCommitted = txnBOutcome.ok;
                            },
                        );

                        assert(txnAOutcome && txnBOutcome);
                        expect({
                            txnBBlockedInitially,
                            txnBBlockedWhileTxnAOpen,
                            txnAOk: txnAOutcome.ok,
                            txnACode: txnAOutcome.ok ? undefined : txnAOutcome.code,
                            txnBOk: txnBOutcome.ok,
                            txnBCode: txnBOutcome.ok ? undefined : txnBOutcome.code,
                        }).toEqual({
                            txnBBlockedInitially: true,
                            txnBBlockedWhileTxnAOpen: true,
                            txnAOk: true,
                            txnACode: undefined,
                            txnBOk: false,
                            txnBCode: 10,
                        });
                        expect(await getData(missingRef.get())).toEqual({ value: 'from A' });
                    } finally {
                        if (txnA && !txnACommitted) await rawRollback(txnA).catch(() => undefined);
                        if (txnB && !txnBCommitted) await rawRollback(txnB).catch(() => undefined);
                    }
                },
                30_000,
            );

            test.concurrent(
                'probe D5: pessimistic delete on missing document still contends for write lock and wounds younger reader',
                async () => {
                    const [missingRef, unrelatedRef] = refs();

                    let txnA: Uint8Array | undefined;
                    let txnB: Uint8Array | undefined;
                    let txnACommitted = false;
                    let txnBCommitted = false;

                    try {
                        txnA = await rawBeginTxn();
                        await time(50);
                        txnB = await rawBeginTxn();

                        const activeA = txnA;
                        const activeB = txnB;

                        const chunkA = await rawReadDoc(missingRef, activeA);
                        expect(chunkA.missing).toBeDefined();
                        const chunkB = await rawReadDoc(missingRef, activeB);
                        expect(chunkB.missing).toBeDefined();

                        // Older txnA commits delete on the missing document, wounding younger txnB.
                        const txnAOutcome = await captureRpcOutcome(() => rawCommit([{ delete: docResourceName(missingRef) }], activeA));
                        txnACommitted = txnAOutcome.ok;

                        // Even though txnB only writes an unrelated document, txnA's missing-doc
                        // delete wounded txnB's read lock on missingRef.
                        const txnBOutcome = await captureRpcOutcome(() =>
                            rawCommit([rawUpdateWrite(unrelatedRef, { value: { stringValue: 'from B' }, ttl: rawTtlValue() })], activeB),
                        );
                        txnBCommitted = txnBOutcome.ok;

                        expect({
                            txnAOk: txnAOutcome.ok,
                            txnACode: txnAOutcome.ok ? undefined : txnAOutcome.code,
                            txnBOk: txnBOutcome.ok,
                            txnBCode: txnBOutcome.ok ? undefined : txnBOutcome.code,
                        }).toEqual({
                            txnAOk: true,
                            txnACode: undefined,
                            txnBOk: false,
                            txnBCode: 10,
                        });
                        expect(await unrelatedRef.get()).toHaveProperty('exists', false);
                    } finally {
                        if (txnA && !txnACommitted) await rawRollback(txnA).catch(() => undefined);
                        if (txnB && !txnBCommitted) await rawRollback(txnB).catch(() => undefined);
                    }
                },
                30_000,
            );
        });

        type UsedRefs = DocumentReference[] | { read: DocumentReference[]; write: DocumentReference[] };
        async function runTxn(
            name: string,
            refs: UsedRefs,
            runAfterGet: () => void | Promise<void | { awaitAfterTxn?: Promise<unknown> }>,
        ) {
            const { read, write } = Array.isArray(refs) ? { read: refs, write: refs } : refs;
            let awaitAfterTxn: Promise<unknown> | undefined;
            let tries = 0;
            await fs.firestore.runTransaction(async txn => {
                tries++;
                await txn.getAll(...read);

                ({ awaitAfterTxn } = (await runAfterGet()) ?? {});

                for (const ref of write) {
                    txn.set(ref, writeData({ [name]: { tries } }), { merge: true });
                }
            });
            await awaitAfterTxn;
        }

        async function innerTxn(name: string, refs: UsedRefs) {
            const { resolve, promise } = Promise.withResolvers<void>();
            const innerTxnCompleted = runTxn(name, refs, resolve);
            await promise;
            return { innerTxnCompleted };
        }
    });

    function refs() {
        return [fs.collection.doc(), fs.collection.doc()] as const;
    }
});

async function getData(promisedSnap: DocumentSnapshot | Promise<DocumentSnapshot>) {
    const snap = await promisedSnap;
    expect(snap.exists).toBeTrue();
    return readData(snap.data());
}
