import { DocumentReference, DocumentSnapshot, FieldPath, FieldValue, Transaction } from '@google-cloud/firestore';
import assert from 'assert';
import { setTimeout as time } from 'timers/promises';
import { ConcurrentTest, editions, readData, readDataRef, writeData } from './utils';

const optimisticDatabases = editions.filter(e => e.concurrencyMode === 'optimistic');
const suite = optimisticDatabases.length ? describe.each(optimisticDatabases) : describe.skip.each(editions);

suite('$description - concurrent tests', fs => {
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

    describe('optimistic concurrency', () => {
        test.concurrent.each(['existing', 'non-existing'] as const)(
            'no retry when reading %s document that is changed by another process before the txn ends',
            // Optimistic concurrency allows reads to proceed without locking documents.
            // If a transaction only reads a document and does NO writes, it does not
            // need to retry even if the document was updated concurrently, because
            // there is no write-skew or inconsistent state to commit.
            async which => {
                const t = new ConcurrentTest();
                const [ref] = refs();

                if (which === 'existing') await ref.set(writeData({ version: 0 }));

                await t.run(
                    // Process 1
                    async () => {
                        // The transaction in process 1 will run only once, because after reading
                        // the document, no commit is done. Contention is only checked on `commit`
                        // of the transaction.
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn attempt ${++attempt} started`);
                            const snap = await txn.get(ref);
                            const version = snap.get('version') ?? 0;
                            t.event(`read version ${version}`);

                            await t.when('document updated');

                            // Even though the document was updated while this transaction was still
                            // ongoing, no retry is done because no write was performed.
                            t.event('finishing txn');
                        });
                        t.event('txn completed');
                    },
                    // Process 2
                    async () => {
                        // Update the document after the txn read the document
                        await t.when('read version 0');
                        t.event('updating document');
                        await ref.set(writeData({ version: FieldValue.increment(1) }), { merge: true });
                        t.event('document updated');
                    },
                );

                expect(await getData(ref.get())).toEqual({ version: 1 });

                expect(t.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: read version 0',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: read version 0',
                    ' <<1>> | WAITING UNTIL: document updated',
                    '       | <<2>> | EVENT: updating document',
                    '       | <<2>> | EVENT: document updated',
                    ' <<1>> | EVENT: finishing txn',
                    ' <<1>> | EVENT: txn completed',
                ]);
            },
        );

        test.concurrent.each(['existing', 'non-existing'] as const)(
            'retry when updating any other document after reading %s document that is concurrently changed by another process',
            // If a transaction reads a document, and then performs writes (even to
            // OTHER documents), it must retry if the read document was changed
            // concurrently. This prevents writing derived data based on an
            // inconsistent or outdated read state.
            async which => {
                const t = new ConcurrentTest();
                // sharedRef is read by process 1 and updated by process 2
                // writeRef is updated by process 1
                const [sharedRef, writeRef] = refs();

                if (which === 'existing') await sharedRef.set(writeData({ version: 0 }));

                await t.run(
                    // Process 1
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn attempt ${++attempt} started`);
                            const snap = await txn.get(sharedRef);
                            const version = snap.get('version') ?? 0;
                            t.event(`read version ${version}`);

                            if (attempt === 1) await t.when('document updated');
                            t.event('creating document');

                            txn.create(writeRef, writeData({ version }));
                        });
                        t.event('txn completed');
                    },
                    // Process 2
                    async () => {
                        // Update the document when the txn read the document
                        await t.when('read version 0');
                        t.event('updating document');
                        await sharedRef.set(writeData({ version: FieldValue.increment(1) }), { merge: true });
                        t.event('document updated');
                    },
                );

                expect(await getData(sharedRef.get())).toEqual({ version: 1 });
                expect(await getData(writeRef.get())).toEqual({ version: 1 });

                expect(t.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: read version 0',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: read version 0',
                    ' <<1>> | WAITING UNTIL: document updated',
                    '       | <<2>> | EVENT: updating document',
                    '       | <<2>> | EVENT: document updated',
                    ' <<1>> | EVENT: creating document',
                    ' <<1>> | EVENT: txn attempt 2 started',
                    ' <<1>> | EVENT: read version 1',
                    ' <<1>> | EVENT: creating document',
                    ' <<1>> | EVENT: txn completed',
                ]);
            },
        );

        test.concurrent.each(['existing', 'non-existing'] as const)(
            'retry when writing to %s document that was changed by another process before txn ends',
            // The most common concurrency conflict: A transaction reads a document,
            // and then tries to update it. If another regular process updates that
            // same document in the meantime, the transaction notices the version
            // mismatch upon commit and retries.
            async which => {
                const t = new ConcurrentTest();
                const [ref] = refs();

                if (which === 'existing') await ref.set(writeData({ version: 0 }));

                await t.run(
                    // Process 1
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn attempt ${++attempt} started`);
                            const snap = await txn.get(ref);
                            const version = snap.get('version') ?? 0;
                            t.event(`read version ${version}`);

                            if (attempt === 1) await t.when('document updated');
                            txn.set(ref, { version: FieldValue.increment(1) }, { merge: true });
                            t.event('trying to update document');
                        });
                        t.event('txn completed');
                    },
                    // Process 2
                    async () => {
                        // Update the document when the txn read the document
                        await t.when('read version 0');
                        t.event('updating document');
                        await ref.set(writeData({ version: FieldValue.increment(1) }), { merge: true });
                        t.event('document updated');
                    },
                );

                expect(await getData(ref.get())).toEqual({ version: 2 });

                expect(t.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: read version 0',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: read version 0',
                    ' <<1>> | WAITING UNTIL: document updated',
                    '       | <<2>> | EVENT: updating document',
                    '       | <<2>> | EVENT: document updated',
                    ' <<1>> | EVENT: trying to update document',
                    ' <<1>> | EVENT: txn attempt 2 started',
                    ' <<1>> | EVENT: read version 1',
                    ' <<1>> | EVENT: trying to update document',
                    ' <<1>> | EVENT: txn completed',
                ]);
            },
        );

        test.concurrent.each(['existing', 'non-existing'] as const)(
            'retry when writing to %s document that was changed in another transaction concurrently',
            // Similar to the previous test, but tests the collision between TWO concurrent
            // transactions instead of a transaction and a raw write.
            // Whichever transaction commits first wins, and the other one must retry.
            async which => {
                const t = new ConcurrentTest();
                const [ref] = refs();

                if (which === 'existing') await ref.set(writeData({ version: 0 }));

                await t.run(
                    // Process 1
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn A attempt ${++attempt} started`);
                            const snap = await txn.get(ref);
                            const version = snap.get('version') ?? 0;
                            t.event(`txn A read version ${version}`);

                            if (attempt === 1) await t.when('txn B completed');
                            txn.set(ref, { version: FieldValue.increment(1) }, { merge: true });
                            t.event('trying to update document');
                        });
                        t.event('txn A completed');
                    },
                    // Process 2
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn B attempt ${++attempt} started`);

                            // Read and update the document concurrently while the other txn is waiting
                            await t.when('txn A read version 0');
                            const snap = await txn.get(ref);
                            const version = snap.get('version') ?? 0;
                            t.event(`txn B read version ${version}`);
                            txn.set(ref, writeData({ version: FieldValue.increment(1) }), { merge: true });
                            t.event('trying to update document');
                        });
                        t.event('txn B completed');
                    },
                );

                expect(await getData(ref.get())).toEqual({ version: 2 });

                expect(t.log).toEqual([
                    ' <<1>> | EVENT: txn A attempt 1 started',
                    '       | <<2>> | EVENT: txn B attempt 1 started',
                    '       | <<2>> | WAITING UNTIL: txn A read version 0',
                    ' <<1>> | EVENT: txn A read version 0',
                    ' <<1>> | WAITING UNTIL: txn B completed',
                    '       | <<2>> | EVENT: txn B read version 0',
                    '       | <<2>> | EVENT: trying to update document',
                    '       | <<2>> | EVENT: txn B completed',
                    ' <<1>> | EVENT: trying to update document',
                    ' <<1>> | EVENT: txn A attempt 2 started',
                    ' <<1>> | EVENT: txn A read version 1',
                    ' <<1>> | EVENT: trying to update document',
                    ' <<1>> | EVENT: txn A completed',
                ]);
            },
        );

        test.concurrent.each(['existing', 'non-existing'] as const)(
            'no retry when changing separate %s documents concurrently',
            // Two transactions that read and write entirely separate, non-overlapping
            // documents do not interfere with each other and can both commit successfully
            // without retrying.
            async which => {
                const t = new ConcurrentTest();
                const [ref1, ref2] = refs();

                if (which === 'existing') {
                    await ref1.set(writeData({ version: 0 }));
                    await ref2.set(writeData({ version: 0 }));
                }

                await t.run(
                    // Process 1
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn A attempt ${++attempt} started`);
                            const snap = await txn.get(ref1);
                            const version = snap.get('version') ?? 0;
                            t.event(`txn A read version ${version}`);

                            await t.when('txn B completed');
                            txn.set(ref1, writeData({ version: FieldValue.increment(1) }), { merge: true });
                            t.event('trying to update document');
                        });
                        t.event('txn A completed');
                    },
                    // Process 2
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn B attempt ${++attempt} started`);

                            await t.when('txn A read version 0');
                            const snap = await txn.get(ref2);
                            const version = snap.get('version') ?? 0;
                            t.event(`txn B read version ${version}`);
                            txn.set(ref2, writeData({ version: FieldValue.increment(1) }), { merge: true });
                            t.event('trying to update document');
                        });
                        t.event('txn B completed');
                    },
                );

                expect(await getData(ref1.get())).toEqual({ version: 1 });
                expect(await getData(ref2.get())).toEqual({ version: 1 });

                expect(t.log).toEqual([
                    ' <<1>> | EVENT: txn A attempt 1 started',
                    '       | <<2>> | EVENT: txn B attempt 1 started',
                    '       | <<2>> | WAITING UNTIL: txn A read version 0',
                    ' <<1>> | EVENT: txn A read version 0',
                    ' <<1>> | WAITING UNTIL: txn B completed',
                    '       | <<2>> | EVENT: txn B read version 0',
                    '       | <<2>> | EVENT: trying to update document',
                    '       | <<2>> | EVENT: txn B completed',
                    ' <<1>> | EVENT: trying to update document',
                    ' <<1>> | EVENT: txn A completed',
                ]);
            },
        );

        test.concurrent.each(['existing', 'non-existing'] as const)(
            'no retry if %s document was read in another transaction concurrently',
            // Multiple transactions can successfully read the exact same documents
            // concurrently without causing any retries. Reads do not block reads
            // and do not invalidate other reads.
            async which => {
                const t = new ConcurrentTest();
                const [ref] = refs();

                if (which === 'existing') await ref.set(writeData({ version: 0 }));

                await t.run(
                    // Process 1
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn A attempt ${++attempt} started`);
                            const snap = await txn.get(ref);
                            const version = snap.get('version') ?? 0;
                            t.event(`txn A read version ${version}`);

                            await t.when('txn B completed');
                            txn.set(ref, writeData({ version: FieldValue.increment(1) }), { merge: true });
                            t.event('trying to update document');
                        });
                        t.event('txn A completed');
                    },
                    // Process 2
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            t.event(`txn B attempt ${++attempt} started`);

                            // Only read the document
                            await t.when('txn A read version 0');
                            const snap = await txn.get(ref);
                            const version = snap.get('version') ?? 0;
                            t.event(`txn B read version ${version}`);
                        });
                        t.event('txn B completed');
                    },
                );

                expect(await getData(ref.get())).toEqual({ version: 1 });

                expect(t.log).toEqual([
                    ' <<1>> | EVENT: txn A attempt 1 started',
                    '       | <<2>> | EVENT: txn B attempt 1 started',
                    '       | <<2>> | WAITING UNTIL: txn A read version 0',
                    ' <<1>> | EVENT: txn A read version 0',
                    ' <<1>> | WAITING UNTIL: txn B completed',
                    '       | <<2>> | EVENT: txn B read version 0',
                    '       | <<2>> | EVENT: txn B completed',
                    ' <<1>> | EVENT: trying to update document',
                    ' <<1>> | EVENT: txn A completed',
                ]);
            },
        );

        test.concurrent('retry to prevent acting on an inconsistent state (race condition)', async () => {
            // When reading multiple documents in a transaction, the read state must be
            // consistent. If Process 2 updates both Doc A and Doc B simultaneously, but
            // the transaction reads Doc A before the update and Doc B after the update,
            // the transaction will have an inconsistent view. The backend handles this
            // by forcing a retry upon commit.
            const t = new ConcurrentTest();
            const [a, b, c] = refs();

            await a.create(writeData({ version: 0 }));
            await b.create(writeData({ version: 0 }));

            await t.run(
                // Process 1
                async () => {
                    let attempt = 0;
                    await fs.firestore.runTransaction(async txn => {
                        t.event(`txn A attempt ${++attempt} started`);
                        const snapA = await txn.get(a);
                        const versionA = snapA.get('version');
                        t.event(`doc A read, version ${versionA}`);

                        if (attempt === 1) await t.when('docs updated to version 1');

                        const snapB = await txn.get(b);
                        const versionB = snapB.get('version');
                        t.event(`doc B read, version ${versionB}`);

                        expect(snapA.readTime.valueOf()).toBe(snapB.readTime.valueOf());

                        txn.create(c, writeData({ versionA, versionB }));
                        t.event('trying to create derived document');
                    });
                    t.event('txn A completed');
                },
                // Process 2
                async () => {
                    await t.when('doc A read, version 0');
                    await fs.firestore
                        .batch()
                        .update(a, { version: FieldValue.increment(1) })
                        .update(b, { version: FieldValue.increment(1) })
                        .commit();
                    t.event('docs updated to version 1');
                },
            );

            expect(await getData(c.get())).toEqual({ versionA: 1, versionB: 1 });

            expect(t.log).toEqual([
                '       | <<2>> | WAITING UNTIL: doc A read, version 0',
                ' <<1>> | EVENT: txn A attempt 1 started',
                ' <<1>> | EVENT: doc A read, version 0',
                ' <<1>> | WAITING UNTIL: docs updated to version 1',
                '       | <<2>> | EVENT: docs updated to version 1',
                ' <<1>> | EVENT: doc B read, version 0',
                ' <<1>> | EVENT: trying to create derived document',
                ' <<1>> | EVENT: txn A attempt 2 started',
                ' <<1>> | EVENT: doc A read, version 1',
                ' <<1>> | EVENT: doc B read, version 1',
                ' <<1>> | EVENT: trying to create derived document',
                ' <<1>> | EVENT: txn A completed',
            ]);
        });

        test.concurrent('readTime is determined on the first `get` inside the transaction', async () => {
            // Any document read during an optimistic transaction is implicitly pinned to a
            // specific snapshot timeframe. This test ensures that all document reads within
            // the txn observe the database state exactly as it was during the FIRST read operation.
            const t = new ConcurrentTest();
            const [a, b] = refs();

            await a.create(writeData());
            await b.create(writeData());

            let firstInTxn: Date | undefined;
            let secondInTxn: Date | undefined;
            let firstOutside: Date | undefined;
            let secondOutside: Date | undefined;

            await t.run(
                // Process 1
                async () => {
                    await fs.firestore.runTransaction(async txn => {
                        t.event(`txn started`);

                        await t.when('process 2 read doc A');
                        await time(5);

                        firstInTxn = (await txn.get(a)).readTime.toDate();
                        t.event(`process 1 read doc A`);

                        await t.when('process 2 read doc B');
                        await time(5);

                        secondInTxn = (await txn.get(b)).readTime.toDate();
                        t.event(`process 1 read doc B`);
                    });
                },
                // Process 2
                async () => {
                    firstOutside = (await a.get()).readTime.toDate();
                    t.event('process 2 read doc A');

                    await t.when('process 1 read doc A');
                    await time(5);

                    // Using a txn to make sure we get a fresh read time
                    secondOutside = await fs.firestore.runTransaction(async txn => (await txn.get(b)).readTime.toDate());
                    t.event('process 2 read doc B');
                },
            );

            assert(firstInTxn && secondInTxn && firstOutside && secondOutside);
            // Read time of transaction is determined after the first document was read by process 2
            expect(firstInTxn).toBeAfter(firstOutside);
            // Both read in the txn have the same read time
            expect(firstInTxn).toEqual(secondInTxn);
            // The second document in process 2 has a read-time larger than the txn read time
            expect(secondOutside).toBeAfter(secondInTxn);

            expect(t.log).toEqual([
                ' <<1>> | EVENT: txn started',
                ' <<1>> | WAITING UNTIL: process 2 read doc A',
                '       | <<2>> | EVENT: process 2 read doc A',
                '       | <<2>> | WAITING UNTIL: process 1 read doc A',
                ' <<1>> | EVENT: process 1 read doc A',
                ' <<1>> | WAITING UNTIL: process 2 read doc B',
                '       | <<2>> | EVENT: process 2 read doc B',
                ' <<1>> | EVENT: process 1 read doc B',
            ]);
        });

        test.concurrent('no deadlock', async () => {
            // Pessimistic locking can suffer from deadlocks if two transactions acquire locks in
            // opposite orders (A then B vs B then A). Optimistic transactions do not acquire
            // locks, so they cannot deadlock. Instead, one overlapping transaction will just
            // be forced to retry.
            const t = new ConcurrentTest();

            const [ref1, ref2] = refs();

            await t.run(
                async () => {
                    let attempt = 0;
                    await fs.firestore.runTransaction(async txn => {
                        t.event(`txn A attempt ${++attempt} started`);

                        await txn.get(ref1);
                        t.event('in txn A: read doc 1');

                        if (attempt === 1) await t.when('in txn B: read doc 2');
                        await txn.get(ref2);
                        t.event('in txn A: read doc 2');

                        txn.set(ref1, writeData({ txnA: 'written' }));
                        txn.set(ref2, writeData({ txnA: 'written' }));
                    });
                },
                async () => {
                    let attempt = 0;
                    await fs.firestore.runTransaction(async txn => {
                        t.event(`txn B attempt ${++attempt} started`);

                        if (attempt === 1) await t.when('in txn A: read doc 1');
                        await txn.get(ref2);
                        t.event('in txn B: read doc 2');

                        if (attempt === 1) await t.when('in txn A: read doc 2');
                        await txn.get(ref1);
                        t.event('in txn B: read doc 1');

                        txn.set(ref1, writeData({ txnB: 'written' }));
                        txn.set(ref2, writeData({ txnB: 'written' }));
                    });
                },
            );

            expect(t.log).toEqual([
                ' <<1>> | EVENT: txn A attempt 1 started',
                '       | <<2>> | EVENT: txn B attempt 1 started',
                '       | <<2>> | WAITING UNTIL: in txn A: read doc 1',
                ' <<1>> | EVENT: in txn A: read doc 1',
                ' <<1>> | WAITING UNTIL: in txn B: read doc 2',
                '       | <<2>> | EVENT: in txn B: read doc 2',
                '       | <<2>> | WAITING UNTIL: in txn A: read doc 2',
                ' <<1>> | EVENT: in txn A: read doc 2',
                '       | <<2>> | EVENT: in txn B: read doc 1',
                '       | <<2>> | EVENT: txn B attempt 2 started',
                '       | <<2>> | EVENT: in txn B: read doc 2',
                '       | <<2>> | EVENT: in txn B: read doc 1',
            ]);
        });

        describe('queries', () => {
            async function setupQueryTest(testName: string) {
                const [docRef1, docRef2] = refs();

                const testData = writeData({ testName, initial: true });
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

            test.concurrent('retry when updating a document that was part of a query result', async () => {
                // When a transaction executes a query, the backend tracks the documents that matched.
                // If another process modifies one of those matched documents before the transaction
                // commits, the transaction's read assertions are invalidated, and it retries.
                const { docRef1, docRef2, queryDocsInTest } = await setupQueryTest('update');

                const test = new ConcurrentTest();

                await test.run(
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn attempt ${++attempt} started`);

                            const snaps = await queryDocsInTest(txn);
                            expect(snaps.size).toBe(2);
                            test.event(`txn read ${snaps.size} docs from query`);

                            if (attempt === 1) await test.when('document updated outside query');

                            txn.update(docRef1, { other: 'data' });
                            test.event('txn trying to commit');
                        });
                        test.event('txn completed');
                    },
                    async () => {
                        await test.when('txn read 2 docs from query');
                        await docRef2.update({ simple: 'update docRef2' });
                        test.event('document updated outside query');
                    },
                );

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: txn read 2 docs from query',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: txn read 2 docs from query',
                    ' <<1>> | WAITING UNTIL: document updated outside query',
                    '       | <<2>> | EVENT: document updated outside query',
                    ' <<1>> | EVENT: txn trying to commit',
                    ' <<1>> | EVENT: txn attempt 2 started',
                    ' <<1>> | EVENT: txn read 2 docs from query',
                    ' <<1>> | EVENT: txn trying to commit',
                    ' <<1>> | EVENT: txn completed',
                ]);
            });

            test.concurrent('retry when deleting a document that was part of a query result', async () => {
                // Similar to the update case, if a document that was part of a query result
                // is deleted by another process before the transaction commits, the transaction retries.
                const { docRef1, docRef2, queryDocsInTest } = await setupQueryTest('delete');

                const test = new ConcurrentTest();

                await test.run(
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn attempt ${++attempt} started`);

                            const snaps = await queryDocsInTest(txn);
                            expect(snaps.size).toBe(attempt === 1 ? 2 : 1);
                            test.event(`txn read ${snaps.size} docs from query`);

                            if (attempt === 1) await test.when('document deleted outside query');

                            txn.update(docRef1, { other: 'data' });
                            test.event('txn trying to commit');
                        });
                        test.event('txn completed');
                    },
                    async () => {
                        await test.when('txn read 2 docs from query');
                        await docRef2.delete();
                        test.event('document deleted outside query');
                    },
                );

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: txn read 2 docs from query',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: txn read 2 docs from query',
                    ' <<1>> | WAITING UNTIL: document deleted outside query',
                    '       | <<2>> | EVENT: document deleted outside query',
                    ' <<1>> | EVENT: txn trying to commit',
                    ' <<1>> | EVENT: txn attempt 2 started',
                    ' <<1>> | EVENT: txn read 1 docs from query',
                    ' <<1>> | EVENT: txn trying to commit',
                    ' <<1>> | EVENT: txn completed',
                ]);
            });

            test.concurrent('retry when conditional write based on query has concurrent read collision', async () => {
                // Tests a collision where Txn 1 reads a document via a query and conditionally writes
                // based on that result. Concurrently, Txn 2 directly modifies that exact same document.
                // Because the document read by Txn 1's query was modified, Txn 1 retries. On its second
                // attempt, it observes the updated state (the document no longer matches the query).
                const { docRef1, docRef2, testName, queryDocsInTest } = await setupQueryTest('multi');

                const test = new ConcurrentTest();

                await test.run(
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn 1 attempt ${++attempt} started`);

                            if (attempt === 1) await test.when('txn 2 read own document');
                            const snaps = await queryDocsInTest(txn, { exclude: docRef1 });
                            if (snaps.size) {
                                test.event('txn 1 found contested document');
                                if (attempt === 1) await test.when('txn 2 updated contested document');
                                txn.update(docRef1, { testName: 'other name' });
                            } else {
                                test.event('txn 1 did not find any document');
                            }
                        });
                        test.event('txn 1 completed');
                    },
                    async () => {
                        // Txn 2 reads the contested document without using a query.
                        // In optimistic concurrency, the timing of who commits first determines who wins.
                        // We will ensure txn 2 commits before txn 1.
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn 2 attempt ${++attempt} started`);
                            await txn.get(docRef2);
                            test.event('txn 2 read own document');

                            await test.when('txn 1 found contested document');
                            txn.update(docRef2, { testName: 'some other name' });
                            test.event('txn 2 trying to commit');
                        });
                        test.event('txn 2 updated contested document');
                    },
                );

                expect(await readDataRef(docRef1)).toEqual(expect.objectContaining({ testName }));
                expect(await readDataRef(docRef2)).toEqual(expect.objectContaining({ testName: 'some other name' }));

                expect(test.log).toEqual([
                    ' <<1>> | EVENT: txn 1 attempt 1 started',
                    ' <<1>> | WAITING UNTIL: txn 2 read own document',
                    '       | <<2>> | EVENT: txn 2 attempt 1 started',
                    '       | <<2>> | EVENT: txn 2 read own document',
                    '       | <<2>> | WAITING UNTIL: txn 1 found contested document',
                    ' <<1>> | EVENT: txn 1 found contested document',
                    ' <<1>> | WAITING UNTIL: txn 2 updated contested document',
                    '       | <<2>> | EVENT: txn 2 trying to commit',
                    '       | <<2>> | EVENT: txn 2 updated contested document',
                    ' <<1>> | EVENT: txn 1 attempt 2 started',
                    ' <<1>> | EVENT: txn 1 did not find any document',
                    ' <<1>> | EVENT: txn 1 completed',
                ]);
            });

            test.concurrent('retry when creating a document matching a previously read query bounds', async () => {
                // When a query is executed in a transaction, the results establish bounds.
                // If another process concurrently creates a new document that matches the query
                // criteria, the read constraint is violated, and the transaction must retry.
                const testName = 'create';
                const [docRef1, docRef2] = refs();

                await docRef1.create(writeData({ testName }));

                const query = fs.collection.where('testName', '==', testName);

                const test = new ConcurrentTest();

                await test.run(
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn attempt ${++attempt} started`);

                            const snaps = await txn.get(query);
                            expect(snaps.size).toBe(attempt === 1 ? 1 : 2);
                            test.event(`txn read ${snaps.size} docs from query`);

                            if (attempt === 1) await test.when('docRef2 created outside the query');

                            txn.update(docRef1, { other: 'data' });
                        });
                        test.event('txn completed');
                    },
                    async () => {
                        await test.when('txn read 1 docs from query');
                        await docRef2.create(writeData({ testName }));
                        test.event('docRef2 created outside the query');
                    },
                );

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: txn read 1 docs from query',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: txn read 1 docs from query',
                    ' <<1>> | WAITING UNTIL: docRef2 created outside the query',
                    '       | <<2>> | EVENT: docRef2 created outside the query',
                    ' <<1>> | EVENT: txn attempt 2 started',
                    ' <<1>> | EVENT: txn read 2 docs from query',
                    ' <<1>> | EVENT: txn completed',
                ]);
            });

            test.concurrent('readTime is determined on the first query inside the transaction', async () => {
                // Just like regular document reads, executing a query implicitly pins the transaction
                // to a specific snapshot timeframe. This test ensures that the transaction's read-time
                // is permanently established as soon as the first query is executed.
                const test = new ConcurrentTest();
                const { docRef1, docRef2, queryDocsInTest } = await setupQueryTest('read-time-query');

                let firstInTxn: Date | undefined;
                let secondInTxn: Date | undefined;
                let firstOutside: Date | undefined;
                let secondOutside: Date | undefined;

                await test.run(
                    // Process 1
                    async () => {
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn started`);

                            await test.when('process 2 read query');
                            await time(5);

                            const snaps = await queryDocsInTest(txn, { exclude: docRef1 });
                            firstInTxn = snaps.readTime.toDate();
                            test.event(`process 1 read query`);

                            await test.when('process 2 read docRef2');
                            await time(5);

                            secondInTxn = (await txn.get(docRef2)).readTime.toDate();
                            test.event(`process 1 read docRef2`);
                        });
                    },
                    // Process 2
                    async () => {
                        const baseQuery = fs.collection
                            .where('testName', '==', 'read-time-query')
                            .where(FieldPath.documentId(), '!=', docRef1.id);
                        firstOutside = (await baseQuery.get()).readTime.toDate();
                        test.event('process 2 read query');

                        await test.when('process 1 read query');
                        await time(5);

                        // Using a txn to make sure we get a fresh read time
                        secondOutside = await fs.firestore.runTransaction(async txn => (await txn.get(docRef2)).readTime.toDate());
                        test.event('process 2 read docRef2');
                    },
                );

                assert(firstInTxn && secondInTxn && firstOutside && secondOutside);
                // Read time of transaction is determined after the first query was executed by process 2
                expect(firstInTxn).toBeAfter(firstOutside);
                // Both read in the txn have the same read time
                expect(firstInTxn).toEqual(secondInTxn);
                // The second document in process 2 has a read-time larger than the txn read time
                expect(secondOutside).toBeAfter(secondInTxn);

                expect(test.log).toEqual([
                    ' <<1>> | EVENT: txn started',
                    ' <<1>> | WAITING UNTIL: process 2 read query',
                    '       | <<2>> | EVENT: process 2 read query',
                    '       | <<2>> | WAITING UNTIL: process 1 read query',
                    ' <<1>> | EVENT: process 1 read query',
                    ' <<1>> | WAITING UNTIL: process 2 read docRef2',
                    '       | <<2>> | EVENT: process 2 read docRef2',
                    ' <<1>> | EVENT: process 1 read docRef2',
                ]);
            });

            test.concurrent('subsequent queries adhere to the read time determined by the first get', async () => {
                // Once a transaction's read-time is determined by an initial read operation (like a `get()`),
                // any subsequent queries within the same transaction must execute at that exact
                // same read-time. They will not observe any changes that occurred after the transaction started.
                const test = new ConcurrentTest();
                const { docRef1, queryDocsInTest } = await setupQueryTest('read-time-subsequent-query');

                let firstInTxn: Date | undefined;
                let secondInTxn: Date | undefined;
                let firstOutside: Date | undefined;
                let secondOutside: Date | undefined;

                await test.run(
                    // Process 1
                    async () => {
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn started`);

                            await test.when('process 2 read docRef1');
                            await time(5);

                            firstInTxn = (await txn.get(docRef1)).readTime.toDate();
                            test.event(`process 1 read docRef1`);

                            await test.when('process 2 read query');
                            await time(5);

                            const snaps = await queryDocsInTest(txn, { exclude: docRef1 });
                            secondInTxn = snaps.readTime.toDate();
                            test.event(`process 1 read query`);
                        });
                    },
                    // Process 2
                    async () => {
                        firstOutside = (await docRef1.get()).readTime.toDate();
                        test.event('process 2 read docRef1');

                        await test.when('process 1 read docRef1');
                        await time(5);

                        const baseQuery = fs.collection
                            .where('testName', '==', 'read-time-subsequent-query')
                            .where(FieldPath.documentId(), '!=', docRef1.id);
                        secondOutside = await fs.firestore.runTransaction(async txn => (await txn.get(baseQuery)).readTime.toDate());
                        test.event('process 2 read query');
                    },
                );

                assert(firstInTxn && secondInTxn && firstOutside && secondOutside);
                // Read time of transaction is determined after the first document was read by process 2
                expect(firstInTxn).toBeAfter(firstOutside);
                // Both read in the txn have the same read time
                expect(firstInTxn).toEqual(secondInTxn);
                // The second document in process 2 has a read-time larger than the txn read time
                expect(secondOutside).toBeAfter(secondInTxn);

                expect(test.log).toEqual([
                    ' <<1>> | EVENT: txn started',
                    ' <<1>> | WAITING UNTIL: process 2 read docRef1',
                    '       | <<2>> | EVENT: process 2 read docRef1',
                    '       | <<2>> | WAITING UNTIL: process 1 read docRef1',
                    ' <<1>> | EVENT: process 1 read docRef1',
                    ' <<1>> | WAITING UNTIL: process 2 read query',
                    '       | <<2>> | EVENT: process 2 read query',
                    ' <<1>> | EVENT: process 1 read query',
                ]);
            });

            test.concurrent('subsequent queries do not see concurrent modifications until retry', async () => {
                // Documents created or modified after the transaction's read-time was established
                // will not appear in subsequent query results during that same attempt.
                // However, because the concurrent change altered the query results that should
                // have been matched, the transaction will retry upon commit. On the next attempt,
                // it receives a new read-time and successfully reads the updated state containing the new document.
                const test = new ConcurrentTest();
                const testName = 'read-time-query-results';
                const [docRef1, docRef2] = refs();

                await docRef1.create(writeData({ testName: 'other' }));

                const query = fs.collection.where('testName', '==', testName);

                await test.run(
                    // Process 1
                    async () => {
                        let attempt = 0;
                        await fs.firestore.runTransaction(async txn => {
                            test.event(`txn attempt ${++attempt} started`);

                            // 1. First get() establishes the read-time
                            await txn.get(docRef1);
                            test.event('txn read docRef1');

                            // Wait for process 2 to create a document matching our upcoming query
                            if (attempt === 1) await test.when('docRef2 created');

                            // 2. Execute query. Because read-time is before docRef2 was created,
                            // we should NOT see docRef2 in attempt 1.
                            // In attempt 2, the read-time is newer, so we SHOULD see it.
                            const snaps = await txn.get(query);
                            expect(snaps.size).toBe(attempt === 1 ? 0 : 1);
                            test.event(`txn read ${snaps.size} docs from query`);

                            // Write something to ensure the transaction checks for conflicts and retries
                            txn.update(docRef1, { updated: true });
                        });
                        test.event('txn completed');
                    },
                    // Process 2
                    async () => {
                        await test.when('txn read docRef1');
                        await docRef2.create(writeData({ testName }));
                        test.event('docRef2 created');
                    },
                );

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: txn read docRef1',
                    ' <<1>> | EVENT: txn attempt 1 started',
                    ' <<1>> | EVENT: txn read docRef1',
                    ' <<1>> | WAITING UNTIL: docRef2 created',
                    '       | <<2>> | EVENT: docRef2 created',
                    ' <<1>> | EVENT: txn read 0 docs from query',
                    ' <<1>> | EVENT: txn attempt 2 started',
                    ' <<1>> | EVENT: txn read docRef1',
                    ' <<1>> | EVENT: txn read 1 docs from query',
                    ' <<1>> | EVENT: txn completed',
                ]);
            });
        });
    });

    function refs() {
        return [fs.collection.doc(), fs.collection.doc(), fs.collection.doc()] as const;
    }
});

async function getData(promisedSnap: DocumentSnapshot | Promise<DocumentSnapshot>) {
    const snap = await promisedSnap;
    expect(snap.exists).toBeTrue();
    return readData(snap.data());
}
