import { MaybeFinalState, atom, error, final } from '@skunkteam/sherlock';
import { fromPromise } from '@skunkteam/sherlock-utils';
import assert from 'assert';
import { AsyncLocalStorage } from 'async_hooks';
import { setTimeout as time } from 'timers/promises';
import { fs } from './utils';
import { readDataRef } from './utils/firestore';

describe('concurrent tests', () => {
    // no concurrent tests with the Java Emulator..
    const concurrent = fs.connection === 'JAVA EMULATOR' ? test : test.concurrent;

    concurrent('simple txn', async () => {
        const [docRef1] = refs();

        await fs.firestore.runTransaction(async txn => {
            expect(await txn.get(docRef1)).toHaveProperty('exists', false);

            txn.set(docRef1, fs.writeData({ foo: 'bar' }));

            expect(() => txn.get(docRef1)).toThrow('Firestore transactions require all reads to be executed before all writes.');
        });
        expect(await getData(docRef1.get())).toEqual({ foo: 'bar' });
    });

    concurrent('updating same doc multiple times', async () => {
        const [docRef1] = refs();

        await fs.firestore.runTransaction(async txn => {
            expect(await txn.get(docRef1)).toHaveProperty('exists', false);

            txn.set(docRef1, fs.writeData({ foo: fs.exported.FieldValue.increment(1) }));
            txn.update(docRef1, { foo: fs.exported.FieldValue.increment(1) });
        });
        expect(await getData(docRef1.get())).toEqual({ foo: 2 });
    });

    concurrent('using txn.getAll', async () => {
        const [docRef1, docRef2] = refs();

        await docRef1.set(fs.writeData({ some: 'data' }));

        await fs.firestore.runTransaction(async txn => {
            const [snap1, snap2] = await txn.getAll(docRef1, docRef2);

            expect(await getData(snap1)).toEqual({ some: 'data' });
            expect(snap2.exists).toBeFalse();

            txn.set(docRef2, fs.writeData({ foo: 'bar' }));
        });
        expect(await getData(docRef1.get())).toEqual({ some: 'data' });
        expect(await getData(docRef2.get())).toEqual({ foo: 'bar' });
    });

    concurrent('aborting transaction', async () => {
        const [docRef1] = refs();

        await expect(
            fs.firestore.runTransaction(async txn => {
                expect(await txn.get(docRef1)).toHaveProperty('exists', false);

                txn.set(docRef1, fs.writeData({ foo: 'bar' }));

                throw new Error('I quit!');
            }),
        ).rejects.toThrow('I quit!');

        expect(await docRef1.get()).toHaveProperty('exists', false);
    });

    describe('locks', () => {
        // In Java emulator, either leads to:
        // - 10 ABORTED: Transaction lock timeout
        // - inconsistent number of `tries`
        fs.notImplementedInJava ||
            concurrent('retry if document is locked', async () => {
                const [docRef1] = refs();

                await docRef1.set(fs.writeData({ some: 'data' }));

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

        fs.notImplementedInJava ||
            concurrent('lock on non-existing document', async () => {
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

        concurrent('no lock if getting separate documents', async () => {
            const [docRef1, docRef2] = refs();

            await runTxn('outer', [docRef1], async () => {
                const { innerTxnCompleted } = await innerTxn('inner', [docRef2]);
                return { awaitAfterTxn: innerTxnCompleted };
            });
            expect(await getData(docRef1.get())).toEqual({ outer: { tries: 1 } });
            expect(await getData(docRef2.get())).toEqual({ inner: { tries: 1 } });
        });

        // Note: Very slow on Cloud Firestore!!
        concurrent(
            'chaos',
            async () => {
                const [docRef1, docRef2] = refs();

                await runTxn('outer', [docRef1], async () => {
                    // Will need a retry because of `docRef1`,
                    const { innerTxnCompleted: first } = await innerTxn('innerFirst', [docRef2, docRef1]);
                    // Somehow only needs one try, even though `docRef2` could have been locked by 'innerFirst'
                    const { innerTxnCompleted: second } = await innerTxn('innerSecond', [docRef2]);

                    return { awaitAfterTxn: Promise.all([first, second]) };
                });
                expect(await getData(docRef1.get())).toEqual({
                    outer: { tries: 1 },
                    innerFirst: { tries: expect.toBeOneOf([2, 3, 4]) },
                });
                expect(await getData(docRef2.get())).toEqual({
                    innerFirst: { tries: expect.toBeOneOf([2, 3, 4]) },
                    innerSecond: { tries: 1 },
                });
            },
            45_000,
        );

        concurrent('only read locked document', async () => {
            const [docRef1, docRef2] = refs();

            await docRef1.create(fs.writeData());

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

        concurrent('regular `set` waits on transaction', async () => {
            const [docRef1] = refs();

            await docRef1.set(fs.writeData({ some: 'data' }));

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
            concurrent('reading the same doc from different txns', async () => {
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
                        await ref.create(fs.writeData({ value: 'original value' }));
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

            concurrent('reading the same doc from different txns, try to write in second txn', async () => {
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
                        await ref.create(fs.writeData({ value: 'original value' }));
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
                        // Make sure this logging doesn't sneak ahead of the logging in txn A. This little timeout has no effect
                        // on the validity of the test, because of the 250ms wait time inside txn A. If we got here during that
                        // timeout, then this timeout of 1ms would not change the outcome of this test.
                        await time(1);
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
                    ' <<1>> | EVENT: waited 250ms, txn B still pending',
                    ' <<1>> | EVENT: txn A ended',
                    ' <<1>> | EVENT: read: original value',
                    '       | <<2>> | EVENT: txn B ended',
                    '       | <<2>> | EVENT: read: original value',
                ]);
            });

            concurrent('reading the same doc from different txns, try to write in both txns', async () => {
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
                        await ref.create(fs.writeData({ value: 'original value' }));
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

            concurrent('regular writes also wait until all txns-locks are released', async () => {
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
                        await ref1.create(fs.writeData({ log: ['created outside txn'] }));
                        test.event('create outside txn succeeded');

                        await test.when('in txn B: read doc 1');
                        const firstUpdate = ref1
                            .update({ log: fs.exported.FieldValue.arrayUnion('updated outside txn once') })
                            .then(() => test.event('finished first update outside txn'));

                        await time(0); // Make sure the update is sent

                        const secondUpdate = ref1
                            .update({ log: fs.exported.FieldValue.arrayUnion('updated outside txn again') })
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
                            txn.update(ref1, { log: fs.exported.FieldValue.arrayUnion('updated inside txn A') });
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
                            txn.create(ref2, fs.writeData({ from: 'txn B' }));
                        });
                        test.event('end txn B');
                    },
                );

                expect(test.log.slice(0, -2)).toEqual([
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
                    '       | <<2>> | EVENT: end txn A',
                ]);
                // Regular updates suffer from contention (in the current version of the SDK), so
                // they need to be retried and then the order cannot be guaranteed anymore.
                // These last two might come back in different order...
                expect(test.log.slice(-2)).toIncludeSameMembers([
                    ' <<1>> | EVENT: finished first update outside txn',
                    ' <<1>> | EVENT: finished second update outside txn',
                ]);
                const { log } = await getData(ref1.get());
                assert(Array.isArray(log));
                expect(log.slice(0, -2)).toEqual(['created outside txn', 'updated inside txn A']);
                // Even though the writes were done before the transaction finished, they were written afterwards
                expect(log.slice(-2)).toIncludeSameMembers(['updated outside txn once', 'updated outside txn again']);
            });

            concurrent('write using a batch write to a document that is locked by a transaction', async () => {
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
                        await ref.create(fs.writeData({ value: 'original value' }));
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

                expect(test.log).toEqual([
                    '       | <<2>> | WAITING UNTIL: txn A got lock on doc',
                    ' <<1>> | EVENT: txn A got lock on doc',
                    ' <<1>> | WAITING UNTIL: batch write waiting on lock',
                    '       | <<2>> | EVENT: batch write waiting on lock',
                    ' <<1>> | EVENT: transaction completed',
                    '       | <<2>> | EVENT: batch write finished',
                ]);

                const finalSnap = await ref.get();
                expect(fs.readData(finalSnap.data())).toEqual({
                    value: 'changed in batch',
                });
            });

            fs.notImplementedInJava ||
                concurrent('read waiting on write-lock that is waiting on a read-lock', async () => {
                    const [docRef] = refs();
                    await docRef.create(fs.writeData({ some: 'data' }));

                    const test = new ConcurrentTest();
                    await test.run(
                        // Transaction 1 reads the document, but keeps the
                        // transaction alive until the very end.
                        async () => {
                            await fs.firestore.runTransaction(async txn => {
                                test.event('txn 1: started');
                                const snap = await txn.get(docRef);
                                expect(fs.readData(snap.data())).toEqual({ some: 'data' });
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
                                expect(fs.readData(snap.data())).toEqual({ some: 'data', other: 'data' });
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

            fs.notImplementedInRust ||
                fs.notImplementedInJava ||
                fs.notImplementedInCloud ||
                concurrent('deadlock', async () => {
                    const test = new ConcurrentTest(40_000);

                    const [ref1, ref2] = refs();

                    await test.run(
                        async () => {
                            await fs.firestore.runTransaction(
                                async txn => {
                                    await txn.get(ref1);
                                    test.event('in txn A: read doc 1');

                                    await test.when('in txn B: read doc 2');
                                    await txn.get(ref2);
                                    test.event('in txn A: read doc 2');

                                    txn.set(ref1, fs.writeData({ txnA: 'written' }));
                                    txn.set(ref2, fs.writeData({ txnA: 'written' }));
                                },
                                { maxAttempts: 1 },
                            );
                            test.event('end txn A');
                        },
                        async () => {
                            await fs.firestore.runTransaction(
                                async txn => {
                                    await txn.get(ref2);
                                    test.event('in txn B: read doc 2');

                                    await test.when('in txn B: read doc 1');
                                    await txn.get(ref1);
                                    test.event('in txn A: read doc 1');

                                    txn.set(ref1, fs.writeData({ txnB: 'written' }));
                                    txn.set(ref2, fs.writeData({ txnB: 'written' }));
                                },
                                { maxAttempts: 1 },
                            );
                            test.event('end txn B');
                        },
                    );

                    // Aangezien geen van de tests dit lukt, weten we niet wat we hier moeten verwachten.
                    expect(test.log).toEqual([]);
                });

            describe('queries', () => {
                async function setupQueryTest(testName: string) {
                    const [docRef1, docRef2] = refs();

                    const testData = fs.writeData({ testName });
                    await fs.firestore.batch().create(docRef1, testData).create(docRef2, testData).commit();

                    const baseQuery = fs.collection.where('testName', '==', testName);

                    return { docRef1, docRef2, testName, queryDocsInTest };

                    async function queryDocsInTest(
                        txn: FirebaseFirestore.Transaction,
                        opts: { exclude?: FirebaseFirestore.DocumentReference } = {},
                    ) {
                        const excludeId = opts.exclude?.id;
                        const query = excludeId ? baseQuery.where(fs.exported.FieldPath.documentId(), '!=', excludeId) : baseQuery;

                        const snaps = await txn.get(query);
                        // Sanity check
                        if (excludeId) expect(snaps.docs).not.toPartiallyContain({ id: excludeId });
                        return snaps;
                    }
                }

                concurrent('query waiting on write-lock that is waiting on a read-lock', async () => {
                    const [docRef] = refs();
                    await docRef.create(fs.writeData({ some: 'data' }));

                    const test = new ConcurrentTest();
                    await test.run(
                        // Transaction 1 reads the document, but keeps the
                        // transaction alive until the very end.
                        async () => {
                            await fs.firestore.runTransaction(async txn => {
                                test.event('txn 1: started');
                                const snap = await txn.get(docRef);
                                expect(fs.readData(snap.data())).toEqual({ some: 'data' });
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
                                const snap = await txn.get(fs.collection.where(fs.exported.FieldPath.documentId(), '==', docRef.id));
                                expect(snap.docs).toHaveLength(1);
                                expect(fs.readData(snap.docs[0].data())).toEqual({ some: 'data', other: 'data' });
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

                concurrent('update after reading a query', async () => {
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

                    expect(test.log).toEqual([
                        '       | <<2>> | WAITING UNTIL: transaction locked the query',
                        ' <<1>> | EVENT: transaction started',
                        ' <<1>> | EVENT: transaction locked the query',
                        ' <<1>> | EVENT: transaction completed',
                        '       | <<2>> | EVENT: both docs updated outside the query',
                    ]);
                });

                concurrent('delete after reading a query', async () => {
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

                    expect(test.log).toEqual([
                        '       | <<2>> | WAITING UNTIL: transaction locked the query',
                        ' <<1>> | EVENT: transaction started',
                        ' <<1>> | EVENT: transaction locked the query',
                        ' <<1>> | EVENT: transaction completed',
                        '       | <<2>> | EVENT: docRef1 deleted outside the query',
                    ]);
                });

                fs.notImplementedInJava ||
                    fs.notImplementedInRust || // Does not yet have the correct handling of locks in this case.
                    concurrent('conditional write based on query (contested doc is read first in the secondary transaction)', async () => {
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
                    });

                concurrent('conditional write based on query (contested doc is read first in the query)', async () => {
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
                        ' <<1>> | EVENT: txn 1: completed',
                        '       | <<2>> | EVENT: txn 2: completed',
                    ]);
                });

                fs.notImplementedInJava || // Timeout
                    concurrent('create after reading a query', async () => {
                        const testName = 'create';

                        const [docRef1, docRef2] = refs();

                        await docRef1.create(fs.writeData({ testName }));

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
                                await docRef2.create({ first: 'data' });
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

            class ConcurrentTest {
                readonly log: string[] = [];
                readonly lastEvent$ = atom.unresolved<string>();
                readonly processName = new AsyncLocalStorage<string>();

                constructor(private readonly timeout = 10_000) {}

                event(what: string) {
                    this.log.push(`${this.processName.getStore()} EVENT: ${what}`);
                    this.lastEvent$.set(what);
                }

                async when(what: string) {
                    this.log.push(`${this.processName.getStore()} WAITING UNTIL: ${what}`);
                    const errorAfterTime$ = fromPromise(time(this.timeout)).map((): MaybeFinalState<never> => {
                        const currentLog = this.log.map(m => '\n- ' + m).join('');
                        const timeoutSeconds = this.timeout / 1_000;
                        const msg = `${this.processName.getStore()} timeout after ${timeoutSeconds}s, current log: ${currentLog}`;
                        return final(error(msg));
                    });
                    await this.lastEvent$.toPromise({ when: d$ => d$.is(what).or(errorAfterTime$) });
                }

                async run(...processes: Array<() => Promise<unknown>>) {
                    await Promise.all(processes.map(async (p, i) => this.processName.run(`${'       |'.repeat(i)} <<${i + 1}>> |`, p)));
                }
            }
        });

        type UsedRefs =
            | FirebaseFirestore.DocumentReference[]
            | { read: FirebaseFirestore.DocumentReference[]; write: FirebaseFirestore.DocumentReference[] };
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
                    txn.set(ref, fs.writeData({ [name]: { tries } }), { merge: true });
                }
            });
            await awaitAfterTxn;
        }

        async function innerTxn(name: string, refs: UsedRefs) {
            const { resolver, waitForIt } = createResolver();
            const innerTxnCompleted = runTxn(name, refs, resolver);
            await waitForIt;
            return { innerTxnCompleted };
        }
        function createResolver() {
            let resolver: undefined | (() => void);
            const waitForIt = new Promise<void>(res => (resolver = res));
            assert(resolver);
            return { resolver, waitForIt };
        }
    });
});

function refs() {
    return [fs.collection.doc(), fs.collection.doc()] as const;
}

async function getData(promisedSnap: FirebaseFirestore.DocumentSnapshot | Promise<FirebaseFirestore.DocumentSnapshot>) {
    const snap = await promisedSnap;
    expect(snap.exists).toBeTrue();
    return fs.readData(snap.data());
}
