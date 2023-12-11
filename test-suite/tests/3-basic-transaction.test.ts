import { MaybeFinalState, atom, error, final } from '@skunkteam/sherlock';
import { fromPromise } from '@skunkteam/sherlock-utils';
import assert from 'assert';
import { AsyncLocalStorage } from 'async_hooks';
import { range } from 'lodash';
import { setTimeout as time } from 'timers/promises';
import { fs } from './utils';
import { writeData } from './utils/firestore';

describe('concurrent tests', () => {
    test.concurrent('simple txn', async () => {
        const [docRef1] = refs();

        await fs.firestore.runTransaction(async txn => {
            expect(await txn.get(docRef1)).toHaveProperty('exists', false);

            txn.set(docRef1, writeData({ foo: 'bar' }));

            expect(() => txn.get(docRef1)).toThrow('Firestore transactions require all reads to be executed before all writes.');
        });
        expect(await getData(docRef1.get())).toEqual({ foo: 'bar' });
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
        fs.notImplementedInJava ||
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
                    inner: { tries: 2 },
                });
            });

        fs.notImplementedInJava ||
            test.concurrent('lock on non-existing document', async () => {
                const [docRef1] = refs();

                await runTxn('outer', [docRef1], async () => {
                    const { innerTxnCompleted } = await innerTxn('inner', [docRef1]);

                    return { awaitAfterTxn: innerTxnCompleted };
                });

                expect(await getData(docRef1.get())).toEqual({
                    outer: { tries: 1 },
                    inner: { tries: 2 },
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
        fs.notImplementedInRust ||
            test.concurrent(
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
                        innerFirst: { tries: 2 },
                    });
                    expect(await getData(docRef2.get())).toEqual({
                        innerFirst: { tries: 2 },
                        innerSecond: { tries: 1 },
                    });
                },
                45_000,
            );

        test.concurrent('only read locked document', async () => {
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
                inner: { tries: 1 },
            });
        });

        test.concurrent('regular `set` waits on transaction', async () => {
            const [docRef1] = refs();

            await docRef1.set(writeData({ some: 'data' }));

            let setDone = false;
            await runTxn('outer', [docRef1], async () => {
                const updateDone = docRef1.update({ extraProp: 'foo' }).then(() => (setDone = true));
                await time(500);
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
                        await ref.create(fs.writeData({ value: 'original value' }));
                        test.event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn A: doc read');

                            await test.when('in txn B: update requested in txn');
                            await time(500);
                            expect(test.lastEvent$.get()).toBe('in txn B: update requested in txn');
                            test.event('waited 500ms, txn B still pending');
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
                    ' <<1>> | EVENT: waited 500ms, txn B still pending',
                    ' <<1>> | EVENT: txn A ended',
                    ' <<1>> | EVENT: read: original value',
                    '       | <<2>> | EVENT: txn B ended',
                    '       | <<2>> | EVENT: read: original value',
                ]);
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
                        await ref.create(fs.writeData({ value: 'original value' }));
                        test.event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            test.event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            test.event('in txn A: doc read');

                            await test.when('in txn B: update requested in txn');
                            await time(500);
                            expect(test.lastEvent$.get()).toBe('in txn B: update requested in txn');
                            test.event('waited 500ms, txn B still pending');
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
                    ' <<1>> | EVENT: waited 500ms, txn B still pending',
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
                        await ref1.create(fs.writeData({ log: ['created outside txn'] }));
                        test.event('create outside txn succeeded');

                        await test.when('in txn B: read doc 1');
                        const promise = ref1.update({ log: fs.exported.FieldValue.arrayUnion('updated outside txn in process 1') });
                        test.event('started update outside of txn in process 1');

                        await promise;
                        expect(test.lastEvent$.get()).toBe('end txn A');
                        test.event('finished update outside of txn in process 1');
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

                            await test.when('started update outside of txn in process 4');
                            txn.create(ref2, fs.writeData({ from: 'txn B' }));
                        });
                        test.event('end txn B');
                    },
                    // Process 4
                    async () => {
                        await test.when('started update outside of txn in process 1');
                        await time(15);
                        const promise = ref1.update({ log: fs.exported.FieldValue.arrayUnion('updated outside txn in process 4') });
                        test.event('started update outside of txn in process 4');

                        await promise;
                        expect(test.lastEvent$.get()).toBe('finished update outside of txn in process 1');
                        test.event('finished update outside of txn in process 4');
                    },
                );

                expect(test.log).toEqual([
                    //<<1>> | <<2>> | <<3>> | <<4>> |
                    '       | <<2>> | WAITING UNTIL: create outside txn succeeded',
                    '       |       | <<3>> | WAITING UNTIL: in txn A: read doc 1',
                    '       |       |       | <<4>> | WAITING UNTIL: started update outside of txn in process 1',
                    ' <<1>> | EVENT: create outside txn succeeded',
                    ' <<1>> | WAITING UNTIL: in txn B: read doc 1',
                    '       | <<2>> | EVENT: in txn A: read doc 1',
                    '       | <<2>> | WAITING UNTIL: end txn B',
                    '       |       | <<3>> | EVENT: in txn B: read doc 1',
                    '       |       | <<3>> | WAITING UNTIL: started update outside of txn in process 4',
                    ' <<1>> | EVENT: started update outside of txn in process 1',
                    '       |       |       | <<4>> | EVENT: started update outside of txn in process 4',
                    '       |       | <<3>> | EVENT: end txn B',
                    '       | <<2>> | EVENT: end txn A',
                    ' <<1>> | EVENT: finished update outside of txn in process 1',
                    '       |       |       | <<4>> | EVENT: finished update outside of txn in process 4',
                ]);

                expect(fs.readData((await ref1.get()).data())).toEqual({
                    log: [
                        'created outside txn',
                        'updated inside txn A',
                        'updated outside txn in process 1',
                        'updated outside txn in process 4',
                    ],
                });
            });

            fs.notImplementedInRust ||
                fs.notImplementedInJava ||
                fs.notImplementedInCloud ||
                test.concurrent('deadlock', async () => {
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
                test.concurrent('update after reading a query', async () => {
                    const testName = 'update';

                    const [docRef1, docRef2] = refs();

                    await docRef1.create(fs.writeData({ testName }));
                    await docRef2.create(fs.writeData({ testName }));

                    const query = fs.collection.where('testName', '==', testName);

                    const test = new ConcurrentTest();

                    await test.run(
                        async () => {
                            await fs.firestore.runTransaction(async txn => {
                                test.event('transaction started');

                                const snaps = await txn.get(query);
                                expect(snaps.size).toBe(2);
                                test.event('transaction locked the query');
                                await time(500);
                                expect(test.lastEvent$.get()).toBe('transaction locked the query');

                                txn.update(docRef1, { other: 'data' });
                            });
                            test.event('transaction completed');
                        },
                        async () => {
                            await test.when('transaction locked the query');
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

                test.concurrent('delete after reading a query', async () => {
                    const testName = 'delete';

                    const [docRef1, docRef2] = refs();

                    await docRef1.create(fs.writeData({ testName }));
                    await docRef2.create(fs.writeData({ testName }));

                    const query = fs.collection.where('testName', '==', testName);

                    const test = new ConcurrentTest();

                    await test.run(
                        async () => {
                            await fs.firestore.runTransaction(async txn => {
                                test.event('transaction started');

                                const snaps = await txn.get(query);
                                expect(snaps.size).toBe(2);
                                test.event('transaction locked the query');
                                await time(500);
                                expect(test.lastEvent$.get()).toBe('transaction locked the query');

                                txn.update(docRef1, { other: 'data' });
                            });
                            test.event('transaction completed');
                        },
                        async () => {
                            await test.when('transaction locked the query');
                            await docRef1.delete();
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

                test.concurrent('create after reading a query', async () => {
                    const testName = 'create';

                    const [docRef1, docRef2] = refs();

                    await docRef1.create(fs.writeData({ testName }));
                    // await docRef2.create(fs.writeData({ testName }));

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
                    txn.set(ref, writeData({ [name]: { tries } }), { merge: true });
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

function refs(): Array<FirebaseFirestore.DocumentReference<FirebaseFirestore.DocumentData>> {
    return range(2).map(() => fs.collection.doc());
}

async function getData(promisedSnap: FirebaseFirestore.DocumentSnapshot | Promise<FirebaseFirestore.DocumentSnapshot>) {
    const snap = await promisedSnap;
    expect(snap.exists).toBeTrue();
    return fs.readData(snap.data());
}
