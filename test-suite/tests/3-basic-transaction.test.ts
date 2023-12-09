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
        test.concurrent('chaos', async () => {
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
        });

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
            const log: string[] = [];
            const lastEvent$ = atom.unresolved<string>();
            const processName = new AsyncLocalStorage<string>();

            beforeEach(() => {
                log.length = 0;
                lastEvent$.unset();
            });

            function event(what: string) {
                log.push(`${processName.getStore()} | EVENT: ${what}`);
                lastEvent$.set(what);
            }

            async function when(what: string) {
                log.push(`${processName.getStore()} | WAITING UNTIL: ${what}`);
                const errorAfterTime$ = fromPromise(time(10_000)).map((): MaybeFinalState<never> => {
                    const msg = `${processName.getStore()} timeout, current log: ${log.map(m => '\n- ' + m).join('')}`;
                    // console.log('Current lo')
                    return final(error(msg));
                });
                await lastEvent$.toPromise({ when: d$ => d$.is(what).or(errorAfterTime$) });
            }

            async function concurrently(...processes: Array<() => Promise<unknown>>) {
                await Promise.all(processes.map(async (p, i) => processName.run(`Process ${i + 1}`, p)));
            }

            test('reading the same doc from different txns', async () => {
                // Scenario:
                // Process 1 - create doc
                // Process 1 - start txn A
                // Process 1 - in txn A: read doc
                // Process 2 - start txn B
                // Process 2 - in txn B: read doc
                // Process 2 - end txn B
                // Process 1 - end txn A

                const [ref] = refs();

                await concurrently(
                    // Process 1
                    async () => {
                        await ref.create(fs.writeData({ value: 'original value' }));
                        event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            event('in txn A: doc read');

                            await when('txn B ended');
                            return value;
                        });
                        event('read: ' + result);
                        event('txn A ended');
                    },

                    // Process 2
                    async () => {
                        await when('in txn A: doc read');
                        const result = await fs.firestore.runTransaction(async txn => {
                            event('txn B started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            event('in txn B: doc read');

                            return value;
                        });
                        event('read: ' + result);
                        event('txn B ended');
                    },
                );

                expect(log).toEqual([
                    'Process 2 | WAITING UNTIL: in txn A: doc read',
                    'Process 1 | EVENT: doc created',
                    'Process 1 | EVENT: txn A started',
                    'Process 1 | EVENT: in txn A: doc read',
                    'Process 1 | WAITING UNTIL: txn B ended',
                    'Process 2 | EVENT: txn B started',
                    'Process 2 | EVENT: in txn B: doc read',
                    'Process 2 | EVENT: read: original value',
                    'Process 2 | EVENT: txn B ended',
                    'Process 1 | EVENT: read: original value',
                    'Process 1 | EVENT: txn A ended',
                ]);
            });

            test('reading the same doc from different txns, try to write in second txn', async () => {
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

                await concurrently(
                    // Process 1
                    async () => {
                        await ref.create(fs.writeData({ value: 'original value' }));
                        event('doc created');

                        const result = await fs.firestore.runTransaction(async txn => {
                            event('txn A started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            event('in txn A: doc read');

                            await when('in txn B: update requested in txn');
                            await time(500);
                            expect(lastEvent$.get()).toBe('in txn B: update requested in txn');
                            event('waited 500ms, txn B still pending');
                            return value;
                        });
                        event('txn A ended');
                        event('read: ' + result);
                    },

                    // Process 2
                    async () => {
                        await when('in txn A: doc read');
                        const result = await fs.firestore.runTransaction(async txn => {
                            event('txn B started');

                            const snap = await txn.get(ref);
                            const value = snap.get('value') as unknown;
                            event('in txn B: doc read');

                            txn.update(ref, { value: 'changed by txn B' });
                            event('in txn B: update requested in txn');
                            return value;
                        });
                        event('txn B ended');
                        event('read: ' + result);
                    },
                );

                expect(log).toEqual([
                    'Process 2 | WAITING UNTIL: in txn A: doc read',
                    'Process 1 | EVENT: doc created',
                    'Process 1 | EVENT: txn A started',
                    'Process 1 | EVENT: in txn A: doc read',
                    'Process 1 | WAITING UNTIL: in txn B: update requested in txn',
                    'Process 2 | EVENT: txn B started',
                    'Process 2 | EVENT: in txn B: doc read',
                    'Process 2 | EVENT: in txn B: update requested in txn',
                    'Process 1 | EVENT: waited 500ms, txn B still pending',
                    'Process 1 | EVENT: txn A ended',
                    'Process 1 | EVENT: read: original value',
                    'Process 2 | EVENT: txn B ended',
                    'Process 2 | EVENT: read: original value',
                ]);
            });

            test('regular writes also wait until all txns-locks are released', async () => {
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

                await concurrently(
                    // Process 1
                    async () => {
                        await ref1.create(fs.writeData({ log: ['created outside txn'] }));
                        event('create outside txn succeeded');

                        await when('in txn B: read doc 1');
                        const promise = ref1.update({ log: fs.exported.FieldValue.arrayUnion('updated outside txn in process 1') });
                        event('started update outside of txn in process 1');

                        await promise;
                        expect(lastEvent$.get()).toBe('end txn A');
                        event('finished update outside of txn in process 1');
                    },
                    // Process 2
                    async () => {
                        await when('create outside txn succeeded');
                        await fs.firestore.runTransaction(async txn => {
                            await txn.get(ref1);
                            event('in txn A: read doc 1');

                            await when('end txn B');
                            txn.update(ref1, { log: fs.exported.FieldValue.arrayUnion('updated inside txn A') });
                        });
                        event('end txn A');
                    },
                    // Process 3
                    async () => {
                        await when('in txn A: read doc 1');
                        await fs.firestore.runTransaction(async txn => {
                            await txn.get(ref1);
                            event('in txn B: read doc 1');

                            await when('started update outside of txn in process 4');
                            txn.create(ref2, fs.writeData({ from: 'txn B' }));
                        });
                        event('end txn B');
                    },
                    // Process 4
                    async () => {
                        await when('started update outside of txn in process 1');
                        await time(15);
                        const promise = ref1.update({ log: fs.exported.FieldValue.arrayUnion('updated outside txn in process 4') });
                        event('started update outside of txn in process 4');

                        await promise;
                        expect(lastEvent$.get()).toBe('finished update outside of txn in process 1');
                        event('finished update outside of txn in process 4');
                    },
                );

                expect(log).toEqual([
                    'Process 2 | WAITING UNTIL: create outside txn succeeded',
                    'Process 3 | WAITING UNTIL: in txn A: read doc 1',
                    'Process 4 | WAITING UNTIL: started update outside of txn in process 1',
                    'Process 1 | EVENT: create outside txn succeeded',
                    'Process 1 | WAITING UNTIL: in txn B: read doc 1',
                    'Process 2 | EVENT: in txn A: read doc 1',
                    'Process 2 | WAITING UNTIL: end txn B',
                    'Process 3 | EVENT: in txn B: read doc 1',
                    'Process 3 | WAITING UNTIL: started update outside of txn in process 4',
                    'Process 1 | EVENT: started update outside of txn in process 1',
                    'Process 4 | EVENT: started update outside of txn in process 4',
                    'Process 3 | EVENT: end txn B',
                    'Process 2 | EVENT: end txn A',
                    'Process 1 | EVENT: finished update outside of txn in process 1',
                    'Process 4 | EVENT: finished update outside of txn in process 4',
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
            awaitAfterTxn && (await awaitAfterTxn);
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
