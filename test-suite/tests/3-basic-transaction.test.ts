import assert from 'assert';
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

            await runTxn('outer', [docRef1], async () => {
                // Will need a retry because it reads `docRef1`, even though it only writes `docRef2`
                const { innerTxnCompleted } = await innerTxn('inner', { read: [docRef1, docRef2], write: [docRef2] });

                return { awaitAfterTxn: innerTxnCompleted };
            });
            expect(await getData(docRef1.get())).toEqual({
                outer: { tries: 1 },
            });
            expect(await getData(docRef2.get())).toEqual({
                inner: { tries: 2 },
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
