import assert from 'assert';
import { range } from 'lodash';
import { setTimeout as time } from 'timers/promises';
import { fs } from './utils';
import { writeData } from './utils/firestore';

let docRef1: FirebaseFirestore.DocumentReference;
let docRef2: FirebaseFirestore.DocumentReference;
beforeEach(async () => {
    [docRef1, docRef2] = range(2).map(() => fs.collection.doc());
});

test('simple txn', async () => {
    await fs.firestore.runTransaction(async txn => {
        expect(await txn.get(docRef1)).toHaveProperty('exists', false);

        txn.set(docRef1, writeData({ foo: 'bar' }));

        expect(() => txn.get(docRef1)).toThrow('Firestore transactions require all reads to be executed before all writes.');
    });
    expect(await getData(docRef1.get())).toEqual({ foo: 'bar' });
});

test('using txn.getAll', async () => {
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

test('aborting transaction', async () => {
    await expect(
        fs.firestore.runTransaction(async txn => {
            expect(await txn.get(docRef1)).toHaveProperty('exists', false);

            txn.set(docRef1, writeData({ foo: 'bar' }));

            throw new Error('I quit!');
        }),
    ).rejects.toThrow('I quit!');

    expect(await docRef1.get()).toHaveProperty('exists', false);
});

fs.notImplementedInRust ||
    describe('locks', () => {
        test('retry if document is locked', async () => {
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

        test('lock on non-existing document', async () => {
            await runTxn('outer', [docRef1], async () => {
                const { innerTxnCompleted } = await innerTxn('inner', [docRef1]);

                return { awaitAfterTxn: innerTxnCompleted };
            });

            expect(await getData(docRef1.get())).toEqual({
                outer: { tries: 1 },
                inner: { tries: 2 },
            });
        });

        test('no lock if getting separate documents', async () => {
            await runTxn('outer', [docRef1], async () => {
                const { innerTxnCompleted } = await innerTxn('inner', [docRef2]);
                return { awaitAfterTxn: innerTxnCompleted };
            });
            expect(await getData(docRef1.get())).toEqual({ outer: { tries: 1 } });
            expect(await getData(docRef2.get())).toEqual({ inner: { tries: 1 } });
        });

        // Note: Very slow on Cloud Firestore!!
        test('chaos', async () => {
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

        test('regular `set` waits on transaction', async () => {
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

        async function runTxn(
            name: string,
            refs: FirebaseFirestore.DocumentReference[],
            runAfterGet: () => void | Promise<void | { awaitAfterTxn?: Promise<unknown> }>,
        ) {
            let awaitAfterTxn: Promise<unknown> | undefined;
            let tries = 0;
            await fs.firestore.runTransaction(async txn => {
                tries++;
                for (const ref of refs) {
                    await txn.get(ref);
                }

                ({ awaitAfterTxn } = (await runAfterGet()) ?? {});

                for (const ref of refs) {
                    txn.set(ref, writeData({ [name]: { tries } }), { merge: true });
                }
            });
            awaitAfterTxn && (await awaitAfterTxn);
        }
        async function innerTxn(name: string, refs: FirebaseFirestore.DocumentReference[]) {
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

async function getData(promisedSnap: FirebaseFirestore.DocumentSnapshot | Promise<FirebaseFirestore.DocumentSnapshot>) {
    const snap = await promisedSnap;
    expect(snap.exists).toBeTrue();
    return fs.readData(snap.data());
}
