import { range } from 'lodash';
import { fs } from './utils';

describe('readonly transactions', () => {
    interface Doc {
        id: number;
        time: FirebaseFirestore.Timestamp;
    }

    async function createDocs(n = 2) {
        const refs = range(n).map(() => fs.collection.doc());
        const writeTimes: FirebaseFirestore.Timestamp[] = [];
        const checkTimes = [];
        for (const [ref, i] of refs.map((ref, i) => [ref, i] as const)) {
            const { writeTime } = await ref.create(fs.writeData<Doc>({ id: i + 1, time: fs.exported.FieldValue.serverTimestamp() }));
            writeTimes.push(writeTime);
            checkTimes.push(
                expect.toSatisfy(
                    v =>
                        // Every `time` field should not be after the reported write time...
                        v <= writeTime &&
                        // ... and should be after the reported write time of the previous document (if any).
                        (i === 0 || v > writeTimes[i - 1]),
                ),
            );
        }
        return { refs, writeTimes, checkTimes };
    }

    test.concurrent('simple read-only txn', async () => {
        const { refs, checkTimes } = await createDocs();

        const snaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs), { readOnly: true });
        const docs = snaps.map(snap => fs.readData<Doc>(snap.data()));
        expect(docs).toEqual([
            { id: 1, time: checkTimes[0] },
            { id: 2, time: checkTimes[1] },
        ]);
        expect(docs[0].time < docs[1].time).toBeTrue();
    });

    test.concurrent('simple read-only with field mask', async () => {
        const { refs } = await createDocs();

        const snaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs, { fieldMask: ['id'] }), { readOnly: true });
        const docs = snaps.map(snap => snap.data() as Doc);
        expect(docs).toEqual([{ id: 1 }, { id: 2 }]);
    });

    test.concurrent('try to write in a read-only transaction', async () => {
        const { refs } = await createDocs();

        const txnPromise = fs.firestore.runTransaction(
            async txn => {
                txn.update(refs[0], { time: fs.exported.FieldValue.serverTimestamp(), broken: true });
                txn.update(refs[1], { time: fs.exported.FieldValue.serverTimestamp(), broken: true });
            },
            { readOnly: true },
        );

        if (fs.connection === 'JAVA EMULATOR') {
            // Java Firestore Emulator allows writing in read-only transactions.
            await expect(txnPromise).resolves.toBeUndefined();
            const snaps = await Promise.all(refs.map(ref => ref.get()));
            const docs = snaps.map(s => fs.readData(s.data()));
            expect(docs).toEqual([expect.objectContaining({ broken: true }), expect.objectContaining({ broken: true })]);
        } else {
            await expect(txnPromise).rejects.toThrow('INVALID_ARGUMENT: Cannot modify entities in a read-only transaction');
        }
    });

    test.concurrent('with specific read-time', async () => {
        const { refs, writeTimes, checkTimes } = await createDocs();
        const { writeTime: updateTime } = await refs[0].update({ updated: true });

        // When using the time of the first document, we should not find the second document.
        const onlyTheFirst = await fs.firestore.runTransaction(txn => txn.getAll(...refs), { readOnly: true, readTime: writeTimes[0] });
        expect(onlyTheFirst.map(s => s.exists)).toEqual([true, false]);
        expect(fs.readData(onlyTheFirst[0].data())).toEqual({ id: 1, time: checkTimes[0] });

        // When using the time of the second document, we should see the original documents.
        const originalSnaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs), { readOnly: true, readTime: writeTimes[1] });
        const originalDocs = originalSnaps.map(s => fs.readData<Doc>(s.data()));
        expect(originalDocs).toEqual([
            { id: 1, time: checkTimes[0] },
            { id: 2, time: checkTimes[1] },
        ]);

        // When using the time of the last update, we should see the updated documents.
        const updatedSnaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs), { readOnly: true, readTime: updateTime });
        const updatedDocs = updatedSnaps.map(s => fs.readData<Doc>(s.data()));
        expect(updatedDocs).toEqual([
            { id: 1, time: checkTimes[0], updated: true },
            { id: 2, time: checkTimes[1] },
        ]);
    });

    test.concurrent('with specific read-time with field mask', async () => {
        const { refs, writeTimes } = await createDocs();
        const { writeTime: updateTime } = await refs[0].update({ updated: true });

        // When using the time of the first document, we should not find the second document.
        const onlyTheFirst = await fs.firestore.runTransaction(txn => txn.getAll(...refs, { fieldMask: ['id', 'updated'] }), {
            readOnly: true,
            readTime: writeTimes[0],
        });
        expect(onlyTheFirst.map(s => s.exists)).toEqual([true, false]);
        expect(onlyTheFirst[0].data()).toEqual({ id: 1 });

        // When using the time of the second document, we should see the original documents.
        const originalSnaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs, { fieldMask: ['id', 'updated'] }), {
            readOnly: true,
            readTime: writeTimes[1],
        });
        const originalDocs = originalSnaps.map(s => s.data());
        expect(originalDocs).toEqual([{ id: 1 }, { id: 2 }]);

        // When using the time of the last update, we should see the updated documents.
        const updatedSnaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs, { fieldMask: ['id', 'updated'] }), {
            readOnly: true,
            readTime: updateTime,
        });
        const updatedDocs = updatedSnaps.map(s => s.data());
        expect(updatedDocs).toEqual([{ id: 1, updated: true }, { id: 2 }]);
    });
});
