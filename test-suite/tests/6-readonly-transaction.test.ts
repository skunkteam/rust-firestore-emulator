import { FieldValue, Timestamp } from '@google-cloud/firestore';
import { range } from 'lodash';
import { ConcurrentTest, editions, readData, writeData } from './utils';

describe.each(editions)('$description', fs => {
    describe('readonly transactions', () => {
        interface Doc {
            id: number;
            time: Timestamp;
        }

        async function createDocs(n = 2) {
            const refs = range(n).map(() => fs.collection.doc());
            const writeTimes: Timestamp[] = [];
            const checkTimes = [];
            for (const [ref, i] of refs.map((ref, i) => [ref, i] as const)) {
                const { writeTime } = await ref.create(writeData<Doc>({ id: i + 1, time: FieldValue.serverTimestamp() }));
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
            const docs = snaps.map(snap => readData<Doc>(snap.data()));
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
                    txn.update(refs[0], { time: FieldValue.serverTimestamp(), broken: true });
                    txn.update(refs[1], { time: FieldValue.serverTimestamp(), broken: true });
                },
                { readOnly: true },
            );

            // The server reports: 'INVALID_ARGUMENT: Cannot modify entities in a read-only transaction', but this is now checked client-side.
            await expect(txnPromise).rejects.toThrow('Firestore read-only transactions cannot execute writes');
        });

        test.concurrent('with specific read-time', async () => {
            const { refs, writeTimes, checkTimes } = await createDocs();
            const { writeTime: updateTime } = await refs[0].update({ updated: true });

            // When using the time of the first document, we should not find the second document.
            const onlyTheFirst = await fs.firestore.runTransaction(txn => txn.getAll(...refs), { readOnly: true, readTime: writeTimes[0] });
            expect(onlyTheFirst.map(s => s.exists)).toEqual([true, false]);
            expect(readData(onlyTheFirst[0].data())).toEqual({ id: 1, time: checkTimes[0] });

            // When using the time of the second document, we should see the original documents.
            const originalSnaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs), {
                readOnly: true,
                readTime: writeTimes[1],
            });
            const originalDocs = originalSnaps.map(s => readData<Doc>(s.data()));
            expect(originalDocs).toEqual([
                { id: 1, time: checkTimes[0] },
                { id: 2, time: checkTimes[1] },
            ]);

            // When using the time of the last update, we should see the updated documents.
            const updatedSnaps = await fs.firestore.runTransaction(txn => txn.getAll(...refs), { readOnly: true, readTime: updateTime });
            const updatedDocs = updatedSnaps.map(s => readData<Doc>(s.data()));
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

        test.concurrent('read-only txn without readTime maintains snapshot isolation', async () => {
            const test = new ConcurrentTest();
            const docRef1 = fs.collection.doc();
            const docRef2 = fs.collection.doc();
            await Promise.all([docRef1.create(writeData({ v: 1 })), docRef2.create(writeData({ v: 1 }))]);

            await test.run(
                async () => {
                    await fs.firestore.runTransaction(
                        async txn => {
                            const snap1 = await txn.get(docRef1);
                            test.event('ro read docRef1');
                            await test.when('docs updated');
                            const snap2 = await txn.get(docRef2);
                            const [all1, all2] = await txn.getAll(docRef1, docRef2);

                            expect(readData(snap1.data())).toEqual({ v: 1 });
                            expect(readData(snap2.data())).toEqual({ v: 1 });
                            expect([readData(all1.data()), readData(all2.data())]).toEqual([{ v: 1 }, { v: 1 }]);
                            expect(snap1.readTime.isEqual(snap2.readTime)).toBeTrue();
                            expect(snap1.readTime.isEqual(all1.readTime)).toBeTrue();
                        },
                        { readOnly: true },
                    );
                },
                async () => {
                    await test.when('ro read docRef1');
                    await Promise.all([docRef1.update({ v: 2 }), docRef2.update({ v: 2 })]);
                    test.event('docs updated');
                },
            );

            expect(test.log).toEqual([
                '       | <<2>> | WAITING UNTIL: ro read docRef1',
                ' <<1>> | EVENT: ro read docRef1',
                ' <<1>> | WAITING UNTIL: docs updated',
                '       | <<2>> | EVENT: docs updated',
            ]);
            expect(readData((await docRef1.get()).data())).toEqual({ v: 2 });
            expect(readData((await docRef2.get()).data())).toEqual({ v: 2 });
        });

        test.concurrent('rejects future and negative readTime in read-only txn', async () => {
            const docRef = fs.collection.doc();
            const future = Timestamp.fromMillis(Date.now() + 60_000);
            const negative = Timestamp.fromMillis(-1000);

            await expect(fs.firestore.runTransaction(txn => txn.get(docRef), { readOnly: true, readTime: future })).rejects.toThrow(
                /3 INVALID_ARGUMENT/,
            );

            await expect(fs.firestore.runTransaction(txn => txn.get(docRef), { readOnly: true, readTime: negative })).rejects.toThrow(
                /3 INVALID_ARGUMENT/,
            );
        });
    });
});
