import { fromEventPattern } from '@skunkteam/sherlock-utils';
import { range } from 'lodash';
import { fs } from './utils';
import { writeData } from './utils/firestore';

describe('listen to document updates', () => {
    test.concurrent('create, listen, stop', async () => {
        const doc = fs.collection.doc();

        const { writeTime: createTime } = await doc.create(writeData({ some: 'data' }));
        const { stop, getCurrent } = listen(doc);

        checkSnap(await getCurrent(), { createTime, updateTime: createTime, data: { some: 'data' } });

        stop();
    });

    test.concurrent('no update when writing an empty update', async () => {
        const doc = fs.collection.doc();

        const { stop, snapshots, getNext, getCurrent } = listen(doc);

        const emptySnap = await getCurrent();
        expect(snapshots).toHaveLength(1);
        expect(emptySnap.isEqual(snapshots[0])).toBeTrue();

        const initialData = writeData({ some: { nested: 'data' } });
        const [firstSnap, { writeTime: createTime }] = await Promise.all([getNext(), doc.create(initialData)]);

        expect(snapshots).toHaveLength(2);
        expect(firstSnap.isEqual(snapshots[1])).toBeTrue();
        checkSnap(firstSnap, { createTime, updateTime: createTime, data: { some: { nested: 'data' } } });

        // Try to write the same data again through a `set` and `update`
        const { writeTime: setTime } = await doc.set(initialData);
        expect(setTime).toEqual(createTime);
        const { writeTime: updateTime } = await doc.update('some.nested', 'data');
        expect(updateTime).toEqual(createTime);

        // Now really update the document, to force a 'true' update
        const [secondSnap, { writeTime: realUpdateTime }] = await Promise.all([getNext(), doc.update({ some: { other: 'data' } })]);
        expect(realUpdateTime).not.toEqual(createTime);

        expect(snapshots).toHaveLength(3);
        expect(secondSnap.isEqual(snapshots[2])).toBeTrue();
        checkSnap(secondSnap, { createTime, updateTime: realUpdateTime, data: { some: { other: 'data' } } });

        stop();
    });

    test.concurrent('create, listen, update, stop', async () => {
        const doc = fs.collection.doc();

        const { writeTime: createTime } = await doc.create(writeData({ some: 'data' }));
        const { stop, getCurrent, getNext } = listen(doc);

        checkSnap(await getCurrent(), { createTime, updateTime: createTime, data: { some: 'data' } });

        const [secondSnap, { writeTime: updateTime }] = await Promise.all([getNext(), doc.update({ some: 'other data' })]);

        checkSnap(secondSnap, { createTime, updateTime, data: { some: 'other data' } });

        expect(updateTime).not.toBe(createTime);

        stop();
    });

    test.concurrent('listen, create, update, stop', async () => {
        const doc = fs.collection.doc();

        const { stop, getCurrent, getNext } = listen(doc);

        const firstSnap = await getCurrent();
        expect(firstSnap.exists).toBeFalse();

        const [secondSnap, { writeTime: createTime }] = await Promise.all([getNext(), doc.create(writeData({ some: 'data' }))]);
        checkSnap(secondSnap, { createTime, updateTime: createTime, data: { some: 'data' } });

        const [thirdSnap, { writeTime: updateTime }] = await Promise.all([getNext(), doc.update({ some: 'other data' })]);
        checkSnap(thirdSnap, { createTime, updateTime, data: { some: 'other data' } });

        expect(updateTime).not.toBe(createTime);

        stop();
    });

    test.concurrent('a lot of docs: create, listen, update, stop', async () => {
        const refs = range(450).map(() => fs.collection.doc());

        await refs.reduce((batch, ref, id) => batch.create(ref, { id }), fs.firestore.batch()).commit();

        const listeners = refs.map(listen);

        const firstVersions = await Promise.all(listeners.map(({ getCurrent }) => getCurrent()));
        expect(firstVersions.map(snap => snap.get('id') as unknown)).toEqual(range(450));

        const [, ...secondVersions] = await Promise.all([
            refs.reduce((batch, ref) => batch.update(ref, { add: 'data' }), fs.firestore.batch()).commit(),
            ...listeners.map(({ getNext }) => getNext()),
        ]);

        expect(secondVersions.map(snap => snap.get('add') as unknown)).toEqual(range(450).map(() => 'data'));

        for (const { stop } of listeners) stop();
    });

    test.concurrent('receiving a single update on multiple updates in single txn', async () => {
        const doc = fs.collection.doc();
        const { stop, getCurrent, getNext } = listen(doc);

        const firstSnap = await getCurrent();
        expect(firstSnap.exists).toBeFalse();

        const nextSnap = getNext();

        await fs.firestore.runTransaction(async txn => {
            expect(await txn.get(doc)).toHaveProperty('exists', false);
            txn.create(doc, { created: fs.exported.FieldValue.serverTimestamp() });
            txn.update(doc, { updated: fs.exported.FieldValue.serverTimestamp() });
            txn.update(doc, { counter: fs.exported.FieldValue.increment(1) });
            txn.update(doc, { counter: fs.exported.FieldValue.increment(1) });
            txn.update(doc, { array: fs.exported.FieldValue.arrayUnion({ id: 1 }, { id: 2 }) });
            txn.update(doc, { array: fs.exported.FieldValue.arrayUnion({ id: 2 }, { id: 3 }) });
            txn.update(doc, { array: fs.exported.FieldValue.arrayRemove({ id: 1 }) });
        });

        const snap = (await nextSnap).data();
        expect(snap).toEqual({
            created: expect.any(fs.exported.Timestamp),
            updated: expect.any(fs.exported.Timestamp),
            counter: 2,
            array: [{ id: 2 }, { id: 3 }],
        });
        expect(snap?.created).toEqual(snap?.updated);

        stop();
    });
});

function listen(doc: FirebaseFirestore.DocumentReference) {
    const snapshot$ = fromEventPattern<FirebaseFirestore.DocumentSnapshot>(value$ =>
        doc.onSnapshot(
            snapshot => value$.set(snapshot),
            err => value$.setError(err),
        ),
    );
    const snapshots: FirebaseFirestore.DocumentSnapshot[] = [];
    return {
        // Start listening:
        stop: snapshot$.react(snap => snapshots.push(snap)),
        snapshots,
        getCurrent: () => snapshot$.toPromise(),
        getNext: () => snapshot$.toPromise({ skipFirst: true }),
    };
}

function checkSnap(
    snap: FirebaseFirestore.DocumentSnapshot,
    expected: { createTime: FirebaseFirestore.Timestamp; updateTime: FirebaseFirestore.Timestamp; data: FirebaseFirestore.DocumentData },
) {
    expect(fs.readData(snap.data())).toEqual(expected.data);
    expect(snap.createTime).toEqual(expected.createTime);
    expect(snap.updateTime).toEqual(expected.updateTime);
}
