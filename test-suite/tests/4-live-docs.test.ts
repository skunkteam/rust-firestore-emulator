import { fromEventPattern } from '@skunkteam/sherlock-utils';
import { noop, range } from 'lodash';
import { fs } from './utils';
import { writeData } from './utils/firestore';

describe('listen to document updates', () => {
    test.concurrent('create, listen, stop', async () => {
        const doc = fs.collection.doc();
        const { writeTime: createTime } = await doc.create(writeData({ some: 'data' }));
        const { stop, snapshot$ } = listen(doc);
        const firstSnap = await snapshot$.toPromise();
        expect(firstSnap.data()).toEqual({ some: 'data', ttl: expect.anything() });
        expect(firstSnap.createTime).toEqual(createTime);
        expect(firstSnap.updateTime).toEqual(createTime);

        stop();
    });

    test.concurrent('create, listen, update, stop', async () => {
        const doc = fs.collection.doc();
        const { writeTime: createTime } = await doc.create(writeData({ some: 'data' }));
        const { stop, snapshot$ } = listen(doc);
        const firstSnap = await snapshot$.toPromise();
        expect(firstSnap.data()).toEqual({ some: 'data', ttl: expect.anything() });
        expect(firstSnap.createTime).toEqual(createTime);
        expect(firstSnap.updateTime).toEqual(createTime);

        const secondPromise = snapshot$.toPromise({ skipFirst: true });
        const { writeTime: updateTime } = await doc.update({ some: 'other data' });
        const secondSnap = await secondPromise;
        expect(secondSnap.data()).toEqual({ some: 'other data', ttl: expect.anything() });
        expect(secondSnap.createTime).toEqual(createTime);
        expect(secondSnap.updateTime).toEqual(updateTime);

        expect(updateTime).not.toBe(createTime);

        stop();
    });

    test.concurrent('listen, create, update, stop', async () => {
        const doc = fs.collection.doc();
        const { stop, snapshot$ } = listen(doc);
        const firstSnap = await snapshot$.toPromise();
        expect(firstSnap.exists).toBeFalse();

        const secondPromise = snapshot$.toPromise({ skipFirst: true });
        const { writeTime: createTime } = await doc.create(writeData({ some: 'data' }));
        const secondSnap = await secondPromise;
        expect(secondSnap.data()).toEqual({ some: 'data', ttl: expect.anything() });
        expect(secondSnap.createTime).toEqual(createTime);
        expect(secondSnap.updateTime).toEqual(createTime);

        const thirdPromise = snapshot$.toPromise({ skipFirst: true });
        const { writeTime: updateTime } = await doc.update({ some: 'other data' });
        const thirdSnap = await thirdPromise;
        expect(thirdSnap.data()).toEqual({ some: 'other data', ttl: expect.anything() });
        expect(thirdSnap.createTime).toEqual(createTime);
        expect(thirdSnap.updateTime).toEqual(updateTime);

        expect(updateTime).not.toBe(createTime);

        stop();
    });

    test.concurrent('a lot of docs: create, listen, update, stop', async () => {
        const refs = range(450).map(() => fs.collection.doc());

        await refs.reduce((batch, ref, id) => batch.create(ref, { id }), fs.firestore.batch()).commit();

        const listeners = refs.map(listen);

        const firstVersions = await Promise.all(listeners.map(({ snapshot$ }) => snapshot$.toPromise()));
        expect(firstVersions.map(snap => snap.get('id') as unknown)).toEqual(range(450));

        const secondVersionsPromise = Promise.all(listeners.map(({ snapshot$ }) => snapshot$.toPromise({ skipFirst: true })));

        await refs.reduce((batch, ref) => batch.update(ref, { add: 'data' }), fs.firestore.batch()).commit();

        const secondVersions = await secondVersionsPromise;
        expect(secondVersions.map(snap => snap.get('add') as unknown)).toEqual(range(450).map(() => 'data'));

        for (const { stop } of listeners) stop();
    });
});

function listen(doc: FirebaseFirestore.DocumentReference) {
    const snapshot$ = fromEventPattern<FirebaseFirestore.DocumentSnapshot>(value$ =>
        doc.onSnapshot(
            snapshot => value$.set(snapshot),
            err => value$.setError(err),
        ),
    );
    return {
        // Start listening:
        stop: snapshot$.react(noop),
        snapshot$,
    };
}
