import { fromEventPattern } from '@skunkteam/sherlock-utils';
import assert from 'assert';
import { noop, omit, range } from 'lodash';
import { fs } from './utils';

fs.notImplementedInRust ||
    describe('listen to query updates', () => {
        describe.each([
            {
                description: 'full collection',
                createDoc: async (coll: FirebaseFirestore.CollectionReference, data: FirebaseFirestore.DocumentData) => {
                    const included = coll.doc();
                    // SubCollection with the same name as the main collection
                    const notIncluded = subCollection(coll).doc();
                    await included.create(fs.writeData({ included: true, ...data }));
                    await notIncluded.create(fs.writeData({ included: false, ...data }));

                    return [included, notIncluded];
                },
                listen: (coll: FirebaseFirestore.CollectionReference) => baseListen(coll),
            },
            {
                description: 'equality query',
                createDoc: async (coll: FirebaseFirestore.CollectionReference, data: FirebaseFirestore.DocumentData) => {
                    const included = coll.doc();
                    const notIncluded = coll.doc();
                    await included.create(fs.writeData({ included: true, ...data }));
                    await notIncluded.create(fs.writeData({ included: false, ...data }));
                    return [included, notIncluded];
                },
                listen: (coll: FirebaseFirestore.CollectionReference) => baseListen(coll.where('included', '==', true)),
            },
            {
                description: 'inequality query',
                createDoc: async (coll: FirebaseFirestore.CollectionReference, data: FirebaseFirestore.DocumentData) => {
                    const included = coll.doc();
                    const notIncluded = coll.doc();
                    await included.create(fs.writeData({ included: true, ...data }));
                    await notIncluded.create(fs.writeData({ included: false, ...data }));
                    return [included, notIncluded];
                },
                listen: (coll: FirebaseFirestore.CollectionReference) => baseListen(coll.where('included', '!=', false)),
            },
            {
                description: 'greater than query',
                createDoc: async (coll: FirebaseFirestore.CollectionReference, data: FirebaseFirestore.DocumentData) => {
                    const included = coll.doc();
                    const notIncluded = coll.doc();
                    await included.create(fs.writeData({ included: true, number: Math.random() * 1_000, ...data }));
                    await notIncluded.create(fs.writeData({ included: false, number: -Math.random() * 1_000, ...data }));
                    return [included, notIncluded];
                },
                listen: (coll: FirebaseFirestore.CollectionReference) => baseListen(coll.where('number', '>=', 0), 'number'),
            },
        ] as const)('$description', ({ createDoc, listen }) => {
            describe('single (relevant) document', () => {
                test.concurrent('create, listen, stop', async () => {
                    const coll = subCollection();

                    await createDoc(coll, { some: 'data' });
                    const { stop, getCurrent } = listen(coll);

                    expect(await getCurrent()).toEqual([{ some: 'data' }]);

                    stop();
                });

                test.concurrent('create, listen, update, stop', async () => {
                    const coll = subCollection();

                    const docs = await createDoc(coll, { some: 'data' });
                    const { stop, getCurrent, getNext } = listen(coll);

                    expect(await getCurrent()).toEqual([{ some: 'data' }]);

                    const [secondData] = await Promise.all([getNext(), update(docs, { some: 'other data' })]);

                    expect(secondData).toEqual([{ some: 'other data' }]);

                    stop();
                });

                test.concurrent('listen, create, update, stop', async () => {
                    const coll = subCollection();

                    const { stop, getCurrent, getNext } = listen(coll);
                    expect(await getCurrent()).toEqual([]);

                    const resultPromise = getNext();
                    const docs = await createDoc(coll, { some: 'data' });
                    expect(await resultPromise).toEqual([{ some: 'data' }]);

                    const [secondData] = await Promise.all([getNext(), update(docs, { some: 'other data' })]);

                    expect(secondData).toEqual([{ some: 'other data' }]);

                    stop();
                });
            });

            test.concurrent('add/remove relevant documents', async () => {
                const coll = subCollection();

                const { stop, getCurrent, getNext } = listen(coll);
                expect(await getCurrent()).toEqual([]);

                const docPairs: FirebaseFirestore.DocumentReference[][] = [];
                for (let i = 1; i < 10; i++) {
                    const [newData, docs] = await Promise.all([getNext(), createDoc(coll, { some: 'doc: ' + i })]);
                    docPairs.push(docs);
                    expect(newData).toBeArrayOfSize(i);
                }

                while (docPairs.length) {
                    const docsToRemove = docPairs.pop();
                    assert(docsToRemove);
                    const [newData] = await Promise.all([getNext(), ...docsToRemove.map(doc => doc.delete())]);
                    expect(newData).toBeArrayOfSize(docPairs.length);
                }

                stop();
            });

            test.concurrent('batch write', async () => {
                const coll = subCollection();

                const refs = await Promise.all(range(5).map(i => createDoc(coll, { some: 'doc: ' + i }))).then(r => r.flat());

                const { stop, getCurrent, getNext } = listen(coll);
                expect(await getCurrent()).toBeArrayOfSize(5);

                const updateBatch = fs.firestore.batch();
                for (const ref of refs) {
                    updateBatch.update(ref, { with: 'update' });
                }
                const [newData] = await Promise.all([getNext(), updateBatch.commit()]);
                expect(newData).toBeArrayOfSize(5);
                for (const data of newData) {
                    expect(data).toEqual({ some: expect.stringMatching(/^doc: \d$/), with: 'update' });
                }

                const deleteBatch = fs.firestore.batch();
                for (const ref of refs) {
                    deleteBatch.delete(ref);
                }
                const [noData] = await Promise.all([getNext(), deleteBatch.commit()]);
                expect(noData).toBeArrayOfSize(0);

                stop();
            });
        });
    });

function subCollection(coll = fs.collection) {
    return coll.doc().collection('collection');
}

function baseListen(query: FirebaseFirestore.Query, excludeProp?: string) {
    const snapshot$ = fromEventPattern<FirebaseFirestore.QuerySnapshot>(value$ =>
        query.onSnapshot(
            snapshot => value$.set(snapshot),
            err => value$.setError(err),
        ),
    );

    const document$ = snapshot$
        .map(snap => snap.docs.map(doc => fs.readData(doc.data())))
        .map(data => {
            return data.map(({ included, ...rest }) => {
                expect(included).toBeTrue();
                if (excludeProp) {
                    expect(rest).toHaveProperty(excludeProp);
                }
                return excludeProp ? omit(rest, [excludeProp]) : rest;
            });
        });
    return {
        // Start listening:
        stop: snapshot$.react(noop),
        snapshot$,
        getCurrent: () => document$.toPromise(),
        getNext: () => document$.toPromise({ skipFirst: true }),
    };
}
async function update(docs: FirebaseFirestore.DocumentReference[], data: FirebaseFirestore.DocumentData) {
    return Promise.all(docs.map(ref => ref.update(data)));
}
