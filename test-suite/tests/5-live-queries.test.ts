import { fromEventPattern } from '@skunkteam/sherlock-utils';
import assert from 'assert';
import { omit, range } from 'lodash';
import { setTimeout as time } from 'timers/promises';
import { fs } from './utils';

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
            query: (coll: FirebaseFirestore.Query) => coll,
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
            query: (coll: FirebaseFirestore.Query) => coll.where('included', '==', true).orderBy('included'),
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
            query: (coll: FirebaseFirestore.Query) => coll.where('included', '!=', false).orderBy('included'),
        },
        {
            description: 'greater than query',
            createDoc: async (coll: FirebaseFirestore.CollectionReference, data: FirebaseFirestore.DocumentData) => {
                const included = coll.doc();
                const notIncluded = coll.doc();
                await included.create(fs.writeData({ included: true, number: 1_000, ...data }));
                await notIncluded.create(fs.writeData({ included: false, number: -1_000, ...data }));
                return [included, notIncluded];
            },
            query: (coll: FirebaseFirestore.Query) => coll.where('number', '>=', 0).orderBy('number'),
        },
    ] as const)('$description', ({ createDoc, query }) => {
        describe('single (relevant) document', () => {
            test.concurrent('create, listen, stop', async () => {
                const coll = subCollection();

                await createDoc(coll, { some: 'data' });
                const { stop, getCurrent } = listen(query(coll));

                expect(await getCurrent()).toEqual([{ some: 'data' }]);

                stop();
            });

            test.concurrent('create, listen, update, stop', async () => {
                const coll = subCollection();

                const docs = await createDoc(coll, { some: 'data' });
                const { stop, getCurrent, getNext } = listen(query(coll));

                expect(await getCurrent()).toEqual([{ some: 'data' }]);

                const [secondData] = await Promise.all([getNext(), update(docs, { some: 'other data' })]);

                expect(secondData).toEqual([{ some: 'other data' }]);

                stop();
            });

            test.concurrent('listen, create, update, stop', async () => {
                const coll = subCollection();

                const { stop, getCurrent, getNext } = listen(query(coll));
                expect(await getCurrent()).toEqual([]);

                const resultPromise = getNext();
                const docs = await createDoc(coll, { some: 'data' });
                expect(await resultPromise).toEqual([{ some: 'data' }]);

                const [secondData] = await Promise.all([getNext(), update(docs, { some: 'other data' })]);

                expect(secondData).toEqual([{ some: 'other data' }]);

                stop();
            });
        });

        fs.notImplementedInRust ||
            test.concurrent('add/remove relevant documents', async () => {
                const coll = subCollection();

                const { stop, getCurrent, getNext } = listen(query(coll));
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

        fs.notImplementedInRust ||
            test.concurrent('batch write', async () => {
                const coll = subCollection();

                const refs = await Promise.all(range(5).map(i => createDoc(coll, { some: 'doc: ' + i }))).then(r => r.flat());

                const { stop, getCurrent, getNext, queryResults } = listen(query(coll));
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

                // There should only be 3 updates:
                // 1. the initial update for the listen
                // 2. the update batch
                // 3. the delete batch
                expect(queryResults).toBeArrayOfSize(3);

                stop();
            });

        fs.notImplementedInRust ||
            test.concurrent('limit', async () => {
                const coll = subCollection();

                const { stop, getCurrent, getNext } = listen(query(coll).orderBy('ordered').limit(3));
                expect(await getCurrent()).toEqual([]);

                const [onlyOne, [refOne]] = await Promise.all([getNext(), createDoc(coll, { ordered: 1 })]);
                expect(onlyOne).toEqual([{ ordered: 1 }]);

                const [nowTwo] = await Promise.all([getNext(), createDoc(coll, { ordered: 5 })]);
                expect(nowTwo).toEqual([{ ordered: 1 }, { ordered: 5 }]);

                const [nowThree, [refThree]] = await Promise.all([getNext(), createDoc(coll, { ordered: 3 })]);
                expect(nowThree).toEqual([{ ordered: 1 }, { ordered: 3 }, { ordered: 5 }]);

                const [replacedOne] = await Promise.all([getNext(), createDoc(coll, { ordered: 4 })]);
                expect(replacedOne).toEqual([{ ordered: 1 }, { ordered: 3 }, { ordered: 4 }]);

                const [movedOne] = await Promise.all([getNext(), refThree.update({ ordered: 10 })]);
                expect(movedOne).toEqual([{ ordered: 1 }, { ordered: 4 }, { ordered: 5 }]);

                const [removed] = await Promise.all([getNext(), refOne.delete()]);
                expect(removed).toEqual([{ ordered: 4 }, { ordered: 5 }, { ordered: 10 }]);

                stop();
            });
    });
});

function subCollection(coll = fs.collection) {
    return coll.doc().collection('collection');
}

function listen(query: FirebaseFirestore.Query) {
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
                return omit(rest, ['number']);
            });
        });
    const queryResults: FirebaseFirestore.DocumentData[][] = [];
    return {
        // Start listening:
        stop: document$.react(docs => queryResults.push(docs)),
        queryResults,
        getCurrent: () => document$.toPromise(),
        getNext: () =>
            Promise.race([
                document$.toPromise({ skipFirst: true }),
                time(1000).then(() => Promise.reject(`Timeout after ${queryResults.length} total snapshots.`)),
            ]),
    };
}
async function update(docs: FirebaseFirestore.DocumentReference[], data: FirebaseFirestore.DocumentData) {
    return Promise.all(docs.map(ref => ref.update(data)));
}
