import { CollectionReference, DocumentData, DocumentReference, FieldPath, Query, QuerySnapshot } from '@google-cloud/firestore';
import { fromEventPattern } from '@skunkteam/sherlock-utils';
import assert from 'assert';
import { omit, range } from 'lodash';
import { setTimeout as time } from 'timers/promises';
import { editions, readData, writeData } from './utils';

describe.each(editions)('$description', fs => {
    describe('listen to query updates', () => {
        describe.each([
            {
                description: 'full collection',
                createDoc: async (coll: CollectionReference, data: DocumentData) => {
                    const included = coll.doc();
                    // SubCollection with the same name as the main collection
                    const notIncluded = subCollection(coll).doc();
                    await included.create(writeData({ included: true, ...data }));
                    await notIncluded.create(writeData({ included: false, ...data }));

                    return [included, notIncluded];
                },
                query: (coll: Query) => coll,
                removeDoc: (doc: DocumentReference) => doc.delete(),
            },
            {
                description: 'equality query',
                createDoc: async (coll: CollectionReference, data: DocumentData) => {
                    const included = coll.doc();
                    const notIncluded = coll.doc();
                    await included.create(writeData({ included: true, ...data }));
                    await notIncluded.create(writeData({ included: false, ...data }));
                    return [included, notIncluded];
                },
                query: (coll: Query) => coll.where('included', '==', true).orderBy('included'),
                removeDoc: (doc: DocumentReference) => doc.update({ included: false }),
            },
            {
                description: 'inequality query',
                createDoc: async (coll: CollectionReference, data: DocumentData) => {
                    const included = coll.doc();
                    const notIncluded = coll.doc();
                    await included.create(writeData({ included: true, ...data }));
                    await notIncluded.create(writeData({ included: false, ...data }));
                    return [included, notIncluded];
                },
                query: (coll: Query) => coll.where('included', '!=', false).orderBy('included'),
                removeDoc: (doc: DocumentReference) => doc.update({ included: false }),
            },
            {
                description: 'greater than query',
                createDoc: async (coll: CollectionReference, data: DocumentData) => {
                    const included = coll.doc();
                    const notIncluded = coll.doc();
                    await included.create(writeData({ included: true, number: 1_000, ...data }));
                    await notIncluded.create(writeData({ included: false, number: -1_000, ...data }));
                    return [included, notIncluded];
                },
                query: (coll: Query) => coll.where('number', '>=', 0).orderBy('number'),
                removeDoc: (doc: DocumentReference) => doc.update({ number: -0.1 }),
            },
        ] as const)('$description', ({ createDoc, query, removeDoc }) => {
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

            test.concurrent('add/remove relevant documents', async () => {
                const coll = subCollection();
                const otherColl = subCollection();

                const { stop, getCurrent, getNext, queryResults } = listen(query(coll));
                expect(await getCurrent()).toEqual([]);

                const docPairs: DocumentReference[][] = [];
                for (let i = 1; i < 10; i++) {
                    const data = { some: 'doc: ' + i };
                    const [newData, docs] = await Promise.all([getNext(), createDoc(coll, data)]);
                    docPairs.push(docs);
                    expect(newData).toBeArrayOfSize(i);
                    // Also add same data in unrelated collection, which should not be included in query.
                    await createDoc(otherColl, data);
                }

                while (docPairs.length) {
                    const docsToRemove = docPairs.pop();
                    assert(docsToRemove);
                    const [newData] = await Promise.all([getNext(), ...docsToRemove.map(removeDoc)]);
                    expect(newData).toBeArrayOfSize(docPairs.length);
                }

                stop();

                expect(queryResults).toHaveLength(19);
            });

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

                const [removed] = await Promise.all([getNext(), removeDoc(refOne)]);
                expect(removed).toEqual([{ ordered: 4 }, { ordered: 5 }, { ordered: 10 }]);

                stop();
            });
        });
    });

    describe('array-contains-any and not-in live queries', () => {
        test.concurrent(
            'array-contains-any with limit(3) tracks additions, in-place updates, removals, batches, and deletions',
            async () => {
                const coll = subCollection();
                const q = coll.where('tags', 'array-contains-any', ['alpha', 'beta']).orderBy(FieldPath.documentId()).limit(3);
                const { stop, getCurrent, getNext } = listen(q, {
                    transform: snap => snap.docs.map(d => ({ id: d.id, ...readData(d.data()) })),
                });
                try {
                    expect(await getCurrent()).toEqual([]);

                    const [snap1] = await Promise.all([getNext(), coll.doc('d2').create(writeData({ tags: ['alpha'], note: 'v1' }))]);
                    expect(snap1).toEqual([{ id: 'd2', tags: ['alpha'], note: 'v1' }]);

                    // Non-matching doc does not alter snapshot; add d5 and d1 next.
                    await coll.doc('d0').create(writeData({ tags: ['gamma'] }));

                    const [snap2] = await Promise.all([getNext(), coll.doc('d5').create(writeData({ tags: ['beta'] }))]);
                    expect(snap2.map(d => d.id)).toEqual(['d2', 'd5']);

                    const [snap3] = await Promise.all([getNext(), coll.doc('d1').create(writeData({ tags: ['alpha', 'gamma'] }))]);
                    expect(snap3.map(d => d.id)).toEqual(['d1', 'd2', 'd5']);

                    // Adding d3 pushes d5 beyond limit(3).
                    const [snap4] = await Promise.all([getNext(), coll.doc('d3').create(writeData({ tags: ['beta'] }))]);
                    expect(snap4.map(d => d.id)).toEqual(['d1', 'd2', 'd3']);

                    // In-place update on matching d2 preserves membership while delivering updated field.
                    const [snap5] = await Promise.all([getNext(), coll.doc('d2').update({ note: 'v2' })]);
                    expect(snap5).toEqual([
                        { id: 'd1', tags: ['alpha', 'gamma'] },
                        { id: 'd2', tags: ['alpha'], note: 'v2' },
                        { id: 'd3', tags: ['beta'] },
                    ]);

                    // Removing 'alpha' from d2 lets d5 re-enter the limit(3) window.
                    const [snap6] = await Promise.all([getNext(), coll.doc('d2').update({ tags: ['gamma'] })]);
                    expect(snap6.map(d => d.id)).toEqual(['d1', 'd3', 'd5']);

                    // Batch write adds d4 and deletes d5.
                    const batch = fs.firestore.batch();
                    batch.set(coll.doc('d4'), writeData({ tags: ['alpha'] }));
                    batch.delete(coll.doc('d5'));
                    const [snap7] = await Promise.all([getNext(), batch.commit()]);
                    expect(snap7.map(d => d.id)).toEqual(['d1', 'd3', 'd4']);

                    // Direct deletion of d1 leaves d3 and d4.
                    const [snap8] = await Promise.all([getNext(), coll.doc('d1').delete()]);
                    expect(snap8.map(d => d.id)).toEqual(['d3', 'd4']);
                } finally {
                    stop();
                }
            },
        );

        test.concurrent('not-in with orderBy and limit(3) tracks additions, updates, batches, and deletions', async () => {
            const coll = subCollection();
            const q = coll.where('status', 'not-in', ['archived', 'deleted']).orderBy('status').limit(3);
            const { stop, getCurrent, getNext } = listen<{ id: string; status: string; v: number }>(q, {
                transform: snap => snap.docs.map(d => ({ id: d.id, ...readData<{ status: string; v: number }>(d.data()) })),
            });
            try {
                expect(await getCurrent()).toEqual([]);

                // Excluded docs (`archived` and missing `status`) do not enter snapshot.
                await Promise.all([
                    coll.doc('skip-arch').create(writeData({ status: 'archived' })),
                    coll.doc('skip-missing').create(writeData({ other: true })),
                ]);

                const [snap1] = await Promise.all([getNext(), coll.doc('s-d').create(writeData({ status: 'd', v: 1 }))]);
                expect(snap1).toEqual([{ id: 's-d', status: 'd', v: 1 }]);

                const [snap2] = await Promise.all([getNext(), coll.doc('s-b').create(writeData({ status: 'b', v: 1 }))]);
                expect(snap2.map(d => d.id)).toEqual(['s-b', 's-d']);

                const [snap3] = await Promise.all([getNext(), coll.doc('s-a').create(writeData({ status: 'a', v: 1 }))]);
                expect(snap3.map(d => d.id)).toEqual(['s-a', 's-b', 's-d']);

                // Adding status 'c' pushes 's-d' out of limit(3).
                const [snap4] = await Promise.all([getNext(), coll.doc('s-c').create(writeData({ status: 'c', v: 1 }))]);
                expect(snap4.map(d => d.id)).toEqual(['s-a', 's-b', 's-c']);

                // In-place update on matching s-b updates field value cleanly.
                const [snap5] = await Promise.all([getNext(), coll.doc('s-b').update({ v: 2 })]);
                expect(snap5.find(d => d.id === 's-b')?.v).toBe(2);

                // Updating s-b to 'deleted' removes it and admits s-d back into window.
                const [snap6] = await Promise.all([getNext(), coll.doc('s-b').update({ status: 'deleted' })]);
                expect(snap6.map(d => d.id)).toEqual(['s-a', 's-c', 's-d']);

                // Batch adds 'e' and deletes s-a.
                const batch = fs.firestore.batch();
                batch.set(coll.doc('s-e'), writeData({ status: 'e', v: 1 }));
                batch.delete(coll.doc('s-a'));
                const [snap7] = await Promise.all([getNext(), batch.commit()]);
                expect(snap7.map(d => d.id)).toEqual(['s-c', 's-d', 's-e']);
            } finally {
                stop();
            }
        });
    });

    function subCollection(coll = fs.collection) {
        return coll.doc().collection('collection');
    }
});

function listen<T = DocumentData>(query: Query, options?: { transform?: (snap: QuerySnapshot) => T[] }) {
    const snapshot$ = fromEventPattern<QuerySnapshot>(value$ =>
        query.onSnapshot(
            snapshot => value$.set(snapshot),
            err => value$.setError(err),
        ),
    );

    const document$ = snapshot$.map(snap => {
        if (options?.transform) {
            return options.transform(snap);
        }
        return snap.docs
            .map(doc => readData(doc.data()))
            .map(({ included, ...rest }) => {
                expect(included).toBeTrue();
                return omit(rest, ['number']) as T;
            });
    });
    const queryResults: T[][] = [];
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
async function update(docs: DocumentReference[], data: DocumentData) {
    return Promise.all(docs.map(ref => ref.update(data)));
}
