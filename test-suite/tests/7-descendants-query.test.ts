import { AggregateField, FieldPath, FirestoreDataConverter, Query, Timestamp } from '@google-cloud/firestore';
import { QueryOptions } from '@google-cloud/firestore/build/src/reference/query-options';
import assert from 'assert';
import { fs } from './utils';

describe('query with allDescendants', () => {
    test.concurrent('with 0 results', async () => {
        // unique root document for concurrent tests
        const rootPhantomDoc = fs.collection.doc();
        // query all descendant documents from sub-collection of root-document for this test
        const snap = await allDescendants(rootPhantomDoc).get();
        expectQuerySnap(snap, []);
    });

    describe('with intermediate phantom document', () => {
        // create documents and collections
        // --------------------------------
        // rootPhantomDoc
        //   \ level1 (collection)
        //     \ phantomDoc
        //       \ level2 (collection)
        //         \ nestedDoc (with contents)
        async function createData() {
            const rootPhantomDoc = fs.collection.doc();
            const phantomDoc = rootPhantomDoc.collection('level1').doc();
            const nestedDoc = phantomDoc.collection('level2').doc();
            await nestedDoc.create({ which: 'nested' });
            return { rootPhantomDoc, nestedDoc };
        }

        test.concurrent('query all descendant documents', async () => {
            const { rootPhantomDoc, nestedDoc } = await createData();
            expectQuerySnap(await allDescendants(rootPhantomDoc).get(), [{ ref: nestedDoc, data: { which: 'nested' } }]);
        });

        test.concurrent('query descendant documents with restriction on collectionId', async () => {
            const { rootPhantomDoc, nestedDoc } = await createData();
            expectQuerySnap(await allDescendants(rootPhantomDoc, 'level2').get(), [{ ref: nestedDoc, data: { which: 'nested' } }]);
        });

        test.concurrent('query descendant documents with (failing) restriction on collectionId', async () => {
            const { rootPhantomDoc } = await createData();
            expectQuerySnap(await allDescendants(rootPhantomDoc, 'level1').get(), []);
        });
    });

    describe('with direct and descendant results', () => {
        // create documents and collections
        // --------------------------------
        // rootPhantomDoc
        //   \ children (collection)
        //     \ child
        //       \ children (collection)
        //         \ grandchild
        //   \ siblings (collection)
        //     \ sibling
        //       \ children (collection)
        //         \ nibling

        // Trivia: Nibling is a gender-neutral term used to refer to a child of
        // one's sibling, as a replacement for "niece" or "nephew".
        async function createData() {
            const rootPhantomDoc = fs.collection.doc();
            const child = rootPhantomDoc.collection('children').doc();
            await child.create({ which: 'child', value: 1 });
            const grandchild = child.collection('children').doc();
            await grandchild.create({ which: 'grandchild', value: 3 });
            const sibling = rootPhantomDoc.collection('siblings').doc();
            await sibling.create({ which: 'sibling', value: 5 });
            const nibling = sibling.collection('children').doc();
            await nibling.create({ which: 'nibling', value: 7 });
            return {
                rootPhantomDoc,
                child: { ref: child, data: { which: 'child', value: 1 } },
                grandchild: { ref: grandchild, data: { which: 'grandchild', value: 3 } },
                sibling: { ref: sibling, data: { which: 'sibling', value: 5 } },
                nibling: { ref: nibling, data: { which: 'nibling', value: 7 } },
            };
        }

        describe.each(['without', 'with'] as const)('%s restriction on collectionId', type => {
            async function prepareQuery() {
                const docs = await createData();
                const query = allDescendants(docs.rootPhantomDoc, type === 'with' ? 'children' : undefined);
                return { query, ...docs };
            }

            test.concurrent('query', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.get();
                expectQuerySnap(await querySnap, [child, grandchild, ...(type === 'without' ? [sibling] : []), nibling]);
            });

            test.concurrent('aggregate without orderBy', async () => {
                const { query } = await prepareQuery();
                const querySnap = query.aggregate({ avg: AggregateField.average('value') }).get();
                if (fs.connection === 'RUST EMULATOR') {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    return expect((await querySnap).data()).toEqual({ avg: type === 'without' ? 16 / 4 : 11 / 3 });
                }
                await expect(querySnap).rejects.toThrow(
                    fs.connection === 'JAVA EMULATOR'
                        ? '3 INVALID_ARGUMENT: This query requires an index that has fields [value] after __name__ and Firestore does not currently support such an index. Please try the query again with additional ordering on those fields.'
                        : '3 INVALID_ARGUMENT: This query requires an index with fields [value] after __name__ and Firestore does not currently support such an index. Please try the query again with additional ordering on those fields.',
                );
            });

            test.concurrent('aggregate with orderBy', async () => {
                const { query } = await prepareQuery();
                const querySnap = query
                    .orderBy('value')
                    .aggregate({ avg: AggregateField.average('value') })
                    .get();
                if (fs.connection === 'RUST EMULATOR') {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    return expect((await querySnap).data()).toEqual({ avg: type === 'without' ? 16 / 4 : 11 / 3 });
                }
                await expect(querySnap).rejects.toThrow('3 INVALID_ARGUMENT: collection group queries are only allowed at the root parent');
            });

            test.concurrent('count', async () => {
                const { query } = await prepareQuery();
                const querySnap = query.count().get();
                expect((await querySnap).data()).toEqual({ count: type === 'without' ? 4 : 3 });
            });

            test.concurrent('endAt', async () => {
                const { query, child, grandchild } = await prepareQuery();
                const querySnap = query.endAt(await grandchild.ref.get()).get();
                expectQuerySnap(await querySnap, [child, grandchild]);
            });

            test.concurrent('explain', async () => {
                const { query } = await prepareQuery();
                const explained = query.explain();
                switch (fs.connection) {
                    case 'RUST EMULATOR':
                        await expect(explained).rejects.toThrow('12 UNIMPLEMENTED: explain_options is not supported yet');
                        break;
                    case 'JAVA EMULATOR':
                        await expect(explained).rejects.toThrow('No explain results');
                        break;
                    case 'CLOUD FIRESTORE':
                        expect((await explained).metrics).toEqual({
                            executionStats: null,
                            planSummary: {
                                indexesUsed: [{ properties: '(__name__ ASC)', query_scope: 'Collection group' }],
                            },
                        });
                }
            });

            test.concurrent('limit', async () => {
                const { query, child } = await prepareQuery();
                const querySnap = query.limit(1).get();
                expectQuerySnap(await querySnap, [child]);
            });

            test.concurrent('limitToLast', async () => {
                const { query, nibling } = await prepareQuery();
                const querySnap = query.orderBy(FieldPath.documentId()).limitToLast(1).get();
                if (fs.connection === 'RUST EMULATOR') {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    expectQuerySnap(await querySnap, [nibling]);
                    return;
                }
                await expect(querySnap).rejects.toThrow(
                    type === 'without'
                        ? '3 INVALID_ARGUMENT: kind is required for all orders except __key__ ascending'
                        : // Throws error suggesting a collectionGroup index for `children` on `__name__` (descending) even when the index exists.
                          fs.connection === 'JAVA EMULATOR'
                          ? `9 FAILED_PRECONDITION: collection group queries are only allowed if they're at the root or they're an ascending primary key scan`
                          : '9 FAILED_PRECONDITION: The query requires an index.',
                );
            });

            test.concurrent('offset', async () => {
                const { query, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.offset(1).get();
                expectQuerySnap(await querySnap, [grandchild, ...(type === 'without' ? [sibling] : []), nibling]);
            });

            test.concurrent('onSnapshot', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = new Promise<FirebaseFirestore.QuerySnapshot>((resolve, reject) => query.onSnapshot(resolve, reject));
                if (fs.connection === 'JAVA EMULATOR' || fs.connection === 'RUST EMULATOR') {
                    expectQuerySnap(await querySnap, [child, grandchild, ...(type === 'without' ? [sibling] : []), nibling]);
                    return;
                }
                await expect(querySnap).rejects.toThrow(
                    type === 'without'
                        ? 'Error 3: All-collection queries are not supported by real-time queries.'
                        : `Error 3: 'all_descendants' in CollectionSelector with document parent is not supported by real-time queries.`,
                );
            });

            test.concurrent('orderBy document name descending', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.orderBy(FieldPath.documentId(), 'desc').get();
                if (fs.connection === 'RUST EMULATOR') {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    expectQuerySnap(await querySnap, [nibling, ...(type === 'without' ? [sibling] : []), grandchild, child]);
                    return;
                }
                await expect(querySnap).rejects.toThrow(
                    type === 'without'
                        ? '3 INVALID_ARGUMENT: kind is required for all orders except __key__ ascending'
                        : // Throws error suggesting a collectionGroup index for `children` on `__name__` (descending) even when the index exists.
                          fs.connection === 'JAVA EMULATOR'
                          ? `9 FAILED_PRECONDITION: collection group queries are only allowed if they're at the root or they're an ascending primary key scan`
                          : '9 FAILED_PRECONDITION: The query requires an index.',
                );
            });

            test.concurrent('orderBy field', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.orderBy('value').get();
                if (fs.connection === 'RUST EMULATOR') {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    expectQuerySnap(await querySnap, [child, grandchild, ...(type === 'without' ? [sibling] : []), nibling]);
                    return;
                }
                await expect(querySnap).rejects.toThrow('3 INVALID_ARGUMENT: collection group queries are only allowed at the root parent');
            });

            test.concurrent('select', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.select('which').get();
                const expectedDocs = [child, grandchild, ...(type === 'without' ? [sibling] : []), nibling];
                expectQuerySnap(
                    await querySnap,
                    expectedDocs.map(e => ({ ref: e.ref, data: { which: e.data.which } })),
                );
            });

            test.concurrent('startAfter', async () => {
                const { query, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.startAfter(await grandchild.ref.get()).get();
                expectQuerySnap(await querySnap, [...(type === 'without' ? [sibling] : []), nibling]);
            });

            test.concurrent('startAt', async () => {
                const { query, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.startAt(await grandchild.ref.get()).get();
                expectQuerySnap(await querySnap, [grandchild, ...(type === 'without' ? [sibling] : []), nibling]);
            });

            test.concurrent('where document name', async () => {
                const { query, rootPhantomDoc, grandchild } = await prepareQuery();
                // value for `__name__` is relative to root of the query (`rootPhantomDoc`)
                const querySnap = query
                    .where(FieldPath.documentId(), '==', grandchild.ref.path.slice(rootPhantomDoc.path.length + 1))
                    .get();
                expectQuerySnap(await querySnap, [grandchild]);
            });

            test.concurrent('where field', async () => {
                const { query, grandchild } = await prepareQuery();
                const querySnap = query.where('value', '==', 3).get();
                if (fs.connection === 'RUST EMULATOR') {
                    expectQuerySnap(await querySnap, [grandchild]);
                    return;
                }
                await expect(querySnap).rejects.toThrow('3 INVALID_ARGUMENT: collection group queries are only allowed at the root parent');
            });

            // withConverter strips the `kindless` and thus re-introduces a collectionId filter with a fake name, so we skip this test
            // when type === 'without'
            if (type === 'with') {
                test.concurrent('withConverter', async () => {
                    const { query } = await prepareQuery();
                    const converter: FirestoreDataConverter<string, FirebaseFirestore.DocumentData> = {
                        fromFirestore: snap => JSON.stringify(snap.data()),
                        toFirestore: v => JSON.parse(v) as FirebaseFirestore.DocumentData,
                    };
                    const querySnap = await query.withConverter(converter).get();
                    // explicit typescript type so we would get a compile error if `data()` does not promise to return a `string`
                    querySnap.docs[0].data() satisfies string;
                    expect(querySnap.docs.map(d => JSON.parse(d.data()) as unknown)).toEqual([
                        { value: 1, which: 'child' },
                        { value: 3, which: 'grandchild' },
                        { value: 7, which: 'nibling' },
                    ]);
                });
            }
        });
    });
});

function expectQuerySnap(querySnap: FirebaseFirestore.QuerySnapshot, expected: MappedQueryDocumentSnapshot[]) {
    expect(querySnap.readTime).toBeInstanceOf(Timestamp);
    expect(toPathsAndData(querySnap)).toStrictEqual(expected.map(e => ({ path: e.ref.path, data: e.data })));
}

function toPathsAndData(querySnap: FirebaseFirestore.QuerySnapshot) {
    return querySnap.docs.map(d => ({ path: d.ref.path, data: d.data() }));
}

type MappedQueryDocumentSnapshot = { ref: FirebaseFirestore.DocumentReference; data: FirebaseFirestore.DocumentData };

/**
 * Builds a `Query` that queries all recursively descendant documents from a given document. When `collection` is given it only returns
 * documents where its immediate parent collection has this name. Please note that this parent collection does not have to be a direct child
 * of the given `DocumentReference` since the query is recursive.
 *
 * Descendant documents will be found even if the given `DocumentReference` itself, or any intermediate documents, do not actually exist.
 *
 * The returned query does not support live queries and `onSnapshot` will throw a runtime error
 *
 * When the query was constructed with a `collection` argument, you cannot use `withConverter` on it as that will re-introduce an (internal)
 * predicate on the `collectionId` with a non existing collection.
 */
function allDescendants(parent: FirebaseFirestore.DocumentReference, collection?: string) {
    // determine if this will be a "kindless" query meaning without restriction on the name of the direct parent (collection) of the found
    // documents.
    const kindless = collection === undefined;
    // build a query from the document reference and optionally restrict the name of the collection owning the found document(s)
    const query = parent.collection(kindless ? 'unused' : collection);
    assert('_queryOptions' in query, 'Firestore query always has private _queryOptions');
    // cast Query as its constructor is `protected` and we need a public constructor for Typescript to not complain.
    const PublicQuery = Query as unknown as {
        new (
            firestore: FirebaseFirestore.Firestore,
            options: QueryOptions<unknown, FirebaseFirestore.DocumentData>,
        ): FirebaseFirestore.Query;
    };
    // construct a new Query instance with a new QueryOptions instance similar to what Query does internally in the other query building
    // methods.
    return new PublicQuery(
        query.firestore,
        (query._queryOptions as QueryOptions<unknown, FirebaseFirestore.DocumentData>).with({
            allDescendants: true,
            kindless,
        }),
    );
}
