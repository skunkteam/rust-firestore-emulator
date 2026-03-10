import {
    AggregateField,
    CollectionReference,
    DocumentData,
    DocumentReference,
    FieldPath,
    Firestore,
    FirestoreDataConverter,
    Query,
    QuerySnapshot,
    Timestamp,
} from '@google-cloud/firestore';
import assert from 'assert';
import { connection, editions, readData, writeData } from './utils';

describe.each(editions)('$description - query with allDescendants', fs => {
    test.concurrent.each([
        // unique root document for concurrent tests
        { description: 'doc as root', ref: fs.collection.doc() },
        { description: 'collection as root', ref: fs.collection.doc().collection('empty') },
    ])('with 0 results ($description)', async ({ ref }) => {
        // query all descendant documents from sub-collection of root-document for this test
        const snap = await allDescendants(ref).get();
        expectQuerySnap(snap, []);
    });

    describe.each(['with document as root', 'with collection as root'] as const)('with intermediate phantom document (%s)', desc => {
        // create documents and collections
        // --------------------------------
        // rootPhantomDoc
        //   \ level1 (collection)
        //     \ phantomDoc
        //       \ level2 (collection)
        //         \ nestedDoc (with contents)
        async function createData() {
            const rootPhantomDoc = fs.collection.doc();
            const rootPhantomCol = rootPhantomDoc.collection('level1');
            const phantomDoc = rootPhantomCol.doc();
            const nestedDoc = phantomDoc.collection('level2').doc();
            await nestedDoc.create(writeData({ which: 'nested' }));
            return { root: desc === 'with document as root' ? rootPhantomDoc : rootPhantomCol, nestedDoc };
        }

        test.concurrent('query all descendant documents', async () => {
            const { root, nestedDoc } = await createData();
            expectQuerySnap(await allDescendants(root).get(), [{ ref: nestedDoc, data: { which: 'nested' } }]);
        });

        test.concurrent('query descendant documents with restriction on collectionId', async () => {
            const { root, nestedDoc } = await createData();
            expectQuerySnap(await allDescendants(root, 'level2').get(), [{ ref: nestedDoc, data: { which: 'nested' } }]);
        });

        test.concurrent('query descendant documents with (failing) restriction on collectionId', async () => {
            const { root } = await createData();
            expectQuerySnap(await allDescendants(root, 'level1').get(), []);
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
            await child.create(writeData({ which: 'child', value: 1 }));
            const grandchild = child.collection('children').doc();
            await grandchild.create(writeData({ which: 'grandchild', value: 3 }));
            const sibling = rootPhantomDoc.collection('siblings').doc();
            await sibling.create(writeData({ which: 'sibling', value: 5 }));
            const nibling = sibling.collection('children').doc();
            await nibling.create(writeData({ which: 'nibling', value: 7 }));
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
                if (connection === 'RUST EMULATOR' || fs.enterprise) {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    return expect((await querySnap).data()).toEqual({ avg: type === 'without' ? 16 / 4 : 11 / 3 });
                }
                await expect(querySnap).rejects.toThrow(
                    connection === 'JAVA EMULATOR'
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
                if (connection === 'RUST EMULATOR' || fs.enterprise) {
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
                switch (connection) {
                    case 'RUST EMULATOR':
                        await expect(explained).rejects.toThrow('12 UNIMPLEMENTED: explain_options is not supported yet');
                        break;
                    case 'JAVA EMULATOR':
                        await expect(explained).rejects.toThrow('No explain results');
                        break;
                    case 'CLOUD FIRESTORE':
                        if (fs.enterprise) {
                            await expect(explained).rejects.toThrow(
                                '3 INVALID_ARGUMENT: Explain options are not supported in RunQuery API for Enterprise edition. Please use the ExecutePipeline API instead.',
                            );
                        } else {
                            expect((await explained).metrics).toEqual({
                                executionStats: null,
                                planSummary: {
                                    indexesUsed: [{ properties: '(__name__ ASC)', query_scope: 'Collection group' }],
                                },
                            });
                        }
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
                if (connection === 'RUST EMULATOR' || fs.enterprise) {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    expectQuerySnap(await querySnap, [nibling]);
                    return;
                }
                await expect(querySnap).rejects.toThrow(
                    type === 'without'
                        ? '3 INVALID_ARGUMENT: kind is required for all orders except __key__ ascending'
                        : // Throws error suggesting a collectionGroup index for `children` on `__name__` (descending) even when the index exists.
                          connection === 'JAVA EMULATOR'
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
                const querySnap = new Promise<QuerySnapshot>((resolve, reject) => query.onSnapshot(resolve, reject));
                if (connection === 'JAVA EMULATOR' || connection === 'RUST EMULATOR') {
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
                if (connection === 'RUST EMULATOR' || fs.enterprise) {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    expectQuerySnap(await querySnap, [nibling, ...(type === 'without' ? [sibling] : []), grandchild, child]);
                    return;
                }
                await expect(querySnap).rejects.toThrow(
                    type === 'without'
                        ? '3 INVALID_ARGUMENT: kind is required for all orders except __key__ ascending'
                        : // Throws error suggesting a collectionGroup index for `children` on `__name__` (descending) even when the index exists.
                          connection === 'JAVA EMULATOR'
                          ? `9 FAILED_PRECONDITION: collection group queries are only allowed if they're at the root or they're an ascending primary key scan`
                          : '9 FAILED_PRECONDITION: The query requires an index.',
                );
            });

            test.concurrent('orderBy field', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.orderBy('value').get();
                if (connection === 'RUST EMULATOR' || fs.enterprise) {
                    // rust emulator does not care about indices and does not recognize this to be unsupported (yet)
                    expectQuerySnap(await querySnap, [child, grandchild, ...(type === 'without' ? [sibling] : []), nibling]);
                    return;
                }
                await expect(querySnap).rejects.toThrow('3 INVALID_ARGUMENT: collection group queries are only allowed at the root parent');
            });

            test.concurrent('select', async () => {
                const { query, child, grandchild, sibling, nibling } = await prepareQuery();
                const querySnap = query.select('which', 'ttl').get();
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
                if (connection === 'RUST EMULATOR' || fs.enterprise) {
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
                    const converter: FirestoreDataConverter<string, DocumentData> = {
                        fromFirestore: snap => JSON.stringify(readData(snap.data())),
                        toFirestore: v => JSON.parse(v) as DocumentData,
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

    function expectQuerySnap(querySnap: QuerySnapshot, expected: MappedQueryDocumentSnapshot[]) {
        expect(querySnap.readTime).toBeInstanceOf(Timestamp);
        fs.enterprise
            ? expect(toPathsAndData(querySnap)).toIncludeSameMembers(expected.map(e => ({ path: e.ref.path, data: e.data })))
            : expect(toPathsAndData(querySnap)).toStrictEqual(expected.map(e => ({ path: e.ref.path, data: e.data })));
    }
});

function toPathsAndData(querySnap: QuerySnapshot) {
    return querySnap.docs.map(d => ({ path: d.ref.path, data: readData(d.data()) }));
}

type MappedQueryDocumentSnapshot = { ref: DocumentReference; data: DocumentData };

/**
 * Builds a `Query` that queries all recursively descendant documents from a given document. When `collectionGroup` is given it only returns
 * documents where its immediate parent collection has this name. Please note that this parent collection does not have to be a direct child
 * of the given `DocumentReference` since the query is recursive.
 *
 * Descendant documents will be found even if the given `DocumentReference` itself, or any intermediate documents, do not actually exist.
 *
 * The returned query does not support live queries and `onSnapshot` will throw a runtime error
 *
 * When the query was constructed without a `collection` argument, you cannot use `withConverter` on it as that will re-introduce an
 * (internal) predicate on the `collectionId` with a non existing collection.
 */
function allDescendants(root: Firestore | DocumentReference | CollectionReference, collectionGroup?: string): Query {
    // build a query from a fake collection, just to get things started. We overrule the collectionId of the query later on. Note that
    // this piece of code is responsible for setting the correct `parent` on the QueryOptions. This is the parent of the "fake
    // collection", i.e.:
    // - when `root` is the top-level Firestore object: the top-level Firestore object itself
    // - when `root` is a DocumentReference: the document (`root`) itself
    // - when `root` is a CollectionReference: the parent-document of `root`
    let query: Query<DocumentData> =
        'collection' in root ? root.collection('UNUSED') : (root.parent ?? root.firestore).collection('UNUSED');
    assert('_queryOptions' in query, 'Query is missing _queryOptions property');
    // construct a new Query instance with a new QueryOptions instance similar to what Query does internally in the other query
    // building methods.
    // cast Query as its constructor is `protected` and we need a public constructor for Typescript to not complain.
    query = new (Query as any)(
        query.firestore,
        (query._queryOptions as any).with({
            allDescendants: true,
            // `kindless` means without restriction on the name of the direct parent (collection) of the found documents. This ignores
            // the `collectionId` of the query.
            kindless: collectionGroup === undefined,
            // intentionally illegal name that starts and ends with two underscores so using `withConverter` will reintroduce this
            // string to the query and will throw an error
            collectionId: collectionGroup ?? '__converter_not_compatible_with_collection_id_query__',
        }),
    );
    if ('doc' in root && 'path' in root) {
        root satisfies CollectionReference;

        // If `root` is a `CollectionReference`, then the "parent" of the query is set to the parent of `root` (c.q. either the entire
        // database or a specific document). Therefore, all paths from this point on are "rooted" at this parent (including in the where
        // clause that we give to the query object). Our caller asks for a specific collection as root, but that is not possible with
        // the current Firestore API, so we have to account for that here. We need to limit our search to all documents that have the
        // given `root` as parent. Because the parent of `root` is already the parent of the current `query`, we only need to account
        // for the last collection ID in `root`, that is:
        const collectionId = root.id;

        // We want to restrict the query to return only documents at `<collectionId>/**/*`. So, we need a `where` clause that selects
        // only the documents with a document name that starts with `<collectionId>/`. Unfortunately we cannot filter based on the
        // document name starting with `<collectionId>/`. We can only use greater than, greater than equals, smaller than, etc. Lucky
        // for us, it is possible to simulate a "starts with" operation using one `>=` and one `<` operator, i.e. by querying a range of
        // values. To explain the used values in that range we need to look at lexicographical ordering first (which is the ordering
        // that is used in the database to compare strings).
        //
        // With lexicographical ordering, everything that is longer than a certain string A, but also starts with that string A, comes
        // after that string A (in a dictionary for example). Take the word `cat`. It will always be the first of all the words that
        // start with `cat`. The following words are ordered lexicographically:
        // - case
        // - cat
        // - catnip
        // - cats
        // - cause
        //
        // So all strings that start with A are "equal to or larger than" the string A itself. This also means that we are able to find
        // the theoretical "next word", i.e. the word that should always come strictly after A itself, nothing can be in between. That
        // theoretical next word is, in the case of `cat`: `cat` + <the lowest possible letter in the alphabet>. Let's limit ourselves
        // to letters of the English alphabet, in which case the first letter is `a`. So the theoretical next word after `cat` is
        // `cata`.
        //
        // We can do the same with collection IDs. In this case the alphabet is not limited to letters of the English alphabet, but to
        // the ASCII character space, so the first character is `\0` (the null byte). This gives us the following theoretical next
        // collection ID:
        const theoreticalNextCollectionId = `${collectionId}\0`;

        // Example: if `collectionId` is 'my-collection', then `theoreticalNextCollectionId` is 'my-collection\0'. This means that there
        // cannot be a collection ID between `collectionId` and `theoreticalNextCollectionId`. So, theoretically, we could use the
        // following WHERE clause: `FieldPath.collectionId() >= collectionId && FieldPath.collectionId() < theoreticalNextCollectionId`.

        // There is one additional complication. `FieldPath.collectionId()` does not exist. We are not allowed to use a collection ID
        // (such as `collectionId` or `theoreticalNextCollectionId`) in a WHERE clause that filters document paths. We *must* use
        // document paths. So we will add an additional component to each collection ID so that it becomes a document path. We will use
        // the null byte trick again to find the theoretical first document ID within the given collection ID.
        let fromDocumentPath = `${collectionId}/\0`;
        let toDocumentPath = `${theoreticalNextCollectionId}/\0`;

        // So now we can use the following WHERE clause: `FieldPath.documentId() >= fromDocumentPath && FieldPath.documentId() <
        // toDocumentPath`.
        query = query.where(FieldPath.documentId(), '>=', fromDocumentPath).where(FieldPath.documentId(), '<', toDocumentPath);
    }
    return query;
}
