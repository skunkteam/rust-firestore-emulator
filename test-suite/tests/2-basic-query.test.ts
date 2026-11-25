import {
    AggregateField,
    AggregateQuery,
    AggregateSpec,
    DocumentData,
    DocumentReference,
    FieldPath,
    FieldValue,
    Filter,
    GeoPoint,
    Query,
    QueryDocumentSnapshot,
    Timestamp,
} from '@google-cloud/firestore';
import { differenceWith, isEqual, omit, orderBy, sortedUniq, uniq } from 'lodash';
import {
    RawDocument,
    RawProtoValue,
    callRawStreamRpc,
    callRawUnaryRpc,
    connection,
    editions,
    expectRpcCode,
    readData,
    toRunQueryProto,
    writeData,
} from './utils';

interface RawListDocumentsRequest {
    parent: string;
    collectionId?: string;
    pageSize?: number;
    pageToken?: string;
    orderBy?: string;
    mask?: { fieldPaths: string[] };
    showMissing?: boolean;
    readTime?: Timestamp;
}

interface RawListDocument extends RawDocument {
    createTime?: unknown;
    updateTime?: unknown;
}

interface RawListDocumentsResponse {
    documents?: RawListDocument[];
    nextPageToken?: string;
}

interface RawListCollectionIdsRequest {
    parent: string;
    pageSize?: number;
    pageToken?: string;
}

interface RawListCollectionIdsResponse {
    collectionIds?: string[];
    nextPageToken?: string;
}

interface InternalListDocumentsClient {
    initializeIfNeeded(requestTag: string): Promise<void>;
    createCallOptions(methodName: string): Record<string, unknown>;
    _clientPool: {
        run<T>(
            requestTag: string,
            requiresGrpc: boolean,
            op: (gapicClient: {
                listDocuments(
                    req: RawListDocumentsRequest,
                    options: Record<string, unknown>,
                ): Promise<[RawListDocument[], RawListDocumentsRequest | null, RawListDocumentsResponse]>;
                listCollectionIds(
                    req: RawListCollectionIdsRequest,
                    options: Record<string, unknown>,
                ): Promise<[string[], RawListCollectionIdsRequest | null, RawListCollectionIdsResponse]>;
            }) => Promise<T>,
        ): Promise<T>;
    };
}

async function listDocumentsPage(
    firestore: (typeof editions)[number]['firestore'],
    req: RawListDocumentsRequest,
    tag = 'listDocumentsPage',
): Promise<{ documents: RawListDocument[]; nextPageToken: string }> {
    const client = firestore as unknown as InternalListDocumentsClient;
    await client.initializeIfNeeded(tag);
    const callOptions = client.createCallOptions('listDocuments');
    const [documents, , rawResponse] = await client._clientPool.run(tag, false, gapicClient =>
        gapicClient.listDocuments(req, { ...callOptions, autoPaginate: false }),
    );
    return {
        documents: rawResponse.documents ?? documents ?? [],
        nextPageToken: rawResponse.nextPageToken ?? '',
    };
}

async function listCollectionIdsPage(
    firestore: (typeof editions)[number]['firestore'],
    req: RawListCollectionIdsRequest,
    tag = 'listCollectionIdsPage',
): Promise<{ collectionIds: string[]; nextPageToken: string }> {
    const client = firestore as unknown as InternalListDocumentsClient;
    await client.initializeIfNeeded(tag);
    const callOptions = client.createCallOptions('listCollectionIds');
    const [collectionIds, , rawResponse] = await client._clientPool.run(tag, false, gapicClient =>
        gapicClient.listCollectionIds(req, { ...callOptions, autoPaginate: false }),
    );
    return {
        collectionIds: rawResponse.collectionIds ?? collectionIds ?? [],
        nextPageToken: rawResponse.nextPageToken ?? '',
    };
}

function docIdsFromRaw(docs: RawDocument[]): string[] {
    return docs.map(d => {
        const name = d.name ?? '';
        return name.slice(name.lastIndexOf('/') + 1);
    });
}

describe.each(editions)('$description', fs => {
    interface Data {
        type: string;
        ordered?: unknown;
    }
    // Everything is defined in order, to make testing easier below
    // Official ordering according to https://firebase.google.com/docs/firestore/manage-data/data-types:
    //  1. Null values
    //  2. Boolean values
    //  3. NaN values
    //  4. Integer and floating-point values, sorted in numerical order
    //  5. Date values
    //  6. Text string values
    //  7. Byte values
    //  8. Cloud Firestore references
    //  9. Geographical point values (Note from Google: At this time we do not recommend using this data type) NOT TESTED
    // 10. Array values
    // 11. Map values
    const numbers: Data[] = [
        { type: 'number', ordered: -500 },
        { type: 'number', ordered: Number.MIN_VALUE },
        { type: 'number', ordered: 2 },
        { type: 'number', ordered: 2.1 },
        { type: 'number', ordered: Number.MAX_VALUE },
    ];
    // Will be created in two steps (0.5 plus 0.5), because JavaScript doesn't know the difference between integers and doubles.
    const integerStoredAsDouble = { type: 'number', ordered: 1 } satisfies Data;
    numbers.splice(2, 0, integerStoredAsDouble);

    const nanType: Data = { type: 'NaN', ordered: NaN };
    const nullType: Data = { type: 'null', ordered: null };
    const booleans: Data[] = [
        { type: 'boolean', ordered: false },
        { type: 'boolean', ordered: true },
    ];
    const dates: Data[] = [
        { type: 'date', ordered: Timestamp.fromDate(new Date('2022-01-01')) },
        { type: 'date', ordered: Timestamp.fromDate(new Date('2023-01-01')) },
    ];
    const strings = [
        // Should not order above any Date/Number
        { type: 'string', ordered: '1' },
        // Capitals first
        { type: 'string', ordered: 'A' },
        { type: 'string', ordered: 'B' },
        { type: 'string', ordered: 'a' },
        { type: 'string', ordered: 'b' },
        // Special chars last
        { type: 'string', ordered: 'Æ' },
    ] satisfies Data[];
    const bytes: Data[] = strings.map(({ ordered }) => ({ type: 'bytes', ordered: Buffer.from(ordered, 'utf-8') }));
    const storedReferences: Data[] = [
        // Firestore Enterprise edition does not support the special `__id...__` syntax.
        ...(fs.enterprise ? [] : [{ type: 'ref', ordered: fs.firestore.doc('cola/__id-9223372036854775808__') }]),
        { type: 'ref', ordered: fs.firestore.doc('cola/\0') },
        { type: 'ref', ordered: fs.firestore.doc('cola/_') },
        { type: 'ref', ordered: fs.firestore.doc('cola\0/doca') },
        ...(fs.enterprise ? [] : [{ type: 'ref', ordered: fs.firestore.doc('cola\0/doca/colb/__id-9223372036854775808__') }]),
        { type: 'ref', ordered: fs.firestore.doc('cola\0/doca/colb/\0') },
        { type: 'ref', ordered: fs.firestore.doc('cola\0/doca/colb/docb') },
        { type: 'ref', ordered: fs.firestore.doc('cola\0/doca\0') },
        { type: 'ref', ordered: fs.firestore.doc('cola\0/docb') },
        { type: 'ref', ordered: fs.firestore.doc('colb/doca') },
    ];
    const references = storedReferences.map(sanitizeData);
    const arrays: Data[] = [
        { type: 'array', ordered: [1, 2, 3] },
        { type: 'array', ordered: [1, 2, 3, 1] },
        { type: 'array', ordered: [2] },
    ];
    const maps: Data[] = [
        { type: 'map', ordered: { a: 'aaa', b: 'baz' } },
        { type: 'map', ordered: { a: 'foo', b: 'bar' } },
        { type: 'map', ordered: { a: 'foo', b: 'bar', c: 'qux' } },
        { type: 'map', ordered: { a: 'foo', b: 'baz' } },
        { type: 'map', ordered: { b: 'aaa', c: 'baz' } },
        { type: 'map', ordered: { c: 'aaa' } },
    ];

    // `nothing` does not have an `ordered` field and will not be included if filtered/ordered by that field
    const nothing: Data = { type: 'none' };
    // All test data, ordered by the `ordered` field according to https://firebase.google.com/docs/firestore/manage-data/data-types
    const storedTestData = [
        nullType,
        ...booleans,
        nanType,
        ...numbers,
        ...dates,
        ...strings,
        ...bytes,
        ...storedReferences,
        ...arrays,
        ...maps,
        nothing,
    ];
    // Since the Document References in `storedTestData` are tough on `Jest`, this is a copy of that data with the References sanitized
    const testData = storedTestData.map(sanitizeData);

    const GROUPED_COLLECTION = 'subCollection';
    const collectionGroupQuery = fs.firestore.collectionGroup(GROUPED_COLLECTION);

    describe.each([
        {
            description: 'collection',
            collection: fs.collection,
            createRef: () => fs.collection.doc(),
            orderByDocId: fs.collection.orderBy(FieldPath.documentId()),
            getDocId: (snap: QueryDocumentSnapshot) => snap.id,
        },
        {
            // Note that these tests need custom collection group indexes in Firestore Standard edition
            description: 'collection group',
            collection: collectionGroupQuery,
            createRef: () => fs.collection.doc().collection(GROUPED_COLLECTION).doc(),
            orderByDocId: collectionGroupQuery.orderBy('path'),
            getDocId: (snap: QueryDocumentSnapshot) => snap.ref.path,
        },
    ] as const)('on $description', ({ description, collection, createRef, orderByDocId, getDocId }) => {
        beforeAll(async () => {
            await Promise.all(
                storedTestData.map(async data => {
                    const ref = createRef();
                    // Also store path for collection group queries
                    const path = ref.path;
                    if (data === integerStoredAsDouble) {
                        await ref.set(writeData({ ...data, path, ordered: integerStoredAsDouble.ordered - 0.5 }));
                        await ref.update({ ordered: FieldValue.increment(0.5) });
                    } else {
                        await ref.set(writeData({ ...data, path }));
                    }
                }),
            );
        });

        test('get whole collection', async () => {
            expect(await getData(collection)).toIncludeSameMembers(testData);
        });

        test('basic equality', async () => {
            expect(await getData(collection.where('type', '==', 'date'))).toIncludeSameMembers(dates);
            expect(await getData(collection.where('type', '==', 'number'))).toIncludeSameMembers(numbers);
        });

        describe('ordering', () => {
            test('basic string order', async () => {
                const result = await getData(collection.orderBy('type'));
                expect(result.map(r => r.type)).toEqual(testData.map(r => r.type).sort());
            });

            test('ordering all data', async () => {
                // Note that `nothing` is not present here! Because it has no `ordered` property.
                const ordered = without(testData, nothing);
                expect(await getData(collection.orderBy('ordered'))).toEqual(ordered);
                expect(await getData(collection.orderBy('ordered', 'asc'))).toEqual(ordered);
                expect(await getData(collection.orderBy('ordered', 'desc'))).toEqual(ordered.toReversed());
            });

            // Note: This needs an index in the Cloud Firestore
            test(`multiple orderBy's`, async () => {
                expect(await getData(collection.orderBy('type').orderBy('ordered', 'desc'))).toIncludeSameMembers(
                    // Note that this still skips `nothing`, because of the missing `ordered` property.
                    orderBy(without(testData, nothing), ['type', 'ordered'], ['asc', 'desc']),
                );
            });
        });

        describe('inequality filter', () => {
            describe('basic string inequality', () => {
                test.each([
                    // Note: the capital 'N' of 'NaN' in nanType comes before all the lower case characters
                    { operator: '<=', compareTo: 'date', expected: [nanType, ...arrays, ...booleans, ...bytes, ...dates] },
                    { operator: '<', compareTo: 'e', expected: [nanType, ...arrays, ...booleans, ...bytes, ...dates] },
                    { operator: '>=', compareTo: 'number', expected: [...numbers, ...references, ...strings] },
                    { operator: '>', compareTo: 'n', expected: [nothing, nullType, ...numbers, ...references, ...strings] },
                    { operator: '!=', compareTo: 'string', expected: without(testData, ...strings) },
                ] as const)("`type` $operator '$compareTo'", async ({ operator, compareTo, expected }) => {
                    // Note that we filter on the `type` (string) field! Not the actual type of anything.
                    expect(await getData(collection.where('type', operator, compareTo))).toIncludeSameMembers(expected);
                });
            });

            fs.enterprise
                ? test('will not implicitly order by the compared field', async () => {
                      const result = await getData(collection.where('type', '>', 'A')); // should be everything

                      expect(result.map(r => r.type)).toIncludeSameMembers(testData.map(r => r.type));
                      expect(result.map(r => r.type)).not.toEqual(testData.map(r => r.type).sort());

                      // less than or greater than should have the same implicit ordering
                      const secondResult = await getData(collection.where('type', '<', 'z')); // should be everything
                      expect(secondResult).toEqual(result);
                  })
                : test('will implicitly order by the compared field', async () => {
                      const result = await getData(collection.where('type', '>', 'A')); // should be everything

                      expect(result.map(r => r.type)).toEqual(testData.map(r => r.type).sort());

                      // less than or greater than should have the same implicit ordering
                      const secondResult = await getData(collection.where('type', '<', 'z')); // should be everything
                      expect(secondResult).toEqual(result);
                  });

            test('can apply arbitrary order-by clauses (that also filter on existence)', async () => {
                const allResults = await getData(collection.where('type', '>', 'A'));
                const allResultsExplicitOrderBy = await getData(collection.where('type', '>', 'A').orderBy('type'));
                const allResultsDifferentOrderBy = await getData(collection.where('type', '>', 'A').orderBy('ordered'));
                const allResultsDoubleOrderBy = await getData(collection.where('type', '>', 'A').orderBy('type').orderBy('ordered'));
                const allResultsDifferentDoubleOrderBy = await getData(
                    collection.where('type', '>', 'A').orderBy('ordered').orderBy('type'),
                );

                // implicit ordering by the inequality field if not otherwise specified in case of
                // Standard Edition:
                expect(allResults).toIncludeSameMembers(allResultsExplicitOrderBy);
                fs.enterprise
                    ? expect(allResults).not.toEqual(allResultsExplicitOrderBy)
                    : expect(allResults).toStrictEqual(allResultsExplicitOrderBy);

                // One result is missing from the last two queries, because one specific value has no `ordered` field.
                expect(allResultsDifferentOrderBy).toIncludeSameMembers(without(allResults, nothing));
                expect(allResultsDoubleOrderBy).toIncludeSameMembers(without(allResults, nothing));

                // The extra ordering on the field `ordered` should have an effect on the resulting data (using another index)
                expect(allResultsDoubleOrderBy).not.toStrictEqual(without(allResults, nothing));

                // no implicit ordering, both should be the same, because the additional ordering on `type` has no effect after the ordering on
                // the field `ordered`.
                expect(allResultsDifferentOrderBy).toStrictEqual(allResultsDifferentDoubleOrderBy);

                // Ordering on field `type` (ordering on the string value):
                expect(sortedUniq(allResultsExplicitOrderBy.map(r => r.type))).toEqual([
                    'NaN',
                    'array',
                    'boolean',
                    'bytes',
                    'date',
                    'map',
                    'none',
                    'null',
                    'number',
                    'ref',
                    'string',
                ]);

                // Ordering on the defined natural ordering between data-types:
                expect(sortedUniq(allResultsDifferentOrderBy.map(r => r.type))).toEqual([
                    'null',
                    'boolean',
                    'NaN',
                    'number',
                    'date',
                    'string',
                    'bytes',
                    'ref',
                    'array',
                    'map',
                ]);
            });

            describe('implicitly filter by Type', () => {
                test.each([
                    { type: 'number', data: numbers }, // compare using integer
                    { type: 'number', data: numbers, whereClauseValue: -499.5 }, // compare using double
                    { type: 'string', data: strings },
                    { type: 'bytes', data: bytes },
                    { type: 'reference', data: storedReferences },
                    { type: 'boolean', data: booleans },
                    { type: 'date', data: dates },
                    { type: 'array', data: arrays },
                    { type: 'map', data: maps },
                ] as const)('$type', async ({ data, whereClauseValue = data[0].ordered }) => {
                    const saneData = data.map(sanitizeData);
                    const result1 = await getData(collection.where('ordered', '>', whereClauseValue));
                    const expected1 = saneData.slice(1);
                    const result2 = await getData(collection.where('ordered', '<=', whereClauseValue));
                    const expected2 = saneData.slice(0, 1);
                    if (fs.enterprise) {
                        expect(result1).toIncludeSameMembers(expected1);
                        expect(result2).toIncludeSameMembers(expected2);
                    } else {
                        expect(result1).toEqual(expected1);
                        expect(result2).toEqual(expected2);
                    }
                });
            });

            describe('inequality on type', () => {
                test.each([
                    { type: 'number', data: numbers },
                    { type: 'number', data: numbers, itemToExclude: numbers.find(e => e.ordered === 1) },
                    { type: 'number', data: numbers, itemToExclude: numbers.find(e => e.ordered === Number.MAX_VALUE) },
                    { type: 'string', data: strings },
                    { type: 'bytes', data: bytes },
                    { type: 'reference', data: storedReferences },
                    { type: 'boolean', data: booleans },
                    { type: 'date', data: dates },
                    { type: 'array', data: arrays },
                    { type: 'map', data: maps },
                ] as const)('$type', async ({ data, itemToExclude = data[0] }) => {
                    const result = await getData(collection.where('ordered', '!=', itemToExclude.ordered));
                    fs.enterprise
                        ? expect(result).toIncludeSameMembers(without(storedTestData, itemToExclude).map(sanitizeData))
                        : expect(result).toEqual(without(storedTestData, itemToExclude, nullType, nothing).map(sanitizeData));
                });
            });

            describe('equality on type', () => {
                test.each([
                    { type: 'number', data: numbers },
                    { type: 'number', data: numbers, itemToUse: numbers.find(e => e.ordered === 1) },
                    { type: 'string', data: strings },
                    { type: 'bytes', data: bytes },
                    { type: 'reference', data: storedReferences },
                    { type: 'boolean', data: booleans },
                    { type: 'date', data: dates },
                    { type: 'array', data: arrays },
                    { type: 'map', data: maps },
                ] as const)('$type', async ({ data, itemToUse = data[0] }) => {
                    expect(await getData(collection.where('ordered', '==', itemToUse.ordered))).toEqual([sanitizeData(itemToUse)]);
                });
            });

            describe.each([
                { type: 'Null', data: nullType },
                { type: 'NaN', data: nanType },
            ])('only == / != operations on $type values', ({ type, data }) => {
                test.each(['>', '>=', '<', '<='] as const)('cannot perform %s', operator => {
                    expect(() => collection.where('ordered', operator, data.ordered)).toThrow(
                        `Invalid query. You can only perform '==' and '!=' comparisons on ${type}.`,
                    );
                });

                test('can perform `==`', async () => {
                    expect(await getData(collection.where('ordered', '==', data.ordered))).toEqual([data]);
                });

                test('can perform `!=`', async () => {
                    expect(await getData(collection.where('ordered', '!=', data.ordered))).toIncludeSameMembers(
                        fs.enterprise
                            ? without(testData, data)
                            : // `nullType` (and `nothing`) is always excluded in '!=' queries
                              without(testData, data, nullType, nothing),
                    );
                });
            });

            test('multiple inequalities', async () => {
                const result = await getData(collection.where('type', '!=', 'none').where('ordered', '>', 0));
                const expected = testData.filter(d => d.type === 'number' && (d.ordered as number) > 0);
                fs.enterprise ? expect(result).toIncludeSameMembers(expected) : expect(result).toStrictEqual(expected);
            });

            test('multiple array contains (only possible in Enterprise edition)', async () => {
                const path = new FieldPath('`backticks`', 'nested'); // Test escaping of backticks.
                const promise = getData(collection.where(path, 'array-contains', 'data').where('type', 'array-contains-any', [1, 2]));
                fs.enterprise
                    ? await expect(promise).resolves.toEqual([])
                    : await expect(promise).rejects.toThrow(
                          connection === 'JAVA EMULATOR'
                              ? '9 FAILED_PRECONDITION: Only a single array-contains clause is allowed in a query'
                              : "3 INVALID_ARGUMENT: A maximum of 1 'ARRAY_CONTAINS' filter is allowed per disjunction.",
                      );
            });

            test('not-in, OR + inequality existence, and lexicographical multi-inequality orderBy', async () => {
                expect(await getData(collection.where('type', 'not-in', ['string', 'bytes']))).toIncludeSameMembers(
                    without(testData, ...strings, ...bytes),
                );

                expect(await getData(collection.where('ordered', 'not-in', [-500]).where('type', '==', 'number'))).toIncludeSameMembers(
                    numbers.slice(1),
                );

                const notInAndGt = await getData(collection.where('type', 'not-in', ['none']).where('ordered', '>', 0));
                const expectedNums = testData.filter(d => d.type === 'number' && (d.ordered as number) > 0);
                fs.enterprise ? expect(notInAndGt).toIncludeSameMembers(expectedNums) : expect(notInAndGt).toStrictEqual(expectedNums);

                const orWithIneq = await getData(
                    collection.where(Filter.or(Filter.where('type', '==', 'none'), Filter.where('ordered', '>', 2))),
                );
                const orderedGtTwo = testData.filter(d => d.type === 'number' && (d.ordered as number) > 2);
                expect(orWithIneq).toIncludeSameMembers(fs.enterprise ? [nothing, ...orderedGtTwo] : orderedGtTwo);

                const typeFirst = await getData(collection.where('type', '>', 'A').where('ordered', '>', 0));
                const orderedFirst = await getData(collection.where('ordered', '>', 0).where('type', '>', 'A'));
                fs.enterprise ? expect(typeFirst).toIncludeSameMembers(orderedFirst) : expect(typeFirst).toStrictEqual(orderedFirst);
            });
        });

        describe('paginating results', () => {
            // On Enterprise Edition, single-collection scans do not implicitly order by `__name__`.
            // In the `collection group` suite above, each document is created under its own unique parent
            // document (1 leaf per parent), and Enterprise scans parent paths ascending, so implicit
            // order happens to match `orderBy('path')` here even though leaves under a shared parent are unordered.
            fs.enterprise && description === 'collection'
                ? test('document should not implicitly be ordered by name', async () => {
                      const implicit = await getData(collection);
                      const explicit = await getData(orderByDocId);
                      expect(implicit).toIncludeSameMembers(explicit);
                      expect(implicit).not.toEqual(explicit);
                  })
                : test('documents should implicitly be ordered by name', async () => {
                      const implicit = await getData(collection);
                      const explicit = await getData(orderByDocId);
                      expect(implicit).toEqual(explicit);
                  });

            describe.each([
                {
                    description: 'document ID',
                    orderedCollection: orderByDocId,
                    getFieldValues: (snap: QueryDocumentSnapshot) => [getDocId(snap)],
                },
                {
                    description: 'type and ordered field',
                    orderedCollection: collection.orderBy('type').orderBy('ordered', 'desc'),
                    getFieldValues: (snap: QueryDocumentSnapshot) => {
                        const { type, ordered } = readData<Data>(snap.data());
                        return [type, ordered];
                    },
                },
            ])('ordered by $description', ({ orderedCollection, getFieldValues }) => {
                let snapshots: QueryDocumentSnapshot[];
                let docs: Data[];

                beforeAll(async () => {
                    const snap = await orderedCollection.get();
                    snapshots = snap.docs;
                    docs = snapshots.map(snap => sanitizeData(readData<Data>(snap.data())));
                });

                test('limit', async () => {
                    const firstTen = await getData(orderedCollection.limit(10));
                    expect(firstTen).toEqual(docs.slice(0, 10));
                });

                test('offset', async () => {
                    const skippedTen = await getData(orderedCollection.offset(10));
                    expect(skippedTen).toEqual(docs.slice(10));
                });

                test('limit and offset', async () => {
                    const secondTen = await getData(orderedCollection.offset(10).limit(10));
                    expect(secondTen).toEqual(docs.slice(10, 20));
                    expect(secondTen).toHaveLength(10);
                });

                describe.each([
                    { using: 'snapshot', getCursor: (idx: number) => [snapshots[idx]] },
                    { using: 'fieldValues', getCursor: (idx: number) => getFieldValues(snapshots[idx]) },
                ])('cursor using $using', ({ getCursor }) => {
                    const cursorIdx = 15;
                    test.each([
                        { command: 'startAt', from: cursorIdx, to: undefined },
                        { command: 'startAfter', from: cursorIdx + 1, to: undefined },
                        { command: 'endAt', from: 0, to: cursorIdx + 1 },
                        { command: 'endBefore', from: 0, to: cursorIdx },
                    ] as const)('$command', async ({ command, from, to }) => {
                        const results = await getData(orderedCollection[command](...getCursor(cursorIdx)));
                        expect(results).toEqual(docs.slice(from, to));
                    });

                    test('combining startAfter/endBefore', async () => {
                        const endBeforeIdx = 20;
                        const result = await getData(
                            orderedCollection.startAfter(...getCursor(cursorIdx)).endBefore(...getCursor(endBeforeIdx)),
                        );
                        expect(result).toEqual(docs.slice(cursorIdx + 1, endBeforeIdx));
                    });

                    test('combining startAt/endBefore', async () => {
                        const endBeforeIdx = 20;
                        const result = await getData(
                            orderedCollection.startAt(...getCursor(cursorIdx)).endBefore(...getCursor(endBeforeIdx)),
                        );
                        expect(result).toEqual(docs.slice(cursorIdx, endBeforeIdx));
                    });
                    test('combining startAfter/endAt', async () => {
                        const endAtIdx = 20;
                        const result = await getData(orderedCollection.startAfter(...getCursor(cursorIdx)).endAt(...getCursor(endAtIdx)));
                        expect(result).toEqual(docs.slice(cursorIdx + 1, endAtIdx + 1));
                    });
                });
            });
        });

        describe('projection', () => {
            test.each([
                { requested: [], got: [] },
                { requested: ['type'], got: ['type'] },
                { requested: ['type', 'ordered', 'non_existing'], got: ['type', 'ordered'] },
            ])('select subset of fields', async ({ requested, got }) => {
                const result = await collection.select(...requested).get();
                expect(uniq(result.docs.flatMap(d => Object.keys(d.data())))).toIncludeSameMembers(got);
            });
        });

        describe('testing read-time in queries', () => {
            let beforeFirstDocument: Timestamp;
            let beforeExtraDocument: Timestamp;
            let afterExtraDocument: Timestamp;
            let extraRef: DocumentReference;
            let query: Query;

            beforeAll(async () => {
                beforeFirstDocument = Timestamp.fromMillis(Date.now() - 1_000_000);
                beforeExtraDocument = Timestamp.now();
                query = collection.where('type', '==', 'number');
                extraRef = createRef();
                const { writeTime } = await extraRef.create(writeData({ type: 'number' }));
                afterExtraDocument = writeTime;
            });

            afterAll(async () => {
                await extraRef.delete();
            });

            test('in transaction with read-time', async () => {
                const cases = [
                    { readTime: beforeFirstDocument, expectedCount: 0 },
                    { readTime: beforeExtraDocument, expectedCount: numbers.length },
                    { readTime: afterExtraDocument, expectedCount: numbers.length + 1 },
                    { readTime: undefined, expectedCount: numbers.length + 1 },
                ];
                for (const { readTime, expectedCount } of cases) {
                    const count = await fs.firestore.runTransaction(async txn => (await txn.get(query)).docs.length, {
                        readOnly: true,
                        readTime,
                    });
                    expect(count).toBe(expectedCount);
                }
            });
        });
    });

    describe('list', () => {
        const data = writeData({ prop: 'foo' });

        test('listDocuments', async () => {
            const scope = fs.collection.doc().collection('collection');

            expect(await scope.listDocuments()).toBeEmpty();
            // Create three different docs in the given collection
            await scope.doc('foo').create(data);
            await scope.doc('bar').create(data);
            // This one is only a nested collection, this is a so called "missing doc"
            await scope.doc('baz').collection('subFoo').doc().collection('really nested').doc().create(data);

            expect((await scope.listDocuments()).map(c => c.id)).toIncludeSameMembers(['foo', 'bar', 'baz']);
        });

        test('listDocuments pagination, boundary mutations, masks, orderBy, showMissing, and token validation', async () => {
            const parentDoc = fs.collection.doc();
            const dbRoot = (fs.firestore as unknown as { formattedName: string }).formattedName;
            const parent = `${dbRoot}/documents/${parentDoc.path}`;
            const collectionId = 'pagedDocs';
            const col = parentDoc.collection(collectionId);

            // Common 1. Malformed/garbage pageToken on an empty collection -> 3 INVALID_ARGUMENT.
            await expectRpcCode(
                () => listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2, pageToken: 'garbage-token' }),
                3,
                'empty-collection-garbage-token',
            );
            await expectRpcCode(
                () => callRawUnaryRpc(fs.firestore, 'listDocuments', { parent, collectionId, pageToken: 'garbage-token' }),
                3,
                'empty-collection-raw-unary-garbage-token',
            );

            // Seed documents for default, orderBy, and mask tests.
            const seedDocs: Record<string, Record<string, unknown>> = {
                a: { 'rank': 1, 'other': 40, 'label': 'la', 'a,b': 10 },
                b: { 'rank': 2, 'other': 30, 'label': 'lb', 'a,b': 20 },
                c: { 'rank': 3, 'other': 20, 'label': 'lc', 'a,b': 30 },
                d: { 'rank': 4, 'other': 10, 'label': 'ld', 'a,b': 40 },
            };
            await Promise.all(Object.entries(seedDocs).map(([id, doc]) => col.doc(id).set(writeData(doc))));

            // Common 2. Malformed/garbage and raw-doc-path pageToken on non-empty collection -> 3 INVALID_ARGUMENT.
            await expectRpcCode(
                () => listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2, pageToken: 'garbage-token' }),
                3,
                'non-empty-garbage-token',
            );
            await expectRpcCode(
                () =>
                    listDocumentsPage(fs.firestore, {
                        parent,
                        collectionId,
                        pageSize: 2,
                        pageToken: `${parent}/${collectionId}/a`,
                    }),
                3,
                'raw-doc-path-token',
            );

            // Common 3. Default orderBy: '' pagination with deleted returned boundary doc & altered pageSize.
            const defaultP1 = await listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2 });
            const defaultP1Ids = docIdsFromRaw(defaultP1.documents);
            expect(defaultP1Ids).toHaveLength(2);
            expect(defaultP1.nextPageToken).not.toBe('');

            const deletedBoundaryId = defaultP1Ids[defaultP1Ids.length - 1];
            await col.doc(deletedBoundaryId).delete();
            const defaultP2AfterDel = await listDocumentsPage(fs.firestore, {
                parent,
                collectionId,
                pageSize: 2,
                pageToken: defaultP1.nextPageToken,
            });
            const defaultP2Ids = docIdsFromRaw(defaultP2AfterDel.documents);
            expect(defaultP2AfterDel.nextPageToken).toBe('');
            if (!fs.enterprise) {
                expect(defaultP1Ids).toEqual(['a', 'b']);
                expect(defaultP2Ids).toEqual(['c', 'd']);
            } else {
                expect(defaultP2Ids).toHaveLength(2);
                expect(defaultP2Ids).not.toContain(deletedBoundaryId);
                expect([...defaultP1Ids, ...defaultP2Ids]).toIncludeSameMembers(['a', 'b', 'c', 'd']);
            }

            // Restore deleted boundary doc, then verify altering pageSize (1 -> 3) succeeds.
            await col.doc(deletedBoundaryId).set(writeData(seedDocs[deletedBoundaryId]));
            const size1P1 = await listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 1 });
            expect(size1P1.documents).toHaveLength(1);
            expect(size1P1.nextPageToken).not.toBe('');
            const size3P2 = await listDocumentsPage(fs.firestore, {
                parent,
                collectionId,
                pageSize: 3,
                pageToken: size1P1.nextPageToken,
            });
            expect(size3P2.documents).toHaveLength(3);
            expect(size3P2.nextPageToken).toBe('');
            expect([...docIdsFromRaw(size1P1.documents), ...docIdsFromRaw(size3P2.documents)]).toIncludeSameMembers(['a', 'b', 'c', 'd']);

            // Common 4. Reusing showMissing: false token with showMissing: true -> 3 INVALID_ARGUMENT,
            //           and showMissing: true + orderBy: '__name__' -> 3 INVALID_ARGUMENT.
            await expectRpcCode(
                () =>
                    listDocumentsPage(fs.firestore, {
                        parent,
                        collectionId,
                        pageSize: 2,
                        showMissing: true,
                        pageToken: size1P1.nextPageToken,
                    }),
                3,
                'reuse-show-missing-false-to-true',
            );

            const missColId = 'missingPaged';
            const missCol = parentDoc.collection(missColId);
            await Promise.all([
                missCol.doc('d1').set(writeData({ v: 1 })),
                missCol
                    .doc('d2')
                    .collection('sub')
                    .doc('s1')
                    .set(writeData({ v: 2 })),
                missCol
                    .doc('d3')
                    .collection('sub')
                    .doc('s1')
                    .set(writeData({ v: 3 })),
                missCol.doc('d4').set(writeData({ v: 4 })),
            ]);

            await expectRpcCode(
                () =>
                    listDocumentsPage(fs.firestore, {
                        parent,
                        collectionId: missColId,
                        showMissing: true,
                        orderBy: '__name__',
                    }),
                3,
                'show-missing-with-order-by',
            );

            if (!fs.enterprise) {
                // Standard: garbage pageToken with orderBy -> 3 INVALID_ARGUMENT.
                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent,
                            collectionId,
                            pageSize: 2,
                            orderBy: 'rank asc',
                            pageToken: 'garbage-token',
                        }),
                    3,
                    'non-empty-orderby-garbage-token',
                );

                // Standard: deleted & mutated boundary doc under orderBy ASC.
                const ascP1 = await listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2, orderBy: 'rank asc' });
                expect(docIdsFromRaw(ascP1.documents)).toEqual(['a', 'b']);
                expect(ascP1.nextPageToken).not.toBe('');

                await col.doc('b').delete();
                const ascP2AfterDel = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank asc',
                    pageToken: ascP1.nextPageToken,
                });
                expect(docIdsFromRaw(ascP2AfterDel.documents)).toEqual(['c', 'd']);
                expect(ascP2AfterDel.nextPageToken).toBe('');

                await col.doc('b').set(writeData(seedDocs.b));
                const ascP1Reset = await listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2, orderBy: 'rank asc' });
                expect(docIdsFromRaw(ascP1Reset.documents)).toEqual(['a', 'b']);

                await col.doc('b').update({ rank: 0 });
                const ascP2MovedEarlier = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank asc',
                    pageToken: ascP1Reset.nextPageToken,
                });
                expect(docIdsFromRaw(ascP2MovedEarlier.documents)).toEqual(['c', 'd']);

                await col.doc('b').update({ rank: 5 });
                const ascP2MovedLater = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 3,
                    orderBy: 'rank asc',
                    pageToken: ascP1Reset.nextPageToken,
                });
                expect(docIdsFromRaw(ascP2MovedLater.documents)).toEqual(['c', 'd', 'b']);
                await col.doc('b').update({ rank: 2 });

                // Standard: deleted & mutated boundary doc under orderBy DESC.
                const descP1 = await listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2, orderBy: 'rank desc' });
                expect(docIdsFromRaw(descP1.documents)).toEqual(['d', 'c']);
                expect(descP1.nextPageToken).not.toBe('');

                await col.doc('c').delete();
                const descP2AfterDel = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank desc',
                    pageToken: descP1.nextPageToken,
                });
                expect(docIdsFromRaw(descP2AfterDel.documents)).toEqual(['b', 'a']);
                expect(descP2AfterDel.nextPageToken).toBe('');

                await col.doc('c').set(writeData(seedDocs.c));
                const descP1Reset = await listDocumentsPage(fs.firestore, { parent, collectionId, pageSize: 2, orderBy: 'rank desc' });
                await col.doc('c').update({ rank: 5 });
                const descP2MovedEarlier = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank desc',
                    pageToken: descP1Reset.nextPageToken,
                });
                expect(docIdsFromRaw(descP2MovedEarlier.documents)).toEqual(['b', 'a']);

                await col.doc('c').update({ rank: 0 });
                const descP2MovedLater = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 3,
                    orderBy: 'rank desc',
                    pageToken: descP1Reset.nextPageToken,
                });
                expect(docIdsFromRaw(descP2MovedLater.documents)).toEqual(['b', 'a', 'c']);
                await col.doc('c').update({ rank: 3 });

                // Standard: mask omitting order_by field (and empty mask).
                const maskedP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank desc',
                    mask: { fieldPaths: ['label'] },
                });
                expect(docIdsFromRaw(maskedP1.documents)).toEqual(['d', 'c']);
                expect(Object.keys(maskedP1.documents[0].fields ?? {})).toEqual(['label']);
                const maskedP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 3,
                    orderBy: 'rank desc',
                    mask: { fieldPaths: ['label'] },
                    pageToken: maskedP1.nextPageToken,
                });
                expect(docIdsFromRaw(maskedP2.documents)).toEqual(['b', 'a']);
                expect(Object.keys(maskedP2.documents[0].fields ?? {})).toEqual(['label']);

                const emptyMaskP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank asc',
                    mask: { fieldPaths: [] },
                });
                expect(docIdsFromRaw(emptyMaskP1.documents)).toEqual(['a', 'b']);
                expect(emptyMaskP1.documents[0].fields ?? {}).toEqual({});
                const emptyMaskP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: 'rank asc',
                    mask: { fieldPaths: [] },
                    pageToken: emptyMaskP1.nextPageToken,
                });
                expect(docIdsFromRaw(emptyMaskP2.documents)).toEqual(['c', 'd']);

                // Standard: tied sort keys (ASC and DESC) with deleted boundary doc.
                const tieColId = 'tiedDocs';
                const tieCol = parentDoc.collection(tieColId);
                await Promise.all([
                    tieCol.doc('t1').set(writeData({ rank: 10 })),
                    tieCol.doc('t2').set(writeData({ rank: 10 })),
                    tieCol.doc('t3').set(writeData({ rank: 10 })),
                    tieCol.doc('t4').set(writeData({ rank: 10 })),
                ]);

                const tieAscP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: tieColId,
                    pageSize: 2,
                    orderBy: 'rank asc',
                });
                expect(docIdsFromRaw(tieAscP1.documents)).toEqual(['t1', 't2']);
                await tieCol.doc('t2').delete();
                const tieAscP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: tieColId,
                    pageSize: 2,
                    orderBy: 'rank asc',
                    pageToken: tieAscP1.nextPageToken,
                });
                expect(docIdsFromRaw(tieAscP2.documents)).toEqual(['t3', 't4']);

                await tieCol.doc('t2').set(writeData({ rank: 10 }));
                const tieDescP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: tieColId,
                    pageSize: 2,
                    orderBy: 'rank desc',
                });
                expect(docIdsFromRaw(tieDescP1.documents)).toEqual(['t4', 't3']);
                await tieCol.doc('t3').delete();
                const tieDescP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: tieColId,
                    pageSize: 2,
                    orderBy: 'rank desc',
                    pageToken: tieDescP1.nextPageToken,
                });
                expect(docIdsFromRaw(tieDescP2.documents)).toEqual(['t2', 't1']);

                // Standard: multi-clause `orderBy: 'rank asc, other desc'` with tied `rank` and tied `other`
                // values across page boundaries (proving 3-element cursors and implicit `__name__ DESC`).
                const multiColId = 'multiOrderDocs';
                const multiCol = parentDoc.collection(multiColId);
                await Promise.all([
                    multiCol.doc('m1').set(writeData({ rank: 1, other: 20 })),
                    multiCol.doc('m2').set(writeData({ rank: 1, other: 20 })),
                    multiCol.doc('m3').set(writeData({ rank: 1, other: 20 })),
                    multiCol.doc('m4').set(writeData({ rank: 1, other: 10 })),
                    multiCol.doc('m7').set(writeData({ rank: 1, other: 10 })),
                    multiCol.doc('m5').set(writeData({ rank: 2, other: 30 })),
                    multiCol.doc('m6').set(writeData({ rank: 2, other: 30 })),
                ]);

                const multiP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: multiColId,
                    pageSize: 2,
                    orderBy: 'rank asc, other desc',
                });
                expect(docIdsFromRaw(multiP1.documents)).toEqual(['m3', 'm2']);
                expect(multiP1.nextPageToken).not.toBe('');

                await multiCol.doc('m2').delete();
                const multiP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: multiColId,
                    pageSize: 2,
                    orderBy: 'rank asc, other desc',
                    pageToken: multiP1.nextPageToken,
                });
                expect(docIdsFromRaw(multiP2.documents)).toEqual(['m1', 'm7']);
                expect(multiP2.nextPageToken).not.toBe('');

                const multiP3 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: multiColId,
                    pageSize: 2,
                    orderBy: 'rank asc, other desc',
                    pageToken: multiP2.nextPageToken,
                });
                expect(docIdsFromRaw(multiP3.documents)).toEqual(['m4', 'm6']);
                expect(multiP3.nextPageToken).not.toBe('');

                const multiP4 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: multiColId,
                    pageSize: 2,
                    orderBy: 'rank asc, other desc',
                    pageToken: multiP3.nextPageToken,
                });
                expect(docIdsFromRaw(multiP4.documents)).toEqual(['m5']);
                expect(multiP4.nextPageToken).toBe('');

                // Standard: backtick-quoted comma field in orderBy and invalid direction -> 3 INVALID_ARGUMENT.
                const commaP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: '`a,b` desc',
                });
                expect(docIdsFromRaw(commaP1.documents)).toEqual(['d', 'c']);
                const commaP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 2,
                    orderBy: '`a,b` desc',
                    pageToken: commaP1.nextPageToken,
                });
                expect(docIdsFromRaw(commaP2.documents)).toEqual(['b', 'a']);

                await expectRpcCode(
                    () => listDocumentsPage(fs.firestore, { parent, collectionId, orderBy: 'rank sideways' }),
                    3,
                    'invalid-order-by-direction',
                );

                // Standard: reusing pageToken with altered orderBy (including `'rank'` or `'rank asc, __name__ asc'`),
                //           mask, collectionId, or parent -> 3 INVALID_ARGUMENT; altering pageSize succeeds.
                const probeToken = ascP1Reset.nextPageToken;
                for (const [label, badOrder] of [
                    ['reuse-altered-direction', 'rank desc'],
                    ['reuse-altered-field', 'other asc'],
                    ['reuse-omitted-orderby', ''],
                    ['reuse-implicit-dir-orderby', 'rank'],
                    ['reuse-explicit-name-orderby', 'rank asc, __name__ asc'],
                ] as const) {
                    await expectRpcCode(
                        () =>
                            listDocumentsPage(fs.firestore, {
                                parent,
                                collectionId,
                                pageSize: 2,
                                orderBy: badOrder,
                                pageToken: probeToken,
                            }),
                        3,
                        label,
                    );
                }
                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent,
                            collectionId,
                            pageSize: 2,
                            orderBy: 'rank desc',
                            mask: { fieldPaths: ['other'] },
                            pageToken: maskedP1.nextPageToken,
                        }),
                    3,
                    'reuse-altered-mask',
                );
                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent,
                            collectionId,
                            pageSize: 2,
                            orderBy: 'rank desc',
                            pageToken: maskedP1.nextPageToken,
                        }),
                    3,
                    'reuse-omitted-mask',
                );
                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent,
                            collectionId: tieColId,
                            pageSize: 2,
                            orderBy: 'rank asc',
                            pageToken: probeToken,
                        }),
                    3,
                    'reuse-altered-collection-id',
                );
                const otherParent = `${dbRoot}/documents/${fs.collection.doc().path}`;
                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent: otherParent,
                            collectionId,
                            pageSize: 2,
                            orderBy: 'rank asc',
                            pageToken: probeToken,
                        }),
                    3,
                    'reuse-altered-parent',
                );

                const alteredPageSize = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId,
                    pageSize: 1,
                    orderBy: 'rank asc',
                    pageToken: probeToken,
                });
                expect(docIdsFromRaw(alteredPageSize.documents)).toEqual(['c']);

                // Standard: showMissing = true pagination across phantom docs + deleted phantom boundary doc.
                const missP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: missColId,
                    pageSize: 2,
                    showMissing: true,
                });
                expect(docIdsFromRaw(missP1.documents)).toEqual(['d1', 'd2']);
                expect(missP1.documents[0].createTime).toBeDefined();
                expect(missP1.documents[1].createTime).toBeNil();
                expect(missP1.nextPageToken).not.toBe('');

                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent,
                            collectionId: missColId,
                            pageSize: 2,
                            showMissing: false,
                            pageToken: missP1.nextPageToken,
                        }),
                    3,
                    'reuse-show-missing-true-to-false',
                );

                await missCol.doc('d2').collection('sub').doc('s1').delete();
                const missP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: missColId,
                    pageSize: 2,
                    showMissing: true,
                    pageToken: missP1.nextPageToken,
                });
                expect(docIdsFromRaw(missP2.documents)).toEqual(['d3', 'd4']);
                expect(missP2.documents[0].createTime).toBeNil();
                expect(missP2.documents[1].createTime).toBeDefined();
                expect(missP2.nextPageToken).toBe('');
            } else {
                // Enterprise Edition rejects ALL non-empty orderBy in ListDocuments.
                for (const badOrder of ['__name__', 'rank asc', '`a,b` desc', 'rank sideways']) {
                    await expectRpcCode(
                        () => listDocumentsPage(fs.firestore, { parent, collectionId, orderBy: badOrder }),
                        3,
                        `enterprise-rejects-orderby-${badOrder}`,
                    );
                }

                // Enterprise: showMissing = false omits phantom docs, and unpaginated showMissing = true
                // returns all real + phantom docs.
                const noPhantom = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: missColId,
                    showMissing: false,
                });
                expect(docIdsFromRaw(noPhantom.documents)).toIncludeSameMembers(['d1', 'd4']);

                const allWithMissing = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: missColId,
                    showMissing: true,
                });
                expect(docIdsFromRaw(allWithMissing.documents)).toIncludeSameMembers(['d1', 'd2', 'd3', 'd4']);
                expect(allWithMissing.nextPageToken).toBe('');
                for (const doc of allWithMissing.documents) {
                    const id = doc.name?.slice(doc.name.lastIndexOf('/') + 1);
                    if (id === 'd1' || id === 'd4') {
                        expect(doc.createTime).toBeDefined();
                    } else {
                        expect(doc.createTime).toBeNil();
                    }
                }

                // Cloud Enterprise drops phantom docs once paginated with pageSize, whereas the Rust
                // emulator preserves phantom docs across pages (required by the Web UI).
                const missP1 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: missColId,
                    pageSize: 2,
                    showMissing: true,
                });
                expect(missP1.documents).toHaveLength(2);
                expect(missP1.nextPageToken).not.toBe('');

                await expectRpcCode(
                    () =>
                        listDocumentsPage(fs.firestore, {
                            parent,
                            collectionId: missColId,
                            pageSize: 2,
                            showMissing: false,
                            pageToken: missP1.nextPageToken,
                        }),
                    3,
                    'enterprise-reuse-show-missing-true-to-false',
                );

                const missP2 = await listDocumentsPage(fs.firestore, {
                    parent,
                    collectionId: missColId,
                    pageSize: 3,
                    showMissing: true,
                    pageToken: missP1.nextPageToken,
                });
                expect(missP2.nextPageToken).toBe('');
                const allMissingDocs = [...missP1.documents, ...missP2.documents];
                const allMissingIds = docIdsFromRaw(allMissingDocs);
                expect(uniq(allMissingIds)).toHaveLength(allMissingIds.length);
                expect(['d1', 'd2', 'd3', 'd4']).toIncludeAllMembers(allMissingIds);
                if (connection !== 'CLOUD FIRESTORE') {
                    expect(allMissingIds).toIncludeSameMembers(['d1', 'd2', 'd3', 'd4']);
                }
                for (const doc of allMissingDocs) {
                    const id = doc.name?.slice(doc.name.lastIndexOf('/') + 1);
                    if (id === 'd1' || id === 'd4') {
                        expect(doc.createTime).toBeDefined();
                    } else {
                        expect(doc.createTime).toBeNil();
                    }
                }
            }
        }, 30_000);

        test('listCollections', async () => {
            const scope = fs.collection.doc();

            expect(await scope.listCollections()).toBeEmpty();
            // Create three different collections
            await scope.collection('foo').doc().create(data);
            await scope.collection('bar').doc().create(data);
            await scope.collection('baz').doc().create(data);
            expect((await scope.listCollections()).map(c => c.id)).toIncludeSameMembers(['foo', 'bar', 'baz']);
        });

        test('listCollectionIds pagination and validation', async () => {
            const parentDoc = fs.collection.doc();
            const dbRoot = (fs.firestore as unknown as { formattedName: string }).formattedName;
            const docsRoot = `${dbRoot}/documents`;
            const parent = `${docsRoot}/${parentDoc.path}`;

            // Nonexistent doc parent returns empty list and empty nextPageToken.
            expect(await listCollectionIdsPage(fs.firestore, { parent, pageSize: 2 })).toEqual({
                collectionIds: [],
                nextPageToken: '',
            });

            // Create 5 subcollections out of lexicographic order.
            await Promise.all(['c3', 'c1', 'c5', 'c2', 'c4'].map(name => parentDoc.collection(name).doc('d').create(writeData({ name }))));

            // (a) pageSize: 2 paginates lexicographically across 3 pages and ends with empty nextPageToken.
            const p1 = await listCollectionIdsPage(fs.firestore, { parent, pageSize: 2 });
            expect(p1.collectionIds).toEqual(['c1', 'c2']);
            expect(p1.nextPageToken).not.toBe('');

            const p2 = await listCollectionIdsPage(fs.firestore, { parent, pageSize: 2, pageToken: p1.nextPageToken });
            expect(p2.collectionIds).toEqual(['c3', 'c4']);
            expect(p2.nextPageToken).not.toBe('');

            const p3 = await listCollectionIdsPage(fs.firestore, { parent, pageSize: 2, pageToken: p2.nextPageToken });
            expect(p3).toEqual({ collectionIds: ['c5'], nextPageToken: '' });

            // Changing pageSize to 0 (return all remaining) with a valid token succeeds.
            const rest = await listCollectionIdsPage(fs.firestore, { parent, pageSize: 0, pageToken: p1.nextPageToken });
            expect(rest).toEqual({ collectionIds: ['c3', 'c4', 'c5'], nextPageToken: '' });

            // (b) Exact fit (pageSize == total), pageSize > total, and pageSize == 0 return empty nextPageToken.
            for (const pageSize of [5, 6, 0]) {
                expect(await listCollectionIdsPage(fs.firestore, { parent, pageSize })).toEqual({
                    collectionIds: ['c1', 'c2', 'c3', 'c4', 'c5'],
                    nextPageToken: '',
                });
            }

            // (c) Negative pageSize -> 3 INVALID_ARGUMENT.
            await expectRpcCode(
                () => listCollectionIdsPage(fs.firestore, { parent, pageSize: -1 }),
                3,
                'list-collection-ids-negative-page-size',
            );

            // (d) Arbitrary / raw-string pageTokens -> 3 INVALID_ARGUMENT.
            for (const badToken of ['not-a-valid-token', 'c2']) {
                await expectRpcCode(
                    () => listCollectionIdsPage(fs.firestore, { parent, pageSize: 2, pageToken: badToken }),
                    3,
                    `list-collection-ids-bad-token-${badToken}`,
                );
            }

            // (e) Collection-path parent -> 3 INVALID_ARGUMENT.
            for (const colParent of [`${docsRoot}/someCol`, `${docsRoot}/${fs.collection.path}`]) {
                await expectRpcCode(
                    () => listCollectionIdsPage(fs.firestore, { parent: colParent }),
                    3,
                    `list-collection-ids-collection-parent-${colParent}`,
                );
            }
        });
    });

    describe('aggregation queries', () => {
        const cities = [
            { city: 'Buren', population: 27_718, area: 142.92 },
            { city: 'Tiel', population: 41_978, area: 35.51 },
            { city: 'Woerden', population: 53_253, area: 92.92 },
            { city: 'Zoetermeer', population: 127_047, area: 37.05 },
        ];
        const allCities = fs.collection.doc().collection('cityCollection');
        const biggerCities = allCities.where('population', '>', 50_000);

        beforeAll(async () => {
            await Promise.all(cities.map(cityData => allCities.doc().create(writeData(cityData))));
        });

        test('count method', async () => {
            expect(await getAggregate(allCities.count())).toEqual({ count: 4 });
            expect(await getAggregate(biggerCities.count())).toEqual({ count: 2 });
        });

        test('count Field', async () => {
            const countField = AggregateField.count();
            await expect(getAggregate(allCities.aggregate({ countField }))).resolves.toEqual({
                countField: 4,
            });
            await expect(getAggregate(biggerCities.aggregate({ countField }))).resolves.toEqual({
                countField: 2,
            });
        });

        test('average', async () => {
            const averagePopulation = AggregateField.average('population');
            await expect(getAggregate(allCities.aggregate({ averagePopulation }))).resolves.toEqual({
                averagePopulation: 62_499,
            });

            await expect(getAggregate(biggerCities.aggregate({ averagePopulation }))).resolves.toEqual({
                averagePopulation: 90_150,
            });
        });

        test('sum', async () => {
            const sumArea = AggregateField.sum('area');
            await expect(getAggregate(allCities.aggregate({ sumArea }))).resolves.toEqual({
                // Known current limitation of the Rust emulator, depending of the order of the aggregation we
                // either get the exact sum of 308.4 or slightly more (you know, floating point).
                sumArea: expect.toBeWithin(308.4, 308.4000001),
            });

            await expect(getAggregate(biggerCities.aggregate({ sumArea }))).resolves.toEqual({
                sumArea: 129.97,
            });
        });

        test('all together now', async () => {
            const fields = {
                count: AggregateField.count(),
                sumArea: AggregateField.sum('area'),
                averagePopulation: AggregateField.average('population'),
            };
            await expect(getAggregate(allCities.aggregate(fields))).resolves.toEqual({
                count: 4,
                // Known current limitation of the Rust emulator, depending of the order of the aggregation we
                // either get the exact sum of 308.4 or slightly more (you know, floating point).
                sumArea: expect.toBeWithin(308.4, 308.4000001),
                averagePopulation: 62_499,
            });

            await expect(getAggregate(biggerCities.aggregate(fields))).resolves.toEqual({
                count: 2,
                sumArea: 129.97,
                averagePopulation: 90_150,
            });
        });

        test('sum and avg i64 intermediate overflow and cancellation wire semantics', async () => {
            interface RawAggregationResponseChunk {
                result?: {
                    aggregateFields?: Record<string, RawProtoValue>;
                };
            }

            const I64_MAX = 9_223_372_036_854_775_807n;
            const I64_MIN = -9_223_372_036_854_775_808n;

            const wireRepr = (val: RawProtoValue | undefined) => {
                if (val?.integerValue !== undefined) {
                    return { kind: 'integerValue', value: val.integerValue };
                }
                if (val?.doubleValue !== undefined) {
                    return { kind: 'doubleValue', value: val.doubleValue };
                }
                return { kind: 'other', value: val };
            };

            const probeSingleAgg = async (
                subCol: ReturnType<typeof fs.collection.doc>['parent'],
                alias: 's' | 'a',
                kind: 'sum' | 'avg',
            ) => {
                // Standard Edition rejects `orderBy(__name__)` combined with `sum(v)`/`avg(v)` because
                // `__name__` cannot precede `v` in its single-field index plan; Enterprise allows it.
                const queryProto = toRunQueryProto(fs.enterprise ? subCol.orderBy(FieldPath.documentId()) : subCol);
                try {
                    const chunks = await callRawStreamRpc<object, RawAggregationResponseChunk>(fs.firestore, 'runAggregationQuery', {
                        parent: queryProto.parent,
                        structuredAggregationQuery: {
                            structuredQuery: queryProto.structuredQuery,
                            aggregations: [{ alias, [kind]: { field: { fieldPath: 'v' } } }],
                        },
                    });
                    const fields = chunks.find(c => c.result?.aggregateFields)?.result?.aggregateFields;
                    return { ok: true as const, res: wireRepr(fields?.[alias]) };
                } catch (err: unknown) {
                    const code = typeof err === 'object' && err !== null ? (err as { code?: unknown }).code : undefined;
                    const message = err instanceof Error ? err.message : String(err);
                    return { ok: false as const, code, message };
                }
            };

            const probeCases = async (name: string, values: Array<bigint | number>) => {
                const subCol = fs.collection.doc().collection(`sumOverflow_${name}`);
                await Promise.all(values.map((v, idx) => subCol.doc(`d${String(idx).padStart(2, '0')}`).set(writeData({ v }))));
                const [s, a] = await Promise.all([probeSingleAgg(subCol, 's', 'sum'), probeSingleAgg(subCol, 'a', 'avg')]);
                return { s, a };
            };

            const [
                maxPosNeg,
                maxNegPos,
                cancelNeg2,
                finalOverflow,
                singleRound,
                avgCancel,
                avgOffsetCancel,
                mixedDoubleFirst,
                mixedDoubleLast,
            ] = await Promise.all([
                probeCases('maxPosNeg', [I64_MAX, 1n, -1n]),
                probeCases('maxNegPos', [I64_MAX, -1n, 1n]),
                probeCases('cancelNeg2', [I64_MAX, I64_MAX, I64_MIN, I64_MIN]),
                probeCases('finalOverflow', [I64_MAX, I64_MAX]),
                probeCases('singleRound', [I64_MAX - 512n, 1535n]),
                probeCases('avgCancel', [I64_MAX, I64_MAX, -I64_MAX, -I64_MAX, 3n]),
                probeCases('avgOffsetCancel', [I64_MAX, 1n, -I64_MAX, -1n, 3n]),
                probeCases('mixedDoubleFirst', [1.5, I64_MAX, I64_MAX]),
                probeCases('mixedDoubleLast', [I64_MAX, I64_MAX, 1.5]),
            ]);

            const longOverflow = {
                ok: false,
                code: 3,
                message: expect.stringContaining('Long overflow'),
            };

            const okInt = (value: string) => ({ ok: true, res: { kind: 'integerValue', value } });
            const okDbl = (value: number) => ({ ok: true, res: { kind: 'doubleValue', value } });

            if (fs.enterprise) {
                expect({
                    maxPosNeg,
                    maxNegPos,
                    cancelNeg2,
                    finalOverflow,
                    singleRound,
                    avgCancel,
                    avgOffsetCancel,
                    mixedDoubleFirst,
                    mixedDoubleLast,
                }).toEqual({
                    maxPosNeg: { s: longOverflow, a: longOverflow },
                    maxNegPos: { s: okInt('9223372036854775807'), a: okDbl(Number(I64_MAX) / 3) },
                    cancelNeg2: { s: longOverflow, a: longOverflow },
                    finalOverflow: { s: longOverflow, a: longOverflow },
                    singleRound: { s: longOverflow, a: longOverflow },
                    avgCancel: { s: longOverflow, a: longOverflow },
                    avgOffsetCancel: { s: longOverflow, a: longOverflow },
                    mixedDoubleFirst: { s: okDbl(2 ** 64), a: okDbl(2 ** 64 / 3) },
                    mixedDoubleLast: { s: longOverflow, a: longOverflow },
                });
            } else {
                expect({
                    maxPosNeg,
                    maxNegPos,
                    cancelNeg2,
                    finalOverflow,
                    singleRound,
                    avgCancel,
                    avgOffsetCancel,
                    mixedDoubleFirst,
                    mixedDoubleLast,
                }).toEqual({
                    maxPosNeg: { s: okInt('9223372036854775807'), a: okDbl(Number(I64_MAX) / 3) },
                    maxNegPos: { s: okInt('9223372036854775807'), a: okDbl(Number(I64_MAX) / 3) },
                    cancelNeg2: { s: okInt('-2'), a: okDbl(-0.5) },
                    finalOverflow: { s: okDbl(2 ** 64), a: okDbl(2 ** 63) },
                    singleRound: { s: okDbl(2 ** 63), a: okDbl(2 ** 62) },
                    avgCancel: { s: okInt('3'), a: okDbl(0.6) },
                    avgOffsetCancel: { s: okInt('3'), a: okDbl(0.6) },
                    mixedDoubleFirst: { s: okDbl(2 ** 64), a: okDbl(2 ** 64 / 3) },
                    mixedDoubleLast: { s: okDbl(2 ** 64), a: okDbl(2 ** 64 / 3) },
                });
            }
        });

        async function getAggregate(aggregate: AggregateQuery<AggregateSpec, unknown, DocumentData>) {
            const snap = await aggregate.get();
            return snap.data();
        }
    });

    describe('value comparison edge cases', () => {
        test('cross-type numeric, -0/+0, NaN, and GeoPoint comparisons in where filters', async () => {
            const subCol = fs.collection.doc().collection('comparisonEdgeCases');
            const docIntZero = subCol.doc('int-zero');
            const docNegZero = subCol.doc('neg-zero');
            const docIntOne = subCol.doc('int-one');
            const docDoubleOne = subCol.doc('double-one');
            const docNan = subCol.doc('nan');
            const docGeo = subCol.doc('geo');

            await Promise.all([
                docIntZero.set(writeData({ val: 0, arr: [0] })),
                // -0 is encoded by @google-cloud/firestore as doubleValue: -0.0
                docNegZero.set(writeData({ val: -0, arr: [-0] })),
                docIntOne.set(writeData({ val: 1, arr: [1] })),
                docDoubleOne
                    .set(writeData({ val: 0.5, arr: [-0, { nested: NaN }] }))
                    .then(() => docDoubleOne.update({ val: FieldValue.increment(0.5) })),
                docNan.set(writeData({ val: NaN, arr: [{ nested: NaN }] })),
                docGeo.set(writeData({ geo: new GeoPoint(52.0907, 5.1214) })),
            ]);

            const ids = async (q: Query) => (await q.get()).docs.map(d => d.id);
            const missingValIds = fs.enterprise ? ['geo'] : [];

            // 0 (int) and -0.0 (double) compare equal under ==, !=, in, and array-contains
            expect(await ids(subCol.where('val', '==', 0))).toIncludeSameMembers(['int-zero', 'neg-zero']);
            expect(await ids(subCol.where('val', '==', -0))).toIncludeSameMembers(['int-zero', 'neg-zero']);
            expect(await ids(subCol.where('val', '!=', -0))).toIncludeSameMembers(['int-one', 'double-one', 'nan', ...missingValIds]);

            // 1 (int) and 1.0 (double) compare equal under ==, !=, and in
            expect(await ids(subCol.where('val', '==', 1))).toIncludeSameMembers(['int-one', 'double-one']);
            expect(await ids(subCol.where('val', '!=', 1))).toIncludeSameMembers(['int-zero', 'neg-zero', 'nan', ...missingValIds]);
            expect(await ids(subCol.where('val', 'in', [-0, 1]))).toIncludeSameMembers(['int-zero', 'neg-zero', 'int-one', 'double-one']);

            // NaN matches NaN in == (UnaryFilter IS_NAN), in (Enterprise edition only), and nested array-contains
            expect(await ids(subCol.where('val', '==', NaN))).toEqual(['nan']);
            expect(await ids(subCol.where('val', 'in', [NaN]))).toEqual(fs.enterprise ? ['nan'] : []);
            expect(await ids(subCol.where('arr', 'array-contains', { nested: NaN }))).toIncludeSameMembers(['double-one', 'nan']);

            // array-contains matches across 0 (int) and -0.0 (double)
            expect(await ids(subCol.where('arr', 'array-contains', -0))).toIncludeSameMembers(['int-zero', 'neg-zero', 'double-one']);
            expect(await ids(subCol.where('arr', 'array-contains', 0))).toIncludeSameMembers(['int-zero', 'neg-zero', 'double-one']);

            // GeoPoint equality and inequality
            expect(await ids(subCol.where('geo', '==', new GeoPoint(52.0907, 5.1214)))).toEqual(['geo']);
            expect(await ids(subCol.where('geo', '!=', new GeoPoint(52.0907, 5.1214)))).toIncludeSameMembers(
                fs.enterprise ? ['int-zero', 'neg-zero', 'int-one', 'double-one', 'nan'] : [],
            );

            // not-in and array-contains-any across 0/-0, 1/1.0, NaN, and { nested: NaN }
            expect(await ids(subCol.where('val', 'not-in', [-0, 1]))).toIncludeSameMembers(['nan', ...missingValIds]);
            expect(await ids(subCol.where('val', 'not-in', [NaN]))).toIncludeSameMembers(
                fs.enterprise
                    ? ['int-zero', 'neg-zero', 'int-one', 'double-one', 'geo']
                    : ['int-zero', 'neg-zero', 'int-one', 'double-one', 'nan'],
            );
            expect(await ids(subCol.where('arr', 'array-contains-any', [-0, 999]))).toIncludeSameMembers([
                'int-zero',
                'neg-zero',
                'double-one',
            ]);
            expect(await ids(subCol.where('arr', 'array-contains-any', [0, 1]))).toIncludeSameMembers([
                'int-zero',
                'neg-zero',
                'int-one',
                'double-one',
            ]);
            expect(await ids(subCol.where('arr', 'array-contains-any', [NaN, { nested: NaN }]))).toIncludeSameMembers([
                'double-one',
                'nan',
            ]);
        });

        test('exact 64-bit integer vs float64 precision above 2^53', async () => {
            const subCol = fs.collection.doc().collection('precisionEdgeCases');
            // 9007199254740992 (2^53) is outside Number.isSafeInteger, so @google-cloud/firestore encodes it as doubleValue,
            // whereas BigInt values are encoded as integerValue.
            const twoPow53Double = 9007199254740992;
            const twoPow53Int = 9007199254740992n;
            const twoPow53PlusOneInt = 9007199254740993n;
            const maxInt64 = 9223372036854775807n;
            const twoPow63Double = 9223372036854775808;

            await Promise.all([
                subCol.doc('double-2p53').set(writeData({ val: twoPow53Double })),
                subCol.doc('int-2p53').set(writeData({ val: twoPow53Int })),
                subCol.doc('int-2p53-plus-1').set(writeData({ val: twoPow53PlusOneInt })),
                subCol.doc('int-max').set(writeData({ val: maxInt64 })),
                subCol.doc('double-2p63').set(writeData({ val: twoPow63Double })),
            ]);

            const ids = async (q: Query) => (await q.get()).docs.map(d => d.id);

            // 2^53 as double equals 2^53 as int64, but does NOT equal 2^53 + 1 as int64
            expect(await ids(subCol.where('val', '==', twoPow53Double))).toIncludeSameMembers(['double-2p53', 'int-2p53']);
            expect(await ids(subCol.where('val', '==', twoPow53PlusOneInt))).toEqual(['int-2p53-plus-1']);

            // i64::MAX (2^63 - 1) is strictly less than 2^63 as double
            expect(await ids(subCol.where('val', '==', maxInt64))).toEqual(['int-max']);
            expect(await ids(subCol.where('val', '==', twoPow63Double))).toEqual(['double-2p63']);

            const orderedIds = await ids(subCol.orderBy('val', 'asc'));
            expect(orderedIds.slice(0, 2)).toIncludeSameMembers(['double-2p53', 'int-2p53']);
            expect(orderedIds.slice(2)).toEqual(['int-2p53-plus-1', 'int-max', 'double-2p63']);
        });

        test('implicit __name__ direction and deduplicated inequality cursor ordering', async () => {
            const subCol = fs.collection.doc().collection('implicitOrderEdgeCases');
            await Promise.all([
                subCol.doc('doc-a').set(writeData({ val: 10 })),
                subCol.doc('doc-b').set(writeData({ val: 10 })),
                subCol.doc('doc-c').set(writeData({ val: 20 })),
            ]);

            const ids = async (q: Query) => (await q.get()).docs.map(d => d.id);

            // Standard Edition inherits `DESC` for implicit `__name__` ties; Enterprise leaves ties unspecified.
            const descIds = await ids(subCol.orderBy('val', 'desc'));
            expect(descIds[0]).toBe('doc-c');
            if (fs.enterprise) {
                expect(descIds.slice(1)).toIncludeSameMembers(['doc-a', 'doc-b']);
            } else {
                expect(descIds.slice(1)).toEqual(['doc-b', 'doc-a']);
            }

            // Duplicate inequalities on a single field (`val > 0 AND val < 100`) must order cleanly in
            // Standard Edition and paginate cleanly with snapshot `startAfter` (`[val ASC, __name__ ASC]`).
            const ineqIds = await ids(subCol.where('val', '>', 0).where('val', '<', 100));
            if (fs.enterprise) {
                expect(ineqIds).toIncludeSameMembers(['doc-a', 'doc-b', 'doc-c']);
            } else {
                expect(ineqIds).toEqual(['doc-a', 'doc-b', 'doc-c']);
            }

            const docASnap = await subCol.doc('doc-a').get();
            const afterAIds = await ids(subCol.where('val', '>', 0).where('val', '<', 100).startAfter(docASnap));
            expect(afterAIds).toEqual(['doc-b', 'doc-c']);
        });
    });

    async function getData(query: Query) {
        const snap = await query.get();
        return snap.docs.map(snap => sanitizeData(readData<Data>(snap.data())));
    }
    function sanitizeData(data: Data): Data {
        // Remove the 'path' property, because that one is only added for certain collection group queries
        data = omit(data, 'path');
        // Document References cannot be printed correctly by `Jest`
        if (data.ordered instanceof DocumentReference) {
            return { ...data, ordered: { fsRef: data.ordered.path } };
        }
        return data;
    }
    function without(testData: Data[], ...data: Data[]) {
        return differenceWith(testData, data, isEqual);
    }
});
