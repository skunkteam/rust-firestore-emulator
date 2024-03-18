import { differenceWith, isEqual, omit, orderBy, reverse, uniq } from 'lodash';
import { fs } from './utils';

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
    { type: 'date', ordered: fs.exported.Timestamp.fromDate(new Date('2022-01-01')) },
    { type: 'date', ordered: fs.exported.Timestamp.fromDate(new Date('2023-01-01')) },
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
const storedReferences: Data[] = strings.map(({ ordered }) => ({ type: 'ref', ordered: fs.collection.doc(ordered) }));
const references = storedReferences.map(sanitizeData);
const arrays: Data[] = [
    { type: 'array', ordered: [1, 2, 3] },
    { type: 'array', ordered: [1, 2, 3, 1] },
    { type: 'array', ordered: [2] },
];
const maps: Data[] = fs.notImplementedInRust || [
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
        orderByDocId: fs.collection.orderBy(fs.exported.FieldPath.documentId()),
        getDocId: (snap: FirebaseFirestore.QueryDocumentSnapshot) => snap.id,
    },
    {
        // Note that these tests need custom collection group indexes in Firestore
        description: 'collection group',
        collection: collectionGroupQuery,
        createRef: () => fs.collection.doc().collection(GROUPED_COLLECTION).doc(),
        orderByDocId: collectionGroupQuery.orderBy('path'),
        getDocId: (snap: FirebaseFirestore.QueryDocumentSnapshot) => snap.ref.path,
    },
])('on $description', ({ collection, createRef, orderByDocId, getDocId }) => {
    beforeAll(async () => {
        await Promise.all(
            storedTestData.map(async data => {
                const ref = createRef();
                // Also store path for collection group queries
                const path = ref.path;
                if (data === integerStoredAsDouble) {
                    await ref.set(fs.writeData({ ...data, path, ordered: integerStoredAsDouble.ordered - 0.5 }));
                    await ref.update({ ordered: fs.exported.FieldValue.increment(0.5) });
                } else {
                    await ref.set(fs.writeData({ ...data, path }));
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
            // `ordered` is reversed from this point, but it is only used locally, so it shouldn't matter
            expect(await getData(collection.orderBy('ordered', 'desc'))).toEqual(reverse(ordered));
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

        test('will implicitly order by the compared field', async () => {
            const result = await getData(collection.where('type', '>', 'A')); // should be everything

            expect(result.map(r => r.type)).toEqual(testData.map(r => r.type).sort());

            // less than or greater than should have the same implicit ordering
            const secondResult = await getData(collection.where('type', '<', 'z')); // should be everything
            expect(secondResult).toEqual(result);
        });

        if (!fs.notImplementedInRust) {
            test('cannot order by a different field than the queried inequality', async () => {
                await expect(collection.where('type', '>', 'z').orderBy('ordered').get()).rejects.toThrow('3 INVALID_ARGUMENT');

                // The where will not match any documents, but the query should work!
                expect(await collection.where('type', '>', 'z').orderBy('type').get()).toHaveProperty('docs', []);
                // Ordering by another field after ordering by the inequality field should work
                expect(await collection.where('type', '>', 'z').orderBy('type').orderBy('ordered', 'desc').get()).toHaveProperty(
                    'docs',
                    [],
                );
            });
        }

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
                ...(fs.notImplementedInRust || [{ type: 'map', data: maps }]),
            ] as const)('$type', async ({ data, whereClauseValue = data[0].ordered }) => {
                const saneData = data.map(sanitizeData);
                expect(await getData(collection.where('ordered', '>', whereClauseValue))).toEqual(saneData.slice(1));
                expect(await getData(collection.where('ordered', '<=', whereClauseValue))).toEqual(saneData.slice(0, 1));
            });
        });

        describe('inequality on type', () => {
            test.each([
                { type: 'number', data: numbers },
                { type: 'number', data: numbers, itemToExclude: numbers.find(e => e.ordered === Number.MAX_VALUE) },
                { type: 'string', data: strings },
                { type: 'bytes', data: bytes },
                { type: 'reference', data: storedReferences },
                { type: 'boolean', data: booleans },
                { type: 'date', data: dates },
                { type: 'array', data: arrays },
                ...(fs.notImplementedInRust || [{ type: 'map', data: maps }]),
            ] as const)('$type', async ({ data, itemToExclude = data[0] }) => {
                expect(await getData(collection.where('ordered', '!=', itemToExclude.ordered))).toEqual(
                    without(storedTestData, itemToExclude, nullType, nothing).map(sanitizeData),
                );
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
                ...(fs.notImplementedInRust || [{ type: 'map', data: maps }]),
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
                    // `nullType` (and `nothing`) is always excluded in '!=' queries
                    without(testData, data, nullType, nothing),
                );
            });
        });

        fs.notImplementedInRust ||
            test('multiple inequalities (should not be possible)', async () => {
                await expect(
                    getData(collection.where('ordered', '>', numbers[0].ordered).where('type', '>', numbers[0].type)),
                ).rejects.toThrow('3 INVALID_ARGUMENT: Cannot have inequality filters on multiple properties: [ordered, type]');
            });
    });

    describe('paginating results', () => {
        test('documents should implicitly be ordered by name', async () => {
            const implicit = await getData(collection);
            const explicit = await getData(orderByDocId);
            expect(implicit).toEqual(explicit);
        });

        describe.each([
            {
                description: 'document ID',
                orderedCollection: orderByDocId,
                getFieldValues: (snap: FirebaseFirestore.QueryDocumentSnapshot) => [getDocId(snap)],
            },
            {
                description: 'type and ordered field',
                orderedCollection: collection.orderBy('type').orderBy('ordered', 'desc'),
                getFieldValues: (snap: FirebaseFirestore.QueryDocumentSnapshot) => {
                    const { type, ordered } = fs.readData<Data>(snap.data());
                    return [type, ordered];
                },
            },
        ])('ordered by $description', ({ orderedCollection, getFieldValues }) => {
            let snapshots: FirebaseFirestore.QueryDocumentSnapshot[];
            let docs: Data[];

            beforeAll(async () => {
                const snap = await orderedCollection.get();
                snapshots = snap.docs;
                docs = snapshots.map(snap => sanitizeData(fs.readData<Data>(snap.data())));
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
                    const result = await getData(orderedCollection.startAt(...getCursor(cursorIdx)).endBefore(...getCursor(endBeforeIdx)));
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
        await Promise.all(cities.map(cityData => allCities.doc().create(fs.writeData(cityData))));
    });

    test('count method', async () => {
        expect(await getAggregate(allCities.count())).toEqual({ count: 4 });
        expect(await getAggregate(biggerCities.count())).toEqual({ count: 2 });
    });

    test('count Field', async () => {
        const countField = fs.exported.AggregateField.count();
        await expect(getAggregate(allCities.aggregate({ countField }))).resolves.toEqual({
            countField: 4,
        });
        await expect(getAggregate(biggerCities.aggregate({ countField }))).resolves.toEqual({
            countField: 2,
        });
    });

    test('average', async () => {
        const averagePopulation = fs.exported.AggregateField.average('population');
        await expect(getAggregate(allCities.aggregate({ averagePopulation }))).resolves.toEqual({
            averagePopulation: 62_499,
        });

        await expect(getAggregate(biggerCities.aggregate({ averagePopulation }))).resolves.toEqual({
            averagePopulation: 90_150,
        });
    });

    test('sum', async () => {
        const sumArea = fs.exported.AggregateField.sum('area');
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
            count: fs.exported.AggregateField.count(),
            sumArea: fs.exported.AggregateField.sum('area'),
            averagePopulation: fs.exported.AggregateField.average('population'),
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

    async function getAggregate(
        aggregate: FirebaseFirestore.AggregateQuery<FirebaseFirestore.AggregateSpec, unknown, FirebaseFirestore.DocumentData>,
    ) {
        const snap = await aggregate.get();
        return snap.data();
    }
});

async function getData(query: FirebaseFirestore.Query) {
    const snap = await query.get();
    return snap.docs.map(snap => sanitizeData(fs.readData<Data>(snap.data())));
}
function sanitizeData(data: Data): Data {
    // Remove the 'path' property, because that one is only added for certain collection group queries
    data = omit(data, 'path');
    // Document References cannot be printed correctly by `Jest`
    if (data.ordered instanceof fs.exported.DocumentReference) {
        return { ...data, ordered: { fsRef: data.ordered.path } };
    }
    return data;
}
function without(testData: Data[], ...data: Data[]) {
    return differenceWith(testData, data, isEqual);
}
