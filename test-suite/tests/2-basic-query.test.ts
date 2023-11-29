import { orderBy, reverse, without } from 'lodash';
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
const references: Data[] = strings.map(({ ordered }) => ({ type: 'ref', ordered: fs.collection.doc(ordered) }));
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
const allTestData = [
    nullType,
    ...booleans,
    nanType,
    ...numbers,
    ...dates,
    ...strings,
    ...bytes,
    ...references,
    ...arrays,
    ...maps,
    nothing,
];

beforeAll(async () => {
    await Promise.all(allTestData.map(data => fs.collection.doc().set(fs.writeData(data))));
});

test('basic equality', async () => {
    expect(await getData(fs.collection.where('type', '==', 'date'))).toIncludeSameMembers(dates);
    expect(await getData(fs.collection.where('type', '==', 'number'))).toIncludeSameMembers(numbers);
});

describe('ordering', () => {
    test('basic string order', async () => {
        const result = await getData(fs.collection.orderBy('type'));
        expect(result.map(r => r.type)).toEqual(allTestData.map(r => r.type).sort());
    });

    test('ordering all data', async () => {
        // Note that `nothing` is not present here! Because it has no `ordered` property.
        const ordered = without(allTestData, nothing);
        expect(await getData(fs.collection.orderBy('ordered'))).toEqual(ordered);
        expect(await getData(fs.collection.orderBy('ordered', 'asc'))).toEqual(ordered);
        expect(await getData(fs.collection.orderBy('ordered', 'desc'))).toEqual(reverse(ordered.slice()));
    });

    // Note: This needs an index in the Cloud Firestore
    test(`multiple orderBy's`, async () => {
        expect(await getData(fs.collection.orderBy('type').orderBy('ordered', 'desc'))).toIncludeSameMembers(
            // Note that this still skips `nothing`, because of the missing `ordered` property.
            orderBy(without(allTestData, nothing), ['type', 'ordered'], ['asc', 'desc']),
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
            { operator: '!=', compareTo: 'string', expected: without(allTestData, ...strings) },
        ] as const)("`type` $operator '$compareTo'", async ({ operator, compareTo, expected }) => {
            // Note that we filter on the `type` (string) field! Not the actual type of anything.
            expect(await getData(fs.collection.where('type', operator, compareTo))).toIncludeSameMembers(expected);
        });
    });

    test('will implicitly order by the compared field', async () => {
        const result = await getData(fs.collection.where('type', '>', 'A')); // should be everything

        expect(result.map(r => r.type)).toEqual(allTestData.map(r => r.type).sort());

        // less than or greater than should have the same implicit ordering
        const secondResult = await getData(fs.collection.where('type', '<', 'z')); // should be everything
        expect(secondResult).toEqual(result);
    });

    if (!fs.notImplementedInRust) {
        test('cannot order by a different field than the queried inequality', async () => {
            await expect(fs.collection.where('type', '>', 'z').orderBy('ordered').get()).rejects.toThrow('3 INVALID_ARGUMENT');

            // The where will not match any documents, but the query should work!
            expect(await fs.collection.where('type', '>', 'z').orderBy('type').get()).toHaveProperty('docs', []);
            // Ordering by another field after ordering by the inequality field should work
            expect(await fs.collection.where('type', '>', 'z').orderBy('type').orderBy('ordered', 'desc').get()).toHaveProperty('docs', []);
        });
    }

    if (!fs.notImplementedInRust) {
        describe('implicitly filter by Type', () => {
            test.each([
                { type: 'number', data: numbers },
                { type: 'string', data: strings },
                { type: 'bytes', data: bytes },
                { type: 'reference', data: references },
                { type: 'boolean', data: booleans },
                { type: 'date', data: dates },
                { type: 'array', data: arrays },
                { type: 'map', data: maps },
            ] as const)('$type', async ({ data }) => {
                expect(await getData(fs.collection.where('ordered', '>', data[0].ordered))).toEqual(data.slice(1));
                expect(await getData(fs.collection.where('ordered', '<=', data[0].ordered))).toEqual(data.slice(0, 1));
            });
        });
    }

    describe.each([
        { type: 'Null', data: nullType },
        { type: 'NaN', data: nanType },
    ])('only ==/!= operations on $type values', ({ type, data }) => {
        test.each(['>', '>=', '<', '<='] as const)('cannot perform %s', operator => {
            expect(() => fs.collection.where('ordered', operator, data.ordered)).toThrow(
                `Invalid query. You can only perform '==' and '!=' comparisons on ${type}.`,
            );
        });

        test('can perform `==`', async () => {
            expect(await getData(fs.collection.where('ordered', '==', data.ordered))).toEqual([data]);
        });

        if (!fs.notImplementedInRust) {
            test('can perform `!=`', async () => {
                expect(await getData(fs.collection.where('ordered', '!=', data.ordered))).toIncludeSameMembers(
                    // `nullType` (and `nothing`) is always excluded in '!=' queries
                    without(allTestData, data, nullType, nothing),
                );
            });
        }
    });
});

async function getData(query: FirebaseFirestore.Query) {
    const snap = await query.get();
    return snap.docs.map(snap => fs.readData<Data>(snap.data()));
}
