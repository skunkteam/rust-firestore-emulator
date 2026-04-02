import { CollectionReference, DocumentData, FieldValue, Timestamp } from '@google-cloud/firestore';
import * as p from '@google-cloud/firestore/pipelines';
import { editions, notImplementedInRust, writeData } from './utils';

const enterpriseDatabases = editions.filter(e => e.enterprise);
const suite = enterpriseDatabases.length ? describe.each(enterpriseDatabases) : describe.skip.each(editions);

suite('$description', fs => {
    describe('pipeline operations', () => {
        let cityRef: CollectionReference;
        let emptyRef: CollectionReference;

        beforeAll(async () => {
            cityRef = fs.collection.doc().collection('cities');
            emptyRef = fs.collection.doc().collection('empty');
            const cities: DocumentData[] = [
                { city: 'Amersfoort', population: 160000, area: 63, region: 'Utrecht' },
                { city: 'Utrecht', population: 360000, area: 99, region: 'Utrecht' },
                { city: 'Amsterdam', population: 900000, area: 219, region: 'Noord-Holland' },
                { city: 'Haarlem', population: 160000, area: 32, region: 'Noord-Holland', arr: [1, 2], emptyArr: [], zero: 0 },
                { city: 'Rotterdam', population: 650000, area: 324, region: 'Zuid-Holland' },
            ];
            await Promise.all(cities.map(c => cityRef.add(writeData(c))));
        });

        test.concurrent('basic pipeline support', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef);
            const snap = await pipe.execute();
            expect(snap).toBeDefined();
            expect(snap.results).toHaveLength(5);
        });

        notImplementedInRust ||
            describe('pipeline source stages', () => {
                test.concurrent('collectionGroup', async () => {
                    const pipe = fs.firestore.pipeline().collectionGroup('cities').sort(p.descending('population')).limit(1);
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Amsterdam', population: 900000 });
                });

                test.concurrent('database', async () => {
                    const pipe = fs.firestore.pipeline().database().where(p.field('area').greaterThan(300)).limit(1);
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Rotterdam', area: 324 });
                });

                test.concurrent('documents', async () => {
                    const snapshot = await cityRef.where('city', '==', 'Amersfoort').get();
                    const pipe = fs.firestore.pipeline().documents([snapshot.docs[0].ref]);
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Amersfoort', population: 160000 });
                });

                test.concurrent('createFrom', async () => {
                    const pipe = fs.firestore.pipeline().createFrom(cityRef.where('city', '==', 'Haarlem'));
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Haarlem', area: 32 });
                });
            });

        test.concurrent('simple aggregate', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate(p.countAll().as('totalCount'), p.sum('population').as('totalPop'), p.average('area').as('avgArea'));

            const snap = await pipe.execute();
            expect(snap).toBeDefined();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([
                {
                    totalCount: 5,
                    totalPop: 2230000,
                    avgArea: expect.closeTo(147.4, 1),
                },
            ]);
        });

        test.concurrent('grouping by region', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate({
                    accumulators: [p.countAll().as('count'), p.sum('population').as('pop')],
                    groups: [p.field('region').stringConcat('.').as('region')],
                });

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toIncludeSameMembers([
                { region: 'Utrecht.', count: 2, pop: 520000 },
                { region: 'Noord-Holland.', count: 2, pop: 1060000 },
                { region: 'Zuid-Holland.', count: 1, pop: 650000 },
            ]);
        });

        test.concurrent('where clause before aggregate', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .where(p.field('population').greaterThan(200000))
                .aggregate(p.countAll().as('totalCount'), p.sum('population').as('totalPop'));

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([
                {
                    totalCount: 3, // Utrecht, Amsterdam, Rotterdam
                    totalPop: 1910000,
                },
            ]);
        });

        test.concurrent('limit clause before aggregate', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef).limit(2).aggregate(p.countAll().as('totalCount'));
            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([{ totalCount: 2 }]);
        });

        test.concurrent('limit negative edge case', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef).limit(-1);
            await expect(pipe.execute()).rejects.toThrow('3 INVALID_ARGUMENT');
        });

        test.concurrent('aggregate on empty collection', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(emptyRef)
                .aggregate(p.countAll().as('totalCount'), p.sum('population').as('totalPop'), p.average('area').as('avgArea'));

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([
                {
                    totalCount: 0,
                    totalPop: null,
                    avgArea: null,
                },
            ]);
        });

        test.concurrent('aggregate on missing fields', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate(p.sum('nonExistingField').as('sumMissing'), p.average('nonExistingField').as('avgMissing'));

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([
                {
                    sumMissing: null,
                    avgMissing: null,
                },
            ]);
        });

        describe('extended pipeline stages', () => {
            test.concurrent('addFields, removeFields, select', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .addFields(p.stringConcat(p.constant('City: '), p.field('city')).as('newName'))
                    .select(p.field('newName').as('selectedName'), 'area')
                    .removeFields('area');

                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({ selectedName: 'City: Haarlem' });
            });

            test.concurrent('offset and limit', async () => {
                const pipe = fs.firestore.pipeline().collection(cityRef).sort(p.ascending('city')).offset(2).limit(2);

                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(2);
                expect(results[0].city).toBe('Haarlem');
                expect(results[1].city).toBe('Rotterdam');
            });

            test.concurrent('distinct', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .distinct(p.stringConcat(p.constant('Region: '), p.field('region')).as('regionPrefix'));

                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(3);
                const regions = results.map(r => r.regionPrefix).sort();
                expect(regions).toEqual(['Region: Noord-Holland', 'Region: Utrecht', 'Region: Zuid-Holland']);
            });

            test.concurrent('distinct on missing field', async () => {
                const pipe = fs.firestore.pipeline().collection(cityRef).distinct('missingField');
                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(1);
                expect(results[0]).toEqual({ missingField: null });
            });

            test.concurrent('aggregate with missing grouping field', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .aggregate({
                        accumulators: [p.countAll().as('count')],
                        groups: ['missingField'],
                    });
                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(1);
                expect(results[0]).toEqual({ count: 5, missingField: null });
            });
        });

        describe('expression groups', () => {
            test.concurrent('math expressions', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem')) // population: 160000, area: 32
                    .limit(1)
                    .select(
                        p.field('population').add(5).as('add'),
                        p.field('population').subtract(5).as('sub'),
                        p.field('area').multiply(2).as('mul'),
                        p.field('population').divide(2).as('div'),

                        // Note the danger of using the JavaScript SDK combined with server-side
                        // computation, JavaScript only has one numeric type (float64) while
                        // Firestore has two (integer and float). The SDK automatically detects
                        // integers and uses the integer type in communication with Firestore.
                        // This can lead to unexpected results if you are not careful. For example:
                        p.field('area').divide(5.0).as('integer_div'), // yields 6
                        p.field('area').add(0.5).subtract(0.5).divide(5.0).as('floating_div'), // yields 6.4
                        ...(notImplementedInRust || [p.constant(1.0).type().as('integer_type'), p.constant(1.1).type().as('float_type')]),

                        p.field('population').mod(3).as('mod'),
                        p.constant(-10).abs().as('abs'),
                        p.constant(1.5).ceil().as('ceil'),
                        p.field('area').pow(2).as('pow'),
                        p.constant(100).log10().as('log10'),
                        p.constant(Math.E).ln().as('ln'),
                        p.constant(1.5).round().as('round'),
                        p.constant(9).sqrt().as('sqrt'),
                        p.field('population').equal(160000).as('eq'),
                        p.field('population').lessThanOrEqual(160000).as('lte'),
                        p.field('population').lessThan(160000).as('lt'),
                        p.field('population').greaterThanOrEqual(160000).as('gte'),
                        p.field('population').greaterThan(160000).as('gt'),
                        p.field('population').notEqual(160000).as('ne'),
                        p.constant('a,b').split(',').as('split'),
                        p.and(p.constant(false).not(), p.constant(true)).as('and'),
                        p.or(p.constant(false), p.constant(true)).as('or'),
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({
                    add: 160005,
                    sub: 159995,
                    mul: 64,
                    div: 80000,

                    integer_div: 6, // Would be 6.4 if division was floating point
                    floating_div: 6.4,
                    ...(notImplementedInRust ? {} : { integer_type: 'int64', float_type: 'float64' }),

                    mod: 160000 % 3,
                    abs: 10,
                    ceil: 2,
                    pow: 1024,
                    log10: 2,
                    ln: expect.closeTo(1),
                    round: 2,
                    sqrt: 3,
                    eq: true,
                    gt: false,
                    gte: true,
                    lt: false,
                    lte: true,
                    ne: false,
                    split: ['a', 'b'],
                    and: true,
                    or: true,
                });
            });

            test.concurrent('string operations', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        p.toUpper(p.field('city')).as('upper'),
                        p.toLower(p.field('city')).as('lower'),
                        p.stringConcat(p.field('city'), p.constant(' test')).as('concat'),
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({
                    upper: 'HAARLEM',
                    lower: 'haarlem',
                    concat: 'Haarlem test',
                });
            });
            test.concurrent('logic operations', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        p.isAbsent(p.field('missingField')).as('isMissingFieldAbsent'),
                        p.isAbsent(p.field('city')).as('isCityAbsent'),
                        p.isError(p.constant(1).divide(0)).as('divByZeroError'),
                        p.isError(p.constant(1).divide(2)).as('divByTwoError'),
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({
                    isMissingFieldAbsent: true,
                    isCityAbsent: false,
                    divByZeroError: true,
                    divByTwoError: false,
                });
            });

            test.concurrent('math edge cases', async () => {
                const expressions: Record<string, p.Expression> = {
                    divZero: p.constant(1).divide(0),
                    modZero: p.constant(1).mod(0),
                    sqrtNeg: p.constant(-9).sqrt(),
                    lnZero: p.constant(0).ln(),
                    log10Neg: p.constant(-1).log10(),
                    addStringNum: p.constant('String').add(5),
                    divZeroField: p.constant(1).divide(p.field('zero')),
                };

                for (const expr of Object.values(expressions)) {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(p.field('city').equal('Haarlem'))
                        .limit(1)
                        .select(expr.as('res'));
                    await expect(pipe.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);
                }

                // But inside isError they evaluate to true
                const isErrorPipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(p.isError(expressions.divZeroField).as('isErrorDivField'));
                const isErrorSnap = await isErrorPipe.execute();
                expect(isErrorSnap.results[0].data()).toEqual({ isErrorDivField: true });

                const validMissingPipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(p.field('missing').add(10).as('addMissingNum'), p.field('missing').abs().as('absMissing'));
                const validMissingSnap = await validMissingPipe.execute();
                expect(validMissingSnap.results[0].data()).toEqual({ addMissingNum: null, absMissing: null });
            });

            test.concurrent('string edge cases', async () => {
                const expressions: Record<string, p.Expression> = {
                    toUpperNum: p.toUpper(p.constant(123)),
                    charLenNum: p.charLength(p.constant(123)),
                    toUpperNumField: p.toUpper(p.field('population')),
                };

                for (const expr of Object.values(expressions)) {
                    const pipe = fs.firestore.pipeline().collection(cityRef).limit(1).select(expr.as('res'));
                    await expect(pipe.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);
                }

                const validPipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        p.stringConcat(p.constant('a'), p.constant(null)).as('concatNull'),
                        p.stringConcat(p.constant('a'), p.field('missing')).as('concatMissing'),
                        p.constant('a,b').split(p.constant(null)).as('splitNull'),
                        p.isError(expressions.toUpperNumField).as('isErrorUpperNumField'),
                    );
                const validSnap = await validPipe.execute();
                expect(validSnap.results[0].data()).toEqual({
                    concatNull: null,
                    concatMissing: null,
                    splitNull: null,
                    isErrorUpperNumField: true,
                });
            });

            test.concurrent('array edge cases', async () => {
                const pipeArrStr = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .limit(1)
                    .select(p.field('city').arrayLength().as('arrLenStr'));
                await expect(pipeArrStr.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);

                const pipeValid = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(p.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(p.field('arr').arrayConcat([2, 3], [4, 5]).as('concatArrNum'));
                const validSnap = await pipeValid.execute();
                expect(validSnap.results[0].data()).toEqual({ concatArrNum: [1, 2, 2, 3, 4, 5] });
            });
        });

        describe('extended aggregations', () => {
            test.concurrent('minimum and maximum', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .aggregate(p.minimum('population').as('minPop'), p.maximum('population').as('maxPop'));
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({ minPop: 160000, maxPop: 900000 });
            });
        });

        notImplementedInRust ||
            describe('uncharted pipeline stages', () => {
                test.concurrent('replaceWith', async () => {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(p.field('city').equal('Haarlem'))
                        .limit(1)
                        .replaceWith(
                            p.map({
                                newField: p.stringConcat(p.field('city'), p.constant(' tests')),
                                pop: p.field('population'),
                            }),
                        );

                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toEqual({
                        newField: 'Haarlem tests',
                        pop: 160000,
                    });
                });

                test.concurrent('sample', async () => {
                    // sample(2) should return exactly 2 documents pseudo-randomly when 5 exist
                    const pipe = fs.firestore.pipeline().collection(cityRef).sample(2);

                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(2);

                    // Assert they are cities from our list
                    for (const result of snap.results) {
                        expect(result.data()).toContainKey('city');
                        expect(result.get('city')).toBeOneOf(['Amersfoort', 'Utrecht', 'Amsterdam', 'Haarlem', 'Rotterdam']);
                    }
                });

                test.concurrent('union', async () => {
                    // We combine pipeline targeting 'Haarlem' with pipeline targeting 'Amersfoort'
                    const pipe1 = fs.firestore.pipeline().collection(cityRef).where(p.field('city').equal('Haarlem'));
                    const pipe2 = fs.firestore.pipeline().collection(cityRef).where(p.field('city').equal('Amersfoort'));

                    const unionPipe = pipe1.union(pipe2);
                    const snap = await unionPipe.execute();

                    const cities = snap.results.map(r => r.get('city'));
                    expect(cities).toIncludeSameMembers(['Amersfoort', 'Haarlem']);
                });

                test.concurrent('unnest', async () => {
                    // Haarlem has arr: [1, 2]. Unnesting should result in 2 documents
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(p.field('city').equal('Haarlem'))
                        .unnest(p.field('arr').as('item'), 'idx');

                    const snap = await pipe.execute();
                    const results = snap.results.map(r => r.data());

                    expect(results).toEqual([
                        expect.objectContaining({ item: 1, idx: 0, city: 'Haarlem' }),
                        expect.objectContaining({ item: 2, idx: 1, city: 'Haarlem' }),
                    ]);
                });
            });

        describe('Exhaustive API Type Checks', () => {
            // 1. Core Classes (Abstract Syntax Nodes)
            type PipelineClasses = {
                [K in keyof typeof p]: (typeof p)[K] extends abstract new (...args: any[]) => any ? K : never;
            }[keyof typeof p];

            // 2. Modifiers & Core Builders
            type CoreBuilderMethods = 'as' | 'expressionType';

            // 3. Broken SDK Exports
            const brokenSDKExports = ['regexFind', 'regexFindAll'] as const;
            type BrokenSDKExports = (typeof brokenSDKExports)[number];

            // 4. Specialized Executors (Aggregates & Logic)
            type AggregateOperators = 'count' | 'sum' | 'average' | 'minimum' | 'maximum' | 'countDistinct' | 'countAll' | 'countIf';
            type LogicOperators = 'and' | 'or' | 'xor' | 'not' | 'conditional';
            type OrderByOperators = 'ascending' | 'descending';

            // Extract unhandled methods and functions
            type RelevantExpressionMethods = Exclude<
                keyof p.Expression,
                CoreBuilderMethods | AggregateOperators | LogicOperators | OrderByOperators
            >;
            type RelevantExportedFunctions = Exclude<
                keyof typeof p,
                PipelineClasses | AggregateOperators | LogicOperators | OrderByOperators | BrokenSDKExports
            >;

            type OperatorTestCase = {
                name: string;
                expr: p.Expression;
                expected: unknown;
            };

            const exprMethodTests: Record<RelevantExpressionMethods, OperatorTestCase | OperatorTestCase[]> = {
                abs: [
                    { name: 'abs (neg)', expr: p.constant(-10).abs(), expected: 10 },
                    { name: 'abs (pos)', expr: p.constant(10).abs(), expected: 10 },
                    { name: 'abs (zero)', expr: p.constant(0).abs(), expected: 0 },
                ],
                ceil: [
                    { name: 'ceil (pos)', expr: p.constant(1.5).ceil(), expected: 2 },
                    { name: 'ceil (neg)', expr: p.constant(-1.5).ceil(), expected: -1 },
                ],
                pow: [
                    { name: 'pow (int)', expr: p.constant(2).pow(3), expected: 8 },
                    { name: 'pow (zero)', expr: p.constant(9).pow(0), expected: 1 },
                ],
                sqrt: [
                    { name: 'sqrt (pos)', expr: p.constant(9).sqrt(), expected: 3 },
                    { name: 'sqrt (zero)', expr: p.constant(0).sqrt(), expected: 0 },
                ],
                log10: { name: 'log10', expr: p.constant(100).log10(), expected: 2 },
                ln: { name: 'ln', expr: p.constant(Math.E).ln(), expected: expect.closeTo(1) },
                round: [
                    { name: 'round (1.5)', expr: p.constant(1.5).round(), expected: 2 },
                    { name: 'round (1.4)', expr: p.constant(1.4).round(), expected: 1 },
                    { name: 'round (-1.5)', expr: p.constant(-1.5).round(), expected: -2 },
                ],
                add: [
                    { name: 'add (pos)', expr: p.constant(5).add(2), expected: 7 },
                    { name: 'add (neg)', expr: p.constant(5).add(-2), expected: 3 },
                    { name: 'add (zero)', expr: p.constant(5).add(0), expected: 5 },
                ],
                subtract: [
                    { name: 'subtract (pos)', expr: p.constant(5).subtract(2), expected: 3 },
                    { name: 'subtract (neg)', expr: p.constant(5).subtract(-2), expected: 7 },
                ],
                multiply: [
                    { name: 'multiply (pos)', expr: p.constant(5).multiply(2), expected: 10 },
                    { name: 'multiply (neg)', expr: p.constant(5).multiply(-2), expected: -10 },
                    { name: 'multiply (zero)', expr: p.constant(5).multiply(0), expected: 0 },
                ],
                divide: [
                    { name: 'divide', expr: p.constant(10).divide(2), expected: 5 },
                    { name: 'divide', expr: p.constant(10.0).divide(3), expected: 3.0 }, // because of integer division server side
                    { name: 'divide', expr: p.constant(10.5).divide(3), expected: 3.5 },
                ],
                mod: [
                    { name: 'mod (pos)', expr: p.constant(10).mod(3), expected: 1 },
                    { name: 'mod (exact)', expr: p.constant(10).mod(5), expected: 0 },
                ],

                equal: [
                    { name: 'equal (true)', expr: p.constant(1).equal(1), expected: true },
                    { name: 'equal (false)', expr: p.constant(1).equal(2), expected: false },
                ],
                notEqual: [
                    { name: 'notEqual (true)', expr: p.constant(1).notEqual(2), expected: true },
                    { name: 'notEqual (false)', expr: p.constant(1).notEqual(1), expected: false },
                ],
                lessThan: [
                    { name: 'lessThan (true)', expr: p.constant(1).lessThan(2), expected: true },
                    { name: 'lessThan (false)', expr: p.constant(2).lessThan(1), expected: false },
                    { name: 'lessThan (equal)', expr: p.constant(1).lessThan(1), expected: false },
                ],
                lessThanOrEqual: [
                    { name: 'lessThanOrEqual (true)', expr: p.constant(1).lessThanOrEqual(2), expected: true },
                    { name: 'lessThanOrEqual (equal)', expr: p.constant(1).lessThanOrEqual(1), expected: true },
                    { name: 'lessThanOrEqual (false)', expr: p.constant(2).lessThanOrEqual(1), expected: false },
                ],
                greaterThan: [
                    { name: 'greaterThan (true)', expr: p.constant(2).greaterThan(1), expected: true },
                    { name: 'greaterThan (false)', expr: p.constant(1).greaterThan(2), expected: false },
                    { name: 'greaterThan (equal)', expr: p.constant(1).greaterThan(1), expected: false },
                ],
                greaterThanOrEqual: [
                    { name: 'greaterThanOrEqual (true)', expr: p.constant(2).greaterThanOrEqual(1), expected: true },
                    { name: 'greaterThanOrEqual (equal)', expr: p.constant(2).greaterThanOrEqual(2), expected: true },
                    { name: 'greaterThanOrEqual (false)', expr: p.constant(1).greaterThanOrEqual(2), expected: false },
                ],
                isAbsent: [
                    { name: 'isAbsent (true)', expr: p.field('nonExist').isAbsent(), expected: true },
                    { name: 'isAbsent (false)', expr: p.field('city').isAbsent(), expected: false },
                ],
                isError: [
                    { name: 'isError (true)', expr: p.constant(1).divide(0).isError(), expected: true },
                    { name: 'isError (false)', expr: p.constant(1).divide(1).isError(), expected: false },
                ],
                asBoolean: [
                    { name: 'asBoolean (true)', expr: p.constant(true).asBoolean(), expected: true },
                    { name: 'asBoolean (false)', expr: p.constant(false).asBoolean(), expected: false },
                ],

                toUpper: [
                    { name: 'toUpper (lower)', expr: p.constant('v').toUpper(), expected: 'V' },
                    { name: 'toUpper (upper)', expr: p.constant('V').toUpper(), expected: 'V' },
                ],
                toLower: [
                    { name: 'toLower (upper)', expr: p.constant('V').toLower(), expected: 'v' },
                    { name: 'toLower (lower)', expr: p.constant('v').toLower(), expected: 'v' },
                ],
                charLength: notImplementedInRust || { name: 'charLength', expr: p.constant('🇳🇱').charLength(), expected: 2 },
                byteLength: notImplementedInRust || { name: 'byteLength', expr: p.constant('🇳🇱').byteLength(), expected: 8 },
                split: [
                    { name: 'split (match)', expr: p.constant('a,b').split(','), expected: ['a', 'b'] },
                    { name: 'split (no match)', expr: p.constant('a%b').split(','), expected: ['a%b'] },
                ],

                ifAbsent: notImplementedInRust || {
                    name: 'ifAbsent',
                    expr: p.field('nonExist').ifAbsent(p.constant('def')),
                    expected: 'def',
                },
                ifError: notImplementedInRust || { name: 'ifError', expr: p.constant(1).divide(0).ifError(p.constant(0)), expected: 0 },
                arrayConcat: { name: 'arrayConcat', expr: p.array([1]).arrayConcat([2]), expected: [1, 2] },
                arrayLength: [
                    { name: 'arrayLength (2)', expr: p.array([1, 2]).arrayLength(), expected: 2 },
                    { name: 'arrayLength (0)', expr: p.array([] as any[]).arrayLength(), expected: 0 },
                ],
                arrayContains: notImplementedInRust || [
                    { name: 'arrayContains (true)', expr: p.array([1, 2]).arrayContains(2), expected: true },
                    { name: 'arrayContains (false)', expr: p.array([1, 2]).arrayContains(3), expected: false },
                ],
                arrayContainsAny: notImplementedInRust || [
                    { name: 'arrayContainsAny (true)', expr: p.array([1, 2]).arrayContainsAny([2, 3]), expected: true },
                    { name: 'arrayContainsAny (false)', expr: p.array([1, 2]).arrayContainsAny([3, 4]), expected: false },
                ],
                mapMerge: notImplementedInRust || [
                    { name: 'mapMerge (unique)', expr: p.map({ a: 1 }).mapMerge(p.map({ b: 2 })), expected: { a: 1, b: 2 } },
                    { name: 'mapMerge (overwrite)', expr: p.map({ a: 1 }).mapMerge(p.map({ a: 2 })), expected: { a: 2 } },
                ],
                type: notImplementedInRust || [
                    { name: 'type (int64)', expr: p.constant(1.0).type(), expected: 'int64' },
                    { name: 'type (string)', expr: p.constant('foo').type(), expected: 'string' },
                    { name: 'type (boolean)', expr: p.constant(true).type(), expected: 'boolean' },
                ],
                stringConcat: [
                    { name: 'stringConcat (3 args)', expr: p.constant('a').stringConcat('b', 'c'), expected: 'abc' },
                    { name: 'stringConcat (empty)', expr: p.constant('a').stringConcat(''), expected: 'a' },
                ],
                timestampTruncate: notImplementedInRust || {
                    name: 'timestampTruncate',
                    expr: p.constant(new Date('2024-01-01T12:00:00Z')).timestampTruncate('day', 'UTC'),
                    expected: Timestamp.fromDate(new Date('2024-01-01T00:00:00Z')),
                },
                documentId: notImplementedInRust || {
                    name: 'documentId',
                    expr: p.constant(fs.collection.doc('document-id')).documentId(),
                    expected: 'document-id',
                },
                exists: notImplementedInRust || [
                    { name: 'exists (false)', expr: p.field('nonExist').exists(), expected: false },
                    { name: 'exists (true)', expr: p.field('city').exists(), expected: true },
                ],
                arrayGet: notImplementedInRust || [
                    { name: 'arrayGet (first)', expr: p.array([1, 2]).arrayGet(0), expected: 1 },
                    { name: 'arrayGet (second)', expr: p.array([1, 2]).arrayGet(1), expected: 2 },
                ],
                arrayContainsAll: notImplementedInRust || [
                    { name: 'arrayContainsAll (true)', expr: p.array([1, 2]).arrayContainsAll(p.array([1, 2])), expected: true },
                    { name: 'arrayContainsAll (false)', expr: p.array([1, 2]).arrayContainsAll(p.array([1, 2, 3])), expected: false },
                ],
                mapGet: notImplementedInRust || { name: 'mapGet', expr: p.map({ a: 1 }).mapGet('a'), expected: 1 },
                mapRemove: notImplementedInRust || { name: 'mapRemove', expr: p.map({ a: 1 }).mapRemove('a'), expected: {} },
                length: notImplementedInRust || { name: 'length', expr: p.constant('aa').length(), expected: 2 },
                trim: notImplementedInRust || [
                    { name: 'trim (both)', expr: p.constant(' a ').trim(), expected: 'a' },
                    { name: 'trim (none)', expr: p.constant('a').trim(), expected: 'a' },
                    { name: 'trim (empty)', expr: p.constant('   ').trim(), expected: '' },
                ],
                endsWith: notImplementedInRust || [
                    { name: 'endsWith (true)', expr: p.constant('aa').endsWith('a'), expected: true },
                    { name: 'endsWith (false)', expr: p.constant('aa').endsWith('b'), expected: false },
                ],
                startsWith: notImplementedInRust || [
                    { name: 'startsWith (true)', expr: p.constant('aa').startsWith('a'), expected: true },
                    { name: 'startsWith (false)', expr: p.constant('aa').startsWith('b'), expected: false },
                ],
                substring: notImplementedInRust || [
                    { name: 'substring (middle)', expr: p.constant('abc').substring(1, 2), expected: 'bc' },
                    { name: 'substring (start)', expr: p.constant('abc').substring(0, 2), expected: 'ab' },
                ],
                join: notImplementedInRust || [
                    { name: 'join (comma)', expr: p.array(['a', 'b']).join(','), expected: 'a,b' },
                    { name: 'join (single)', expr: p.array(['a']).join(','), expected: 'a' },
                ],
                reverse: notImplementedInRust || { name: 'reverse', expr: p.array([1, 2]).reverse(), expected: [2, 1] },
                arraySum: notImplementedInRust || [
                    { name: 'arraySum (pos)', expr: p.array([1, 2]).arraySum(), expected: 3 },
                    { name: 'arraySum (neg/pos)', expr: p.array([-1, 1]).arraySum(), expected: 0 },
                ],
                stringReverse: notImplementedInRust || [
                    { name: 'stringReverse (ab)', expr: p.constant('ab').stringReverse(), expected: 'ba' },
                    { name: 'stringReverse (empty)', expr: p.constant('').stringReverse(), expected: '' },
                ],
                logicalMaximum: notImplementedInRust || {
                    name: 'logicalMaximum',
                    expr: p.constant(1).logicalMaximum(p.constant(2)),
                    expected: 2,
                },
                logicalMinimum: notImplementedInRust || {
                    name: 'logicalMinimum',
                    expr: p.constant(1).logicalMinimum(p.constant(2)),
                    expected: 1,
                },
                collectionId: notImplementedInRust || {
                    name: 'collectionId',
                    expr: p.constant(fs.collection.doc('b')).collectionId(),
                    expected: fs.collection.id,
                },
                timestampAdd: notImplementedInRust || {
                    name: 'timestampAdd',
                    expr: p.constant(new Date('2024-01-01T12:00:00Z')).timestampAdd('day', 1),
                    expected: Timestamp.fromDate(new Date('2024-01-02T12:00:00Z')),
                },
                timestampSubtract: notImplementedInRust || {
                    name: 'timestampSubtract',
                    expr: p.constant(new Date('2024-01-01T12:00:00Z')).timestampSubtract('day', 1),
                    expected: Timestamp.fromDate(new Date('2023-12-31T12:00:00Z')),
                },
                timestampToUnixMicros: notImplementedInRust || {
                    name: 'timestampToUnixMicros',
                    expr: p.constant(new Date('2024-01-01T12:00:00Z')).timestampToUnixMicros(),
                    expected: new Date('2024-01-01T12:00:00Z').getTime() * 1000,
                },
                timestampToUnixSeconds: notImplementedInRust || {
                    name: 'timestampToUnixSeconds',
                    expr: p.constant(new Date('2024-01-01T12:00:00Z')).timestampToUnixSeconds(),
                    expected: new Date('2024-01-01T12:00:00Z').getTime() / 1000,
                },
                unixMicrosToTimestamp: notImplementedInRust || {
                    name: 'unixMicrosToTimestamp',
                    expr: p.constant(1704110400000000).unixMicrosToTimestamp(),
                    expected: Timestamp.fromDate(new Date('2024-01-01T12:00:00Z')),
                },
                timestampToUnixMillis: notImplementedInRust || {
                    name: 'timestampToUnixMillis',
                    expr: p.constant(new Date('2024-01-01T12:00:00Z')).timestampToUnixMillis(),
                    expected: new Date('2024-01-01T12:00:00Z').getTime(),
                },
                unixSecondsToTimestamp: notImplementedInRust || {
                    name: 'unixSecondsToTimestamp',
                    expr: p.constant(1704110400).unixSecondsToTimestamp(),
                    expected: Timestamp.fromDate(new Date('2024-01-01T12:00:00Z')),
                },
                unixMillisToTimestamp: notImplementedInRust || {
                    name: 'unixMillisToTimestamp',
                    expr: p.constant(1704110400000).unixMillisToTimestamp(),
                    expected: Timestamp.fromDate(new Date('2024-01-01T12:00:00Z')),
                },
                floor: { name: 'floor', expr: p.constant(1.5).floor(), expected: 1 },
                equalAny: notImplementedInRust || [
                    { name: 'equalAny (true)', expr: p.constant(1).equalAny(p.array([1, 2])), expected: true },
                    { name: 'equalAny (false)', expr: p.constant(3).equalAny(p.array([1, 2])), expected: false },
                ],
                notEqualAny: notImplementedInRust || [
                    { name: 'notEqualAny (true)', expr: p.constant(1).notEqualAny(p.array([2, 3])), expected: true },
                    { name: 'notEqualAny (false)', expr: p.constant(1).notEqualAny(p.array([1, 2])), expected: false },
                ],
                like: notImplementedInRust || [
                    { name: 'like (true)', expr: p.constant('abc').like('ab%'), expected: true },
                    { name: 'like (false)', expr: p.constant('abc').like('x%'), expected: false },
                ],
                cosineDistance: notImplementedInRust || {
                    name: 'cosineDistance',
                    expr: p.constant(FieldValue.vector([1, 2])).cosineDistance(p.constant(FieldValue.vector([1, 2]))),
                    expected: 0,
                },
                euclideanDistance: notImplementedInRust || {
                    name: 'euclideanDistance',
                    expr: p.constant(FieldValue.vector([1, 2])).euclideanDistance(p.constant(FieldValue.vector([1, 2]))),
                    expected: 0,
                },
                dotProduct: notImplementedInRust || {
                    name: 'dotProduct',
                    expr: p.constant(FieldValue.vector([1, 2])).dotProduct(p.constant(FieldValue.vector([1, 2]))),
                    expected: 5,
                },
                vectorLength: notImplementedInRust || {
                    name: 'vectorLength',
                    expr: p.constant(FieldValue.vector([1, 2])).vectorLength(),
                    expected: 2,
                },
                exp: notImplementedInRust || { name: 'exp', expr: p.constant(2).exp(), expected: expect.anything() },
                regexMatch: notImplementedInRust || [
                    { name: 'regexMatch (true)', expr: p.constant('abc').regexMatch('.*b.*'), expected: true },
                    { name: 'regexMatch (false)', expr: p.constant('abc').regexMatch('.*d.*'), expected: false },
                ],
                regexContains: notImplementedInRust || [
                    { name: 'regexContains (true)', expr: p.constant('abc').regexContains('b'), expected: true },
                    { name: 'regexContains (false)', expr: p.constant('abc').regexContains('d'), expected: false },
                ],
                regexFind: notImplementedInRust || { name: 'regexFind', expr: p.constant('abc').regexFind('[bd]'), expected: 'b' },
                regexFindAll: notImplementedInRust || {
                    name: 'regexFindAll',
                    expr: p.constant('abcd').regexFindAll('[bd]'),
                    expected: ['b', 'd'],
                },
                stringContains: notImplementedInRust || [
                    { name: 'stringContains (true)', expr: p.constant('abc').stringContains('b'), expected: true },
                    { name: 'stringContains (false)', expr: p.constant('abc').stringContains('d'), expected: false },
                ],
            };

            const exprExportTests: Record<RelevantExportedFunctions, OperatorTestCase | OperatorTestCase[]> = {
                abs: [
                    { name: 'abs (neg)', expr: p.abs(p.constant(-10)), expected: 10 },
                    { name: 'abs (pos)', expr: p.abs(p.constant(10)), expected: 10 },
                    { name: 'abs (zero)', expr: p.abs(p.constant(0)), expected: 0 },
                ],
                ceil: [
                    { name: 'ceil (pos)', expr: p.ceil(p.constant(1.5)), expected: 2 },
                    { name: 'ceil (neg)', expr: p.ceil(p.constant(-1.5)), expected: -1 },
                ],
                pow: [
                    { name: 'pow (int)', expr: p.pow(p.constant(2), 3), expected: 8 },
                    { name: 'pow (zero)', expr: p.pow(p.constant(9), 0), expected: 1 },
                ],
                sqrt: [
                    { name: 'sqrt (pos)', expr: p.sqrt(p.constant(9)), expected: 3 },
                    { name: 'sqrt (zero)', expr: p.sqrt(p.constant(0)), expected: 0 },
                ],
                log10: { name: 'log10', expr: p.log10(p.constant(100)), expected: 2 },
                ln: { name: 'ln', expr: p.ln(p.constant(Math.E)), expected: expect.closeTo(1) },
                round: [
                    { name: 'round (1.5)', expr: p.round(p.constant(1.5)), expected: 2 },
                    { name: 'round (1.4)', expr: p.round(p.constant(1.4)), expected: 1 },
                    { name: 'round (-1.5)', expr: p.round(p.constant(-1.5)), expected: -2 },
                ],
                add: [
                    { name: 'add (pos)', expr: p.add(p.constant(5), 2), expected: 7 },
                    { name: 'add (neg)', expr: p.add(p.constant(5), -2), expected: 3 },
                    { name: 'add (zero)', expr: p.add(p.constant(5), 0), expected: 5 },
                ],
                subtract: [
                    { name: 'subtract (pos)', expr: p.subtract(p.constant(5), 2), expected: 3 },
                    { name: 'subtract (neg)', expr: p.subtract(p.constant(5), -2), expected: 7 },
                ],
                multiply: [
                    { name: 'multiply (pos)', expr: p.multiply(p.constant(5), 2), expected: 10 },
                    { name: 'multiply (neg)', expr: p.multiply(p.constant(5), -2), expected: -10 },
                    { name: 'multiply (zero)', expr: p.multiply(p.constant(5), 0), expected: 0 },
                ],
                divide: { name: 'divide', expr: p.divide(p.constant(10), 2), expected: 5 },
                mod: [
                    { name: 'mod (pos)', expr: p.mod(p.constant(10), p.constant(3)), expected: 1 },
                    { name: 'mod (exact)', expr: p.mod(p.constant(10), p.constant(5)), expected: 0 },
                ],

                equal: [
                    { name: 'equal (true)', expr: p.equal(p.constant(1), 1), expected: true },
                    { name: 'equal (false)', expr: p.equal(p.constant(1), 2), expected: false },
                ],
                notEqual: [
                    { name: 'notEqual (true)', expr: p.notEqual(p.constant(1), 2), expected: true },
                    { name: 'notEqual (false)', expr: p.notEqual(p.constant(1), 1), expected: false },
                ],
                lessThan: [
                    { name: 'lessThan (true)', expr: p.lessThan(p.constant(1), 2), expected: true },
                    { name: 'lessThan (false)', expr: p.lessThan(p.constant(2), 1), expected: false },
                    { name: 'lessThan (equal)', expr: p.lessThan(p.constant(1), 1), expected: false },
                ],
                lessThanOrEqual: [
                    { name: 'lessThanOrEqual (true)', expr: p.lessThanOrEqual(p.constant(1), 2), expected: true },
                    { name: 'lessThanOrEqual (equal)', expr: p.lessThanOrEqual(p.constant(1), 1), expected: true },
                    { name: 'lessThanOrEqual (false)', expr: p.lessThanOrEqual(p.constant(2), 1), expected: false },
                ],
                greaterThan: [
                    { name: 'greaterThan (true)', expr: p.greaterThan(p.constant(2), 1), expected: true },
                    { name: 'greaterThan (false)', expr: p.greaterThan(p.constant(1), 2), expected: false },
                    { name: 'greaterThan (equal)', expr: p.greaterThan(p.constant(1), 1), expected: false },
                ],
                greaterThanOrEqual: [
                    { name: 'greaterThanOrEqual (true)', expr: p.greaterThanOrEqual(p.constant(2), 1), expected: true },
                    { name: 'greaterThanOrEqual (equal)', expr: p.greaterThanOrEqual(p.constant(2), 2), expected: true },
                    { name: 'greaterThanOrEqual (false)', expr: p.greaterThanOrEqual(p.constant(1), 2), expected: false },
                ],
                isAbsent: [
                    { name: 'isAbsent (true)', expr: p.isAbsent(p.field('nonExist')), expected: true },
                    { name: 'isAbsent (false)', expr: p.isAbsent(p.field('city')), expected: false },
                ],
                isError: [
                    { name: 'isError (true)', expr: p.isError(p.constant(1).divide(0)), expected: true },
                    { name: 'isError (false)', expr: p.isError(p.constant(1).divide(1)), expected: false },
                ],

                toUpper: [
                    { name: 'toUpper (lower)', expr: p.toUpper(p.constant('v')), expected: 'V' },
                    { name: 'toUpper (upper)', expr: p.toUpper(p.constant('V')), expected: 'V' },
                ],
                toLower: [
                    { name: 'toLower (upper)', expr: p.toLower(p.constant('V')), expected: 'v' },
                    { name: 'toLower (lower)', expr: p.toLower(p.constant('v')), expected: 'v' },
                ],
                charLength: notImplementedInRust || { name: 'charLength', expr: p.charLength(p.constant('123')), expected: 3 },
                split: [
                    { name: 'split (match)', expr: p.split(p.constant('a,b'), ','), expected: ['a', 'b'] },
                    { name: 'split (no match)', expr: p.split(p.constant('a%b'), ','), expected: ['a%b'] },
                ],

                ifAbsent: notImplementedInRust || {
                    name: 'ifAbsent',
                    expr: p.ifAbsent(p.field('nonExist'), p.constant('def')),
                    expected: 'def',
                },
                ifError: notImplementedInRust || { name: 'ifError', expr: p.ifError(p.constant(1).divide(0), p.constant(0)), expected: 0 },
                arrayConcat: { name: 'arrayConcat', expr: p.arrayConcat(p.array([1]), [2]), expected: [1, 2] },
                arrayLength: [
                    { name: 'arrayLength (2)', expr: p.arrayLength(p.array([1, 2])), expected: 2 },
                    { name: 'arrayLength (0)', expr: p.arrayLength(p.array([] as any[])), expected: 0 },
                ],
                arrayContains: notImplementedInRust || [
                    { name: 'arrayContains (true)', expr: p.arrayContains(p.array([1, 2]), 2), expected: true },
                    { name: 'arrayContains (false)', expr: p.arrayContains(p.array([1, 2]), 3), expected: false },
                ],
                arrayContainsAny: notImplementedInRust || [
                    { name: 'arrayContainsAny (true)', expr: p.arrayContainsAny(p.array([1, 2]), [2, 3]), expected: true },
                    { name: 'arrayContainsAny (false)', expr: p.arrayContainsAny(p.array([1, 2]), [3, 4]), expected: false },
                ],
                mapMerge: notImplementedInRust || [
                    { name: 'mapMerge (unique)', expr: p.mapMerge(p.map({ a: 1 }), p.map({ b: 2 })), expected: { a: 1, b: 2 } },
                    { name: 'mapMerge (overwrite)', expr: p.mapMerge(p.map({ a: 1 }), p.map({ a: 2 })), expected: { a: 2 } },
                ],
                type: notImplementedInRust || [
                    { name: 'type (string)', expr: p.type(p.constant('str1')), expected: 'string' },
                    { name: 'type (int64)', expr: p.type(p.constant(1.0)), expected: 'int64' },
                    { name: 'type (boolean)', expr: p.type(p.constant(true)), expected: 'boolean' },
                ],

                stringConcat: [
                    { name: 'stringConcat (2 args)', expr: p.stringConcat(p.constant('a'), p.constant('b')), expected: 'ab' },
                    { name: 'stringConcat (empty)', expr: p.stringConcat(p.constant('a'), p.constant('')), expected: 'a' },
                ],
                arrayReverse: notImplementedInRust || { name: 'arrayReverse', expr: p.arrayReverse(p.array([1, 2])), expected: [2, 1] },
                currentTimestamp: notImplementedInRust || {
                    name: 'currentTimestamp',
                    expr: p.currentTimestamp().type(),
                    expected: 'timestamp',
                },
                timestampTruncate: notImplementedInRust || {
                    name: 'timestampTruncate',
                    expr: p.timestampTruncate(p.constant(new Date('2024-01-01T12:00:00Z')), 'day'),
                    expected: Timestamp.fromDate(new Date('2024-01-01T00:00:00Z')),
                },
                documentId: notImplementedInRust || {
                    name: 'documentId',
                    expr: p.documentId(fs.collection.doc('document-id')),
                    expected: 'document-id',
                },
                exists: notImplementedInRust || [
                    { name: 'exists (false)', expr: p.exists(p.field('nonExist')), expected: false },
                    { name: 'exists (true)', expr: p.exists(p.field('city')), expected: true },
                ],
                arrayGet: notImplementedInRust || [
                    { name: 'arrayGet (first)', expr: p.arrayGet(p.array([1, 2]), 0), expected: 1 },
                    { name: 'arrayGet (second)', expr: p.arrayGet(p.array([1, 2]), 1), expected: 2 },
                ],
                arrayContainsAll: notImplementedInRust || [
                    { name: 'arrayContainsAll (true)', expr: p.arrayContainsAll(p.array([1, 2]), p.array([1, 2])), expected: true },
                    { name: 'arrayContainsAll (false)', expr: p.arrayContainsAll(p.array([1, 2]), p.array([1, 2, 3])), expected: false },
                ],
                mapGet: notImplementedInRust || { name: 'mapGet', expr: p.mapGet(p.map({ a: 1 }), 'a'), expected: 1 },
                mapRemove: notImplementedInRust || { name: 'mapRemove', expr: p.mapRemove(p.map({ a: 1 }), 'a'), expected: {} },
                byteLength: notImplementedInRust || { name: 'byteLength', expr: p.byteLength(p.constant('aa')), expected: 2 },
                length: notImplementedInRust || { name: 'length', expr: p.length(p.constant('aa')), expected: 2 },
                trim: notImplementedInRust || [
                    { name: 'trim (both)', expr: p.trim(p.constant(' a ')), expected: 'a' },
                    { name: 'trim (none)', expr: p.trim(p.constant('a')), expected: 'a' },
                    { name: 'trim (empty)', expr: p.trim(p.constant('   ')), expected: '' },
                ],
                endsWith: notImplementedInRust || [
                    { name: 'endsWith (true)', expr: p.endsWith(p.constant('aa'), 'a'), expected: true },
                    { name: 'endsWith (false)', expr: p.endsWith(p.constant('aa'), 'b'), expected: false },
                ],
                startsWith: notImplementedInRust || [
                    { name: 'startsWith (true)', expr: p.startsWith(p.constant('aa'), 'a'), expected: true },
                    { name: 'startsWith (false)', expr: p.startsWith(p.constant('aa'), 'b'), expected: false },
                ],
                substring: notImplementedInRust || [
                    { name: 'substring (middle)', expr: p.substring(p.constant('abc'), 1, 2), expected: 'bc' },
                    { name: 'substring (start)', expr: p.substring(p.constant('abc'), 0, 2), expected: 'ab' },
                ],
                join: notImplementedInRust || [
                    { name: 'join (comma)', expr: p.join(p.array(['a', 'b']), ','), expected: 'a,b' },
                    { name: 'join (single)', expr: p.join(p.array(['a']), ','), expected: 'a' },
                ],
                reverse: notImplementedInRust || { name: 'reverse', expr: p.reverse(p.array([1, 2])), expected: [2, 1] },
                arraySum: notImplementedInRust || [
                    { name: 'arraySum (pos)', expr: p.arraySum(p.array([1, 2])), expected: 3 },
                    { name: 'arraySum (neg/pos)', expr: p.arraySum(p.array([-1, 1])), expected: 0 },
                ],
                stringReverse: notImplementedInRust || [
                    { name: 'stringReverse (ab)', expr: p.stringReverse(p.constant('ab')), expected: 'ba' },
                    { name: 'stringReverse (empty)', expr: p.stringReverse(p.constant('')), expected: '' },
                ],
                logicalMaximum: notImplementedInRust || {
                    name: 'logicalMaximum',
                    expr: p.logicalMaximum(p.constant(1), p.constant(2)),
                    expected: 2,
                },
                logicalMinimum: notImplementedInRust || {
                    name: 'logicalMinimum',
                    expr: p.logicalMinimum(p.constant(1), p.constant(2)),
                    expected: 1,
                },
                collectionId: notImplementedInRust || {
                    name: 'collectionId',
                    expr: p.collectionId(p.constant(fs.collection.doc('document-id'))),
                    expected: fs.collection.id,
                },
                timestampAdd: notImplementedInRust || {
                    name: 'timestampAdd',
                    expr: p.timestampAdd(p.constant(new Date('2024-01-01T12:00:00Z')), 'day', 1),
                    expected: Timestamp.fromDate(new Date('2024-01-02T12:00:00Z')),
                },
                timestampSubtract: notImplementedInRust || {
                    name: 'timestampSubtract',
                    expr: p.timestampSubtract(p.constant(new Date('2024-01-01T12:00:00Z')), 'day', 1),
                    expected: Timestamp.fromDate(new Date('2023-12-31T12:00:00Z')),
                },
                timestampToUnixMicros: notImplementedInRust || {
                    name: 'timestampToUnixMicros',
                    expr: p.timestampToUnixMicros(p.constant(new Date('2024-01-01T12:00:00Z'))),
                    expected: new Date('2024-01-01T12:00:00Z').getTime() * 1000,
                },
                timestampToUnixSeconds: notImplementedInRust || {
                    name: 'timestampToUnixSeconds',
                    expr: p.timestampToUnixSeconds(p.constant(new Date('2024-01-01T12:00:00Z'))),
                    expected: new Date('2024-01-01T12:00:00Z').getTime() / 1000,
                },
                unixMicrosToTimestamp: notImplementedInRust || {
                    name: 'unixMicrosToTimestamp',
                    expr: p.unixMicrosToTimestamp(p.constant(1704110400000000)),
                    expected: Timestamp.fromDate(new Date('2024-01-01T12:00:00Z')),
                },
                timestampToUnixMillis: notImplementedInRust || {
                    name: 'timestampToUnixMillis',
                    expr: p.timestampToUnixMillis(p.constant(new Date('2024-01-01T12:00:00Z'))),
                    expected: new Date('2024-01-01T12:00:00Z').getTime(),
                },
                unixSecondsToTimestamp: notImplementedInRust || {
                    name: 'unixSecondsToTimestamp',
                    expr: p.unixSecondsToTimestamp(p.constant(1704110400)),
                    expected: Timestamp.fromDate(new Date('2024-01-01T12:00:00Z')),
                },
                unixMillisToTimestamp: notImplementedInRust || {
                    name: 'unixMillisToTimestamp',
                    expr: p.unixMillisToTimestamp(p.constant(1704110400000)),
                    expected: Timestamp.fromDate(new Date('2024-01-01T12:00:00Z')),
                },
                equalAny: notImplementedInRust || [
                    { name: 'equalAny (true)', expr: p.equalAny(p.constant(1), p.array([1, 2])), expected: true },
                    { name: 'equalAny (false)', expr: p.equalAny(p.constant(3), p.array([1, 2])), expected: false },
                ],
                notEqualAny: notImplementedInRust || [
                    { name: 'notEqualAny (true)', expr: p.notEqualAny(p.constant(1), p.array([2, 3])), expected: true },
                    { name: 'notEqualAny (false)', expr: p.notEqualAny(p.constant(1), p.array([1, 2])), expected: false },
                ],
                like: notImplementedInRust || [
                    { name: 'like (true)', expr: p.like(p.constant('abc'), 'ab%'), expected: true },
                    { name: 'like (false)', expr: p.like(p.constant('abc'), 'x%'), expected: false },
                ],
                cosineDistance: notImplementedInRust || {
                    name: 'cosineDistance',
                    expr: p.cosineDistance(p.constant(FieldValue.vector([1, 2])), p.constant(FieldValue.vector([1, 2]))),
                    expected: 0,
                },
                euclideanDistance: notImplementedInRust || {
                    name: 'euclideanDistance',
                    expr: p.euclideanDistance(p.constant(FieldValue.vector([1, 2])), p.constant(FieldValue.vector([1, 2]))),
                    expected: 0,
                },
                dotProduct: notImplementedInRust || {
                    name: 'dotProduct',
                    expr: p.dotProduct(p.constant(FieldValue.vector([1, 2])), p.constant(FieldValue.vector([1, 2]))),
                    expected: 5,
                },
                vectorLength: notImplementedInRust || {
                    name: 'vectorLength',
                    expr: p.vectorLength(p.constant(FieldValue.vector([1, 2]))),
                    expected: 2,
                },
                exp: notImplementedInRust || { name: 'exp', expr: p.exp(p.constant(2)), expected: expect.anything() },
                regexMatch: notImplementedInRust || [
                    { name: 'regexMatch (true)', expr: p.regexMatch(p.constant('abc'), '.*b.*'), expected: true },
                    { name: 'regexMatch (false)', expr: p.regexMatch(p.constant('abc'), '.*d.*'), expected: false },
                ],
                regexContains: notImplementedInRust || [
                    { name: 'regexContains (true)', expr: p.regexContains(p.constant('abc'), 'b'), expected: true },
                    { name: 'regexContains (false)', expr: p.regexContains(p.constant('abc'), 'd'), expected: false },
                ],
                stringContains: notImplementedInRust || [
                    { name: 'stringContains (true)', expr: p.stringContains(p.constant('abc'), 'b'), expected: true },
                    { name: 'stringContains (false)', expr: p.stringContains(p.constant('abc'), 'd'), expected: false },
                ],

                field: { name: 'field', expr: p.field('population'), expected: 160000 },
                constant: { name: 'constant', expr: p.constant('yup'), expected: 'yup' },
                map: notImplementedInRust || { name: 'map', expr: p.map({ a: p.constant(1) }), expected: { a: 1 } },
                array: { name: 'array', expr: p.array([p.constant(1), p.constant(2)]), expected: [1, 2] },
                concat: notImplementedInRust || [
                    { name: 'concat (string)', expr: p.concat(p.constant('a'), p.constant('b')), expected: 'ab' },
                    { name: 'concat (array)', expr: p.concat(p.array([1]), p.array([2])), expected: [1, 2] },
                    {
                        name: 'concat (blob)',
                        expr: p.concat(p.constant(new Uint8Array([1, 2])), p.constant(new Uint8Array([3, 4]))),
                        expected: new Uint8Array([1, 2, 3, 4]),
                    },
                ],
            };

            const logicTests: Record<LogicOperators, OperatorTestCase | OperatorTestCase[]> = {
                and: [
                    { name: 'and (true)', expr: p.and(p.constant(true), p.constant(true)), expected: true },
                    { name: 'and (false)', expr: p.and(p.constant(true), p.constant(false)), expected: false },
                ],
                or: [
                    { name: 'or (true)', expr: p.or(p.constant(false), p.constant(true)), expected: true },
                    { name: 'or (false)', expr: p.or(p.constant(false), p.constant(false)), expected: false },
                ],
                xor: notImplementedInRust || [
                    { name: 'xor (true, false)', expr: p.xor(p.constant(true), p.constant(false)), expected: true },
                    { name: 'xor (false, false)', expr: p.xor(p.constant(false), p.constant(false)), expected: false },
                    { name: 'xor (true, true)', expr: p.xor(p.constant(true), p.constant(true)), expected: false },
                ],
                not: [
                    { name: 'not (true)', expr: p.constant(false).not(), expected: true },
                    { name: 'not (false)', expr: p.constant(true).not(), expected: false },
                ],
                conditional: notImplementedInRust || {
                    name: 'conditional',
                    expr: p.constant(true).conditional(p.constant('yes'), p.constant('no')),
                    expected: 'yes',
                },
            };

            const allValidScalars = [
                ...Object.values(exprMethodTests).flat(),
                ...Object.values(exprExportTests).flat(),
                ...Object.values(logicTests).flat(),
            ];

            describe.each(allValidScalars)('Exhaustively mapping scalar operators against the SDK: $name', ({ expr, expected }) => {
                test.concurrent('evaluates structurally', async () => {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(p.field('city').equal('Haarlem'))
                        .limit(1)
                        .select(expr.as('res'));

                    const snap = await pipe.execute();
                    const result = snap.results[0].data();
                    expect(result.res).toEqual(expected);
                });
            });

            test.concurrent.each(brokenSDKExports)('Broken SDK export %s natively throws TypeError before networking', fnName => {
                expect(p[fnName] satisfies Function).toBeUndefined();
            });

            test.concurrent('all AggregateOperators', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .aggregate(
                        p.countAll().as('countAll'),
                        ...(notImplementedInRust || [
                            p.count(p.field('zero')).as('count'),
                            p.countIf(p.field('population').greaterThan(200000)).as('countIf'),
                            p.countDistinct(p.field('region')).as('countDistinct'),
                        ]),
                        p.sum('population').as('sum'),
                        p.average('area').as('average'),
                        p.maximum('population').as('maximum'),
                        p.minimum('population').as('minimum'),
                    );

                const snap = await pipe.execute();
                expect(snap).toBeDefined();
                const results = snap.results.map(r => r.data());

                expect(results).toEqual([
                    {
                        count: notImplementedInRust ? undefined : 1,
                        countAll: 5,
                        countIf: notImplementedInRust ? undefined : 3,
                        countDistinct: notImplementedInRust ? undefined : 3,
                        sum: 2230000,
                        average: expect.closeTo(147.4, 1),
                        maximum: 900000,
                        minimum: 160000,
                    } satisfies Record<AggregateOperators, unknown>,
                ]);
            });
        });
    });
});
