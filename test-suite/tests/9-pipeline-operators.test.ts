import { CollectionReference, DocumentData } from '@google-cloud/firestore';
import {
    and,
    ascending,
    average,
    charLength,
    constant,
    countAll,
    descending,
    Expression,
    field,
    isAbsent,
    isError,
    map,
    maximum,
    minimum,
    or,
    stringConcat,
    sum,
    toLower,
    toUpper,
} from '@google-cloud/firestore/pipelines';
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

        test('basic pipeline support', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef);
            const snap = await pipe.execute();
            expect(snap).toBeDefined();
            expect(snap.results).toHaveLength(5);
        });

        notImplementedInRust ||
            describe('pipeline source stages', () => {
                test('collectionGroup', async () => {
                    const pipe = fs.firestore.pipeline().collectionGroup('cities').sort(descending('population')).limit(1);
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Amsterdam', population: 900000 });
                });

                test('database', async () => {
                    const pipe = fs.firestore.pipeline().database().where(field('area').greaterThan(300)).limit(1);
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Rotterdam', area: 324 });
                });

                test('documents', async () => {
                    const snapshot = await cityRef.where('city', '==', 'Amersfoort').get();
                    const pipe = fs.firestore.pipeline().documents([snapshot.docs[0].ref]);
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Amersfoort', population: 160000 });
                });

                test('createFrom', async () => {
                    const pipe = fs.firestore.pipeline().createFrom(cityRef.where('city', '==', 'Haarlem'));
                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toMatchObject({ city: 'Haarlem', area: 32 });
                });
            });

        test('simple aggregate', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate(countAll().as('totalCount'), sum('population').as('totalPop'), average('area').as('avgArea'));

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

        test('grouping by region', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate({
                    accumulators: [countAll().as('count'), sum('population').as('pop')],
                    groups: ['region'],
                });

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toIncludeSameMembers([
                { region: 'Utrecht', count: 2, pop: 520000 },
                { region: 'Noord-Holland', count: 2, pop: 1060000 },
                { region: 'Zuid-Holland', count: 1, pop: 650000 },
            ]);
        });

        test('where clause before aggregate', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .where(field('population').greaterThan(200000))
                .aggregate(countAll().as('totalCount'), sum('population').as('totalPop'));

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([
                {
                    totalCount: 3, // Utrecht, Amsterdam, Rotterdam
                    totalPop: 1910000,
                },
            ]);
        });

        test('limit clause before aggregate', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef).limit(2).aggregate(countAll().as('totalCount'));
            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toEqual([{ totalCount: 2 }]);
        });

        test('limit negative edge case', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef).limit(-1);
            await expect(pipe.execute()).rejects.toThrow('3 INVALID_ARGUMENT');
        });

        test('aggregate on empty collection', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(emptyRef)
                .aggregate(countAll().as('totalCount'), sum('population').as('totalPop'), average('area').as('avgArea'));

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

        test('aggregate on missing fields', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate(sum('nonExistingField').as('sumMissing'), average('nonExistingField').as('avgMissing'));

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
            test('addFields, removeFields, select', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .addFields(stringConcat(constant('City: '), field('city')).as('newName'))
                    .select(field('newName').as('selectedName'), 'area')
                    .removeFields('area');

                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({ selectedName: 'City: Haarlem' });
            });

            test('offset and limit', async () => {
                const pipe = fs.firestore.pipeline().collection(cityRef).sort(ascending('city')).offset(2).limit(2);

                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(2);
                expect(results[0].city).toBe('Haarlem');
                expect(results[1].city).toBe('Rotterdam');
            });

            test('distinct', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .distinct(stringConcat(constant('Region: '), field('region')).as('regionPrefix'));

                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(3);
                const regions = results.map(r => r.regionPrefix).sort();
                expect(regions).toEqual(['Region: Noord-Holland', 'Region: Utrecht', 'Region: Zuid-Holland']);
            });

            test('distinct on missing field', async () => {
                const pipe = fs.firestore.pipeline().collection(cityRef).distinct('missingField');
                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(1);
                expect(results[0]).toEqual({ missingField: null });
            });

            test('aggregate with missing grouping field', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .aggregate({
                        accumulators: [countAll().as('count')],
                        groups: ['missingField'],
                    });
                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(1);
                expect(results[0]).toEqual({ count: 5, missingField: null });
            });
        });

        describe('expression groups', () => {
            test('math expressions', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem')) // population: 160000, area: 32
                    .limit(1)
                    .select(
                        field('population').add(5).as('add'),
                        field('population').subtract(5).as('sub'),
                        field('area').multiply(2).as('mul'),
                        field('population').divide(2).as('div'),

                        // Note the danger of using the JavaScript SDK combined with server-side
                        // computation, JavaScript only has one numeric type (float64) while
                        // Firestore has two (integer and float). The SDK automatically detects
                        // integers and uses the integer type in communication with Firestore.
                        // This can lead to unexpected results if you are not careful. For example:
                        field('area').divide(5.0).as('integer_div'), // yields 6
                        field('area').add(0.5).subtract(0.5).divide(5.0).as('floating_div'), // yields 6.4
                        constant(1.0).type().as('integer_type'),
                        constant(1.1).type().as('float_type'),

                        field('population').mod(3).as('mod'),
                        constant(-10).abs().as('abs'),
                        constant(1.5).ceil().as('ceil'),
                        field('area').pow(2).as('pow'),
                        constant(100).log10().as('log10'),
                        constant(Math.E).ln().as('ln'),
                        constant(1.5).round().as('round'),
                        constant(9).sqrt().as('sqrt'),
                        field('population').equal(160000).as('eq'),
                        field('population').lessThanOrEqual(160000).as('lte'),
                        field('population').lessThan(160000).as('lt'),
                        field('population').greaterThanOrEqual(160000).as('gte'),
                        field('population').greaterThan(160000).as('gt'),
                        field('population').notEqual(160000).as('ne'),
                        constant('a,b').split(',').as('split'),
                        and(constant(false).not(), constant(true)).as('and'),
                        or(constant(false), constant(true)).as('or'),
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
                    integer_type: 'int64',
                    float_type: 'float64',

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

            test('string operations', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        toUpper(field('city')).as('upper'),
                        toLower(field('city')).as('lower'),
                        stringConcat(field('city'), constant(' test')).as('concat'),
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({
                    upper: 'HAARLEM',
                    lower: 'haarlem',
                    concat: 'Haarlem test',
                });
            });
            test('logic operations', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        isAbsent(field('missingField')).as('isMissingFieldAbsent'),
                        isAbsent(field('city')).as('isCityAbsent'),
                        isError(constant(1).divide(0)).as('divByZeroError'),
                        isError(constant(1).divide(2)).as('divByTwoError'),
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

            test('math edge cases', async () => {
                const expressions: Record<string, Expression> = {
                    divZero: constant(1).divide(0),
                    modZero: constant(1).mod(0),
                    sqrtNeg: constant(-9).sqrt(),
                    lnZero: constant(0).ln(),
                    log10Neg: constant(-1).log10(),
                    addStringNum: constant('String').add(5),
                    divZeroField: constant(1).divide(field('zero')),
                };

                for (const expr of Object.values(expressions)) {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(field('city').equal('Haarlem'))
                        .limit(1)
                        .select(expr.as('res'));
                    await expect(pipe.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);
                }

                // But inside isError they evaluate to true
                const isErrorPipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .select(isError(expressions.divZeroField).as('isErrorDivField'));
                const isErrorSnap = await isErrorPipe.execute();
                expect(isErrorSnap.results[0].data()).toEqual({ isErrorDivField: true });

                const validMissingPipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .select(field('missing').add(10).as('addMissingNum'), field('missing').abs().as('absMissing'));
                const validMissingSnap = await validMissingPipe.execute();
                expect(validMissingSnap.results[0].data()).toEqual({ addMissingNum: null, absMissing: null });
            });

            test('string edge cases', async () => {
                const expressions: Record<string, Expression> = {
                    toUpperNum: toUpper(constant(123)),
                    charLenNum: charLength(constant(123)),
                    toUpperNumField: toUpper(field('population')),
                };

                for (const expr of Object.values(expressions)) {
                    const pipe = fs.firestore.pipeline().collection(cityRef).limit(1).select(expr.as('res'));
                    await expect(pipe.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);
                }

                const validPipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        stringConcat(constant('a'), constant(null)).as('concatNull'),
                        stringConcat(constant('a'), field('missing')).as('concatMissing'),
                        constant('a,b').split(constant(null)).as('splitNull'),
                        isError(expressions.toUpperNumField).as('isErrorUpperNumField'),
                    );
                const validSnap = await validPipe.execute();
                expect(validSnap.results[0].data()).toEqual({
                    concatNull: null,
                    concatMissing: null,
                    splitNull: null,
                    isErrorUpperNumField: true,
                });
            });

            test('array edge cases', async () => {
                const pipeArrStr = fs.firestore.pipeline().collection(cityRef).limit(1).select(field('city').arrayLength().as('arrLenStr'));
                await expect(pipeArrStr.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);

                const pipeValid = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(field('city').equal('Haarlem'))
                    .limit(1)
                    .select(field('arr').arrayConcat([2, 3], [4, 5]).as('concatArrNum'));
                const validSnap = await pipeValid.execute();
                expect(validSnap.results[0].data()).toEqual({ concatArrNum: [1, 2, 2, 3, 4, 5] });
            });
        });

        describe('extended aggregations', () => {
            test('minimum and maximum', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .aggregate(minimum('population').as('minPop'), maximum('population').as('maxPop'));
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({ minPop: 160000, maxPop: 900000 });
            });
        });

        notImplementedInRust ||
            describe('uncharted pipeline stages', () => {
                test('replaceWith', async () => {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(field('city').equal('Haarlem'))
                        .limit(1)
                        .replaceWith(
                            map({
                                newField: stringConcat(field('city'), constant(' tests')),
                                pop: field('population'),
                            }),
                        );

                    const snap = await pipe.execute();
                    expect(snap.results).toHaveLength(1);
                    expect(snap.results[0].data()).toEqual({
                        newField: 'Haarlem tests',
                        pop: 160000,
                    });
                });

                test('sample', async () => {
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

                test('union', async () => {
                    // We combine pipeline targeting 'Haarlem' with pipeline targeting 'Amersfoort'
                    const pipe1 = fs.firestore.pipeline().collection(cityRef).where(field('city').equal('Haarlem'));
                    const pipe2 = fs.firestore.pipeline().collection(cityRef).where(field('city').equal('Amersfoort'));

                    const unionPipe = pipe1.union(pipe2);
                    const snap = await unionPipe.execute();

                    const cities = snap.results.map(r => r.get('city'));
                    expect(cities).toIncludeSameMembers(['Amersfoort', 'Haarlem']);
                });

                test('unnest', async () => {
                    // Haarlem has arr: [1, 2]. Unnesting should result in 2 documents
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(field('city').equal('Haarlem'))
                        .unnest(field('arr').as('item'), 'idx');

                    const snap = await pipe.execute();
                    const results = snap.results.map(r => r.data());

                    expect(results).toEqual([
                        expect.objectContaining({ item: 1, idx: 0, city: 'Haarlem' }),
                        expect.objectContaining({ item: 2, idx: 1, city: 'Haarlem' }),
                    ]);
                });
            });
    });
});
