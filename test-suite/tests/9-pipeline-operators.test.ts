import { CollectionReference, DocumentData } from '@google-cloud/firestore';
import {
    abs,
    add,
    ascending,
    average,
    ceil,
    charLength,
    constant,
    countAll,
    divide,
    Expression,
    field,
    isAbsent,
    isError,
    ln,
    log10,
    maximum,
    minimum,
    mod,
    multiply,
    pow,
    round,
    split,
    sqrt,
    stringConcat,
    subtract,
    sum,
    toLower,
    toUpper,
} from '@google-cloud/firestore/pipelines';
import { editions, notImplementedInRust, writeData } from './utils';

// TODO: exclude pipelines from Rust emulator tests for now...
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

        notImplementedInRust ||
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

        notImplementedInRust ||
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

        notImplementedInRust ||
            test('limit clause before aggregate', async () => {
                const pipe = fs.firestore.pipeline().collection(cityRef).limit(2).aggregate(countAll().as('totalCount'));
                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());

                expect(results).toEqual([{ totalCount: 2 }]);
            });

        notImplementedInRust ||
            test('limit negative edge case', async () => {
                const pipe = fs.firestore.pipeline().collection(cityRef).limit(-1);
                await expect(pipe.execute()).rejects.toThrow('3 INVALID_ARGUMENT');
            });

        notImplementedInRust ||
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

        notImplementedInRust ||
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
            notImplementedInRust ||
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

            notImplementedInRust ||
                test('offset and limit', async () => {
                    const pipe = fs.firestore.pipeline().collection(cityRef).sort(ascending('city')).offset(2).limit(2);

                    const snap = await pipe.execute();
                    const results = snap.results.map(r => r.data());
                    expect(results).toHaveLength(2);
                    expect(results[0].city).toBe('Haarlem');
                    expect(results[1].city).toBe('Rotterdam');
                });

            notImplementedInRust ||
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

            notImplementedInRust ||
                test('distinct on missing field', async () => {
                    const pipe = fs.firestore.pipeline().collection(cityRef).distinct('missingField');
                    const snap = await pipe.execute();
                    const results = snap.results.map(r => r.data());
                    expect(results).toHaveLength(1);
                    expect(results[0]).toEqual({ missingField: null });
                });

            notImplementedInRust ||
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
            notImplementedInRust ||
                test('math expressions', async () => {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(field('city').equal('Haarlem')) // population: 160000, area: 32
                        .limit(1)
                        .select(
                            add(field('population'), constant(5)).as('add'),
                            subtract(field('population'), constant(5)).as('sub'),
                            multiply(field('area'), constant(2)).as('mul'),
                            divide(field('population'), constant(2)).as('div'),
                            mod(field('population'), constant(3)).as('mod'),
                            abs(constant(-10)).as('abs'),
                            ceil(constant(1.5)).as('ceil'),
                            pow(field('area'), constant(2)).as('pow'),
                            log10(constant(100)).as('log10'),
                            ln(constant(Math.E)).as('ln'),
                            round(constant(1.5)).as('round'),
                            sqrt(constant(9)).as('sqrt'),
                        );
                    const snap = await pipe.execute();
                    const res = snap.results[0].data();
                    expect(res).toEqual({
                        add: 160005,
                        sub: 159995,
                        mul: 64,
                        div: 80000,
                        mod: 160000 % 3,
                        abs: 10,
                        ceil: 2,
                        pow: 1024,
                        log10: 2,
                        ln: expect.closeTo(1),
                        round: 2,
                        sqrt: 3,
                    });
                });

            notImplementedInRust ||
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
            notImplementedInRust ||
                test('logic operations', async () => {
                    const pipe = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(field('city').equal('Haarlem'))
                        .limit(1)
                        .select(
                            isAbsent(field('missingField')).as('isMissingFieldAbsent'),
                            isAbsent(field('city')).as('isCityAbsent'),
                            isError(divide(constant(1), constant(0))).as('divByZeroError'),
                            isError(divide(constant(1), constant(2))).as('divByTwoError'),
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

            notImplementedInRust ||
                test('math edge cases', async () => {
                    const expressions: Record<string, Expression> = {
                        divZero: divide(constant(1), constant(0)),
                        modZero: mod(constant(1), constant(0)),
                        sqrtNeg: sqrt(constant(-9)),
                        lnZero: ln(constant(0)),
                        log10Neg: log10(constant(-1)),
                        addStringNum: add(constant('String'), constant(5)),
                        divZeroField: divide(constant(1), field('zero')),
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
                        .select(add(field('missing'), constant(10)).as('addMissingNum'), abs(field('missing')).as('absMissing'));
                    const validMissingSnap = await validMissingPipe.execute();
                    expect(validMissingSnap.results[0].data()).toEqual({ addMissingNum: null, absMissing: null });
                });

            notImplementedInRust ||
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
                            split(constant('a,b'), constant(null)).as('splitNull'),
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

            notImplementedInRust ||
                test('array edge cases', async () => {
                    const pipeArrStr = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .limit(1)
                        .select(field('city').arrayLength().as('arrLenStr'));
                    await expect(pipeArrStr.execute()).rejects.toThrow(/3 INVALID_ARGUMENT/);

                    const pipeValid = fs.firestore
                        .pipeline()
                        .collection(cityRef)
                        .where(field('city').equal('Haarlem'))
                        .limit(1)
                        .select(field('arr').arrayConcat([2]).as('concatArrNum'));
                    const validSnap = await pipeValid.execute();
                    expect(validSnap.results[0].data()).toEqual({ concatArrNum: [1, 2, 2] });
                });
        });

        describe('extended aggregations', () => {
            notImplementedInRust ||
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
    });
});
