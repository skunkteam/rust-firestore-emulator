import * as Pipelines from '@google-cloud/firestore/pipelines';
import assert from 'node:assert';
import { editions, writeData } from './utils';

const enterpriseDatabases = editions.filter(e => e.enterprise);
const suite = enterpriseDatabases.length ? describe.each(enterpriseDatabases) : describe.skip.each(editions);

suite('$description', fs => {
    describe('pipeline operations', () => {
        let cityRef: FirebaseFirestore.CollectionReference;
        let emptyRef: FirebaseFirestore.CollectionReference;

        beforeAll(async () => {
            cityRef = fs.collection.doc().collection('cities');
            emptyRef = fs.collection.doc().collection('empty');
            const cities = [
                { city: 'Amersfoort', population: 160000, area: 63, region: 'Utrecht' },
                { city: 'Utrecht', population: 360000, area: 99, region: 'Utrecht' },
                { city: 'Amsterdam', population: 900000, area: 219, region: 'Noord-Holland' },
                { city: 'Haarlem', population: 160000, area: 32, region: 'Noord-Holland' },
                { city: 'Rotterdam', population: 650000, area: 324, region: 'Zuid-Holland' },
            ];
            await Promise.all(cities.map(c => cityRef.add(writeData(c))));
        });

        test('simple aggregate', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate(
                    Pipelines.countAll().as('totalCount'),
                    Pipelines.sum('population').as('totalPop'),
                    Pipelines.average('area').as('avgArea'),
                );

            const snap = await pipe.execute();
            expect(snap).toBeDefined();
            const results = snap.results.map(r => r.data());

            expect(results).toHaveLength(1);
            expect(results[0].totalCount).toBe(5);
            expect(results[0].totalPop).toBe(2230000);
            expect(results[0].avgArea).toBeCloseTo(147.4, 1);
        });

        test('grouping by region', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate({
                    accumulators: [Pipelines.countAll().as('count'), Pipelines.sum('population').as('pop')],
                    groups: ['region'],
                });

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toHaveLength(3);
            const utrecht = results.find(r => r.region === 'Utrecht');
            assert(utrecht);
            expect(utrecht.count).toBe(2);
            expect(utrecht.pop).toBe(520000);

            const noordHolland = results.find(r => r.region === 'Noord-Holland');
            assert(noordHolland);
            expect(noordHolland.count).toBe(2);
            expect(noordHolland.pop).toBe(1060000);

            const zuidHolland = results.find(r => r.region === 'Zuid-Holland');
            assert(zuidHolland);
            expect(zuidHolland.count).toBe(1);
            expect(zuidHolland.pop).toBe(650000);
        });

        test('where clause before aggregate', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .where(Pipelines.field('population').greaterThan(200000))
                .aggregate(Pipelines.countAll().as('totalCount'), Pipelines.sum('population').as('totalPop'));

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toHaveLength(1);
            expect(results[0].totalCount).toBe(3); // Utrecht, Amsterdam, Rotterdam
            expect(results[0].totalPop).toBe(1910000);
        });

        test('limit clause before aggregate', async () => {
            const pipe = fs.firestore.pipeline().collection(cityRef).limit(2).aggregate(Pipelines.countAll().as('totalCount'));
            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toHaveLength(1);
            expect(results[0].totalCount).toBe(2);
        });

        test('aggregate on empty collection', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(emptyRef)
                .aggregate(
                    Pipelines.countAll().as('totalCount'),
                    Pipelines.sum('population').as('totalPop'),
                    Pipelines.average('area').as('avgArea'),
                );

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toHaveLength(1);
            expect(results[0].totalCount).toBe(0);
            expect(results[0].totalPop).toBeNull();
            expect(results[0].avgArea).toBeNull();
        });

        test('aggregate on missing fields', async () => {
            const pipe = fs.firestore
                .pipeline()
                .collection(cityRef)
                .aggregate(Pipelines.sum('nonExistingField').as('sumMissing'), Pipelines.average('nonExistingField').as('avgMissing'));

            const snap = await pipe.execute();
            const results = snap.results.map(r => r.data());

            expect(results).toHaveLength(1);
            expect(results[0].sumMissing).toBeNull();
            expect(results[0].avgMissing).toBeNull();
        });

        describe('extended pipeline stages', () => {
            test('addFields, removeFields, select', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(Pipelines.field('city').equal('Haarlem'))
                    .limit(1)
                    .addFields(
                        Pipelines.stringConcat(Pipelines.constant('City: '), Pipelines.field('city')).as('newName')
                    )
                    .select(Pipelines.field('newName').as('selectedName'), 'area')
                    .removeFields('area');
                
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res).toEqual({ selectedName: 'City: Haarlem' });
            });

            test('offset and limit', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .sort(Pipelines.ascending('city'))
                    .offset(2)
                    .limit(2);
                
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
                    .distinct(Pipelines.stringConcat(Pipelines.constant('Region: '), Pipelines.field('region')).as('regionPrefix'));
                
                const snap = await pipe.execute();
                const results = snap.results.map(r => r.data());
                expect(results).toHaveLength(3);
                const regions = results.map(r => r.regionPrefix).sort();
                expect(regions).toEqual(['Region: Noord-Holland', 'Region: Utrecht', 'Region: Zuid-Holland']);
            });
        });

        describe('expression groups', () => {
            test('math expressions', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(Pipelines.field('city').equal('Haarlem')) // population: 160000, area: 32
                    .limit(1)
                    .select(
                        Pipelines.add(Pipelines.field('population'), Pipelines.constant(5)).as('add'),
                        Pipelines.subtract(Pipelines.field('population'), Pipelines.constant(5)).as('sub'),
                        Pipelines.multiply(Pipelines.field('area'), Pipelines.constant(2)).as('mul'),
                        Pipelines.divide(Pipelines.field('population'), Pipelines.constant(2)).as('div'),
                        Pipelines.mod(Pipelines.field('population'), Pipelines.constant(3)).as('mod'),
                        Pipelines.abs(Pipelines.constant(-10)).as('abs'),
                        Pipelines.ceil(Pipelines.constant(1.5)).as('ceil'),
                        Pipelines.pow(Pipelines.field('area'), Pipelines.constant(2)).as('pow'),
                        Pipelines.log10(Pipelines.constant(100)).as('log10'),
                        Pipelines.ln(Pipelines.constant(Math.E)).as('ln'),
                        Pipelines.round(Pipelines.constant(1.5)).as('round'),
                        Pipelines.sqrt(Pipelines.constant(9)).as('sqrt')
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res.add).toBe(160005);
                expect(res.sub).toBe(159995);
                expect(res.mul).toBe(64);
                expect(res.div).toBe(80000);
                expect(res.mod).toBe(160000 % 3);
                expect(res.abs).toBe(10);
                expect(res.ceil).toBe(2);
                expect(res.pow).toBe(1024);
                expect(res.log10).toBe(2);
                expect(res.ln).toBeCloseTo(1);
                expect(res.round).toBe(2);
                expect(res.sqrt).toBe(3);
            });

            test('string operations', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(Pipelines.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        Pipelines.toUpper(Pipelines.field('city')).as('upper'),
                        Pipelines.toLower(Pipelines.field('city')).as('lower'),
                        Pipelines.stringConcat(Pipelines.field('city'), Pipelines.constant(' test')).as('concat')
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res.upper).toBe('HAARLEM');
                expect(res.lower).toBe('haarlem');
                expect(res.concat).toBe('Haarlem test');
            });
            test('logic operations', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .where(Pipelines.field('city').equal('Haarlem'))
                    .limit(1)
                    .select(
                        Pipelines.isAbsent(Pipelines.field('missingField')).as('isMissingFieldAbsent'),
                        Pipelines.isAbsent(Pipelines.field('city')).as('isCityAbsent'),
                        Pipelines.isError(Pipelines.divide(Pipelines.constant(1), Pipelines.constant(0))).as('divByZeroError'),
                        Pipelines.isError(Pipelines.divide(Pipelines.constant(1), Pipelines.constant(2))).as('divByTwoError')
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res.isMissingFieldAbsent).toBe(true);
                expect(res.isCityAbsent).toBe(false);
                expect(res.divByZeroError).toBe(true);
                expect(res.divByTwoError).toBe(false);
            });
        });

        describe('extended aggregations', () => {
            test('minimum and maximum', async () => {
                const pipe = fs.firestore
                    .pipeline()
                    .collection(cityRef)
                    .aggregate(
                        Pipelines.minimum('population').as('minPop'),
                        Pipelines.maximum('population').as('maxPop'),
                    );
                const snap = await pipe.execute();
                const res = snap.results[0].data();
                expect(res.minPop).toBe(160000);
                expect(res.maxPop).toBe(900000);
            });
        });
    });
});
