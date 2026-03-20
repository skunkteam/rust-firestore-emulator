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
    });
});
