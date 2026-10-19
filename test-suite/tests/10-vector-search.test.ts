import { FieldPath, FieldValue, VectorValue } from '@google-cloud/firestore';
import { editions, notImplementedInJava, readData, readDataRef, writeData } from './utils';

describe.each(editions)('$description - vector search', fs => {
    test.concurrent('stores and retrieves VectorValue at top-level and nested paths', async () => {
        const docRef = fs.collection.doc();
        const topVector = FieldValue.vector([1.5, -2.25, 3.0]);
        const nestedVector = FieldValue.vector([0.1, 0.2, 0.3, 0.4]);

        await docRef.set(
            writeData({
                embedding: topVector,
                vectorQueryFields: {
                    nested: nestedVector,
                },
            }),
        );

        try {
            const data = await readDataRef<{
                embedding: VectorValue;
                vectorQueryFields: { nested: VectorValue };
            }>(docRef);

            expect(data.embedding).toBeInstanceOf(VectorValue);
            expect(data.embedding.toArray()).toEqual([1.5, -2.25, 3.0]);
            expect(data.vectorQueryFields.nested).toBeInstanceOf(VectorValue);
            expect(data.vectorQueryFields.nested.toArray()).toEqual([0.1, 0.2, 0.3, 0.4]);

            // Updating a nested path under a VectorValue replaces it with a map instead of mutating internal keys
            await docRef.update({ 'embedding.nested': 'replaced' });
            const updated = await readDataRef<{
                embedding: { nested: string };
            }>(docRef);
            expect(updated.embedding).toEqual({ nested: 'replaced' });
        } finally {
            await docRef.delete();
        }
    });

    test.concurrent('orders VectorValue between Array and Map, first by length then element-by-element', async () => {
        const subCol = fs.collection.doc().collection('collection');
        const docs = [
            { id: 'map', val: { a: 1 } },
            { id: 'vec-len-3', val: FieldValue.vector([1, 0, 0]) },
            { id: 'vec-len-2-high', val: FieldValue.vector([2, 0]) },
            { id: 'vec-len-2-low', val: FieldValue.vector([1, 5]) },
            { id: 'array', val: [99, 99, 99] },
        ];

        await Promise.all(docs.map(d => subCol.doc(d.id).set(writeData({ val: d.val }))));

        try {
            const snap = await subCol.orderBy('val').get();
            expect(snap.docs.map(d => d.id)).toEqual(['array', 'vec-len-2-low', 'vec-len-2-high', 'vec-len-3', 'map']);
        } finally {
            await Promise.all(docs.map(d => subCol.doc(d.id).delete()));
        }
    });

    describe('findNearest queries', () => {
        const subCol = fs.collection.doc().collection('collection');
        const seedDocs: Array<{ id: string } & Record<string, unknown>> = [
            {
                id: 'doc-origin-x',
                title: 'Origin X',
                category: 'tech',
                tags: ['nl', 'b2b'],
                embedding: FieldValue.vector([1, 0]),
                vectorQueryFields: { nested: FieldValue.vector([1, 0, 0]) },
            },
            {
                id: 'doc-diag',
                title: 'Diagonal',
                category: 'tech',
                tags: ['nl', 'b2c'],
                embedding: FieldValue.vector([3, 4]),
                vectorQueryFields: { nested: FieldValue.vector([3, 4, 0]) },
            },
            {
                id: 'doc-origin-y',
                title: 'Origin Y',
                category: 'finance',
                tags: ['nl', 'b2b'],
                embedding: FieldValue.vector([0, 2]),
                vectorQueryFields: { nested: FieldValue.vector([0, 2, 0]) },
            },
            {
                id: 'doc-neg-x',
                title: 'Negative X',
                category: 'tech',
                tags: ['be', 'b2b'],
                embedding: FieldValue.vector([-1, 0]),
                vectorQueryFields: { nested: FieldValue.vector([-1, 0, 0]) },
            },
            {
                id: 'doc-wrong-dim',
                title: 'Wrong Dimension',
                category: 'tech',
                tags: ['nl', 'b2b'],
                embedding: FieldValue.vector([1, 0, 0, 0]),
                vectorQueryFields: { nested: FieldValue.vector([1, 0]) },
            },
            {
                id: 'doc-non-vector',
                title: 'Not a vector',
                category: 'tech',
                tags: ['nl', 'b2b'],
                embedding: [1, 0],
                vectorQueryFields: { nested: 'plain-string' },
            },
            {
                id: 'doc-missing',
                title: 'Missing vector',
                category: 'tech',
                tags: ['nl', 'b2b'],
            },
        ];

        beforeAll(async () => {
            await Promise.all(seedDocs.map(({ id, ...rest }) => subCol.doc(id).set(writeData(rest))));
        });

        afterAll(async () => {
            await Promise.all(seedDocs.map(({ id }) => subCol.doc(id).delete()));
        });

        test.concurrent('EUCLIDEAN distance measure sorts ascending and populates distanceResultField', async () => {
            const snap = await subCol
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    distanceMeasure: 'EUCLIDEAN',
                    limit: 3,
                    distanceResultField: 'distance',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['doc-origin-x', 'doc-neg-x', 'doc-origin-y']);
            const distances = snap.docs.map(d => readData<{ distance: number }>(d.data()).distance);
            expect(distances[0]).toBeCloseTo(0, 6);
            expect(distances[1]).toBeCloseTo(2, 6);
            expect(distances[2]).toBeCloseTo(Math.sqrt(5), 6);
        });

        test.concurrent('COSINE distance measure sorts ascending and skips invalid/wrong-dimension vectors', async () => {
            const snap = await subCol
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    distanceMeasure: 'COSINE',
                    limit: 10,
                    distanceResultField: 'cosineDistance',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['doc-origin-x', 'doc-diag', 'doc-origin-y', 'doc-neg-x']);
            const distances = snap.docs.map(d => readData<{ cosineDistance: number }>(d.data()).cosineDistance);
            expect(distances[0]).toBeCloseTo(0, 6);
            expect(distances[1]).toBeCloseTo(0.4, 6);
            expect(distances[2]).toBeCloseTo(1, 6);
            expect(distances[3]).toBeCloseTo(2, 6);
        });

        test.concurrent('DOT_PRODUCT distance measure sorts descending (highest score first)', async () => {
            const snap = await subCol
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 1],
                    distanceMeasure: 'DOT_PRODUCT',
                    limit: 4,
                    distanceResultField: 'score',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['doc-diag', 'doc-origin-y', 'doc-origin-x', 'doc-neg-x']);
            const scores = snap.docs.map(d => readData<{ score: number }>(d.data()).score);
            expect(scores).toEqual([7, 2, 1, -1]);
        });

        test.concurrent('applies distanceThreshold for EUCLIDEAN, COSINE, and DOT_PRODUCT', async () => {
            const euclideanSnap = await subCol
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    distanceMeasure: 'EUCLIDEAN',
                    limit: 10,
                    distanceThreshold: 2.0,
                })
                .get();
            expect(euclideanSnap.docs.map(d => d.id)).toEqual(['doc-origin-x', 'doc-neg-x']);

            const cosineSnap = await subCol
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    distanceMeasure: 'COSINE',
                    limit: 10,
                    distanceThreshold: 0.5,
                })
                .get();
            expect(cosineSnap.docs.map(d => d.id)).toEqual(['doc-origin-x', 'doc-diag']);

            const dotSnap = await subCol
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 1],
                    distanceMeasure: 'DOT_PRODUCT',
                    limit: 10,
                    distanceThreshold: 2.0,
                })
                .get();
            expect(dotSnap.docs.map(d => d.id)).toEqual(['doc-diag', 'doc-origin-y']);
        });

        // Java emulator does not resolve nested FieldPaths for vectorField in findNearest
        notImplementedInJava ||
            test.concurrent('combines pre-filters (where == and array-contains) and nested vectorField', async () => {
                const vectorPath = new FieldPath('vectorQueryFields', 'nested');
                const snap = await subCol
                    .where('category', '==', 'tech')
                    .where('tags', 'array-contains', 'nl')
                    .findNearest({
                        vectorField: vectorPath,
                        queryVector: [1, 1, 0],
                        distanceMeasure: 'COSINE',
                        limit: 5,
                        distanceResultField: 'distance',
                    })
                    .get();

                // Only doc-origin-x and doc-diag match category=='tech', tags array-contains 'nl', and dim 3
                // Query [1, 1, 0] vs doc-diag [3, 4, 0]: dot = 7 / (sqrt(2)*5) = 7 / 7.071 = 0.9899 -> dist ~ 0.01005
                // Query [1, 1, 0] vs doc-origin-x [1, 0, 0]: dot = 1 / sqrt(2) = 0.7071 -> dist ~ 0.29289
                expect(snap.docs.map(d => d.id)).toEqual(['doc-diag', 'doc-origin-x']);

                const firstData = snap.docs[0].data();
                expect(firstData.title).toBe('Diagonal');
                expect(typeof firstData.distance).toBe('number');
                expect(firstData.distance).toBeCloseTo(1 - 7 / (Math.sqrt(2) * 5), 6);
            });

        test.concurrent('supports select projection with findNearest', async () => {
            const snap = await subCol
                .select('title', 'distance')
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    distanceMeasure: 'EUCLIDEAN',
                    limit: 2,
                    distanceResultField: 'distance',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['doc-origin-x', 'doc-neg-x']);
            const firstData = snap.docs[0].data();
            expect(firstData.title).toBe('Origin X');
            expect(firstData.category).toBeUndefined();
            expect(firstData.distance).toBeCloseTo(0, 6);

            // When distanceResultField is not included in select(), projection masks it out
            const maskedSnap = await subCol
                .select('title')
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    distanceMeasure: 'EUCLIDEAN',
                    limit: 1,
                    distanceResultField: 'distance',
                })
                .get();

            expect(maskedSnap.docs.map(d => d.id)).toEqual(['doc-origin-x']);
            const maskedData = maskedSnap.docs[0].data();
            expect(maskedData.title).toBe('Origin X');
            expect(maskedData.distance).toBeUndefined();
        });

        notImplementedInJava ||
            test.concurrent('rejects invalid findNearest arguments', async () => {
                await expect(
                    subCol
                        .findNearest({
                            vectorField: 'embedding',
                            queryVector: [0, 0],
                            distanceMeasure: 'COSINE',
                            limit: 5,
                        })
                        .get(),
                ).rejects.toThrow(/INVALID_ARGUMENT|FAILED_PRECONDITION/i);

                await expect(
                    subCol
                        .findNearest({
                            vectorField: 'embedding',
                            queryVector: [1, 0],
                            distanceMeasure: 'EUCLIDEAN',
                            limit: 1001,
                        })
                        .get(),
                ).rejects.toThrow(/INVALID_ARGUMENT/i);
            });
    });
});
