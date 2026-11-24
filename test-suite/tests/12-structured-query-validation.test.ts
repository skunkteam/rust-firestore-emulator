import { AggregateField, GeoPoint, Query, Timestamp } from '@google-cloud/firestore';
import { setTimeout as time } from 'timers/promises';
import {
    ConcurrentTest,
    RawProtoValue,
    RawRunQueryRequest,
    callRawUnaryRpc,
    editions,
    expectRpcCode,
    notImplementedInJava,
    rawRunQuery,
    readData,
    writeData,
} from './utils';

describe.each(editions)('$description', fs => {
    const makeCol = (suffix: string) => fs.collection.doc().collection(suffix);
    const projectId = 'firestore-test-406503';
    const databaseId = fs.firestore.databaseId;
    const dbRoot = `projects/${projectId}/databases/${databaseId}`;
    const docsRoot = `${dbRoot}/documents`;

    const idsOf = async (q: Query): Promise<string[]> => (await q.get()).docs.map(d => d.id);
    const rawIds = async (req: RawRunQueryRequest): Promise<string[]> => {
        const chunks = await rawRunQuery(fs.firestore, req);
        return chunks.filter(c => c.document?.name).map(c => c.document!.name!.split('/').pop()!);
    };

    describe('1. array-contains-any and not-in execution, null/NaN/missing matrix, aggregations, and transactions', () => {
        test.concurrent('executes array-contains-any and not-in across scalar, nested array, map, and BigInt types', async () => {
            const col = makeCol('types-and-bigint');
            const ts1 = Timestamp.fromDate(new Date('2024-01-01T00:00:00Z'));
            const ts2 = Timestamp.fromDate(new Date('2025-01-01T00:00:00Z'));
            const refA = col.doc('ref-target-a');
            const refB = col.doc('ref-target-b');
            const geo = new GeoPoint(52.37, 4.9);

            await Promise.all([
                col.doc('d1').set(
                    writeData({
                        v: 9007199254740992n,
                        ts: ts1,
                        bin: Buffer.from('aa', 'utf8'),
                        ref: refA,
                        geo,
                        arr: [1, 2],
                        tags: [9007199254740993n, { k: 'a' }, ts1],
                    }),
                ),
                col.doc('d2').set(
                    writeData({
                        v: 9007199254740993n,
                        ts: ts2,
                        bin: Buffer.from('bb', 'utf8'),
                        ref: refB,
                        arr: [3, 4],
                        tags: [ts2, true],
                    }),
                ),
            ]);

            expect(await idsOf(col.where('v', 'not-in', [9007199254740992]))).toEqual(['d2']);
            expect(await idsOf(col.where('v', 'not-in', [9007199254740992n]))).toEqual(['d2']);
            expect(await idsOf(col.where('v', 'not-in', [9007199254740993n]))).toEqual(['d1']);

            expect(await idsOf(col.where('ts', 'not-in', [ts1]))).toEqual(['d2']);
            expect(await idsOf(col.where('bin', 'not-in', [Buffer.from('aa', 'utf8')]))).toEqual(['d2']);
            expect(await idsOf(col.where('ref', 'not-in', [refA]))).toEqual(['d2']);
            expect(await idsOf(col.where('geo', 'not-in', [new GeoPoint(0, 0)]))).toIncludeSameMembers(
                fs.enterprise ? ['d1', 'd2'] : ['d1'],
            );
            expect(await idsOf(col.where('arr', 'in', [[1, 2]]))).toEqual(['d1']);
            expect(await idsOf(col.where('arr', 'not-in', [[1, 2]]))).toEqual(['d2']);

            expect(await idsOf(col.where('tags', 'array-contains-any', [9007199254740993n, false]))).toEqual(['d1']);
            expect(await idsOf(col.where('tags', 'array-contains-any', [{ k: 'a' }, 'nope']))).toEqual(['d1']);
            expect(await idsOf(col.where('tags', 'array-contains-any', [ts2]))).toEqual(['d2']);
        });

        test.concurrent('enforces Standard vs Enterprise null, NaN, and missing-field matrix', async () => {
            const col = makeCol('null-nan-matrix');
            await Promise.all([
                col.doc('d1').set(writeData({ v: 1, arr: [1] })),
                col.doc('d2').set(writeData({ v: 2, arr: [2] })),
                col.doc('d_null').set(writeData({ v: null, arr: [null] })),
                col.doc('d_nan').set(writeData({ v: Number.NaN, arr: [Number.NaN] })),
                col.doc('d_nested_nan').set(writeData({ arr: [{ nested: Number.NaN }] })),
                col.doc('d_missing').set(writeData({ other: 1 })),
            ]);

            expect(await idsOf(col.where('v', 'not-in', [1]))).toIncludeSameMembers(
                fs.enterprise ? ['d2', 'd_null', 'd_nan', 'd_nested_nan', 'd_missing'] : ['d2', 'd_nan'],
            );
            expect(await idsOf(col.where('v', 'not-in', [null]))).toIncludeSameMembers(
                fs.enterprise ? ['d1', 'd2', 'd_nan', 'd_nested_nan', 'd_missing'] : [],
            );
            expect(await idsOf(col.where('v', 'not-in', [1, null]))).toIncludeSameMembers(
                fs.enterprise ? ['d2', 'd_nan', 'd_nested_nan', 'd_missing'] : [],
            );
            expect(await idsOf(col.where('v', 'not-in', [Number.NaN]))).toIncludeSameMembers(
                fs.enterprise ? ['d1', 'd2', 'd_null', 'd_nested_nan', 'd_missing'] : ['d1', 'd2', 'd_nan'],
            );
            expect(await idsOf(col.where('v', 'in', [null]))).toEqual(fs.enterprise ? ['d_null'] : []);

            // Note: `@google-cloud/firestore` rewrites `where(f, 'array-contains', null|NaN)` to UnaryFilter IS_NOT_NULL|IS_NOT_NAN,
            // so top-level `ARRAY_CONTAINS` with null / NaN is verified via `rawIds` below while `array-contains-any` runs via SDK.
            expect(await idsOf(col.where('arr', 'array-contains-any', [null]))).toEqual(fs.enterprise ? ['d_null'] : []);
            expect(await idsOf(col.where('arr', 'array-contains-any', [Number.NaN]))).toEqual(fs.enterprise ? ['d_nan'] : []);
            expect(await idsOf(col.where('arr', 'array-contains-any', [{ nested: Number.NaN }]))).toEqual(['d_nested_nan']);

            // Raw FieldFilter ARRAY_CONTAINS, ==, !=, <, <= with null and NaN
            const colParent = `${docsRoot}/${col.parent!.path}`;
            const makeRawFilterReq = (fieldPath: string, op: string, value: RawProtoValue): RawRunQueryRequest => ({
                parent: colParent,
                structuredQuery: {
                    from: [{ collectionId: col.id }],
                    where: {
                        fieldFilter: {
                            field: { fieldPath },
                            op,
                            value,
                        },
                    },
                },
            });

            expect(await rawIds(makeRawFilterReq('arr', 'ARRAY_CONTAINS', { nullValue: 'NULL_VALUE' }))).toEqual(
                fs.enterprise ? ['d_null'] : [],
            );
            expect(await rawIds(makeRawFilterReq('arr', 'ARRAY_CONTAINS', { doubleValue: Number.NaN }))).toEqual(
                fs.enterprise ? ['d_nan'] : [],
            );
            expect(await rawIds(makeRawFilterReq('v', 'EQUAL', { nullValue: 'NULL_VALUE' }))).toEqual(fs.enterprise ? ['d_null'] : []);
            expect(await rawIds(makeRawFilterReq('v', 'NOT_EQUAL', { nullValue: 'NULL_VALUE' }))).toIncludeSameMembers(
                fs.enterprise ? ['d1', 'd2', 'd_nan', 'd_nested_nan', 'd_missing'] : [],
            );
            expect(await rawIds(makeRawFilterReq('v', 'EQUAL', { doubleValue: Number.NaN }))).toEqual(fs.enterprise ? ['d_nan'] : []);
            expect(await rawIds(makeRawFilterReq('v', 'NOT_EQUAL', { doubleValue: Number.NaN }))).toIncludeSameMembers(
                fs.enterprise ? ['d1', 'd2', 'd_null', 'd_nested_nan', 'd_missing'] : ['d1', 'd2', 'd_nan'],
            );
            expect(await rawIds(makeRawFilterReq('v', 'LESS_THAN', { nullValue: 'NULL_VALUE' }))).toEqual([]);
            expect(await rawIds(makeRawFilterReq('v', 'LESS_THAN', { doubleValue: Number.NaN }))).toEqual([]);
            expect(await rawIds(makeRawFilterReq('v', 'LESS_THAN_OR_EQUAL', { nullValue: 'NULL_VALUE' }))).toEqual(
                fs.enterprise ? ['d_null'] : [],
            );
            expect(await rawIds(makeRawFilterReq('v', 'LESS_THAN_OR_EQUAL', { doubleValue: Number.NaN }))).toEqual(
                fs.enterprise ? ['d_nan'] : [],
            );
        });

        if (fs.enterprise) {
            test.concurrent('executes 5,000-element IN, NOT_IN, and ARRAY_CONTAINS_ANY on Enterprise Edition', async () => {
                const col = makeCol('enterprise-5k');
                await Promise.all([
                    col.doc('hit').set(writeData({ v: 4999, tags: ['needle'] })),
                    col.doc('miss').set(writeData({ v: 9999, tags: ['other'] })),
                ]);

                const arr5k = Array.from({ length: 5000 }, (_, i) => i);
                expect(await idsOf(col.where('v', 'in', arr5k))).toEqual(['hit']);
                expect(await idsOf(col.where('v', 'not-in', arr5k))).toEqual(['miss']);

                const tag5k = [...Array.from({ length: 4999 }, (_, i) => `k-${i}`), 'needle'];
                expect(await idsOf(col.where('tags', 'array-contains-any', tag5k))).toEqual(['hit']);
            });
        }

        test.concurrent('computes count, sum, and average aggregations with array-contains-any and not-in', async () => {
            const col = makeCol('aggregations');
            await Promise.all([
                col.doc('a').set(writeData({ score: 10, tag: 'keep', tags: ['r'] })),
                col.doc('b').set(writeData({ score: 20, tag: 'keep', tags: ['r', 'ts'] })),
                col.doc('c').set(writeData({ score: 100, tag: 'drop', tags: ['py'] })),
            ]);

            const spec = {
                count: AggregateField.count(),
                sum: AggregateField.sum('score'),
                avg: AggregateField.average('score'),
            };

            // Zero-composite-index aggregations across both editions: `.count()` on separate filter fields
            // and full `count`/`sum`/`avg` when filtering on the aggregated field `score`.
            expect((await col.where('tags', 'array-contains-any', ['r', 'go']).count().get()).data()).toEqual({ count: 2 });
            expect((await col.where('tag', 'not-in', ['drop']).count().get()).data()).toEqual({ count: 2 });
            expect((await col.where('score', 'not-in', [100]).aggregate(spec).get()).data()).toEqual({
                count: 2,
                sum: 30,
                avg: 15,
            });

            if (fs.enterprise) {
                const acaSnap = await col.where('tags', 'array-contains-any', ['r', 'go']).aggregate(spec).get();
                expect(acaSnap.data()).toEqual({ count: 2, sum: 30, avg: 15 });

                const notInSnap = await col.where('tag', 'not-in', ['drop']).aggregate(spec).get();
                expect(notInSnap.data()).toEqual({ count: 2, sum: 30, avg: 15 });
            }
        });

        test.concurrent('enforces pessimistic read locks and optimistic write-skew retries for array-contains-any and not-in', async () => {
            const col = makeCol('txn-concurrency');
            const docKeep = col.doc('keep');
            const docMissing = col.doc('missing');
            const docDrop = col.doc('drop');
            await Promise.all([
                docKeep.set(writeData({ tag: 'keep', tags: ['x'], count: 1 })),
                docMissing.set(writeData({ other: true, count: 10 })),
                docDrop.set(writeData({ tag: 'drop', tags: ['y'], count: 100 })),
            ]);

            if (fs.concurrencyMode === 'pessimistic') {
                const t = new ConcurrentTest();
                const targetDoc = fs.enterprise ? docMissing : docKeep;

                await t.run(
                    async () => {
                        await fs.firestore.runTransaction(async txn => {
                            const snap = await txn.get(col.where('tag', 'not-in', ['drop']));
                            expect(snap.docs.map(d => d.id)).toIncludeSameMembers(fs.enterprise ? ['keep', 'missing'] : ['keep']);
                            t.event('txn queried');
                            await time(300);
                            t.event('txn committing');
                            txn.update(docKeep, { count: 2 });
                        });
                    },
                    async () => {
                        await t.when('txn queried');
                        await targetDoc.update({ count: 99 });
                        t.event('writer finished');
                    },
                );
                const commitIdx = t.log.indexOf(' <<1>> | EVENT: txn committing');
                const writerIdx = t.log.indexOf('       | <<2>> | EVENT: writer finished');
                expect(commitIdx).toBeGreaterThanOrEqual(0);
                expect(writerIdx).toBeGreaterThan(commitIdx);

                // Verify array-contains-any read locks as well
                const t2 = new ConcurrentTest();
                await t2.run(
                    async () => {
                        await fs.firestore.runTransaction(async txn => {
                            const snap = await txn.get(col.where('tags', 'array-contains-any', ['x', 'z']));
                            expect(snap.docs.map(d => d.id)).toEqual(['keep']);
                            t2.event('aca queried');
                            await time(300);
                            t2.event('aca committing');
                            txn.update(docKeep, { count: 3 });
                        });
                    },
                    async () => {
                        await t2.when('aca queried');
                        await docKeep.update({ count: 50 });
                        t2.event('aca writer finished');
                    },
                );
                const acaCommitIdx = t2.log.indexOf(' <<1>> | EVENT: aca committing');
                const acaWriterIdx = t2.log.indexOf('       | <<2>> | EVENT: aca writer finished');
                expect(acaCommitIdx).toBeGreaterThanOrEqual(0);
                expect(acaWriterIdx).toBeGreaterThan(acaCommitIdx);
            } else {
                const t = new ConcurrentTest();
                let attempts = 0;
                const docLate = col.doc('late');
                await t.run(
                    async () => {
                        await fs.firestore.runTransaction(async txn => {
                            attempts++;
                            const snap = await txn.get(col.where('tags', 'array-contains-any', ['x', 'z']));
                            t.event(`opt queried ${attempts}`);
                            if (attempts === 1) {
                                await t.when('opt concurrent insert');
                            }
                            txn.set(col.doc('summary'), writeData({ seen: snap.docs.map(d => d.id).sort() }));
                        });
                    },
                    async () => {
                        await t.when('opt queried 1');
                        await docLate.set(writeData({ tags: ['z'], count: 5 }));
                        t.event('opt concurrent insert');
                    },
                );
                expect(attempts).toBeGreaterThanOrEqual(2);
                const summary = readData((await col.doc('summary').get()).data());
                expect(summary.seen).toEqual(['keep', 'late']);
            }
        });
    });

    notImplementedInJava ||
        describe('2. Server-side runtime validation matrix', () => {
            test.concurrent('rejects malformed filter subfields, unset Value payloads, and array operand bounds', async () => {
                const col = makeCol('val-wire-bounds');
                await col.doc('seed').set(writeData({ a: 1, tags: ['x'] }));
                const parent = `${docsRoot}/${col.parent!.path}`;
                const run = (structuredQuery: Record<string, unknown>, label: string) =>
                    rawRunQuery(fs.firestore, { parent, structuredQuery }, label);

                // Missing required filter fields & unset Value payloads
                for (const [label, where] of [
                    ['missing-filter-type', {}],
                    ['missing-field-filter-field', { fieldFilter: { op: 'EQUAL', value: { integerValue: '1' } } }],
                    ['missing-field-filter-value', { fieldFilter: { field: { fieldPath: 'a' }, op: 'EQUAL' } }],
                    ['missing-unary-operand', { unaryFilter: { op: 'IS_NULL' } }],
                    ['unset-field-filter-value', { fieldFilter: { field: { fieldPath: 'a' }, op: 'EQUAL', value: {} } }],
                    [
                        'unset-array-item-value',
                        { fieldFilter: { field: { fieldPath: 'a' }, op: 'IN', value: { arrayValue: { values: [{}] } } } },
                    ],
                ] as const) {
                    await expectRpcCode(() => run({ from: [{ collectionId: col.id }], where }, label), 3, label);
                }

                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: 'a' }, direction: 'ASCENDING' }],
                                startAt: { values: [{}], before: true },
                            },
                            'unset-cursor-value',
                        ),
                    3,
                    'unset-cursor-value',
                );

                // Out-of-range GeoPointValue and TimestampValue in filters and cursors
                for (const [label, value] of [
                    ['geo-lat-out-of-range', { geoPointValue: { latitude: 999, longitude: 0 } }],
                    ['ts-neg-nanos', { timestampValue: { seconds: 0, nanos: -1 } }],
                    ['ts-max-seconds-exceeded', { timestampValue: { seconds: '253402300800', nanos: 0 } }],
                ] as const) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: { fieldFilter: { field: { fieldPath: 'a' }, op: 'EQUAL', value } },
                                },
                                `filter-${label}`,
                            ),
                        3,
                        `filter-${label}`,
                    );
                }
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: 'a' }, direction: 'ASCENDING' }],
                                startAt: { values: [{ geoPointValue: { latitude: 0, longitude: 181 } }], before: true },
                            },
                            'cursor-geo-lng-out-of-range',
                        ),
                    3,
                    'cursor-geo-lng-out-of-range',
                );

                // Non-array and empty array operands for IN / ARRAY_CONTAINS_ANY / NOT_IN
                for (const op of ['IN', 'ARRAY_CONTAINS_ANY', 'NOT_IN']) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: { fieldFilter: { field: { fieldPath: 'a' }, op, value: { integerValue: '1' } } },
                                },
                                `${op}-non-array`,
                            ),
                        3,
                        `${op}-non-array`,
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: { fieldFilter: { field: { fieldPath: 'a' }, op, value: { arrayValue: { values: [] } } } },
                                },
                                `${op}-empty-array`,
                            ),
                        3,
                        `${op}-empty-array`,
                    );
                }

                // Array size caps (Standard 30/30/10 including duplicates vs Enterprise unconstrained)
                const dup30 = Array.from({ length: 30 }, () => ({ integerValue: '1' }));
                const dup31 = Array.from({ length: 31 }, () => ({ integerValue: '1' }));
                const dup10 = Array.from({ length: 10 }, () => ({ integerValue: '2' }));
                const dup11 = Array.from({ length: 11 }, () => ({ integerValue: '2' }));

                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: { fieldFilter: { field: { fieldPath: 'a' }, op: 'IN', value: { arrayValue: { values: dup30 } } } },
                        },
                        'in-30-ok',
                    ),
                ).resolves.toBeDefined();
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: {
                                fieldFilter: {
                                    field: { fieldPath: 'tags' },
                                    op: 'ARRAY_CONTAINS_ANY',
                                    value: { arrayValue: { values: dup30 } },
                                },
                            },
                        },
                        'aca-30-ok',
                    ),
                ).resolves.toBeDefined();
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: { fieldFilter: { field: { fieldPath: 'a' }, op: 'NOT_IN', value: { arrayValue: { values: dup10 } } } },
                        },
                        'not-in-10-ok',
                    ),
                ).resolves.toBeDefined();

                if (fs.enterprise) {
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: { fieldFilter: { field: { fieldPath: 'a' }, op: 'IN', value: { arrayValue: { values: dup31 } } } },
                            },
                            'ent-in-31-ok',
                        ),
                    ).resolves.toBeDefined();
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: {
                                    fieldFilter: {
                                        field: { fieldPath: 'tags' },
                                        op: 'ARRAY_CONTAINS_ANY',
                                        value: { arrayValue: { values: dup31 } },
                                    },
                                },
                            },
                            'ent-aca-31-ok',
                        ),
                    ).resolves.toBeDefined();
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: {
                                    fieldFilter: { field: { fieldPath: 'a' }, op: 'NOT_IN', value: { arrayValue: { values: dup11 } } },
                                },
                            },
                            'ent-not-in-11-ok',
                        ),
                    ).resolves.toBeDefined();
                } else {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: { field: { fieldPath: 'a' }, op: 'IN', value: { arrayValue: { values: dup31 } } },
                                    },
                                },
                                'std-in-31-err',
                            ),
                        3,
                        'std-in-31-err',
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: {
                                            field: { fieldPath: 'tags' },
                                            op: 'ARRAY_CONTAINS_ANY',
                                            value: { arrayValue: { values: dup31 } },
                                        },
                                    },
                                },
                                'std-aca-31-err',
                            ),
                        3,
                        'std-aca-31-err',
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: { field: { fieldPath: 'a' }, op: 'NOT_IN', value: { arrayValue: { values: dup11 } } },
                                    },
                                },
                                'std-not-in-11-err',
                            ),
                        3,
                        'std-not-in-11-err',
                    );
                }
            });

            test.concurrent('enforces disjunction, negation exclusivity, NOT_IN exclusion, and DNF limits', async () => {
                const col = makeCol('val-dnf-negation');
                await col.doc('seed').set(writeData({ a: 1, b: 2, tags: ['x'] }));
                const parent = `${docsRoot}/${col.parent!.path}`;
                const run = (structuredQuery: Record<string, unknown>, label: string) =>
                    rawRunQuery(fs.firestore, { parent, structuredQuery }, label);

                // Empty OR (rejected on both editions) vs Empty AND (accepted on both editions)
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: { compositeFilter: { op: 'OR', filters: [] } },
                            },
                            'empty-or',
                        ),
                    3,
                    'empty-or',
                );
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: { compositeFilter: { op: 'AND', filters: [] } },
                        },
                        'empty-and',
                    ),
                ).resolves.toBeDefined();

                // Single-field OR with 1 ARRAY_CONTAINS / ARRAY_CONTAINS_ANY per branch is valid on both editions.
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: {
                                compositeFilter: {
                                    op: 'OR',
                                    filters: [
                                        {
                                            fieldFilter: {
                                                field: { fieldPath: 'tags' },
                                                op: 'ARRAY_CONTAINS',
                                                value: { stringValue: 'x' },
                                            },
                                        },
                                        {
                                            fieldFilter: {
                                                field: { fieldPath: 'tags' },
                                                op: 'ARRAY_CONTAINS_ANY',
                                                value: { arrayValue: { values: [{ stringValue: 'y' }] } },
                                            },
                                        },
                                    ],
                                },
                            },
                        },
                        'or-array-contains-per-branch-ok',
                    ),
                ).resolves.toBeDefined();

                // Single-field 30-disjunction OR and deduplicated IN (15 * 2 = 30) succeed on both editions.
                const or30 = Array.from({ length: 30 }, (_, i) => ({
                    fieldFilter: { field: { fieldPath: 'a' }, op: 'EQUAL', value: { integerValue: String(i) } },
                }));
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: { compositeFilter: { op: 'OR', filters: or30 } },
                        },
                        'single-field-or-30-ok',
                    ),
                ).resolves.toBeDefined();
                const in15Dup = [
                    ...Array.from({ length: 15 }, (_, i) => ({ integerValue: String(i) })),
                    ...Array.from({ length: 15 }, (_, i) => ({ integerValue: String(i) })),
                ];
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: {
                                compositeFilter: {
                                    op: 'AND',
                                    filters: [
                                        {
                                            fieldFilter: {
                                                field: { fieldPath: 'a' },
                                                op: 'IN',
                                                value: { arrayValue: { values: in15Dup } },
                                            },
                                        },
                                        {
                                            fieldFilter: {
                                                field: { fieldPath: 'a' },
                                                op: 'IN',
                                                value: { arrayValue: { values: [{ integerValue: '1' }, { integerValue: '2' }] } },
                                            },
                                        },
                                    ],
                                },
                            },
                        },
                        'single-field-in-dedup-30-ok',
                    ),
                ).resolves.toBeDefined();

                const vals6 = Array.from({ length: 6 }, (_, i) => ({ integerValue: String(i) }));
                const vals16 = Array.from({ length: 16 }, (_, i) => ({ integerValue: String(i) }));
                const dnf36 = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            { fieldFilter: { field: { fieldPath: 'a' }, op: 'IN', value: { arrayValue: { values: vals6 } } } },
                            { fieldFilter: { field: { fieldPath: 'b' }, op: 'IN', value: { arrayValue: { values: vals6 } } } },
                        ],
                    },
                };
                const dnf256 = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            { fieldFilter: { field: { fieldPath: 'a' }, op: 'IN', value: { arrayValue: { values: vals16 } } } },
                            { fieldFilter: { field: { fieldPath: 'b' }, op: 'IN', value: { arrayValue: { values: vals16 } } } },
                        ],
                    },
                };
                const twoAcSameField = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            { fieldFilter: { field: { fieldPath: 'tags' }, op: 'ARRAY_CONTAINS', value: { stringValue: 'x' } } },
                            { fieldFilter: { field: { fieldPath: 'tags' }, op: 'ARRAY_CONTAINS', value: { stringValue: 'y' } } },
                        ],
                    },
                };
                const negPairSameField = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            { fieldFilter: { field: { fieldPath: 'a' }, op: 'NOT_EQUAL', value: { integerValue: '1' } } },
                            { unaryFilter: { field: { fieldPath: 'a' }, op: 'IS_NOT_NULL' } },
                        ],
                    },
                };
                const notInAndNan = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'NOT_IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }] } },
                                },
                            },
                            { unaryFilter: { field: { fieldPath: 'a' }, op: 'IS_NOT_NAN' } },
                        ],
                    },
                };
                const notInOr = {
                    compositeFilter: {
                        op: 'OR',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'NOT_IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }] } },
                                },
                            },
                            { fieldFilter: { field: { fieldPath: 'a' }, op: 'EQUAL', value: { integerValue: '2' } } },
                        ],
                    },
                };
                const notInAndIn = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'NOT_IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }] } },
                                },
                            },
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'IN',
                                    value: { arrayValue: { values: [{ integerValue: '2' }] } },
                                },
                            },
                        ],
                    },
                };
                const notInAndAca = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'NOT_IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }] } },
                                },
                            },
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'tags' },
                                    op: 'ARRAY_CONTAINS_ANY',
                                    value: { arrayValue: { values: [{ stringValue: 'x' }] } },
                                },
                            },
                        ],
                    },
                };

                const intAndDouble30 = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'IN',
                                    value: {
                                        arrayValue: {
                                            values: [
                                                ...Array.from({ length: 15 }, (_, i) => ({ integerValue: String(i) })),
                                                ...Array.from({ length: 15 }, (_, i) => ({ doubleValue: i })),
                                            ],
                                        },
                                    },
                                },
                            },
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'b' },
                                    op: 'IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }, { integerValue: '2' }] } },
                                },
                            },
                        ],
                    },
                };
                const plusMinusZeroIn = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'a' },
                                    op: 'IN',
                                    value: {
                                        arrayValue: {
                                            values: [
                                                ...Array.from({ length: 14 }, (_, i) => ({ doubleValue: i + 1.5 })),
                                                { doubleValue: 0.0 },
                                                { doubleValue: -0.0 },
                                            ],
                                        },
                                    },
                                },
                            },
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'b' },
                                    op: 'IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }, { integerValue: '2' }] } },
                                },
                            },
                        ],
                    },
                };
                const acaIntAndDouble = {
                    compositeFilter: {
                        op: 'AND',
                        filters: [
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'tags' },
                                    op: 'ARRAY_CONTAINS_ANY',
                                    value: {
                                        arrayValue: {
                                            values: [
                                                ...Array.from({ length: 15 }, (_, i) => ({ integerValue: String(i) })),
                                                { doubleValue: 0.0 },
                                            ],
                                        },
                                    },
                                },
                            },
                            {
                                fieldFilter: {
                                    field: { fieldPath: 'b' },
                                    op: 'IN',
                                    value: { arrayValue: { values: [{ integerValue: '1' }, { integerValue: '2' }] } },
                                },
                            },
                        ],
                    },
                };

                const stdOnlyChecks = [
                    ['dnf-36', dnf36],
                    ['dnf-256', dnf256],
                    ['dnf-int-double-60', intAndDouble30],
                    ['dnf-plus-minus-zero-32', plusMinusZeroIn],
                    ['dnf-aca-int-double-32', acaIntAndDouble],
                    ['two-array-contains', twoAcSameField],
                    ['neg-pair', negPairSameField],
                    ['not-in-and-is-not-nan', notInAndNan],
                    ['not-in-or', notInOr],
                    ['not-in-and-in', notInAndIn],
                    ['not-in-and-aca', notInAndAca],
                ] as const;

                for (const [label, where] of stdOnlyChecks) {
                    if (fs.enterprise) {
                        await expect(run({ from: [{ collectionId: col.id }], where }, `ent-${label}`)).resolves.toBeDefined();
                    } else {
                        await expectRpcCode(() => run({ from: [{ collectionId: col.id }], where }, `std-${label}`), 3, `std-${label}`);
                    }
                }
            });

            test.concurrent('validates __name__ references, Datastore __id<i64>__ sentinels, and reserved FieldPaths', async () => {
                const col = makeCol('val-name-and-paths');
                const seedRef = col.doc('seed');
                await seedRef.set(writeData({ a: 1 }));
                const parent = `${docsRoot}/${col.parent!.path}`;
                const validDocRef = `${docsRoot}/${seedRef.path}`;
                const dsZeroRef = `${docsRoot}/${col.parent!.path}/${col.id}/__id0__`;
                const dsZeroAncRef = `${dsZeroRef}/sub/d1`;
                const dsOneRef = `${docsRoot}/${col.parent!.path}/${col.id}/__id1__`;
                const dsDocRef = `${docsRoot}/${col.parent!.path}/${col.id}/__id-9223372036854775808__`;
                const run = (structuredQuery: Record<string, unknown>, label: string) =>
                    rawRunQuery(fs.firestore, { parent, structuredQuery }, label);

                // Both Standard and Enterprise accept non-zero Datastore `__id<i64>__` on `__name__`, plus `__id0__` and `docsRoot` on general fields.
                for (const [label, refVal] of [
                    ['ds-id1-ok', dsOneRef],
                    ['ds-id-min-ok', dsDocRef],
                ] as const) {
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: {
                                    fieldFilter: {
                                        field: { fieldPath: '__name__' },
                                        op: 'EQUAL',
                                        value: { referenceValue: refVal },
                                    },
                                },
                            },
                            label,
                        ),
                    ).resolves.toBeDefined();
                }
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: { fieldFilter: { field: { fieldPath: 'ref' }, op: 'EQUAL', value: { referenceValue: dsZeroRef } } },
                        },
                        'ref-ds-id0-ok',
                    ),
                ).resolves.toBeDefined();
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                            startAt: { values: [{ referenceValue: dsZeroRef }], before: true },
                        },
                        'cursor-ds-id0-ok',
                    ),
                ).resolves.toBeDefined();
                for (const [label, refVal] of [
                    ['ds-name-id0', dsZeroRef],
                    ['ds-name-id0-anc', dsZeroAncRef],
                ] as const) {
                    const req = {
                        from: [{ collectionId: col.id }],
                        where: {
                            fieldFilter: {
                                field: { fieldPath: '__name__' },
                                op: 'EQUAL',
                                value: { referenceValue: refVal },
                            },
                        },
                    };
                    if (!fs.enterprise) {
                        await expectRpcCode(() => run(req, `std-${label}`), 3, `std-${label}`);
                    } else {
                        await expect(run(req, `ent-${label}`)).resolves.toBeDefined();
                    }
                }
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: { fieldFilter: { field: { fieldPath: 'ref' }, op: 'EQUAL', value: { referenceValue: docsRoot } } },
                        },
                        'ref-docs-root-ok',
                    ),
                ).resolves.toBeDefined();

                // Invalid ReferenceValues (collection, odd subcollection, reserved, malformed/non-canonical __id, collection __id) across editions
                const badRefPaths = [
                    `${docsRoot}/${col.parent!.path}/${col.id}`,
                    `${docsRoot}/${seedRef.path}/sub`,
                    `${docsRoot}/${col.parent!.path}/${col.id}/__custom__`,
                    `${docsRoot}/${col.parent!.path}/${col.id}/__id-0__`,
                    `${docsRoot}/${col.parent!.path}/${col.id}/__id01__`,
                    `${docsRoot}/${col.parent!.path}/${col.id}/__id-01__`,
                    `${docsRoot}/${col.parent!.path}/${col.id}/__id+1__`,
                    `${docsRoot}/${col.parent!.path}/__id123__/doc1`,
                ];
                for (const badRef of badRefPaths) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: {
                                            field: { fieldPath: 'ref' },
                                            op: 'EQUAL',
                                            value: { referenceValue: badRef },
                                        },
                                    },
                                },
                                `bad-ref-${badRef}`,
                            ),
                        3,
                        `bad-ref-${badRef}`,
                    );
                }

                // Standard __name__ operator and root restrictions (Enterprise accepts index-free __name__ scans)
                const otherDbRef = `projects/${projectId}/databases/other-db/documents/${seedRef.path}`;
                const otherProjRef = `projects/other-proj/databases/${databaseId}/documents/${seedRef.path}`;
                const stdNameInvalid = [
                    ['unary-is-null', { unaryFilter: { field: { fieldPath: '__name__' }, op: 'IS_NULL' } }],
                    ['unary-is-not-null', { unaryFilter: { field: { fieldPath: '__name__' }, op: 'IS_NOT_NULL' } }],
                    ['unary-is-nan', { unaryFilter: { field: { fieldPath: '__name__' }, op: 'IS_NAN' } }],
                    ['unary-is-not-nan', { unaryFilter: { field: { fieldPath: '__name__' }, op: 'IS_NOT_NAN' } }],
                    [
                        'array-contains',
                        { fieldFilter: { field: { fieldPath: '__name__' }, op: 'ARRAY_CONTAINS', value: { referenceValue: validDocRef } } },
                    ],
                    [
                        'array-contains-any',
                        {
                            fieldFilter: {
                                field: { fieldPath: '__name__' },
                                op: 'ARRAY_CONTAINS_ANY',
                                value: { arrayValue: { values: [{ referenceValue: validDocRef }] } },
                            },
                        },
                    ],
                    ['scalar-non-ref', { fieldFilter: { field: { fieldPath: '__name__' }, op: 'EQUAL', value: { stringValue: 'x' } } }],
                    [
                        'in-non-ref',
                        {
                            fieldFilter: {
                                field: { fieldPath: '__name__' },
                                op: 'IN',
                                value: { arrayValue: { values: [{ referenceValue: validDocRef }, { integerValue: '1' }] } },
                            },
                        },
                    ],
                    [
                        'not-in-wrong-db',
                        {
                            fieldFilter: {
                                field: { fieldPath: '__name__' },
                                op: 'NOT_IN',
                                value: { arrayValue: { values: [{ referenceValue: otherDbRef }] } },
                            },
                        },
                    ],
                    [
                        'eq-wrong-proj',
                        { fieldFilter: { field: { fieldPath: '__name__' }, op: 'EQUAL', value: { referenceValue: otherProjRef } } },
                    ],
                ] as const;

                if (!fs.enterprise) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: {
                                            field: { fieldPath: '__name__' },
                                            op: 'EQUAL',
                                            value: { referenceValue: docsRoot },
                                        },
                                    },
                                },
                                'std-name-empty-key',
                            ),
                        3,
                        'std-name-empty-key',
                    );
                    for (const [label, where] of stdNameInvalid) {
                        await expectRpcCode(
                            () => run({ from: [{ collectionId: col.id }], where }, `std-name-${label}`),
                            3,
                            `std-name-${label}`,
                        );
                    }
                } else {
                    for (const [label, where] of stdNameInvalid) {
                        await expect(run({ from: [{ collectionId: col.id }], where }, `ent-name-${label}`)).resolves.toBeDefined();
                    }
                    // Enterprise also permits valid __name__ NOT_IN with valid ReferenceValues
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: {
                                    fieldFilter: {
                                        field: { fieldPath: '__name__' },
                                        op: 'NOT_IN',
                                        value: { arrayValue: { values: [{ referenceValue: validDocRef }] } },
                                    },
                                },
                            },
                            'ent-name-not-in-ok',
                        ),
                    ).resolves.toBeDefined();
                }

                // Reserved and empty FieldPaths across FieldFilter, UnaryFilter, Order, select, and GetDocument mask
                for (const badPath of ['__custom__', 'a.__custom__', 'a..b']) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: { fieldFilter: { field: { fieldPath: badPath }, op: 'EQUAL', value: { integerValue: '1' } } },
                                },
                                `filter-bad-path-${badPath}`,
                            ),
                        3,
                        `filter-bad-path-${badPath}`,
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: { unaryFilter: { field: { fieldPath: badPath }, op: 'IS_NULL' } },
                                },
                                `unary-bad-path-${badPath}`,
                            ),
                        3,
                        `unary-bad-path-${badPath}`,
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    orderBy: [{ field: { fieldPath: badPath }, direction: 'ASCENDING' }],
                                },
                                `order-bad-path-${badPath}`,
                            ),
                        3,
                        `order-bad-path-${badPath}`,
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    select: { fields: [{ fieldPath: badPath }] },
                                },
                                `select-bad-path-${badPath}`,
                            ),
                        3,
                        `select-bad-path-${badPath}`,
                    );
                    await expectRpcCode(
                        () =>
                            callRawUnaryRpc(fs.firestore, 'getDocument', {
                                name: validDocRef,
                                mask: { fieldPaths: [badPath] },
                            }),
                        3,
                        `get-mask-bad-path-${badPath}`,
                    );
                }

                // Backtick-quoted `` `__name__` `` resolves to DocumentName across filter, orderBy, select, and GetDocument mask;
                // nested `a.__name__` is rejected across select/mask (and filter/orderBy on Standard).
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            where: {
                                fieldFilter: {
                                    field: { fieldPath: '`__name__`' },
                                    op: 'EQUAL',
                                    value: { referenceValue: validDocRef },
                                },
                            },
                            orderBy: [{ field: { fieldPath: '`__name__`' }, direction: 'ASCENDING' }],
                            select: { fields: [{ fieldPath: '`__name__`' }] },
                        },
                        'quoted-name-query-ok',
                    ),
                ).resolves.toBeDefined();
                await expect(
                    callRawUnaryRpc(fs.firestore, 'getDocument', {
                        name: validDocRef,
                        mask: { fieldPaths: ['`__name__`'] },
                    }),
                ).resolves.toBeDefined();
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: '`__name__`' }, direction: 'ASCENDING' }],
                                startAt: { values: [{ stringValue: 'x' }], before: true },
                            },
                            'quoted-name-cursor-non-ref',
                        ),
                    3,
                    'quoted-name-cursor-non-ref',
                );
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                select: { fields: [{ fieldPath: 'a.__name__' }] },
                            },
                            'select-nested-name-err',
                        ),
                    3,
                    'select-nested-name-err',
                );
                await expectRpcCode(
                    () =>
                        callRawUnaryRpc(fs.firestore, 'getDocument', {
                            name: validDocRef,
                            mask: { fieldPaths: ['a.__name__'] },
                        }),
                    3,
                    'get-mask-nested-name-err',
                );
                if (!fs.enterprise) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    orderBy: [
                                        { field: { fieldPath: '__name__' }, direction: 'ASCENDING' },
                                        { field: { fieldPath: '`__name__`' }, direction: 'ASCENDING' },
                                    ],
                                },
                                'std-dup-quoted-name-orderby',
                            ),
                        3,
                        'std-dup-quoted-name-orderby',
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: {
                                            field: { fieldPath: 'a.__name__' },
                                            op: 'EQUAL',
                                            value: { integerValue: '1' },
                                        },
                                    },
                                },
                                'std-filter-nested-name-err',
                            ),
                        3,
                        'std-filter-nested-name-err',
                    );
                }
            });

            test.concurrent('validates order_by duplicates, __name__-last, DESC tie-breakers, and pre-synthesis cursors', async () => {
                const col = makeCol('val-orderby-cursors');
                await Promise.all([
                    col.doc('d1').set(writeData({ a: 10 })),
                    col.doc('d2').set(writeData({ a: 10 })),
                    col.doc('d3').set(writeData({ a: 20 })),
                ]);
                const parent = `${docsRoot}/${col.parent!.path}`;
                const d1Ref = `${docsRoot}/${col.doc('d1').path}`;
                const d2Ref = `${docsRoot}/${col.doc('d2').path}`;
                const otherDbRef = `projects/${projectId}/databases/other-db/documents/${col.doc('d1').path}`;
                const run = (structuredQuery: Record<string, unknown>, label: string, newTransaction?: Record<string, never>) =>
                    rawRunQuery(
                        fs.firestore,
                        {
                            parent,
                            structuredQuery,
                            ...(newTransaction ? { newTransaction: { readWrite: newTransaction } } : {}),
                        },
                        label,
                    );

                // DESC tie-breaking with explicit 2-field orderBy
                expect(
                    await rawIds({
                        parent,
                        structuredQuery: {
                            from: [{ collectionId: col.id }],
                            orderBy: [
                                { field: { fieldPath: 'a' }, direction: 'DESCENDING' },
                                { field: { fieldPath: '__name__' }, direction: 'DESCENDING' },
                            ],
                        },
                    }),
                ).toEqual(['d3', 'd2', 'd1']);

                if (!fs.enterprise) {
                    expect(
                        await rawIds({
                            parent,
                            structuredQuery: {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: 'a' }, direction: 'DESCENDING' }],
                            },
                        }),
                    ).toEqual(['d3', 'd2', 'd1']);
                }

                // Explicit orderBy('a', DESC) rejects a 2-value cursor when __name__ is omitted from orderBy.
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: 'a' }, direction: 'DESCENDING' }],
                                startAt: { values: [{ integerValue: '10' }, { referenceValue: d2Ref }], before: false },
                            },
                            'cursor-exceeds-explicit-orderby',
                        ),
                    3,
                    'cursor-exceeds-explicit-orderby',
                );

                // Valid 2-value cursor when explicit orderBy includes __name__
                expect(
                    await rawIds({
                        parent,
                        structuredQuery: {
                            from: [{ collectionId: col.id }],
                            orderBy: [
                                { field: { fieldPath: 'a' }, direction: 'DESCENDING' },
                                { field: { fieldPath: '__name__' }, direction: 'DESCENDING' },
                            ],
                            startAt: { values: [{ integerValue: '10' }, { referenceValue: d2Ref }], before: false },
                        },
                    }),
                ).toEqual(['d1']);

                // Cursor __name__ slot rejects non-ReferenceValue (with newTransaction rollback cleanup) on both editions
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                                startAt: { values: [{ stringValue: d1Ref }], before: true },
                            },
                            'cursor-name-non-ref-newtxn',
                            {},
                        ),
                    3,
                    'cursor-name-non-ref-newtxn',
                );

                // Cursor __name__ slot accepts docsRoot and rejects wrong-database references across both editions
                await expect(
                    run(
                        {
                            from: [{ collectionId: col.id }],
                            orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                            startAt: { values: [{ referenceValue: docsRoot }], before: true },
                        },
                        'cursor-name-docs-root-ok',
                    ),
                ).resolves.toBeDefined();
                await expectRpcCode(
                    () =>
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                                startAt: { values: [{ referenceValue: otherDbRef }], before: true },
                            },
                            'cursor-name-wrong-db',
                        ),
                    3,
                    'cursor-name-wrong-db',
                );

                if (!fs.enterprise) {
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    orderBy: [
                                        { field: { fieldPath: 'a' }, direction: 'ASCENDING' },
                                        { field: { fieldPath: 'a' }, direction: 'DESCENDING' },
                                    ],
                                },
                                'std-dup-orderby',
                            ),
                        3,
                        'std-dup-orderby',
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    orderBy: [
                                        { field: { fieldPath: '__name__' }, direction: 'ASCENDING' },
                                        { field: { fieldPath: 'a' }, direction: 'ASCENDING' },
                                    ],
                                },
                                'std-name-before-field',
                            ),
                        3,
                        'std-name-before-field',
                    );
                    await expectRpcCode(
                        () =>
                            run(
                                {
                                    from: [{ collectionId: col.id }],
                                    where: {
                                        fieldFilter: { field: { fieldPath: 'a' }, op: 'GREATER_THAN', value: { integerValue: '0' } },
                                    },
                                    orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                                },
                                'std-ineq-synthesized-after-name',
                            ),
                        3,
                        'std-ineq-synthesized-after-name',
                    );
                } else {
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [
                                    { field: { fieldPath: 'a' }, direction: 'ASCENDING' },
                                    { field: { fieldPath: 'a' }, direction: 'DESCENDING' },
                                ],
                            },
                            'ent-dup-orderby-ok',
                        ),
                    ).resolves.toBeDefined();
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                orderBy: [
                                    { field: { fieldPath: '__name__' }, direction: 'ASCENDING' },
                                    { field: { fieldPath: 'a' }, direction: 'ASCENDING' },
                                ],
                            },
                            'ent-name-before-field-ok',
                        ),
                    ).resolves.toBeDefined();
                    await expect(
                        run(
                            {
                                from: [{ collectionId: col.id }],
                                where: {
                                    fieldFilter: { field: { fieldPath: 'a' }, op: 'GREATER_THAN', value: { integerValue: '0' } },
                                },
                                orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                            },
                            'ent-ineq-synthesized-after-name-ok',
                        ),
                    ).resolves.toBeDefined();
                }
            });

            test.concurrent('validates allDescendants order_by, cursors, and multi-leaf Enterprise ordering', async () => {
                const scopeDoc = fs.collection.doc();
                const scopeDocPath = `${docsRoot}/${scopeDoc.path}`;
                const cgId = `cg_val_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;

                // Create multiple leaves per parent in scrambled order so leaf order != __name__ ASC/DESC.
                const specs = [
                    { parent: 'p_b', id: 'd3', tie: 10 },
                    { parent: 'p_a', id: 'd2', tie: 10 },
                    { parent: 'p_b', id: 'd1', tie: 10 },
                    { parent: 'p_a', id: 'd3', tie: 10 },
                    { parent: 'p_b', id: 'd2', tie: 10 },
                    { parent: 'p_a', id: 'd1', tie: 10 },
                    { parent: 'p_a', id: 'd0', tie: 5 },
                    { parent: 'p_b', id: 'd9', tie: 20 },
                ];
                await Promise.all(
                    specs.map(s =>
                        scopeDoc
                            .collection('parents')
                            .doc(s.parent)
                            .collection(cgId)
                            .doc(s.id)
                            .set(writeData({ tie: s.tie })),
                    ),
                );

                const cursorRef = `${scopeDocPath}/parents/p_b/${cgId}/d2`;
                const runDesc = (parent: string, structuredQuery: Record<string, unknown>, label: string) =>
                    rawRunQuery(fs.firestore, { parent, structuredQuery }, label);
                const relDescPaths = async (parent: string, structuredQuery: Record<string, unknown>, label: string): Promise<string[]> => {
                    const chunks = await runDesc(parent, structuredQuery, label);
                    return chunks
                        .filter(c => c.document?.name)
                        .map(c =>
                            c
                                .document!.name!.slice(scopeDocPath.length + 1)
                                .replace('parents/', '')
                                .replace(`/${cgId}/`, '/'),
                        );
                };

                // (a) orderBy: [tie DESC] + 2-value startAt cursor [10, docRef] is rejected on both editions
                for (const before of [true, false]) {
                    await expectRpcCode(
                        () =>
                            runDesc(
                                docsRoot,
                                {
                                    from: [{ collectionId: cgId, allDescendants: true }],
                                    orderBy: [{ field: { fieldPath: 'tie' }, direction: 'DESCENDING' }],
                                    startAt: { values: [{ integerValue: '10' }, { referenceValue: cursorRef }], before },
                                },
                                `desc-tie-2val-cursor-before-${before}`,
                            ),
                        3,
                        `desc-tie-2val-cursor-before-${before}`,
                    );
                }
                await expectRpcCode(
                    () =>
                        runDesc(
                            scopeDocPath,
                            {
                                from: [{ allDescendants: true }],
                                orderBy: [{ field: { fieldPath: 'tie' }, direction: 'DESCENDING' }],
                                startAt: { values: [{ integerValue: '10' }, { referenceValue: cursorRef }], before: true },
                            },
                            'desc-kindless-tie-2val-cursor',
                        ),
                    3,
                    'desc-kindless-tie-2val-cursor',
                );

                // (b) orderBy: [] + 1-value startAt cursor [docRef] is rejected on both editions
                for (const parent of [docsRoot, scopeDocPath]) {
                    await expectRpcCode(
                        () =>
                            runDesc(
                                parent,
                                {
                                    from: [{ collectionId: cgId, allDescendants: true }],
                                    startAt: { values: [{ referenceValue: cursorRef }], before: true },
                                },
                                `desc-no-orderby-1val-cursor-${parent === docsRoot ? 'root' : 'doc'}`,
                            ),
                        3,
                        `desc-no-orderby-1val-cursor-${parent === docsRoot ? 'root' : 'doc'}`,
                    );
                }

                // (c) Multi-leaf-per-parent allDescendants with no orderBy returns all docs; explicit __name__ ASC orders lexicographically
                const expectedAsc = ['p_a/d0', 'p_a/d1', 'p_a/d2', 'p_a/d3', 'p_b/d1', 'p_b/d2', 'p_b/d3', 'p_b/d9'];
                const noOrderPaths = await relDescPaths(
                    docsRoot,
                    { from: [{ collectionId: cgId, allDescendants: true }] },
                    'desc-multi-leaf-no-orderby',
                );
                if (fs.enterprise) {
                    expect(noOrderPaths).toIncludeSameMembers(expectedAsc);
                } else {
                    expect(noOrderPaths).toEqual(expectedAsc);
                }

                const explicitNameAsc = await relDescPaths(
                    docsRoot,
                    {
                        from: [{ collectionId: cgId, allDescendants: true }],
                        orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
                    },
                    'desc-multi-leaf-explicit-name-asc',
                );
                expect(explicitNameAsc).toEqual(expectedAsc);

                // (d) Enterprise explicit 2-field orderBy (tie DESC, __name__ DESC/ASC) + 2-value cursor obeys explicit __name__ direction
                if (fs.enterprise) {
                    const oneValStartAt = await relDescPaths(
                        docsRoot,
                        {
                            from: [{ collectionId: cgId, allDescendants: true }],
                            orderBy: [{ field: { fieldPath: 'tie' }, direction: 'DESCENDING' }],
                            startAt: { values: [{ integerValue: '10' }], before: true },
                        },
                        'ent-desc-tie-1val-cursor',
                    );
                    expect(oneValStartAt.slice(0, 6)).toIncludeSameMembers(['p_a/d1', 'p_a/d2', 'p_a/d3', 'p_b/d1', 'p_b/d2', 'p_b/d3']);
                    expect(oneValStartAt[6]).toBe('p_a/d0');

                    const descNameDescCursor = await relDescPaths(
                        docsRoot,
                        {
                            from: [{ collectionId: cgId, allDescendants: true }],
                            orderBy: [
                                { field: { fieldPath: 'tie' }, direction: 'DESCENDING' },
                                { field: { fieldPath: '__name__' }, direction: 'DESCENDING' },
                            ],
                            startAt: { values: [{ integerValue: '10' }, { referenceValue: cursorRef }], before: true },
                        },
                        'ent-desc-tie-desc-name-desc-cursor',
                    );
                    expect(descNameDescCursor).toEqual(['p_b/d2', 'p_b/d1', 'p_a/d3', 'p_a/d2', 'p_a/d1', 'p_a/d0']);

                    const descNameAscCursor = await relDescPaths(
                        docsRoot,
                        {
                            from: [{ collectionId: cgId, allDescendants: true }],
                            orderBy: [
                                { field: { fieldPath: 'tie' }, direction: 'DESCENDING' },
                                { field: { fieldPath: '__name__' }, direction: 'ASCENDING' },
                            ],
                            startAt: { values: [{ integerValue: '10' }, { referenceValue: cursorRef }], before: true },
                        },
                        'ent-desc-tie-desc-name-asc-cursor',
                    );
                    expect(descNameAscCursor).toEqual(['p_b/d2', 'p_b/d3', 'p_a/d0']);
                }
            });
        });
});
