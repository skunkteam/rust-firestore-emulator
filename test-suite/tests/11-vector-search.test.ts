import {
    CollectionReference,
    DocumentReference,
    FieldPath,
    FieldValue,
    Firestore,
    VectorQuery,
    VectorValue,
} from '@google-cloud/firestore';
import type { EventEmitter } from 'events';
import { setTimeout as time } from 'timers/promises';
import {
    ConcurrentTest,
    callRawStreamRpc,
    callRawUnaryRpc,
    connection,
    editions,
    expectCloudLimitationCanary,
    notImplementedInJava,
    readData,
    writeData,
} from './utils';

interface RawProtoValue {
    nullValue?: 'NULL_VALUE' | null;
    booleanValue?: boolean;
    integerValue?: string;
    doubleValue?: number;
    stringValue?: string;
    arrayValue?: { values?: RawProtoValue[] };
    mapValue?: { fields?: Record<string, RawProtoValue> };
}

interface RawDocument {
    name?: string;
    fields?: Record<string, RawProtoValue>;
}

interface RawRunQueryRequest {
    parent: string;
    structuredQuery?: Record<string, unknown>;
    transaction?: Uint8Array;
    newTransaction?: { readWrite?: Record<string, never> };
}

interface RawRunQueryResponseChunk {
    document?: RawDocument;
    transaction?: Uint8Array;
}

interface RawAggregationResponseChunk {
    result?: {
        aggregateFields?: Record<string, RawProtoValue>;
    };
}

interface RawBeginTransactionResponse {
    transaction: Uint8Array;
}

interface RawListenResponse {
    targetChange?: {
        targetChangeType?: 'NO_CHANGE' | 'ADD' | 'REMOVE' | 'CURRENT' | 'RESET';
        targetIds?: number[];
        cause?: { code?: number; message?: string };
    };
    documentChange?: {
        document?: RawDocument;
        targetIds?: number[];
        removedTargetIds?: number[];
    };
    documentDelete?: { document?: string };
    documentRemove?: { document?: string };
}

interface InternalVectorQueryProtoCarrier {
    toProto(transaction?: Uint8Array): RawRunQueryRequest;
}

interface InternalListenClient {
    initializeIfNeeded(tag: string): Promise<void>;
    requestStream(methodName: string, bidirectional: boolean, request: object, tag: string): Promise<NodeJS.ReadWriteStream & EventEmitter>;
}

function toRunQueryProto(vq: VectorQuery, transaction?: Uint8Array): RawRunQueryRequest {
    return (vq as unknown as InternalVectorQueryProtoCarrier).toProto(transaction);
}

function docNameFromParent(parent: string, docRef: DocumentReference): string {
    const dbRoot = parent.split('/documents')[0];
    return `${dbRoot}/documents/${docRef.path}`;
}

function shortDocId(name: string): string {
    const parts = name.split('/');
    return parts[parts.length - 1];
}

function createVectorMap(coords: number[]): RawProtoValue {
    return {
        mapValue: {
            fields: {
                __type__: { stringValue: '__vector__' },
                value: { arrayValue: { values: coords.map(doubleValue => ({ doubleValue })) } },
            },
        },
    };
}

async function expectRpcCode(fn: () => Promise<unknown>, expectedCode: number, label = 'rpc'): Promise<void> {
    let resolvedValue: unknown;
    let didResolve = false;
    try {
        resolvedValue = await fn();
        didResolve = true;
    } catch (err: unknown) {
        const code = typeof err === 'object' && err !== null ? (err as { code?: unknown }).code : undefined;
        const msg = err instanceof Error ? err.message : String(err);
        expect({ label, code, detail: code === expectedCode ? 'ok' : msg }).toEqual({
            label,
            code: expectedCode,
            detail: 'ok',
        });
        return;
    }
    if (didResolve) {
        throw new Error(`[${label}] Expected gRPC status code ${expectedCode}, but call resolved with ${JSON.stringify(resolvedValue)}`);
    }
}

async function expectRunQueryNewTxnError(
    firestore: Firestore,
    req: RawRunQueryRequest,
    expectedCode: number,
    database: string,
    label: string,
): Promise<void> {
    const client = firestore as unknown as InternalListenClient;
    let capturedTx: Uint8Array | undefined;
    try {
        await client.initializeIfNeeded(label);
        const stream = await client.requestStream('runQuery', false, req, label);
        await new Promise<void>((resolve, reject) => {
            let settled = false;
            const finish = (err?: unknown) => {
                if (settled) return;
                settled = true;
                if (err !== undefined) {
                    reject(err instanceof Error ? err : new Error(String(err)));
                } else {
                    resolve();
                }
            };
            stream.on('data', (chunk: unknown) => {
                const typed = chunk as RawRunQueryResponseChunk;
                if (typed.transaction && typed.transaction.length > 0) {
                    capturedTx = typed.transaction;
                }
            });
            stream.on('error', err => finish(err));
            stream.on('end', () => finish());
            stream.resume();
        });
    } catch (err: unknown) {
        const code = typeof err === 'object' && err !== null ? (err as { code?: unknown }).code : undefined;
        const msg = err instanceof Error ? err.message : String(err);
        expect({ label, code, detail: code === expectedCode ? 'ok' : msg }).toEqual({
            label,
            code: expectedCode,
            detail: 'ok',
        });
        if (connection === 'CLOUD FIRESTORE' && capturedTx && capturedTx.length > 0) {
            await callRawUnaryRpc(firestore, 'rollback', { database, transaction: capturedTx }).catch(() => undefined);
        }
        return;
    }
    if (connection === 'CLOUD FIRESTORE' && capturedTx && capturedTx.length > 0) {
        await callRawUnaryRpc(firestore, 'rollback', { database, transaction: capturedTx }).catch(() => undefined);
    }
    throw new Error(`[${label}] Expected gRPC status code ${expectedCode}, but call resolved`);
}

interface ListenSnapshot {
    docs: RawDocument[];
}

class RawVectorListener {
    private readonly stream: NodeJS.ReadWriteStream & EventEmitter;
    private readonly docs = new Map<string, RawDocument>();
    private readonly queue: ListenSnapshot[] = [];
    private readonly waiters: Array<{ resolve: (snap: ListenSnapshot) => void; reject: (err: unknown) => void }> = [];
    private readonly pendingErrors: Array<{ code?: number; message?: string }> = [];
    private isCurrent = false;
    private dirty = true;
    private closed = false;

    private constructor(stream: NodeJS.ReadWriteStream & EventEmitter) {
        this.stream = stream;
        this.stream.on('data', (chunk: unknown) => {
            this.handleMessage(chunk as RawListenResponse);
        });
        this.stream.on('error', (err: unknown) => {
            const rpcErr =
                typeof err === 'object' && err !== null ? (err as { code?: number; message?: string }) : { message: String(err) };
            this.pushError(rpcErr);
        });
        this.stream.resume();
    }

    static async open(firestore: Firestore, vq: VectorQuery, tag: string): Promise<RawVectorListener> {
        const query = toRunQueryProto(vq);
        const database = query.parent.split('/documents')[0];
        const client = firestore as unknown as InternalListenClient;
        await client.initializeIfNeeded(tag);
        const stream = await client.requestStream(
            'listen',
            true,
            {
                database,
                addTarget: {
                    query,
                    targetId: 1,
                },
            },
            tag,
        );
        return new RawVectorListener(stream);
    }

    private handleMessage(msg: RawListenResponse): void {
        if (msg.targetChange) {
            const change = msg.targetChange;
            const noTargets = !change.targetIds || change.targetIds.length === 0;
            if (change.targetChangeType === 'RESET') {
                this.docs.clear();
                this.isCurrent = false;
                this.dirty = true;
            } else if (change.targetChangeType === 'CURRENT') {
                this.isCurrent = true;
            } else if (change.targetChangeType === 'REMOVE') {
                this.pushError({ code: change.cause?.code, message: change.cause?.message });
            } else if (change.targetChangeType === 'NO_CHANGE' && noTargets && this.isCurrent && this.dirty) {
                this.dirty = false;
                const snap: ListenSnapshot = { docs: Array.from(this.docs.values()) };
                const waiter = this.waiters.shift();
                if (waiter) {
                    waiter.resolve(snap);
                } else {
                    this.queue.push(snap);
                }
            }
        } else if (msg.documentChange?.document?.name) {
            const doc = msg.documentChange.document;
            const docName = doc.name as string;
            const targets = msg.documentChange.targetIds ?? [];
            const removed = msg.documentChange.removedTargetIds ?? [];
            if (targets.includes(1)) {
                this.docs.set(docName, doc);
                this.dirty = true;
            } else if (removed.includes(1)) {
                this.docs.delete(docName);
                this.dirty = true;
            }
        } else if (msg.documentDelete?.document || msg.documentRemove?.document) {
            const name = (msg.documentDelete?.document ?? msg.documentRemove?.document) as string;
            this.docs.delete(name);
            this.dirty = true;
        }
    }

    private pushError(err: { code?: number; message?: string }): void {
        this.pendingErrors.push(err);
        while (this.waiters.length > 0) {
            const w = this.waiters.shift();
            w?.reject(new Error(`Listen stream failed (${String(err.code)}): ${err.message ?? ''}`));
        }
    }

    async nextSnapshot(timeoutMs = 8_000): Promise<ListenSnapshot> {
        const queued = this.queue.shift();
        if (queued) return queued;
        const pendingErr = this.pendingErrors[0];
        if (pendingErr) {
            throw new Error(`Listen stream already errored (${String(pendingErr.code)}): ${pendingErr.message ?? ''}`);
        }
        return new Promise<ListenSnapshot>((resolve, reject) => {
            const timer = setTimeout(() => {
                const idx = this.waiters.findIndex(w => w.resolve === wrappedResolve);
                if (idx >= 0) this.waiters.splice(idx, 1);
                reject(new Error('Timed out waiting for Listen snapshot'));
            }, timeoutMs);
            const wrappedResolve = (snap: ListenSnapshot) => {
                clearTimeout(timer);
                resolve(snap);
            };
            const wrappedReject = (err: unknown) => {
                clearTimeout(timer);
                reject(err instanceof Error ? err : new Error(String(err)));
            };
            this.waiters.push({ resolve: wrappedResolve, reject: wrappedReject });
        });
    }

    close(): void {
        if (this.closed) return;
        this.closed = true;
        this.stream.end();
    }
}

describe.each(editions)('$description - vector search', fs => {
    const makeCol = (scenario: string): CollectionReference => fs.collection.doc(scenario).collection('collection');

    describe('1. VectorValue storage and atomic subpath overwrite', () => {
        test.concurrent('stores 1D, 2048D, -0.0, +/-Infinity, vectors in arrays, ____ key, and nested maps', async () => {
            const col = makeCol('storage-roundtrip');
            const docRef = col.doc('v1');
            const maxDimCoords = Array.from({ length: 2048 }, (_, idx) => idx * 0.5);
            await docRef.set(
                writeData({
                    ____: 'allowed',
                    oneD: FieldValue.vector([42]),
                    maxD: FieldValue.vector(maxDimCoords),
                    special: FieldValue.vector([-0, Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY]),
                    vecArr: [FieldValue.vector([1, 2]), FieldValue.vector([3, 4])],
                    nested: { deep: FieldValue.vector([9, 8]) },
                }),
            );

            const snap = await docRef.get();
            const data = readData(snap.data());
            expect(data.____).toBe('allowed');

            const oneD = data.oneD as VectorValue;
            expect(oneD).toBeInstanceOf(VectorValue);
            expect(oneD.toArray()).toEqual([42]);

            const maxD = data.maxD as VectorValue;
            expect(maxD).toBeInstanceOf(VectorValue);
            expect(maxD.isEqual(FieldValue.vector(maxDimCoords))).toBe(true);

            const special = (data.special as VectorValue).toArray();
            expect(Object.is(special[0], -0)).toBe(true);
            expect(special[1]).toBe(Number.NEGATIVE_INFINITY);
            expect(special[2]).toBe(Number.POSITIVE_INFINITY);

            const vecArr = data.vecArr as VectorValue[];
            expect(vecArr).toHaveLength(2);
            expect(vecArr[0].isEqual(FieldValue.vector([1, 2]))).toBe(true);
            expect(vecArr[1].isEqual(FieldValue.vector([3, 4]))).toBe(true);

            const nestedDeep = (data.nested as { deep: VectorValue }).deep;
            expect(nestedDeep.isEqual(FieldValue.vector([9, 8]))).toBe(true);
        });

        test.concurrent('atomic subpath update replaces vector field with map instead of mutating sentinel internals', async () => {
            const col = makeCol('storage-subpath');
            const docRef = col.doc('subpath');
            await docRef.set(writeData({ embedding: FieldValue.vector([1, 2]) }));
            await docRef.update({ 'embedding.nested': 'replaced' });

            const data = readData((await docRef.get()).data());
            expect(data).toEqual({ embedding: { nested: 'replaced' } });
        });
    });

    describe('2. Canonical cross-type and intra-type orderBy sorting', () => {
        test.concurrent('orders Array < Vector (length then element-wise) < Map', async () => {
            const col = makeCol('orderby-vector');
            const items: Array<{ id: string; val: unknown }> = [
                { id: 'array', val: [999] },
                { id: 'vec-1d', val: FieldValue.vector([100]) },
                { id: 'vec-2d-neginf', val: FieldValue.vector([Number.NEGATIVE_INFINITY, 0]) },
                { id: 'vec-2d-low', val: FieldValue.vector([-5, 10]) },
                { id: 'vec-2d-high', val: FieldValue.vector([-5, 11]) },
                { id: 'vec-2d-posinf', val: FieldValue.vector([Number.POSITIVE_INFINITY, 0]) },
                { id: 'vec-3d', val: FieldValue.vector([-999, -999, -999]) },
                { id: 'map-plain', val: { a: 1 } },
                { id: 'map-nonvec', val: { kind: 'other', value: [1] } },
            ];
            await Promise.all(items.map(item => col.doc(item.id).set(writeData({ val: item.val }))));

            const expectedAsc = [
                'array',
                'vec-1d',
                'vec-2d-neginf',
                'vec-2d-low',
                'vec-2d-high',
                'vec-2d-posinf',
                'vec-3d',
                'map-plain',
                'map-nonvec',
            ];
            const ascSnap = await col.orderBy('val', 'asc').get();
            expect(ascSnap.docs.map(d => d.id)).toEqual(expectedAsc);

            const descSnap = await col.orderBy('val', 'desc').get();
            expect(descSnap.docs.map(d => d.id)).toEqual([...expectedAsc].reverse());
        });
    });

    describe('3. EUCLIDEAN findNearest ordering and distanceResultField', () => {
        test.concurrent('orders closest euclidean vectors and populates distanceResultField', async () => {
            const col = makeCol('euclidean-basic');
            await Promise.all([
                col.doc('exact').set(writeData({ embedding: FieldValue.vector([1, 2]) })),
                col.doc('mid').set(writeData({ embedding: FieldValue.vector([-1, 0]) })),
                col.doc('far').set(writeData({ embedding: FieldValue.vector([4, 6]) })),
            ]);

            const snap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 2],
                    limit: 3,
                    distanceMeasure: 'EUCLIDEAN',
                    distanceResultField: 'dist',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['exact', 'mid', 'far']);
            expect(snap.docs[0].get('dist')).toBeCloseTo(0, 9);
            expect(snap.docs[1].get('dist')).toBeCloseTo(Math.sqrt(8), 9);
            expect(snap.docs[2].get('dist')).toBeCloseTo(5, 9);
        });

        test.concurrent('handles subnormal-underflow (1e-200) and large finite (1e100) vectors', async () => {
            const col = makeCol('euclidean-scale-1e100');
            await Promise.all([
                col.doc('tiny').set(writeData({ embedding: FieldValue.vector([1e-200, 0]) })),
                col.doc('large').set(writeData({ embedding: FieldValue.vector([1e100, 0]) })),
            ]);

            const snap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: FieldValue.vector([0, 0]),
                    limit: 2,
                    distanceMeasure: 'EUCLIDEAN',
                    distanceResultField: 'dist',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['tiny', 'large']);
            const tinyDist = snap.docs[0].get('dist') as number;
            const largeDist = snap.docs[1].get('dist') as number;
            expect(tinyDist).toBe(0);
            expect(largeDist / 1e100).toBeCloseTo(1, 9);
        });

        test.concurrent('handles finite square-overflow (1e200) candidate vector on Enterprise', async () => {
            // Cloud Standard Edition flat vector index returns 13 INTERNAL when finite difference squares overflow f64::MAX
            if (!fs.enterprise) return;
            const col = makeCol('euclidean-1e200');
            await col.doc('huge').set(writeData({ embedding: FieldValue.vector([1e200, 0]) }));

            const snap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: FieldValue.vector([0, 0]),
                    limit: 1,
                    distanceMeasure: 'EUCLIDEAN',
                    distanceResultField: 'dist',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['huge']);
            expect(snap.docs[0].get('dist')).toBe(Number.POSITIVE_INFINITY);
        });

        notImplementedInJava ||
            test.concurrent('handles +Infinity candidate vector according to edition semantics', async () => {
                const col = makeCol('euclidean-inf');
                await col.doc('inf').set(writeData({ embedding: FieldValue.vector([Number.POSITIVE_INFINITY, 0]) }));

                const snap = await col
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: FieldValue.vector([0, 0]),
                        limit: 1,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'dist',
                    })
                    .get();

                if (fs.enterprise) {
                    expect(snap.docs.map(d => d.id)).toEqual(['inf']);
                    expect(snap.docs[0].get('dist')).toBe(Number.POSITIVE_INFINITY);
                } else {
                    expect(snap.docs.map(d => d.id)).toEqual([]);
                }
            });

        notImplementedInJava ||
            test.concurrent('handles +Infinity queryVector against finite candidates under EUCLIDEAN', async () => {
                const col = makeCol('euclidean-inf-query');
                await col.doc('finite').set(writeData({ embedding: FieldValue.vector([1, 0]) }));

                const snap = await col
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: FieldValue.vector([Number.POSITIVE_INFINITY, 0]),
                        limit: 5,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'dist',
                    })
                    .get();

                if (fs.enterprise) {
                    expect(snap.docs.map(d => d.id)).toEqual(['finite']);
                    expect(snap.docs[0].get('dist')).toBe(Number.POSITIVE_INFINITY);
                } else {
                    expect(snap.docs.map(d => d.id)).toEqual([]);
                }
            });
    });

    describe('4. COSINE findNearest ordering, silent skipping, and zero-vector candidate error', () => {
        test.concurrent('orders cosine distances and skips missing fields, arrays, strings, and mismatched dimensions', async () => {
            const col = makeCol('cosine-skip');
            await Promise.all([
                col.doc('aligned').set(writeData({ embedding: FieldValue.vector([2, 0]) })),
                col.doc('diag').set(writeData({ embedding: FieldValue.vector([1, 1]) })),
                col.doc('ortho').set(writeData({ embedding: FieldValue.vector([0, 5]) })),
                col.doc('opposite').set(writeData({ embedding: FieldValue.vector([-3, 0]) })),
                col.doc('missing').set(writeData({ other: 'value' })),
                col.doc('arr').set(writeData({ embedding: [1, 0] })),
                col.doc('str').set(writeData({ embedding: 'not-a-vector' })),
                col.doc('dim3').set(writeData({ embedding: FieldValue.vector([1, 0, 0]) })),
            ]);

            const snap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    limit: 10,
                    distanceMeasure: 'COSINE',
                    distanceResultField: 'dist',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['aligned', 'diag', 'ortho', 'opposite']);
            expect(snap.docs[0].get('dist')).toBeCloseTo(0, 9);
            expect(snap.docs[1].get('dist')).toBeCloseTo(1 - 1 / Math.SQRT2, 9);
            expect(snap.docs[2].get('dist')).toBeCloseTo(1, 9);
            expect(snap.docs[3].get('dist')).toBeCloseTo(2, 9);
        });

        notImplementedInJava ||
            test.concurrent('rejects when an unfiltered [0, 0] candidate is scanned under COSINE', async () => {
                const col = makeCol('cosine-zero-cand');
                await Promise.all([
                    col.doc('valid').set(writeData({ embedding: FieldValue.vector([1, 0]) })),
                    col.doc('zero').set(writeData({ embedding: FieldValue.vector([0, 0]) })),
                ]);

                await expect(
                    col
                        .findNearest({
                            vectorField: 'embedding',
                            queryVector: [1, 0],
                            limit: 10,
                            distanceMeasure: 'COSINE',
                        })
                        .get(),
                ).rejects.toThrow(fs.enterprise ? /3 INVALID_ARGUMENT/ : /9 FAILED_PRECONDITION.*magnitude of zero/i);
            });
    });

    describe('5. DOT_PRODUCT findNearest descending similarity ordering', () => {
        test.concurrent('orders highest dot product first with expected scores', async () => {
            const col = makeCol('dot-product');
            await Promise.all([
                col.doc('high').set(writeData({ embedding: FieldValue.vector([3, 4]) })),
                col.doc('mid').set(writeData({ embedding: FieldValue.vector([1, 1]) })),
                col.doc('zero').set(writeData({ embedding: FieldValue.vector([2, -1]) })),
                col.doc('neg').set(writeData({ embedding: FieldValue.vector([-1, -1]) })),
            ]);

            const snap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 2],
                    limit: 4,
                    distanceMeasure: 'DOT_PRODUCT',
                    distanceResultField: 'score',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['high', 'mid', 'zero', 'neg']);
            expect(snap.docs.map(d => d.get('score') as number)).toEqual([11, 3, 0, -3]);
        });

        test.concurrent('handles +Infinity queryVector against finite candidates under DOT_PRODUCT', async () => {
            const col = makeCol('dot-inf-query');
            await col.doc('finite').set(writeData({ embedding: FieldValue.vector([1, 2]) }));

            const snap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: FieldValue.vector([Number.POSITIVE_INFINITY, 0]),
                    limit: 5,
                    distanceMeasure: 'DOT_PRODUCT',
                    distanceResultField: 'score',
                })
                .get();

            expect(snap.docs.map(d => d.id)).toEqual(['finite']);
            expect(snap.docs[0].get('score')).toBe(Number.POSITIVE_INFINITY);
        });
    });

    describe('6. Inclusive distanceThreshold filtering across distance measures', () => {
        test.concurrent('applies inclusive upper bound for EUCLIDEAN and COSINE, and inclusive lower bound for DOT_PRODUCT', async () => {
            const col = makeCol('threshold');
            await Promise.all([
                col.doc('d1').set(writeData({ embedding: FieldValue.vector([1, 0]) })),
                col.doc('d2').set(writeData({ embedding: FieldValue.vector([3, 4]) })),
                col.doc('d3').set(writeData({ embedding: FieldValue.vector([0, 2]) })),
                col.doc('d4').set(writeData({ embedding: FieldValue.vector([5.001, 0]) })),
                col.doc('d5').set(writeData({ embedding: FieldValue.vector([-1, 1]) })),
            ]);

            // EUCLIDEAN to [0, 0] with threshold 5.0: [1,0] (1), [-1,1] (sqrt(2)), [0,2] (2), [3,4] (5.0 exact) included; [5.001, 0] excluded
            const eucSnap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [0, 0],
                    limit: 10,
                    distanceMeasure: 'EUCLIDEAN',
                    distanceThreshold: 5,
                })
                .get();
            expect(eucSnap.docs.map(d => d.id)).toEqual(['d1', 'd5', 'd3', 'd2']);

            // COSINE to [1, 0] with threshold 1.0: [1,0] (0), [5.001,0] (0), [3,4] (0.4), [0,2] (1.0 exact) included; [-1,1] (1 + 1/sqrt(2)) excluded
            const cosSnap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    limit: 10,
                    distanceMeasure: 'COSINE',
                    distanceThreshold: 1,
                })
                .get();
            expect(cosSnap.docs.map(d => d.id)).toEqual(['d1', 'd4', 'd2', 'd3']);

            // DOT_PRODUCT to [1, 0] with threshold 3.0: [5.001, 0] (5.001) and [3, 4] (3.0 exact) included; <= 1.0 excluded
            const dotSnap = await col
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    limit: 10,
                    distanceMeasure: 'DOT_PRODUCT',
                    distanceThreshold: 3,
                })
                .get();
            expect(dotSnap.docs.map(d => d.id)).toEqual(['d4', 'd2']);

            // Raw gRPC check for distanceThreshold === 0 (the Node SDK omits falsy `0` in vector-query.js)
            const baseZeroProto = toRunQueryProto(
                col.findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    limit: 10,
                    distanceMeasure: 'EUCLIDEAN',
                    distanceResultField: 'dist',
                }),
            );
            const sq = baseZeroProto.structuredQuery as { findNearest: Record<string, unknown> };
            const zeroThresholdChunks = await callRawStreamRpc<RawRunQueryRequest, RawRunQueryResponseChunk>(fs.firestore, 'runQuery', {
                parent: baseZeroProto.parent,
                structuredQuery: {
                    ...sq,
                    findNearest: { ...sq.findNearest, distanceThreshold: { value: 0 } },
                },
            });
            const zeroThresholdDocs = zeroThresholdChunks.map(c => c.document).filter((d): d is RawDocument => Boolean(d));
            expect(zeroThresholdDocs.map(d => shortDocId(d.name ?? ''))).toEqual(['d1']);
            expect(zeroThresholdDocs[0].fields?.dist?.doubleValue).toBe(0);
        });
    });

    describe('7. Composite pre-filters and inequality orderBy precedence', () => {
        // Java emulator does not support nested FieldPaths on vectorField in findNearest
        notImplementedInJava ||
            test.concurrent('combines category == and tags array-contains pre-filters on 3D nested FieldPath', async () => {
                const col = makeCol('composite-nested');
                await Promise.all([
                    col.doc('match-close').set(
                        writeData({
                            category: 'books',
                            tags: ['ml', 'rust'],
                            vectorQueryFields: { nested: FieldValue.vector([1, 1, 0]) },
                        }),
                    ),
                    col.doc('match-far').set(
                        writeData({
                            category: 'books',
                            tags: ['ml'],
                            vectorQueryFields: { nested: FieldValue.vector([4, 1, 0]) },
                        }),
                    ),
                    col.doc('wrong-cat').set(
                        writeData({
                            category: 'music',
                            tags: ['ml'],
                            vectorQueryFields: { nested: FieldValue.vector([1, 1, 0]) },
                        }),
                    ),
                    col.doc('wrong-tag').set(
                        writeData({
                            category: 'books',
                            tags: ['fiction'],
                            vectorQueryFields: { nested: FieldValue.vector([1, 1, 0]) },
                        }),
                    ),
                ]);

                const snap = await col
                    .where('category', '==', 'books')
                    .where('tags', 'array-contains', 'ml')
                    .findNearest({
                        vectorField: new FieldPath('vectorQueryFields', 'nested'),
                        queryVector: [1, 1, 0],
                        limit: 5,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'dist',
                    })
                    .get();

                expect(snap.docs.map(d => d.id)).toEqual(['match-close', 'match-far']);
                expect(snap.docs[0].get('dist')).toBeCloseTo(0, 9);
                expect(snap.docs[1].get('dist')).toBeCloseTo(3, 9);
            });

        test.concurrent('uses explicit orderBy as secondary tie-breaker after distance', async () => {
            const col = makeCol('ineq-orderby');
            await Promise.all([
                col.doc('a').set(writeData({ price: 50, embedding: FieldValue.vector([1, 0]) })),
                col.doc('b').set(writeData({ price: 10, embedding: FieldValue.vector([10, 0]) })),
                col.doc('c').set(writeData({ price: 20, embedding: FieldValue.vector([1, 0]) })),
                col.doc('d').set(writeData({ price: 20, embedding: FieldValue.vector([1, 0]) })),
                col.doc('excluded').set(writeData({ price: 100, embedding: FieldValue.vector([0, 0]) })),
            ]);

            const ascSnap = await col
                .where('price', '<', 100)
                .orderBy('price', 'asc')
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [0, 0],
                    limit: 10,
                    distanceMeasure: 'EUCLIDEAN',
                })
                .get();
            expect(ascSnap.docs.map(d => d.id)).toEqual(['c', 'd', 'a', 'b']);

            const ascLimit1Snap = await col
                .where('price', '<', 100)
                .orderBy('price', 'asc')
                .findNearest({
                    vectorField: 'embedding',
                    queryVector: [0, 0],
                    limit: 1,
                    distanceMeasure: 'EUCLIDEAN',
                })
                .get();
            expect(ascLimit1Snap.docs.map(d => d.id)).toEqual(['c']);

            if (fs.enterprise) {
                const descSnap = await col
                    .where('price', '<', 100)
                    .orderBy('price', 'desc')
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: [0, 0],
                        limit: 10,
                        distanceMeasure: 'EUCLIDEAN',
                    })
                    .get();
                expect(descSnap.docs.map(d => d.id)).toEqual(['c', 'd', 'a', 'b']);
            }
        });
    });

    describe('8. select() projection shadowing and RunAggregationQuery over findNearest', () => {
        test.concurrent(
            'shadows/masks distanceResultField under select() and computes count/sum/average via RunAggregationQuery',
            async () => {
                const col = makeCol('projection-agg');
                await Promise.all([
                    col.doc('p1').set(
                        writeData({
                            title: 'one',
                            price: 5,
                            meta: { author: 'alice', rank: 5 },
                            embedding: FieldValue.vector([1, 0]),
                        }),
                    ),
                    col.doc('p2').set(
                        writeData({
                            title: 'two',
                            price: 7,
                            meta: { author: 'bob', rank: 7 },
                            embedding: FieldValue.vector([2, 0]),
                        }),
                    ),
                ]);

                const included = await col
                    .select('title', 'meta')
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: [1, 0],
                        limit: 2,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'meta',
                    })
                    .get();
                expect(included.docs.map(d => d.data())).toEqual([
                    { title: 'one', meta: 0 },
                    { title: 'two', meta: 1 },
                ]);

                const shadowedSubfield = await col
                    .select('meta.author')
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: [1, 0],
                        limit: 1,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'meta',
                    })
                    .get();
                expect(shadowedSubfield.docs.map(d => d.data())).toEqual([{}]);

                const maskedOut = await col
                    .select('title')
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: [1, 0],
                        limit: 1,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'meta',
                    })
                    .get();
                expect(maskedOut.docs.map(d => d.data())).toEqual([{ title: 'one' }]);

                const nameOnly = await col
                    .select('__name__')
                    .findNearest({
                        vectorField: 'embedding',
                        queryVector: [1, 0],
                        limit: 1,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'meta',
                    })
                    .get();
                expect(nameOnly.docs.map(d => d.data())).toEqual([{}]);

                const virtualDistQuery = toRunQueryProto(
                    col.findNearest({
                        vectorField: 'embedding',
                        queryVector: [1, 0],
                        limit: 2,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'dist',
                    }),
                );

                if (fs.enterprise) {
                    const virtualAggChunks = await callRawStreamRpc<object, RawAggregationResponseChunk>(
                        fs.firestore,
                        'runAggregationQuery',
                        {
                            parent: virtualDistQuery.parent,
                            structuredAggregationQuery: {
                                structuredQuery: virtualDistQuery.structuredQuery,
                                aggregations: [
                                    { alias: 'n', count: {} },
                                    { alias: 'sumDist', sum: { field: { fieldPath: 'dist' } } },
                                    { alias: 'avgDist', avg: { field: { fieldPath: 'dist' } } },
                                ],
                            },
                        },
                    );
                    const virtualAggFields = virtualAggChunks.find(c => c.result?.aggregateFields)?.result?.aggregateFields;
                    expect(virtualAggFields).toBeDefined();
                    expect(virtualAggFields?.n?.integerValue).toBe('2');
                    expect(virtualAggFields?.sumDist?.doubleValue).toBeCloseTo(1, 9);
                    expect(virtualAggFields?.avgDist?.doubleValue).toBeCloseTo(0.5, 9);

                    const baseQuery = toRunQueryProto(
                        col.findNearest({
                            vectorField: 'embedding',
                            queryVector: [1, 0],
                            limit: 2,
                            distanceMeasure: 'EUCLIDEAN',
                            distanceResultField: 'meta',
                        }),
                    );
                    const aggChunks = await callRawStreamRpc<object, RawAggregationResponseChunk>(fs.firestore, 'runAggregationQuery', {
                        parent: baseQuery.parent,
                        structuredAggregationQuery: {
                            structuredQuery: baseQuery.structuredQuery,
                            aggregations: [
                                { alias: 'n', count: {} },
                                { alias: 'sumDist', sum: { field: { fieldPath: 'meta' } } },
                                { alias: 'avgDist', avg: { field: { fieldPath: 'meta' } } },
                                { alias: 'sumShadowed', sum: { field: { fieldPath: 'meta.rank' } } },
                                { alias: 'avgShadowed', avg: { field: { fieldPath: 'meta.rank' } } },
                            ],
                        },
                    });

                    const aggFields = aggChunks.find(c => c.result?.aggregateFields)?.result?.aggregateFields;
                    expect(aggFields).toBeDefined();
                    expect(aggFields?.n?.integerValue).toBe('2');
                    expect(aggFields?.sumDist?.doubleValue).toBeCloseTo(1, 9);
                    expect(aggFields?.avgDist?.doubleValue).toBeCloseTo(0.5, 9);
                    expect(aggFields?.sumShadowed && 'nullValue' in aggFields.sumShadowed).toBe(true);
                    expect(aggFields?.avgShadowed && 'nullValue' in aggFields.avgShadowed).toBe(true);
                } else {
                    const countChunks = await callRawStreamRpc<object, RawAggregationResponseChunk>(fs.firestore, 'runAggregationQuery', {
                        parent: virtualDistQuery.parent,
                        structuredAggregationQuery: {
                            structuredQuery: virtualDistQuery.structuredQuery,
                            aggregations: [{ alias: 'n', count: {} }],
                        },
                    });
                    const countFields = countChunks.find(c => c.result?.aggregateFields)?.result?.aggregateFields;
                    expect(countFields?.n?.integerValue).toBe('2');
                }
            },
        );
    });

    describe('9. Listen behavior with findNearest queries', () => {
        notImplementedInJava ||
            test.concurrent('injects distance on initial snapshot and delivers subsequent updates incrementally', async () => {
                const col = makeCol('listen-topk');
                await Promise.all([
                    col.doc('d1').set(writeData({ embedding: FieldValue.vector([1, 0.5]) })),
                    col.doc('d_init_far').set(writeData({ embedding: FieldValue.vector([0, 1]) })),
                ]);

                const listener = await RawVectorListener.open(
                    fs.firestore,
                    col.findNearest({
                        vectorField: 'embedding',
                        queryVector: [1, 0],
                        limit: 2,
                        distanceMeasure: 'COSINE',
                        distanceResultField: 'dist',
                        distanceThreshold: 0.2,
                    }),
                    'listen-cosine-topk',
                );

                try {
                    const init = await listener.nextSnapshot();
                    expect(init.docs.map(d => shortDocId(d.name ?? ''))).toEqual(['d1']);
                    expect(init.docs[0].fields?.dist?.doubleValue).toBeCloseTo(1 - 1 / Math.sqrt(1.25), 9);

                    await col.doc('d_far').set(writeData({ embedding: FieldValue.vector([0, 1]) }));
                    await col.doc('d2').set(writeData({ embedding: FieldValue.vector([1, 0]) }));
                    await col.doc('d_zero').set(writeData({ embedding: FieldValue.vector([0, 0]) }));

                    let next = await listener.nextSnapshot();
                    while (!next.docs.some(d => shortDocId(d.name ?? '') === 'd_zero')) {
                        next = await listener.nextSnapshot();
                    }
                    const nextIds = next.docs.map(d => shortDocId(d.name ?? ''));
                    expect(nextIds).toIncludeSameMembers(['d1', 'd_far', 'd2', 'd_zero']);
                    const d1After = next.docs.find(d => shortDocId(d.name ?? '') === 'd1');
                    expect(d1After?.fields?.dist?.doubleValue).toBeCloseTo(1 - 1 / Math.sqrt(1.25), 9);
                    for (const id of ['d_far', 'd2', 'd_zero']) {
                        const doc = next.docs.find(d => shortDocId(d.name ?? '') === id);
                        expect(doc?.fields?.dist).toBeUndefined();
                    }

                    await col.doc('d2').delete();
                    let afterDel = await listener.nextSnapshot();
                    while (afterDel.docs.some(d => shortDocId(d.name ?? '') === 'd2')) {
                        afterDel = await listener.nextSnapshot();
                    }
                    expect(afterDel.docs.map(d => shortDocId(d.name ?? ''))).toIncludeSameMembers(['d1', 'd_far', 'd_zero']);
                } finally {
                    listener.close();
                }
            });

        test.concurrent('removes tracked documents incrementally when they exit prefilters', async () => {
            const col = makeCol('listen-prefilter-exit');
            await Promise.all([
                col.doc('p_init').set(writeData({ price: 10, embedding: FieldValue.vector([1, 0]) })),
                col.doc('p_skip').set(writeData({ price: 100, embedding: FieldValue.vector([1, 0]) })),
            ]);

            const listener = await RawVectorListener.open(
                fs.firestore,
                col.where('price', '<', 50).findNearest({
                    vectorField: 'embedding',
                    queryVector: [1, 0],
                    limit: 5,
                    distanceMeasure: 'EUCLIDEAN',
                    distanceResultField: 'dist',
                }),
                'listen-prefilter-exit',
            );

            try {
                const init = await listener.nextSnapshot();
                expect(init.docs.map(d => shortDocId(d.name ?? ''))).toEqual(['p_init']);
                expect(init.docs[0].fields?.dist?.doubleValue).toBe(0);

                await col.doc('p_added').set(writeData({ price: 20, embedding: FieldValue.vector([2, 0]) }));
                let afterAdd = await listener.nextSnapshot();
                while (!afterAdd.docs.some(d => shortDocId(d.name ?? '') === 'p_added')) {
                    afterAdd = await listener.nextSnapshot();
                }
                expect(afterAdd.docs.map(d => shortDocId(d.name ?? ''))).toIncludeSameMembers(['p_init', 'p_added']);

                await col.doc('p_added').update({ price: 200 });
                let afterExit = await listener.nextSnapshot();
                while (afterExit.docs.some(d => shortDocId(d.name ?? '') === 'p_added')) {
                    afterExit = await listener.nextSnapshot();
                }
                expect(afterExit.docs.map(d => shortDocId(d.name ?? ''))).toEqual(['p_init']);
                expect(afterExit.docs[0].fields?.dist?.doubleValue).toBe(0);
            } finally {
                listener.close();
            }
        });
    });

    notImplementedInJava ||
        describe('10. Edition-aware transactional lock scope and NewTransaction rollback', () => {
            test.concurrent(
                'verifies transactional read-set semantics via ConcurrentTest across editions',
                async () => {
                    const col = makeCol('txn-lock-scope');
                    const otherDoc = makeCol('txn-lock-scope-other').doc('o1');
                    const d1 = col.doc('d1');
                    const d2 = col.doc('d2');
                    const d3 = col.doc('d3');
                    await Promise.all([
                        d1.set(writeData({ category: 'keep', tags: ['v'], vectorQueryFields: { nested: FieldValue.vector([2, 1, 0]) } })),
                        d2.set(writeData({ category: 'keep', tags: ['v'], vectorQueryFields: { nested: FieldValue.vector([5, 5, 0]) } })),
                        d3.set(writeData({ category: 'skip', tags: ['v'], vectorQueryFields: { nested: FieldValue.vector([0, 0, 0]) } })),
                        otherDoc.set(writeData({ ok: 1 })),
                    ]);

                    const vq = col
                        .where('category', '==', 'keep')
                        .where('tags', 'array-contains', 'v')
                        .findNearest({
                            vectorField: new FieldPath('vectorQueryFields', 'nested'),
                            queryVector: [1, 1, 0],
                            limit: 1,
                            distanceMeasure: 'EUCLIDEAN',
                        });

                    const sampleProto = toRunQueryProto(vq);
                    const database = sampleProto.parent.split('/documents')[0];
                    const ct = new ConcurrentTest({ timeout: 25_000 });

                    await ct.run(
                        async () => {
                            let begunTx: Uint8Array | undefined;
                            let committed = false;
                            try {
                                const begun = await callRawUnaryRpc<object, RawBeginTransactionResponse>(fs.firestore, 'beginTransaction', {
                                    database,
                                    options: { readWrite: {} },
                                });
                                begunTx = begun.transaction;
                                const chunks = await callRawStreamRpc<RawRunQueryRequest, RawRunQueryResponseChunk>(
                                    fs.firestore,
                                    'runQuery',
                                    toRunQueryProto(vq, begun.transaction),
                                );
                                const returnedIds = chunks.filter(c => c.document?.name).map(c => shortDocId(c.document?.name ?? ''));
                                expect(returnedIds).toEqual(['d1']);

                                ct.event('query-done');
                                await ct.when('actor-b-ready-to-commit');

                                if (fs.concurrencyMode === 'optimistic') {
                                    await expectRpcCode(
                                        () =>
                                            callRawUnaryRpc(fs.firestore, 'commit', {
                                                database,
                                                transaction: begun.transaction,
                                                writes: [
                                                    {
                                                        update: {
                                                            name: docNameFromParent(sampleProto.parent, d3),
                                                            fields: { updatedByTxn: { booleanValue: true } },
                                                        },
                                                        updateMask: { fieldPaths: ['updatedByTxn'] },
                                                    },
                                                ],
                                            }),
                                        10,
                                        'optimistic-commit-aborted',
                                    );
                                } else {
                                    await callRawUnaryRpc(fs.firestore, 'commit', {
                                        database,
                                        transaction: begun.transaction,
                                        writes: [],
                                    });
                                }
                                committed = true;
                                ct.event('txn-finished');
                            } finally {
                                if (!committed && begunTx) {
                                    await callRawUnaryRpc(fs.firestore, 'rollback', { database, transaction: begunTx }).catch(
                                        () => undefined,
                                    );
                                }
                            }
                        },
                        async () => {
                            await ct.when('query-done');

                            if (!fs.enterprise) {
                                // Standard (pessimistic): pre-filtered non-top-K `d2` is NOT locked
                                await d2.update({ standardWriteOk: true });
                                let d1WriteDone = false;
                                const d1Write = d1.update({ blockedTopK: true }).then(() => {
                                    d1WriteDone = true;
                                });
                                await time(250);
                                expect(d1WriteDone).toBe(false);
                                ct.event('actor-b-ready-to-commit');
                                await d1Write;
                                expect(d1WriteDone).toBe(true);
                            } else if (fs.concurrencyMode === 'pessimistic') {
                                // Enterprise (pessimistic): outside-collection `otherDoc` is unlocked; scanned collection docs `d2` and `d3` are locked
                                await otherDoc.update({ outsideColOk: true });
                                let d2WriteDone = false;
                                let d3WriteDone = false;
                                const d2Write = d2.update({ blockedCandidate: true }).then(() => {
                                    d2WriteDone = true;
                                });
                                const d3Write = d3.update({ filteredOutLocked: true }).then(() => {
                                    d3WriteDone = true;
                                });
                                await time(400);
                                expect({ d2WriteDone, d3WriteDone }).toEqual({ d2WriteDone: false, d3WriteDone: false });
                                ct.event('actor-b-ready-to-commit');
                                await Promise.all([d2Write, d3Write]);
                                expect({ d2WriteDone, d3WriteDone }).toEqual({ d2WriteDone: true, d3WriteDone: true });
                            } else {
                                // Enterprise (optimistic): moving `d2` strictly closer into top-1 causes tx commit ABORTED (10)
                                await d2.update({ vectorQueryFields: { nested: FieldValue.vector([1, 1, 0]) } });
                                ct.event('actor-b-ready-to-commit');
                                await ct.when('txn-finished');
                            }
                        },
                    );
                },
                30_000,
            );

            test.concurrent(
                'rolls back NewTransaction on mid-query COSINE [0, 0] failure without leaking locks',
                async () => {
                    const col = makeCol('newtxn-nan-rollback');
                    const validDoc = col.doc('valid');
                    await Promise.all([
                        validDoc.set(writeData({ embedding: FieldValue.vector([1, 0]) })),
                        col.doc('badZero').set(writeData({ embedding: FieldValue.vector([0, 0]) })),
                    ]);

                    const baseProto = toRunQueryProto(
                        col.findNearest({
                            vectorField: 'embedding',
                            queryVector: [1, 0],
                            limit: 5,
                            distanceMeasure: 'COSINE',
                        }),
                    );
                    const database = baseProto.parent.split('/documents')[0];
                    await expectRunQueryNewTxnError(
                        fs.firestore,
                        {
                            ...baseProto,
                            newTransaction: { readWrite: {} },
                        },
                        fs.enterprise ? 3 : 9,
                        database,
                        'newtxn-cosine-zero',
                    );

                    await validDoc.update({ afterFailureOk: true });
                    expect(readData((await validDoc.get()).data()).afterFailureOk).toBe(true);
                },
                30_000,
            );
        });

    describe('11. Isolated collectionGroup segment-wise tie-breaking', () => {
        const runRoot = fs.collection.parent;
        if (!runRoot) {
            throw new Error('Expected test collection to have a parent run document');
        }
        const tieRunId = `tie-${runRoot.id}`;
        const docX = runRoot.collection('parents').doc('doc').collection('collection').doc('x');
        const docDash1X = runRoot.collection('parents').doc('doc-1').collection('collection').doc('x');

        afterAll(async () => {
            await Promise.all([docX.delete().catch(() => undefined), docDash1X.delete().catch(() => undefined)]);
        });

        test('orders parents/doc/collection/x before parents/doc-1/collection/x segment-wise', async () => {
            try {
                await Promise.all([
                    docX.set(writeData({ runId: tieRunId, tieEmbedding: FieldValue.vector([1, 0]) })),
                    docDash1X.set(writeData({ runId: tieRunId, tieEmbedding: FieldValue.vector([1, 0]) })),
                ]);

                const snap = await fs.firestore
                    .collectionGroup('collection')
                    .where('runId', '==', tieRunId)
                    .findNearest({
                        vectorField: 'tieEmbedding',
                        queryVector: [1, 0],
                        limit: 2,
                        distanceMeasure: 'EUCLIDEAN',
                        distanceResultField: 'dist',
                    })
                    .get();

                expect(snap.docs.map(d => d.ref.path)).toEqual([docX.path, docDash1X.path]);
            } finally {
                await Promise.all([docX.delete().catch(() => undefined), docDash1X.delete().catch(() => undefined)]);
            }
        });
    });

    notImplementedInJava ||
        describe('12. Domain validation and Cloud limitation canaries', () => {
            test.concurrent(
                'rejects invalid vectors, reserved keys/masks/transforms, and invalid findNearest inputs',
                async () => {
                    const col = makeCol('validation');
                    const seed = col.doc('seed');
                    await seed.set(writeData({ embedding: FieldValue.vector([1, 0]) }));
                    const sampleProto = toRunQueryProto(
                        col.findNearest({
                            vectorField: 'embedding',
                            queryVector: [1, 0],
                            limit: 1,
                            distanceMeasure: 'EUCLIDEAN',
                        }),
                    );
                    const database = sampleProto.parent.split('/documents')[0];
                    const badTargetName = docNameFromParent(sampleProto.parent, col.doc('raw-bad'));

                    // Client SDK synchronous checks
                    expect(() =>
                        col.findNearest({ vectorField: 'embedding', queryVector: [1, 0], limit: 0, distanceMeasure: 'EUCLIDEAN' }),
                    ).toThrow();
                    expect(() =>
                        col.findNearest({ vectorField: 'embedding', queryVector: [], limit: 5, distanceMeasure: 'EUCLIDEAN' }),
                    ).toThrow();

                    // Client SDK wire rejections
                    await expect(col.doc('sdk-0d').set(writeData({ v: FieldValue.vector([]) }))).rejects.toThrow(/3 INVALID_ARGUMENT/);
                    await expect(
                        col.doc('sdk-2049d').set(writeData({ v: FieldValue.vector(new Array<number>(2049).fill(0)) })),
                    ).rejects.toThrow(/3 INVALID_ARGUMENT/);
                    await expect(col.doc('sdk-nan').set(writeData({ v: FieldValue.vector([Number.NaN]) }))).rejects.toThrow(
                        /3 INVALID_ARGUMENT/,
                    );
                    await expect(col.doc('sdk-reserved').set(writeData({ __custom__: 1 }))).rejects.toThrow(/3 INVALID_ARGUMENT/);
                    await expect(col.doc('sdk-reserved-tf').update({ __custom__: FieldValue.serverTimestamp() })).rejects.toThrow(
                        /3 INVALID_ARGUMENT/,
                    );
                    await expect(
                        col.findNearest({ vectorField: 'embedding', queryVector: [1, 0], limit: 1001, distanceMeasure: 'EUCLIDEAN' }).get(),
                    ).rejects.toThrow(/3 INVALID_ARGUMENT/);
                    await expect(
                        col.findNearest({ vectorField: 'embedding', queryVector: [0, 0], limit: 5, distanceMeasure: 'COSINE' }).get(),
                    ).rejects.toThrow(fs.enterprise ? /3 INVALID_ARGUMENT/ : /9 FAILED_PRECONDITION.*magnitude of zero/i);

                    // Raw protobuf wire Commit rejections (code === 3)
                    const badVectorMaps: RawProtoValue[] = [
                        createVectorMap([]),
                        createVectorMap(new Array<number>(2049).fill(1)),
                        createVectorMap([Number.NaN]),
                        {
                            mapValue: {
                                fields: {
                                    __type__: { stringValue: '__vector__' },
                                    value: { arrayValue: { values: [{ integerValue: '1' }] } },
                                },
                            },
                        },
                        { mapValue: { fields: { __type__: { stringValue: '__vector__' } } } },
                        {
                            mapValue: {
                                fields: {
                                    __type__: { stringValue: '__vector__' },
                                    value: { arrayValue: { values: [{ doubleValue: 1 }] } },
                                    extra: { integerValue: '1' },
                                },
                            },
                        },
                    ];
                    for (const badMap of badVectorMaps) {
                        await expectRpcCode(
                            () =>
                                callRawUnaryRpc(fs.firestore, 'commit', {
                                    database,
                                    writes: [{ update: { name: badTargetName, fields: { v: badMap } } }],
                                }),
                            3,
                            'raw-commit-bad-vector',
                        );
                    }

                    // Reserved __custom__ key, updateMask, and updateTransforms over wire
                    await expectRpcCode(
                        () =>
                            callRawUnaryRpc(fs.firestore, 'commit', {
                                database,
                                writes: [{ update: { name: badTargetName, fields: { __custom__: { integerValue: '1' } } } }],
                            }),
                        3,
                        'raw-commit-reserved-key',
                    );
                    await expectRpcCode(
                        () =>
                            callRawUnaryRpc(fs.firestore, 'commit', {
                                database,
                                writes: [
                                    {
                                        update: { name: badTargetName, fields: { ok: { integerValue: '1' } } },
                                        updateMask: { fieldPaths: ['__custom__'] },
                                    },
                                ],
                            }),
                        3,
                        'raw-commit-reserved-mask',
                    );
                    await expectRpcCode(
                        () =>
                            callRawUnaryRpc(fs.firestore, 'commit', {
                                database,
                                writes: [
                                    {
                                        update: { name: docNameFromParent(sampleProto.parent, seed), fields: {} },
                                        updateMask: { fieldPaths: [] },
                                        updateTransforms: [{ fieldPath: '__custom__', setToServerValue: 'REQUEST_TIME' }],
                                    },
                                ],
                            }),
                        3,
                        'raw-commit-reserved-transform',
                    );

                    // Raw protobuf wire RunQuery rejections
                    const invalidRunQueries: Array<{ label: string; expectedCode: number; req: RawRunQueryRequest }> = [
                        {
                            label: 'raw-limit-0',
                            expectedCode: 3,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    findNearest: {
                                        vectorField: { fieldPath: 'embedding' },
                                        queryVector: createVectorMap([1, 0]),
                                        distanceMeasure: 'EUCLIDEAN',
                                        limit: { value: 0 },
                                    },
                                },
                            },
                        },
                        {
                            label: 'raw-limit-1001',
                            expectedCode: 3,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    findNearest: {
                                        vectorField: { fieldPath: 'embedding' },
                                        queryVector: createVectorMap([1, 0]),
                                        distanceMeasure: 'EUCLIDEAN',
                                        limit: { value: 1001 },
                                    },
                                },
                            },
                        },
                        {
                            label: 'raw-0d',
                            expectedCode: 3,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    findNearest: {
                                        vectorField: { fieldPath: 'embedding' },
                                        queryVector: createVectorMap([]),
                                        distanceMeasure: 'EUCLIDEAN',
                                        limit: { value: 5 },
                                    },
                                },
                            },
                        },
                        {
                            label: 'raw-2049d',
                            expectedCode: 3,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    findNearest: {
                                        vectorField: { fieldPath: 'embedding' },
                                        queryVector: createVectorMap(new Array<number>(2049).fill(1)),
                                        distanceMeasure: 'EUCLIDEAN',
                                        limit: { value: 5 },
                                    },
                                },
                            },
                        },
                        // Live Cloud Standard Edition accepts raw NaN queryVector over RunQuery (matching 0
                        // results on empty/flat-index scans), whereas Enterprise returns 3 INVALID_ARGUMENT.
                        ...(fs.enterprise
                            ? [
                                  {
                                      label: 'raw-nan-2d',
                                      expectedCode: 3,
                                      req: {
                                          parent: sampleProto.parent,
                                          structuredQuery: {
                                              ...sampleProto.structuredQuery,
                                              findNearest: {
                                                  vectorField: { fieldPath: 'embedding' },
                                                  queryVector: createVectorMap([Number.NaN, 0]),
                                                  distanceMeasure: 'EUCLIDEAN',
                                                  limit: { value: 5 },
                                              },
                                          },
                                      },
                                  },
                              ]
                            : []),
                        {
                            label: 'raw-where-0d',
                            expectedCode: 3,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    where: {
                                        fieldFilter: {
                                            field: { fieldPath: 'embedding' },
                                            op: 'EQUAL',
                                            value: createVectorMap([]),
                                        },
                                    },
                                },
                            },
                        },
                        {
                            label: 'raw-cursor-0d',
                            expectedCode: 3,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    orderBy: [{ field: { fieldPath: 'embedding' }, direction: 'ASCENDING' }],
                                    startAt: { values: [createVectorMap([])], before: true },
                                },
                            },
                        },
                        ...(fs.enterprise
                            ? [
                                  {
                                      label: 'raw-where-nan',
                                      expectedCode: 3,
                                      req: {
                                          parent: sampleProto.parent,
                                          structuredQuery: {
                                              ...sampleProto.structuredQuery,
                                              where: {
                                                  fieldFilter: {
                                                      field: { fieldPath: 'embedding' },
                                                      op: 'EQUAL',
                                                      value: createVectorMap([Number.NaN, 0]),
                                                  },
                                              },
                                          },
                                      },
                                  },
                              ]
                            : []),
                        {
                            label: 'raw-cosine-zero',
                            expectedCode: fs.enterprise ? 3 : 9,
                            req: {
                                parent: sampleProto.parent,
                                structuredQuery: {
                                    ...sampleProto.structuredQuery,
                                    findNearest: {
                                        vectorField: { fieldPath: 'embedding' },
                                        queryVector: createVectorMap([0, 0]),
                                        distanceMeasure: 'COSINE',
                                        limit: { value: 5 },
                                    },
                                },
                            },
                        },
                    ];
                    const outcomes: Array<{ label: string; code: number | 'RESOLVED'; message?: string }> = [];
                    for (const item of invalidRunQueries) {
                        try {
                            await callRawStreamRpc(fs.firestore, 'runQuery', item.req);
                            outcomes.push({ label: item.label, code: 'RESOLVED' });
                        } catch (err: unknown) {
                            const code = typeof err === 'object' && err !== null ? Number((err as { code?: unknown }).code) : -1;
                            const message = err instanceof Error ? err.message : String(err);
                            outcomes.push({
                                label: item.label,
                                code,
                                ...(code === item.expectedCode ? {} : { message }),
                            });
                        }
                    }
                    expect(outcomes).toEqual(invalidRunQueries.map(item => ({ label: item.label, code: item.expectedCode })));
                },
                30_000,
            );

            test.concurrent(
                'enforces Cloud limitation canaries on findNearest combined with outer limit, offset, and cursors',
                async () => {
                    const col = makeCol('cloud-canaries');
                    await col.doc('d1').set(writeData({ embedding: FieldValue.vector([1, 0]) }));

                    const baseProto = toRunQueryProto(
                        col.findNearest({
                            vectorField: 'embedding',
                            queryVector: [1, 0],
                            limit: 5,
                            distanceMeasure: 'EUCLIDEAN',
                        }),
                    );

                    await expectCloudLimitationCanary('StructuredQuery.limit combined with findNearest', () =>
                        callRawStreamRpc(fs.firestore, 'runQuery', {
                            parent: baseProto.parent,
                            structuredQuery: { ...baseProto.structuredQuery, limit: { value: 1 } },
                        }),
                    );
                    await expectCloudLimitationCanary('non-zero StructuredQuery.offset combined with findNearest', () =>
                        callRawStreamRpc(fs.firestore, 'runQuery', {
                            parent: baseProto.parent,
                            structuredQuery: { ...baseProto.structuredQuery, offset: 1 },
                        }),
                    );
                    await expectCloudLimitationCanary('StructuredQuery.startAt combined with findNearest', () =>
                        callRawStreamRpc(fs.firestore, 'runQuery', {
                            parent: baseProto.parent,
                            structuredQuery: {
                                ...baseProto.structuredQuery,
                                startAt: { values: [{ integerValue: '1' }], before: true },
                            },
                        }),
                    );
                    await expectCloudLimitationCanary('StructuredQuery.endAt combined with findNearest', () =>
                        callRawStreamRpc(fs.firestore, 'runQuery', {
                            parent: baseProto.parent,
                            structuredQuery: {
                                ...baseProto.structuredQuery,
                                endAt: { values: [{ integerValue: '1' }], before: false },
                            },
                        }),
                    );
                },
            );
        });
});
