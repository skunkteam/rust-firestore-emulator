import { CollectionReference, DocumentReference, Timestamp } from '@google-cloud/firestore';
import { setTimeout as sleep } from 'timers/promises';
import { callRawStreamRpc, callRawUnaryRpc, connection, editions, expectRpcCode, readData, readDataRef, writeData } from './utils';

const PROJECT_ID = 'firestore-test-406503';
const ONE_HOUR_MS = 60 * 60 * 1_000;
const EXPIRED_TXN_DETAILS = 'The referenced transaction has expired or is no longer valid.';

const rawIdleTimeoutMs = process.env.FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS;
const rawMaxLifetimeMs = process.env.FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS;
const isFastRustEmulator = connection === 'RUST EMULATOR' && rawIdleTimeoutMs != null && rawIdleTimeoutMs !== '';
const idleTimeoutMs = isFastRustEmulator ? Number(rawIdleTimeoutMs) : 60_000;
const maxLifetimeMs = isFastRustEmulator && rawMaxLifetimeMs != null && rawMaxLifetimeMs !== '' ? Number(rawMaxLifetimeMs) : null;
const unusedWaitMs = idleTimeoutMs + 2_500;
const usedWaitMs = idleTimeoutMs * 2 + 2_500;
const refreshIntervalMs = Math.round(idleTimeoutMs * 0.6);
const parkLeadMs = isFastRustEmulator ? 800 : 3_500;
const parkVerifyPendingWaitMs = isFastRustEmulator ? 200 : 1_000;
const testTimeoutMs = Math.max(120_000, Math.round((maxLifetimeMs ?? idleTimeoutMs * 2.4) + 75_000));

type Edition = (typeof editions)[number];

interface RawBeginTransactionRequest {
    database: string;
    options: {
        readWrite?: {
            retryTransaction?: Uint8Array;
        };
        readOnly?: Record<string, never>;
    };
}

interface RawBeginTransactionResponse {
    transaction?: Uint8Array;
}

interface RawGetDocumentRequest {
    name: string;
    transaction: Uint8Array;
}

interface RawDocumentResponse {
    name?: string;
    fields?: Record<string, unknown>;
}

interface RawBatchGetDocumentsRequest {
    database: string;
    documents: string[];
    transaction: Uint8Array;
}

interface RawBatchGetDocumentsChunk {
    found?: RawDocumentResponse;
    missing?: string;
}

interface RawTransactionRunQueryRequest {
    parent: string;
    structuredQuery: {
        from: Array<{ collectionId: string }>;
    };
    transaction: Uint8Array;
}

interface RawRunQueryChunk {
    document?: RawDocumentResponse;
}

interface RawTimestampValue {
    seconds: number;
    nanos: number;
}

interface RawCommitValue {
    integerValue?: string;
    timestampValue?: RawTimestampValue;
}

interface RawCommitRequest {
    database: string;
    transaction: Uint8Array;
    writes: Array<{
        update: {
            name: string;
            fields: Record<string, RawCommitValue>;
        };
    }>;
}

interface RawRollbackRequest {
    database: string;
    transaction: Uint8Array;
}

function toProtoTimestamp(ms: number): RawTimestampValue {
    const ts = Timestamp.fromMillis(ms);
    return { seconds: ts.seconds, nanos: ts.nanoseconds };
}

async function captureRpcError(fn: () => Promise<unknown>, label: string): Promise<unknown> {
    try {
        await fn();
    } catch (err: unknown) {
        return err;
    }
    throw new Error(`[${label}] Expected RPC to fail, but it succeeded`);
}

async function expectExpiredTransaction(fn: () => Promise<unknown>, label: string): Promise<void> {
    const err = await captureRpcError(fn, label);
    const code = (err as { code?: unknown }).code;
    const message = err instanceof Error ? err.message : String(err);
    if (code !== 10 || !message.includes(EXPIRED_TXN_DETAILS)) {
        throw new Error(`[${label}] Expected code=10 (${EXPIRED_TXN_DETAILS}), got code=${String(code)} message=${message}`);
    }
}

async function runTransactionTtlSuiteForEdition(fs: Edition): Promise<void> {
    const dbRoot = `projects/${PROJECT_ID}/databases/${fs.firestore.databaseId}`;
    const docsRoot = `${dbRoot}/documents`;
    const docPath = (ref: DocumentReference): string => `${docsRoot}/${ref.path}`;

    const beginRw = async (retryTransaction?: Uint8Array): Promise<Uint8Array> => {
        const res = await callRawUnaryRpc<RawBeginTransactionRequest, RawBeginTransactionResponse>(
            fs.firestore,
            'beginTransaction',
            {
                database: dbRoot,
                options: { readWrite: retryTransaction ? { retryTransaction } : {} },
            },
            retryTransaction ? 'begin-rw-retry' : 'begin-rw',
        );
        expect(res.transaction).toBeDefined();
        expect(res.transaction!.length).toBeGreaterThan(0);
        return res.transaction!;
    };

    const beginRo = async (): Promise<Uint8Array> => {
        const res = await callRawUnaryRpc<RawBeginTransactionRequest, RawBeginTransactionResponse>(
            fs.firestore,
            'beginTransaction',
            {
                database: dbRoot,
                options: { readOnly: {} },
            },
            'begin-ro',
        );
        expect(res.transaction).toBeDefined();
        expect(res.transaction!.length).toBeGreaterThan(0);
        return res.transaction!;
    };

    const rawGet = async (ref: DocumentReference, transaction: Uint8Array, label: string): Promise<RawDocumentResponse> =>
        callRawUnaryRpc<RawGetDocumentRequest, RawDocumentResponse>(
            fs.firestore,
            'getDocument',
            { name: docPath(ref), transaction },
            label,
        );

    const rawBatchGet = async (refs: DocumentReference[], transaction: Uint8Array, label: string): Promise<RawBatchGetDocumentsChunk[]> =>
        callRawStreamRpc<RawBatchGetDocumentsRequest, RawBatchGetDocumentsChunk>(
            fs.firestore,
            'batchGetDocuments',
            { database: dbRoot, documents: refs.map(docPath), transaction },
            label,
        );

    const rawQueryInTxn = async (holderDoc: DocumentReference, col: CollectionReference, transaction: Uint8Array, label: string) =>
        callRawStreamRpc<RawTransactionRunQueryRequest, RawRunQueryChunk>(
            fs.firestore,
            'runQuery',
            {
                parent: docPath(holderDoc),
                structuredQuery: { from: [{ collectionId: col.id }] },
                transaction,
            },
            label,
        );

    const rawCommitEmpty = async (transaction: Uint8Array, label: string): Promise<void> => {
        await callRawUnaryRpc<RawCommitRequest>(fs.firestore, 'commit', { database: dbRoot, transaction, writes: [] }, label);
    };

    const rawCommitValue = async (ref: DocumentReference, transaction: Uint8Array, value: number, label: string): Promise<void> => {
        await callRawUnaryRpc<RawCommitRequest>(
            fs.firestore,
            'commit',
            {
                database: dbRoot,
                transaction,
                writes: [
                    {
                        update: {
                            name: docPath(ref),
                            fields: {
                                v: { integerValue: String(value) },
                                ttl: { timestampValue: toProtoTimestamp(Date.now() + ONE_HOUR_MS) },
                            },
                        },
                    },
                ],
            },
            label,
        );
    };

    const rawRollback = async (transaction: Uint8Array, label: string): Promise<void> => {
        await callRawUnaryRpc<RawRollbackRequest>(fs.firestore, 'rollback', { database: dbRoot, transaction }, label);
    };

    const expectExpiredRpcMatrix = async (
        holderDoc: DocumentReference,
        col: CollectionReference,
        ref: DocumentReference,
        transaction: Uint8Array,
        prefix: string,
    ): Promise<void> => {
        await expectExpiredTransaction(() => rawGet(ref, transaction, `${prefix}-get`), `${prefix}-get`);
        await expectExpiredTransaction(() => rawBatchGet([ref], transaction, `${prefix}-batch-get`), `${prefix}-batch-get`);
        await expectExpiredTransaction(() => rawQueryInTxn(holderDoc, col, transaction, `${prefix}-query`), `${prefix}-query`);
        await expectExpiredTransaction(() => rawCommitEmpty(transaction, `${prefix}-commit-empty`), `${prefix}-commit-empty`);
        await expectExpiredTransaction(() => rawCommitValue(ref, transaction, 99, `${prefix}-commit-write`), `${prefix}-commit-write`);
    };

    const runUnusedScenario = async (): Promise<void> => {
        const holderDoc = fs.collection.doc();
        const col = holderDoc.collection('ttl-unused');
        const doc = col.doc('d');
        await doc.set(writeData({ v: 1 }));

        const [rwExpired, rwRollback, roExpired, roRollback] = await Promise.all([beginRw(), beginRw(), beginRo(), beginRo()]);

        await sleep(unusedWaitMs);

        await expectExpiredRpcMatrix(holderDoc, col, doc, rwExpired, 'unused-rw');
        await expectExpiredRpcMatrix(holderDoc, col, doc, roExpired, 'unused-ro');

        // Idle-expired Rollback (<270s) is idempotent Ok(()), leaving subsequent reads expired.
        await rawRollback(rwRollback, 'unused-rw-rollback-1');
        await rawRollback(rwRollback, 'unused-rw-rollback-2');
        await expectExpiredTransaction(() => rawGet(doc, rwRollback, 'unused-rw-post-rb-get'), 'unused-rw-post-rb-get');

        await rawRollback(roRollback, 'unused-ro-rollback-1');
        await rawRollback(roRollback, 'unused-ro-rollback-2');
        await expectExpiredTransaction(() => rawGet(doc, roRollback, 'unused-ro-post-rb-get'), 'unused-ro-post-rb-get');

        // Retry fails on expired RO with INVALID_ARGUMENT, succeeds on expired RW with rotated token.
        await expectRpcCode(() => beginRw(roExpired), 3, 'unused-ro-retry');
        await expectRpcCode(() => beginRw(roRollback), 3, 'unused-ro-rb-retry');

        const retriedRw = await beginRw(rwExpired);
        expect(Buffer.from(retriedRw).equals(Buffer.from(rwExpired))).toBe(false);
        await rawGet(doc, retriedRw, 'unused-rw-retried-get');
        await rawCommitValue(doc, retriedRw, 2, 'unused-rw-retried-commit');

        const retriedAfterRb = await beginRw(rwRollback);
        expect(Buffer.from(retriedAfterRb).equals(Buffer.from(rwRollback))).toBe(false);
        await rawCommitEmpty(retriedAfterRb, 'unused-rw-rb-retried-empty-commit');

        expect(await readDataRef(doc)).toEqual({ v: 2 });
    };

    const runUsedLocksAndPriorityScenario = async (): Promise<void> => {
        const holderDoc = fs.collection.doc();
        const col = holderDoc.collection('ttl-used');
        const docStayAlive = col.doc('stay-alive');
        const docParkedNonTx = col.doc('parked-nontx');
        const docParkedYoung = col.doc('parked-young');
        const docArriving = col.doc('arriving');
        const docPrioritySeed = col.doc('priority-seed');
        const docPriorityContend = col.doc('priority-contend');

        await Promise.all([
            docStayAlive.set(writeData({ v: 1 })),
            docParkedNonTx.set(writeData({ v: 1 })),
            docParkedYoung.set(writeData({ v: 1 })),
            docArriving.set(writeData({ v: 1 })),
            docPrioritySeed.set(writeData({ v: 1 })),
            docPriorityContend.set(writeData({ v: 1 })),
        ]);

        const [rwStayAlive, rwUsedExpiry, rwPriorityOld] = await Promise.all([beginRw(), beginRw(), beginRw()]);
        const tCreated = Date.now();
        await Promise.all([
            rawGet(docStayAlive, rwStayAlive, 'stay-alive-init-get'),
            rawGet(docParkedNonTx, rwUsedExpiry, 'used-init-get-nontx'),
            rawGet(docParkedYoung, rwUsedExpiry, 'used-init-get-young'),
            rawGet(docArriving, rwUsedExpiry, 'used-init-get-arriving'),
            rawGet(docPrioritySeed, rwPriorityOld, 'priority-old-init-get'),
        ]);

        // Wait past unused threshold: used RW stays alive past idleTimeoutMs.
        await sleep(Math.max(0, tCreated + unusedWaitMs - Date.now()));
        await rawGet(docStayAlive, rwStayAlive, 'stay-alive-mid-get');
        await rawCommitValue(docStayAlive, rwStayAlive, 2, 'stay-alive-mid-commit');
        expect(await readDataRef(docStayAlive)).toEqual({ v: 2 });

        // Start a younger mid-aged transaction while rwPriorityOld is still alive/about to expire.
        const rwPriorityMid = await beginRw();
        await rawGet(docPriorityContend, rwPriorityMid, 'priority-mid-read');

        // For pessimistic editions, park both a non-tx write and a younger RW commit shortly
        // before `rwUsedExpiry` expires at 2*idle so they block on its read locks first and
        // unblock upon TTL expiration rather than hitting lock write timeout.
        let parkedNonTxDone = false;
        let parkedYoungDone = false;
        let parkedNonTxErr: unknown = null;
        let parkedYoungErr: unknown = null;
        let parkedNonTxPromise: Promise<void> = Promise.resolve();
        let parkedYoungPromise: Promise<void> = Promise.resolve();

        if (fs.concurrencyMode === 'pessimistic') {
            const parkAt = tCreated + idleTimeoutMs * 2 - parkLeadMs;
            await sleep(Math.max(0, parkAt - Date.now()));

            const youngTxn = await beginRw();
            parkedNonTxPromise = docParkedNonTx.set(writeData({ v: 2 })).then(
                () => {
                    parkedNonTxDone = true;
                },
                (err: unknown) => {
                    parkedNonTxErr = err;
                },
            );
            parkedYoungPromise = rawCommitValue(docParkedYoung, youngTxn, 2, 'parked-young-commit').then(
                () => {
                    parkedYoungDone = true;
                },
                (err: unknown) => {
                    parkedYoungErr = err;
                },
            );

            await sleep(parkVerifyPendingWaitMs);
            expect(parkedNonTxErr).toBeNull();
            expect(parkedYoungErr).toBeNull();
            expect(parkedNonTxDone).toBe(false);
            expect(parkedYoungDone).toBe(false);
        }

        // Wait past 2*idle used threshold.
        await sleep(Math.max(0, tCreated + usedWaitMs - Date.now()));

        await Promise.all([parkedNonTxPromise, parkedYoungPromise]);
        if (fs.concurrencyMode === 'pessimistic') {
            expect(parkedNonTxErr).toBeNull();
            expect(parkedYoungErr).toBeNull();
            expect(parkedNonTxDone).toBe(true);
            expect(parkedYoungDone).toBe(true);
            expect(await readDataRef(docParkedNonTx)).toEqual({ v: 2 });
            expect(await readDataRef(docParkedYoung)).toEqual({ v: 2 });
        }

        await expectExpiredRpcMatrix(holderDoc, col, docArriving, rwUsedExpiry, 'used-rw');
        await rawRollback(rwUsedExpiry, 'used-rw-rollback');

        // Arriving write after expiry succeeds cleanly and sees updated values.
        await docArriving.set(writeData({ v: 3 }));
        expect(await readDataRef(docArriving)).toEqual({ v: 3 });

        // Retried expired RW inherits original t=0 lineage priority, beating younger rwPriorityMid.
        const rwPriorityRetried = await beginRw(rwPriorityOld);
        await rawGet(docPriorityContend, rwPriorityRetried, 'priority-retried-read');
        await rawCommitValue(docPriorityContend, rwPriorityRetried, 2, 'priority-retried-commit');
        await expectRpcCode(
            () => rawCommitValue(docPriorityContend, rwPriorityMid, 99, 'priority-mid-aborted-commit'),
            10,
            'priority-mid-aborted-commit',
        );
        expect(await readDataRef(docPriorityContend)).toEqual({ v: 2 });
    };

    const runActivityRefreshScenario = async (): Promise<void> => {
        const doc = fs.collection.doc().collection('ttl-refresh').doc('d');
        await doc.set(writeData({ v: 1 }));

        const rwRefresh = await beginRw();
        await rawGet(doc, rwRefresh, 'refresh-init-get');

        for (let step = 0; step < 4; step += 1) {
            await sleep(refreshIntervalMs);
            await rawGet(doc, rwRefresh, `refresh-ping-${step}`);
        }

        // 4 * 0.6 * idle = 2.4 * idle > 2 * idle used threshold, below max lifetime (30s fast / 270s Cloud).
        await rawCommitValue(doc, rwRefresh, 2, 'refresh-final-commit');
        expect(await readDataRef(doc)).toEqual({ v: 2 });
    };

    const runSdkAutoRetryScenario = async (): Promise<void> => {
        const doc = fs.collection.doc().collection('ttl-sdk-retry').doc('d');
        await doc.set(writeData({ v: 1 }));

        let attempts = 0;
        await fs.firestore.runTransaction(async tx => {
            attempts += 1;
            const snap = await tx.get(doc);
            const data = readData<{ v: number }>(snap.data());
            if (attempts === 1) {
                await sleep(usedWaitMs);
            }
            tx.set(doc, writeData({ v: data.v + 10 }));
        });

        expect(attempts).toBe(2);
        expect(await readDataRef(doc)).toEqual({ v: 11 });
    };

    const runMaxLifetimeScenario = async (maxMs: number): Promise<void> => {
        const holderDoc = fs.collection.doc();
        const col = holderDoc.collection('ttl-max-lifetime');
        const doc = col.doc('d');
        await doc.set(writeData({ v: 1 }));

        const rwMax = await beginRw();
        const tMaxCreated = Date.now();
        await rawGet(doc, rwMax, 'max-init-get');

        for (let step = 0; tMaxCreated + (step + 1) * refreshIntervalMs <= tMaxCreated + maxMs - 2_000; step += 1) {
            const nextPingAt = tMaxCreated + (step + 1) * refreshIntervalMs;
            await sleep(Math.max(0, nextPingAt - Date.now()));
            await rawGet(doc, rwMax, `max-ping-${step}`);
        }

        await sleep(Math.max(0, tMaxCreated + maxMs + 2_000 - Date.now()));

        await expectExpiredRpcMatrix(holderDoc, col, doc, rwMax, 'max-rw');
        await expectExpiredTransaction(() => rawRollback(rwMax, 'max-rw-rollback'), 'max-rw-rollback');

        const retriedAfterMax = await beginRw(rwMax);
        expect(Buffer.from(retriedAfterMax).equals(Buffer.from(rwMax))).toBe(false);
        await rawGet(doc, retriedAfterMax, 'max-rw-retried-get');
        await rawCommitValue(doc, retriedAfterMax, 2, 'max-rw-retried-commit');
        expect(await readDataRef(doc)).toEqual({ v: 2 });
    };

    const scenarios = [runUnusedScenario(), runUsedLocksAndPriorityScenario(), runActivityRefreshScenario(), runSdkAutoRetryScenario()];
    if (maxLifetimeMs != null && Number.isFinite(maxLifetimeMs) && maxLifetimeMs > 0) {
        scenarios.push(runMaxLifetimeScenario(maxLifetimeMs));
    }

    const results = await Promise.allSettled(scenarios);
    const firstRejected = results.find((r): r is PromiseRejectedResult => r.status === 'rejected');
    if (firstRejected) {
        throw firstRejected.reason;
    }
}

describe('Transaction TTL expiration and GC', () => {
    test.concurrent.each(editions)(
        '$description: expires idle RW/RO transactions, releases pessimistic locks, preserves retry lineage priority, and resets idle TTL on activity',
        async fs => {
            await runTransactionTtlSuiteForEdition(fs);
        },
        testTimeoutMs,
    );
});
