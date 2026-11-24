import type { Firestore, Query, VectorQuery } from '@google-cloud/firestore';
import type { EventEmitter } from 'events';
import { connection } from './env';

export interface RawProtoValue {
    nullValue?: 'NULL_VALUE' | null;
    booleanValue?: boolean;
    integerValue?: string;
    doubleValue?: number;
    stringValue?: string;
    referenceValue?: string;
    timestampValue?: { seconds?: number | string; nanos?: number };
    bytesValue?: Uint8Array | string;
    geoPointValue?: { latitude?: number; longitude?: number };
    arrayValue?: { values?: RawProtoValue[] };
    mapValue?: { fields?: Record<string, RawProtoValue> };
}

export interface RawDocument {
    name?: string;
    fields?: Record<string, RawProtoValue>;
}

export interface RawRunQueryRequest {
    parent: string;
    structuredQuery?: Record<string, unknown>;
    transaction?: Uint8Array;
    newTransaction?: { readWrite?: Record<string, never> };
}

export interface RawRunQueryResponseChunk {
    document?: RawDocument;
    transaction?: Uint8Array;
}

interface InternalQueryProtoCarrier {
    toProto(transaction?: Uint8Array): RawRunQueryRequest;
}

export function toRunQueryProto(query: Query | VectorQuery, transaction?: Uint8Array): RawRunQueryRequest {
    return (query as unknown as InternalQueryProtoCarrier).toProto(transaction);
}

export function isDefined<T>(inp: T | undefined | null): inp is T {
    return inp != null;
}

export function expectToBeOrdered(inp: string[]) {
    expect(inp).toEqual(inp.slice().sort());
}

export async function expectCloudLimitationCanary(description: string, fn: () => unknown, expectedCode = 3): Promise<void> {
    try {
        await Promise.resolve().then(fn);
    } catch (err: unknown) {
        const rpcErr = typeof err === 'object' && err !== null ? (err as { code?: unknown }) : undefined;
        if (rpcErr?.code !== expectedCode) {
            const detail = err instanceof Error ? err.message : String(err);
            throw new Error(`Expected gRPC code ${expectedCode} for ${description}, got ${String(rpcErr?.code)}: ${detail}`, {
                cause: err,
            });
        }
        return;
    }
    if (connection === 'CLOUD FIRESTORE') {
        throw new Error(
            `CANARY UNLOCKED: Cloud Firestore now permits ${description}; consider implementing parity in rust-firestore-emulator`,
        );
    }
    throw new Error(`Expected cloud limitation error (code ${expectedCode}) for ${description}, but call succeeded on ${connection}`);
}

interface InternalFirestoreRpcClient {
    initializeIfNeeded(requestTag: string): Promise<void>;
    request<TReq extends object, TRes>(methodName: string, request: TReq, requestTag: string): Promise<TRes>;
    requestStream<TReq extends object>(
        methodName: string,
        bidirectional: boolean,
        request: TReq,
        requestTag: string,
    ): Promise<NodeJS.ReadableStream & EventEmitter>;
}

export async function callRawUnaryRpc<TReq extends object, TRes = unknown>(
    firestore: Firestore,
    rpc: string,
    req: TReq,
    tag = rpc,
): Promise<TRes> {
    const client = firestore as unknown as InternalFirestoreRpcClient;
    await client.initializeIfNeeded(tag);
    return client.request<TReq, TRes>(rpc, req, tag);
}

const CORE_ERROR_KEYS = new Set(['message', 'name', 'stack', 'cause']);

export function normalizeStreamError(err: unknown): Error {
    if (err instanceof Error) {
        return err;
    }
    if (typeof err === 'object' && err !== null) {
        const obj = err as Record<string, unknown>;
        let msg: string;
        if (typeof obj.message === 'string' && obj.message.length > 0) {
            msg = obj.message;
        } else if (typeof obj.details === 'string' && obj.details.length > 0) {
            msg = obj.details;
        } else {
            try {
                msg = JSON.stringify(err) ?? 'Unknown stream error';
            } catch {
                msg = 'Unserializable stream error';
            }
        }
        const normalized = new Error(msg, { cause: err });
        if (!Array.isArray(err)) {
            for (const [key, value] of Object.entries(obj)) {
                if (!CORE_ERROR_KEYS.has(key) && key !== '__proto__') {
                    Object.defineProperty(normalized, key, {
                        value,
                        enumerable: true,
                        writable: true,
                        configurable: true,
                    });
                }
            }
        }
        return normalized;
    }
    return new Error(String(err), { cause: err });
}

export async function callRawStreamRpc<TReq extends object, TChunk = unknown>(
    firestore: Firestore,
    rpc: string,
    req: TReq,
    tag = rpc,
    onChunk?: (chunk: TChunk) => void,
): Promise<TChunk[]> {
    const client = firestore as unknown as InternalFirestoreRpcClient;
    await client.initializeIfNeeded(tag);
    const stream = await client.requestStream<TReq>(rpc, false, req, tag);
    return new Promise<TChunk[]>((resolve, reject) => {
        const chunks: TChunk[] = [];
        let settled = false;
        const finish = (err?: unknown) => {
            if (settled) return;
            settled = true;
            if (err !== undefined) {
                reject(normalizeStreamError(err));
            } else {
                resolve(chunks);
            }
        };
        stream.on('data', (chunk: unknown) => {
            if (settled) return;
            try {
                const typed = chunk as TChunk;
                onChunk?.(typed);
                chunks.push(typed);
            } catch (err: unknown) {
                finish(err ?? new Error(`Unknown error in onChunk during ${rpc}`));
            }
        });
        stream.on('end', () => finish());
        stream.on('error', err => finish(err ?? new Error(`Unknown stream error during ${rpc}`)));
        stream.on('close', () => {
            setImmediate(() => {
                if (settled) return;
                const state = stream as NodeJS.ReadableStream & { errored?: unknown; readableEnded?: boolean };
                if (state.errored != null) {
                    finish(state.errored);
                } else if (state.readableEnded) {
                    finish();
                } else {
                    finish(new Error(`Stream closed prematurely during ${rpc}`));
                }
            });
        });
        stream.resume();
    });
}

export async function rawRunQuery(firestore: Firestore, req: RawRunQueryRequest, tag = 'runQuery'): Promise<RawRunQueryResponseChunk[]> {
    let capturedTx: Uint8Array | undefined;
    try {
        return await callRawStreamRpc<RawRunQueryRequest, RawRunQueryResponseChunk>(firestore, 'runQuery', req, tag, chunk => {
            if (chunk.transaction && chunk.transaction.length > 0) {
                capturedTx = chunk.transaction;
            }
        });
    } finally {
        if (connection === 'CLOUD FIRESTORE' && req.newTransaction && capturedTx && capturedTx.length > 0) {
            const database = req.parent.split('/documents')[0];
            await callRawUnaryRpc(firestore, 'rollback', { database, transaction: capturedTx }).catch(() => undefined);
        }
    }
}

export async function expectRpcCode(fn: () => Promise<unknown>, expectedCode: number, label = 'rpc'): Promise<void> {
    let resolvedValue: unknown;
    try {
        resolvedValue = await fn();
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
    throw new Error(`[${label}] Expected gRPC status code ${expectedCode}, but call resolved with ${JSON.stringify(resolvedValue)}`);
}
