import type { Firestore } from '@google-cloud/firestore';
import type { EventEmitter } from 'events';
import { connection } from './firestore';

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
            throw new Error(`Expected gRPC code ${expectedCode} for ${description}, got ${String(rpcErr?.code)}: ${detail}`);
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

export async function callRawStreamRpc<TReq extends object, TChunk = unknown>(
    firestore: Firestore,
    rpc: string,
    req: TReq,
    tag = rpc,
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
                reject(err instanceof Error ? err : new Error(String(err)));
            } else {
                resolve(chunks);
            }
        };
        stream.on('data', (chunk: unknown) => {
            chunks.push(chunk as TChunk);
        });
        stream.on('end', () => finish());
        stream.on('error', err => finish(err));
        stream.resume();
    });
}
