import type { Firestore } from '@google-cloud/firestore';
import { EventEmitter } from 'events';
import { callRawStreamRpc, normalizeStreamError } from './common';
import { ConcurrentTest } from './concurrent';
import { connection, notImplementedInJava, notImplementedInRust } from './env';

class MockStream extends EventEmitter {
    errored?: unknown;
    readableEnded = false;
    readonly resume = jest.fn(() => this);
}

function createMockFirestore(stream: MockStream) {
    const initializeIfNeeded = jest.fn(async (_tag: string) => undefined);
    const requestStream = jest.fn(async () => stream as unknown as NodeJS.ReadableStream & EventEmitter);
    const firestore = {
        initializeIfNeeded,
        requestStream,
    } as unknown as Firestore;
    return { firestore, initializeIfNeeded, requestStream };
}

describe('test-suite utils (offline)', () => {
    describe('env', () => {
        test('resolves connection and skip guards without initializing Firestore clients', () => {
            expect(['CLOUD FIRESTORE', 'RUST EMULATOR', 'JAVA EMULATOR']).toContain(connection);
            expect(notImplementedInRust).toEqual(connection === 'RUST EMULATOR' ? [] : undefined);
            expect(notImplementedInJava).toEqual(connection === 'JAVA EMULATOR' ? [] : undefined);
        });
    });

    describe('normalizeStreamError', () => {
        test('returns Error instances unchanged', () => {
            const original = new Error('boom');
            expect(normalizeStreamError(original)).toBe(original);
        });

        test('extracts non-empty message and preserves extra RPC properties', () => {
            const raw = { message: 'invalid request', details: 'ignored details', code: 3, metadata: { trace: 'abc' } };
            const normalized = normalizeStreamError(raw) as Error & {
                code?: unknown;
                details?: unknown;
                metadata?: unknown;
            };
            expect(normalized).toBeInstanceOf(Error);
            expect(normalized.message).toBe('invalid request');
            expect(normalized.cause).toBe(raw);
            expect(normalized.code).toBe(3);
            expect(normalized.details).toBe('ignored details');
            expect(normalized.metadata).toEqual({ trace: 'abc' });
        });

        test('falls through empty message to details without overwriting Error.message', () => {
            const raw = { message: '', details: 'details fallback', code: 13 };
            const normalized = normalizeStreamError(raw) as Error & { code?: unknown; details?: unknown };
            expect(normalized.message).toBe('details fallback');
            expect(normalized.details).toBe('details fallback');
            expect(normalized.code).toBe(13);
            expect(normalized.cause).toBe(raw);
        });

        test('handles non-string message via JSON.stringify without overwriting Error.message', () => {
            const raw = { message: 404, code: 5 };
            const normalized = normalizeStreamError(raw) as Error & { code?: unknown };
            expect(normalized.message).toBe(JSON.stringify(raw));
            expect(typeof normalized.message).toBe('string');
            expect(normalized.code).toBe(5);
            expect(normalized.cause).toBe(raw);
        });

        test('never overwrites core Error properties (message, name, stack, cause)', () => {
            const raw = {
                message: 'real message',
                name: 'SpoofedError',
                stack: 'spoofed-stack',
                cause: 'spoofed-cause',
                code: 7,
                details: 'permission denied',
                metadata: { region: 'us' },
            };
            const normalized = normalizeStreamError(raw) as Error & {
                code?: unknown;
                details?: unknown;
                metadata?: unknown;
            };
            expect(normalized.message).toBe('real message');
            expect(normalized.name).toBe('Error');
            expect(normalized.stack).toContain('normalizeStreamError');
            expect(normalized.stack).not.toBe('spoofed-stack');
            expect(normalized.cause).toBe(raw);
            expect(normalized.code).toBe(7);
            expect(normalized.details).toBe('permission denied');
            expect(normalized.metadata).toEqual({ region: 'us' });
        });

        test('serializes plain objects without message or details via JSON.stringify', () => {
            const raw = { code: 14, transient: true };
            const normalized = normalizeStreamError(raw) as Error & { code?: unknown; transient?: unknown };
            expect(normalized.message).toBe('{"code":14,"transient":true}');
            expect(normalized.code).toBe(14);
            expect(normalized.transient).toBe(true);
            expect(normalized.cause).toBe(raw);
        });

        test('handles circular objects and non-JSON-serializable objects without throwing', () => {
            const circular: Record<string, unknown> = { code: 13 };
            circular.self = circular;
            const normalized = normalizeStreamError(circular) as Error & { code?: unknown };
            expect(normalized).toBeInstanceOf(Error);
            expect(normalized.message.length).toBeGreaterThan(0);
            expect(normalized.message).not.toBe('[object Object]');
            expect(normalized.code).toBe(13);
            expect(normalized.cause).toBe(circular);

            const undefJson = { toJSON: () => undefined, code: 2 };
            const normalizedUndef = normalizeStreamError(undefJson) as Error & { code?: unknown };
            expect(normalizedUndef.message).toBe('Unknown stream error');
            expect(normalizedUndef.code).toBe(2);
            expect(normalizedUndef.cause).toBe(undefJson);
        });

        test('normalizes primitive values with cause', () => {
            for (const prim of ['raw string', 42, true, null, undefined]) {
                const normalized = normalizeStreamError(prim);
                expect(normalized).toBeInstanceOf(Error);
                expect(normalized.message).toBe(String(prim));
                expect(normalized.cause).toBe(prim);
            }
        });
    });

    describe('callRawStreamRpc', () => {
        test('collects chunks on data + end and invokes onChunk', async () => {
            const stream = new MockStream();
            const { firestore, initializeIfNeeded, requestStream } = createMockFirestore(stream);
            const seen: number[] = [];

            const promise = callRawStreamRpc<{ req: boolean }, { seq: number }>(
                firestore,
                'runQuery',
                { req: true },
                'customTag',
                chunk => {
                    seen.push(chunk.seq);
                },
            );

            await Promise.resolve();
            await Promise.resolve();
            expect(initializeIfNeeded).toHaveBeenCalledWith('customTag');
            expect(requestStream).toHaveBeenCalledWith('runQuery', false, { req: true }, 'customTag');
            expect(stream.resume).toHaveBeenCalledTimes(1);

            stream.emit('data', { seq: 1 });
            stream.emit('data', { seq: 2 });
            stream.emit('end');

            await expect(promise).resolves.toEqual([{ seq: 1 }, { seq: 2 }]);
            expect(seen).toEqual([1, 2]);
        });

        test('rejects with normalized error when stream emits error', async () => {
            const stream = new MockStream();
            const { firestore } = createMockFirestore(stream);

            const promise = callRawStreamRpc(firestore, 'runQuery', {});
            await Promise.resolve();
            await Promise.resolve();

            const rawErr = { message: '', details: 'rpc exploded', code: 13 };
            stream.emit('error', rawErr);

            await expect(promise).rejects.toMatchObject({
                message: 'rpc exploded',
                code: 13,
                cause: rawErr,
            });
        });

        test('rejects and settles on first error thrown by onChunk', async () => {
            const stream = new MockStream();
            const { firestore } = createMockFirestore(stream);

            const promise = callRawStreamRpc<Record<string, never>, { seq: number }>(firestore, 'runQuery', {}, 'tag', chunk => {
                if (chunk.seq === 2) {
                    throw new Error('chunk callback failed');
                }
            });

            await Promise.resolve();
            await Promise.resolve();

            stream.emit('data', { seq: 1 });
            stream.emit('data', { seq: 2 });
            stream.emit('data', { seq: 3 });
            stream.emit('end');

            await expect(promise).rejects.toThrow('chunk callback failed');
        });

        test('handles close event: rejects with stream.errored when set', async () => {
            const stream = new MockStream();
            const { firestore } = createMockFirestore(stream);

            const promise = callRawStreamRpc(firestore, 'runQuery', {});
            await Promise.resolve();
            await Promise.resolve();

            stream.errored = { message: '', details: 'transport died', code: 14 };
            stream.emit('close');

            await expect(promise).rejects.toMatchObject({
                message: 'transport died',
                code: 14,
            });
        });

        test('handles close event: resolves when readableEnded is true without end event', async () => {
            const stream = new MockStream();
            const { firestore } = createMockFirestore(stream);

            const promise = callRawStreamRpc<Record<string, never>, { seq: number }>(firestore, 'runQuery', {});
            await Promise.resolve();
            await Promise.resolve();

            stream.emit('data', { seq: 10 });
            stream.readableEnded = true;
            stream.emit('close');

            await expect(promise).resolves.toEqual([{ seq: 10 }]);
        });

        test('handles close event: rejects on premature close without errored or readableEnded', async () => {
            const stream = new MockStream();
            const { firestore } = createMockFirestore(stream);

            const promise = callRawStreamRpc(firestore, 'runQuery', {});
            await Promise.resolve();
            await Promise.resolve();

            stream.emit('close');

            await expect(promise).rejects.toThrow('Stream closed prematurely during runQuery');
        });
    });

    describe('ConcurrentTest', () => {
        test('buffers consecutive event() calls before when() without losing repeated names', async () => {
            const ct = new ConcurrentTest({ timeout: 500 });
            const order: string[] = [];

            await ct.run(
                async () => {
                    ct.event('alpha');
                    ct.event('beta');
                    ct.event('alpha');
                    order.push('emitted-all');
                },
                async () => {
                    await Promise.resolve();
                    await ct.when('alpha');
                    order.push('got-alpha-1');
                    await ct.when('beta');
                    order.push('got-beta');
                    await ct.when('alpha');
                    order.push('got-alpha-2');
                },
            );

            expect(order).toEqual(['emitted-all', 'got-alpha-1', 'got-beta', 'got-alpha-2']);
        });

        test('preserves monotonic cursor across concurrent when() calls under same processName', async () => {
            const ct = new ConcurrentTest({ timeout: 500 });
            let triggerSecondWait!: () => void;
            const secondWaitTrigger = new Promise<void>(resolve => {
                triggerSecondWait = resolve;
            });
            let signalPhase2Ready!: () => void;
            const phase2Ready = new Promise<void>(resolve => {
                signalPhase2Ready = resolve;
            });
            let secondResolvedPrematurely = false;

            await ct.run(
                async () => {
                    // w1 starts at cursor 0, awaiting 'first' asynchronously.
                    const w1 = ct.when('first');
                    // w2 runs in the same processName microtask immediately after triggerSecondWait(),
                    // matching 'second' at index 1 on the fast path BEFORE w1's continuation runs.
                    const w2 = secondWaitTrigger.then(() => ct.when('second'));
                    await Promise.all([w1, w2]);

                    // Cursor must now be at 2 (never rewound to 1 when w1 resolved for index 0),
                    // so awaiting 'second' again must wait for a fresh emission rather than re-matching index 1.
                    const w3 = ct.when('second').then(() => {
                        secondResolvedPrematurely = true;
                    });
                    signalPhase2Ready();
                    await w3;
                },
                async () => {
                    triggerSecondWait();
                    ct.event('first');
                    ct.event('second');

                    await phase2Ready;
                    await Promise.resolve();
                    expect(secondResolvedPrematurely).toBe(false);
                    ct.event('second');
                },
            );

            expect(secondResolvedPrematurely).toBe(true);
        });

        test('includes full log and caller stack frame on timeout', async () => {
            const ct = new ConcurrentTest({ timeout: 30 });

            async function callSiteWaitingHelper(harness: ConcurrentTest) {
                await harness.when('never-arrives');
            }

            let caught: unknown;
            try {
                await ct.run(async () => {
                    await callSiteWaitingHelper(ct);
                });
            } catch (err: unknown) {
                caught = err;
            }

            expect(caught).toBeInstanceOf(Error);
            const timeoutError = caught as Error;
            expect(timeoutError.message).toContain('timeout after 0.03s');
            expect(timeoutError.message).toContain('WAITING UNTIL: never-arrives');
            expect(timeoutError.stack).toContain('callSiteWaitingHelper');
        });
    });
});
