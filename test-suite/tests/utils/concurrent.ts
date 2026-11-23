import { MaybeFinalState, atom, error, final, unresolved } from '@skunkteam/sherlock';
import { fromPromise } from '@skunkteam/sherlock-utils';
import { AsyncLocalStorage } from 'async_hooks';
import { setTimeout as time } from 'timers/promises';

const ABORT_REASON_CLEANUP = 'cleanup';
const ABORT_REASON_SIBLING_FAILED = 'sibling-failed';

export class ConcurrentTest {
    readonly log: string[] = [];
    readonly lastEvent$ = atom.unresolved<string>();
    readonly processName = new AsyncLocalStorage<string>();
    private readonly events: string[] = [];
    private readonly eventVersion$ = atom(0);
    private readonly cursors = new Map<string | undefined, number>();
    private readonly activeWaits = new Set<AbortController>();
    private runFailed = false;

    private readonly timeout: number;
    constructor({ timeout = 8_000 } = {}) {
        this.timeout = timeout;
    }

    event(what: string) {
        this.log.push(`${this.processName.getStore()} EVENT: ${what}`);
        this.events.push(what);
        this.lastEvent$.set(what);
        this.eventVersion$.set(this.events.length);
    }

    private findMatchingEventIndex(startIdx: number, what: string[]): number {
        for (let i = startIdx; i < this.events.length; i++) {
            if (what.includes(this.events[i])) {
                return i;
            }
        }
        return -1;
    }

    async when(...what: string[]) {
        const procName = this.processName.getStore();
        const timeoutErr = new Error('timeout');
        if (this.runFailed) {
            throw new Error(`${procName} aborted because a sibling process failed`);
        }

        this.log.push(`${procName} WAITING UNTIL: ${what.join(' OR ')}`);
        const cursor = this.cursors.get(procName) ?? 0;
        let matchIdx = this.findMatchingEventIndex(cursor, what);
        if (matchIdx !== -1) {
            this.cursors.set(procName, matchIdx + 1);
            return;
        }

        const ac = new AbortController();
        this.activeWaits.add(ac);
        const timeoutPromise = time(this.timeout, undefined, { signal: ac.signal }).catch((e: unknown) => e);

        const errorAfterTime$ = fromPromise(timeoutPromise).map((): MaybeFinalState<never> => {
            if (ac.signal.aborted) {
                if (ac.signal.reason === ABORT_REASON_SIBLING_FAILED) {
                    return final(error(new Error(`${procName} aborted because a sibling process failed`)));
                }
                return unresolved;
            }
            const currentLog = this.log.map(m => '\n- ' + m).join('');
            const timeoutSeconds = this.timeout / 1_000;
            timeoutErr.message = `${procName} timeout after ${timeoutSeconds}s, current log: ${currentLog}`;
            return final(error(timeoutErr));
        });
        try {
            await this.eventVersion$.toPromise({
                when: d$ =>
                    d$
                        .map(() => {
                            matchIdx = this.findMatchingEventIndex(cursor, what);
                            return matchIdx !== -1;
                        })
                        .or(errorAfterTime$),
            });
            this.cursors.set(procName, matchIdx + 1);
        } finally {
            this.activeWaits.delete(ac);
            if (!ac.signal.aborted) {
                ac.abort(ABORT_REASON_CLEANUP);
            }
        }
    }

    async run(...processes: Array<() => Promise<unknown>>) {
        this.runFailed = false;
        let firstError: unknown;
        const settled = await Promise.allSettled(
            processes.map(async (p, i) =>
                this.processName.run(`${'       |'.repeat(i)} <<${i + 1}>> |`, async () => {
                    try {
                        return await p();
                    } catch (err) {
                        if (!this.runFailed) {
                            this.runFailed = true;
                            firstError = err;
                        }
                        for (const ac of this.activeWaits) {
                            if (!ac.signal.aborted) {
                                ac.abort(ABORT_REASON_SIBLING_FAILED);
                            }
                        }
                        throw err;
                    }
                }),
            ),
        );
        if (this.runFailed) {
            throw firstError;
        }
        for (const outcome of settled) {
            if (outcome.status === 'rejected') {
                throw outcome.reason;
            }
        }
    }
}
