import { MaybeFinalState, atom, error, final, unresolved } from '@skunkteam/sherlock';
import { fromPromise } from '@skunkteam/sherlock-utils';
import { AsyncLocalStorage } from 'async_hooks';
import { setTimeout as time } from 'timers/promises';

export class ConcurrentTest {
    readonly log: string[] = [];
    readonly lastEvent$ = atom.unresolved<string>();
    readonly processName = new AsyncLocalStorage<string>();

    private readonly timeout: number;
    constructor({ timeout = 8_000 } = {}) {
        this.timeout = timeout;
    }

    event(what: string) {
        this.log.push(`${this.processName.getStore()} EVENT: ${what}`);
        this.lastEvent$.set(what);
    }

    async when(...what: string[]) {
        const ac = new AbortController();
        const timeoutPromise = time(this.timeout, undefined, { signal: ac.signal }).catch(e => e);

        this.log.push(`${this.processName.getStore()} WAITING UNTIL: ${what.join(' OR ')}`);
        const errorAfterTime$ = fromPromise(timeoutPromise).map((e): MaybeFinalState<never> => {
            if (e === 'aborted') return unresolved;
            const currentLog = this.log.map(m => '\n- ' + m).join('');
            const timeoutSeconds = this.timeout / 1_000;
            const msg = `${this.processName.getStore()} timeout after ${timeoutSeconds}s, current log: ${currentLog}`;
            return final(error(msg));
        });
        try {
            await this.lastEvent$.toPromise({ when: d$ => d$.map(v => what.includes(v)).or(errorAfterTime$) });
        } finally {
            ac.abort('aborted');
        }
    }

    async run(...processes: Array<() => Promise<unknown>>) {
        await Promise.all(processes.map(async (p, i) => this.processName.run(`${'       |'.repeat(i)} <<${i + 1}>> |`, p)));
    }
}
