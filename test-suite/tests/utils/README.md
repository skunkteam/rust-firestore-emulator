# Conformance Test Utilities (`test-suite/tests/utils`)

## Overview
Shared TypeScript harness utilities for the Cloud Firestore conformance test suite (`test-suite/tests`). Selects the active target backend (`CLOUD FIRESTORE`, `RUST EMULATOR`, or `JAVA EMULATOR`) from environment variables, provisions isolated test collection paths and database editions (`standardEdition`, `enterpriseEdition`, `enterpriseEditionPessimistic`), attaches 1-hour `ttl` fallback timestamps to written test documents, and provides deterministic interleaving primitives (`ConcurrentTest`) for multi-client concurrency and locking assertions.

## Interface
- `[index.ts]`: Barrel module re-exporting `* from './common'`, `* from './concurrent'`, and named symbols (`connection`, `editions`, `enterpriseEdition`, `enterpriseEditionPessimistic`, `notImplementedInJava`, `notImplementedInRust`, `readData`, `readDataRef`, `standardEdition`, `writeData`) from `./firestore`.
- `[firestore.ts:connection]`: `'RUST EMULATOR' | 'JAVA EMULATOR' | 'CLOUD FIRESTORE'` — resolves the active backend from `process.env.FIRESTORE_EMULATOR_HOST` and `process.env.RUST_EMULATOR`.
- `[firestore.ts:notImplementedInRust]` & `[firestore.ts:notImplementedInJava]`: Conditional `[] | undefined` skip guards used with Jest parameterized suites (`describe.each`) to skip test cases unsupported by a specific emulator while still running them against Cloud Firestore.
- `[firestore.ts:standardEdition]`, `[firestore.ts:enterpriseEdition]`, `[firestore.ts:enterpriseEditionPessimistic]`, `[firestore.ts:editions]`: Edition descriptors binding a `@google-cloud/firestore` `Firestore` client, isolated `CollectionReference` (`tests/<auto-id>/collection`), `enterprise` boolean, and `concurrencyMode` (`'pessimistic' | 'optimistic'`).
- `[firestore.ts:writeData]`, `[firestore.ts:readData]`, `[firestore.ts:readDataRef]`: Document fixture helpers that inject a 1-hour `ttl` `Timestamp` field on writes and assert/strip `ttl` when reading back document snapshots.
- `[concurrent.ts:ConcurrentTest]`: Deterministic concurrency coordinator exposing `log: string[]`, `lastEvent$`, `processName: AsyncLocalStorage<string>`, `event(what: string)`, `when(...what: string[]): Promise<void>`, and `run(...processes: Array<() => Promise<unknown>>): Promise<void>`.
- `[common.ts:isDefined]` & `[common.ts:expectToBeOrdered]`: Non-nullish type guard (`inp != null`) and UTF-16 lexicographic sort-order assertion helper (`expect(inp).toEqual(inp.slice().sort())`).

## Invariants
- **Backend Target Resolution (`[firestore.ts:connection]`)**: When `FIRESTORE_EMULATOR_HOST` is unset or empty, `connection` always resolves to `'CLOUD FIRESTORE'` regardless of `RUST_EMULATOR`; when `FIRESTORE_EMULATOR_HOST` is set, `RUST_EMULATOR` selects `'RUST EMULATOR'` and its absence selects `'JAVA EMULATOR'`.
- **Edition Filtering on Java Emulator (`[firestore.ts:editions]`, `[firestore.ts:notImplementedInJava]`)**: `editions` includes `enterpriseEdition` (`databaseId: 'enterprise'`, optimistic concurrency) and `enterpriseEditionPessimistic` (`databaseId: 'enterprise-pessimistic'`, pessimistic concurrency) alongside `standardEdition` unless `connection === 'JAVA EMULATOR'`, in which case only `standardEdition` is included.
- **Rust Emulator Edition Provisioning (`[firestore.ts:beforeAll]`)**: When `connection === 'RUST EMULATOR'`, every test file importing `./utils` configures each database in `editions` via `PATCH http://${FIRESTORE_EMULATOR_HOST}/v1/projects/firestore-test-406503/databases/${databaseId}` with `{ databaseEdition, concurrencyMode }` and throws if `!res.ok`.
- **TTL Envelope Contract (`[firestore.ts:writeData]`, `[firestore.ts:readData]`)**: Every document written via `writeData` carries `ttl = Timestamp.fromMillis(Date.now() + ms('1h'))` as a Cloud Firestore TTL cleanup fallback; `readData` requires defined document data (`assert(data, ...)`) and asserts `expect(ttl).toBeInstanceOf(Timestamp)` before returning the remaining fields.
- **Bounded Step Synchronization (`[concurrent.ts:ConcurrentTest.when]`)**: `when(...what)` waits on the reactive `lastEvent$` atom for up to `timeout` ms (default `8_000` ms), rejecting with the full chronological `log` on timeout and always aborting its timer `AbortController` in `finally`.

## Side Effects
- **Module-Load Client & Hook Registration (`[firestore.ts]`, `[index.ts]`)**: Importing `./utils` reads `process.env.FIRESTORE_EMULATOR_HOST` and `process.env.RUST_EMULATOR`, instantiates three `@google-cloud/firestore` clients targeting project `'firestore-test-406503'` (`(default)`, `'enterprise'`, `'enterprise-pessimistic'`), registers a conditional Jest `beforeAll` hook (`fetch` `PATCH` against `/v1/projects/.../databases/...` on the Rust emulator), and registers three unconditional Jest `afterAll` hooks invoking `recursiveDelete` on the per-suite test roots (`standardCollection`, `enterpriseDoc`, `enterprisePessimisticDoc`).
- **Network & Clock I/O (`[firestore.ts:writeData]`, `[firestore.ts:readDataRef]`)**: Reads `Date.now()` when constructing fixtures and performs gRPC document reads via `ref.get()`.
- **Async Context & Reactive State (`[concurrent.ts:ConcurrentTest]`)**: Binds per-worker labels in `AsyncLocalStorage`, mutates `this.log` and `@skunkteam/sherlock` `lastEvent$` state, and schedules `timers/promises` timeouts.

## Verification
- Run (from repository root, builds Rust binary + runs suite): `just sdk-test`
- Run (inside `test-suite/`, lint & format check): `npm run lint`
