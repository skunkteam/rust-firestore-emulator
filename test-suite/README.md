# Cloud Firestore Conformance Suite (`test-suite`)

## Overview
Official `@google-cloud/firestore` TypeScript/Jest conformance test package (`firebase-test-suite`) that verifies `firestore-emulator` behaves identically to the production Google Cloud Firestore service. Rather than treating unit tests or Google's Java-based Firebase emulator as the specification, this suite executes the same SDK operations, concurrency interleavings, queries, listeners, and Enterprise Edition pipelines against three interchangeable targets:

1. **Google Cloud Firestore (Ground Truth)**: `npm test` connects directly to Cloud Firestore (`projectId: 'firestore-test-406503'`) when `FIRESTORE_EMULATOR_HOST` is unset, establishing the authoritative behavior for Standard Edition (pessimistic), Enterprise Edition (optimistic), and Enterprise Edition (pessimistic).
2. **Rust Firestore Emulator**: `npm run test:rust-emulator` spawns `../target/debug/firestore-emulator` on `127.0.0.1:8080` (**without** compiling it first) and runs Jest with `RUST_EMULATOR=true`. From the repository root, `just sdk-test` compiles the binary (`cargo build --no-default-features`) before invoking `npm run test:rust-emulator`.
3. **Google Java-Based Firebase Emulator**: `npm run test:java-emulator` runs the suite via `firebase emulators:exec --only firestore` (`--maxConcurrency 1`) to compare behavior against Google's official local Java emulator.

Every new emulator feature or bugfix affecting Firestore semantics must add, update, or ungate (`notImplementedInRust`) conformance tests in `./tests/` so cloud parity is continuously enforced in local development (`just` / `just sdk-test`) and CI (`compat-test`).

## Interface
- `[package.json:scripts.test]`: `npm test` — runs `NODE_OPTIONS='--experimental-vm-modules' jest` against live Google Cloud Firestore (when `FIRESTORE_EMULATOR_HOST` is unset) or any externally running `FIRESTORE_EMULATOR_HOST`.
- `[package.json:scripts.test:rust-emulator]`: `npm run test:rust-emulator [-- <jest-args>...]` — exports `FIRESTORE_EMULATOR_HOST=127.0.0.1:8080`, starts `../target/debug/firestore-emulator` in the background with `RUST_LOG="${RUST_LOG:=OFF}"` (**no Cargo build step**), runs `RUST_EMULATOR=true NODE_OPTIONS='--experimental-vm-modules' jest "$@"`, sends `SIGINT` (`kill -INT $PID`) on completion, and returns Jest's exit code.
- `[package.json:scripts.test:java-emulator]`: `npm run test:java-emulator [-- <jest-args>...]` — executes `firebase emulators:exec --only firestore` running `NODE_OPTIONS='--experimental-vm-modules' jest --maxConcurrency 1` with forwarded CLI arguments.
- `[package.json:scripts.build:rust]`: `npm run build:rust` — runs `cd .. && cargo build` (with default Cargo features).
- `[package.json:scripts.lint]`: `npm run lint` — runs `eslint tests && prettier -c tests`.
- `[jest.config.js]`: `ts-jest` Node test runner configuration matching `<rootDir>/tests/**/*.test.ts`, loading `jest-extended/all`, and setting `testTimeout: 10_000` ms.
- `[tsconfig.json]`, `[.eslintrc.js]`, `[.prettierrc]`, `[.npmrc]`: Strict TypeScript (`es2024`/`esnext`, `strict: true`), ESLint, Prettier (`printWidth: 140`, `tabWidth: 4`), and npm shell (`script-shell = /bin/bash`) configurations; `node_modules/.bin/tsc` is also invoked from the repository root by `just ui-check` (`../justfile`).
- Subdirectories:
  - `tests`: [./tests/README.md](./tests/README.md) - Numbered conformance test suites (`1-get-set.test.ts` through `10-rust-oracle-edge-cases.test.ts`) and shared backend/concurrency test utilities (`./tests/utils/README.md`).

## Invariants
- **No-Build Binary Execution in `test:rust-emulator` (`[package.json:scripts.test:rust-emulator]`, `[../justfile:sdk-test]`)**: `npm run test:rust-emulator` executes `../target/debug/firestore-emulator` directly without invoking `cargo build`. Running it without rebuilding after Rust changes silently tests a stale binary (or fails if `target/debug/firestore-emulator` is absent); always run `just sdk-test` from the workspace root or run `cargo build` / `npm run build:rust` prior to `npm run test:rust-emulator`.
- **Mandatory Feature & Bugfix Conformance Gate**: Any new Firestore feature, newly supported operator/stage (lifting `notImplementedInRust`), or bugfix for emulator behavior must include corresponding conformance tests in `[./tests/README.md](./tests/README.md)`, verified against Google Cloud Firestore (`npm test`) for ground-truth accuracy and against the Rust emulator (`just sdk-test`).
- **Ground-Truth Cloud Parity (`[tests/utils/firestore.ts:connection]`, `[tests/utils/firestore.ts:notImplementedInRust]`, `[tests/utils/firestore.ts:notImplementedInJava]`)**: Test assertions encode actual Google Cloud Firestore behavior; known emulator gaps are skipped via `notImplementedInRust` or `notImplementedInJava` rather than altering expected values or error codes.
- **Bash Script Shell & VM Modules (`[.npmrc:script-shell]`, `[package.json:scripts]`)**: npm scripts require `/bin/bash` (`f() { ... }; f`, `printf ' %q'`) and `NODE_OPTIONS='--experimental-vm-modules'`.
- **Concurrency & Watch Boundaries (`[package.json:scripts.test:java-emulator]`, `[../.github/workflows/rust.yml:jobs.compat-test]`, `[../justfile:watch]`)**: `test:java-emulator` pins `--maxConcurrency 1`, CI `compat-test` passes `-- --maxConcurrency=3` after `cargo build --no-default-features`, and `just watch` passes `--ignore test-suite` to `cargo watch`.
- **Workspace-Relative ESLint Project Path (`[.eslintrc.js:overrides[1].parserOptions.project]`, `[../justfile:ui-check]`)**: `.eslintrc.js` references `project: 'test-suite/tsconfig.json'` (a repository-root-relative path), and root `just lint` / `just ui-check` depends on `./test-suite/node_modules/.bin/tsc` being installed via `npm ci` or `npm install` in `test-suite/`.

## Side Effects
- **Local Emulator Process & Port Binding (`[package.json:scripts.test:rust-emulator]`, `[package.json:scripts.test:java-emulator]`)**: `test:rust-emulator` binds `127.0.0.1:8080` via `../target/debug/firestore-emulator` and terminates it with `SIGINT`; `test:java-emulator` starts and stops the Firebase CLI Java Firestore emulator subprocess (potentially writing `*-debug.log` files ignored by `[.gitignore]`).
- **Cloud & Emulator Database Mutations (`[tests/utils/firestore.ts]`)**: Running `npm test` without `FIRESTORE_EMULATOR_HOST` performs live reads, writes, queries, transactions, pipeline executions, and `afterAll` `recursiveDelete` calls against GCP project `firestore-test-406503` (`(default)`, `enterprise`, and `enterprise-pessimistic` databases; requires active Application Default Credentials).
- **Build Artifacts (`[package.json:scripts.build:rust]`, `[../justfile:sdk-test]`)**: `npm run build:rust` compiles `../target/debug/firestore-emulator` with default features, whereas `just sdk-test` and CI `compat-test` compile `../target/debug/firestore-emulator` with `--no-default-features`.

## Verification
- Run (from repository root — builds `--no-default-features` + runs conformance suite): `just sdk-test`
- Run (inside `test-suite/`, against live Google Cloud Firestore ground truth): `npm test`
- Run (inside `test-suite/`, against pre-built `../target/debug/firestore-emulator` — **no build step**): `npm run test:rust-emulator`
- Run (inside `test-suite/`, against Google's Java-based Firebase emulator): `npm run test:java-emulator`
- Run (inside `test-suite/`, Prettier check): `npx prettier -c tests`
