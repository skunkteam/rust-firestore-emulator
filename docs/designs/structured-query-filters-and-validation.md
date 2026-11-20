# `StructuredQuery` Filter Completion and Runtime Validation Parity

*   **Author:** Paco van der Linden
*   **Status:** Implemented
*   **Date:** 2026-11-17

---

## Overview

`rust-firestore-emulator` emulates Google Cloud Firestore in memory across six Rust workspace crates under `crates/*`—touching runtime crates ([emulator-database](../../crates/emulator-database/Cargo.toml) and [emulator-http](../../crates/emulator-http/Cargo.toml)) alongside `googleapis`, `emulator-grpc`, `emulator-tracing`, and verification crate [emulator-tests](../../crates/emulator-tests/Cargo.toml)—paired with a zero-build browser Web UI ([ui/README.md](../../ui/README.md)) and a `@google-cloud/firestore` conformance suite ([test-suite/README.md](../../test-suite/README.md)) verified against live Cloud Firestore (`firestore-test-406503`). In production, Cloud Firestore offers **Standard Edition** (index-backed `StructuredQuery` execution with pessimistic concurrency by default) and **Enterprise Edition** (`StructuredQuery` plus index-free multi-stage `ExecutePipeline`, supporting optimistic and pessimistic concurrency).

This design completes `StructuredQuery` filter execution (`ARRAY_CONTAINS_ANY` and `NOT_IN`) and aligns runtime (non-deploy-time) query validation and canonical `order_by` normalization with live Cloud Firestore across Standard Edition (pessimistic) and Enterprise Edition (optimistic and pessimistic). Every `StructuredQuery` entrypoint—gRPC `RunQuery`, `RunAggregationQuery`, and `Listen` (`onSnapshot`), plus HTTP REST `:runQuery` and the Web UI visual query builder ([ui/components/query.js](../../ui/components/query.js))—will route through a single non-panicking, unified validation pipeline (with `O(|AST|)` filter validation) in `crates/emulator-database`. The design supersedes community prototype [PR #195](https://github.com/skunkteam/rust-firestore-emulator/pull/195) and adds zero Cargo or npm dependencies.

## The Problem

Applications and SDK test suites using `StructuredQuery` filters or raw wire queries hit four categories of failures and silent behavioral drift against `rust-firestore-emulator`:

1.  **Unimplemented `ARRAY_CONTAINS_ANY` and `NOT_IN` Filter Operators and Standard `null`/`NaN` Drift:**
    *   In [filter.rs](../../crates/emulator-database/src/database/query/filter.rs), field-filter evaluation returns `UNIMPLEMENTED` errors for `ARRAY_CONTAINS_ANY` and `NOT_IN`, and its missing-field fast path returns `true` on Enterprise Edition only for `NOT_EQUAL` (omitting `NOT_IN`).
    *   On Standard Edition, filter evaluation returns `true` instead of `false` when `NOT_EQUAL` compares a non-null field against a `null` operand or when `ARRAY_CONTAINS` compares an array containing `null` or `NaN` against a top-level `null` or `NaN` operand. Existing coverage in [2-basic-query.test.ts](../../test-suite/tests/2-basic-query.test.ts) only avoided `UNIMPLEMENTED` on an Enterprise `array-contains` + `array-contains-any` query because composite filter evaluation short-circuited on a missing leading field before reaching `ARRAY_CONTAINS_ANY`.
    *   In [structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs), black-box tests explicitly pin `NOT_IN` and `ARRAY_CONTAINS_ANY` to `UNIMPLEMENTED`, and [http_and_multiplex.rs](../../crates/emulator-tests/src/suites/http_and_multiplex.rs) lacks REST `:runQuery` filter tests.
2.  **Unset `Value` Payloads, Empty `OR` Filters, and Divergent Runtime Validation:**
    *   **Unset `Value` Operands and Empty `OR` Composite Filters ([filter.rs](../../crates/emulator-database/src/database/query/filter.rs)):** Wire decoding rejects missing required protobuf subfields (`filter_type`, `field`, `value`, `operand_type`), yet query validation still accepts unset `Value` payloads (missing `value_type`) and empty `OR` composite filters (`filters: []`, reachable over raw gRPC and REST `:runQuery` JSON) instead of returning `3 INVALID_ARGUMENT`.
    *   **Flat Array-Containment Counting and Missing Disjunctive Normal Form (DNF) and Negation Limits ([query.rs](../../crates/emulator-database/src/database/query.rs)):** Query validation duplicates the same array-containment count across both vector and regular query branches, rejecting valid Standard Edition `OR` queries that use one `ARRAY_CONTAINS` or `ARRAY_CONTAINS_ANY` per branch. It also omits Standard Edition's 30-disjunction ceiling across `AND`/`OR`/`IN`/`ARRAY_CONTAINS_ANY`, negation exclusivity (at most one `!=`, `NOT_IN`, `IS_NOT_NULL`, or `IS_NOT_NAN`), `NOT_IN` + `OR`/`IN`/`ARRAY_CONTAINS_ANY` exclusion, and array-operand type/non-empty/length validation (`<= 30` for Standard `IN`/`ARRAY_CONTAINS_ANY`, `<= 10` for Standard `NOT_IN`, without Standard Edition's `30`/`10` caps on Enterprise—pre-implementation probes reached `5,000` elements, with larger arrays untested).
    *   **String-Split Inequality Sorting, Late Cursor-Length Checks, Missing Standard `order_by` Guards, and Cursor Panic ([query.rs](../../crates/emulator-database/src/database/query.rs), [filter.rs](../../crates/emulator-database/src/database/query/filter.rs)):** Inequality field extraction splits dotted string representations on `'.'`, allocating temporary strings and mis-splitting escaped dots like `` `a.b` ``. Standard `order_by` permits duplicate fields and fields trailing `__name__`. Cursor lengths are checked *after* implicit `order_by` synthesis rather than against explicit `order_by` (and existing raw cursor assertions in [query/tests.rs](../../crates/emulator-database/src/database/query/tests.rs) and [structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs) pin that post-synthesis behavior even though Cloud Firestore rejects over-length cursors with `3 INVALID_ARGUMENT`). Missing cursor document fields also trigger a panic during cursor boundary comparisons.
    *   **Missing `__name__` Reference/Operator Validation, Reserved Paths, and Unbounded Debug Logs ([query.rs](../../crates/emulator-database/src/database/query.rs), [field_path.rs](../../crates/emulator-database/src/database/field_path.rs), [projection.rs](../../crates/emulator-database/src/database/projection.rs), [reference.rs](../../crates/emulator-database/src/database/reference.rs)):** Reserved (`__.*__` of length `>= 5`) and empty (`a..b`) `FieldPath` segments are accepted in `FieldFilter`, `UnaryFilter`, `Order`, `StructuredQuery.select`, and read/response `DocumentMask`s (whereas write-time `Write.update_mask` in [validation.rs](../../crates/emulator-database/src/database/validation.rs) already validates paths). Query `ReferenceValue` document paths in filters and cursors are unvalidated (canonical Datastore `__id<i64>__` document IDs are valid in query references across both editions). Today, a raw gRPC query `WHERE __name__ != "not-a-ref"` silently matches every document on Standard Edition instead of failing with `3 INVALID_ARGUMENT`, and derived debug formatting on filters dumps full multi-thousand-element Enterprise arrays into logs.
3.  **Hardcoded `"IN"` Logic, Lossy Array Validation, and Stripped Error Codes in the Web UI:**
    *   In [query.js](../../ui/components/query.js), the operator registry omits `ARRAY_CONTAINS_ANY` and `NOT_IN`, and visual query construction and filter-row rendering hardcode `"IN"` in emitted payloads, row initialization, and error strings.
    *   Although [values.js](../../ui/values.js) preserves `-0.0` formatting in serialized `doubleValue` JSON, the visual JSON-array builder in [query.js](../../ui/components/query.js) skips unquoted `|n| >= 2^53` integer precision detection (silently rounding large unquoted integers during `JSON.parse`), turns `-0` into integer `0` and large safe/unsafe numbers into scientific-notation integer strings, forwards unvalidated `{ ... }` objects and nested JavaScript arrays (`[[1]]`), and lacks array-filter coverage in [ui_regressions.rs](../../crates/emulator-http/tests/ui_regressions.rs). Meanwhile, query execution in [app.js](../../ui/app.js) displays only error message strings, dropping HTTP codes and Firestore status names ([api.js](../../ui/api.js)).
4.  **Duplicated Raw-RPC Test Shims:**
    *   Because `@google-cloud/firestore` rejects many invalid queries client-side before transmitting them, server-side wire validation tests must invoke raw GAPIC RPCs. Currently, [11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts) defines local raw-RunQuery, `NewTransaction` rollback cleanup, and status-assertion setup logic, while [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts) uses an inline `try/catch` RPC status loop instead of sharing minimal utilities in [common.ts](../../test-suite/tests/utils/common.ts).

## Goals

*   **Complete `ARRAY_CONTAINS_ANY` and `NOT_IN` Filter Execution:** Evaluate `ARRAY_CONTAINS_ANY` and `NOT_IN` over gRPC (`RunQuery`, `RunAggregationQuery`, `Listen`), HTTP REST `:runQuery`, optimistic and pessimistic transactions, and `findNearest` pre-filters for Standard and Enterprise editions—matching Cloud Firestore's `null`, `NaN`, cross-type numeric/`-0.0`, and missing-field rules (including fixing Standard `NOT_EQUAL` with `null` and `ARRAY_CONTAINS` with top-level `null`/`NaN`) without altering `googleapis::Value`'s edition-agnostic `PartialEq` and `Ord`.
*   **Reject Unset `Value` Operands and Empty `OR` Composite Filters:** Preserve non-panicking missing-subfield errors in wire filter decoding, reject unset `Value` operands (missing `value_type`), and reject empty `OR` composite filters (`filters: []`) with `3 INVALID_ARGUMENT` (`400 Bad Request` on REST) while keeping empty `AND` composite filters as valid tautologies.
*   **Enforce `O(|AST|)` Runtime Filter, `__name__`, and Reserved-Path Validation:** Enforce validation cleanly across regular and `findNearest` queries:
    *   Check reserved/empty `FieldPath` segments across `FieldFilter`, `UnaryFilter`, `Order`, `StructuredQuery.select`, and read/response `DocumentMask`s (matching existing write-time `Write.update_mask` validation).
    *   Enforce bounded `O(|AST|)` Standard DNF disjunction (`<= 30`) and per-disjunction array-containment (`<= 1`) limits, Standard array-length caps (`IN`/`ARRAY_CONTAINS_ANY <= 30`, `NOT_IN <= 10`), negation exclusivity (`<= 1` of `!=`, `NOT_IN`, `IS_NOT_NULL`, `IS_NOT_NAN`), and `NOT_IN` vs. `OR`/`IN`/`ARRAY_CONTAINS_ANY` exclusion.
    *   Validate query `ReferenceValue` document paths with zero heap allocations (allowing database roots and canonical `__id<i64>__` Datastore IDs across both editions while rejecting non-canonical `-0`/leading-zero IDs), enforce Standard `__name__` operator and database-root rules, and validate cursor `__name__` slots across both editions.
*   **Canonicalize `order_by` Validation, Cursor Validation, and Segment-Wise Inequality Sorting:** Validate cursor value counts against explicit `order_by` *before* implicit synthesis, reject duplicate fields and trailing fields after `__name__` on Standard Edition, sort non-`__name__` inequality fields directly by path segment (handling escaped dots without temporary string splitting) across both `order_by` synthesis and vector search tie-breaking, and handle missing cursor document fields safely without panicking.
*   **Bound Filter Debug Output:** Implement bounded debug formatting on field filters (matching vector search specs) so `ArrayValue` operands log element count and a bounded prefix instead of dumping full arrays.
*   **Generalize Web UI Array Filters, Reuse `ui/values.js` Validation, and Surface Query Error Codes:** Update [query.js](../../ui/components/query.js) to support `IN`, `ARRAY_CONTAINS_ANY`, and `NOT_IN`; reuse document-editor unquoted `|n| >= 2^53` integer detection and ProtoJSON normalization from [values.js](../../ui/values.js) while preserving safe integers, `-0.0`, and `Infinity`/`-Infinity` doubles and rejecting nested JavaScript arrays; prefix API error banners in [app.js](../../ui/app.js) with `[code status]`; and cover these paths in [ui_regressions.rs](../../crates/emulator-http/tests/ui_regressions.rs).
*   **Share Minimal Raw-RPC Conformance Helpers and Maintain Zero-New-Index Cloud Parity:** Extract shared raw `RunQuery` and gRPC status-assertion helpers into [common.ts](../../test-suite/tests/utils/common.ts) (reused by [11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts) and [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts)), replace `UNIMPLEMENTED` pins and align post-synthesis cursor assertions in [query/tests.rs](../../crates/emulator-database/src/database/query/tests.rs) and [structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs), add REST `:runQuery` wire tests in [http_and_multiplex.rs](../../crates/emulator-tests/src/suites/http_and_multiplex.rs), add Jest conformance tests using zero new Cloud composite indexes on `firestore-test-406503`, and update affected directory `README.md` contracts.

## Non-Goals

*   **Enterprise Pipeline Array and Membership Functions:** Implementing `array_contains`, `array_contains_any`, `array_contains_all`, `equal_any`, or `not_equal_any` in [function.rs](../../crates/emulator-database/src/database/pipeline/function.rs) and lifting their `notImplementedInRust` guards ([test-suite/tests/utils/README.md](../../test-suite/tests/utils/README.md)) in [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts) is deferred to a separate pipeline increment.
*   **Deploy-Time Composite Index Enforcement:** Simulating `firestore.indexes.json`, single-field index exemptions, or `9 FAILED_PRECONDITION: The query requires an index` checks is out of scope.
*   **REST `:runAggregationQuery`, `explain_options`, and `SUM`/`AVG` Field-Path Validation:** HTTP query work is scoped strictly to existing REST `:runQuery` (`StructuredQuery` without `findNearest`); REST aggregation endpoints, query explain execution, and validating `FieldReference` paths on `StructuredAggregationQuery` `SUM`/`AVG` aggregations ([database.rs](../../crates/emulator-database/src/database.rs)) remain out of scope.
*   **General `__id<i64>__` Ordering Changes in `crates/googleapis`:** Altering cross-crate numeric reference ordering in [value_ext.rs](../../crates/googleapis/src/value_ext.rs) (beyond `REFERENCE_NAME_MIN_ID`) is out of scope; `crates/googleapis` remains untouched.
*   **Cross-Crate Logging or Macro Refactors:** Logging/telemetry changes and workspace-wide `unimplemented!` macro refactors beyond bounded filter debug formatting are excluded.
*   **Complex Web UI Array Editors or Client-Side Query Rule Duplication:** Building multi-row array sub-forms, visual/raw tab-switch confirmation modals, or client-side DNF/`NOT_IN` validation in `ui/` is excluded; server-side validation errors surface directly through `[code status]` banners.
*   **Extracting Vector-Specific Listener Harness Helpers:** The custom bidirectional vector listener remains local to [11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts); live `ARRAY_CONTAINS_ANY` and `NOT_IN` conformance tests use standard SDK `onSnapshot`.
*   **Provisioning New Cloud Composite Indexes:** No new composite indexes will be deployed to GCP project `firestore-test-406503`.

## Open Questions

None.

## Critical User Journeys (CUJs)

### CUJ 1: `ARRAY_CONTAINS_ANY` and `NOT_IN` Execution Across gRPC, REST, Listeners, Aggregations, Transactions, and `findNearest`

*   **Given**: Standard Edition (pessimistic) and Enterprise Edition (optimistic and pessimistic) databases populated with documents containing arrays, client SDK `BigInt` values alongside wire 64-bit integer/`f64`/`-0.0` numbers, `null`, `NaN`, and missing fields.
*   **When**: A client runs `where(..., 'array-contains-any', [...])` or `where(..., 'not-in', [...])` via `Query.get()`, `Query.count()` / `aggregate()`, `Query.onSnapshot()`, pessimistic/optimistic `runTransaction`, `findNearest` pre-filters, or HTTP `POST .../documents:runQuery`.
*   **Then**:
    *   `ARRAY_CONTAINS_ANY` matches documents whose target field is an `ArrayValue` containing at least one element equal to any operand element under Cloud Firestore's edition-aware equality rules (including cross-type `42` == `42.0` and `0` == `-0.0`, excluding top-level `null` and `NaN` matches on Standard while matching them on Enterprise).
    *   `NOT_IN` excludes documents matching any operand element while handling missing fields (excluded on Standard, always matched on Enterprise—including for `NOT_IN [null]`), `null` (on Standard, a `null` document field evaluates to `false` for that document while non-`null` documents still match, whereas `null` inside the `NOT_IN` array evaluates to `false` for every document; on Enterprise, present `null` fields match when `null` is absent from the array), and `NaN` (`NaN != NaN` on Standard vs. `NaN == NaN` on Enterprise), ordering results implicitly by `(field ASC, __name__ ASC)` on Standard Edition when `orderBy` is omitted.
    *   `onSnapshot` delivers initial and incremental `added`/`modified`/`removed` changes as documents enter or leave the `ARRAY_CONTAINS_ANY` or `NOT_IN` match set, and transactional reads enforce edition-appropriate read locks or optimistic commit verification.

### CUJ 2: Non-Panicking Wire Validation, Bounded DNF Counting, `__name__` Enforcement, and Canonical `order_by`

*   **Given**: A client sending raw gRPC `RunQuery` / `Listen` requests or raw REST `:runQuery` JSON payloads—including missing optional protobuf fields, empty `CompositeFilter.filters`, deeply nested `AND`/`OR`/`IN`/`ARRAY_CONTAINS_ANY` trees, explicit `DESC` orderings with range filters, and `__name__` or reserved `__.*__` paths.
*   **When**: Query parsing and validation process the request.
*   **Then**:
    *   Missing `filter_type`, `field`, `value`, `value_type`, or `operand_type` fields and empty `OR` `CompositeFilter.filters` return `3 INVALID_ARGUMENT` (`400 Bad Request` over REST) cleanly without panicking request workers or crashing the emulator, while empty `AND` `CompositeFilter.filters` evaluates as `true`.
    *   Standard Edition permits `OR` queries with one `ARRAY_CONTAINS` or `ARRAY_CONTAINS_ANY` per branch up to 30 total DNF disjunctions and rejects trees exceeding 30 disjunctions, per-disjunction containment limits, array-length limits, negation exclusivity, or `NOT_IN` disjunction combinations in `O(|AST|)` filter-validation time and `O(depth)` stack space. Enterprise Edition executes nested compositions within wire recursion limits without Standard Edition's 30-disjunction or `30`/`10` array-length caps (pre-implementation probes reached `5,000` elements pending conformance verification, with larger arrays untested).
    *   Query `ReferenceValue` operands accept database roots (`projects/{p}/databases/{d}` and `.../documents`) and canonical `__id<i64>__` Datastore sentinels across both editions while rejecting collection/odd-subcollection paths and non-canonical/non-numeric reserved document IDs (`__id-0__`, `__id01__`, `__id+1__`, `__custom__`). Standard Edition enforces index-key `__name__` operator, non-`ReferenceValue` operand, and database-root checks, duplicate `order_by` checks, and `__name__`-must-be-last checks (while Enterprise Edition executes index-free scans without those key-index restrictions). Both editions reject non-`ReferenceValue` and wrong-database cursor values at explicit `__name__` slots, out-of-range `GeoPointValue`/`TimestampValue` and unsupported `FieldReferenceValue`/`FunctionValue`/`PipelineValue` operands, and cursors exceeding explicit `order_by` length, and reserved `__.*__` (`len >= 5`) or empty path segments fail with `3 INVALID_ARGUMENT` (while single-segment `` `__name__` `` parses to `FieldReference::DocumentName` across queries and masks).
    *   Standard Edition appends missing inequality fields and `__name__` without duplicates in segment-wise lexicographical path order (handling field segments containing `.` without temporary string splitting), inherits the last explicit direction (defaulting to ascending on empty `order_by`), and keeps cursor slots and vector tie-breaking aligned.

### CUJ 3: Visual Array-Filter Authoring and Diagnostic Error Banners in the Web UI

*   **Given**: A developer inspecting an emulated database in the browser UI's Structured Query Runner (`Visual Builder` or `Raw StructuredQuery JSON` mode).
*   **When**: The developer selects `in`, `array-contains-any`, or `not-in`, enters a JSON array containing strings, booleans, `null`, `-0`, integers, or ProtoJSON value objects (or submits an invalid query that the server rejects).
*   **Then**:
    *   The visual query builder emits the selected operator (`"IN"`, `"ARRAY_CONTAINS_ANY"`, or `"NOT_IN"`), rejects unquoted `|n| >= 2^53` integers (outside explicit `"doubleValue"` properties) before JSON parsing, encodes safe non-`-0` integers (`|n| < 2^53`) as `integerValue`, encodes `-0`, fractional numbers (such as `1.5`), and unsafe-integer floats (such as `1e25`) as numeric `doubleValue`, rejects overflowing numeric literals (such as `1e309`) across both shorthand arrays and explicit `{ "doubleValue": 1e309 }` while accepting string `"Infinity"`/`"-Infinity"` `doubleValue`s, rejects shorthand nested JavaScript arrays, and disallows `arrayValue` objects in `ARRAY_CONTAINS_ANY` while permitting single-level `{ arrayValue: { values: [...] } }` operands in `IN`/`NOT_IN`.
    *   Server-side validation failures render in the query error banner prefixed with `[code status]` (for example, `[400 INVALID_ARGUMENT] ...`) so the developer sees both the HTTP/Firestore status and reason.

### CUJ 4: Zero-New-Index Cloud Conformance and Shared Raw-RPC Wire Testing

*   **Given**: The `test-suite/` Jest runner targeting live GCP project `firestore-test-406503` (`npm test`) and a freshly built Rust emulator (`just sdk-test`).
*   **When**: Conformance tests execute SDK queries (`array-contains-any`, `not-in`, `onSnapshot`, aggregations, transactions) and raw GAPIC `RunQuery` wire-validation checks via shared raw-RPC and status-assertion helpers.
*   **Then**:
    *   Every test passes identically against live Cloud Firestore (`Standard`, `Enterprise` optimistic, and `Enterprise` pessimistic) and the Rust emulator without leaking un-rolled-back `NewTransaction` handles on Cloud.

---

## Technical Plan (Architecture)

All `StructuredQuery` traffic across Tonic gRPC and Axum HTTP/REST converges on query parsing and validation in `crates/emulator-database`. Keeping semantic filter validation, edition policy branching, and `order_by` normalization inside the query subsystem preserves `crates/googleapis` as an edition-agnostic wire and value-ordering crate ([value-partial-eq.md](./value-partial-eq.md)) and leaves Enterprise Pipeline execution (`crates/emulator-database/src/database/pipeline`) uncoupled. Transactional scans (`Query::once`), snapshot recording (`LockStrategy::confirm_all`), and optimistic commit verification (`OptimisticState::check_concurrency_violations`, detailed in [database/README.md](../../crates/emulator-database/src/database/README.md)) all evaluate filter predicates uniformly, so documents matched by `ARRAY_CONTAINS_ANY` or `NOT_IN`—including Enterprise missing-field matches—automatically participate in pessimistic read-lock retention and optimistic snapshot checks without altering `transaction.rs`.

```text
  gRPC Clients / Node SDK                       Browser Web UI (ui/app.js + ui/components/query.js)
  - RunQuery                                    - Visual Builder (array ops: IN / ARRAY_CONTAINS_ANY / NOT_IN)
  - RunAggregationQuery                         - Raw StructuredQuery JSON
  - Listen (onSnapshot)                         - Error banner surfaces "[code status] message"
            |                                                          |
            v                                                          v
  crates/emulator-grpc                                       crates/emulator-http
  (protobuf StructuredQuery)                                 (POST .../documents:runQuery -> ProtoJSON)
            \                                                          /
             \-------------------------\    /-------------------------/
                                        v  v
                     StructuredQuery Parsing & Validation (crates/emulator-database)
                       1. Wire Decoding
                          - Filter / FieldFilter / UnaryFilter / Order: missing-subfield & enum checks
                          - Projection (select & DocumentMask): reserved / empty FieldPath validation
                       2. Edition-Aware Runtime Validation (shared by regular & vector queries)
                          - Cross-edition checks: parent/from, field paths (`__name__` / `` `__name__` `` -> DocumentName),
                            non-empty OR & ArrayValue, unset Value / GeoPoint / Timestamp / pipeline ValueType rejection,
                            vector sentinel maps & zero-alloc ReferenceValue checks (incl. canonical `__id<i64>__`)
                          - Standard filter rules: array caps (30/30/10), <= 1 negation op,
                            NOT_IN vs OR/IN/ARRAY_CONTAINS_ANY, __name__ op/root checks, O(|AST|) storage-dedup DNF check
                          - Standard duplicate order_by rejection (regular & optional vector order_by)
                          - Regular queries: pre-synthesis cursor length & cross-edition __name__ slot check,
                            then segment-wise inequality & __name__ order_by synthesis
                          - Vector queries: Standard inequality & __name__ order_by synthesis (`last_dir`)
                          - Standard __name__-must-be-last check (regular & vector queries)
                                         |
                                         v
                     Execution / Live-Query Event Processor / Optimistic Commit Verification
                       (Query::once, Listener, LockStrategy::confirm_all, OptimisticState::check_concurrency_violations)
                       - Prefilters & predicate evaluation (edition equality + direct ArrayValue scan)
                       - Vector distance tie-breaking & non-panicking cursor boundary comparison
```

### 1. Wire Filter Decoding, Segment-Wise Inequality Sorting, and Bounded Debug Formatting ([filter.rs](../../crates/emulator-database/src/database/query/filter.rs))

*   **Plain Data Filter AST & Wire-Presence Decoding:** Keep field filter structs as plain data matching composite and unary filter structs (with no derived operand caches). Restrict wire decoding to missing-subfield errors, field-path string syntax parsing, and protobuf enum decoding so all semantic query validation—whether arriving from wire requests or in-process query builders—runs once inside unified query validation.
*   **Segment-Wise Inequality Sorting:** Non-`__name__` inequality field collection sorts and deduplicates directly over parsed path segments rather than splitting formatted strings on `'.'`, eliminating temporary string allocations during comparisons and handling escaped dots (`` `a.b` ``) consistently with Firestore field paths.
*   **Bounded Filter Debug Formatting:** Replace derived debug output on field filters with bounded manual formatting matching vector search specifications ([find_nearest.rs](../../crates/emulator-database/src/database/query/find_nearest.rs)): `ArrayValue` operands print element count and up to a 3-element prefix rather than dumping multi-thousand-element arrays.

### 2. Edition-Aware Filter Evaluation (`ARRAY_CONTAINS_ANY`, `NOT_IN`, `null`, `NaN`, and Missing Fields) ([filter.rs](../../crates/emulator-database/src/database/query/filter.rs))

Pre-implementation live Cloud Firestore probes on `firestore-test-406503` (to be codified permanently in `test-suite/`) indicate a unified edition-aware equality rule across Standard and Enterprise editions without altering `googleapis::Value`:

*   **Edition-aware scalar/element equality:** On Enterprise Edition, equality delegates directly to `googleapis::Value` equality (matching `null == null` and `NaN == NaN`); on Standard Edition, equality returns `false` if either operand is `null` or `NaN`, and delegates to `googleapis::Value` equality otherwise (preserving cross-type numeric `42 == 42.0` and `0 == -0.0`). Range comparisons (`<`, `<=`, `>`, `>=`) apply Firestore type-compatibility gating (while `<`, `<=`, `>`, `>=`, `!=`, `NOT_IN`, `IS_NOT_NULL`, and `IS_NOT_NAN` all count as inequalities when synthesizing implicit `order_by` on Standard Edition).
*   **Direct `ArrayValue` membership evaluation:** Array filters (`IN`, `NOT_IN`, `ARRAY_CONTAINS_ANY`) scan the stored `ArrayValue` slice directly with edition-aware element equality (`false` immediately on Standard if the candidate element is `null` or `NaN`).

| Operator | Missing Field (Standard) | Missing Field (Enterprise) | Present Field Evaluation (`value`) |
| --- | --- | --- | --- |
| `<`, `<=`, `>`, `>=` | `false` | `false` | Requires type compatibility; on Standard requires neither `value` nor operand is `null`/`NaN`; compares via `<op>` |
| `==` (`Equal`) | `false` | `false` | Edition-aware equality (`value == operand`, excluding `null`/`NaN` on Standard) |
| `!=` (`NotEqual`) | `false` | `true` | On Enterprise, `!eq(value, operand)`; on Standard, `false` if `value` or operand is `null`, otherwise `!eq(value, operand)` (so `NaN != NaN` is `true` on raw `FieldFilter`) |
| `IN` (`In`) | `false` | `false` | Operand array contains any element matching `value` under edition-aware equality |
| `NOT_IN` (`NotIn`) | `false` | `true` | On Enterprise, `!contains(value)`; on Standard, `false` if `value` is `null` or operand array contains `null`, otherwise `!contains(value)` |
| `ARRAY_CONTAINS` | `false` | `false` | `value` is an `ArrayValue` with any element matching operand under edition-aware equality |
| `ARRAY_CONTAINS_ANY` | `false` | `false` | `value` is an `ArrayValue` with any element matching any operand array element under edition-aware equality |

Target Cloud behaviors captured by this table:

1.  **`NOT_IN` with `null` on Standard Edition:** On Standard Edition, a `null` document field causes `NOT_IN` to evaluate to `false` for that document alone (documents with non-`null` values still match `NOT_IN [1]`), whereas `null` anywhere inside the `NOT_IN` operand array (`NOT_IN [null]` or `NOT_IN [1, null]`) evaluates to `false` for every document (matching `FieldFilter(NotEqual, NullValue)`). Enterprise Edition treats `null` as a comparable value and matches missing fields unconditionally (including `NOT_IN [null]`).
2.  **`NOT_IN` and `!=` with `NaN` on Standard Edition:** Because Standard equality rejects `NaN`, a document with `val: NaN` matches both raw `val != NaN` and `val NOT_IN [NaN]` on Standard Edition, whereas on Enterprise Edition `NaN == NaN` is `true` so `val: NaN` is excluded. (`@google-cloud/firestore` rewrites `.where(f, '!=', NaN)` to unary `IS_NOT_NAN`, whose behavior is unchanged.)
3.  **`ARRAY_CONTAINS` and `ARRAY_CONTAINS_ANY` with `null`/`NaN` vs. `{ nested: NaN }`:** Top-level `null` and `NaN` operands never match array elements on Standard Edition and match `null`/`NaN` elements on Enterprise Edition, while `{ nested: NaN }` is a `MapValue` (neither `null` nor `NaN`) and matches on both editions via structural value equality.

### 3. Reserved `FieldPath` Validation and Zero-Allocation Query Reference Parsing ([field_path.rs](../../crates/emulator-database/src/database/field_path.rs), [projection.rs](../../crates/emulator-database/src/database/projection.rs), [reference.rs](../../crates/emulator-database/src/database/reference.rs), [query.rs](../../crates/emulator-database/src/database/query.rs))

*   **Segment-Wise Path Ordering and Query Reference Check ([field_path.rs](../../crates/emulator-database/src/database/field_path.rs), [projection.rs](../../crates/emulator-database/src/database/projection.rs)):**
    *   Multi-segment field paths adopt segment-wise total ordering without altering public `FieldReference` semantics so non-`__name__` inequality fields sort consistently by path segments.
    *   Query field-reference parsing recognizes both unquoted `__name__` and single-segment backtick-quoted `` `__name__` `` as `FieldReference::DocumentName` across filters, `order_by`, `select`, and read/response `DocumentMask`s, while delegating multi-segment and non-`__name__` paths to non-reserved segment validation (rejecting empty segments such as `a..b`, `.a`, `a.`, `` `a`. `` and reserved segments `^__.+__$` with length `>= 5` such as `__custom__` or `a.__name__`, while permitting `"____"`).
    *   Projection decoding enforces query field-reference validation on every parsed `StructuredQuery.select` and `DocumentMask` path (covering `CreateDocument`, `UpdateDocument`, `GetDocument`, `BatchGetDocuments`, and `ListDocuments`; because projections cannot be constructed unvalidated, query validation does not re-check `select`).
*   **Shared Zero-Allocation Query `ReferenceValue` Validator ([reference.rs](../../crates/emulator-database/src/database/reference.rs)):**
    *   Extract one shared internal zero-allocation path/segment validator inside `reference.rs` (returning borrowed project/database ID slices and path parity, parameterized on whether Datastore numeric IDs `__id<i64>__` are allowed) used by both strict resource/write reference parsing (rejecting all `__.*__` segments) and query document-reference validation without heap allocations or global string-atom interning.
    *   Query `ReferenceValue` strings must match `projects/{project_id}/databases/{database_id}` or `projects/{project_id}/databases/{database_id}/documents[/{rel_path}]` with non-empty IDs, either empty `rel_path` (database roots) or an even number of slash-separated segments (`>= 2`, rejecting collection and odd subcollection paths), every segment non-empty, not `"."` or `".."`, and `<= 1500` bytes, collection segments (even indices) non-reserved, and document segments (odd indices) non-reserved except for canonical `__id<i64>__` Datastore IDs (`__id0__`, `__id-9223372036854775808__` permitted on general `ReferenceValue` fields, cursors, and Enterprise `__name__`; `__id0__` rejected on Standard `__name__` filters with `"Document ID of type long must be non-zero"`; `__id-0__`, `__id01__`, `__id+1__` rejected everywhere).
*   **Recursive Query Operand Validation ([query.rs](../../crates/emulator-database/src/database/query.rs)):**
    *   Relocate operand validation from [vector.rs](../../crates/emulator-database/src/database/vector.rs) (which had `query.rs` as its sole caller) into the query module, keeping `vector.rs` focused on vector math and `findNearest` scoring.
    *   Walk all `FieldFilter` values and `Cursor` values recursively across `ArrayValue` and non-vector `MapValue` entries (delegating `__vector__` sentinel maps—described in [vector-search.md](./vector-search.md)—to existing vector classification), rejecting unset `Value` payloads (missing `value_type`), out-of-range `GeoPointValue`/`TimestampValue`, unsupported `FieldReferenceValue`/`FunctionValue`/`PipelineValue` variants, and invalid `ReferenceValue` paths via the zero-allocation query reference validator.

### 4. Unified Runtime Validation and Canonical `order_by` Normalization ([query.rs](../../crates/emulator-database/src/database/query.rs))

Query validation executes a single unified pipeline across regular and `findNearest` queries (covering both wire-decoded requests and in-process query builder callers):

1.  **Cross-Edition Structural, Field-Reference, Filter-Structure, and Operand Checks:**
    *   Verify `parent` points to a document or database root (never a collection) and `from` is non-empty (projections are already validated during decoding).
    *   In one unified walk over `order_by` and `filter`: validate query field references across every clause, reject empty `CompositeOperator::OR` (`filters` empty) while allowing empty `AND`, require `IN`/`ARRAY_CONTAINS_ANY`/`NOT_IN` operands to be non-empty `ArrayValue`s, and validate all filter operands and cursor values recursively (rejecting unset payloads and invalid document `ReferenceValue` paths).
2.  **Standard Edition Filter Constraints (Shared Across Regular and `findNearest` Queries):**
    *   **Array Size Caps:** `IN <= 30`, `ARRAY_CONTAINS_ANY <= 30`, `NOT_IN <= 10`.
    *   **Negation Exclusivity:** At most one `!=`, `NOT_IN`, `IS_NOT_NAN`, or `IS_NOT_NULL` filter per query.
    *   **`NOT_IN` Disjunction Exclusion:** `NOT_IN` cannot appear in the same query as `OR`, `IN`, or `ARRAY_CONTAINS_ANY`.
    *   **`__name__` Filter Rules:** Reject any `UnaryFilter` or `ARRAY_CONTAINS` / `ARRAY_CONTAINS_ANY` on `__name__`; require scalar `__name__` filters and every element of `__name__` `IN`/`NOT_IN` arrays to be a `ReferenceValue` matching the target `(project_id, database_id)` root (reflecting Standard Edition's key-index restrictions; Enterprise executes index-free scans, while Phase 3 conformance tests verify live Enterprise behavior so any shared Cloud rejection moves to Step 1).
    *   **Clamped DNF Summary Fold (`O(|AST|)` Time, `O(depth)` Stack, Zero Heap Allocations):** After verifying `<= 30` array caps, fold the filter AST using a bounded accumulator (`<= 30` disjunctions, `<= 1` array-containment per branch): `IN` contributes unique operand count (using wire/storage equality `storage_eq` with `NaN == NaN`, keeping `1 != 1.0` and `0.0 != -0.0` distinct) with `0` array containment, `ARRAY_CONTAINS_ANY` contributes unique operand count with `1` array containment per branch, `ARRAY_CONTAINS` contributes `(1, 1)`, and remaining scalar/`NOT_IN`/unary filters contribute `(1, 0)`; `AND` multiplies disjunction counts (clamped) and sums containment counts, while `OR` sums disjunctions and takes the maximum branch containment.
3.  **Standard Duplicate `order_by` Check (Regular and `findNearest`):** Reject duplicate field references in explicit `order_by` on Standard Edition (covering regular queries and optional pre-filter/tie-breaker clauses on `findNearest`).
4.  **`findNearest` vs. Cursor Validation & Implicit `order_by` Synthesis:**
    *   When `findNearest` is present, validate the vector search spec, reject `limit`, `offset > 0`, and cursors, and on Standard Edition synthesize missing inequality fields and `__name__` into `order_by` using `last_dir` (aligning pre-filtering `OR` + inequality field-existence exclusions and tie-breaking via `order_by_cmp`).
    *   Otherwise, validate cursors *before* implicit synthesis: reject cursors exceeding explicit `order_by` value count, and across both editions require `ReferenceValue` operands and `(project_id, database_id)` root matching at explicit `__name__` cursor slots. Then synthesize missing Standard inequality fields (`<`, `<=`, `>`, `>=`, `!=`, `NOT_IN`, `IS_NOT_NULL`, `IS_NOT_NAN`) in segment-wise lexicographical path order (inheriting the last explicit sort direction, defaulting to ascending) and append trailing `__name__` (matching that direction) when absent on Standard Edition or Enterprise collection-group (`all_descendants`) queries.
5.  **Standard `__name__`-Must-Be-Last Check and Defensive Cursor Comparison:** Reject any Standard Edition `order_by` (regular or `findNearest`, safely handling empty `order_by`) where `__name__` precedes a trailing field (including `WHERE a > 0 ORDER BY __name__ FIND_NEAREST(...)`). Keep Standard `findNearest` tie-breaking aligned via `order_by_cmp` (and Enterprise via precomputed segment-wise inequality sorting), and handle missing fields in cursor boundary comparisons safely without panicking (since query prefiltering already excludes documents missing `order_by` fields).

### 5. Web UI Array Filter Generalization, Lossless Validation, and Error Diagnostics ([values.js](../../ui/values.js), [query.js](../../ui/components/query.js), [app.js](../../ui/app.js))

*   **Shared Validation Exports & `doubleValue` Context ([values.js](../../ui/values.js)):** Export the document editor's unquoted `|n| >= 2^53` integer detector (tracking JSON property context so exact IEEE-754 integers inside `"doubleValue"` are allowed while unquoted 64-bit integers in `"integerValue"` or shorthand arrays are rejected) and ProtoJSON value normalizer so Visual Builder array filters reuse them directly without code duplication, while keeping `normalizeProtoValue`'s rejection of overflowing non-finite numeric `doubleValue`s (such as `1e309` / `1e500`).
*   **Array Operator Generalization ([query.js](../../ui/components/query.js)):** Replace the special-case `"in"` filter row kind with an `"array"` operator kind shared by `IN`, `ARRAY_CONTAINS_ANY`, and `NOT_IN`, emitting the row's selected operator, initializing `[]` on operator switches into an `"array"` operator, and interpolating the active operator into validation messages.
*   **Safe Array Item Conversion & Rejections:** Run unquoted `|n| >= 2^53` integer detection *before* `JSON.parse` so large unquoted integers fail cleanly; in shorthand JS arrays (scalar primitives only), encode safe non-`-0` integers (`|n| < 2^53`) as `integerValue` strings, encode `-0`, fractions (e.g. `1.5`), and unsafe-integer floats (e.g. `1e25`) as `doubleValue` (matching existing double serialization and `-0.0` formatting), reject overflowing shorthand numeric literals (`Infinity`/`-Infinity`) consistently with `normalizeProtoValue`, reject shorthand nested JavaScript arrays (`[[1]]`) with operator-appropriate messages, and validate explicit ProtoJSON objects with `inArray = f.op === "ARRAY_CONTAINS_ANY"` so `IN`/`NOT_IN` permit single-level `{ arrayValue: { values: [...] } }` while `ARRAY_CONTAINS_ANY` disallows `arrayValue` items and doubly nested `arrayValue`s are rejected.
*   **Diagnostic Error Banners ([app.js](../../ui/app.js)):** Export the query runner action for direct regression testing and format caught API error objects as `[code status] message` so HTTP status codes and canonical gRPC status names appear directly in error banners.

### 6. Shared Raw-RPC Helpers, Rust Wire Suites, and Zero-New-Index Conformance Stratification ([common.ts](../../test-suite/tests/utils/common.ts), [structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs), [http_and_multiplex.rs](../../crates/emulator-tests/src/suites/http_and_multiplex.rs))

*   **Minimal Shared Raw-RPC Helpers ([common.ts](../../test-suite/tests/utils/common.ts), documented in [test-suite/tests/utils/README.md](../../test-suite/tests/utils/README.md)):** Extract raw `RunQuery` request-conversion and streaming helpers (composing on existing streaming RPC plumbing and issuing best-effort rollback on `CLOUD FIRESTORE` whenever a `newTransaction` stream returns a transaction token) plus gRPC status-code assertion helpers (including `referenceValue` support on raw proto values). Deduplicate the local helpers in [11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts) (keeping its custom vector listener local) and adopt the status assertion helper in [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts).
*   **In-Memory Rust Wire & HTTP Suites (`crates/emulator-tests/`):** Replace `UNIMPLEMENTED` pins and align post-synthesis cursor assertions in [structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs), and add socket-free REST `:runQuery` filter/validation tests in [http_and_multiplex.rs](../../crates/emulator-tests/src/suites/http_and_multiplex.rs).
*   **Zero-New-Index Cloud Conformance Stratification (`test-suite/tests/`):**
    1.  **Tier 1 — Server-Side Validation Rejections (0 composite indexes):** Raw-RPC and SDK tests covering array caps, empty `ArrayValue`, empty `OR`, negation exclusivity, `NOT_IN` + `OR`/`IN`/`ARRAY_CONTAINS_ANY`, Standard vs. Enterprise `__name__`/`__id<i64>__`/`order_by` boundaries, reserved/empty `FieldPath`s, and cursor errors; DNF `> 30` disjunction rejection tests span distinct fields so `3 INVALID_ARGUMENT` fires before Cloud index checks, while valid (`OK`) Standard DNF/deduplication/multi-branch containment tests target a single field in isolated subcollections (or reuse indexed `(type, ordered)` fields) and conservative same-field over-counts remain in Rust suites.
    2.  **Tier 2 — Single-Field Execution & Live/Transactional Parity (0 composite indexes):** Isolated subcollections using Cloud's automatic single-field indexes for `array-contains-any` and `not-in` across cross-type numbers/`-0.0`/`BigInt`, the full `null`/`NaN`/missing-field Standard vs. Enterprise matrix, 5,000-element Enterprise arrays, `onSnapshot` live query transitions ([5-live-queries.test.ts](../../test-suite/tests/5-live-queries.test.ts)), `count`/`sum`/`average` aggregations, and pessimistic/optimistic transactions.
    3.  **Tier 3 — Multi-Field, `collectionGroup`, and `findNearest` Pre-Filter Execution (0 new composite indexes):** Reuse only already-indexed `(type, ordered)` / `path` fields in [2-basic-query.test.ts](../../test-suite/tests/2-basic-query.test.ts) (`collection` and `subCollection`) for `NOT_IN` + `==`, `NOT_IN` + `>`, `OR` + inequality missing-field existence, and lexicographical multi-inequality `order_by`; and reuse `tags` + `category` + `vectorQueryFields.nested` (and `price` + `embedding` / Enterprise unindexed tie-breaking) in [11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts).

---

## Alternatives Considered

### Alternative A: Review and Patch PR #195 in Place

*   **Pros**: Reuses the existing `feat-array-contains-any` branch commit directly.
*   **Cons**: Most of PR #195 was superseded by value comparison unification ([value-partial-eq.md](./value-partial-eq.md)), `findNearest` vector search ([vector-search.md](./vector-search.md)), and non-panicking missing-subfield decoding / explicit-direction tail synthesis; its GitHub base branch (`feat-vector-search`) was deleted/merged; and it omits `NOT_IN`, unset-`Value` / empty-`OR` rejection, bounded DNF validation, pre-synthesis cursor and Standard `__name__`-last checks, `__name__` reference checks, Web UI array controls, black-box wire tests, and directory `README.md` updates.
*   **Reason for Rejection**: Branching cleanly from `master` while carrying `Co-authored-by: Merijn van der Linden <njirem@gmail.com>` attribution and PR #195's `array-contains-any` scenarios avoids retrofitting an outdated diff.

### Alternative B: Implement `ARRAY_CONTAINS_ANY` Alone and Defer `NOT_IN` and `order_by` Normalization

*   **Pros**: Smaller initial diff focused on a single filter operator.
*   **Cons**: `NOT_IN` is the sole remaining unimplemented `StructuredQuery` field operator and shares array-operand validation, negation exclusivity, DNF interaction rules, `__name__` reference checks, and Web UI `"array"` controls with `ARRAY_CONTAINS_ANY` and `IN`; leaving `NOT_IN` and `order_by` bugs behind forces a second pass over the same validation code paths.
*   **Reason for Rejection**: Completing both remaining `StructuredQuery` operators and their shared validation/`order_by` pipeline in one cohesive unit closes `StructuredQuery` filter parity without reworking the same files.

### Alternative C: Include Enterprise Pipeline Array and Membership Functions (`pipeline/function.rs`) in This Increment

*   **Pros**: Lifts `notImplementedInRust` guards for `array_contains`, `array_contains_any`, `array_contains_all`, `equal_any`, and `not_equal_any` in [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts) in the same PR.
*   **Cons**: Enterprise Pipeline expressions evaluate a separate `Function` AST with distinct three-valued `NULL`/missing propagation and `.isError()` semantics rather than `StructuredQuery` filter rules.
*   **Reason for Rejection**: Keeping `StructuredQuery` and Enterprise Pipeline expressions in separate increments isolates review and verification surfaces.

### Alternative D: Materialize Explicit DNF Clauses for Standard Edition Validation

*   **Pros**: Mirrors algebraic DNF expansion directly and simplifies cross-branch tautologies or overlapping same-field `IN` intersections.
*   **Cons**: Expanding nested `AND` of `OR`/`IN`/`ARRAY_CONTAINS_ANY` trees into explicit clause lists allocates `O(k^m)` heap memory and CPU (for example, nested `IN` clauses expand to millions of branches before hitting a post-expansion check) and risks breaking lazy short-circuiting if rewritten ASTs are used for evaluation.
*   **Reason for Rejection**: A single clamped DNF summary traversal enforces Standard Edition's `<= 30` disjunction and `<= 1` per-disjunction array-containment bounds in `O(|AST|)` time and `O(depth)` stack space with zero heap allocations while leaving the original filter AST untouched for short-circuit evaluation.

### Alternative E: Pre-Index Array Operands in a Search Structure on Field Filters

*   **Pros**: Logarithmic membership checks on large Enterprise arrays instead of linear scans over stored array operands.
*   **Cons**: Document queries already scan every document in the target collection, and memory/throughput benchmarking is tracked separately ([vector-search.md](./vector-search.md), [black-box-rust-testing-foundation.md](./black-box-rust-testing-foundation.md)). Embedding a pre-sorted or tree-indexed cache inside field filter nodes runs before Standard validation rejects oversized arrays, breaks plain-data encapsulation, and forces optimistic transaction tracking to clone derived index structures on every Enterprise optimistic read.
*   **Reason for Rejection**: Keeping field filter nodes as plain data evaluated directly over stored `ArrayValue` operands avoids premature complexity and clone overhead; if future benchmarks prove large Enterprise arrays are a hotspot, a post-validation shared execution index can be added without altering the filter AST model.

### Alternative F: Encode `query.proto` Docstring Inequality Restrictions Without Cloud Verification

*   **Pros**: Avoids probing live Cloud Firestore behavior across Standard and Enterprise databases.
*   **Cons**: Comments in `query.proto` are outdated—claiming inequality fields must lead `order_by` and omitting Standard vs. Enterprise divergences on empty `AND` vs. empty `OR`, 5,000-element Enterprise arrays (pre-implementation probe target pending conformance tests), `NOT_IN [null]` evaluation, and `__name__` operator rules.
*   **Reason for Rejection**: Grounding validation and evaluation rules in live `firestore-test-406503` probes and conformance tests prevents false `INVALID_ARGUMENT` rejections on valid SDK queries.

### Alternative G: Validate Query `ReferenceValue`s via the Strict Document Reference Parser

*   **Pros**: Reuses the existing strict document-reference parser with a one-line call.
*   **Cons**: The strict parser rejects all `__.*__` reserved segments—breaking valid Standard Edition Datastore numeric IDs (`__id-9223372036854775808__`, tested in [2-basic-query.test.ts](../../test-suite/tests/2-basic-query.test.ts))—and interns every parsed project, database, collection, and document ID into the global string-atom table.
*   **Reason for Rejection**: Extracting a shared internal zero-allocation segment validator lets query validation verify document paths and permit database roots plus canonical `__id<i64>__` document IDs across both editions in `O(len)` time with zero heap allocations and zero string interning, while keeping write validation strict.

### Alternative H: Build a Fluent `StructuredQuery` Proto Builder Framework (or Extract the Custom Vector Listener) in `test-suite/tests/utils/`

*   **Pros**: Provides a typed DSL for constructing raw protobuf requests and a shared bidirectional `Listen` client across test suites.
*   **Cons**: Adds significant maintenance surface in `test-suite/tests/utils/` for a bounded set of raw wire-validation tests, while `onSnapshot` already covers live `ARRAY_CONTAINS_ANY` and `NOT_IN` queries without custom `Listen` framing.
*   **Reason for Rejection**: Extracting only the shared raw-RunQuery, `NewTransaction` rollback cleanup, and status-assertion helpers eliminates local setup logic in `11-vector-search.test.ts` and duplicated status assertions in `9-pipeline-operators.test.ts` without unnecessary test-utility abstraction.

### Alternative I: Add Multi-Item Array Sub-Editors, Tab-Switch Modals, or Client-Side DNF Validation

*   **Pros**: Provides richer form controls in the browser UI.
*   **Cons**: Multi-item array sub-editors and client-side query rule duplication expand `ui/components/query.js` complexity and risk drifting from server-side edition rules.
*   **Reason for Rejection**: Generalizing `"in"` to `"array"` while reusing the document editor's integer and ProtoJSON validators and `[code status]` error banners keeps the zero-build UI minimal.

---

## Non-Functional Requirements

*   **CPU and Memory Discipline:**
    *   DNF disjunction counting runs in `O(|AST|)` time and `O(depth)` stack space using bounded integer arithmetic with zero heap allocations; query document-reference path validation runs in `O(len)` time and `O(1)` stack space with zero heap allocations and zero global string interning.
    *   Filter decoding, validation, and evaluation introduce zero extra heap allocations per field filter (reusing the decoded `Value` operand directly).
*   **Crash Resilience and Workspace Safety Lints:** Because `[profile.release]` in root [Cargo.toml](../../Cargo.toml) sets `panic = "abort"` and `[workspace.lints.rust]` sets `unsafe_code = "forbid"`, all query decoding, validation, normalization, cursor comparison, and evaluation paths must introduce zero `unsafe` blocks and return database invalid-argument errors instead of panicking on malformed protobuf or JSON inputs.
*   **Bounded Debug Output and Zero Dependency / Quota Cost:** Field-filter debug formatting caps `ArrayValue` output to element count and a bounded prefix; the design adds zero Cargo or npm dependencies and provisions zero Cloud composite indexes on `firestore-test-406503`.

## Risks & Mitigations

*   **Deeply Nested `CompositeFilter` Recursion:** Adversarial or malformed nested `CompositeFilter` payloads traverse wire decoding, DNF counting, and filter evaluation recursively. *Mitigation:* gRPC protobuf decoding rejects messages beyond 100 levels (capping nested composite filter AST depth at `~50`, 2 protobuf layers per composite filter) and REST `:runQuery` JSON parsing rejects nesting beyond 128 levels (capping composite filter AST depth at `~42`, 3 JSON containers per composite filter)—neither wire parser ceiling is currently pinned by automated tests on disk—while in-process query builder callers bypass wire parsers and remain trusted test/internal code.
*   **Conservative DNF Counting on Redundant Cross-Branch Clauses:** Bounded DNF counting deduplicates within each `IN`/`ARRAY_CONTAINS_ANY` slice and folds across `AND`/`OR` without materializing clause cross-products, trading conservative rejection of redundant cross-branch duplicates (such as 31 identical `OR` clauses or overlapping same-field `AND(a IN [1..6], a IN [2..7])`) for guaranteed `O(|AST|)` time and zero heap allocations. *Mitigation:* Affected callers can simplify redundant cross-branch clauses or use Enterprise Edition, while unit and wire suites pin this intentional bound.
*   **Skipped Local Composite Index Checks:** Standard Edition multi-field queries pass locally without `firestore.indexes.json`. *Mitigation:* Running `cd test-suite && npm test` (with `FIRESTORE_EMULATOR_HOST` unset) against live `firestore-test-406503` catches any index-requirement mismatch against real Cloud Firestore.
*   **Leaked Cloud `NewTransaction` Locks on Raw-RPC Errors:** Failing raw `RunQuery` streams carrying `newTransaction` on Cloud Firestore can leave pessimistic locks open if a transaction handle was allocated. *Mitigation:* The shared raw `RunQuery` helper in [common.ts](../../test-suite/tests/utils/common.ts) captures any streamed `transaction` token and invokes `rollback` best-effort in cleanup paths when targeting live Cloud Firestore.
*   **Array-Typed Operands on Raw Wire Queries:** Query operand validation enforces unset-`Value`, `GeoPointValue`/`TimestampValue` range, unsupported `FieldReferenceValue`/`FunctionValue`/`PipelineValue`, `ReferenceValue` document-path, and vector sentinel checks while recursing over nested `ArrayValue` items to support array-valued filters and cursors ([2-basic-query.test.ts](../../test-suite/tests/2-basic-query.test.ts) covers array comparisons, and explicit `IN [[...]]` coverage is added in `12-structured-query-validation.test.ts`). Direct nested `ArrayValue`s or empty/reserved `MapValue` keys inside raw wire operands are left unrejected pending explicit Cloud `RunQuery` probing, relying on `googleapis::Value`'s total `Ord` and `PartialEq` ([value-partial-eq.md](./value-partial-eq.md)) for panic safety.

## Additional Resources

*   Superseded community prototype: [skunkteam/rust-firestore-emulator#195](https://github.com/skunkteam/rust-firestore-emulator/pull/195) (`feat-array-contains-any`)
*   Prior workspace design docs: [value-partial-eq.md](./value-partial-eq.md), [vector-search.md](./vector-search.md), [emulator-web-ui.md](./emulator-web-ui.md), and [black-box-rust-testing-foundation.md](./black-box-rust-testing-foundation.md)
*   Firestore v1 wire schema: [google/firestore/v1/query.proto](../../crates/googleapis/include/google/firestore/v1/query.proto) (`StructuredQuery`, `Filter`, `FieldFilter`, `CompositeFilter`, `UnaryFilter`, `Order`, `Cursor`; available after submodule initialization)

## Future Improvements

*   **Enterprise Pipeline Array and Membership Functions:** Implement `array_contains`, `array_contains_any`, `array_contains_all`, `equal_any`, and `not_equal_any` in [function.rs](../../crates/emulator-database/src/database/pipeline/function.rs) and ungate their conformance tests in [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts).
*   **Post-Validation `Arc<[Value]>` Large-Array Execution Index:** If future throughput benchmarks show linear scans over 5,000-element Enterprise `IN`/`NOT_IN`/`ARRAY_CONTAINS_ANY` operands are a bottleneck, attach a post-validation `Arc<[Value]>` binary-search index during query preparation so `Query::clone()` in `OptimisticState` stays `O(1)` and invalid Standard queries incur zero sort overhead.
*   **Compact `StoredValue` Layout:** Replace in-memory `googleapis::Value` storage with compact interned/reference-counted representations during the memory-benchmarking workstream while keeping `Query::validate` and `FieldFilter::eval` semantics unchanged.

---

## Detailed Implementation Plan

### Implementation Phasing & Checkpoints

Carry `Co-authored-by: Merijn van der Linden <njirem@gmail.com>` attribution on commits incorporating `ARRAY_CONTAINS_ANY` evaluation and test scenarios from PR #195. Partition the 27 target files across four sequential phases so the workspace compiles and passes tests at every checkpoint:

| Phase | Scope | Target Files | Verification Gate |
| --- | --- | --- | --- |
| **Phase 1** | `emulator-database` reference/path validators, `Projection` checks, `FieldFilter::eval`, segment-wise `sorted_non_name_inequality_fields`, `DnfSummary`, relocated `validate_query_operand_value`, pre-synthesis cursor / `order_by` validation, and black-box gRPC wire tests (`structured_query.rs`) | Items `1–8` | `cargo nextest run --workspace --profile ci && cargo check --workspace --all-targets --all-features` |
| **Phase 2** | Black-box HTTP REST `:runQuery` tests (`http_and_multiplex.rs`), shared `ui/values.js` exports & `Infinity` normalization, Web UI `"array"` filter + `[code status]` banner updates, and `ui_regressions.rs` | Items `9–13` | `cargo nextest run --workspace --profile ci && just lint` |
| **Phase 3** | Shared raw-RPC helpers in `test-suite/tests/utils/common.ts` and Cloud-verified Jest conformance suites (`2`, `5`, `9`, `11`, `12`) | Items `14–19` | `cd test-suite && npm run lint && npx tsc -p tsconfig.json --noEmit && npm test` *(with `FIRESTORE_EMULATOR_HOST` unset against live `firestore-test-406503`)* and `just sdk-test` |
| **Phase 4** | Workspace directory `README.md` contracts + root `README.md`, full verification, and mutation audit | Items `20–27` | `just && just mutants emulator-database filter.rs && just mutants emulator-database query.rs` |

> **Workspace Lint & Visibility Guard:** Because `crates/emulator-database/src/lib.rs` re-exports `pub use database::*;`, root [Cargo.toml](../../Cargo.toml) defines `[workspace.lints.rust]` (`lines 69–83`: `unsafe_code = "forbid"`, `unreachable_pub = "warn"`, `missing_copy_implementations = "warn"`, `missing_debug_implementations = "warn"`, and `unused_qualifications = "warn"`), and [crates/emulator-database/Cargo.toml](../../crates/emulator-database/Cargo.toml) (`lines 25–26`) opts in with `[lints] workspace = true`, all internal helpers (`is_datastore_numeric_id`, `parse_query_ref_root`, `validate_query_reference`, `validate_query_operand_value`, `validate_geo_point`, `validate_timestamp`, `DnfSummary`, `sorted_non_name_inequality_fields`) must use private / `pub(super)` / `pub(crate)` visibility (never unintended `pub`), derive `Copy` and `Debug` on small value structs such as `DnfSummary`, and remove the unused `unimplemented` macro import from `filter.rs`.

### Target Files to Create/Modify

#### 1. [crates/emulator-database/src/database/reference.rs](../../crates/emulator-database/src/database/reference.rs) (`Modify`)

*   **Type / Scope**: Reference Parsing & Validation (`emulator-database` crate)
*   **Proposed Changes**:
    *   Add `pub(crate) fn is_datastore_numeric_id(s: &str) -> bool` returning `true` if and only if `s` strips prefix `"__id"` and suffix `"__"` to a slice `n` matching canonical `i64` decimal syntax (`!n.starts_with('+') && !n.starts_with("-0") && !(n.starts_with('0') && n.len() > 1) && n.parse::<i64>().is_ok()`).
    *   Extract a shared internal zero-allocation parser helper `fn parse_resource_path(s: &str, allow_datastore_id: bool) -> Option<(&str, &str, &str, usize)>` parsing `projects/{project_id}/databases/{database_id}[/documents/{rel_path}]`, verifying non-empty `project_id` and `database_id`, validating each `'/'`-separated segment in `rel_path` is non-empty, not `"."` or `".."`, and `<= 1500` bytes, rejecting `is_reserved_name(seg)` (the existing predicate in `reference.rs` matching `^__.+__$` with length `>= 5`, permitting `"____"`) on collection segments (even index) and `is_reserved_name(seg) && !(allow_datastore_id && is_datastore_numeric_id(seg))` on document segments (odd index), and returning `(project_id, database_id, rel_path, slash_count)` where `slash_count` counts `'/'` separators within `rel_path` alone.
    *   Update `Ref::from_str` (`parse_ref`, also backing `DocumentRef::from_str`) to delegate to `parse_resource_path(s, false)` so write and resource paths continue rejecting all `__.*__` segments.
    *   Add `pub(crate) fn parse_query_ref_root(s: &str) -> crate::error::Result<QueryRefMeta<'_>>` calling `parse_resource_path(s, true)`, accepting database roots (`rel_path.is_empty()` -> `has_doc_path = false`) and even-segment document paths (`slash_count % 2 == 1` -> `has_doc_path = true`, tracking `has_zero_datastore_id` when any odd segment equals `"__id0__"`, rejecting collection and odd subcollection paths), and returning `Ok(QueryRefMeta)` or `GenericDatabaseError::invalid_argument(format!("Invalid document reference: {s}"))`.
    *   Add unit tests covering valid document references, database roots (`projects/p/databases/d` and `.../documents`), canonical Datastore IDs (`__id0__`, `__id-9223372036854775808__`, `__id123__` permitted; `__id-0__`, `__id01__`, `__id-01__`, `__id00__`, `__id+1__` rejected), collection/odd-subcollection path rejection, `__custom__` rejection, and `> 1500`-byte segment rejection.
*   **Rationale**: Enforces Cloud Firestore's document-path structure, 1,500-byte segment limit, reserved-name rules, and canonical `__id<i64>__` sentinels without duplicating `Ref::from_str` path checks or `DefaultAtom` interning.

#### 2. [crates/emulator-database/src/database/field_path.rs](../../crates/emulator-database/src/database/field_path.rs) (`Modify`)

*   **Type / Scope**: Field Path & Reference Types (`emulator-database` crate)
*   **Proposed Changes**:
    *   Derive `PartialOrd, Ord` on `FieldPath(Vec<String>)` only (do **not** implement `Ord` on public `FieldReference`).
    *   Update `FieldReference::from_str` to parse `FieldPath` first and map single-segment `["__name__"]` (handling both `"__name__"` and backtick-quoted `` "`__name__`" ``) to `Self::DocumentName`.
    *   Add `pub(crate) fn validate_query_reference(&self) -> Result<()>` on `FieldReference` returning `Ok(())` for `Self::DocumentName` and calling existing `FieldPath::validate_non_reserved` (`p.validate_non_reserved()`) for `Self::FieldPath(p)`.
    *   Add unit tests verifying segment-wise `FieldPath` ordering (`a.z < a-b`, `` `a.b` `` vs. nested `a.b`), `` "`__name__`" `` mapping to `DocumentName`, and `validate_query_reference` acceptance of `__name__`, `` "`__name__`" ``, and `"____"` vs. rejection of `__custom__`, `a.__name__`, `__name__.a`, `a.__custom__`, and `a..b`.
*   **Rationale**: Provides segment-wise sorting without temporary `to_string()` allocations during comparisons, without exposing a non-Firestore `Ord` on public `FieldReference`, plus Cloud-conformant `` "`__name__`" `` parsing and reusable validation.

#### 3. [crates/emulator-database/src/database/projection.rs](../../crates/emulator-database/src/database/projection.rs) (`Modify`)

*   **Type / Scope**: Query `select` & `DocumentMask` Projection (`emulator-database` crate)
*   **Proposed Changes**:
    *   In `TryFrom<structured_query::Projection> for Projection` and `TryFrom<DocumentMask> for Projection`, invoke `field.validate_query_reference()?` on every parsed `FieldReference` before returning `Ok(Self { fields })`.
    *   Add unit tests asserting `InvalidArgument` on `__custom__`, `a.__name__`, and `a..b`, and `Ok` on `__name__`, `` "`__name__`" ``, and `"____"` across both `TryFrom` implementations.
*   **Rationale**: Enforces Cloud Firestore's `3 INVALID_ARGUMENT` (`400 Bad Request` on REST) rejection of reserved and empty field-path segments in `StructuredQuery.select` and `CreateDocument` / `UpdateDocument` / `GetDocument` / `BatchGetDocuments` / `ListDocuments` masks; because `Projection.fields` is private and `TryFrom` is its sole external constructor, `Query::validate` does not re-check `select`.

#### 4. [crates/emulator-database/src/database/vector.rs](../../crates/emulator-database/src/database/vector.rs) (`Modify`)

*   **Type / Scope**: Vector Distance & Search Scoring Helpers (`emulator-database` crate)
*   **Proposed Changes**:
    *   Remove `pub(crate) fn validate_query_operand_value` (`lines 115–140`), `query_operand_vector_validation` (`lines 567–625`, a single `#[gtest]` function containing 6 assertion checks), and the now-unused `VectorFormatError` import (`line 4`, retaining `VectorClassification` and `classify_vector_map` which remain used by `validate_query_vector`), relocating operand validation and tests into `query.rs` and `query/tests.rs` under Items `6` and `7`.
*   **Rationale**: Eliminates `query -> vector -> reference` coupling when expanding operand validation to unset `Value`s and `ReferenceValue` document paths, keeping `vector.rs` scoped strictly to vector math and `findNearest` scoring.

#### 5. [crates/emulator-database/src/database/query/filter.rs](../../crates/emulator-database/src/database/query/filter.rs) (`Modify`)

*   **Type / Scope**: `StructuredQuery` Filter AST, Wire Decoding, Segment Sorting, `Debug`, and Predicate Evaluation (`emulator-database` crate)
*   **Proposed Changes**:
    *   Remove the `unimplemented` macro import from `crate`.
    *   Derive `PartialEq, Eq` on `CompositeOperator`, `FieldOperator`, and `UnaryOperator`.
    *   Add `pub(super) const fn wire_name(self) -> &'static str` mapping all 10 `FieldOperator` variants to uppercase protobuf `field_filter::Operator` names (`"LESS_THAN"`, `"LESS_THAN_OR_EQUAL"`, `"GREATER_THAN"`, `"GREATER_THAN_OR_EQUAL"`, `"EQUAL"`, `"NOT_EQUAL"`, `"ARRAY_CONTAINS"`, `"IN"`, `"ARRAY_CONTAINS_ANY"`, `"NOT_IN"`), add `pub(super) const fn is_array_operand(self) -> bool` (`In | ArrayContainsAny | NotIn`), and narrow `FieldOperator::needs_type_compat` from `{LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Equal}` to `LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual` (because `Equal` delegates to `value_matches_eq` without type-compatibility gating, while `LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual | NotEqual | NotIn` plus unary `IsNotNan | IsNotNull` remain the inequality operators returned by `sorted_non_name_inequality_fields`).
    *   Keep `FieldFilter { pub field: FieldReference, pub op: FieldOperator, pub value: Value }` as a plain data struct and keep `Filter::try_from`, `CompositeFilter::try_from`, `FieldFilter::try_from`, and `UnaryFilter::try_from` restricted to missing-subfield wire errors and enum decoding (preserving `"missing filter_type in Filter"`, `"missing field in FieldFilter"`, `"missing value in FieldFilter"`, `"missing operand_type in UnaryFilter"`, and `use itertools::Itertools;` for `.try_collect()?`).
    *   Upgrade `Filter::sorted_non_name_inequality_fields(&self) -> Vec<FieldReference>` (`lines 29–42`) to collect `FieldPath` values (filtering out `DocumentName`), `sort()`, `dedup()` via `FieldPath::Ord`, and map to `Vec<FieldReference>` (removing `.unique()` and `to_string().split('.')`).
    *   Replace `#[derive(Debug)]` on `FieldFilter` with a manual `fmt::Debug` implementation formatting `field`, `op`, `array_len` (`values.len()`), and `array_prefix` (`&values[..values.len().min(3)]`) when `self.value` is an `ArrayValue`, and `field`, `op`, `value` otherwise.
    *   Implement `fn value_matches_eq(lhs: &Value, rhs: &Value, is_enterprise: bool) -> bool` (`(is_enterprise || (!lhs.is_null() && !lhs.is_nan() && !rhs.is_null() && !rhs.is_nan())) && lhs == rhs`), `FieldFilter::operand_contains(&self, cand: &Value, is_enterprise: bool) -> bool` (`(!is_enterprise && (cand.is_null() || cand.is_nan()))` returns `false`, otherwise `self.value.as_array().is_some_and(|arr| arr.iter().any(|rhs| value_matches_eq(cand, rhs, is_enterprise)))`), and explicit `FieldFilter::eval` arms across all 10 `FieldOperator`s:
        *   Missing field: `Ok(is_enterprise && matches!(self.op, NotEqual | NotIn))`.
        *   Present field:
            *   `LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual`: require `value.is_compatible_with(&self.value) && (is_enterprise || (!value.is_null() && !value.is_nan() && !self.value.is_null() && !self.value.is_nan()))` before comparing via `<`, `<=`, `>`, `>=` (since `is_compatible_with` returns `true` for `(Null, Null)` and `(NaN, NaN)`).
            *   `Equal`: `value_matches_eq(value, &self.value, is_enterprise)`.
            *   `NotEqual`: `(is_enterprise || (!value.is_null() && !self.value.is_null())) && !value_matches_eq(value, &self.value, is_enterprise)`.
            *   `ArrayContains`: `value.as_array().is_some_and(|arr| arr.iter().any(|el| value_matches_eq(el, &self.value, is_enterprise)))`.
            *   `In`: `self.operand_contains(value, is_enterprise)`.
            *   `ArrayContainsAny`: `value.as_array().is_some_and(|arr| arr.iter().any(|el| self.operand_contains(el, is_enterprise)))`.
            *   `NotIn`: `(is_enterprise || (!value.is_null() && !self.value.as_array().is_some_and(|arr| arr.iter().any(Value::is_null)))) && !self.operand_contains(value, is_enterprise)`.
    *   Keep existing `FieldFilter { ... }` struct literals in `filter.rs::tests` (`lines 473–518`) unchanged and add unit tests covering `ArrayContainsAny` and `NotIn`, the Standard vs. Enterprise `null`/`NaN`/missing-field matrix (including `< null` and `< NaN`), cross-type `0`/`-0.0` and `42`/`42.0` array matching, segment-wise `sorted_non_name_inequality_fields` (with escaped `.`), and bounded `fmt::Debug` formatting.
*   **Rationale**: Completes `ARRAY_CONTAINS_ANY` and `NOT_IN` evaluation, fixes Standard `null`/`NaN` and string-split sorting mismatches without breaking `FieldFilter`'s plain-data model, and bounds debug log size.

#### 6. [crates/emulator-database/src/database/query.rs](../../crates/emulator-database/src/database/query.rs) (`Modify`)

*   **Type / Scope**: Unified Runtime Validation, `order_by` Normalization, Cursor Safety, and `findNearest` Tie-Breaking (`emulator-database` crate)
*   **Proposed Changes**:
    *   Keep `Order::try_from` limited to `"order_by without field"` wire decoding (`order.field` is validated in `Query::validate`; note `QueryBuilder` in `query.rs` constructs `Query` directly in-process without `TryFrom`).
    *   Move `validate_query_operand_value(value: &Value, enterprise_edition: bool) -> Result<()>` from `vector.rs` into `query.rs` (private `fn`), calling `googleapis::classify_vector_map` for `__vector__` sentinel maps and adding:
        *   `None => Err(GenericDatabaseError::invalid_argument("Cannot convert firestore.v1.Value with type unset."))`
        *   `Some(ValueType::ReferenceValue(s)) => reference::parse_query_ref_root(s).map(|_| ())`
        *   `Some(ValueType::GeoPointValue(geo)) => validation::validate_geo_point(geo)`
        *   `Some(ValueType::TimestampValue(ts)) => validation::validate_timestamp(ts)`
        *   `Some(ValueType::FieldReferenceValue(_) | ValueType::FunctionValue(_) | ValueType::PipelineValue(_)) => Err(GenericDatabaseError::invalid_argument("Value type is not supported in StructuredQuery operands."))`
    *   Add `#[derive(Clone, Copy, Debug)] struct DnfSummary { disjunctions: u8, max_array_contains: u8 }` (`const MAX_STANDARD_DISJUNCTIONS: u8 = 30; const DNF_DISJUNCTION_CAP: u8 = 31; const MAX_ARRAY_CONTAINS_CAP: u8 = 2;`):
        *   `fn unique_value_count(values: &[Value]) -> u8`: pairwise wire/storage equality count (`value_dnf_dedup_eq`: `(a.is_nan() && b.is_nan()) || a.storage_eq(b)`, keeping `1 != 1.0` and `0.0 != -0.0` distinct) clamped before casting on validated non-empty `<= 30`-element slices.
        *   Leaf arms: `Filter::Unary(_) => DnfSummary { disjunctions: 1, max_array_contains: 0 }`, `FieldOperator::ArrayContains => DnfSummary { disjunctions: 1, max_array_contains: 1 }`, `FieldOperator::In => DnfSummary { disjunctions: unique_value_count(arr), max_array_contains: 0 }`, `FieldOperator::ArrayContainsAny => DnfSummary { disjunctions: unique_value_count(arr), max_array_contains: 1 }`, and all other `FieldOperator`s (including `NotIn`) `=> DnfSummary { disjunctions: 1, max_array_contains: 0 }`.
        *   `CompositeOperator::And`: fold from `DnfSummary { disjunctions: 1, max_array_contains: 0 }` via `disjunctions: acc.disjunctions.saturating_mul(child.disjunctions).min(31)` and `max_array_contains: acc.max_array_contains.saturating_add(child.max_array_contains).min(2)`.
        *   `CompositeOperator::Or`: fold from `DnfSummary { disjunctions: 0, max_array_contains: 0 }` via `disjunctions: acc.disjunctions.saturating_add(child.disjunctions).min(31)` and `max_array_contains: acc.max_array_contains.max(child.max_array_contains)`.
    *   Refactor `Query::validate(&mut self) -> Result<()>` (`lines 417–521`) into one unified sequence (noting that Cloud Firestore validation errors refer to `__name__` as `__key__` / `the key`, and `self.parent.root()` returns `&RootRef { project_id, database_id }` from `reference.rs`):
        1. Validate `!matches!(self.parent, Ref::Collection(_))` (`"parent must point to a Document or Root"`) and `!self.from.is_empty()` (`"Query FROM is mandatory"`). (`self.select` is already validated by `Projection::try_from`.)
        2. Validate `order.field.validate_query_reference()?` across `self.order_by`, and in a single recursive walk over `self.filter` check: `CompositeOperator::Or` non-empty (`"Composite filter must have at least one sub-filter."`); `FieldReference::validate_query_reference()?` on every `FieldFilter` and `UnaryFilter`; for `In | ArrayContainsAny | NotIn`, non-`ArrayValue` (`format!("'{}' requires an ArrayValue.", ff.op.wire_name())`) and empty `ArrayValue` (`format!("'{}' requires a non-empty ArrayValue.", ff.op.wire_name())`); and `validate_query_operand_value(&ff.value, enterprise_edition)?` (and across all `start_at`/`end_at` cursor values).
        3. When `!enterprise_edition`, enforce Standard filter constraints via one shared helper across regular and `find_nearest` queries (running structural/cap checks first so `DnfSummary` sees validated `<= 30`-element arrays):
            *   Array length caps: `In` `> 30` (`"'IN' supports up to 30 comparison values."`), `ArrayContainsAny` `> 30` (`"'ARRAY_CONTAINS_ANY' supports up to 30 comparison values."`), `NotIn` `> 10` (`"'NOT_IN' supports up to 10 comparison values."`).
            *   Negation count `> 1` across `NotEqual | NotIn | IsNotNan | IsNotNull` -> `"Only a single 'NOT_EQUAL', 'NOT_IN', 'IS_NOT_NAN', or 'IS_NOT_NULL' filter allowed per query."`.
            *   `NotIn` combined with `CompositeOperator::Or | FieldOperator::In | FieldOperator::ArrayContainsAny` -> `"'NOT_IN' cannot be used in the same query with 'IN', 'ARRAY_CONTAINS_ANY' or 'OR'."`.
            *   `__name__` rules: any `UnaryFilter` on `DocumentName` -> `"__key__ filter value must be a Key"`; `FieldFilter` on `DocumentName` with `ArrayContains | ArrayContainsAny` -> `"the name __key__ is reserved"`; scalar `__name__` filters require a non-empty document `ReferenceValue` (`has_doc_path == true`, otherwise `"Key path is empty."`), reject `__id0__` (`has_zero_datastore_id == true` -> `"Document ID of type long must be non-zero"`), and check `let root = self.parent.root(); (meta.project_id, meta.database_id) == (root.project_id.as_ref(), root.database_id.as_ref())` (`"__key__ filter reference must match the target project and database"`); `In | NotIn` on `DocumentName` require every array element to pass the same `ReferenceValue` and root checks.
            *   `DnfSummary` (second pass after the `<= 30` array caps above): `max_array_contains > 1` -> `"A maximum of 1 'ARRAY_CONTAINS' filter is allowed per disjunction."`; `disjunctions > 30` -> `"Too many disjunctions after normalization; maximum is 30."`.
        4. When `!enterprise_edition`, reject duplicate `FieldReference` entries in explicit `self.order_by` (`format!("order by clause cannot contain duplicate fields: {}", order.field)`).
        5. When `self.find_nearest` is `Some(spec)`, run `spec.validate(enterprise_edition)?`, reject `limit.is_some()`, `offset > 0`, and `start_at`/`end_at`, and when `!enterprise_edition` call `synthesize_standard_order_by(last_dir)`. When `None`, validate `start_at`/`end_at` before synthesis (`cursor.values.len() > self.order_by.len()` -> `"Cursor has too many values."`; for each `(order, cursor_val)` where `order.field.is_document_name()`, require `ReferenceValue` -> `"Cursor __key__ value is not a document reference."` and across both editions require `let root = self.parent.root(); (p, d) == (root.project_id.as_ref(), root.database_id.as_ref())` -> `"The cursor key is in a different database than the query"`), then run `synthesize_standard_order_by(last_dir)` on `!enterprise_edition` or append `DocumentName` on Enterprise `all_descendants` queries.
        6. When `!enterprise_edition`, reject `self.order_by` if any non-final element is `DocumentName`: `if self.order_by.split_last().is_some_and(|(_, prefix)| prefix.iter().any(|o| o.field.is_document_name()))` -> `"order by clause cannot contain more fields after the key"`.
    *   In `Query::once_with_strategy`, precompute `enterprise_tie_fields` once before `select_and_sort_nearest`; in `find_nearest_tie_cmp`, delegate Standard Edition to `order_by_cmp` (with an allocation-free `DocumentName` fast path) and Enterprise Edition to `enterprise_tie_fields` then `cmp_firestore_order`. Return `bool` from `Filter::eval` and `Query::matches_prefilters`.
    *   In `doc_on_left_of_cursor` (`line 920`), replace `.expect(...)` on `order.field.get_value(doc)` with `let Some(doc_value) = order.field.get_value(doc) else { return false; };`.
*   **Rationale**: Consolidates all query validation into one non-panicking pipeline with `O(|AST|)` filter/DNF traversal, zero `u8` overflow, zero per-comparison vector tie-break allocations, and safe empty-`order_by` handling.

#### 7. [crates/emulator-database/src/database/query/tests.rs](../../crates/emulator-database/src/database/query/tests.rs) (`Modify`)

*   **Type / Scope**: `QueryBuilder` & Operand Unit Tests (`emulator-database` crate)
*   **Proposed Changes**:
    *   Keep existing `FieldFilter { ... }` struct literals intact.
    *   Relocate `query_operand_vector_validation` (with its 6 assertion checks) from `vector.rs` and add unit tests for recursive unset `Value::default()`, out-of-range `GeoPointValue`/`TimestampValue`, unsupported `FieldReferenceValue`/`FunctionValue`/`PipelineValue`, malformed `ReferenceValue` rejection (collection paths, `__custom__`) inside `ArrayValue` and `MapValue`, `docsRoot`, and cross-edition `__id-9223372036854775808__`.
    *   Update `implicit_order_by_matches_firestore_semantics` case 5 (`lines 452–475`): add explicit `.order_by("a".parse()?, Direction::Ascending).order_by("__name__".parse()?, Direction::Ascending)` for the valid 2-value cursor `[10, document-1]`, and assert that omitting explicit `order_by` with that 2-value cursor fails with `InvalidArgument("Cursor has too many values.")`.
    *   Keep unit additions focused on internal/white-box invariants rather than duplicating the full wire validation matrix in `structured_query.rs`: `DnfSummary` (`u8`-overflow `16 * 16 = 256` regression, `unique_value_count` `1 != 1.0` / `0.0 != -0.0` / `NaN == NaN`, `OR` with one `ArrayContains` per branch vs. `AND` with two, conservative same-field over-count checks), `` "`__name__`" `` mapping to `DocumentName`, Standard `find_nearest` inequality synthesis (`ORDER BY __name__` rejection, `ORDER BY b DESC` tie-breaking, `OR` + inequality missing-field exclusion), explicit `orderBy('a', DESC)` rejecting a 2-value cursor without explicit `__name__`, and escaped-dot segment sorting (`` `a.b` `` vs. `a.b`).
*   **Rationale**: Aligns existing cursor tests with Cloud Firestore's pre-synthesis cursor limit, covers relocated operand validation, and pins internal `DnfSummary` and `QueryBuilder` unit invariants without duplicating black-box suites.

#### 8. [crates/emulator-tests/src/suites/structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs) (`Modify`)

*   **Type / Scope**: In-Memory gRPC Black-Box Wire Suite (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Replace `unimplemented_field_filter_operators_return_unimplemented` (`lines 231–248`) with end-to-end gRPC tests covering `ARRAY_CONTAINS_ANY` and `NOT_IN` across `RunQuery`, `RunAggregationQuery` (`count`, `sum`, `avg`), `Listen` (`listen_query` / `Listener::process_event` initial and incremental updates), pessimistic and optimistic transactions, and Standard vs. Enterprise `null`, `NaN`, missing-field, cross-type numeric, `IN [[...]]`, `< null`/`< NaN`, and 5,000-element Enterprise array semantics.
    *   Extend `run_query_rejects_malformed_filter_subfields_with_invalid_argument` (`lines 378–437`) with raw gRPC malformed-wire tests verifying `Code::InvalidArgument` (and zero panics):
        *   **Wire & Array Bounds:** Unset `Value::default()`, out-of-range `GeoPointValue`/`TimestampValue`, and unsupported `FieldReferenceValue`/`FunctionValue`/`PipelineValue` in `FieldFilter.value`, `ArrayValue.values`, and `Cursor.values`, non-array/empty `ArrayValue` on `IN`/`ARRAY_CONTAINS_ANY`/`NOT_IN`, Standard `> 30` / `> 10` caps, and malformed `Listen` targets.
        *   **Disjunction & Negation Rules:** Empty `OR` (`InvalidArgument`) vs. empty `AND` (`Ok`), negation exclusivity (`!=`, `NOT_IN`, `IS_NOT_NULL`, `IS_NOT_NAN`), `NOT_IN` combined with `OR`/`IN`/`ARRAY_CONTAINS_ANY`, and `DnfSummary` bounds (including `AND` of two 16-element `IN` filters = `256` disjunctions, `int` vs. `double` / `+0.0` vs. `-0.0` storage-equality DNF rejections, and conservative same-field over-counts).
        *   **`__name__` & Reserved Paths:** `__name__` unary/array-contains operator errors, non-`ReferenceValue` and wrong-database `__name__` operands (scalar and inside `IN`/`NOT_IN`), canonical `__id0__`/`__id-9223372036854775808__` acceptance vs. `__id-0__`/`__id01__`/`__id-01__` rejection across both editions, `` "`__name__`" `` mapping to `DocumentName`, and reserved/empty `FieldPath`s across filters, `Order`, `Projection`, and `GetDocumentRequest.mask`.
        *   **`order_by`, `findNearest` & Cursors:** Standard duplicate `order_by`, `__name__`-not-last (`ORDER BY __name__, a`, `WHERE a > 0 ORDER BY __name__`, and `WHERE a > 0 ORDER BY __name__ FIND_NEAREST(...)`), `findNearest` `ORDER BY b DESC` / `OR` + inequality pre-filtering, and non-`ReferenceValue` / wrong-db `__name__` cursor slots.
    *   Update `structured_query_implicit_order_by_and_cursor_tie_breakers` (`lines 441–579`) cases 1 (`lines 462–474`) and 4 (`lines 504–523`) so valid 2-value cursors specify explicit 2-field `order_by` (`order_desc("tie").order_desc("__name__")` and `order_asc("a").order_asc("__name__")`), while omitting explicit `__name__` (only `order_desc("tie")` or omitted `order_by`) asserts `Code::InvalidArgument` (`"Cursor has too many values."`).
*   **Rationale**: Replaces obsolete `Code::Unimplemented` pins, aligns cursor wire tests with Cloud Firestore, and verifies crash-free validation over gRPC without TCP sockets.

#### 9. [crates/emulator-tests/src/suites/http_and_multiplex.rs](../../crates/emulator-tests/src/suites/http_and_multiplex.rs) (`Modify`)

*   **Type / Scope**: In-Memory HTTP REST Black-Box Suite (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Add a socket-free `http_oneshot` test exercising `POST /v1/{DOCUMENTS_ROOT}:runQuery` across Standard and Enterprise modes with `IN`, `ARRAY_CONTAINS_ANY`, and `NOT_IN` (verifying `i64` vs. `-0.0` double array operands, missing-field/`null` edition differences, empty `AND` returning `200 OK`, and empty `OR` / Standard `> 10` `NOT_IN` / reserved paths returning `400 Bad Request` with `"status": "INVALID_ARGUMENT"`).
*   **Rationale**: Closes the missing REST `:runQuery` black-box test gap on the exact HTTP JSON endpoint consumed by the Web UI.

#### 10. [ui/values.js](../../ui/values.js) (`Modify`)

*   **Type / Scope**: Web UI Value & ProtoJSON Codec Helpers (ES Module with `// @ts-check`)
*   **Proposed Changes**:
    *   Add `export` to `findUnsafeUnquotedInteger(rawText)` (`line 607`) and `normalizeProtoValue(rawVal, pathLabel, inArray)` (`line 747`), and update `findUnsafeUnquotedInteger` to track the preceding JSON property key so exact IEEE-754 integers inside `"doubleValue"` (such as `{"doubleValue": 9007199254740992}`) are allowed while unquoted `|n| >= 2^53` integers in `"integerValue"` or shorthand arrays remain rejected, and keep `normalizeProtoValue`'s rejection of overflowing numeric `doubleValue` literals (`1e309`, `1e500`).
*   **Rationale**: Shares unquoted `|n| >= 2^53` integer precision detection and ProtoJSON object normalization with [query.js](../../ui/components/query.js) while rejecting overflowing numeric literals consistently across both the document editor and query builder.

#### 11. [ui/components/query.js](../../ui/components/query.js) (`Modify`)

*   **Type / Scope**: Web UI Structured Query Runner Component (ES Module with `// @ts-check`)
*   **Proposed Changes**:
    *   Import `findUnsafeUnquotedInteger` and `normalizeProtoValue` from `../values.js`.
    *   Update `FILTER_OPERATORS` JSDoc comment (`lines 21–24`) and entries (`line 37`): change `IN` from `kind: "in"` to `kind: "array"`, and add `{ op: "ARRAY_CONTAINS_ANY", label: "array-contains-any (JSON array)", kind: "array" }` and `{ op: "NOT_IN", label: "not-in (JSON array)", kind: "array" }`.
    *   In `buildStructuredQueryFromVisual` (`lines 162–200`):
        *   Branch on `opMeta.kind === "array"`, run `const unsafeLiteral = findUnsafeUnquotedInteger(f.valueText);` before `JSON.parse(f.valueText)` (returning `` { valid: false, error: `Unquoted 64-bit integer ${unsafeLiteral} in ${f.op} filter for "${fp}" loses precision in JSON.parse; wrap in quotes inside ProtoJSON integerValue` } `` matching `ui/values.js:671`), and interpolate `${f.op}` in parse, non-array, and item error messages.
        *   Convert `parsedArr` items with an indexed `for (let i = 0; i < parsedArr.length; i++)` loop pushing onto `values`: `null`/`boolean`/`string` to `{ nullValue: null }` / `{ booleanValue: item }` / `{ stringValue: item }`; `typeof item === "number"` rejects non-finite overflowing literals (`!Number.isFinite(item)`) with an error directing the user to `{"doubleValue": "Infinity"}` / `"-Infinity"` (consistent with `normalizeProtoValue`), and maps `Number.isSafeInteger(item) && !Object.is(item, -0)` to `{ integerValue: String(item) }` and remaining finite numbers to `{ doubleValue: item }` (`-0` serializes as `-0.0` via `stringifyProtoJson`).
        *   Reject nested JS arrays before `typeof item === "object"` (using `"Nested arrays are not supported..."` for `ARRAY_CONTAINS_ANY` and instructing explicit `{"arrayValue": {"values": [...]}}` for `IN`/`NOT_IN`).
        *   Validate object items via `const res = normalizeProtoValue(item, \`${fp}[${i}]\`, f.op === "ARRAY_CONTAINS_ANY"); if (!res.valid || !res.value) return { valid: false, error: res.error }; values.push(res.value);` (allowing single-level `{ arrayValue: { values: [...] } }` operands on `IN`/`NOT_IN` while rejecting `arrayValue` objects on `ARRAY_CONTAINS_ANY`, doubly nested `arrayValue`s, and non-`Value` objects), and emit `op: f.op` in `fieldFilter`.
    *   In `renderFilterRow` (`lines 695–738`): check `nextMeta?.kind === "array"` on operator `<select>` change to initialize `f.valueText = "[]"` when `!f.valueText.trim().startsWith("[")`, and render the JSON-array `<input>` when `kind === "array"`.
*   **Rationale**: Enables `ARRAY_CONTAINS_ANY` and `NOT_IN` in the visual query builder while fixing hardcoded `"IN"` serialization, unquoted `|n| >= 2^53` precision loss (without blocking valid `"doubleValue"` numbers), scientific-notation integer serialization, `-0` coercion, overflowing number handling, invalid object pass-through, and nested-array pass-through.

#### 12. [ui/app.js](../../ui/app.js) (`Modify`)

*   **Type / Scope**: Web UI Root Controller (ES Module with `// @ts-check`)
*   **Proposed Changes**:
    *   Add `export` to `export async function executeCurrentQuery()` (`line 749`) and, in its `catch (err)` block (`line 799`), format `err instanceof ApiError` as `` `[${err.code} ${err.status}] ${err.message}` `` before falling back to `err instanceof Error ? err.message : String(err)`.
*   **Rationale**: Surfaces HTTP status codes and canonical Firestore status names (such as `[400 INVALID_ARGUMENT]`) in query error banners and makes `executeCurrentQuery` directly testable from Node.

#### 13. [crates/emulator-http/tests/ui_regressions.rs](../../crates/emulator-http/tests/ui_regressions.rs) (`Modify`)

*   **Type / Scope**: Web UI Node ES-Module Regression Suite (`emulator-http` integration test)
*   **Proposed Changes**:
    *   Add a Node test `query_builder_array_operators_validation_and_error_banners` verifying `buildStructuredQueryFromVisual` across `IN`, `ARRAY_CONTAINS_ANY`, and `NOT_IN` (safe integer vs. `-0.0`, float, `{"doubleValue": 9007199254740992}`, explicit `{"doubleValue": "Infinity"}` / `"-Infinity"` serialization via `stringifyProtoJson`, consistent overflow rejection on `[1e309]`, `[-1e309]`, and `[{"doubleValue": 1e309}]`, unquoted `|n| >= 2^53` rejection outside `doubleValue` via `findUnsafeUnquotedInteger`, nested JS array `[[1]]` vs. `ARRAY_CONTAINS_ANY` `arrayValue` rejection, invalid vs. valid ProtoJSON object validation via `normalizeProtoValue`) and `await executeCurrentQuery()` producing `[400 INVALID_ARGUMENT]` in `state.queryState.error` (`app.js`).
*   **Rationale**: Extends the existing Node ES-module UI regression harness to cover array operators and error banner formatting.

#### 14. [test-suite/tests/utils/common.ts](../../test-suite/tests/utils/common.ts) (`Modify`)

*   **Type / Scope**: Shared TypeScript Conformance Test Utilities
*   **Proposed Changes**:
    *   Import `Query` and `VectorQuery` types from `@google-cloud/firestore`.
    *   Export shared raw-RPC interfaces `RawProtoValue` (including `nullValue`, `booleanValue`, `integerValue`, `doubleValue`, `stringValue`, `referenceValue?: string`, `timestampValue?: { seconds?: number | string; nanos?: number }`, `bytesValue?: Uint8Array | string`, `geoPointValue?: { latitude?: number; longitude?: number }`, `arrayValue`, `mapValue`), `RawDocument`, `RawRunQueryRequest`, and `RawRunQueryResponseChunk`.
    *   Export `toRunQueryProto(query: Query | VectorQuery, transaction?: Uint8Array): RawRunQueryRequest` delegating to the SDK's internal `toProto(transaction)` method.
    *   Extend `callRawStreamRpc` with an optional `onChunk?: (chunk: TChunk) => void` hook (`lines 54–82`), and export `rawRunQuery(firestore: Firestore, req: RawRunQueryRequest, tag = 'runQuery'): Promise<RawRunQueryResponseChunk[]>` wrapping `callRawStreamRpc(firestore, 'runQuery', req, tag, onChunk)` to capture any non-empty `chunk.transaction` and—when `connection === 'CLOUD FIRESTORE'` (the suite connection target flag in `common.ts`) and `req.newTransaction` was sent—derive `const database = req.parent.split('/documents')[0]` and automatically issue `callRawUnaryRpc(firestore, 'rollback', { database, transaction }).catch(() => undefined)` in a `finally` cleanup block (covering both stream errors and streams that resolve unexpectedly).
    *   Export `expectRpcCode(fn: () => Promise<unknown>, expectedCode: number, label = 'rpc'): Promise<void>` asserting exact gRPC status codes with diagnostic failure output.
*   **Rationale**: Centralizes raw-RPC query helpers and automatic Cloud `NewTransaction` rollback cleanup without duplicating `requestStream` / `stream.on(...)` listener plumbing (`utils/index.ts` already re-exports `* from './common'`).

#### 15. [test-suite/tests/9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts) (`Modify`)

*   **Type / Scope**: Enterprise Pipeline Conformance Suite
*   **Proposed Changes**:
    *   Import `expectRpcCode` from `./utils` and replace the manual `try / catch` status-code assertion loop in the raw `executePipeline` stage-validation test (`lines 2224–2239`) with `await expectRpcCode(..., 3, item.label)`.
*   **Rationale**: Eliminates duplicated status-code assertion setup using the shared utility.

#### 16. [test-suite/tests/11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts) (`Modify`)

*   **Type / Scope**: Vector Search Conformance Suite
*   **Proposed Changes**:
    *   Remove only the extracted/obsolete definitions (`lines 24–49` for `RawProtoValue`, `RawDocument`, `RawRunQueryRequest`, `RawRunQueryResponseChunk`; `lines 76–78` for `InternalVectorQueryProtoCarrier`; `lines 85–87` for `toRunQueryProto`; and `lines 110–181` for `expectRpcCode` and `expectRunQueryNewTxnError`), keeping `RawAggregationResponseChunk` (`51–55`), `RawBeginTransactionResponse` (`57–59`), `RawListenResponse` (`61–74`), `InternalListenClient` (`80–83`), `docNameFromParent` (`89–92`), `shortDocId` (`94–97`), `createVectorMap` (`99–108`), and `RawVectorListener` local, while importing `rawRunQuery`, `toRunQueryProto`, `expectRpcCode`, and raw types from `./utils`.
    *   Add conformance tests reusing existing Cloud composite vector indexes:
        *   Cross-edition (`editions`) `findNearest` pre-filtered by `.where('category', '==', 'books').where('tags', 'array-contains-any', ['ml', 'go'])` using `vectorField: new FieldPath('vectorQueryFields', 'nested')`, 3D `queryVector: [1, 1, 0]` (reusing `tags CONTAINS + category ASC + 3D vectorQueryFields.nested`).
        *   Cross-edition (`editions`) repeated single-field inequality `find_nearest_tie_cmp` tie-breaking (`where('price', '<', 100).where('price', '>', 5)` without `orderBy`) using `price ASC + 2D embedding`.
        *   Enterprise-only (`if (fs.enterprise)`) `findNearest` pre-filtered by `not-in`, plus unsorted multi-field inequality `find_nearest_tie_cmp` tie-breaking (`where('b', '>', 0).where('a', '>', 0)` matching `where('a', '>', 0).where('b', '>', 0)`).
        *   Standard-only (`if (!fs.enterprise)`) `findNearest` validation rejections (`rawRunQuery` + `expectRpcCode(..., 3)`) for `> 30` DNF disjunctions and `NOT_IN` + `OR`.
*   **Rationale**: Deduplicates raw-RPC helpers without breaking local vector/aggregation/transaction helpers, and verifies `ARRAY_CONTAINS_ANY`, `NOT_IN`, DNF bounds, and canonical tie-breaking on `findNearest` queries using only existing Cloud composite vector indexes.

#### 17. [test-suite/tests/2-basic-query.test.ts](../../test-suite/tests/2-basic-query.test.ts) (`Modify`)

*   **Type / Scope**: Core `StructuredQuery` Conformance Suite
*   **Proposed Changes**:
    *   Add `Filter` to the `@google-cloud/firestore` import (`lines 1–13`).
    *   Inside the existing `collection` and `collectionGroup('subCollection')` suite (reusing already-seeded `storedTestData` and `nothing` fixtures in `2-basic-query.test.ts`, plus existing `(type, ordered)` and `path` Cloud indexes in `test-suite/firestore.indexes.json`), add tests for `where('type', 'not-in', ['string', 'bytes'])`, `where('ordered', 'not-in', [-500]).where('type', '==', 'number')`, `where('type', 'not-in', ['none']).where('ordered', '>', 0)`, `Filter.or(Filter.where('type', '==', 'none'), Filter.where('ordered', '>', 2))` (verifying `nothing` is excluded on Standard via synthesized `order_by` existence and included on Enterprise without explicit `orderBy`), and lexicographical implicit `orderBy` (`where('type', '>', 'A').where('ordered', '>', 0)` matching `where('ordered', '>', 0).where('type', '>', 'A')`).
    *   Extend `comparisonEdgeCases` with `not-in` and `array-contains-any` checks across `0` / `-0`, `1` / `1.0`, `NaN`, and `{ nested: NaN }`.
*   **Rationale**: Verifies multi-field and `collectionGroup` execution against live Cloud Firestore with zero new composite indexes and zero extra `beforeAll` fixture writes.

#### 18. [test-suite/tests/5-live-queries.test.ts](../../test-suite/tests/5-live-queries.test.ts) (`Modify`)

*   **Type / Scope**: Real-Time `onSnapshot` Query Conformance Suite
*   **Proposed Changes**:
    *   Extend top-level `listen(query, options?)` (`lines 204–232`) with an optional transform/raw option (making the `expect(included).toBeTrue()` assertion and `omit(rest, ['number'])` projection opt-out rather than adding a second `onSnapshot` collector helper), and add a standalone `describe('array-contains-any and not-in live queries', ...)` block verifying initial snapshots, live document additions, in-place field updates entering/leaving the match set, batch writes, deletions, and `.limit(3)` window updates for `where('tags', 'array-contains-any', ['alpha', 'beta']).orderBy(FieldPath.documentId()).limit(3)` and `where('status', 'not-in', ['archived', 'deleted']).orderBy('status').limit(3)`.
*   **Rationale**: Verifies `Listener` initial snapshot and incremental update behavior for `ARRAY_CONTAINS_ANY` and `NOT_IN` across all editions with deterministic `.limit(3)` ordering using automatic single-field Cloud indexes (zero composite indexes).

#### 19. [test-suite/tests/12-structured-query-validation.test.ts](../../test-suite/tests/12-structured-query-validation.test.ts) (`Create`)

*   **Type / Scope**: `ARRAY_CONTAINS_ANY`, `NOT_IN`, and Wire-Validation Conformance Suite
*   **Proposed Changes**:
    *   Parameterize over `editions` ([test-suite/tests/utils/README.md](../../test-suite/tests/utils/README.md)) using isolated single-field subcollections (`fs.collection.doc().collection(...)`):
        1. **Execution, `null`/`NaN`/Missing Matrix, Large Enterprise Arrays, Aggregations & Transactions:**
            *   **Matrix & Types:** Verify `array-contains-any` and `not-in` across scalar/array/map types, nested `IN [[...]]` array operands, `BigInt` `2^53 + 1` precision, and the full `null`/`NaN`/missing-field Standard vs. Enterprise matrix (`NOT_IN [1]`, `NOT_IN [null]`, `NOT_IN [1, null]`, `NOT_IN [NaN]`, `IN [null]`, `ARRAY_CONTAINS` and `ARRAY_CONTAINS_ANY` with top-level `null` and `NaN`, raw `FieldFilter` `==`/`!=`/`<` with `null` and `NaN`).
            *   **Large Arrays, Aggregations & Concurrency:** Verify 5,000-element `IN`, `NOT_IN`, and `ARRAY_CONTAINS_ANY` on `fs.enterprise` against live Cloud Firestore; `count()`, `sum()`, and `average()` aggregations; and pessimistic/optimistic `runTransaction` reads (write-skew retry and lock behavior).
        2. **Server-Side Runtime Validation Matrix (`rawRunQuery` + `expectRpcCode(..., 3)` vs. `OK`, gating confirmed Java emulator gaps with `notImplementedInJava`):**
            *   **Wire & Array Bounds:** Missing `Filter.filter_type` / `FieldFilter.field` / `FieldFilter.value` / `UnaryFilter.operand_type`, unset `Value` operands (`FieldFilter.value`, `ArrayValue.values`, `Cursor.values`), non-array and empty `[]` operands on `IN`/`ARRAY_CONTAINS_ANY`/`NOT_IN`, and Standard `30`/`30`/`10` caps (including duplicate elements).
            *   **Disjunction & Negation Rules:** Empty `OR` (`3`) vs. empty `AND` (`OK`), negation exclusivity (`!=`, `NOT_IN`, `IS_NOT_NULL`, `IS_NOT_NAN`), `NOT_IN` combined with `OR`/`IN`/`ARRAY_CONTAINS_ANY`, valid (`OK`) single-field `OR` with one `ARRAY_CONTAINS`/`ARRAY_CONTAINS_ANY` per branch vs. `AND` (`3`), valid single-field `30`-disjunction `OR`/`IN` deduplication (`OK`), and distinct-field `6 * 6 = 36` / `16 * 16 = 256` disjunction rejections (`3`, failing before Cloud composite-index checks; conservative same-field over-counts remain in Rust tests).
            *   **`__name__` & Reserved Paths (Standard & Enterprise Cloud Parity):** `__name__` allowed/disallowed operators across both editions, non-`ReferenceValue` (scalar and inside `IN`/`NOT_IN`), wrong project/database, collection, root, and odd-segment subcollection-collection path rejections, canonical `__id0__` / `__id-9223372036854775808__` acceptance vs. `__id-0__` / `__id01__` / `__id-01__` rejection across both editions, backtick-quoted `` "`__name__`" `` resolved as `DocumentName` across queries and masks, and reserved (`__custom__`, `a.__name__`) / empty (`a..b`) `FieldPath`s across `FieldFilter`, `UnaryFilter`, `Order`, `select`, and `GetDocument` `mask`.
            *   **`order_by` & Cursors (Standard & Enterprise Cloud Parity):** Duplicate `order_by` and `__name__`-not-last (`ORDER BY __name__, a`, `WHERE a > 0 ORDER BY __name__`) tested across both Standard and Enterprise Cloud, `DESC` tie-breaking on `__name__`, explicit-`order_by` cursor length bounds (including `orderBy('a', DESC)` rejecting a 2-value cursor without explicit `__name__`), and `__name__` cursor `ReferenceValue` / root checks (exercising automatic `newTransaction` rollback cleanup).
*   **Rationale**: Codifies the execution and validation ruleset in `test-suite/` conformance tests with zero new Cloud composite indexes and verifies live Standard/Enterprise edition parity.

#### 20. [crates/emulator-database/src/database/query/README.md](../../crates/emulator-database/src/database/query/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-database/query`)
*   **Proposed Changes**:
    *   Update `[filter.rs:Filter]` (`sorted_non_name_inequality_fields` using segment-wise `FieldPath::Ord` instead of string splitting), `[filter.rs:FieldFilter]` (bounded `fmt::Debug` formatting `array_len` and 3-element `array_prefix`), `[find_nearest.rs]` (Standard `order_by` synthesis + `order_by_cmp` tie-breaking), and replace the obsolete unimplemented `ArrayContainsAny`/`NotIn` invariant and `stderr` side-effect bullets with `value_matches_eq` and `NotIn` missing/`null`/`NaN` semantics across editions.
*   **Rationale**: Keeps `query/README.md` synchronized with `filter.rs` and `find_nearest.rs` interfaces, invariants, and side effects.

#### 21. [crates/emulator-database/src/database/README.md](../../crates/emulator-database/src/database/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-database/database`)
*   **Proposed Changes**:
    *   Update `[field_path.rs]` (`FieldPath: Ord`, `` "`__name__`" `` mapping to `DocumentName`, `FieldReference::validate_query_reference`), `[projection.rs]` (reserved/empty path rejection across `Projection` and `DocumentMask`), `[reference.rs]` (canonical `is_datastore_numeric_id`, shared zero-allocation `parse_resource_path` helper, and `parse_query_ref_root`), `[validation.rs]` (`validate_geo_point`, `validate_timestamp`), `[vector.rs]` (moving `validate_query_operand_value` out of `vector.rs` into `query.rs`), and `[query.rs:Query::validate]` (`DnfSummary` with storage-equality `unique_value_count`, non-empty `OR`, non-empty `ArrayValue`, unset-`Value` / `GeoPointValue` / `TimestampValue` / unsupported `ValueType` and `ReferenceValue` validation, Standard array/negation/`NOT_IN`/`__name__` rules, duplicate `order_by` rejection, explicit-`order_by` cursor checks, `last_dir` + segment-wise `sorted_non_name_inequality_fields` across regular and `find_nearest` Standard queries, and `__name__`-last enforcement).
*   **Rationale**: Preserves accurate subsystem contracts in `database/README.md`.

#### 22. [ui/README.md](../../ui/README.md) (`Modify`)

*   **Type / Scope**: Web UI Package Documentation (`ui`)
*   **Proposed Changes**:
    *   Update `[app.js]` (`line 12`) to list `executeCurrentQuery`, update `[api.js:ApiError]` (`line 13`) fields from `(httpStatus, rpcStatus, message)` to `(code, status, message)`, and update `[values.js]` (`lines 17–20`) to include `stringifyProtoJson`, `findUnsafeUnquotedInteger` (allowing exact `doubleValue` numbers), and `normalizeProtoValue` (rejecting non-finite numeric `doubleValue` overflows).
*   **Rationale**: Keeps `ui/README.md` synchronized with exported UI helpers and actual `ApiError` fields.

#### 23. [ui/components/README.md](../../ui/components/README.md) (`Modify`)

*   **Type / Scope**: UI Component Documentation (`ui/components`)
*   **Proposed Changes**:
    *   Update `[query.js]` interface (`line 9`) and `[query.js:FILTER_OPERATORS]` invariant (`line 16`) to list all 14 supported operators (including `ARRAY_CONTAINS_ANY` and `NOT_IN` under `kind: "array"`), `findUnsafeUnquotedInteger` / `normalizeProtoValue` reuse, `Number.isSafeInteger` / `-0` / `"Infinity"` / `"-Infinity"` encoding, and nested JS array rejection.
*   **Rationale**: Removes the outdated statement that `ARRAY_CONTAINS_ANY` and `NOT_IN` are omitted from `FILTER_OPERATORS`.

#### 24. [test-suite/tests/utils/README.md](../../test-suite/tests/utils/README.md) (`Modify`)

*   **Type / Scope**: Conformance Harness Utility Documentation (`test-suite/tests/utils`)
*   **Proposed Changes**:
    *   Document the optional `onChunk` callback on `callRawStreamRpc`, plus `RawProtoValue`, `RawDocument`, `RawRunQueryRequest`, `RawRunQueryResponseChunk`, `toRunQueryProto`, `rawRunQuery` (and its automatic Cloud `rollback` side effect deriving database root via `req.parent.split('/documents')[0]` on `newTransaction` streams), and `expectRpcCode` under `[common.ts]`.
*   **Rationale**: Synchronizes `test-suite/tests/utils/README.md` with the extracted raw-RPC helpers.

#### 25. [test-suite/tests/README.md](../../test-suite/tests/README.md) (`Modify`)

*   **Type / Scope**: Conformance Suite Catalog Documentation (`test-suite/tests`)
*   **Proposed Changes**:
    *   Update `[2-basic-query.test.ts]` (`line 10`, noting `not-in`, `OR` + inequality, and canonical multi-inequality `orderBy` alongside existing `array-contains-any`), `[5-live-queries.test.ts]`, and `[11-vector-search.test.ts]` descriptions, add `[12-structured-query-validation.test.ts]`, and update the suite range in `Invariants` / `Side Effects` (`lines 28, 32`).
*   **Rationale**: Keeps `test-suite/tests/README.md` synchronized with the suite files.

#### 26. [test-suite/README.md](../../test-suite/README.md) (`Modify`)

*   **Type / Scope**: Package Documentation (`test-suite`)
*   **Proposed Changes**:
    *   Update the numbered test suite range in `Interface` (`line 21`) from `1-get-set.test.ts` through `11-vector-search.test.ts` to `1-get-set.test.ts` through `12-structured-query-validation.test.ts`.
*   **Rationale**: Prevents stale file-range references in `test-suite/README.md`.

#### 27. [README.md](../../README.md) (`Modify`)

*   **Type / Scope**: Root System Blueprint
*   **Proposed Changes**:
    *   Remove ``- Some field filters (such as `ArrayContainsAny` and `NotIn`)`` from `### Missing Features` (`line 21`).
*   **Rationale**: Reflects full `StructuredQuery` field-filter operator support in the top-level blueprint.

### Verification Plan

See [test-suite/README.md](../../test-suite/README.md) and [README.md](../../README.md) for prerequisite tooling (`just`, `cargo +nightly`, `cargo-nextest`, `cargo-llvm-cov`, `cargo-mutants`, `node`, `npm install`, GCP Application Default Credentials for live `firestore-test-406503`, Firebase CLI / Java runtime for Java emulator comparison, and git submodule initialization for `query.proto`). All post-implementation acceptance checks below apply upon completion:

*   **Compilation & Feature-Matrix Builds:**
    *   `cargo build` (default `ui` feature) and `cargo build --no-default-features` must exit `0` with zero warnings.
*   **Rust Unit, Property, Doc, HTTP/UI & Black-Box Wire Tests:**
    *   `just test` (`cargo nextest run --workspace` + `cargo test --doc --workspace --exclude googleapis`), `cargo nextest run --workspace --profile ci`, `cargo nextest run -p emulator-http --test ui_regressions`, and `cargo nextest run --workspace --exclude emulator-tests --no-default-features --profile ci` must exit `0` and pass.
*   **Static Analysis, Formatting, TypeScript & Web UI Type-Checking:**
    *   `cargo +nightly fmt --all --check` (run `just fmt` only when intending to apply formatting in place via `cargo +nightly fmt --all`), `just lint` (`cargo clippy --workspace --all-targets --all-features -- -D warnings` + `just ui-check` via `tsc` strict JSDoc check on `ui/app.js`), `cargo clippy --workspace --all-targets --no-default-features -- -D warnings`, and `cd test-suite && npm run lint && npx tsc -p tsconfig.json --noEmit` (`eslint.config.cjs` + `prettier -c` + `tsc`) must exit `0` with zero warnings or errors.
*   **Line/Region Coverage & Scoped Mutation Testing:**
    *   `just cov` (`cargo llvm-cov nextest --workspace`) must exit `0` (`justfile` configures no `--fail-under-*` threshold), with manual inspection of its report confirming reachable new lines and regions in `filter.rs`, `query.rs`, `field_path.rs`, `projection.rs`, and `reference.rs` are covered (excluding unreachable defensive fallbacks such as `doc_on_left_of_cursor`'s `else { return false; }`).
    *   `just mutants emulator-database filter.rs` and `just mutants emulator-database query.rs` (scratch output under `./tmp/mutants`) must exit `0`, confirming `emulator-database` unit assertions (excluding `proptest_*`/`stress_*`/`multithreaded*` under `NEXTEST_PROFILE=mutants` and excluding `crates/emulator-tests`) catch operator/boundary mutations.
*   **End-to-End SDK Conformance (Ground-Truth Cloud, Rust Emulator & Java Comparison):**
    *   `cd test-suite && npm test` (with `FIRESTORE_EMULATOR_HOST` unset, targeting live GCP project `firestore-test-406503` across Firestore database IDs `(default)`, `enterprise`, and `enterprise-pessimistic`) must exit `0` and pass with zero new Cloud composite indexes.
    *   `just sdk-test` (fresh `cargo build --no-default-features` + `npm run test:rust-emulator`) must exit `0` and pass.
    *   `cd test-suite && npm run test:java-emulator` must exit `0` and pass (gating any confirmed Java emulator gaps with `notImplementedInJava` after execution).
    *   `just` (full workspace gate: `test` + `lint` + `sdk-test`) must exit `0`.

---

<!-- Provenance: Indicates initial structural drafting via EGM. -->

*Drafted with EGM (Elephant-Goldfish Model)*
