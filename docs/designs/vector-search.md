# `VectorValue` and `findNearest` Vector Search

*   **Author:** Paco van der Linden
*   **Status:** Draft
*   **Date:** 2026-11-12

---

## Overview

`rust-firestore-emulator` is an in-memory Cloud Firestore emulator organized into workspace crates for protobuf types and codecs ([crates/googleapis/Cargo.toml](../../crates/googleapis/Cargo.toml)), database storage and query execution ([crates/emulator-database/Cargo.toml](../../crates/emulator-database/Cargo.toml)), the Firestore v1 gRPC server ([crates/emulator-grpc/Cargo.toml](../../crates/emulator-grpc/Cargo.toml)), the HTTP/REST server ([crates/emulator-http/Cargo.toml](../../crates/emulator-http/Cargo.toml)), and an embedded browser document inspector ([ui/README.md](../../ui/README.md)). In production, Cloud Firestore offers **Standard Edition** (index-backed `StructuredQuery` execution with pessimistic transactions by default) and **Enterprise Edition** (`StructuredQuery` plus index-free multi-stage `ExecutePipeline`, supporting optimistic and pessimistic concurrency modes). Locally, `rust-firestore-emulator` emulates both editions via in-memory scans verified against live Cloud Firestore in [test-suite/README.md](../../test-suite/README.md).

This design specifies Firestore `VectorValue` storage, validation, ordering, and `findNearest` vector search across Standard and Enterprise editions, superseding community contribution [PR #194](https://github.com/skunkteam/rust-firestore-emulator/pull/194). On the Firestore v1 protobuf and ProtoJSON wire, a `VectorValue` is encoded as a reserved two-key `MapValue` (`{"__type__": "__vector__", "value": ArrayValue([DoubleValue(...), ...])}`) holding 1 to 2,048 non-`NaN` 64-bit floats, while behaving at runtime as an atomic scalar value sorted between `Array` and `Map` (`Array < Vector < Map`). The specification aligns write validation, cross-type ordering, distance math (`EUCLIDEAN`, `COSINE`, `DOT_PRODUCT`), `StructuredQuery.findNearest` execution (across `RunQuery`, `Listen`, `RunAggregationQuery`, and transactions), Enterprise Pipeline vector functions and the `find_nearest` stage, and REST/Web UI pass-through with live Cloud Firestore.

## The Problem

Applications that use Cloud Firestore vector embeddings and `findNearest` search in production cannot run local integration tests against `rust-firestore-emulator`:

1.  **Rejected Vector Queries and Pipeline Operations:**
    *   In [query.rs](../../crates/emulator-database/src/database/query.rs), `Query::from_structured` rejects `StructuredQuery.find_nearest` with `UNIMPLEMENTED`.
    *   In [function.rs](../../crates/emulator-database/src/database/pipeline/function.rs) and [mod.rs](../../crates/emulator-database/src/database/pipeline/mod.rs), Enterprise Pipeline vector expressions (`cosine_distance`, `euclidean_distance`, `dot_product`, `vector_length`) and the `find_nearest` stage return `UNIMPLEMENTED` (`GenericDatabaseError::NotImplemented`). In [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts), those vector expression tests remain gated behind `notImplementedInRust` (the conformance suite's skip flag for features not yet supported by the Rust emulator).
    *   In [lib.rs](../../crates/emulator-grpc/src/lib.rs), `execute_pipeline` packs every result document into a single `ExecutePipelineResponse` message. Because one 2,048-dimension vector encodes to ~22 KiB on the protobuf wire, returning ~185 unprojected vector documents exceeds the 4 MiB default inbound gRPC frame limit in `@grpc/grpc-js` (`8 RESOURCE_EXHAUSTED`), whereas Cloud Firestore streams results across multiple frames.
2.  **Wire-Map Representation Treated as a Mutable User Map:**
    *   In [value_ext.rs](../../crates/googleapis/src/value_ext.rs) and [field_path.rs](../../crates/emulator-database/src/database/field_path.rs), `Value::as_map` and `Value::as_map_mut` expose `__vector__` maps as ordinary user maps. As a result, `orderBy` ranks vectors as maps instead of sorting `Array < Vector < Map` by `(dimension_len, elements)`, while field-path operations (`embedding.value`, `embedding.__type__`, `embedding.nested`) traverse or mutate internal sentinel keys instead of treating vectors atomically.
    *   In [database.rs](../../crates/emulator-database/src/database.rs), `validate_writable_fields` does not validate map keys or `__vector__` payloads, and `perform_write` does not validate `update_mask` or `FieldTransform` paths/operands against reserved names or malformed vector maps. Invalid writes (reserved `^__.+__$` keys of `len >= 5`, extraneous keys, empty or `> 2048`-dimension vectors, non-`DoubleValue` elements, or `NaN` coordinates) succeed locally even though Cloud Firestore rejects them with `INVALID_ARGUMENT`.
3.  **Web UI and REST Side Effects on Vector-Bearing Documents:**
    *   Even when vector search runs exclusively over gRPC, developers inspect and edit scalar fields on those documents in the embedded Web UI over REST `GET` and `PATCH` ([projects.rs](../../crates/emulator-http/src/routes/projects.rs)). The Web UI offers two modes: **Structured mode**, a form/tree editor over `DraftNode` state diffed against `cleanDoc` (the last-loaded server document) to compute top-level `PATCH updateMask` paths, and **Raw JSON mode**, a full-document JSON text editor.
    *   On the server, `MapValue` serializes over a randomized `HashMap` ([build.rs](../../crates/googleapis/build.rs)), while in [editor.js](../../ui/components/editor.js) `fieldsToDraftNodes` sorts keys alphabetically and dirty/mask diffing runs `JSON.stringify` on every keystroke and save. Editing an unrelated field stringifies ~100 KiB per character typed, marks untouched vector fields dirty when key order differs, normalizes `-0.0` to `0` whenever a top-level field or shared parent container is re-serialized through `JSON.stringify`, and expands a 2,048-dimension vector into >20,000 DOM nodes.
    *   Meanwhile, [ser.rs](../../crates/googleapis/src/ser.rs) rejects REST `:runQuery` `findNearest` inside `serde::Deserialize`, returning `400 Bad Request` instead of `501 Not Implemented`.

## Goals

*   **Canonical `VectorValue` Semantics and Trait Coherence:** Recognize valid `__vector__` maps (`fields.len() == 2`, `"__type__": "__vector__"`, `"value": ArrayValue` of `1..=2048` non-`NaN` `DoubleValue` elements) in one canonical classifier, enforce `Array < Vector < Map` and `(dimension_len, element_by_element)` ordering, treat vectors as atomic non-maps across path traversal, handle missing `value_type` without panicking, and preserve `(a == b) == (a.cmp(&b) == Ordering::Equal)` alongside zero-allocation `O(K)` equality for regular maps ([value-partial-eq.md](./value-partial-eq.md)).
*   **Cloud-Parity Write, Mask, and Transform Validation:** Reject malformed `__vector__` payloads (extraneous keys, missing `"value"` array, zero length, `> 2048` dimensions, non-`DoubleValue` elements, `NaN` elements) and empty or reserved `^__.+__$` (`len >= 5`, allowing `"____"`) field names across `CreateDocument`, `UpdateDocument`, `BatchWrite`, and `Commit` (`doc.fields`, `update_mask`, and `FieldTransform` paths/operands) with `INVALID_ARGUMENT`, while accepting `±Infinity` coordinates and `VectorValue` elements nested inside `ArrayValue`.
*   **`StructuredQuery.findNearest` Across Standard and Enterprise Editions:** Support `EUCLIDEAN`, `COSINE`, and `DOT_PRODUCT` nearest-neighbor queries with `FindNearest.limit` (`1..=1000`), inclusive `distance_threshold`, pre-projection `distance_result_field` injection, `select` masking, `where` and explicit `order_by` pre-filters (with distance tie-breaking via explicit `order_by` + direction-matched `__name__` on Standard when `order_by` is non-empty, and inequality filter fields `ASC` + `__name__ ASC` otherwise across Standard and Enterprise), `all_descendants` collection-group scans, Standard skipping of non-finite (`±Infinity`) candidates plus Standard implicit `< +Infinity` (`EUCLIDEAN`/`COSINE`) / `> -Infinity` (`DOT_PRODUCT`) distance bounds when `distance_threshold` is omitted, edition-aware zero-norm `COSINE` rejection (`9 FAILED_PRECONDITION` on Standard, `3 INVALID_ARGUMENT` on Enterprise), validation of `__vector__` `FieldFilter`/`Cursor` query operands, and rejection of outer `StructuredQuery.limit`, `offset`, and cursors.
*   **Support Across `Listen`, `RunAggregationQuery`, and Transactions:** Support `Listen` (`onSnapshot`) `findNearest` evaluation on initial `AddTarget` catch-up and reconnect/`Lagged` resets (while keeping `find_nearest` attached to the `Query` for `distanceResultField` injection and threshold/error checks, and delivering subsequent mutations matching base pre-filters incrementally as `DocumentChange`/`DocumentRemove`/`DocumentDelete`), `RunAggregationQuery` (`COUNT`, `SUM`, `AVG`, with Enterprise empty-set `SUM` returning `NULL_VALUE`), and enforce edition-aware transaction concurrency: lock returned top-`K` matched documents on Standard Edition pessimistic transactions, lock all scanned collection documents on Enterprise Edition (pessimistic concurrency) transactions, and verify result-set stability at commit time on optimistic transactions.
*   **Enterprise Edition Pipeline Vector Support:** Implement the four pipeline vector expressions (`cosine_distance`, `euclidean_distance`, `dot_product`, `vector_length`, enabling all gated vector tests in [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts)), implement the `find_nearest` pipeline stage (`limit` and `distance_field` options, rejecting unknown options with `INVALID_ARGUMENT`), and stream `ExecutePipelineResponse` frames in `<= 1 MiB` chunks.
*   **Targeted CPU and Allocation Discipline:** Keep `googleapis::google::firestore::v1::Value` as the workspace value representation while iterating vector coordinates directly from `&[Value]` without per-document `Vec<f64>` allocations, selecting top-`K` neighbors via linear `select_nth_unstable_by` partitioning, carrying computed distances alongside matched document versions without cloning unprojected vector fields, and bounding query debug output to vector dimensionality and the first 3 coordinates.
*   **Lossless REST and Web UI Document Pass-Through:** Serialize `Document.fields` and `MapValue.fields` in deterministic sorted-key order, render `__vector__` maps in Structured mode as collapsed read-only summary rows with structural diffing so editing unrelated top-level fields neither stalls the browser nor mutates `-0.0` (preserving nested `-0.0` vectors as well when sibling fields in the same parent container are untouched), and return HTTP `501 Not Implemented` for REST `:runQuery` `findNearest`.
*   **Conformance, PR #194 Baseline, and Canary Test Coverage:** Carry forward every SDK and Rust scenario from [PR #194](https://github.com/skunkteam/rust-firestore-emulator/pull/194), verify parity against the live Cloud Firestore test project across Standard, Enterprise (optimistic), and Enterprise (pessimistic) databases, and guard upstream Cloud Firestore feature restrictions (outer `StructuredQuery.limit`, non-zero `offset`, or cursors combined with `findNearest`, and pipeline `find_nearest` `threshold` / `distance_threshold` options) with canary assertions (`expectCloudLimitationCanary`, which passes while Cloud rejects a combination and fails loudly if Cloud unlocks it).

## Non-Goals

*   **REST `:runQuery` Execution of `StructuredQuery.findNearest`:** REST endpoints in this repository exist only to back the embedded Web UI, which does not execute vector queries; `:runQuery` with `findNearest` returns `501 Not Implemented`.
*   **Web UI Vector Query Builder or Vector Creation/Editing Controls:** Beyond collapsed read-only pass-through for existing `__vector__` fields, building UI controls to author or edit vector embeddings or run nearest-neighbor searches is out of scope.
*   **Workspace-Wide `StoredValue` Refactor, MVCC Version Pruning, and 100k-Document Stress Tests:** Replacing `googleapis::google::firestore::v1::Value` with a custom in-memory AST or custom `prost::Message`, pruning historical MVCC versions, and running 100,000-document scale benchmarks are deferred to a separate memory-benchmarking workstream.
*   **Simulating Deploy-Time Vector Index Management:** Ingesting `firestore.indexes.json`, exposing index admin RPCs, or failing Standard Edition queries with `FAILED_PRECONDITION` when a vector index is undeclared is excluded by design.

## Open Questions

None.

## Critical User Journeys (CUJs)

### CUJ 1: Atomic `VectorValue` Storage, Validation, and Cross-Type Ordering

*   **Given**: A Firestore database in `rust-firestore-emulator` (Standard or Enterprise edition).
*   **When**: A client writes documents containing `FieldValue.vector(...)` at top-level, nested map, and `ArrayValue` element paths (including `-0.0` and `±Infinity` coordinates) and runs `orderBy("val")` across mixed field types (`Array`, `VectorValue` of varying lengths, and `Map`). The client also attempts invalid writes (`0` dimensions, `2,049` dimensions, non-`DoubleValue` or `NaN` coordinates, missing `"value"` or extraneous keys in a `__vector__` map, or reserved `^__.+__$` keys of length `>= 5` across `CreateDocument`, `UpdateDocument`, `BatchWrite`, or `Commit`) and updates a subpath beneath an existing vector (`embedding.nested = "replaced"`).
*   **Then**:
    *   Valid vectors (`1..=2048` non-`NaN` `f64` elements, preserving `-0.0` bits) and `"____"` keys round-trip intact.
    *   Invalid vector payloads and empty or reserved `^__.+__$` path segments fail at write time with `3 INVALID_ARGUMENT`.
    *   Subpath updates overwrite the `embedding` field atomically rather than mutating `__type__` or `value`.
    *   `orderBy("val")` sorts `Array < Vector < Map`, ordering vectors by dimension count first and element-by-element second.

### CUJ 2: `StructuredQuery.findNearest` Search Across `RunQuery`, `Listen`, `RunAggregationQuery`, and Transactions

*   **Given**: A collection (or `all_descendants` collection group) populated with documents containing embeddings of varying dimensions, non-vector values, and scalar metadata fields, with zero index configuration in the emulator.
*   **When**: A client executes `findNearest` via `RunQuery`, `Listen`, or `RunAggregationQuery` using `EUCLIDEAN`, `COSINE`, or `DOT_PRODUCT`. Queries optionally combine `where(...)` and `orderBy(...)` pre-filters, `distanceThreshold`, `distanceResultField`, `select(...)` field masks, or gRPC transaction consistency selectors (`Transaction`, `NewTransaction`, `ReadTime`).
*   **Then**:
    *   Non-vector fields, missing fields (including missing `orderBy` fields), and mismatched-dimension vectors are silently skipped, and Standard Edition skips non-finite (`±Infinity`) candidate vectors while applying an implicit `< +Infinity` (`EUCLIDEAN`/`COSINE`) or `> -Infinity` (`DOT_PRODUCT`) distance bound when `distanceThreshold` is omitted.
    *   Candidate distances match direct unscaled IEEE 754 `f64` Cloud Firestore results: identical finite vectors evaluate to exact `0` under `COSINE`, `1e-200` Euclidean underflows to `0` while `1e100` evaluates to `1e100` (and Enterprise evaluates `1e200`/`+Infinity` Euclidean to `+Infinity`), finite zero-norm `COSINE` query/candidate vectors return `9 FAILED_PRECONDITION` (`"Cannot compute cosine distance against a vector with a magnitude of zero."`) on Standard Edition and `3 INVALID_ARGUMENT` on Enterprise Edition, and any other `NaN` distance evaluation returns `3 INVALID_ARGUMENT` (`"<measure>_distance(...) result is NaN."`).
    *   `distanceThreshold` filters inclusively (`<=` for `EUCLIDEAN`/`COSINE`, `>=` for `DOT_PRODUCT`), and equal distances tie-break via explicit `order_by` followed by direction-matched segment-wise `__name__` on Standard Edition when `order_by` is non-empty, or via inequality filter fields `ASC` followed by segment-wise `__name__ ASC` otherwise (Standard without explicit `order_by` and Enterprise Edition).
    *   `distanceResultField` shadows colliding ancestor/descendant fields consistently across `select(...)` projection and `RunAggregationQuery` (`COUNT`, `SUM`, `AVG` — with Enterprise empty-set `SUM` returning `NULL_VALUE` vs. `0` on Standard) without altering subsequent reads of the stored document.
    *   `Listen` evaluates `findNearest` (including `distanceResultField` and initial errors) during initial `AddTarget` catch-up and reconnect/`Lagged` resets while keeping `findNearest` on the `Query`, and streams subsequent updates incrementally using base pre-filters (`where` / `order_by` existence) via `DocumentChange`/`DocumentRemove`/`DocumentDelete`; transactions enforce edition-specific lock (top-`K` on Standard pessimistic vs. full scanned collection on Enterprise pessimistic) or commit-time `ABORTED` validation scopes.
    *   Invalid query parameters (`FindNearest.limit` outside `1..=1000`, `NaN` threshold, zero-norm `COSINE` query vector, outer `StructuredQuery.limit`, non-zero `offset`, or cursors) fail cleanly (`3 INVALID_ARGUMENT`, or `9 FAILED_PRECONDITION` for Standard zero-norm `COSINE`).

### CUJ 3: Enterprise Edition Pipeline Vector Expressions, `find_nearest` Stage, and Chunked Streaming

*   **Given**: An Enterprise Edition database in `rust-firestore-emulator` containing documents with vector embeddings (including batches exceeding 4 MiB of serialized vector payloads).
*   **When**: A client executes `.pipeline()` queries evaluating `cosineDistance`, `euclideanDistance`, `dotProduct`, and `vectorLength` (including error-wrapping via `.isError()`), or running `.collection(...).findNearest(...)` with `limit` and `distanceField` options.
*   **Then**:
    *   All vector expression tests in [9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts) pass.
    *   Missing or `null` inputs return `NullValue`, while non-vector types, dimension mismatches, and `NaN` distance evaluations surface as catchable `InvalidArgument` errors under `isError()` without masking `NotImplemented` errors.
    *   The `find_nearest` stage ranks matching documents, requires `FieldReferenceValue` (`field`, optional `distance_field`) and a `__vector__` `MapValue` query vector, and rejects raw `ArrayValue`/`StringValue` stage operands, unsupported options (such as `threshold` or `distance_threshold`), or invalid `limit` values with `3 INVALID_ARGUMENT`.
    *   `ExecutePipelineResponse` streams results lazily in `<= 1 MiB` frames (attaching `transaction` on the first frame only) without exceeding the 4 MiB client frame limit.

### CUJ 4: Lossless Web UI Inspection and Scalar Edits on Vector Documents

*   **Given**: A document written via gRPC containing a 2,048-dimension `VectorValue` (including `-0.0` float coordinates) alongside top-level scalar fields such as `title: "draft"`.
*   **When**: A developer opens the document in the embedded Web UI in Structured mode, edits `title` to `"published"`, and clicks `Save Changes`.
*   **Then**:
    *   The Web UI renders the vector field as a single collapsed read-only summary row (`vector (2048 dims) [v0, v1, v2, …]`) without expanding 2,048 child inputs or running `JSON.stringify` per keystroke.
    *   The resulting `PATCH` `updateMask.fieldPaths` contains only `title`.
    *   Untouched top-level vectors (and nested vectors whose sibling fields in the same container are untouched) retain their exact `-0.0` coordinates in Structured mode, whereas switching to Raw JSON mode formats the full document as standard JSON text.

### CUJ 5: Live Cloud Conformance and Limitation Canary Detection

*   **Given**: The live Cloud Firestore test project provisioned on `--database="(default)"` with the four Standard Edition composite vector indexes—`collection` (`2D` `embedding`, `COLLECTION` scope), `collection` (`price ASC + 2D embedding`, `COLLECTION` scope), `collection` (`tags CONTAINS + category ASC + 3D vectorQueryFields.nested`, `COLLECTION` scope), and `collection` (`runId ASC + 2D tieEmbedding`, `COLLECTION_GROUP` scope)—alongside the index-free `enterprise` and `enterprise-pessimistic` databases wired in [firestore.ts](../../test-suite/tests/utils/firestore.ts).
*   **When**: A maintainer runs the Jest conformance suite against live Cloud Firestore and against the Rust emulator.
*   **Then**: All scoped vector search and validation tests pass identically on live Cloud (Standard and Enterprise) and the Rust emulator. Limitation canaries pass while Cloud rejects unsupported combinations and fail with an explicit `"CANARY UNLOCKED"` diagnostic if Google lifts a restriction—prompting maintainers to implement emulator parity and remove the canary.

## Technical Plan (Architecture)

The design spans five workspace boundaries while keeping `googleapis::google::firestore::v1::Value` as the sole in-memory value representation. Vector classification, ordering, and deterministic ProtoJSON map serialization live in `crates/googleapis`; write validation, distance math, `StructuredQuery.findNearest`, and Enterprise Pipeline execution live in `crates/emulator-database`; response streaming and transaction error cleanup live in `crates/emulator-grpc`; and REST/UI pass-through lives in `crates/emulator-http` and `ui/`.

```text
+-----------------------------------------------------------------------------------+
|                               Client / Test Suite                                 |
|   @google-cloud/firestore v9.4.0 (gRPC RunQuery, Listen, Aggregation, Pipeline)   |
|   Embedded Web UI (REST GET / PATCH documents, collapsed read-only VectorValue)   |
+------------------------+-----------------------------------------+----------------+
                         | gRPC (Protobuf)                         | HTTP (ProtoJSON)
                         v                                         v
+----------------------------------------+   +--------------------------------------+
|          crates/emulator-grpc          |   |         crates/emulator-http         |
| - run_query / listen / aggregation     |   | - GET / PATCH pass-through (50 MiB)  |
| - execute_pipeline (<= 1 MiB lazy      |   | - POST :runQuery findNearest -> 501  |
|   chunks, tx ID on frame 0 only)       |   +------------------+-------------------+
| - NewTransaction rollback on error     |                      |
+-------------------+--------------------+                      |
                    |                                           |
                    +---------------------+---------------------+
                                          v
+-----------------------------------------------------------------------------------+
|                             crates/emulator-database                              |
|                                                                                   |
|  +-------------------------------+    +----------------------------------------+  |
|  |     database/validation.rs    |    |           database/vector.rs           |  |
|  | - classify_vector_map errors  |    | - DistanceMeasure (EUCLIDEAN, COSINE,  |  |
|  | - commit / perform_write mask |    |   DOT_PRODUCT) + IEEE 754 f64 + 0-norm |  |
|  |   & FieldTransform checks     |    | - select_and_sort_nearest (partition   |  |
|  | - reference::is_reserved_name |    |   + orderBy/ineq/__name__ tie-break)   |  |
|  +-------------------------------+    +-------------------+--------------------+  |
|                                                           |                       |
|            +----------------------------------------------+------------+          |
|            v                                                           v          |
|  +-------------------------------------------+   +-----------------------------+  |
|  |     StructuredQuery & Watch Engine        |   |  Enterprise Pipeline Engine |  |
|  | - query/find_nearest.rs (FindNearest)     |   | - pipeline/function.rs      |  |
|  | - QueryMatch { version, computed_distance}|   |   (4 vector fns + InvalidArg|  |
|  | - Projection & Aggregation shadowing      |   |    caught by is_error)      |  |
|  | - Listen init catch-up / send_err / Lagged|   | - pipeline/find_nearest.rs  |  |
|  | - Edition-aware pessimistic/optimistic tx |   |   (limit, distance_field)   |  |
|  +-------------------------------------------+   +-----------------------------+  |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                                 crates/googleapis                                 |
| - build.rs / ser.rs: deterministic sorted-key MapValue & Document ProtoJSON       |
| - vector_ext.rs: classify_vector_map, Value vector helpers                        |
| - value_ext.rs: total accessors, Array < Vector < Map Ord/PartialEq,              |
|                 cmp_reference_segments                                            |
+-----------------------------------------------------------------------------------+
```

### 1. `crates/googleapis`: Canonical Vector Classifier, Ordering, and Deterministic ProtoJSON

#### Canonical Vector Classifier and Total `Value` Accessors ([vector_ext.rs](../../crates/googleapis/src/vector_ext.rs) and [value_ext.rs](../../crates/googleapis/src/value_ext.rs))

*   To prevent schema drift or duplicate 2,048-element scans across crates, `vector_ext.rs` defines a single zero-allocation classifier `classify_vector_map(fields: &HashMap<String, Value>) -> VectorClassification<'_>`. It returns `NotVectorMap` unless `"__type__"` is `StringValue("__vector__")`, emits typed `VectorFormatError` diagnostics on extraneous entries, missing/non-array `"value"` entries, empty vectors, vectors exceeding 2,048 dimensions, non-`DoubleValue` coordinates, or `NaN` coordinates, and returns `Valid(&[Value])` when valid (`-0.0` and `±Infinity` accepted).
*   `Value` exposes `vector`, `is_vector`, `as_vector_values`, and `iter_vector_f64` built on `classify_vector_map`.
*   Panicking `into_value_type`, `value_type`, and `value_type_mut` accessors are removed from `Value`. Remaining `Value::as_*`, `Value::is_*`, `Add`, and `Sum` methods match `self.value_type` safely when `None`, and `as_map` / `as_map_mut` return `None` when `self.is_vector()` is `true`. Because [field_path.rs](../../crates/emulator-database/src/database/field_path.rs), projections, and pipeline map helpers traverse maps via `as_map` / `as_map_mut`, vectors behave as atomic scalars while `Value::storage_eq` preserves `-0.0` vs. `+0.0` bit patterns (`to_bits()`) for MVCC version deduplication.

#### Cross-Type Ordering, Coherent `PartialEq`, and Segment-Wise Reference Comparison ([value_ext.rs](../../crates/googleapis/src/value_ext.rs))

*   `Value { value_type: None }` sorts before `NullValue` without panicking. A shared single-pass rank classifier places `ArrayValue` at rank `10`, valid vectors at rank `11`, and non-vector `MapValue`s at rank `12` (`Array < Vector < Map`), comparing valid vectors by `(dimension_len, element_by_element)` using `cmp_f64` (`-0.0 == +0.0`).
*   In `PartialEq for ValueType`, `(MapValue(a), MapValue(b))` classifies each map once: valid vectors compare by slice equality, non-vectors compare directly via `HashMap` equality (`O(K)` zero-allocation per [value-partial-eq.md](./value-partial-eq.md)), and mixed valid-vector vs. non-vector pairs return `false`—preserving `(a == b) == (a.cmp(&b) == Ordering::Equal)`.
*   Extracting `cmp_reference_segments` (with `cmp_reference_paths` delegating to it) lets `emulator-database` compare Firestore `__name__` path segments across [CollectionRef](../../crates/emulator-database/src/database/reference.rs) and [DocumentRef](../../crates/emulator-database/src/database/reference.rs) without string allocation while preserving `REFERENCE_NAME_MIN_ID` (`"__id-9223372036854775808__"` → `""`) normalization instead of ordering by `DefaultAtom` (interned string ID) ASCII bytes (`'-' < '/'`).

#### Deterministic Sorted-Key ProtoJSON Serialization ([build.rs](../../crates/googleapis/build.rs) and [ser.rs](../../crates/googleapis/src/ser.rs))

*   [build.rs](../../crates/googleapis/build.rs) annotates `MapValue.fields` and `Document.fields` with `serialize_sorted_map` so REST `GET` and `PATCH` responses always emit keys in lexicographical order (`"__type__"` before `"value"`). `StructuredQuery` ProtoJSON deserialization in `ser.rs` remains strict.

### 2. `crates/emulator-database`: Write, Mask, and Transform Validation

#### Shared Reserved-Name and `FieldPath` Invariants ([reference.rs](../../crates/emulator-database/src/database/reference.rs) and [field_path.rs](../../crates/emulator-database/src/database/field_path.rs))

*   [reference.rs](../../crates/emulator-database/src/database/reference.rs) exposes `is_reserved_name(s)` (`s.len() >= 5 && s.starts_with("__") && s.ends_with("__")`, allowing `"____"` while rejecting `"__a__"`, `"__type__"`, and `"__name__"`).
*   [field_path.rs](../../crates/emulator-database/src/database/field_path.rs) keeps `parse_field_path` and `FieldReference::from_str` unchanged (so backtick-quoted `` "`__name__`" `` stays `FieldReference::FieldPath(FieldPath(["__name__"]))` and passes existing parser property tests), adds `FieldPath::starts_with` and `FieldPath::validate_non_reserved` (rejecting empty segments and `is_reserved_name` segments, including `"__name__"`), and adds `FieldReference::into_non_reserved_field_path` (rejecting `DocumentName` and delegating `FieldPath` to `validate_non_reserved`).

#### Document, Mask, and Transform Validation Rules ([validation.rs](../../crates/emulator-database/src/database/validation.rs) and [database.rs](../../crates/emulator-database/src/database.rs))

*   `validation::validate_writable_fields` runs directly on top-level `Document.fields` and checks every map key (`empty` or `is_reserved_name`) before inspecting values, rejecting `{ __custom__: 1 }`, `{ "": 1 }`, and `{ __type__: "__vector__", value: [1.0] }` at top level. On nested `MapValue`, `classify_vector_map` accepts valid vectors, maps invalid sentinel maps to `InvalidArgument(err.as_error_message())`, and recurses on `NotVectorMap`. On `ArrayValue`, nested arrays remain rejected while `VectorValue` elements inside arrays are accepted.
*   `DocumentMask` and `FieldTransform` paths are checked via `FieldPath::validate_non_reserved`, and `AppendMissingElements` / `RemoveAllFromArray` / `Increment` operands are validated recursively before state mutation.
*   `FirestoreDatabase::commit` validates each write after read-only and optimistic concurrency checks and before acquiring write guards (preserving `NotImplemented` for `operation: None`, `Delete` masks/transforms, and standalone `Transform` consistently with `BatchWrite`), while `perform_write` checks document preconditions first and validates `Update(doc)` fields, masks, and transforms before mutating state.

### 3. `crates/emulator-database`: Distance Math and Top-K Selection

#### Distance Measures and Direct IEEE 754 Evaluation ([vector.rs](../../crates/emulator-database/src/database/vector.rs))

*   `vector.rs` implements `DistanceMeasure` (`Euclidean`, `Cosine`, `DotProduct`), edition-aware query-vector validation (`validate_query_vector`), `compute_query_distance`, and zero-allocation `compute_distance` over `&[Value]` coordinates.
*   `compute_distance` evaluates `Euclidean` (`sum((a_i - b_i)^2).sqrt()`), unclamped `Cosine` (`1.0 - (dot / (norm_a_sq * norm_b_sq).sqrt())`), and `DotProduct` directly in unscaled IEEE 754 `f64` matching live Cloud Firestore (yielding exact `0.0` for identical finite `Cosine` vectors, preserving unclamped overshoot and `1e±100` overflow/underflow semantics, normalizing `-0.0` to `+0.0`, and returning `InvalidArgument("<measure>_distance(...) result is NaN.")` when `NaN` arises). On Standard Edition (`!enterprise_edition`), `compute_query_distance` rejects finite zero-magnitude `Cosine` vectors with `9 FAILED_PRECONDITION` (`"Cannot compute cosine distance against a vector with a magnitude of zero."`).

#### Partitioned Top-`K` Selection and Tie-Breaking ([vector.rs](../../crates/emulator-database/src/database/vector.rs))

*   `select_and_sort_nearest` ranks candidates by distance (`total_cmp` ascending for `Euclidean`/`Cosine`, descending for `DotProduct`) with caller-supplied tie-breaking (`find_nearest_tie_cmp` for `StructuredQuery.findNearest` and lazy segment-wise `__name__` for Pipelines), partitioning top-`K` matches in linear average time via `select_nth_unstable_by` before sorting the retained prefix.

### 4. `crates/emulator-database`: `StructuredQuery.findNearest`, Watch, Aggregations, and Transactions

#### Query Validation and Pre-Filter Semantics ([query/find_nearest.rs](../../crates/emulator-database/src/database/query/find_nearest.rs) and [query.rs](../../crates/emulator-database/src/database/query.rs))

*   `Query::from_structured` validates non-negative `offset` and `limit` and parses `FindNearest::try_from_proto(proto, enterprise_edition)` (`limit` in `1..=1000`, non-reserved `vector_field` and optional `distance_result_field`, valid edition-checked `query_vector`, non-`NaN` `distance_threshold`, and bounded `Debug` formatting printing dimensionality and up to 3 coordinates); `QueryBuilder::find_nearest` accepts `FindNearest` directly for Rust callers.
*   `Query::validate` validates all `FieldFilter` and `Cursor` operands via `vector::validate_query_operand_value` (`NaN` rejected on Enterprise Edition only), calls `FindNearest::validate(enterprise_edition)`, and rejects `find_nearest` combined with outer `limit`, `offset > 0`, or `start_at`/`end_at` cursors (`InvalidArgument`). When `find_nearest` is absent, `Query::validate` synthesizes implicit inequality and trailing `__name__ ASC` `order_by` clauses and enforces `"Cursor has too many values."`. When `find_nearest` is present, implicit `order_by` synthesis is skipped; `find_nearest_tie_cmp` breaks equal-distance ties via explicit `order_by` (plus trailing `__name__` matching the last `order_by` direction) on Standard Edition when `order_by` is non-empty, or via inequality filter fields `ASC` (plus `__name__ ASC`) otherwise across Standard and Enterprise editions. `Query::missing_docs` (`ListDocuments`) rejects `"find_nearest"`.

#### `QueryMatch` Carrier and Shadowing-Aware Projection ([query.rs](../../crates/emulator-database/src/database/query.rs) and [projection.rs](../../crates/emulator-database/src/database/projection.rs))

*   `DocumentMeta` holds per-document MVCC metadata and its `FifoRwLock`, `StoredDocumentVersion` represents an immutable point-in-time document snapshot, and `QueryMatch` pairs `version: Arc<StoredDocumentVersion>` with `computed_distance: Option<f64>`.
*   During `find_nearest` execution in `Query::once`, documents failing collection, `where`, or explicit `order_by` pre-filters are skipped before inspecting `vector_field` (so pre-filtered `[0.0, 0.0]` docs do not trigger `COSINE` errors), missing/non-vector/mismatched-dimension fields are silently skipped, and Standard Edition skips non-finite (`±Infinity`) candidate vectors as well as implicit `±Infinity` distance results (`dist < +Infinity` for `EUCLIDEAN`/`COSINE`, `dist > -Infinity` for `DOT_PRODUCT`) when `distance_threshold` is omitted.
*   In [projection.rs](../../crates/emulator-database/src/database/projection.rs), `Projection::project` and `project_with_distance` share a single field-copy helper enforcing pre-projection `distance_result_field` ancestor/descendant shadowing in `O(selected_fields)` without mutating `StoredDocumentVersion` or cloning unprojected vector fields.

#### `RunAggregationQuery` and `Listen` Semantics ([database.rs](../../crates/emulator-database/src/database.rs) and [listener.rs](../../crates/emulator-database/src/listener.rs))

*   `RunAggregationQuery` (`SUM` and `AVG`) resolves field operands via `Query::effective_numeric_field_value`, returning `computed_distance` on an exact `distance_result_field` match and `None` on ancestor/descendant collision shadowing; on empty numeric sets, `SUM` returns `Value::null()` (`NULL_VALUE`) on Enterprise Edition and `Value::integer(0)` on Standard Edition.
*   In [listener.rs](../../crates/emulator-database/src/listener.rs), `QueryTarget` tracks per-query watch state and `Listener::go` drives the per-stream event loop. `Listener::set_query` evaluates `find_nearest` during initial `AddTarget` catch-up (applying threshold, `limit`, `distanceResultField` projection, and initial `COSINE` errors) while keeping `find_nearest` intact on `Query` so `BroadcastStreamRecvError::Lagged` and reconnect `reset()` re-run `findNearest`; `QueryTarget::process_event` evaluates subsequent mutations incrementally per document in `event.updates` against `matches_prefilters` (`DocumentChange` with base `project_base`, `DocumentRemove` on prefilter exit, `DocumentDelete` on deletion) without resetting untouched initial matches. `Listener::go` forwards `reset` and `process_event` errors via `send_err` and returns `Ok(())` so terminated streams stop processing events.

#### Edition-Aware Transaction Concurrency ([query.rs](../../crates/emulator-database/src/database/query.rs) and [transaction.rs](../../crates/emulator-database/src/database/transaction.rs))

*   `FirestoreDatabase` configures transactions from two orthogonal flags (`enterprise_edition` and `optimistic_concurrency`), selecting `LockStrategy::PessimisticTransaction` or `LockStrategy::OptimisticTransaction`, while `Query` carries `enterprise_edition`.
*   `Query::once` sorts applicable collections by `CollectionRef` and per-collection documents by `DocumentRef` before calling `lock_strategy.read(&meta)` so concurrent pessimistic transactions acquire shared read guards in deterministic global order before `FirestoreDatabase::commit` acquires write locks.
*   `LockStrategyDocument` carries `computed_distance: Option<f64>` and `LockStrategy::confirm_all` returns `Vec<QueryMatch>`. In Enterprise `find_nearest`, `scanned_guards` buffers all read guards acquired across matching collections before pre-filtering so any mid-scan error drops all local guards cleanly. On success, Standard Edition confirms only top-`K` matches, whereas Enterprise Edition retains all `scanned_guards` on `PessimisticTransaction` via `retain_pessimistic_guards` (a no-op on `OptimisticTransaction`) while confirming only top-`K` matches so `OptimisticState::observed_docs` tracks only returned documents.
*   In [transaction.rs](../../crates/emulator-database/src/database/transaction.rs), `OptimisticState::check_concurrency_violations` maps `query.once` errors (such as a post-`read_time` `[0.0, 0.0]` `COSINE` write) to `10 ABORTED`.

### 5. `crates/emulator-database`: Enterprise Pipeline Vector Expressions and Stage

#### Static Arity Dispatch, `is_error` Isolation, and Vector Functions ([expressions.rs](../../crates/emulator-database/src/database/pipeline/expressions.rs) and [function.rs](../../crates/emulator-database/src/database/pipeline/function.rs))

*   `expressions::maybe_evaluate` returns `InvalidArgument("missing value_type in value")` when `value.value_type` is `None`. `function::evaluate` preserves its `Result<Option<Value>>` signature because [filter.rs](../../crates/emulator-database/src/database/pipeline/filter.rs) treats `None` (missing field) as non-matching (`false`).
*   After handling `is_error` (updated to catch `InvalidArgument` while propagating `NotImplemented`) and `is_absent`, `function::evaluate` matches `func.name` and validates static argument counts before evaluating operand expressions so unknown functions return `GenericDatabaseError::NotImplemented` immediately without evaluating arguments, even inside `is_error(...)`.
*   `cosine_distance`, `euclidean_distance`, `dot_product` (arity `2`), and `vector_length` (arity `1`) evaluate all operands first via `expressions::maybe_evaluate`, return `NullValue` if any operand is `None` or `null`, and return Cloud-matching `InvalidArgument` diagnostics on non-vector types (`"The function <name>(...) requires `Vector` but got `<TYPE>`."`), dimension mismatches (`"<name>(...) arguments must be the same length."`), or `NaN` results.

#### `find_nearest` Pipeline Stage and Upfront Collection Scan ([pipeline/find_nearest.rs](../../crates/emulator-database/src/database/pipeline/find_nearest.rs) and [mod.rs](../../crates/emulator-database/src/database/pipeline/mod.rs))

*   `pipeline/find_nearest.rs` (wired in [mod.rs](../../crates/emulator-database/src/database/pipeline/mod.rs)) validates 3 positional arguments (`FieldReferenceValue` `field`, valid `__vector__` `MapValue` query vector, and case-insensitive `StringValue` `distance_measure`) and `stage.options` (`limit` in `1..=1000` and `FieldReferenceValue` `distance_field`, rejecting raw `ArrayValue`/`StringValue` field/vector operands and unknown options with `"Stage 'find_nearest(...)' does not support option(s) [...]."`). It ranks matching-dimension candidate `__vector__` fields via `select_and_sort_nearest` and injects `distance_field` when set.
*   On `enterprise-pessimistic` pipelines, [collection.rs](../../crates/emulator-database/src/database/pipeline/collection.rs) currently locks all collection documents and materializes `Vec<Document>` upfront before `find_nearest` or `where` stages execute, until zero-copy/pushdown pipeline streaming lands.

### 6. `crates/emulator-grpc`: Streamed Pipeline Responses and Transaction Cleanup

#### Lazy `<= 1 MiB` `ExecutePipelineResponse` Chunking ([Cargo.toml](../../crates/emulator-grpc/Cargo.toml) and [lib.rs](../../crates/emulator-grpc/src/lib.rs))

*   `crates/emulator-grpc` adds `prost` directly from `[workspace.dependencies]` and streams `ExecutePipelineResponse` messages lazily via `chunk_pipeline_responses` (`std::iter::from_fn`). Multi-document frames remain strictly `<= MAX_PIPELINE_RESPONSE_BYTES` (`1 MiB`, well below `@grpc/grpc-js`'s 4 MiB limit), attach `transaction` on frame `0` only, take at least one document per chunk when oversized, and emit one empty frame when `results` is empty.

#### `NewTransaction` Error Rollback ([lib.rs](../../crates/emulator-grpc/src/lib.rs))

*   Across `batch_get_documents`, `run_query`, `run_aggregation_query`, `execute_pipeline`, and single-shot `commit` (`transaction.is_empty()`), a shared cleanup helper rolls back newly allocated transactions (`database.rollback(id)`) whenever execution returns `Err`, preventing orphaned `RunningTransactions` entries and leaked pessimistic read locks.

### 7. `crates/emulator-http` and `ui/`: REST Pass-Through, 50 MiB Limit, and Collapsed Vector Rows

#### REST Body Limit and `:runQuery` `501` Detection ([projects.rs](../../crates/emulator-http/src/routes/projects.rs))

*   `projects::router()` raises Axum's `DefaultBodyLimit` from 2 MiB to 50 MiB (matching `emulator-grpc`'s `MAX_MESSAGE_SIZE`) so valid multi-vector REST `PATCH` and `POST` payloads do not fail with `413 Payload Too Large`.
*   Inside `handle_run_query`, inspecting the parsed `serde_json::Value` body for `structuredQuery.findNearest` (or `structured_query.find_nearest`) before `RunQueryRequest` deserialization returns `501 Not Implemented` when the value is a JSON object and `400 Bad Request` when it is another non-null JSON type—preserving strict ProtoJSON codecs inside `crates/googleapis`.

#### Web UI Collapsed Read-Only Vector Rows ([values.js](../../ui/values.js) and [editor.js](../../ui/components/editor.js))

*   [values.js](../../ui/values.js) exports strict `__vector__` detection (`isVectorMapValue`, supporting REST `"Infinity"` / `"-Infinity"` string encodings), `formatVectorSummary`, and `Object.is`-based recursive equality `firestoreValueEquals` with `O(1)` reference short-circuiting.
*   [editor.js](../../ui/components/editor.js) represents valid vectors as collapsed read-only `'vector'` `DraftNode`s referencing `cleanDoc` directly, preserving invalid-draft dirty tracking and keeping untouched `-0.0` coordinates intact without per-keystroke `JSON.stringify` or 2,048-row DOM expansion.

## Alternatives Considered

### Alternative A: Review and Merge Community PR #194 Incrementally

*   **Pros**: Reuses commits from [PR #194](https://github.com/skunkteam/rust-firestore-emulator/pull/194) (`StructuredQuery.findNearest` across `RunQuery` and `Listen` returning `Vec<Arc<StoredDocumentVersion>>`, public `QueryBuilder::find_nearest`, and SDK compatibility tests).
*   **Cons**: PR #194 predates the `Value` `PartialEq`/`Ord` unification ([value-partial-eq.md](./value-partial-eq.md)), Enterprise Pipeline engine, and REST/Web UI stack. While it implemented `Array < Vector < Map` rank ordering in `Ord`, it omitted the `PartialEq` vector classification required for `Eq`/`Ord` coherence against `#198`, broke `FieldReference::from_str` on `` "`__name__`" ``, and failed `cosineDistance([1, 2], [1, 2]) == 0` due to double `sqrt` rounding (`1.11e-16`), while rejecting valid `±Infinity` coordinates and explicit `order_by` pre-filters. It also permitted outer `limit`/`offset` combinations that Cloud rejects, cloned full `Document` maps prior to `select` masking, tie-broke collection groups by `DefaultAtom` string order, and omitted write-time validation, `RunAggregationQuery` `distanceResultField` shadowing, Enterprise Edition (pessimistic concurrency) candidate locking, Enterprise Pipelines, and Web UI collapsed vector rows.
*   **Reason for Rejection**: Building from scratch while carrying forward PR #194's test suite and lessons (`notImplementedInJava` coverage, public `QueryBuilder::find_nearest`, and `3-pessimistic-transaction.test.ts` Java compatibility fixes) covers the scoped vector features and invariants defined in Goals and Non-Goals without retrofitting an outdated base.

### Alternative B: Replace `googleapis::Value` with a Compact `StoredValue` AST or Custom `prost::Message` First

*   **Pros**: Storing vectors as `Arc<[f64]>`, strings as `Arc<str>`, and maps as sorted `Box<[(FieldAtom, StoredValue)]>` shrinks a 2,048-dimension vector's footprint from ~208 KiB (`size_of::<Value>() == 104` bytes) to ~16 KiB and lowers clone overhead across large `BytesValue` blobs and documents.
*   **Cons**: Replacing `googleapis::google::firestore::v1::Value` touches every filter, comparator, projection, transform, aggregation, pipeline expression, and gRPC/REST boundary across the workspace, adding conversion cost on every RPC unless paired with custom `prost::Message` wire serialization and MVCC version pruning.
*   **Reason for Rejection**: Workspace-wide memory representation changes affect blobs, strings, maps, and MVCC across every subsystem and belong in the dedicated memory-benchmarking workstream, keeping only targeted optimizations (zero-allocation slice iteration, linear `select_nth_unstable_by` selection, and `QueryMatch`) in scope here.

### Alternative C: Execute `StructuredQuery.findNearest` in REST `:runQuery` and Add Web UI Vector Controls

*   **Pros**: Allows HTTP/JSON callers to execute `findNearest` via `POST :runQuery` and allows developers to create or edit vector coordinates in the browser inspector.
*   **Cons**: Requires building and maintaining REST `:runQuery` execution paths and interactive vector editor controls in `ui/` that have no current runtime or developer consumer.
*   **Reason for Rejection**: REST endpoints in this repository exist solely to back the embedded Web UI, which does not run vector searches; collapsed read-only pass-through and HTTP `501 Not Implemented` protect existing UI workflows without unused surface area.

### Alternative D: Require Explicit Vector Index Configuration in the Emulator

*   **Pros**: Mirrors live Cloud Firestore Standard Edition's `9 FAILED_PRECONDITION` error when a query references an unindexed `(collectionGroup, vectorField, dimension, prefilterFields)` tuple.
*   **Cons**: Requires inventing an emulator-specific index configuration file, CLI flag, or admin API and forces every unit test suite using the emulator to register vector index definitions before running queries.
*   **Reason for Rejection**: Index creation is a deploy-time concern in Cloud Firestore, whereas the emulator has no deployment phase and is designed for zero-setup unit and integration tests where required deploy-time indexes are assumed to exist.

### Alternative E: Uniform Pessimistic Transaction Lock Scope Across Standard and Enterprise Editions

*   **Pros**: Avoids branching lock-confirmation behavior on `db.enterprise_edition()` inside `Query::once`—either by locking only the returned top-`K` documents on both editions or by locking every pre-filter-matching scanned document on both editions.
*   **Cons**: As verified by the live Cloud `ConcurrentTest` + raw `GAPIC` (Generated API Client) gRPC contention oracle across all three editions, locking only top-`K` on Enterprise Edition (pessimistic concurrency) diverges from live Cloud Enterprise Edition, which scans without a vector index and holds read locks across all scanned collection documents (candidates and pre-filtered-out docs in matching collections). Conversely, locking all scanned documents on both editions over-locks Standard Edition relative to index-backed queries and breaks `correct_locking_in_txn_with_find_nearest` expectations that writes outside top-`K` succeed on Standard Edition.
*   **Reason for Rejection**: Edition-aware lock confirmation—locking returned top-`K` documents on Standard Edition and all scanned documents in matching collections on Enterprise Edition (pessimistic concurrency)—matches each edition's execution model and preserves both PR #194 compatibility and live Cloud Enterprise Edition parity.

## Non-Functional Requirements

*   **Memory Footprint and CPU Latency:**
    *   Candidate evaluation iterates `&[Value]` coordinates in place (`iter_vector_f64`) with zero per-document heap allocations, partitions top-`K` candidates in `O(N)` average comparisons via `select_nth_unstable_by` plus `O(K log K)` sorting (`K <= 1000`), and avoids cloning unprojected vector fields via `QueryMatch`.
    *   `PartialEq` on regular `MapValue` instances retains `O(1)` length checks plus `O(K)` zero-allocation hash lookups ([value-partial-eq.md](./value-partial-eq.md)).
*   **gRPC Frame Size Bounds:** `ExecutePipelineResponse` streaming in [crates/emulator-grpc/src/lib.rs](../../crates/emulator-grpc/src/lib.rs) caps multi-document wire frames (including protobuf tag, varint length, and envelope fields) at 1 MiB (`1_048_576` bytes), keeping responses below `@grpc/grpc-js`'s 4 MiB default inbound message limit.
*   **Crash Resilience and Workspace Safety Lints:** Because `[profile.release]` in root [Cargo.toml](../../Cargo.toml) sets `panic = "abort"` and `[workspace.lints.rust]` sets `unsafe_code = "forbid"`, all new and modified `Value` accessors, vector classifiers, comparators, distance evaluators, and pipeline expression/stage functions must introduce zero `unsafe` blocks and return typed `Result`/`Option`/`bool` values rather than panicking on malformed or empty inputs, verified across unit, black-box (`emulator-tests`), and conformance tests.
*   **Bounded Query Debug Formatting:** `FindNearest`'s custom `fmt::Debug` logs query vector dimensionality and up to the first 3 coordinates instead of dumping up to 2,048 floats on `debug!(?query)`.

## Risks & Mitigations

*   **Exact Brute-Force `O(N * D)` Scan & Pipeline Upfront Cloning / Locking:** Without approximate HNSW/IVF indexes, `findNearest` evaluates all pre-filtered candidates in `O(N * D)` (`2.048e7` `f64` coordinates per scan and `~2.1 GiB` of unboxed `Value` heap at `104` B/element for `10,000 × 2,048`-dimension documents), while [collection.rs](../../crates/emulator-database/src/database/pipeline/collection.rs) clones `Vec<Document>` upfront and holds shared read locks across all collection documents on `enterprise-pessimistic` pipelines. Larger workloads and zero-copy/pushdown pipeline streaming are deferred ([Future Improvements](#future-improvements)).
*   **Skipped Standard Edition Deploy-Time Index Checks:** Because the emulator skips index-existence enforcement by design, queries missing a composite vector index pass locally; running `test-suite/` against the live Cloud Firestore test project catches any index mismatch in CI.
*   **`Listen` Catch-Up Evaluation & Broadcast Lag Self-Healing:** `Listen` evaluates `find_nearest` on `AddTarget` catch-up (injecting `distanceResultField` and applying initial `limit`/`distanceThreshold`/errors) while keeping `find_nearest` intact on `Query` and handling subsequent writes incrementally per mutated document against `matches_prefilters` (`DocumentChange`, `DocumentRemove`, `DocumentDelete`) without re-running `O(N * D)` vector scans or leaking pre-existing excluded documents; if write bursts overflow the broadcast channel (`BroadcastStreamRecvError::Lagged`), `Listener::go` logs `warn!` and triggers `reset` so watch clients self-heal with full `findNearest` filtering and `distanceResultField` intact.
*   **Append-Only MVCC Version Retention:** Each update to a 2,048-dimension vector document appends `~208 KiB` to `StoredDocumentVersion` history; long-running local sessions should reset or restart the emulator database until MVCC version pruning lands.
*   **Nested-Container `-0.0` Pass-Through Scope:** Editing a sibling field inside the same parent `MapValue` or `ArrayValue` in Web UI Structured mode re-serializes that top-level container through `JSON.stringify`, normalizing nested `-0.0` coordinates to `0`; callers requiring exact `-0.0` preservation when editing sibling fields inside shared containers should issue targeted updates over gRPC.
*   **50 MiB HTTP/gRPC Payload Memory & Local Trust Boundary:** Raising `emulator-http`'s `DefaultBodyLimit` from 2 MiB to 50 MiB (matching `emulator-grpc`) allows large multi-vector REST payloads in memory and relies on the emulator's local-only development trust boundary rather than untrusted public internet exposure.
*   **Enterprise Edition (Pessimistic Concurrency) Collection-Scan Lock Contention:** Holding shared read guards across all scanned documents in matching collections matches Cloud Enterprise Edition (pessimistic concurrency) semantics and causes concurrent writers to non-top-`K` or pre-filtered-out docs in the same collection to wait up to `timeouts.write` before failing with `10 ABORTED`. Note also that on live Cloud `enterprise-pessimistic`, `RunQuery` with `newTransaction` emits Frame 0 `{ transaction }` and retains locks until explicit client `rollback` on mid-query failure, whereas `emulator-grpc` rolls back `NewTransaction` automatically on error.

## Additional Resources

*   Community baseline PR: [skunkteam/rust-firestore-emulator#194](https://github.com/skunkteam/rust-firestore-emulator/pull/194) (`feat: support VectorValue and findNearest vector search`)
*   Prior workspace design docs: [value-partial-eq.md](./value-partial-eq.md) and [emulator-web-ui.md](./emulator-web-ui.md)
*   Cloud Firestore specifications: [google/firestore/v1/query.proto](../../crates/googleapis/include/google/firestore/v1/query.proto) (`StructuredQuery.FindNearest`), [google/firestore/v1/document.proto](../../crates/googleapis/include/google/firestore/v1/document.proto) (`Value`), [google/firestore/v1/firestore.proto](../../crates/googleapis/include/google/firestore/v1/firestore.proto) (`ExecutePipelineResponse`; available after submodule initialization), and `@google-cloud/firestore` v9.4.0 (`pipelines/stage.js`, `reference/vector-query.js`)

## Future Improvements

*   **Compact `StoredValue` Representation, `Value` Layout Shrinking, and MVCC Version Pruning:** Store vectors as `Arc<[f64]>` (16 KiB vs. ~208 KiB for 2,048 dimensions), box cold `FunctionValue`/`PipelineValue` enum variants if `Value` remains in hot paths, intern field names, optimize large `BytesValue` retention, and prune obsolete MVCC document versions once the dedicated memory-benchmarking suite is in place.
*   **Zero-Copy Enterprise Pipeline Streaming and Lock Pushdown:** Refactor `pipeline::collection::execute` ([collection.rs](../../crates/emulator-database/src/database/pipeline/collection.rs)) to stream `Arc<StoredDocumentVersion>` and push filters/top-`K` bounds down before cloning or locking entire collections on `enterprise-pessimistic` pipelines.
*   **REST `:runQuery` and Web UI Vector Search Support:** Add `StructuredQuery.FindNearest` REST execution and interactive vector query/editor controls if the embedded Web UI later adopts vector search workflows.

---

## Detailed Implementation Plan

### Implementation Phasing & Checkpoints

To keep the workspace buildable and testable at every intermediate step, implement the 46 target files across five sequential phases:

| Phase | Scope | Target Files | Verification Gate |
| --- | --- | --- | --- |
| **Phase 1** | `googleapis` `classify_vector_map`, total `Value` accessors, cross-type `Ord`/`PartialEq`, sorted-map ProtoJSON & `expressions.rs` `value_type` caller | Items `1–6` and `18` | `cargo test -p googleapis && cargo check --workspace --all-targets` |
| **Phase 2** | `emulator-database` reserved-name/path helpers, write/mask/transform validation & `vector.rs` distance kernel | Items `7–9`, `11`, and write-validation wiring in Item `10` (`database.rs`) | `cargo nextest run -p emulator-database` |
| **Phase 3** | `StructuredQuery.findNearest`, `QueryMatch` projection, `Listen`, `RunAggregationQuery`, transactions & `structured_query.rs` alignment | Items `12–17`, `QueryMatch`/aggregation wiring in Item `10`, Items `22–23`, and Items `28–29` (`QuerySpec::find_nearest` + `structured_query.rs`) | `cargo nextest run -p emulator-database && cargo nextest run -p emulator-tests -E 'test(structured_query)'` |
| **Phase 4** | Enterprise Pipeline vector functions, `find_nearest` stage, lazy `<= 1 MiB` gRPC response chunking, `NewTransaction` cleanup & black-box `vector_search` Rust suite | Items `19–21`, `24–27`, `PipelineSpec::find_nearest` in Item `28`, and Items `30–32` | `cargo nextest run --workspace` |
| **Phase 5** | REST `:runQuery` `501`, Web UI collapsed vector rows, [11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts) E2E/Cloud conformance & documentation updates | Items `33–46` | `just lint && cargo clippy --workspace --all-targets && cargo clippy --workspace --no-default-features && just test && just sdk-test` |

> Note: Unwired `pub(crate)` helpers introduced in Phases 2–3 emit expected `dead_code` warnings until Phases 4–5 wire them; full workspace lint verification runs in Phase 5 (`just lint`). Because `crates/emulator-database/src/lib.rs` re-exports `pub use database::*;` and root [Cargo.toml](../../Cargo.toml) enforces strict workspace lints (`unreachable_pub`, `missing_copy_implementations`, `missing_debug_implementations`, `unused_qualifications`), all internal items in new modules (`validation.rs`, `vector.rs`, `query/find_nearest.rs`, `pipeline/find_nearest.rs`) and `crates/emulator-tests/` must use `pub(crate)` / `pub(super)` visibility (never accidental `pub`), derive `Copy` and `Debug` on small value types, and avoid redundant module qualifiers.

### Target Files to Create/Modify

#### 1. [crates/googleapis/build.rs](../../crates/googleapis/build.rs) (`Modify`)

*   **Type / Scope**: Protobuf Build Configuration (`googleapis` crate)
*   **Proposed Changes**:
    *   Add `.field_attribute(".google.firestore.v1.MapValue.fields", "#[serde(serialize_with = \"crate::ser::serialize_sorted_map\")]")` to `tonic_prost_build::configure()`.
    *   Update the existing `.field_attribute(".google.firestore.v1.Document.fields", ...)` attribute (lines 17–20) to a single combined attribute `"#[serde(skip_serializing_if = \"std::collections::HashMap::is_empty\", serialize_with = \"crate::ser::serialize_sorted_map\")]"`.
*   **Rationale**: Guarantees deterministic lexicographical JSON key ordering in serialized `Document.fields` and `MapValue.fields` ProtoJSON outputs (`"__type__"` before `"value"`).

#### 2. [crates/googleapis/src/lib.rs](../../crates/googleapis/src/lib.rs) (`Modify`)

*   **Type / Scope**: Crate Root (`googleapis` crate)
*   **Proposed Changes**:
    *   Declare `pub mod vector_ext;` and re-export `vector_ext::{MAX_VECTOR_DIMENSIONS, VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY, VectorClassification, VectorFormatError, classify_vector_map}` plus `value_ext::{cmp_reference_paths, cmp_reference_segments}`.
*   **Rationale**: Exposes canonical vector classification and segment-wise path comparison across workspace crates without leaking internal dependencies.

#### 3. [crates/googleapis/src/vector_ext.rs](../../crates/googleapis/src/vector_ext.rs) (`Create`)

*   **Type / Scope**: Rust Library Module & Unit Tests (`googleapis` crate)
*   **Proposed Changes**:
    *   Define constants `VECTOR_MAP_TYPE_KEY = "__type__"`, `VECTOR_MAP_TYPE_SENTINEL = "__vector__"`, `VECTOR_MAP_VALUE_KEY = "value"`, and `MAX_VECTOR_DIMENSIONS: usize = 2048`.
    *   Define `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum VectorFormatError { ExtraneousEntries, MissingValueArray, EmptyVector, TooManyDimensions, NonDoubleElement, NanElement }` with `pub const fn as_error_message(self) -> &'static str` returning exact Cloud `InvalidArgument` literals:
        *   `ExtraneousEntries` -> `"Vector value map has extraneous entries."`
        *   `MissingValueArray` -> `"Vector value map is missing 'value' array."`
        *   `EmptyVector` -> `"Cannot have a zero length vector."`
        *   `TooManyDimensions` -> `"Vectors must be at most 2048 dimensions."`
        *   `NonDoubleElement` -> `"Vector must only contain values of type 'double'"`
        *   `NanElement` -> `"Vector cannot contain NaN values."`
    *   Define `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum VectorClassification<'a> { NotVectorMap, Valid(&'a [Value]), Invalid(VectorFormatError) }` and `pub fn classify_vector_map(fields: &HashMap<String, Value>) -> VectorClassification<'_>`.
    *   Extend `Value` with `pub fn vector(coords: impl IntoIterator<Item = f64>) -> Self`, `pub fn is_vector(&self) -> bool`, `pub fn as_vector_values(&self) -> Option<&[Value]>`, and `pub fn iter_vector_f64(&self) -> Option<impl ExactSizeIterator<Item = f64> + Clone + '_>`.
    *   Add unit tests covering `1..=2048` bounds, `-0.0` and `±Infinity` acceptance, and every `VectorFormatError` variant (`0` dims, `2049` dims, `NaN`, `IntegerValue` elements, missing `"value"`, non-array `"value"`, and extraneous map keys).
*   **Rationale**: Centralizes `VectorValue` recognition and exact diagnostic classification with zero-allocation slice classification and iteration shared by `googleapis` and `emulator-database`.

#### 4. [crates/googleapis/src/value_ext.rs](../../crates/googleapis/src/value_ext.rs) (`Modify`)

*   **Type / Scope**: Rust Library Module & Unit Tests (`googleapis` crate)
*   **Proposed Changes**:
    *   Remove `into_value_type`, `value_type`, and `value_type_mut` from `Value` (all callers outside `value_ext.rs` use total `value.value_type.as_ref()` matching after Item `18`).
    *   Make all remaining `Value::as_*` (including `as_map_mut` and `into_array`), `Value::is_*` (`is_nan`, `is_number`, `is_null`), and `Add` / `Sum` implementations match `self.value_type` without `.expect(...)` (returning `None` or `false` when `value_type` is `None`); update `Value::as_map` and `Value::as_map_mut` to return `None` when `self.is_vector()` is `true`.
    *   Share a single-pass `fn classify_value_type_rank(vt: Option<&ValueType>) -> (u8, Option<&[Value]>)` helper between `value_type_rank` (`is_compatible_with`) and `cmp_value_type` so each `MapValue` is classified once per operand; sort `value_type: None` before `NullValue` without panicking, keep `ArrayValue` at rank `10`, assign valid vectors rank `11`, assign non-vector `MapValue` rank `12`, and compare valid vectors by `(va.len(), va)` via `cmp_value_type`.
    *   In `PartialEq for ValueType`, classify `(MapValue(a), MapValue(b))` once per side: `(Valid(va), Valid(vb)) => va.len() == vb.len() && va == vb`, `(Valid(_), _) | (_, Valid(_)) => false`, and non-vectors `a.fields.len() == b.fields.len() && a.fields == b.fields`.
    *   Expose `pub fn cmp_reference_segments<'a>(a: impl IntoIterator<Item = &'a str>, b: impl IntoIterator<Item = &'a str>) -> cmp::Ordering` applying `process_numeric_ids` (normalizing `REFERENCE_NAME_MIN_ID`, `"__id-9223372036854775808__"`, to `""`), and implement `pub fn cmp_reference_paths(a: &str, b: &str) -> cmp::Ordering` via `cmp_reference_segments(a.split('/'), b.split('/'))`, calling `cmp_reference_paths` from `(ReferenceValue(a), ReferenceValue(b))`.
    *   Extend `partial_eq_and_ord_coherence_across_numeric_and_special_values` with equivalence classes for 1D/2D/3D vectors (`-0.0 == +0.0`, `±Infinity`), malformed `__vector__` maps containing `IntegerValue(1)` vs. valid `DoubleValue(1.0)` vectors, `Value::default()` (`value_type: None`), `into_array`/`Add`/`Sum` on `value_type: None`, `REFERENCE_NAME_MIN_ID` segment ordering, and PR #194's `vector_extraction_and_ordering` assertions (using `as_vector_values()` / `iter_vector_f64()`).
*   **Rationale**: Enforces `Array < Vector < Map` and length-first vector ordering, removes panicking `value_type` helpers, preserves `REFERENCE_NAME_MIN_ID` normalization, makes `FieldPath` treat vectors atomically, and preserves `(a == b) == (a.cmp(&b) == Equal)` alongside zero-allocation regular-map equality.

#### 5. [crates/googleapis/src/ser.rs](../../crates/googleapis/src/ser.rs) (`Modify`)

*   **Type / Scope**: ProtoJSON Serialization (`googleapis` crate)
*   **Proposed Changes**:
    *   Implement `pub(crate) fn serialize_sorted_map<S: Serializer>(fields: &HashMap<String, Value>, serializer: S) -> Result<S::Ok, S::Error>` sorting entries by key (`fields.iter().sorted_by_key(|(k, _)| *k)`) into `serializer.serialize_map(Some(fields.len()))`.
    *   Leave `impl Deserialize for StructuredQuery` unchanged (preserving strict codec rejection of `findNearest` in `ser.rs:1343-1350`).
    *   Add unit tests verifying deterministic `"__type__"`-before-`"value"` JSON serialization on `MapValue` and `Document`, plus `-0.0` round-trip fidelity inside `__vector__` maps.
*   **Rationale**: Eliminates randomized `HashMap` key order on REST `GET` responses while keeping ProtoJSON `StructuredQuery` codecs strict.

#### 6. [crates/googleapis/src/README.md](../../crates/googleapis/src/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`googleapis` crate)
*   **Proposed Changes**:
    *   Document `[vector_ext.rs]`, `classify_vector_map`, `cmp_reference_segments`, `cmp_reference_paths`, `Value::{vector, is_vector, as_vector_values, iter_vector_f64}`, `as_map` vector atomicity, `Array < Vector < Map` ordering, and sorted `MapValue`/`Document` serialization.
    *   Remove `into_value_type`, `value_type`, and `value_type_mut` from `Interface` (line 20) and `Invariants` (line 30).
    *   Remove the stale `[value_ext.rs:Ord]` `tracing::warn!` bullet from `Side Effects` (line 37).
*   **Rationale**: Synchronizes `crates/googleapis/src/README.md` with actual crate interfaces, invariants, and side effects.

#### 7. [crates/emulator-database/src/database/reference.rs](../../crates/emulator-database/src/database/reference.rs) (`Modify`)

*   **Type / Scope**: Reference & Identifier Helpers (`emulator-database` crate)
*   **Proposed Changes**:
    *   Extract `pub(crate) fn is_reserved_name(s: &str) -> bool` (`s.starts_with("__") && s.ends_with("__") && s.len() >= 5`) from `Ref::from_str` and reuse it inside `Ref::from_str`.
    *   Add `pub(crate) fn cmp_firestore_order(&self, other: &Self) -> cmp::Ordering` on `DocumentRef` without string allocation:
        ```rust
        self.collection_ref
            .root_ref
            .cmp(&other.collection_ref.root_ref)
            .then_with(|| {
                googleapis::cmp_reference_segments(
                    self.collection_ref
                        .collection_id
                        .split('/')
                        .chain(std::iter::once(self.document_id.as_str())),
                    other
                        .collection_ref
                        .collection_id
                        .split('/')
                        .chain(std::iter::once(other.document_id.as_str())),
                )
            })
        ```
*   **Rationale**: Shares one canonical `^__.+__$` (`len >= 5`) predicate and zero-allocation segment-wise Firestore `__name__` comparator across validation and query ranking.

#### 8. [crates/emulator-database/src/database/validation.rs](../../crates/emulator-database/src/database/validation.rs) (`Create`)

*   **Type / Scope**: Write-Time Schema Validation & Unit Tests (`emulator-database` crate)
*   **Proposed Changes**:
    *   Implement `pub(crate) fn validate_writable_fields(fields: &HashMap<String, Value>) -> Result<()>` (called directly on top-level `Document.fields` without `classify_vector_map`, and called recursively from `VectorClassification::NotVectorMap`) and `pub(crate) fn validate_writable_value(value: &Value, in_array: bool) -> Result<()>`:
        *   Validate every map key before its value: reject `key.is_empty()` (`InvalidArgument("document field name cannot be empty")`) and `reference::is_reserved_name(key)` (`InvalidArgument(format!("the name {key} is reserved"))`), rejecting top-level `{ __custom__: 1 }`, `{ "": 1 }`, and `{ __type__: "__vector__", value: [1.0] }` alike.
        *   `value_type: None` -> `InvalidArgument("document field value is missing a value_type")`.
        *   `ValueType::ArrayValue(arr)` -> if `in_array`, return `InvalidArgument("nested arrays are not supported")`; else recurse into `arr.values` with `in_array = true`.
        *   `ValueType::MapValue(map)` -> match `classify_vector_map(&map.fields)`: `Valid(_)` -> `Ok(())`; `Invalid(err)` -> `Err(InvalidArgument(err.as_error_message()))`; `NotVectorMap` -> call `validate_writable_fields(&map.fields)`.
        *   Preserve existing variant error literals (`"fieldReferenceValue is not valid in stored documents"`, `"functionValue is not valid in stored documents"`, and `"pipelineValue is not valid in stored documents"`).
    *   Implement `pub(crate) fn validate_update_mask(mask: &DocumentMask) -> Result<()>` and `pub(crate) fn validate_field_transforms(transforms: &[FieldTransform]) -> Result<()>`, checking each path via `FieldPath::validate_non_reserved` and validating `SetToServerValue` paths plus `AppendMissingElements` / `RemoveAllFromArray` (`in_array = true`) and `Increment` / `Maximum` / `Minimum` (`in_array = false`) operands (rejecting `transform_type: None` with `InvalidArgument("empty transform_type in field_transform")`).
    *   Implement `pub(crate) fn validate_write(write: &Write) -> Result<()>`: preserve `operation: None` -> `NotImplemented("missing operation in write")` (matching `get_doc_name_from_write` across `Commit` and `BatchWrite`), preserve `Delete(_)` (`unimplemented_option!(update_mask)` + `unimplemented_collection!(update_transforms)`) and `Transform(_)` (`crate::unimplemented!("transform")`), and add validation for `Operation::Update(doc)` (`validate_writable_fields(&doc.fields)`, optional `update_mask`, and `update_transforms`).
    *   Add unit tests covering all `VectorFormatError` variants, `"____"` acceptance, `±Infinity` coordinates, `VectorValue` inside `ArrayValue`, nested array rejection, `value_type: None`, empty `""` keys and top-level `{ __type__: "__vector__", value: [1.0] }`, query-only `ValueType` variant errors, empty/reserved segments (including `` "`__name__`" `` and `emb.__type__`, rejected by `FieldPath::validate_non_reserved`) in `update_mask` and `FieldTransform`, malformed vectors inside transforms, and `operation: None` / `Delete` / `Transform` / `SetToServerValue` / `Maximum` / `Minimum` error preservation.
*   **Rationale**: Adds Cloud-conformant `Update(doc)`, mask, and transform validation across every write path while preserving existing `NotImplemented` and precondition error contracts.

#### 9. [crates/emulator-database/src/database/vector.rs](../../crates/emulator-database/src/database/vector.rs) (`Create`)

*   **Type / Scope**: Shared Vector Distance & Top-`K` Selection Kernel (`emulator-database` crate)
*   **Proposed Changes**:
    *   Define `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub(crate) enum DistanceMeasure { Euclidean, Cosine, DotProduct }` with `pub(crate) const fn fn_name(self) -> &'static str` (`"euclidean_distance"`, `"cosine_distance"`, `"dot_product"`), `TryFrom<i32>` (`"invalid structured_query::find_nearest::DistanceMeasure"`), and case-insensitive (`eq_ignore_ascii_case`) `from_pipeline_str(&str) -> Result<Self>` (`"find_nearest distance_measure must be 'euclidean', 'cosine', or 'dot_product'"`).
    *   Define `pub(crate) fn validate_query_vector(value: &Value, measure: DistanceMeasure, enterprise_edition: bool) -> Result<Vec<f64>>` using `classify_vector_map` (`Invalid(e)` -> `e.as_error_message()`, non-vector -> `"query_vector must be a valid VectorValue"`) and, when `measure == DistanceMeasure::Cosine`, calling `compute_query_distance(Cosine, enterprise_edition, coords.iter().copied(), coords.iter().copied())?` to reject zero-norm query vectors (`9 FAILED_PRECONDITION` `"Cannot compute cosine distance against a vector with a magnitude of zero."` on Standard Edition, `3 INVALID_ARGUMENT` `"cosine_distance(...) result is NaN."` on Enterprise Edition) or `±Infinity` `Cosine` query vectors (`InvalidArgument`).
    *   Define `pub(crate) fn compute_query_distance<Ia, Ib>(measure: DistanceMeasure, enterprise_edition: bool, a: Ia, b: Ib) -> Result<f64>` (handling Standard Edition zero-norm `Cosine` `FailedPrecondition` before delegating to `compute_distance`) and `pub(crate) fn compute_distance<Ia, Ib>(measure: DistanceMeasure, a: Ia, b: Ib) -> Result<f64>`:
        *   Evaluate `Euclidean` (`sum((a_i - b_i)^2).sqrt()`), unclamped `Cosine` (`1.0 - (dot / (norm_a_sq * norm_b_sq).sqrt())`), and `DotProduct` (`sum(a_i * b_i)`) directly in unscaled IEEE 754 `f64`.
        *   At exit across all 3 measures: if `dist.is_nan()`, return `Err(GenericDatabaseError::invalid_argument(format!("{}(...) result is NaN.", measure.fn_name())))`; else return `Ok(if dist == 0.0 { 0.0 } else { dist })`.
    *   Define `passes_threshold(self, distance: f64, threshold: f64) -> bool`, `cmp_distance(self, a: f64, b: f64) -> cmp::Ordering`, and `pub(crate) fn select_and_sort_nearest<T, F>(candidates: &mut Vec<T>, limit: Option<usize>, cmp_fn: F)` using `select_nth_unstable_by` + `truncate(k)` when `limit.is_some_and(|k| k < candidates.len())` followed by `sort_unstable_by`.
    *   Add unit tests covering `[1.0, 2.0]` self-cosine `== 0.0`, unscaled `1e-200` (`0.0`), `1e100` (`1e100`), `1e200` (`f64::INFINITY`), `1e200` Cosine `NaN`, `[1e40, 0.0]` self-cosine `== 0.0`, orthogonal `[1.0, 0.0]` vs. `[0.0, 1.0]` `== 1.0`, `±Infinity`, `-0.0` normalization, edition-aware zero-norm `Cosine` `validate_query_vector` / `compute_query_distance`, threshold inclusivity, and `select_and_sort_nearest` tie-breaking across both `parents/doc/collection/x` < `parents/doc-1/collection/x` and multi-depth `collection/a/collection/x` < `collection/z`.
*   **Rationale**: Provides a single stdlib-only distance and selection implementation matching live Cloud IEEE 754 behavior across `StructuredQuery.findNearest` and Enterprise Pipelines.

#### 10. [crates/emulator-database/src/database.rs](../../crates/emulator-database/src/database.rs) (`Modify`)

*   **Type / Scope**: Core Database Engine (`emulator-database` crate)
*   **Proposed Changes**:
    *   Declare `mod validation;` and `pub(crate) mod vector;`.
    *   Delegate module-private `validate_writable_fields` to `validation::validate_writable_fields`.
    *   In `FirestoreDatabase::commit`: inside Phase 1's write loop (`lines 561–570`), call `validation::validate_write(write)?;` immediately after `let name = get_doc_name_from_write(write)?;` and the read-only `rw_txn` check (which runs after `check_concurrency_violations()` and before `txn.get_write_guard(&name).await?`).
    *   In `FirestoreDatabase::perform_write`: keep `document.check_precondition(condition)?` first and preserve `Operation::Delete`, `Operation::Transform`, and `None` arms; inside `Operation::Update(doc)`, invoke `validation::validate_writable_fields(&doc.fields)?`, optional `validation::validate_update_mask(mask)?`, and `validation::validate_field_transforms(&update_transforms)?` before `apply_updates` / `apply_transform`.
    *   Update `run_query`, `list_docs`, and `run_aggregation_query` to consume `QueryMatch` from `query.once(self).await?`.
    *   In `run_aggregation_query` (`SUM` and `AVG`), resolve operand values via `query.effective_numeric_field_value(&m, field)`, and when `self.enterprise_edition()` is `true` and zero numeric values match `SUM`, return `Value::null()` (`NULL_VALUE`) rather than `Value::integer(0)`.
*   **Rationale**: Enforces write/mask/transform validation across all mutation RPCs in exact error-precedence order and wires `QueryMatch` into `RunQuery`, `ListDocuments`, and `distanceResultField` shadowing in `RunAggregationQuery`.

#### 11. [crates/emulator-database/src/database/field_path.rs](../../crates/emulator-database/src/database/field_path.rs) (`Modify`)

*   **Type / Scope**: Field Path Resolution & Unit Tests (`emulator-database` crate)
*   **Proposed Changes**:
    *   Preserve both `parse_field_path` and `FieldReference::from_str` unchanged (so backtick-escaped `` "`__name__`" `` stays `FieldReference::FieldPath(FieldPath(["__name__"]))` and keeps `proptest_field_reference_doc_name_vs_field_path` passing at lines 389–391).
    *   Add `pub(crate) fn starts_with(&self, prefix: &Self) -> bool` (`self.0.starts_with(&prefix.0)`) and `pub(crate) fn validate_non_reserved(&self) -> Result<()>` (rejecting empty segments with `"field path cannot contain empty segments"` and `reference::is_reserved_name(seg)`—including `"__name__"`—with `format!("the name {seg} is reserved")`) on `FieldPath`.
    *   Add `pub(crate) fn into_non_reserved_field_path(self, context_err: &'static str) -> Result<FieldPath>` on `FieldReference` (`Self::DocumentName => Err(InvalidArgument(context_err))`, `Self::FieldPath(p) => { p.validate_non_reserved()?; Ok(p) }`).
    *   Add unit tests verifying `into_non_reserved_field_path` rejects both `"__name__"` and `` "`__name__`" `` plus PR #194's `vector_is_not_traversed_or_mutated_as_map` suite (`get_value`, `delete_value`, and `set_value` on `embedding.value`, `embedding.__type__`, and `embedding.nested`).
*   **Rationale**: Rejects reserved segments (including backtick-quoted `` `__name__` ``) in vector/mask/transform paths while preserving existing `FieldReference` parser property tests and atomic `FieldPath` vector semantics.

#### 12. [crates/emulator-database/src/database/projection.rs](../../crates/emulator-database/src/database/projection.rs) (`Modify`)

*   **Type / Scope**: Field Mask Projection (`emulator-database` crate)
*   **Proposed Changes**:
    *   Extract a shared private helper `fn project_fields(fields: &[FieldReference], version: &StoredDocumentVersion, distance: Option<(&FieldPath, f64)>) -> Document` that initializes `Document { fields: HashMap::new(), create_time: Some(version.create_time), update_time: Some(version.update_time), name: version.name.to_string() }`, copies each `FieldReference::FieldPath(sel)` in `fields` where `distance.is_none_or(|(dist_path, _)| !sel.starts_with(dist_path))` from `version.fields` into `doc.fields`, and—if `distance` is `Some((dist_path, dist))` and any `FieldReference::FieldPath(sel)` in `fields` satisfies `dist_path.starts_with(sel)`—calls `dist_path.set_value(&mut doc.fields, Value::double(dist))` once.
    *   Delegate `Projection::project(&self, version)` to `project_fields(&self.fields, version, None)`.
    *   Add `pub(crate) fn project_with_distance(projection: &Option<Projection>, version: &StoredDocumentVersion, distance: Option<(&FieldPath, f64)>) -> Document`: when `projection` is `None`, build `version.to_document()` and call `dist_path.set_value(&mut doc.fields, Value::double(dist))` if `distance` is `Some`; when `Some(proj)`, call `project_fields(&proj.fields, version, distance)`.
    *   Add unit tests covering unmasked injection, `select("title", "distance")`, `select("title")` masking out `distance`, `select("__name__")` returning empty fields, `select("meta")` retaining `meta.distance`, and `select("meta.author")` returning `{}` when `distance_result_field = "meta"` overwrote stored `meta: { author: "alice" }`.
*   **Rationale**: Eliminates projection loop duplication and enforces pre-projection ancestor/descendant shadowing without mutating `StoredDocumentVersion` or cloning unprojected 2,048-dimension vectors.

#### 13. [crates/emulator-database/src/database/query/find_nearest.rs](../../crates/emulator-database/src/database/query/find_nearest.rs) (`Create`)

*   **Type / Scope**: `StructuredQuery.FindNearest` Specification (`emulator-database` crate)
*   **Proposed Changes**:
    *   Define `#[derive(Clone)] pub struct FindNearest` with `pub` fields (`vector_field: FieldPath`, `query_vector: Vec<f64>`, `distance_measure: DistanceMeasure`, `limit: usize`, `distance_result_field: Option<FieldPath>`, and `distance_threshold: Option<f64>`), `FindNearest::new`, `distance_result_field`, `distance_threshold`, and a custom `fmt::Debug` logging `query_vector.len()` and up to the first 3 coordinates.
    *   Implement `FindNearest::validate(&self, enterprise_edition: bool)` and `FindNearest::try_from_proto(proto: structured_query::FindNearest, enterprise_edition: bool) -> Result<Self>` enforcing exact diagnostics:
        *   `DistanceMeasure::try_from(proto.distance_measure)?` (`"invalid structured_query::find_nearest::DistanceMeasure"`).
        *   `proto.vector_field`: missing -> `"missing vector_field in find_nearest"`; parsed via `FieldReference::from_str` and `into_non_reserved_field_path("vector_field cannot be __name__ or contain empty or reserved segments")`.
        *   `proto.distance_result_field` (when non-empty): parsed via `FieldReference::from_str` and `into_non_reserved_field_path("distance_result_field cannot be __name__ or contain empty or reserved segments")`.
        *   `proto.query_vector`: missing -> `"missing query_vector in find_nearest"`; validated via `vector::validate_query_vector(..., enterprise_edition)`.
        *   `proto.limit`: missing or `!(1..=1000).contains(&v.value)` -> `"find_nearest limit must be between 1 and 1000"`.
        *   `proto.distance_threshold`: `Some(d)` with `d.is_nan()` -> `"distance_threshold cannot be NaN"`.
*   **Rationale**: Isolates `StructuredQuery.FindNearest` validation and bounded debug formatting with deterministic Cloud-aligned error literals.

#### 14. [crates/emulator-database/src/database/query.rs](../../crates/emulator-database/src/database/query.rs) (`Modify`)

*   **Type / Scope**: Structured Query Planner, Lock Strategy & Executor (`emulator-database` crate)
*   **Proposed Changes**:
    *   Declare `pub mod find_nearest;` and re-export `FindNearest` and `DistanceMeasure`; add `find_nearest: Option<FindNearest>` to `Query` (initialized to `None` in `QueryBuilder::build`, configurable via `QueryBuilder::find_nearest`), expose `pub fn has_find_nearest(&self) -> bool`, and define `#[derive(Clone, Debug)] pub struct QueryMatch { pub version: Arc<StoredDocumentVersion>, pub computed_distance: Option<f64> }`.
    *   Add `computed_distance: Option<f64>` to `LockStrategyDocument` (initialized to `None` in `LockStrategy::read`, updated to `Some(dist)` for ranked `find_nearest` matches) and update `LockStrategy::confirm_all` to return `Vec<QueryMatch>`.
    *   In `Query::from_structured`: validate `offset < 0` (`"offset cannot be negative"`) and `limit.as_ref().is_some_and(|l| l.value < 0)` (`"limit cannot be negative"`), parse `find_nearest` via `FindNearest::try_from_proto(fn_proto, enterprise_edition)`, and let `Query::validate` enforce cross-clause rules uniformly across `from_structured` and `QueryBuilder`.
    *   In `Query::validate`: validate `FieldFilter` and `Cursor` operand values via `vector::validate_query_operand_value` (rejecting structurally malformed `__vector__` maps on both editions and `NaN` vector operands on Enterprise) and validate `self.find_nearest`:
        *   When `self.find_nearest.is_some()`: reject `self.limit.is_some()` (`"A query limit cannot be used with FindNearest"`), `self.offset > 0` (`"A query offset cannot be used with FindNearest"`), and `self.start_at.is_some() || self.end_at.is_some()` (`"A cursor cannot be used with FindNearest"`), skipping implicit `order_by` synthesis.
        *   When `self.find_nearest.is_none()`: run implicit inequality `order_by` synthesis and implicit `__name__ ASC` insertion first, then enforce a **new** check rejecting `start_at` / `end_at` cursors where `cursor.values.len() > self.order_by.len()` (`"Cursor has too many values."`).
    *   In `Query::missing_docs`: add `self.find_nearest.is_some().then_some("find_nearest")` to `not_supported`.
    *   Prepend `self.find_nearest.is_some() ||` to `reset_on_update(&self)`; expose `pub(crate) fn matches_prefilters(&mut self, doc: &StoredDocumentVersion) -> Result<bool>` (`includes_collection`, `filter`, and `order_by` field existence) separately from `includes_document(&mut self, doc: &StoredDocumentVersion) -> Result<bool>`, with `includes_document` and ranking sharing `evaluate_find_nearest_candidate` (matching-dimension extraction, Standard non-finite candidate skipping, `compute_query_distance`, `distance_threshold`, and Standard Edition implicit `< +Infinity` / `> -Infinity` default distance bound when `distance_threshold` is omitted). Also expose `project_base` for incremental listener payloads when `find_nearest` is present.
    *   Update `Query::once` across five steps:
        *   Sort `applicable_collections` by `col.name` and per-collection `col.docs().await` by `meta.name` (`cmp_firestore_order`) before calling `lock_strategy.read(&meta)`.
        *   When `find_nearest` is `Some(spec)`, push `doc.guard` into `scanned_guards` before `matches_prefilters` when `self.enterprise_edition` is `true`, so all scanned collection docs are buffered during evaluation while any mid-scan error drops all local guards without touching `LockStrategy`.
        *   Skip non-finite candidate vectors when `!self.enterprise_edition`, evaluate `compute_query_distance`, and record `doc.computed_distance = Some(dist)` on qualifying candidates.
        *   Rank with `vector::select_and_sort_nearest` using `find_nearest_tie_cmp` (explicit `order_by` + direction-matched trailing `__name__` on Standard Edition when `order_by` is non-empty; inequality filter fields `ASC` + segment-wise `__name__ ASC` otherwise across Standard and Enterprise editions).
        *   If `self.enterprise_edition` is `true`, retain all `scanned_guards` on `LockStrategy::PessimisticTransaction` via `retain_pessimistic_guards` (a no-op on `OptimisticTransaction`) while passing only ranked top-`K` results to `LockStrategy::confirm_all` so `OptimisticState::observed_docs` records only returned top-`K` documents.
    *   Update `Query::project(&self, item: &QueryMatch) -> Document` to delegate to `projection::project_with_distance`, expose `pub(crate) fn distance_result_field(&self) -> Option<&FieldPath>`, and expose `pub(crate) fn effective_numeric_field_value<'a>(&self, item: &'a QueryMatch, field_ref: &FieldReference) -> Option<Cow<'a, Value>>`.
*   **Rationale**: Implements Cloud-conformant `StructuredQuery.findNearest` execution and error-safe edition-aware pessimistic/optimistic locking across `RunQuery`, `Listen`, `RunAggregationQuery`, and `ListDocuments`.

#### 15. [crates/emulator-database/src/database/query/tests.rs](../../crates/emulator-database/src/database/query/tests.rs) (`Modify`)

*   **Type / Scope**: Test Helper Adaptation (`emulator-database` crate)
*   **Proposed Changes**:
    *   Update `eval_query` (line 105) to map `|m| m.version.name.clone()`, and add `query_builder_find_nearest_validates_and_executes` verifying the programmatic `QueryBuilder::find_nearest` API (all end-to-end Rust tests remain in [crates/emulator-tests/src/suites/vector_search.rs](../../crates/emulator-tests/src/suites/vector_search.rs)).
*   **Rationale**: Adapts existing query unit tests to `QueryMatch` while keeping all new Rust tests in the black-box `emulator-tests` suite per [black-box-rust-testing-foundation.md](./black-box-rust-testing-foundation.md).

#### 16. [crates/emulator-database/src/database/transaction.rs](../../crates/emulator-database/src/database/transaction.rs) (`Modify`)

*   **Type / Scope**: Pessimistic & Optimistic Transaction Manager (`emulator-database` crate)
*   **Proposed Changes**:
    *   In `OptimisticState::check_concurrency_violations` (lines 286–295), map `query.once(&db).await` errors to `Err(GenericDatabaseError::aborted("contention: concurrent modification invalidated query"))` and check `if m.version.update_time > rt` on each returned `QueryMatch`.
*   **Rationale**: Reuses existing `observed_docs` + `executed_queries` optimistic validation without extra state while converting concurrent post-`read_time` `COSINE` `NaN` failures into `10 ABORTED` at commit time.

#### 17. [crates/emulator-database/src/listener.rs](../../crates/emulator-database/src/listener.rs) (`Modify`)

*   **Type / Scope**: Real-Time `Listen` (`Watch`) Engine (`emulator-database` crate)
*   **Proposed Changes**:
    *   In `Listener::go` (`lines 104–127`), on `self.reset(database).await` or `self.process_event(&event).await` error, call `self.send_err(err).await?; return Ok(());` so terminated streams stop processing subsequent write events.
    *   On `(db_name, Some(Err(BroadcastStreamRecvError::Lagged(count))))` (`where db_name == database.name`), log `warn!` and call `if let Err(err) = self.reset(database).await { self.send_err(err).await?; return Ok(()); }` to self-heal missed broadcast events.
    *   Keep `find_nearest` attached to `Query` after initial `target.reset` so reconnect and `Lagged` resets preserve k-NN evaluation and `distanceResultField`, while updating `QueryTarget::process_event` to evaluate incremental updates against `self.query.matches_prefilters` when `self.query.has_find_nearest()` (`DocumentChange` projected via `project_base`, `DocumentRemove` when leaving pre-filters, and `DocumentDelete`).
    *   In `QueryTarget::reset`, iterate returned `QueryMatch` items (`m.version.name.clone()` and `self.query.project(&m)`).
*   **Rationale**: Matches Cloud Firestore `Listen` behavior for `findNearest` initial snapshots and incremental updates, self-heals broadcast lag, and cleanly terminates streams after propagating errors.

#### 18. [crates/emulator-database/src/database/pipeline/expressions.rs](../../crates/emulator-database/src/database/pipeline/expressions.rs) (`Modify`)

*   **Type / Scope**: Pipeline Expression Evaluator (`emulator-database` crate)
*   **Proposed Changes**:
    *   In `maybe_evaluate` (lines 11–19), replace panicking `value.value_type()` with `let Some(vt) = value.value_type.as_ref() else { return Err(GenericDatabaseError::invalid_argument("missing value_type in value")); }; match vt { ... }` (keeping `ValueType::FunctionValue(f) => function::evaluate(f, doc)` unchanged).
*   **Rationale**: Replaces the sole external `Value::value_type()` caller with non-panicking matching so `Value` can drop its panicking accessors in Phase 1 while keeping `evaluate` / `maybe_evaluate` signatures unchanged.

#### 19. [crates/emulator-database/src/database/pipeline/function.rs](../../crates/emulator-database/src/database/pipeline/function.rs) (`Modify`)

*   **Type / Scope**: Pipeline Built-In Functions (`emulator-database` crate)
*   **Proposed Changes**:
    *   Keep `pub(super) fn evaluate(func: &Function, doc: &Document) -> Result<Option<Value>>` unchanged (`pipeline/filter.rs` relies on `None` to treat missing-field expressions as non-matching `false`).
    *   Keep `is_error` (lines 21–25, updated to match `Ok(_) => Ok(Some(Value::boolean(false)))`, `Err(GenericDatabaseError::InvalidArgument(_)) => Ok(Some(Value::boolean(true)))`, `Err(err) => Err(err)`) and `is_absent` (lines 26–30) at the top before general function dispatch.
    *   Match `func.name.as_str()` and check static arity before evaluating `func.args`: preserve `0..` arity for `"string_concat" | "array" | "array_concat"` and exact `1`- or `2`-arg checks (`get_exact_expr_args`, including exact `2` for `"and" | "or"`) on fixed-arity functions, and in the `_` unknown-function fallback arm format raw unevaluated `func.args` (`unimplemented!(format!("function {name}({})", args.iter().map(|v| format!("{v:?}")).join(", ")))`) so unknown functions return `GenericDatabaseError::NotImplemented` immediately without evaluating arguments even inside `is_error(...)`.
    *   Dispatch `"cosine_distance"`, `"euclidean_distance"`, `"dot_product"` (`get_exact_expr_args` len `2`), and `"vector_length"` (len `1`):
        *   Evaluate **all** operand expressions via `expressions::maybe_evaluate` before returning `Ok(Some(Value::null()))` if any operand is `None` or `Value::is_null()`.
        *   Non-vector `Value` -> `Err(GenericDatabaseError::invalid_argument(format!("The function {name}(...) requires `Vector` but got `{}`.", pipeline_type_name(&v))))` where `pipeline_type_name` exhaustively maps `NullValue` -> `"NULL"`, `FieldReferenceValue` -> `"FIELD_REFERENCE"`, `value_type: None` -> `"NULL"`, plus `"BOOLEAN"`, `"INT64"`, `"FLOAT64"`, `"TIMESTAMP"`, `"STRING"`, `"BYTES"`, `"PATH"`, `"GEOPOINT"`, `"ARRAY"`, `"MAP"`, `"FUNCTION"`, `"PIPELINE"`.
        *   Different vector lengths -> `Err(GenericDatabaseError::invalid_argument(format!("{name}(...) arguments must be the same length.")))`.
        *   Valid vectors -> `vector::compute_distance` wrapped in `Value::double` (or `Value::integer(vals.len() as i64)` for `vector_length`).
*   **Rationale**: Implements the four pipeline vector operator functions with Cloud-matching `null` propagation and `isError()` fault isolation while keeping `Result<Option<Value>>` intact for `pipeline/filter.rs`.

#### 20. [crates/emulator-database/src/database/pipeline/find_nearest.rs](../../crates/emulator-database/src/database/pipeline/find_nearest.rs) (`Create`)

*   **Type / Scope**: Pipeline `find_nearest` Stage (`emulator-database` crate)
*   **Proposed Changes**:
    *   Implement `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`:
        *   Require exact 3 positional arguments (`"Stage 'find_nearest' expects 3 arguments (field, vector, distance_measure)"`):
            1.  `args[0]`: strict `FieldReferenceValue` parsed via `into_non_reserved_field_path`, rejecting `StringValue` and non-reference types with Cloud-exact diagnostics (`proto_oneof_screaming_name` / `proto_pascal_type_name`).
            2.  `args[1]`: strict valid `__vector__` `MapValue` only (rejecting raw `ArrayValue` and other non-vector values with Cloud-exact diagnostics; validating `1..=2048` coordinates via `VectorValue` / `classify_vector_map` and pre-checking `Cosine` self-distance `"cosine_distance(...) result is NaN."`).
            3.  `args[2]`: `StringValue` parsed via `DistanceMeasure::from_pipeline_str`.
        *   Validate `stage.options`: collect sorted keys not in `["distance_field", "limit"]`; if non-empty, return `InvalidArgument(format!("Stage 'find_nearest(field: FieldReference, vector: Vector, distance_measure: DistanceMeasure, limit: Int64?, distance_field: FieldPath?)' does not support option(s) [{}].", unknown.join(", ")))`. Validate `"limit"` (`None` when omitted) as `IntegerValue(n)` in `1..=1000` (`"find_nearest limit must be an integer between 1 and 1000"`) and `"distance_field"` strictly as `FieldReferenceValue` parsed via `into_non_reserved_field_path` (rejecting `StringValue`).
        *   Extract matching-dimension vectors via `Value::iter_vector_f64`—skipping documents whose field is missing, non-vector (including plain `ArrayValue`), or of different dimension, while propagating `compute_distance` `NaN` errors—partition and sort via `vector::select_and_sort_nearest` (`cmp_reference_paths(&a.name, &b.name)` tie-breaker), and write `Value::double(dist)` via `dist_path.set_value(&mut doc.fields, ...)` when `distance_field` is set.
*   **Rationale**: Implements Enterprise Edition `.pipeline()...findNearest(...)` with strict `FieldReferenceValue` and `__vector__` `MapValue` validation matching live Cloud Firestore option, type, and limit diagnostics.

#### 21. [crates/emulator-database/src/database/pipeline/mod.rs](../../crates/emulator-database/src/database/pipeline/mod.rs) (`Modify`)

*   **Type / Scope**: Pipeline Stage Dispatcher (`emulator-database` crate)
*   **Proposed Changes**:
    *   Declare `mod find_nearest;` and add `"find_nearest" => find_nearest::execute(stage, current_docs)?` inside `execute`.
*   **Rationale**: Registers the `find_nearest` pipeline stage in the dispatcher.

#### 22. [crates/emulator-database/src/database/README.md](../../crates/emulator-database/src/database/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-database` crate)
*   **Proposed Changes**:
    *   Document `[validation.rs]`, `[vector.rs]` (`compute_query_distance`), `[reference.rs:is_reserved_name]`, `QueryMatch`, `LockStrategy::confirm_all` (`Vec<QueryMatch>`), `project_with_distance`, edition-aware `findNearest` locking and tie-breaking, Enterprise `RunAggregationQuery` `SUM` empty-set `NULL_VALUE`, and `Listen` initial catch-up in `Interface` and `Invariants`.
*   **Rationale**: Keeps `database/README.md` synchronized with `validation.rs`, `vector.rs`, and `QueryMatch`.

#### 23. [crates/emulator-database/src/database/query/README.md](../../crates/emulator-database/src/database/query/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-database` crate)
*   **Proposed Changes**:
    *   Document `[find_nearest.rs:FindNearest]`, `QueryBuilder::find_nearest`, `FieldFilter`/`Cursor` operand validation, and edition-aware `StructuredQuery.findNearest` validation and tie-breaking invariants under `Overview`, `Interface`, and `Invariants`.
*   **Rationale**: Keeps `database/query/README.md` synchronized with `find_nearest.rs` and `query.rs`.

#### 24. [crates/emulator-database/src/database/pipeline/README.md](../../crates/emulator-database/src/database/pipeline/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-database` crate)
*   **Proposed Changes**:
    *   Document `[find_nearest.rs:execute]`, the four vector functions (`cosine_distance`, `euclidean_distance`, `dot_product`, `vector_length`), `is_error` `InvalidArgument` vs. `NotImplemented` isolation, and `find_nearest` `Stage.options` (`limit`, `distance_field`) under `Overview`, `Interface`, and `Invariants`.
*   **Rationale**: Keeps `database/pipeline/README.md` synchronized with the expanded pipeline stage and function surface.

#### 25. [crates/emulator-grpc/Cargo.toml](../../crates/emulator-grpc/Cargo.toml) (`Modify`)

*   **Type / Scope**: Crate Manifest (`emulator-grpc` crate)
*   **Proposed Changes**:
    *   Add `prost = { workspace = true }` under `[dependencies]`.
*   **Rationale**: Grants `emulator-grpc` direct access to `prost::Message` and `prost::encoding::encoded_len_varint` for exact pipeline frame sizing without polluting `googleapis`'s public API.

#### 26. [crates/emulator-grpc/src/lib.rs](../../crates/emulator-grpc/src/lib.rs) (`Modify`)

*   **Type / Scope**: gRPC Server Streaming & Transaction Cleanup (`emulator-grpc` crate)
*   **Proposed Changes**:
    *   Import `prost::{Message, encoding::encoded_len_varint}`, define `const MAX_PIPELINE_RESPONSE_BYTES: usize = 1024 * 1024;`, and implement `chunk_pipeline_responses(mut new_transaction: Vec<u8>, results: Vec<Document>, read_time: Timestamp) -> impl Iterator<Item = Result<ExecutePipelineResponse, Status>>` using `std::iter::from_fn` over `results.into_iter().peekable()`.
    *   Emit one empty `ExecutePipelineResponse` when `results` is initially empty; otherwise accumulate documents while `frame_bytes + entry_bytes <= MAX_PIPELINE_RESPONSE_BYTES` (always taking at least 1 document per chunk), where initial `frame_bytes` accounts for `execution_time` and `transaction` (on chunk `0` only when `!new_transaction.is_empty()`), and `entry_bytes` accounts for tag + varint length + `doc.encoded_len()`. Stream via `tokio_stream::iter(...)`.
    *   Add a reusable `NewTransaction` cleanup helper across `batch_get_documents`, `run_query`, `run_aggregation_query`, `execute_pipeline`, and single-shot `commit` (`transaction.is_empty()`) that calls `let _ = database.rollback(id).await;` whenever execution returns `Err`.
*   **Rationale**: Streams large pipeline responses lazily within exact protobuf wire budgets and prevents orphaned `RunningTransactions` entries and leaked pessimistic read locks on query, batch-get, commit, or pipeline failures.

#### 27. [crates/emulator-grpc/src/README.md](../../crates/emulator-grpc/src/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-grpc` crate)
*   **Proposed Changes**:
    *   Update `Invariants` to document lazy `<= 1 MiB` `ExecutePipelineResponse` frame chunking (`MAX_PIPELINE_RESPONSE_BYTES`) and `NewTransaction` rollback cleanup across `batch_get_documents`, `run_query`, `run_aggregation_query`, `execute_pipeline`, and single-shot `commit`.
*   **Rationale**: Keeps `emulator-grpc/src/README.md` synchronized with `execute_pipeline` streaming and transaction cleanup invariants.

#### 28. [crates/emulator-tests/src/harness/builders.rs](../../crates/emulator-tests/src/harness/builders.rs) (`Modify`)

*   **Type / Scope**: Black-Box Test Builders (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Add `pub(crate) fn vec_val(coords: impl IntoIterator<Item = f64>) -> Value` wrapping `Value::vector`.
    *   Add `find_nearest` support on `QuerySpec` (field + builder method forwarding `Option<structured_query::FindNearest>` to `StructuredQuery`).
    *   Add `find_nearest` stage builder helper on `PipelineSpec`.
*   **Rationale**: Equips the centralized `emulator-tests` harness with concise protobuf vector, `FindNearest`, and pipeline builders without duplicating wire boilerplate.

#### 29. [crates/emulator-tests/src/suites/structured_query.rs](../../crates/emulator-tests/src/suites/structured_query.rs) (`Modify`)

*   **Type / Scope**: Black-Box Structured Query Suite (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Update the existing `FindNearest::default()` test at lines 347–370 from `Code::Unimplemented` to `Code::InvalidArgument` (once `FindNearest` validation lands).
*   **Rationale**: Keeps existing black-box structured query expectations aligned with `StructuredQuery.findNearest` validation.

#### 30. [crates/emulator-tests/src/suites/vector_search.rs](../../crates/emulator-tests/src/suites/vector_search.rs) (`Create`)

*   **Type / Scope**: Centralized Black-Box Rust Verification Suite (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Reusing `TestDb`, `TestEnv`, `ContentionProbe`, `ListenHandle`, and `googleapis::{val, fields, doc}` (`pub(crate)` visibility discipline throughout):
        *   **Write, Mask, and Transform Validation:** Verify invalid vector writes (`0D`, `2049D`, `NaN`, non-double elements, missing/extraneous keys), empty/`__custom__`/`__name__`/`` "`__name__`" ``/`emb.__type__` paths across `CreateDocument`, `UpdateDocument`, `BatchWrite`, and atomic multi-write `Commit`, `"____"` acceptance, `±Infinity` and `VectorValue`-in-`ArrayValue` storage, and preservation of `Write { operation: None }` (`Code::Unimplemented`).
        *   **`StructuredQuery.findNearest` & `RunAggregationQuery`:** Verify `EUCLIDEAN`, `COSINE`, `DOT_PRODUCT`, `distanceThreshold` inclusivity, `distanceResultField` shadowing across `select(...)` and `COUNT`/`SUM`/`AVG` (Enterprise `NULL_VALUE` vs. Standard `0` on empty numeric `SUM`), `where` and explicit `order_by` pre-filters + secondary tie-breaking, `all_descendants` collection-group tie-breaking (`parents/doc/collection/x` < `parents/doc-1/collection/x` and `collection/a/collection/x` < `collection/z`), `1e-200`/`1e100`/`1e200`/`±Infinity` edition semantics, zero-norm `COSINE` `FailedPrecondition` (Standard) vs. `InvalidArgument` (Enterprise), `limit`/`offset`/cursor validation errors, and non-vector oversized cursor rejection (`"Cursor has too many values."`).
        *   **Edition-Aware Transaction Concurrency (`#[tokio::test(start_paused = true)]`):** Use `ContentionProbe` to verify Standard Edition top-`K` lock scope (concurrent non-top-`K` write succeeds) vs. Enterprise pessimistic collection-scan lock scope (outside-collection writes succeed while both non-top-`K` candidates and pre-filtered-out docs in the same collection contend/abort), mid-scan `COSINE` error dropping buffered guards cleanly, and optimistic write-skew plus concurrent `[0.0, 0.0]` `COSINE` `Code::Aborted`.
        *   **Real-Time `Listen` (`ListenHandle`):** Verify initial `AddTarget` `findNearest` catch-up (with `distanceResultField` and threshold filtering), incremental base pre-filter updates (`was_reset == false`, excluding non-matching `d_init_far`, retaining `dist`, handling delete deltas and pre-filter exit `DocumentRemove` via `ListenBatch.removed_docs`), `Lagged` raw `listen` reset recovery preserving k-NN/`distanceResultField`, and initial `COSINE [0, 0]` `FailedPrecondition`/`InvalidArgument` error delivery with clean stream termination. Also cover `+Infinity queryVector` edition semantics (`std_inf_q`/`ent_inf_q`).
        *   **Enterprise Pipelines, Raw Stream Chunking & `NewTransaction` Rollback:** Verify `cosine_distance`, `euclidean_distance`, `dot_product`, `vector_length`, `null` propagation, `is_error` catching `InvalidArgument` while propagating `NotImplemented`, and the `find_nearest` stage (`__vector__` `MapValue` and `FieldReferenceValue` inputs with `limit`/`distance_field`, strict rejection of raw `ArrayValue` vectors and `StringValue` `field`/`distance_field`, and unknown option errors). Call `execute_pipeline` via the raw Tonic client to assert every emitted `ExecutePipelineResponse` frame satisfies `frame.encoded_len() <= 1_048_576` on 200 × 2,048-dimension docs (`transaction` on frame `0` only), single oversize document progress, and empty-result single-frame emission, plus `NewTransaction` rollback cleanup on error across `batch_get_documents`, `run_query`, `run_aggregation_query`, `execute_pipeline`, and single-shot `commit`.
*   **Rationale**: Centralizes all new Rust end-to-end vector, concurrency, watch, pipeline, chunking, and rollback tests inside `crates/emulator-tests/` per [black-box-rust-testing-foundation.md](./black-box-rust-testing-foundation.md).

#### 31. [crates/emulator-tests/src/suites/mod.rs](../../crates/emulator-tests/src/suites/mod.rs) (`Modify`)

*   **Type / Scope**: Test Suite Registry (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Declare `mod vector_search;`.
*   **Rationale**: Registers the new black-box vector search test suite.

#### 32. [crates/emulator-tests/src/README.md](../../crates/emulator-tests/src/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-tests` crate)
*   **Proposed Changes**:
    *   Document `vec_val`, `QuerySpec::find_nearest`, and `PipelineSpec::find_nearest` under `[harness/builders.rs]` and `vector_search` under `[suites/]`.
*   **Rationale**: Keeps `crates/emulator-tests/src/README.md` synchronized with added builders and test suites.

#### 33. [crates/emulator-http/src/routes/projects.rs](../../crates/emulator-http/src/routes/projects.rs) (`Modify`)

*   **Type / Scope**: REST Router Body Limit & `:runQuery` Handler (`emulator-http` crate)
*   **Proposed Changes**:
    *   Add `.layer(axum::extract::DefaultBodyLimit::max(50 * 1024 * 1024))` to `projects::router()`, matching `emulator-grpc`'s 50 MiB `MAX_MESSAGE_SIZE`.
    *   In `handle_run_query`, parse `let raw: serde_json::Value = parse_json_body(headers, body)?;`, inspect `raw.get("structuredQuery").or_else(|| raw.get("structured_query"))` for `"findNearest"` / `"find_nearest"`: when present and `!v.is_null()`, return `Err(RestError::not_implemented("StructuredQuery.findNearest over REST is not implemented"))` (`501`) if `v.is_object()` and `Err(RestError::bad_request("invalid findNearest in StructuredQuery"))` (`400`) otherwise; then deserialize `serde_json::from_value::<RunQueryRequest>(raw).map_err(|e| RestError::bad_request(e.to_string()))?`.
*   **Rationale**: Prevents `413 Payload Too Large` on valid multi-vector REST `PATCH`/`POST` payloads exceeding Axum's 2 MiB default limit and handles HTTP `:runQuery` `findNearest` `501` / `400` status mapping at the HTTP boundary without relaxing `googleapis::ser`.

#### 34. [crates/emulator-http/src/routes/README.md](../../crates/emulator-http/src/routes/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`emulator-http` crate)
*   **Proposed Changes**:
    *   Update `Invariants` to document the 50 MiB `DefaultBodyLimit` on `/v1/projects` routes and state that `POST .../documents:runQuery` returns `501 Not Implemented` when `structuredQuery.findNearest` is a JSON object (`400 Bad Request` for non-object values).
*   **Rationale**: Keeps `crates/emulator-http/src/routes/README.md` synchronized with REST body-limit and `:runQuery` status handling.

#### 35. [crates/emulator-http/tests/admin_api.rs](../../crates/emulator-http/tests/admin_api.rs) (`Modify`)

*   **Type / Scope**: HTTP Integration Tests (`emulator-http` crate)
*   **Proposed Changes**:
    *   Add in-memory HTTP integration tests (`RouterBuilder::new(project).build().oneshot(...)`) verifying: (a) `POST :runQuery` with object `findNearest` (`"findNearest": { "vectorField": { "fieldPath": "embedding" }, "distanceMeasure": "EUCLIDEAN", ... }`) returns `501 Not Implemented` (`status == "UNIMPLEMENTED"`) while non-object `"findNearest": 123` returns `400 Bad Request`, (b) `GET` and `PATCH` on a document containing a `__vector__` map (including `-0.0`) serialize `"__type__"` before `"value"` deterministically and reject malformed `__vector__` writes with `400 Bad Request`, and (c) `PATCH` with a valid `> 2 MiB` multi-vector ProtoJSON payload succeeds (`200 OK`) instead of failing with `413 Payload Too Large`.
*   **Rationale**: Verifies HTTP `501`/`400` `findNearest` handling, `> 2 MiB` multi-vector payload acceptance under the raised body limit, and lossless REST pass-through for vector-bearing documents.

#### 36. [ui/values.js](../../ui/values.js) (`Modify`)

*   **Type / Scope**: Web UI Value Helpers (`ui/`)
*   **Proposed Changes**:
    *   Type `isVectorMapValue`, `formatVectorSummary`, and `firestoreValueEquals` parameters as `FirestoreValue | null | undefined`.
    *   Export `isVectorMapValue(val)` checking all 4 rules: `Object.keys(fields).length === 2`, `fields.__type__?.stringValue === '__vector__'`, `Array.isArray(fields.value?.arrayValue?.values)` with `1 <= values.length && values.length <= 2048`, and every element satisfying `(typeof el?.doubleValue === 'number' && !Number.isNaN(el.doubleValue)) || el?.doubleValue === 'Infinity' || el?.doubleValue === '-Infinity'`.
    *   Export `formatVectorSummary(val)` formatting up to the first 3 coordinates via `formatDoubleInput(el.doubleValue)` (`vector (${n} dims) [${head}${n > 3 ? ', …' : ''}]`) and delegate vector maps in `formatValuePreview` to `formatVectorSummary`.
    *   Export `firestoreValueEquals(a, b)` starting with `if (a === b) return true;`, comparing `doubleValue` via `typeof a.doubleValue === 'number' && typeof b.doubleValue === 'number' ? Object.is(a.doubleValue, b.doubleValue) : a.doubleValue === b.doubleValue`, and comparing `arrayValue` elements and `mapValue` keys/values recursively without `JSON.stringify`.
*   **Rationale**: Centralizes strict `__vector__` detection (including REST `±Infinity` string encodings), canonical vector preview formatting, and recursive structural equality with `O(1)` reference short-circuiting in `ui/values.js` under `tsc --checkJs --strict`.

#### 37. [ui/components/editor.js](../../ui/components/editor.js) (`Modify`)

*   **Type / Scope**: Web UI Document Editor Component (`ui/components/`)
*   **Proposed Changes**:
    *   Type `DraftNode.type` as `ValueTypeKey | 'vector'` with optional `rawVectorValue?: FirestoreValue` in the `DraftNode` JSDoc typedef.
    *   In `createEditorSession` and `syncSessionWithDocument`, build `nodes` from `cleanDoc?.fields ?? {}` rather than uncloned `doc.fields` so untouched `rawVectorValue` references are identical (`===`) to `cleanDoc`.
    *   In `valueToDraftNode`, return a `'vector'` node with `rawVectorValue: val` and `children: []` when `isVectorMapValue(val)` is `true`; in `convertSingleNode`, return `node.rawVectorValue ?? null` when `node.type === 'vector'`.
    *   In `computeChangedFieldMask`, replace `JSON.stringify` comparison with `!firestoreValueEquals(cleanFields[k], nextFields[k])`; in `recomputeDirty`, preserve invalid-draft dirty handling (`if (!res.valid || !res.fields) { session.isDirty = true; return; }`) and compute `session.isDirty = computeChangedFieldMask(session.cleanDoc?.fields ?? {}, res.fields).length > 0;`. Track `rawBaselineText` alongside `!session.isDirty ? session.cleanJson : ...` when switching `structured -> raw -> structured` (comparing `rawText.trim() === cleanJson.trim()` / `rawText !== rawBaselineText`) so unedited round-trips preserve `-0.0` vector coordinates and UTF-8 field ordering without triggering false-dirty diffs.
    *   In `renderDraftNode` and `renderValueEditorControl`: preserve array index vs. object key rendering, omit `<select class="select field-type-select">` when `node.type === 'vector'`, render `<span class="type-badge vector">vec</span>` (styled in [ui/styles.css](../../ui/styles.css)), handle `case "vector"` inside `renderValueEditorControl` using `formatVectorSummary(node.rawVectorValue)`, omit `+ Field` / `+ Item` and nested child rendering, and retain the remove `✕` button.
*   **Rationale**: Eliminates >20,000-node DOM expansion and per-keystroke 100 KiB `JSON.stringify` stalls, preserves invalid-draft dirty handling and `tsc --strict` exhaustiveness, and keeps untouched `-0.0` vector coordinates intact during Structured-mode scalar edits.

#### 38. [ui/README.md](../../ui/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`ui/`)
*   **Proposed Changes**:
    *   Document `[values.js:isVectorMapValue]`, `[values.js:formatVectorSummary]`, and `[values.js:firestoreValueEquals]` in `Interface`.
*   **Rationale**: Keeps `ui/README.md` synchronized with exported `values.js` helpers.

#### 39. [ui/components/README.md](../../ui/components/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`ui/components/`)
*   **Proposed Changes**:
    *   Document the collapsed read-only `__vector__` pass-through, `cleanDoc`-aligned `firestoreValueEquals` diffing, and mode-switch `-0.0` preservation invariants for `[editor.js]` in `Invariants`.
*   **Rationale**: Keeps `ui/components/README.md` synchronized with structured editor invariants.

#### 40. [test-suite/tests/utils/common.ts](../../test-suite/tests/utils/common.ts) (`Modify`)

*   **Type / Scope**: Shared Jest Conformance Helpers (`test-suite/`)
*   **Proposed Changes**:
    *   Export three helpers with `strict: true` TypeScript signatures:
        ```ts
        export async function expectCloudLimitationCanary(
            description: string,
            fn: () => unknown,
            expectedCode = 3,
        ): Promise<void>;

        export async function callRawUnaryRpc<TReq extends object, TRes = unknown>(
            firestore: Firestore,
            rpc: string,
            req: TReq,
            tag = rpc,
        ): Promise<TRes>;

        export async function callRawStreamRpc<TReq extends object, TChunk = unknown>(
            firestore: Firestore,
            rpc: string,
            req: TReq,
            tag = rpc,
        ): Promise<TChunk[]>;
        ```
    *   Implement `expectCloudLimitationCanary` (using `connection` from `./firestore`) by executing `await Promise.resolve().then(fn)` inside `try/catch`, asserting `err?.code === expectedCode` on catch, and if `fn()` resolves without throwing: throwing `"CANARY UNLOCKED: Cloud Firestore now permits <description>; consider implementing parity in rust-firestore-emulator"` when `connection === 'CLOUD FIRESTORE'` and `new Error(...)` on the emulator.
    *   Implement `callRawUnaryRpc` and `callRawStreamRpc` by delegating to `@google-cloud/firestore`'s internal GAPIC client (`firestore.request(rpc, req, tag)` and `firestore.requestStream(rpc, false, req, tag)`, calling `stream.resume()` while collecting `'data'` events), automatically reusing emulator plaintext or Cloud ADC (Application Default Credentials) and `x-goog-request-params` database routing headers.
*   **Rationale**: Catches upstream Cloud Firestore restriction relaxations without letting synchronous throws escape, and provides `strict: true` raw GAPIC RPC access across `(default)`, `enterprise`, and `enterprise-pessimistic`.

#### 41. [test-suite/tests/utils/README.md](../../test-suite/tests/utils/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`test-suite/`)
*   **Proposed Changes**:
    *   Document `[common.ts:expectCloudLimitationCanary]`, `[common.ts:callRawUnaryRpc]`, and `[common.ts:callRawStreamRpc]` under `Interface`.
*   **Rationale**: Keeps `test-suite/tests/utils/README.md` synchronized with added conformance utilities.

#### 42. [test-suite/tests/9-pipeline-operators.test.ts](../../test-suite/tests/9-pipeline-operators.test.ts) (`Modify`)

*   **Type / Scope**: Enterprise Pipeline SDK Conformance Tests (`test-suite/`)
*   **Proposed Changes**:
    *   Remove `notImplementedInRust ||` from `cosineDistance`, `euclideanDistance`, `dotProduct`, and `vectorLength` at lines 822–841 and lines 1274–1293.
    *   Add `describe` blocks covering extreme IEEE 754 `vectorFunctions` edge cases (`1e100`, `1e-100`, `1e-200`, collinear cosine overshoot, `Infinity`), `vectorFunctions` error/null handling (`isError()` on dimension mismatch, non-vector field, and stored `[0, 0]` `cosineDistance`, plus `null` return on missing/null fields), and the `findNearest` pipeline stage (`euclidean`, `cosine`, `dot_product`, `limit`, `distanceField`, raw `executePipeline` rejections for `ArrayValue` `vector` and `StringValue` `field`/`distance_field`, `expectCloudLimitationCanary` passing `rawOptions: { threshold: 0.5 }` and `rawOptions: { distance_threshold: 0.5 }` on `findNearest({ ..., rawOptions })`, and streaming >200 unprojected 2,048-dimension vector documents exceeding 4 MiB total payload).
*   **Rationale**: Enables all previously gated pipeline vector operator tests and verifies pipeline `find_nearest` stage behavior and multi-frame gRPC response chunking past the 4 MiB client limit.

#### 43. [test-suite/tests/11-vector-search.test.ts](../../test-suite/tests/11-vector-search.test.ts) (`Create`)

*   **Type / Scope**: End-to-End Vector Search SDK & Live Cloud Conformance Suite (`test-suite/`)
*   **Proposed Changes**:
    *   Create a parameterized test suite reusing the standard 3-edition fixture from [firestore.ts](../../test-suite/tests/utils/firestore.ts) (`Standard` -> `(default)`, `Enterprise` -> `enterprise`, `Enterprise (pessimistic concurrency)` -> `enterprise-pessimistic`) incorporating every PR #194 SDK test scenario plus live-Cloud validation and edge-case scenarios:
        1.  Top-level & nested `FieldValue.vector` storage (`-0.0`, `±Infinity`, 1D, and 2,048D round-trip, `VectorValue` inside `ArrayValue`, `"____"` key allowed) + atomic subpath overwrite (`embedding.nested = "replaced"`).
        2.  Cross-type (`Array < Vector < Map`) and intra-type (`dimension_len`, then element-by-element) `orderBy` sorting.
        3.  `EUCLIDEAN` and `DOT_PRODUCT` `findNearest` ordering and `distanceResultField` population (including unscaled `1e-200`/`1e100`, Enterprise `1e200` `+Infinity` overflow, and edition-aware `+Infinity` candidate and `+Infinity queryVector` handling).
        4.  `COSINE` `findNearest` ordering and silent skipping of missing fields, arrays, strings, and mismatched dimensions, plus edition-aware zero-magnitude rejection (`9 FAILED_PRECONDITION` on Standard, `3 INVALID_ARGUMENT` on Enterprise) when an unfiltered `[0, 0]` candidate is scanned (with `notImplementedInJava ||` where the Java emulator diverges).
        5.  `DOT_PRODUCT` `findNearest` descending similarity ordering and scores.
        6.  Inclusive `distanceThreshold` filtering across `EUCLIDEAN`, `COSINE`, and `DOT_PRODUCT` (including raw `distanceThreshold: 0`).
        7.  Composite `where('category', '==', ...)` + `where('tags', 'array-contains', ...)` pre-filters on 3D nested `FieldPath('vectorQueryFields', 'nested')` (`[1, 1, 0]`, gated with `notImplementedInJava ||`), plus inequality `where('price', '<', 100).orderBy('price').findNearest(...)` verifying distance-tie ordering (`price ASC` before `__name__`).
        8.  `select()` projection shadowing/masking of `distanceResultField` (including `select('meta.author')` when `distanceResultField = 'meta'`) and `RunAggregationQuery` (`count` on Standard; `count`, `sum`, `average` with `NULL_VALUE` empty-set `SUM` on Enterprise).
        9.  `Listen` (`AddTarget` initial `findNearest` snapshot with `distanceResultField` followed by incremental base-query updates covering `d_init_far` exclusion, `dist` retention, deletions, and `price < 50` pre-filter exit; gated with `notImplementedInJava ||` on `listen-topk`).
        10. Live Cloud `ConcurrentTest` + raw GAPIC transactional `runQuery` / `commit` lock-scope oracle across all 3 editions via `callRawUnaryRpc` (`notImplementedInJava ||`; proving a concurrent write to a non-top-`K` pre-filtered vector candidate succeeds immediately on `Standard`, outside-collection writes succeed while scanned collection candidates/filtered-out docs contend until commit on `Enterprise (pessimistic concurrency)`, and entering top-`K` aborts `Enterprise` optimistic transactions), plus mid-query `COSINE` `NewTransaction` rollback verification (cleaning up Frame-0 transaction on live Cloud). Also align [test-suite/tests/3-pessimistic-transaction.test.ts](../../test-suite/tests/3-pessimistic-transaction.test.ts) (`45_000` ms `deadlock` timeout and 4 `JAVA EMULATOR` log-order compatibility assertions).
        11. Isolated collection-group search `db.collectionGroup('collection').where('runId', '==', runId).findNearest({ vectorField: 'tieEmbedding', ... })` across `tests/${id}/parents/doc/collection/x` vs. `tests/${id}/parents/doc-1/collection/x` proving segment-wise `'doc' < 'doc-1'` ordering without colliding with concurrent tests' `embedding` documents.
        12. Domain validation assertions (`notImplementedInJava ||`)—client SDK synchronous and wire checks (`0D`, `2049D`, `NaN` write, reserved `__custom__` key/transform writes, `limit: 0` / `1001`, and zero-magnitude `COSINE` query vector with edition-aware codes), raw GAPIC `commit` wire checks (`0D`, `2049D`, `NaN`, non-double element, missing `"value"`, extraneous keys, and reserved `__custom__` key/`updateMask`/`updateTransforms`), malformed filter/cursor vector checks (`raw-where-0d`, `raw-cursor-0d`, `raw-where-nan`), and `callRawStreamRpc` `runQuery` wire checks (`limit: 0`, `limit: 1001`, `0D`, `2049D`, Enterprise `raw-nan-2d`, and edition-aware zero-magnitude `COSINE` query vector)—plus `expectCloudLimitationCanary` tests scoped strictly to upstream Cloud feature restrictions (outer `StructuredQuery.limit`, non-zero `offset`, and `startAt`/`endAt` cursors combined with `findNearest` via `callRawStreamRpc`).
*   **Rationale**: Carries forward every PR #194 SDK test scenario while verifying live-Cloud validation, edition-aware transaction lock parity, isolation-safe collection-group tie-breaking, and scoped upstream feature canaries across all three database editions.

#### 44. [test-suite/tests/README.md](../../test-suite/tests/README.md) (`Modify`)

*   **Type / Scope**: Module Documentation (`test-suite/`)
*   **Proposed Changes**:
    *   Document `[11-vector-search.test.ts]` under `Interface` and update `1-*.test.ts`..`9-*.test.ts` file ranges in `Invariants` and `Side Effects` to `1-*.test.ts`..`11-*.test.ts`.
*   **Rationale**: Keeps `test-suite/tests/README.md` synchronized with the new vector search conformance suite.

#### 45. [test-suite/README.md](../../test-suite/README.md) (`Modify`)

*   **Type / Scope**: Package Documentation (`test-suite/`)
*   **Proposed Changes**:
    *   Update the `tests` subdirectory file range (line 21) from `10-rust-oracle-edge-cases.test.ts` to `11-vector-search.test.ts`.
*   **Rationale**: Keeps `test-suite/README.md` synchronized with the conformance test directory contents.

#### 46. [README.md](../../README.md) (`Modify`)

*   **Type / Scope**: Repository Root Documentation
*   **Proposed Changes**:
    *   Remove `- Nearest neighbors search` from `### Missing Features` (line 21) and include `VectorValue` embeddings and `findNearest` vector search in the `Driven by Need` feature summary (line 12).
*   **Rationale**: Updates top-level project capability documentation to reflect completed vector search support.

### Verification Plan

*   **Worktree Pre-Flight Bootstrap**:
    *   Initialize protobuf submodule headers: `git submodule update --init --recursive`
    *   Install test-suite dependencies: `npm --prefix test-suite ci`
*   **GCP Test Project (`firestore-test-406503`) Standard Edition Vector Indexes**:
    Already provisioned on `--database="(default)"` in `firestore-test-406503`; run these commands only when bootstrapping a different GCP test project (the `enterprise` and `enterprise-pessimistic` databases execute vector queries index-free):
    ```shell
    gcloud firestore indexes composite create --project=firestore-test-406503 --database="(default)" --collection-group=collection --query-scope=COLLECTION --field-config=vector-config='{"dimension":"2","flat":"{}"}',field-path=embedding
    gcloud firestore indexes composite create --project=firestore-test-406503 --database="(default)" --collection-group=collection --query-scope=COLLECTION --field-config=field-path=price,order=ASCENDING --field-config=vector-config='{"dimension":"2","flat":"{}"}',field-path=embedding
    gcloud firestore indexes composite create --project=firestore-test-406503 --database="(default)" --collection-group=collection --query-scope=COLLECTION --field-config=field-path=tags,array-config=CONTAINS --field-config=field-path=category,order=ASCENDING --field-config=vector-config='{"dimension":"3","flat":"{}"}',field-path=vectorQueryFields.nested
    gcloud firestore indexes composite create --project=firestore-test-406503 --database="(default)" --collection-group=collection --query-scope=COLLECTION_GROUP --field-config=field-path=runId,order=ASCENDING --field-config=vector-config='{"dimension":"2","flat":"{}"}',field-path=tieEmbedding
    ```
*   **Compilation/Build**: `cargo build --workspace --all-targets`
*   **Unit & Black-Box Integration Tests**: `just test` (`cargo nextest run --workspace` and `cargo test --doc --workspace --exclude googleapis`)
*   **Static Analysis, Formatting & Web UI Type-Check**:
    *   `just lint` (`cargo clippy --workspace` and `just ui-check` running `tsc --noEmit --allowJs --checkJs --strict` over `ui/app.js`)
    *   `cargo clippy --workspace --all-targets`
    *   `cargo clippy --workspace --no-default-features`
    *   `cargo +nightly fmt --all -- --check`
    *   `./test-suite/node_modules/.bin/tsc -p test-suite/tsconfig.json --noEmit` and `./test-suite/node_modules/.bin/prettier -c test-suite/tests`
*   **E2E SDK Integration & Live Cloud Conformance**:
    *   Against Rust emulator: `just sdk-test` (`cargo build --no-default-features && npm --prefix test-suite run test:rust-emulator`)
    *   Against live Google Cloud Firestore (`(default)`, `enterprise`, and `enterprise-pessimistic` in `firestore-test-406503`; requires active GCP `ADC` and unset `FIRESTORE_EMULATOR_HOST` per [AGENTS.md](../../AGENTS.md)): `env -u FIRESTORE_EMULATOR_HOST npm --prefix test-suite test -- 9-pipeline-operators 11-vector-search` (and full `env -u FIRESTORE_EMULATOR_HOST npm --prefix test-suite test`)

---

<!-- Provenance: Indicates initial structural drafting via EGM. -->

*Drafted with EGM (Elephant-Goldfish Model)*
