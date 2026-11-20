# Unifying Firestore `Value` `PartialEq` with `Ord` Semantics

*   **Author:** Paco van der Linden
*   **Status:** Implemented
*   **Date:** 2026-11-10

--------------------------------------------------------------------------------

## Overview

In the Firestore emulator, [`googleapis::google::firestore::v1::Value`](../../crates/googleapis/src/value_ext.rs) is the central data representation for document fields, query filters, cursors, field transforms, and pipeline expressions. Prior to this change, `Value` inherited a structural `#[derive(PartialEq)]` implementation from `prost-build` while implementing `Eq`, `PartialOrd`, and `Ord` manually in [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs) according to Firestore's cross-type comparison rules.

This design removes `prost-build`'s derived `PartialEq` from `Value` and `value::ValueType` in `$OUT_DIR/google.firestore.v1.rs`—as well as `LatLng` in `$OUT_DIR/google.r#type.rs`—inside [`crates/googleapis/build.rs`](../../crates/googleapis/build.rs) and implements `PartialEq` manually in [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs) so that `a == b` holds if and only if `a.cmp(b) == Ordering::Equal`. For database paths that require exact wire-representation identity—[`stage_stored_version`](../../crates/emulator-database/src/database/document.rs) and [`StagedDocument::stage_update`](../../crates/emulator-database/src/database.rs) (plus DNF disjunction deduplication in [`value_dnf_dedup_eq`](../../crates/emulator-database/src/database/query.rs)), where changing an integer `1` to a double `1.0` must produce a new document version—`Value` provides an explicit, recursive storage-identity comparison method (`Value::storage_eq`).

## The Problem

Firestore defines value equality and ordering across numeric representations (`IntegerValue(42)` equals `DoubleValue(42.0)`, `-0.0` equals `+0.0`, and `NaN` equals `NaN` for ordering and set membership). Previously, in [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs), `Value` implemented `Ord`, `PartialOrd`, and `Eq` using Firestore's ordering rules, but kept `prost-build`'s derived `PartialEq`, which compared Rust enum variants structurally:

1.  **Violation of Rust's `Eq` and `Ord` Trait Contract:** Rust's standard library requires that for any type implementing `Eq` and `Ord`, `a == b` must be equivalent to `a.cmp(&b) == Ordering::Equal`, and `a == a` must hold for all values (reflexivity). Under `prost`'s derived `PartialEq`, `Value::integer(1) == Value::double(1.0)` was `false` while `Value::integer(1).cmp(&Value::double(1.0))` was `Ordering::Equal`, and `Value::double(f64::NAN) == Value::double(f64::NAN)` was `false`. Standard library types and algorithms (`BTreeSet`, `BTreeMap`, `slice::contains`, `Vec::retain`) rely on this contract for deterministic behavior.
2.  **Error-Prone Workarounds Across Query, Transform, and Pipeline Code:** Because `==`, `!=`, and `slice::contains` on `Value` used `prost`'s structural `PartialEq`, contributors had to bypass standard Rust operators:
    *   [`FieldFilter::eval`](../../crates/emulator-database/src/database/query/filter.rs) worked around `PartialEq` for `FieldOperator::Equal` using `value <= &self.value && value >= &self.value` with `#[allow(clippy::double_comparisons)]`, while `FieldOperator::NotEqual`, `FieldOperator::ArrayContains`, and `FieldOperator::In` used `!=` and `.contains()` and failed to match `IntegerValue` against `DoubleValue`.
    *   [`apply_transform`](../../crates/emulator-database/src/database/commit.rs) used `array.contains(&el)` for `AppendMissingElements` and `elements.values.contains(v)` for `RemoveAllFromArray`, failing to deduplicate or remove equivalent integer/double values.
    *   [`pipeline::function::evaluate`](../../crates/emulator-database/src/database/pipeline/function.rs) used `a == b` and `a != b` for `"equal"` and `"not_equal"`, disagreeing with `"less_than_or_equal"` and `"greater_than_or_equal"` in the same match block.
    *   Introducing a helper method such as `Value::is_firestore_equal` (as attempted in branch `feat-array-contains-any`) leaves `==` and `.contains()` compiling with the wrong behavior whenever a contributor forgets the helper.
3.  **Distinguishing Domain Equality from Storage Identity:** While query evaluation, array transforms, and pipeline operations require Firestore semantic equality (`1 == 1.0`), document persistence in [`stage_stored_version`](../../crates/emulator-database/src/database/document.rs) (and [`DocumentContents::maybe_update`](../../crates/emulator-database/src/database/document.rs) under `#[cfg(test)]`) checks whether an incoming write modifies the stored document fields. Overwriting `IntegerValue(1)` with `DoubleValue(1.0)` (or `-0.0` with `+0.0`) changes the stored field representation and must create a new [`StoredDocumentVersion`](../../crates/emulator-database/src/database/document.rs).

The implemented solution makes Firestore semantic equality the default `PartialEq` on `Value` (aligned with `Ord::cmp`) so standard Rust operators and slice methods behave according to Firestore rules everywhere by default (with edition-aware `null`/`NaN` guards layered explicitly in `FieldFilter::eval`), while introducing an explicit, recursive `Value::storage_eq` method for persistence change detection, DNF deduplication, and wire-representation test assertions.

## Goals

*   **Unify `PartialEq` and `Ord` on `Value` and `LatLng`:** Strip `prost-build`'s derived `PartialEq` from `Value` and `value::ValueType` (in `$OUT_DIR/google.firestore.v1.rs`) and `LatLng` (in `$OUT_DIR/google.r#type.rs`) in [`crates/googleapis/build.rs`](../../crates/googleapis/build.rs) (guarded by a build-time assertion) and implement `PartialEq` in [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs) so `a == b` is equivalent to `a.cmp(b) == Ordering::Equal`.
*   **Guarantee Total, Non-Panicking Comparison:** Handle `Value { value_type: None }` (`Value::default()`) in `Ord::cmp` and `PartialEq::eq` without panicking, and compare `FieldReferenceValue`, `VariableReferenceValue`, `FunctionValue`, and `PipelineValue` deterministically without emitting `tracing::warn!` logs.
*   **Preserve Transitivity Across All Numeric & Coordinate Values:** Incorporate exact `i64`-vs-`f64` comparisons (including integers beyond $2^{53}$), `-0.0 == +0.0` normalization (in both `DoubleValue` and `LatLng`), and reflexive `NaN == NaN` ordering.
*   **Avoid Heap Allocations on Regular Map & Array Equality:** Fast-path `ArrayValue` (`a.values == b.values`) and non-vector `MapValue` (`a.fields.len() == b.fields.len() && a.fields == b.fields`) in `PartialEq::eq` so non-vector map equality runs in $O(K)$ without sorting keys into temporary heap allocations, while valid `VectorValue` maps are classified separately and compared element-wise.
*   **Provide Recursive Storage-Identity Comparison (`Value::storage_eq`):** Expose a public, recursive `Value::storage_eq` method (and a companion `Value::fields_storage_eq` helper for `HashMap<String, Value>`) that compares `DoubleValue` via `f64::to_bits()`, compares `ReferenceValue` strings literally, and recurses through nested containers for use in [`stage_stored_version`](../../crates/emulator-database/src/database/document.rs), [`StagedDocument::stage_update`](../../crates/emulator-database/src/database.rs), [`value_dnf_dedup_eq`](../../crates/emulator-database/src/database/query.rs), and representation-sensitive tests.
*   **Use Standard Rust Equality Operators Across `emulator-database`:** Replace `#[allow(clippy::double_comparisons)]` in [`FieldFilter::eval`](../../crates/emulator-database/src/database/query/filter.rs) with edition-aware `value_matches_eq` / `operand_contains` helpers built directly on `==` instead of double-comparison workarounds or custom `is_firestore_equal` loops.

## Non-Goals

*   **Implementing `std::hash::Hash` for `Value`:** Ordered collections (`BTreeSet` and `BTreeMap`) are already used for pipeline `distinct` and `aggregate` stages; canonical hashing across `i64`/`f64` and `HashMap` is out of scope.
*   **Introducing a Separate Internal `Value` AST in `emulator-database`:** `googleapis::google::firestore::v1::Value` remains the shared value representation across crates to avoid recursive tree conversions at gRPC and HTTP boundaries.
*   **Changing Cross-Type Compatibility Matrix Rules (`is_compatible_with`):** The type-compatibility table in `Value::is_compatible_with` remains the compatibility check for ordering operators (`<`, `<=`, `>`, `>=`), with [`FieldFilter::eval`](../../crates/emulator-database/src/database/query/filter.rs) additionally filtering `null` and `NaN` operands on Standard Edition (`!is_enterprise`).

## Open Questions

*   None. All open design questions were resolved during the brainstorm phase.

## Critical User Journeys (CUJs)

### CUJ 1: Cross-Type Numeric Equality in Queries, Array Transforms, and Pipelines

*   **Given**: A stored Firestore document with field `count: IntegerValue(42)` and field `tags: ArrayValue([IntegerValue(10), DoubleValue(f64::NAN)])`.
*   **When**: A client executes a query filter (`Equal`, `NotEqual`, `In`, `ArrayContains`, or `ArrayContainsAny`), a pipeline expression (`equal` or `not_equal`), or an array field transform (`AppendMissingElements` / `RemoveAllFromArray`) using `DoubleValue(42.0)` or `DoubleValue(10.0)` or `DoubleValue(f64::NAN)`.
*   **Then**: Standard Rust `==`, `!=`, and `slice::contains` on `Value` treat `IntegerValue(42)` and `DoubleValue(42.0)` as equal, treat `IntegerValue(10)` and `DoubleValue(10.0)` as equal, treat `DoubleValue(f64::NAN)` and `DoubleValue(f64::NAN)` as equal, and distinguish `IntegerValue(i64::MAX)` from `DoubleValue(i64::MAX as f64)` without precision loss. On query filters (`FieldFilter::eval`), Enterprise Edition matches `null == null` and `NaN == NaN` directly via `value_matches_eq`, whereas Standard Edition (`!is_enterprise`) explicitly rejects top-level `null`/`NaN` matches in `Equal`, `ArrayContains`, `In`, and `ArrayContainsAny` (and guards `null` in `NotEqual`/`NotIn`).

### CUJ 2: Representation-Preserving Document Updates in `stage_stored_version`

*   **Given**: An existing document stored in `emulator-database` with `{ "score": IntegerValue(1), "nested": MapValue({ "items": ArrayValue([IntegerValue(5)]) }) }`.
*   **When**: A client writes an update changing `"score"` to `DoubleValue(1.0)` or changing `"nested.items"` to `ArrayValue([DoubleValue(5.0)])`, or re-writes the exact same document containing a `DoubleValue(f64::NAN)` field.
*   **Then**: [`stage_stored_version`](../../crates/emulator-database/src/database/document.rs) (and [`StagedDocument::stage_update`](../../crates/emulator-database/src/database.rs) for transactional round-trips) evaluates `Value::fields_storage_eq(&last.fields, &fields)`: it detects the `IntegerValue` to `DoubleValue` change (both at the top level and nested inside arrays/maps) and creates a new [`StoredDocumentVersion`](../../crates/emulator-database/src/database/document.rs), while treating an unchanged `DoubleValue(f64::NAN)` with identical bits as a no-op that does not create a spurious version.

### CUJ 3: Total, Non-Panicking Comparison on Default or Unset Protobuf `Value`s

*   **Given**: A `Value::default()` (`Value { value_type: None }`) constructed in unit tests or decoded from an empty protobuf `Value` submessage before request validation.
*   **When**: Code compares the `Value` (or a parent `Document`, `Write`, `Cursor`, or `StructuredQuery`) using `==`, `!=`, `<`, `<=`, `>`, `>=`, or `Ord::cmp`.
*   **Then**: The comparison completes deterministically without panicking (`Value::default() == Value::default()` evaluates to `true`, and `Value { value_type: None }` orders before all populated `ValueType` variants).

## Technical Plan (Architecture)

The architecture separates **Firestore domain comparison** (the default `PartialEq`, `Eq`, `PartialOrd`, and `Ord` traits on `Value` and `LatLng`) from **storage-representation identity** (`Value::storage_eq` and `Value::fields_storage_eq`).

```mermaid
flowchart TD
    Proto["firestore.proto / document.proto"] --> ProstBuild["tonic-prost-build in crates/googleapis/build.rs"]
    ProstBuild --> PostProc["build.rs Post-Processor (strips PartialEq from Value, ValueType & LatLng with assert!)"]
    PostProc --> GenRs["$OUT_DIR/google.firestore.v1.rs & google.r#type.rs"]
    GenRs --> ValueExt["crates/googleapis/src/value_ext.rs"]

    ValueExt --> TraitImpl["impl PartialEq / Eq / PartialOrd / Ord for Value, ValueType & LatLng"]
    ValueExt --> StorageEq["Value::storage_eq & Value::fields_storage_eq"]

    TraitImpl --> Filter["query/filter.rs (edition-aware value_matches_eq / operand_contains)"]
    TraitImpl --> Transform["database.rs (AppendMissingElements, RemoveAllFromArray)"]
    TraitImpl --> Pipeline["pipeline/ (function.rs, distinct.rs, aggregate.rs, sort.rs)"]

    StorageEq --> DocUpdate["document.rs (stage_stored_version) & database.rs (StagedDocument::stage_update)"]
    StorageEq --> DnfDedup["query.rs (value_dnf_dedup_eq)"]
    StorageEq --> TypeTests["Type-sensitive unit & integration tests"]
```

### 1. Build-Time Suppression of Derived `PartialEq` (`crates/googleapis/build.rs`)

`prost-build` emits `#[derive(Clone, PartialEq, ::prost::Message)]` on every message and `#[derive(Clone, PartialEq, ::prost::Oneof)]` on every `oneof` without a configuration flag to disable `PartialEq`. Custom `Serialize`/`Deserialize` implementations for `Value` and `LatLng` live in [`crates/googleapis/src/ser.rs`](../../crates/googleapis/src/ser.rs), so [`crates/googleapis/build.rs`](../../crates/googleapis/build.rs) strips `PartialEq, ` directly via `strip_partial_eq_derives` right after `.compile_protos(...)`:

1.  Read `$OUT_DIR/google.firestore.v1.rs` and `$OUT_DIR/google.r#type.rs` and verify with `assert_eq!` that each target declaration appears exactly once:
    *   `Value` (`google.firestore.v1.rs`): `"#[derive(Clone, PartialEq, ::prost::Message)]\npub struct Value"` -> `"#[derive(Clone, ::prost::Message)]\npub struct Value"`
    *   `ValueType` (`google.firestore.v1.rs`): `"    #[derive(Clone, PartialEq, ::prost::Oneof)]\n    pub enum ValueType"` -> `"    #[derive(Clone, ::prost::Oneof)]\n    pub enum ValueType"`
    *   `LatLng` (`google.r#type.rs`): `"#[derive(Clone, Copy, PartialEq, ::prost::Message)]\npub struct LatLng"` -> `"#[derive(Clone, Copy, ::prost::Message)]\npub struct LatLng"`
2.  If a future `prost-build` upgrade alters attribute ordering or formatting, `cargo build` fails immediately in `build.rs` with an explicit diagnostic message.
3.  All surrounding `prost` messages (`Document`, `ArrayValue`, `MapValue`, `Function`, `Pipeline`, `Cursor`, `StructuredQuery`, `Write`) keep their generated `#[derive(Clone, PartialEq, ::prost::Message)]` attributes, which delegate field comparisons to the manual `PartialEq` implementations on `Value` and `LatLng`.

### 2. Total Ordering and Fast-Path `PartialEq` (`crates/googleapis/src/value_ext.rs`)

In [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs):

1.  **Total Handling of `Value { value_type: None }` and `VectorValue` Classification:**
    *   `Value::value_type_order(&self) -> u8` delegates to `fn value_type_rank(vt: Option<&ValueType>) -> u8` (backed by `fn classify_value_type_rank(vt: Option<&ValueType>) -> (u8, Option<&[Value]>)`), matching on `self.value_type.as_ref()` instead of calling the panicking `self.value_type()` accessor.
    *   `None` is assigned rank `0` (below `NullValue` at rank `1`). Valid Firestore `VectorValue` maps (`classify_vector_map`) are classified separately at rank `11` (comparing by `(va.len(), va)`), distinct from non-vector/invalid `MapValue`s at rank `12`. Non-document expression variants (`FieldReferenceValue`, `VariableReferenceValue`, `FunctionValue`, `PipelineValue`) occupy `u8::MAX - 3..=u8::MAX`.
2.  **Transitive Numeric and Coordinate Ordering:**
    *   `DoubleValue(a)` vs `DoubleValue(b)` uses `cmp_f64(*a, *b)`, which matches on `(a.is_nan(), b.is_nan())`: `(true, true) => Ordering::Equal`, `(true, false) => Ordering::Less`, `(false, true) => Ordering::Greater`, and for `(false, false)` normalizes `-0.0` and `+0.0` to `0.0` before delegating to `f64::total_cmp`.
    *   `IntegerValue(i)` vs `DoubleValue(d)` (and vice versa) uses `cmp_i64_f64(i, d)`, which compares 64-bit integers against non-`NaN` `f64` values without precision loss above $2^{53}$ by checking the exact power-of-two bounds `i64::MIN as f64` ($-2^{63}$) and `9_223_372_036_854_775_808.0_f64` ($+2^{63}$) before comparing `i` with `d.floor() as i64` and checking `d == d.floor()`.
    *   `<LatLng as PartialEq>::eq` (`self.cmp(other).is_eq()`) and `<LatLng as Ord>::cmp` (`GeoPointValue`) use `cmp_f64` on `latitude` and `longitude` so `-0.0` and `+0.0` coordinates compare equal and `NaN` coordinates preserve strict total ordering.
3.  **Deterministic Comparison of Expression Variants:**
    *   `FieldReferenceValue(a)` vs `FieldReferenceValue(b)` and `VariableReferenceValue(a)` vs `VariableReferenceValue(b)` compare `a.cmp(b)`.
    *   `Function`, `Pipeline`, and `pipeline::Stage` implement `Eq`, `PartialOrd`, and `Ord` in `value_ext.rs` (sorting `options` by key via `.iter().sorted()`, matching `MapValue`), so `FunctionValue(a)` vs `FunctionValue(b)` and `PipelineValue(a)` vs `PipelineValue(b)` compare `a.cmp(b)` deterministically without emitting `tracing::warn!` logs.
4.  **`PartialEq` Implementation on `ValueType` and `Value`:**
    *   A shared helper `fn cmp_value_type(a: &ValueType, b: &ValueType) -> cmp::Ordering` encapsulates the variant rank (including `VectorValue` vs regular `MapValue`) and value ordering for `ValueType`.
    *   `ValueType::eq(&self, other: &Self) -> bool` (`impl PartialEq for ValueType` and `impl Eq for ValueType`) is equivalent to `cmp_value_type(self, other).is_eq()`, with allocation-free fast paths:
        *   When both values are `MapValue`s: classifies both via `classify_vector_map`; valid vectors compare `va.len() == vb.len() && va == vb`, one-valid/one-not returns `false`, and non-vector/invalid maps compare `a.fields.len() == b.fields.len() && a.fields == b.fields` ($O(1)$ length check + $O(K)$ lookups without sorting key vectors).
        *   When both values are `ArrayValue`s: `a.values == b.values` (`Vec::eq` checks `len()` in $O(1)$ before comparing elements via `Value::eq`).
        *   When both values are `StringValue`s or `BytesValue`s: `a == b` ($O(1)$ length check before byte comparison).
        *   All other combinations delegate to `cmp_value_type(self, other).is_eq()`.
    *   `Value::eq(&self, other: &Self) -> bool` delegates to `ValueType::eq` via `match (&self.value_type, &other.value_type) { (None, None) => true, (Some(a), Some(b)) => a == b, _ => false }`, and `<Value as Ord>::cmp` delegates populated variants to `cmp_value_type`.

### 3. Recursive Storage-Identity Comparison (`Value::storage_eq` & `Value::fields_storage_eq`)

To detect representation changes during document writes (where changing `IntegerValue(1)` to `DoubleValue(1.0)` or `-0.0` to `+0.0` must create a new document version, while re-writing an identical `NaN` must not), `Value` provides two public methods in [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs):

*   `pub fn storage_eq(&self, other: &Self) -> bool`:
    *   Performs an exhaustive match over `(self.value_type.as_ref(), other.value_type.as_ref())` without delegating to `PartialEq` on `Value`, `ValueType`, `ArrayValue`, or `MapValue`:
        *   `DoubleValue(a)` vs `DoubleValue(b)`: `a.to_bits() == b.to_bits()`.
        *   `IntegerValue(a)` vs `DoubleValue(b)`: always `false`.
        *   `ReferenceValue(a)` vs `ReferenceValue(b)`: raw string equality `a == b` (without [`REFERENCE_NAME_MIN_ID`](../../crates/googleapis/src/value_ext.rs) normalization, which in `Ord::cmp` maps the sentinel path segment `"__id-9223372036854775808__"` to `""`).
        *   `GeoPointValue(a)` vs `GeoPointValue(b)`: `a.latitude.to_bits() == b.latitude.to_bits() && a.longitude.to_bits() == b.longitude.to_bits()`.
        *   `ArrayValue(a)` vs `ArrayValue(b)`: `a.values.len() == b.values.len()` and element-wise `storage_eq`.
        *   `MapValue(a)` vs `MapValue(b)`: `Self::fields_storage_eq(&a.fields, &b.fields)`.
        *   `FunctionValue(a)` vs `FunctionValue(b)` and `PipelineValue(a)` vs `PipelineValue(b)`: recursive `storage_eq` across `name`, `args`, `options`, and `stages`.
        *   All other same-variant primitives (`NullValue`, `BooleanValue`, `IntegerValue`, `TimestampValue`, `StringValue`, `BytesValue`, `FieldReferenceValue`, `VariableReferenceValue`) use exact equality; mismatched variants return `false`.
*   `pub fn fields_storage_eq(a: &HashMap<String, Self>, b: &HashMap<String, Self>) -> bool`:
    *   Returns `a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|ov| v.storage_eq(ov)))`.

### 4. Database Call-Site Updates (`crates/emulator-database`)

*   **Document Version Detection & DNF Deduplication ([`crates/emulator-database/src/database/document.rs`](../../crates/emulator-database/src/database/document.rs), [`crates/emulator-database/src/database/commit.rs`](../../crates/emulator-database/src/database/commit.rs), [`crates/emulator-database/src/database/query.rs`](../../crates/emulator-database/src/database/query.rs)):**
    *   In `stage_stored_version` (and `StagedDocument::stage_update` for multi-write round-trips), `Value::fields_storage_eq(&last.fields, &fields)` prevents no-op re-writes from creating spurious versions while preserving `IntegerValue` <-> `DoubleValue` and `-0.0` <-> `+0.0` updates (`DocumentContents::maybe_update` is retained under `#[cfg(test)]`). `value_dnf_dedup_eq` in `query.rs` also uses `Value::storage_eq`.
*   **Query Filter Evaluation ([`crates/emulator-database/src/database/query/filter.rs`](../../crates/emulator-database/src/database/query/filter.rs)):**
    *   `FieldFilter::eval(&self, version: &StoredDocumentVersion, is_enterprise: bool) -> bool` replaces the double-comparison workaround with edition-aware `value_matches_eq` and `FieldFilter::operand_contains` built on `==`: Enterprise Edition matches `null == null` and `NaN == NaN` directly, whereas Standard Edition (`!is_enterprise`) rejects top-level `null` and `NaN` matches in `Equal`, `ArrayContains`, `In`, and `ArrayContainsAny`, guards `null` in `NotEqual` and `NotIn`, and extends the inequality guard (`needs_type_compat()`) to reject `null` and `NaN` when `!is_enterprise`.
*   **Field Transforms & Pipeline Functions ([`crates/emulator-database/src/database/commit.rs`](../../crates/emulator-database/src/database/commit.rs), [`crates/emulator-database/src/database/pipeline/function.rs`](../../crates/emulator-database/src/database/pipeline/function.rs)):**
    *   `AppendMissingElements` (`!array.contains(&el)`), `RemoveAllFromArray` (`!elements.values.contains(v)`), and pipeline `"equal"` / `"not_equal"` (`a == b`, `a != b`) require no helper methods and operate using the unified `PartialEq`.

## Alternatives Considered

### Alternative A: Dedicated `Value::is_firestore_equal` Helper Method

*   **Pros**: Requires no post-processing in [`crates/googleapis/build.rs`](../../crates/googleapis/build.rs); leaves `prost`'s derived `PartialEq` in place for persistence checks in [`document.rs`](../../crates/emulator-database/src/database/document.rs).
*   **Cons**: Every query filter, array transform, and pipeline expression must remember to avoid `==`, `!=`, and `slice::contains` and instead write manual `.iter().any(|x| x.is_firestore_equal(...))` loops. Leaves `Value`'s `Eq` and `Ord` implementations in violation of Rust's `std::cmp::Ord` trait contract.
*   **Reason for Rejection**: Error-prone because developers can easily forget which equality method to call (as already happened in [`crates/emulator-database/src/database/pipeline/function.rs`](../../crates/emulator-database/src/database/pipeline/function.rs)), and leaves `BTreeSet`, `BTreeMap`, and `slice::contains` inconsistent with `Ord`.

### Alternative B: Newtype Wrapper `FirestoreValue(Value)` (with `ref-cast` or `bytemuck`)

*   **Pros**: Isolates Firestore `PartialEq` and `Ord` on a wrapper type; crates such as `ref-cast` or `bytemuck::TransparentWrapper` allow casting `&Value` to `&FirestoreValue` and `&[Value]` to `&[FirestoreValue]` without violating `unsafe_code = "forbid"` in workspace code.
*   **Cons**: As long as `Value` retains `prost`'s derived `PartialEq`, writing `a == b` or `arr.contains(&x)` on `Value` or `&[Value]` still compiles with the wrong semantics. Conversely, stripping `PartialEq` from `Value` without providing a replacement `impl PartialEq for Value` breaks `#[derive(PartialEq)]` on all surrounding `prost`-generated messages (`Document`, `ArrayValue`, `MapValue`, `Cursor`, `StructuredQuery`, `Write`).
*   **Reason for Rejection**: Replaces the risk of forgetting `.is_firestore_equal()` with the identical risk of forgetting `FirestoreValue::ref_cast()` without compiler enforcement, while adding wrapper noise and external crate dependencies.

### Alternative C: Separate Internal `emulator_database::Value` Domain Enum

*   **Pros**: Decouples internal database storage and query evaluation from `prost`-generated wire types (`googleapis::google::firestore::v1::Value`).
*   **Cons**: Requires recursively converting and allocating every `Document` and `Value` tree at the gRPC (`emulator-grpc`) and HTTP (`emulator-http`) boundaries on every read, write, and query response, and duplicates value constructors and accessors across crates.
*   **Reason for Rejection**: Introduces unnecessary heap allocations and boilerplate across the workspace for a discrepancy confined to `PartialEq`.

### Alternative D: AST Parsing (`syn` + `prettyplease`) in `crates/googleapis/build.rs`

*   **Pros**: Modifies `$OUT_DIR/google.firestore.v1.rs` and `$OUT_DIR/google.r#type.rs` via a parsed syntax tree independent of whitespace or attribute formatting.
*   **Cons**: Adds extra build dependencies (`syn`, `quote`, `prettyplease`) and full-file parsing/formatting time to `crates/googleapis/build.rs`.
*   **Reason for Rejection**: Matching the exact generated `#[derive(Clone, ...)]` + `pub struct Value` / `pub enum ValueType` / `pub struct LatLng` declarations with `assert_eq!(contents.matches(target).count(), 1, ...)` provides a dependency-free string replacement that fails fast if `prost-build` changes formatting.

### Alternative E: Implementing `Value::storage_eq` via `self.value_type == other.value_type` or `prost::Message::encode_to_vec`

*   **Pros**: Minimal code in `crates/googleapis/src/value_ext.rs`.
*   **Cons**: `ValueType`'s `PartialEq` delegates `ArrayValue` and `MapValue` comparisons back to `Value::eq` (Firestore equality), which would cause nested updates from `{"a": [1]}` to `{"a": [1.0]}` to be ignored during document persistence, and uses IEEE 754 `f64::eq` (`NaN != NaN`, `-0.0 == +0.0`). `prost::Message::encode_to_vec` is non-deterministic for `MapValue` because `HashMap<String, Value>` iteration order is randomized.
*   **Reason for Rejection**: Fails correctness requirements for nested containers, `NaN` idempotency, and map key ordering.

--------------------------------------------------------------------------------

## Non-Functional Requirements

*   **Performance & Latency:** `PartialEq::eq` on non-vector `MapValue` uses `a.fields.len() == b.fields.len() && a.fields == b.fields` ($O(1)$ length check followed by $O(K)$ hash lookups with zero heap allocations) rather than `Ord::cmp`'s $O(K \log K)$ key-sorting allocation. `VectorValue`, `ArrayValue`, `StringValue`, and `BytesValue` equality also short-circuit in $O(1)$ when lengths differ.
*   **Crash Resilience (`panic = "abort"`):** Because `[profile.release]` in [`Cargo.toml`](../../Cargo.toml) sets `panic = "abort"`, `PartialEq::eq`, `Ord::cmp`, and `Value::storage_eq` are total over all representable `Value` states, including `Value { value_type: None }` (`Value::default()`).
*   **Build-Time Safety:** The post-processing step in [`crates/googleapis/build.rs`](../../crates/googleapis/build.rs) asserts exact match counts when stripping `PartialEq` from `Value`, `ValueType`, and `LatLng`, failing the build immediately with an explicit message if a future `prost-build` upgrade changes the generated attribute layout.

--------------------------------------------------------------------------------

## Detailed Implementation Plan

### Target Files to Create/Modify

#### 1. [`crates/googleapis/build.rs`](../../crates/googleapis/build.rs) (Modify)

*   **Type / Scope**: Build Configuration (`googleapis` crate)
*   **Proposed Changes**:
    *   After `.compile_protos(...)`, run `strip_partial_eq_derives` on `$OUT_DIR/google.firestore.v1.rs` and `$OUT_DIR/google.r#type.rs`:
        *   `Value` target: `"#[derive(Clone, PartialEq, ::prost::Message)]\npub struct Value"` -> `"#[derive(Clone, ::prost::Message)]\npub struct Value"`
        *   `ValueType` target: `"    #[derive(Clone, PartialEq, ::prost::Oneof)]\n    pub enum ValueType"` -> `"    #[derive(Clone, ::prost::Oneof)]\n    pub enum ValueType"`
        *   `LatLng` target: `"#[derive(Clone, Copy, PartialEq, ::prost::Message)]\npub struct LatLng"` -> `"#[derive(Clone, Copy, ::prost::Message)]\npub struct LatLng"`
    *   Verify with `assert_eq!(contents.matches(target).count(), 1, ...)` that each target string appears exactly once so any future `prost-build` formatting change fails compilation with an explicit diagnostic message in `build.rs`.
*   **Rationale**: Suppresses `prost-build`'s derived structural `PartialEq` on `Value`, `value::ValueType`, and `LatLng` without introducing new build-time crate dependencies, allowing custom `PartialEq` implementations in `crates/googleapis/src/value_ext.rs`.

#### 2. [`crates/googleapis/Cargo.toml`](../../crates/googleapis/Cargo.toml) (Modify)

*   **Type / Scope**: Crate Manifest (`googleapis` crate)
*   **Proposed Changes**:
    *   Remove `tracing = { workspace = true }` from `[dependencies]`, as [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs) was the only file in `googleapis` referencing `tracing` and `[lints.rust]` enforces `unused_crate_dependencies = "warn"`.
*   **Rationale**: Keeps `cargo clippy --workspace --all-targets -- -D warnings` clean after removing the `tracing::warn!` stubs in `value_ext.rs`.

#### 3. [`crates/googleapis/src/value_ext.rs`](../../crates/googleapis/src/value_ext.rs) (Modify)

*   **Type / Scope**: Rust Library Module & Unit Tests (`googleapis` crate)
*   **Proposed Changes**:
    *   **Total Handling of `None` & `VectorValue` Classification in `value_type_order` and `Ord::cmp`:** Extract `fn classify_value_type_rank(vt: Option<&ValueType>) -> (u8, Option<&[Value]>)` and `fn value_type_rank(vt: Option<&ValueType>) -> u8`, and update `Value::value_type_order(&self) -> u8` and `<Value as Ord>::cmp` to match on `self.value_type.as_ref()` instead of calling `self.value_type()`. Assign `None` rank `0`, valid `VectorValue` maps rank `11`, non-vector/invalid `MapValue`s rank `12`, and `FieldReferenceValue`/`VariableReferenceValue`/`FunctionValue`/`PipelineValue` ranks `u8::MAX - 3..=u8::MAX`.
    *   **Exact Numeric & Float Helpers (`cmp_f64` and `cmp_i64_f64`):**
        *   Add `fn cmp_f64(a: f64, b: f64) -> cmp::Ordering` matching on `(a.is_nan(), b.is_nan())` and normalizing `-0.0` and `+0.0` to `0.0` before delegating to `f64::total_cmp`.
        *   Add `fn cmp_i64_f64(i: i64, d: f64) -> cmp::Ordering` comparing 64-bit integers against non-`NaN` `f64` values without precision loss above $2^{53}$ using `-2^63` (`i64::MIN as f64`) and `+2^63` (`9_223_372_036_854_775_808.0_f64`).
    *   **Complete `PartialEq` & `Ord::cmp` for Expression Variants & `LatLng`:**
        *   Import `Pipeline` and `pipeline::Stage` from `crate::google::firestore::v1` and implement `Eq`, `PartialOrd`, and `Ord` for `Function`, `Pipeline`, and `pipeline::Stage` (comparing `name`, `args`, and `.options.iter().sorted()`, and `stages` for `Pipeline`), removing `use tracing::warn;` and the `tracing::warn!` macro calls.
        *   In `cmp_value_type(a: &ValueType, b: &ValueType) -> cmp::Ordering`, compare `FieldReferenceValue`, `VariableReferenceValue`, `FunctionValue`, and `PipelineValue` deterministically.
        *   Implement `PartialEq for LatLng` (`self.cmp(other).is_eq()`) and update `<LatLng as Ord>::cmp` to use `cmp_f64` on `latitude` and `longitude`.
    *   **Manual `PartialEq` on `value::ValueType` and `Value`:**
        *   Implement `PartialEq` and `Eq` on `value::ValueType`: classify `MapValue` pairs via `classify_vector_map` (comparing valid vectors by `(va.len(), va)` and fast-pathing non-vector/invalid maps with `a.fields.len() == b.fields.len() && a.fields == b.fields`), fast-path `ArrayValue` (`a.values == b.values`), `StringValue` (`a == b`), and `BytesValue` (`a == b`), and delegate all other cases to `cmp_value_type(self, other).is_eq()`.
        *   Implement `PartialEq` for `Value` by delegating to `ValueType::eq`, and delegate `<Value as Ord>::cmp` populated variants to `cmp_value_type`.
    *   **Recursive Storage-Identity API (`Value::storage_eq` & `Value::fields_storage_eq`):**
        *   Add `pub fn storage_eq(&self, other: &Self) -> bool` on `Value` matching `(self.value_type.as_ref(), other.value_type.as_ref())` exhaustively (comparing `DoubleValue` and `LatLng` via `.to_bits()`, `ReferenceValue` raw strings, and recursing through `ArrayValue`, `MapValue`, `FunctionValue`, and `PipelineValue`, returning `false` on mismatched variants including `IntegerValue(1)` vs `DoubleValue(1.0)`).
        *   Add `pub fn fields_storage_eq(a: &HashMap<String, Self>, b: &HashMap<String, Self>) -> bool` on `Value`.
    *   **Unit Tests in `mod tests`:**
        *   Verify `PartialEq`/`Ord` coherence across `IntegerValue` and `DoubleValue`, `Value::default()`, `LatLng`, `FieldReferenceValue`, `VariableReferenceValue`, `FunctionValue`, `PipelineValue`, `ArrayValue`, `VectorValue`, and `MapValue`, plus `Value::storage_eq` / `Value::fields_storage_eq`.
*   **Rationale**: Restores Rust's `Eq`/`Ord` coherence contract on `Value` and `LatLng` and provides an explicit, recursive storage-identity check for persistence, DNF deduplication, and representation-level tests.

#### 4. [`crates/emulator-database/src/database/document.rs`](../../crates/emulator-database/src/database/document.rs), [`crates/emulator-database/src/database/commit.rs`](../../crates/emulator-database/src/database/commit.rs), & [`crates/emulator-database/src/database/query.rs`](../../crates/emulator-database/src/database/query.rs) (Modify)

*   **Type / Scope**: Document Storage, Staging & Query DNF (`emulator-database` crate)
*   **Proposed Changes**:
    *   Use `Value::fields_storage_eq` in [`stage_stored_version`](../../crates/emulator-database/src/database/document.rs) (with [`DocumentContents::maybe_update`](../../crates/emulator-database/src/database/document.rs) retained under `#[cfg(test)]` delegating to it) and [`StagedDocument::stage_update`](../../crates/emulator-database/src/database/commit.rs), and use `Value::storage_eq` in [`value_dnf_dedup_eq`](../../crates/emulator-database/src/database/query.rs).
    *   Add unit tests verifying representation changes (`IntegerValue(1)` -> `DoubleValue(1.0)`) create new `StoredDocumentVersion`s while identical `NaN` re-writes remain no-ops.
*   **Rationale**: Prevents document updates from dropping writes that change stored field representation between `IntegerValue` and `DoubleValue` or `-0.0` and `+0.0`.

#### 5. [`crates/emulator-database/src/database/query/filter.rs`](../../crates/emulator-database/src/database/query/filter.rs) (Modify)

*   **Type / Scope**: Query Filter Evaluation (`emulator-database` crate)
*   **Proposed Changes**:
    *   In [`FieldFilter::eval`](../../crates/emulator-database/src/database/query/filter.rs), remove the `#[allow(clippy::double_comparisons)]` double-comparison workaround and evaluate `Equal`, `NotEqual`, `ArrayContains`, `In`, `ArrayContainsAny`, and `NotIn` using edition-aware (`is_enterprise: bool`) `value_matches_eq` and `operand_contains` helpers built on `==` (enforcing Standard Edition's `null`/`NaN` rules alongside the inequality guard on `!is_enterprise`).
*   **Rationale**: Eliminates the double-comparison workaround now that `Value::eq` is consistent with `Value::cmp`, while preserving exact Standard vs. Enterprise `null`/`NaN` query filter semantics.

### Verification Plan

*   **Compilation/Build**: `cargo build --workspace --all-targets`
*   **Unit Tests**: `cargo test --workspace` (specifically verifying `crates/googleapis` `value_ext::tests` and `crates/emulator-database` unit and integration tests)
*   **Static Analysis & Linting**: `cargo clippy --workspace --all-targets -- -D warnings`

--------------------------------------------------------------------------------

<!-- Provenance: Indicates initial structural drafting and review via EGM. -->

*Drafted and reviewed with EGM*
