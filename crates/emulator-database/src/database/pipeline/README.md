# pipeline

## Overview
Enterprise Edition pipeline execution engine (`StructuredPipeline`) that evaluates sequential document stream transformation stages (`collection`, `aggregate`, `where`, `limit`, `offset`, `sort`, `distinct`, `add_fields`, `select`, `remove_fields`, `find_nearest`) and expression/function trees over gRPC `Document` batches.

## Interface
- `[mod.rs:execute]`: `pub async fn execute(database: &Arc<FirestoreDatabase>, root_ref: &RootRef, StructuredPipeline { pipeline, options }: StructuredPipeline, read_consistency: ReadConsistency) -> Result<(Timestamp, Vec<Document>)>` — evaluates `StructuredPipeline` stages and propagates the snapshot `read_time` from the source stage.
- `[mod.rs:parse_projection_paths]`: `pub(super) fn parse_projection_paths<'a>(map: &'a MapValue) -> Result<Vec<(FieldPath, &'a Value)>>` — validates non-reserved `FieldPath` target aliases and rejects overlapping ancestor/descendant or duplicate alias paths for `add_fields`, `aggregate`, and `select` (`distinct` inserts raw alias keys directly).
- `[collection.rs:execute]`: `pub(super) async fn execute(database: &Arc<FirestoreDatabase>, root_ref: &RootRef, stage: Stage, read_consistency: ReadConsistency) -> Result<(Timestamp, Vec<Document>)>`
- `[add_fields.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[aggregate.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[distinct.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[filter.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[find_nearest.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[limit.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[offset.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[remove_fields.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[select.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[sort.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[expressions.rs:evaluate]` / `[expressions.rs:maybe_evaluate]`: Evaluates `Value` expressions (`FieldReferenceValue`, `FunctionValue`, or literal `Value`) against a `Document`.
- `[function.rs:evaluate]`: Evaluates pipeline `Function` calls across string, numeric, boolean, comparison, error/absence, array, and vector (`cosine_distance`, `euclidean_distance`, `dot_product`, `vector_length`) operators.

## Invariants
- `[mod.rs:execute]`: Pipeline execution requires `database.enterprise_edition()` to be `true`; otherwise it fails immediately with `GenericDatabaseError::FailedPrecondition`.
- `[mod.rs:execute]`: `StructuredPipeline.pipeline` must be `Some` with at least one stage, the first stage must be `"collection"`, and `StructuredPipeline.options` as well as non-`find_nearest` stage `options` must be empty.
- `[mod.rs:get_exact_expr_args]`, `[remove_fields.rs:execute]`, `[sort.rs:execute]`, `[find_nearest.rs:execute]`, `[expressions.rs:evaluate]`: Enforces stage and function argument counts (`collection`, `add_fields`, `distinct`, `where`, `limit`, `offset`, `select` require 1 argument via `get_exact_expr_args`; `aggregate` accepts 1 or 2 `MapValue` arguments for aggregations and group-by fields; `remove_fields` accepts variadic non-reserved `FieldPath` references and clears `doc.name` when removing `__name__`; `sort` accepts one or more `MapValue` arguments containing `"expression"` (any `Value` expression evaluated via `expressions::evaluate`) and optional `"direction"` (`"ascending"` or `"descending"`); `find_nearest` requires 3 positional arguments `(field: FieldReferenceValue, vector: VectorValue, distance_measure: StringValue)` and supports only `limit` (`1..=1000`) and `distance_field` (`FieldReferenceValue`) in `Stage.options`); `expressions.rs` rejects `value_type: None` with `InvalidArgument` and `VariableReferenceValue` with `NotImplemented`.
- `[aggregate.rs:execute]` & `[aggregate.rs:collect_numeric_values]`: Supports `"count"` (0 or 1 `Value` expression argument) and `"sum"`, `"average"`, `"minimum"`, and `"maximum"` (1 `Value` expression argument each, evaluated via `expressions::maybe_evaluate` / `expressions::evaluate`), evaluates group-by expressions via `expressions::evaluate`, and rejects non-empty `Function.options`.
- `[function.rs:evaluate]`: Validates static arity and empty `Function.options`, rejects unknown functions (`GenericDatabaseError::NotImplemented`) before evaluating operand expressions (while handling variadic `>= 2` lazy short-circuiting Kleene 3-valued `"and"`/`"or"`, `"not"` `null` propagation, and `maybe_evaluate` absent/type-incompatible comparisons in early dispatch); `is_error` catches `GenericDatabaseError::InvalidArgument(_)` while propagating `NotImplemented` and other errors. For `cosine_distance`, `euclidean_distance`, `dot_product`, and `vector_length`, evaluates all operands via `maybe_evaluate` before propagating `NULL` for absent/`null` values, rejecting non-vector types with `The function <name>(...) requires \`Vector\` but got \`<TYPE>\`.`, rejecting mismatched vector lengths, and validating `compute_distance` results. Enforces domain constraints on `add`..`ln` (division/modulo by zero, 64-bit integer overflow -> `InvalidArgument`, `pow` domain errors, negative `sqrt`, non-positive `log10`/`ln`) and propagates `null` in `string_concat`/`split`/`add`..`pow`/`array_concat` while still validating non-null sibling operand types.
- `[find_nearest.rs:execute]`: Rejects `VariableReferenceValue` anywhere in `stage.args` or `stage.options` with `NotImplemented` before argument/option validation, enforces `into_non_reserved_field_path` on `field` and `distance_field`, requires a valid `__vector__` `MapValue` query vector and valid `distance_measure`, pre-validates `Cosine` self-distance, skips missing/non-vector/mismatched-dimension fields while propagating `compute_distance` `NaN` errors, ranks matches via `select_and_sort_nearest` (`cmp_reference_paths` tie-breaker), and injects `distance_field` when set.

## Side Effects
- `[collection.rs:execute]`: Executes a collection query on `FirestoreDatabase::run_query` using `QueryBuilder::from_collection` with `enterprise_edition(true)` and the provided `ReadConsistency`, acquiring document or transaction read locks as required.
- `[add_fields.rs:execute]`, `[aggregate.rs:execute]`, `[distinct.rs:execute]`, `[filter.rs:execute]`, `[find_nearest.rs:execute]`, `[limit.rs:execute]`, `[offset.rs:execute]`, `[remove_fields.rs:execute]`, `[select.rs:execute]`, `[sort.rs:execute]`: Mutates in-memory `Vec<Document>` streams (`aggregate` and `distinct` emit synthesized `Document` structs with empty `name` and `None` timestamps).

## Verification
- Run: `cargo test -p emulator-database`
