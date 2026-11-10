# pipeline

## Overview
Enterprise Edition pipeline execution engine (`StructuredPipeline`) that evaluates sequential document stream transformation stages (`collection`, `aggregate`, `where`, `limit`, `offset`, `sort`, `distinct`, `add_fields`, `select`, `remove_fields`) and expression/function trees over gRPC `Document` batches.

## Interface
- `[mod.rs:execute]`: `pub async fn execute(database: &Arc<FirestoreDatabase>, root_ref: &RootRef, StructuredPipeline { pipeline, options }: StructuredPipeline, read_consistency: ReadConsistency) -> Result<Vec<Document>>`
- `[collection.rs:execute]`: `pub(super) async fn execute(database: &Arc<FirestoreDatabase>, root_ref: &RootRef, stage: Stage, read_consistency: ReadConsistency) -> Result<Vec<Document>>`
- `[add_fields.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[aggregate.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[distinct.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[filter.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[limit.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[offset.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[remove_fields.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[select.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[sort.rs:execute]`: `pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>>`
- `[expressions.rs:evaluate]` / `[expressions.rs:maybe_evaluate]`: Evaluates `Value` expressions (`FieldReferenceValue`, `FunctionValue`, or literal `Value`) against a `Document`.
- `[function.rs:evaluate]`: Evaluates pipeline `Function` calls across string, numeric, boolean, comparison, error/absence, and array operators.

## Invariants
- `[mod.rs:execute]`: Pipeline execution requires `database.enterprise_edition()` to be `true`; otherwise it fails immediately with `GenericDatabaseError::FailedPrecondition`.
- `[mod.rs:execute]`: `StructuredPipeline.pipeline` must be `Some` with at least one stage, the first stage must be `"collection"`, and `StructuredPipeline.options` as well as stage `options` must be empty.
- `[mod.rs:get_exact_expr_args]`, `[remove_fields.rs:execute]`, `[sort.rs:execute]`: Enforces stage and function argument counts (`collection`, `add_fields`, `distinct`, `where`, `limit`, `offset`, `select` require 1 argument via `get_exact_expr_args`; `aggregate` accepts 1 or 2 `MapValue` arguments for aggregations and group-by fields; `remove_fields` accepts variadic `FieldReferenceValue` arguments; `sort` accepts one or more `MapValue` arguments containing `"expression"` (`FieldReferenceValue`) and optional `"direction"` (`"ascending"` or `"descending"`)).
- `[aggregate.rs:execute]` & `[aggregate.rs:extract_field_ref]`: Supports `"count"` (0 or 1 field reference argument) and `"sum"`, `"average"`, `"minimum"`, and `"maximum"` (1 field reference argument each).
- `[function.rs:evaluate]`: Enforces argument types and domain constraints for built-in functions (`string_concat`, `to_upper`, `to_lower`, `char_length`, `split`, `add`, `subtract`, `multiply`, `divide`, `mod`, `pow`, `abs`, `ceil`, `floor`, `round`, `sqrt`, `log10`, `ln`, `not`, `and`, `or`, `equal`, `not_equal`, `greater_than`, `greater_than_or_equal`, `less_than`, `less_than_or_equal`, `is_error`, `is_absent`, `array_length`, `array`, `array_concat`), returning `GenericDatabaseError::InvalidArgument` on division/modulo by zero, negative `sqrt`, or non-positive `log10`/`ln`.

## Side Effects
- `[collection.rs:execute]`: Executes a collection query on `FirestoreDatabase::run_query` using `QueryBuilder::from_collection` with `enterprise_edition(true)` and the provided `ReadConsistency`, acquiring document or transaction read locks as required.
- `[add_fields.rs:execute]`, `[aggregate.rs:execute]`, `[distinct.rs:execute]`, `[limit.rs:execute]`, `[offset.rs:execute]`, `[remove_fields.rs:execute]`, `[select.rs:execute]`, `[sort.rs:execute]`: Mutates in-memory `Vec<Document>` streams (`aggregate` and `distinct` emit synthesized `Document` structs with empty `name` and `None` timestamps).

## Verification
- Run: `cargo test -p emulator-database`
