# query

## Overview
Structured query filter representations, `StructuredQuery.findNearest` vector-search specification, and predicate evaluation logic supporting composite (`AND`/`OR`), field comparison/array, and unary (`IS NAN`/`IS NULL`/`IS NOT NAN`/`IS NOT NULL`) filters across Standard and Enterprise editions.

## Interface
- `[filter.rs:Filter]`: `pub enum Filter { Composite(CompositeFilter), Field(FieldFilter), Unary(UnaryFilter) }` with methods `get_inequality_fields`, `field_filters`, `eval`, `as_composite`, `as_field`, `as_unary`.
- `[filter.rs:CompositeFilter]`: `pub struct CompositeFilter { pub op: CompositeOperator, pub filters: Vec<Filter> }`
- `[filter.rs:CompositeOperator]`: `pub enum CompositeOperator { And, Or }`
- `[filter.rs:FieldFilter]`: `pub struct FieldFilter { pub field: FieldReference, pub op: FieldOperator, pub value: Value }`
- `[filter.rs:FieldOperator]`: `pub enum FieldOperator { LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Equal, NotEqual, ArrayContains, In, ArrayContainsAny, NotIn }`
- `[filter.rs:UnaryFilter]`: `pub struct UnaryFilter { pub field: FieldReference, pub op: UnaryOperator }`
- `[filter.rs:UnaryOperator]`: `pub enum UnaryOperator { IsNan, IsNull, IsNotNan, IsNotNull }`
- `[find_nearest.rs:FindNearest]`: `pub struct FindNearest { pub vector_field: FieldPath, pub query_vector: Vec<f64>, pub distance_measure: DistanceMeasure, pub limit: usize, pub distance_result_field: Option<FieldPath>, pub distance_threshold: Option<f64> }` (`new`, `distance_result_field`, `distance_threshold`, `validate`, `try_from_proto`, bounded `fmt::Debug` logging dimensionality and up to 3 coordinates).

## Invariants
- `[filter.rs:CompositeOperator::try_from]`, `[filter.rs:FieldOperator::try_from]`, `[filter.rs:UnaryOperator::try_from]`: Reject `Operator::Unspecified` with `GenericDatabaseError::InvalidArgument`.
- `[filter.rs:FieldFilter::eval]`: Missing fields evaluate to `false` except for `FieldOperator::NotEqual` when `is_enterprise` is `true`; `NotEqual` on `NULL` field values evaluates to `false` unless `is_enterprise` is `true`; ordering and equality operators (`LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`, `Equal`) require `Value::is_compatible_with` type compatibility.
- `[filter.rs:UnaryFilter::eval]`: Missing fields evaluate to `false` except for `UnaryOperator::IsNotNan` and `UnaryOperator::IsNotNull` when `is_enterprise` is `true`; `IsNotNan` on `NULL` values evaluates to `false` unless `is_enterprise` is `true`.
- `[filter.rs:FieldFilter::eval]`: `FieldOperator::ArrayContainsAny` and `FieldOperator::NotIn` are unimplemented and return `GenericDatabaseError::NotImplemented`.
- `[find_nearest.rs:FindNearest::validate]` & `[find_nearest.rs:FindNearest::try_from_proto]`: Requires a valid `DistanceMeasure`, a non-reserved `vector_field`, optional non-reserved `distance_result_field`, valid `query_vector` (`1..=2048` non-`NaN` coordinates, returning `FailedPrecondition` on Standard Edition zero-norm `Cosine` and `InvalidArgument` on Enterprise Edition), `limit` in `1..=1000`, and non-`NaN` `distance_threshold`, rejecting invalid inputs cleanly. Inside `Query::validate`, all `FieldFilter` and `Cursor` values validate nested `__vector__` sentinel maps via `validate_query_operand_value` (`NaN` rejected on Enterprise Edition only), and `find_nearest` cannot be combined with outer `limit`, `offset > 0`, or cursors, skips implicit `order_by` synthesis, treats explicit `order_by` clauses as field-existence pre-filters, and tie-breaks equal distances via `find_nearest_tie_cmp` (on Standard Edition when `order_by` is non-empty: explicit `order_by` then segment-wise `__name__` matching the last `order_by` direction; otherwise across Standard and Enterprise editions: inequality filter fields `ASC` then segment-wise `__name__ ASC`).

## Side Effects
- `[filter.rs:FieldFilter::eval]`: Invoking unimplemented operators (`ArrayContainsAny`, `NotIn`) prints a diagnostic message to `stderr` via the `unimplemented!` macro; all other evaluations are pure in-memory checks.

## Verification
- Run: `cargo test -p emulator-database`
