# query

## Overview
Structured query filter representations and predicate evaluation logic supporting composite (`AND`/`OR`), field comparison/array, and unary (`IS NAN`/`IS NULL`/`IS NOT NAN`/`IS NOT NULL`) filters across Standard and Enterprise editions.

## Interface
- `[filter.rs:Filter]`: `pub enum Filter { Composite(CompositeFilter), Field(FieldFilter), Unary(UnaryFilter) }` with methods `get_inequality_fields`, `field_filters`, `eval`, `as_composite`, `as_field`, `as_unary`.
- `[filter.rs:CompositeFilter]`: `pub struct CompositeFilter { pub op: CompositeOperator, pub filters: Vec<Filter> }`
- `[filter.rs:CompositeOperator]`: `pub enum CompositeOperator { And, Or }`
- `[filter.rs:FieldFilter]`: `pub struct FieldFilter { pub field: FieldReference, pub op: FieldOperator, pub value: Value }`
- `[filter.rs:FieldOperator]`: `pub enum FieldOperator { LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Equal, NotEqual, ArrayContains, In, ArrayContainsAny, NotIn }`
- `[filter.rs:UnaryFilter]`: `pub struct UnaryFilter { pub field: FieldReference, pub op: UnaryOperator }`
- `[filter.rs:UnaryOperator]`: `pub enum UnaryOperator { IsNan, IsNull, IsNotNan, IsNotNull }`

## Invariants
- `[filter.rs:CompositeOperator::try_from]`, `[filter.rs:FieldOperator::try_from]`, `[filter.rs:UnaryOperator::try_from]`: Reject `Operator::Unspecified` with `GenericDatabaseError::InvalidArgument`.
- `[filter.rs:FieldFilter::eval]`: Missing fields evaluate to `false` except for `FieldOperator::NotEqual` when `is_enterprise` is `true`; `NotEqual` on `NULL` field values evaluates to `false` unless `is_enterprise` is `true`; ordering and equality operators (`LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`, `Equal`) require `Value::is_compatible_with` type compatibility.
- `[filter.rs:UnaryFilter::eval]`: Missing fields evaluate to `false` except for `UnaryOperator::IsNotNan` and `UnaryOperator::IsNotNull` when `is_enterprise` is `true`; `IsNotNan` on `NULL` values evaluates to `false` unless `is_enterprise` is `true`.
- `[filter.rs:FieldFilter::eval]`: `FieldOperator::ArrayContainsAny` and `FieldOperator::NotIn` are unimplemented and return `GenericDatabaseError::NotImplemented`.

## Side Effects
- `[filter.rs:FieldFilter::eval]`: Invoking unimplemented operators (`ArrayContainsAny`, `NotIn`) prints a diagnostic message to `stderr` via the `unimplemented!` macro; all other evaluations are pure in-memory checks.

## Verification
- Run: `cargo test -p emulator-database`
