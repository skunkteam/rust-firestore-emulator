# googleapis Source (`src`)

## Overview
Core library implementation for the `googleapis` crate. It includes the `tonic`/`prost`-generated Google Firestore v1 and protobuf well-known types (`tonic::include_proto!("googleapis")`) and extends generated types (`Timestamp`, `TransactionOptions`, `Value`, `Document`, `Precondition`, `StructuredQuery`, and `LatLng`) with constructors, accessors, total ordering, arithmetic aggregations, token serialization, and canonical Proto3 JSON (`serde`) serialization and deserialization.

## Interface
- `[lib.rs:googleapis]`: `tonic::include_proto!("googleapis")` re-exporting generated `google` protobuf modules (`google::firestore::v1`, `google::protobuf`, `google::r#type`, `google::rpc`, `google::api`).
- `[ser.rs]`: Canonical Proto3 JSON `serde::Serialize` and `serde::Deserialize` implementations for `google::protobuf::Timestamp`, `google::firestore::v1::Value`, `Precondition`, `StructuredQuery` (and nested `Filter`, `CompositeFilter`, `FieldFilter`, `UnaryFilter`, `Order`), `RunQueryRequest`, `RunQueryResponse`, and `ListCollectionIdsRequest` (alongside `build.rs`-derived `Serialize`/`Deserialize` on `Document`, `DocumentMask`, `ArrayValue`, `MapValue`, `LatLng`, `Cursor`, `ListDocumentsResponse`, and `ListCollectionIdsResponse`).
- `[timestamp_ext.rs:InvalidTokenError]`: `pub struct InvalidTokenError` (implements `std::error::Error` and `From<InvalidTokenError> for String`).
- `[timestamp_ext.rs:TimestampOutOfRangeError]`: `pub struct TimestampOutOfRangeError(#[from] time::error::ComponentRange)` (implements `std::error::Error` and `From<TimestampOutOfRangeError> for String`).
- `[timestamp_ext.rs:Timestamp]`:
  - `pub fn now() -> Timestamp`
  - `pub fn from_token(token: Vec<u8>) -> Result<Self, InvalidTokenError>`
  - `pub fn get_token(self) -> Result<Vec<u8>, TimestampOutOfRangeError>`
  - Trait implementations: `fmt::Display`, `From<&OffsetDateTime>`, `From<OffsetDateTime>`, and `TryFrom<Timestamp> for OffsetDateTime`.
- `[transaction_options_ext.rs:TransactionOptions::READ_WRITE]`: `pub const READ_WRITE: TransactionOptions` — default read-write `TransactionOptions` constant with an empty `retry_transaction`.
- `[value_ext.rs:Value]`:
  - Constructors: `reference`, `array`, `map`, `null`, `integer`, `boolean`, `double`, `string`, `timestamp`, `bytes`.
  - Accessors & conversions: `as_field_reference`, `as_function_value`, `as_reference`, `as_map`, `as_map_mut`, `as_string`, `into_array`, `as_array`, `as_boolean`, `as_double`, `as_integer`, `into_value_type`, `value_type`, `value_type_mut`.
  - Predicates: `is_nan`, `is_number`, `is_null`, `is_compatible_with`.
  - Trait implementations: `Eq`, `PartialOrd`, `Ord`, `Add`, `Sum`.
- `[value_ext.rs:LatLng]`: Trait implementations `Eq`, `PartialOrd`, and `Ord` using `f64::total_cmp` on `latitude` then `longitude`.

## Invariants
- `[ser.rs:Value]`: Enforces single-key ProtoJSON objects (`nullValue`, `booleanValue`, `integerValue` as decimal string, `doubleValue` with `"NaN"`/`"Infinity"`/`"-Infinity"` support, RFC 3339 `timestampValue`, `stringValue`, padded Base64 `bytesValue`, `referenceValue`, `geoPointValue`, `arrayValue`, `mapValue`, `fieldReferenceValue`); rejects empty `{}` or multi-key objects during deserialization so `Value { value_type: None }` is never produced.
- `[ser.rs:Filter]`: Requires `filter_type`, `field`, `op`, `value`, and `operand_type` on deserialized `Filter`, `FieldFilter`, and `UnaryFilter` nodes so downstream query compilation never encounters missing optional fields.
- `[timestamp_ext.rs:Timestamp::now]`: Guarantees strictly monotonically increasing timestamps across all calls within the process execution; if `OffsetDateTime::now_utc()` is less than or equal to the previous timestamp, `nanos` is incremented by `1`.
- `[timestamp_ext.rs:Timestamp::from_token]` / `[timestamp_ext.rs:Timestamp::get_token]`: Tokens must be exactly 16-byte native-endian `i128` Unix nanosecond representations within `time::OffsetDateTime`'s valid range.
- `[value_ext.rs:Value::value_type]` / `[value_ext.rs:Value::value_type_mut]` / `[value_ext.rs:Value::into_value_type]`: Assume `self.value_type` is `Some(...)` and panic with `"missing value_type in value"` if `None`.
- `[value_ext.rs:Value::value_type_order]` / `[value_ext.rs:Ord]`: Enforce Firestore's canonical value type ordering (`Null < Boolean < NaN < Integer/Double < Timestamp < String < Bytes < Reference < GeoPoint < Array < Map`, with non-document types `FieldReferenceValue`, `FunctionValue`, and `PipelineValue` placed at `usize::MAX - 2..=usize::MAX`), and normalize the minimum Datastore numeric ID segment `"__id-9223372036854775808__"` (`REFERENCE_NAME_MIN_ID`) to `""` when comparing `/`-delimited `ReferenceValue` paths.
- `[value_ext.rs:Add]`: Implements Firestore server-side `increment` rules (`i64::saturating_add` for two integers, promotion to `f64` if either operand is a double, and replacement with `rhs` if `self` is non-numeric).
- `[value_ext.rs:Sum]`: Implements Firestore server-side `sum`/`avg` aggregation rules (skips non-numeric values, folds from `IntegerValue(0)`, and promotes to `DoubleValue(f64)` on 64-bit integer overflow via `checked_add`).

## Side Effects
- `[timestamp_ext.rs:Timestamp::now]`: Acquires a lock on process-global `static LAST: Mutex<Timestamp>` and updates it in place.
- `[value_ext.rs:Ord]`: Emits `tracing::warn!` diagnostics if `FieldReferenceValue`, `FunctionValue`, or `PipelineValue` variants are compared.

## Verification
- Run: `cargo test -p googleapis`
- Run (Cloud Firestore conformance suite against built binary): `just sdk-test`
