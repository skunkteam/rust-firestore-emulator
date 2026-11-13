use std::fmt;

use base64::{
    Engine as _,
    display::Base64Display,
    prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD, BASE64_URL_SAFE, BASE64_URL_SAFE_NO_PAD},
};
use prost::bytes::Bytes;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, IgnoredAny, MapAccess, Visitor},
    ser::SerializeMap,
};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Iso8601};

use crate::google::{
    firestore::v1::{
        ArrayValue, Cursor, Document, ListCollectionIdsRequest, MapValue, Precondition,
        RunQueryRequest, RunQueryResponse, StructuredQuery, Value, list_collection_ids_request,
        precondition, run_query_request, run_query_response,
        structured_query::{
            CollectionSelector, CompositeFilter, Direction, FieldFilter, FieldReference, Filter,
            Order, Projection, UnaryFilter, composite_filter, field_filter, filter, unary_filter,
        },
        value::ValueType,
    },
    protobuf::{Int32Value, Timestamp},
    r#type::LatLng,
};

/// Minimum valid Proto3 `Timestamp` seconds (`0001-01-01T00:00:00Z`).
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
/// Maximum valid Proto3 `Timestamp` seconds (`9999-12-31T23:59:59Z`).
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&self.seconds)
            || !(0..1_000_000_000).contains(&self.nanos)
        {
            return Err(serde::ser::Error::custom(format!(
                "Timestamp ({}, {}) out of Proto3 RFC 3339 bounds",
                self.seconds, self.nanos
            )));
        }
        let dt = OffsetDateTime::try_from(*self).map_err(serde::ser::Error::custom)?;
        let (year, month, day) = dt.to_calendar_date();
        let (hour, minute, second) = dt.to_hms();
        let month_num = u8::from(month);
        let nanos = self.nanos as u32;
        let formatted = if nanos == 0 {
            format!("{year:04}-{month_num:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
        } else if nanos.is_multiple_of(1_000_000) {
            let millis = nanos / 1_000_000;
            format!(
                "{year:04}-{month_num:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
            )
        } else if nanos.is_multiple_of(1_000) {
            let micros = nanos / 1_000;
            format!(
                "{year:04}-{month_num:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z"
            )
        } else {
            format!(
                "{year:04}-{month_num:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos:09}Z"
            )
        };
        serializer.serialize_str(&formatted)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimestampVisitor;

        impl Visitor<'_> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an RFC 3339 timestamp string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                let dt = OffsetDateTime::parse(v, &Iso8601::DEFAULT)
                    .map_err(|e| E::custom(format!("invalid RFC 3339 timestamp '{v}': {e}")))?
                    .to_offset(UtcOffset::UTC);
                let ts = Timestamp::from(dt);
                if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&ts.seconds) {
                    return Err(E::custom(format!(
                        "timestamp '{v}' is outside Proto3 0001-01-01..=9999-12-31 bounds"
                    )));
                }
                Ok(ts)
            }
        }

        deserializer.deserialize_str(TimestampVisitor)
    }
}

struct Base64Bytes<'a>(&'a [u8]);

impl Serialize for Base64Bytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&Base64Display::new(self.0, &BASE64_STANDARD))
    }
}

struct DecodedBytes(Bytes);

impl<'de> Deserialize<'de> for DecodedBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl Visitor<'_> for BytesVisitor {
            type Value = DecodedBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a base64-encoded byte string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                let decoded = BASE64_STANDARD
                    .decode(v)
                    .or_else(|_| BASE64_STANDARD_NO_PAD.decode(v))
                    .or_else(|_| BASE64_URL_SAFE.decode(v))
                    .or_else(|_| BASE64_URL_SAFE_NO_PAD.decode(v))
                    .map_err(|e| E::custom(format!("invalid base64 bytesValue: {e}")))?;
                Ok(DecodedBytes(Bytes::from(decoded)))
            }
        }

        deserializer.deserialize_str(BytesVisitor)
    }
}

struct NullValuePayload;

impl<'de> Deserialize<'de> for NullValuePayload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct NullVisitor;

        impl Visitor<'_> for NullVisitor {
            type Value = NullValuePayload;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("null, \"NULL_VALUE\", or 0")
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(NullValuePayload)
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(NullValuePayload)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v == "NULL_VALUE" {
                    Ok(NullValuePayload)
                } else {
                    Err(E::custom(format!("invalid nullValue string '{v}'")))
                }
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                if v == 0 {
                    Ok(NullValuePayload)
                } else {
                    Err(E::custom(format!("invalid nullValue integer '{v}'")))
                }
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                if v == 0 {
                    Ok(NullValuePayload)
                } else {
                    Err(E::custom(format!("invalid nullValue integer '{v}'")))
                }
            }
        }

        deserializer.deserialize_any(NullVisitor)
    }
}

struct ProtoInt64(i64);

impl<'de> Deserialize<'de> for ProtoInt64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Int64Visitor;

        impl Visitor<'_> for Int64Visitor {
            type Value = ProtoInt64;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 64-bit signed integer or decimal string")
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(ProtoInt64(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                i64::try_from(v)
                    .map(ProtoInt64)
                    .map_err(|_| E::custom(format!("integerValue {v} exceeds i64::MAX")))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse::<i64>()
                    .map(ProtoInt64)
                    .map_err(|e| E::custom(format!("invalid integerValue '{v}': {e}")))
            }
        }

        deserializer.deserialize_any(Int64Visitor)
    }
}

struct ProtoDouble(f64);

impl<'de> Deserialize<'de> for ProtoDouble {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DoubleVisitor;

        impl Visitor<'_> for DoubleVisitor {
            type Value = ProtoDouble;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .write_str("a floating-point number or \"NaN\", \"Infinity\", \"-Infinity\"")
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(ProtoDouble(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(ProtoDouble(v as f64))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(ProtoDouble(v as f64))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    "NaN" => Ok(ProtoDouble(f64::NAN)),
                    "Infinity" => Ok(ProtoDouble(f64::INFINITY)),
                    "-Infinity" => Ok(ProtoDouble(f64::NEG_INFINITY)),
                    other => {
                        let parsed = other.parse::<f64>().map_err(|e| {
                            E::custom(format!("invalid doubleValue '{other}': {e}"))
                        })?;
                        Ok(ProtoDouble(parsed))
                    }
                }
            }
        }

        deserializer.deserialize_any(DoubleVisitor)
    }
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(ref vt) = self.value_type else {
            return Err(serde::ser::Error::custom("Value is missing value_type"));
        };
        let mut map = serializer.serialize_map(Some(1))?;
        match vt {
            ValueType::NullValue(_) => map.serialize_entry("nullValue", &())?,
            ValueType::BooleanValue(b) => map.serialize_entry("booleanValue", b)?,
            ValueType::IntegerValue(i) => map.serialize_entry("integerValue", &i.to_string())?,
            ValueType::DoubleValue(d) => {
                if d.is_nan() {
                    map.serialize_entry("doubleValue", "NaN")?;
                } else if *d == f64::INFINITY {
                    map.serialize_entry("doubleValue", "Infinity")?;
                } else if *d == f64::NEG_INFINITY {
                    map.serialize_entry("doubleValue", "-Infinity")?;
                } else {
                    map.serialize_entry("doubleValue", d)?;
                }
            }
            ValueType::TimestampValue(ts) => map.serialize_entry("timestampValue", ts)?,
            ValueType::StringValue(s) => map.serialize_entry("stringValue", s)?,
            ValueType::BytesValue(b) => map.serialize_entry("bytesValue", &Base64Bytes(b))?,
            ValueType::ReferenceValue(r) => map.serialize_entry("referenceValue", r)?,
            ValueType::GeoPointValue(g) => map.serialize_entry("geoPointValue", g)?,
            ValueType::ArrayValue(a) => map.serialize_entry("arrayValue", a)?,
            ValueType::MapValue(m) => map.serialize_entry("mapValue", m)?,
            ValueType::FieldReferenceValue(f) => map.serialize_entry("fieldReferenceValue", f)?,
            ValueType::FunctionValue(_) => {
                return Err(serde::ser::Error::custom(
                    "FunctionValue ProtoJSON serialization is not supported",
                ));
            }
            ValueType::PipelineValue(_) => {
                return Err(serde::ser::Error::custom(
                    "PipelineValue ProtoJSON serialization is not supported",
                ));
            }
        }
        map.end()
    }
}

const VALUE_VARIANT_KEYS: &[&str] = &[
    "nullValue",
    "booleanValue",
    "integerValue",
    "doubleValue",
    "timestampValue",
    "stringValue",
    "bytesValue",
    "referenceValue",
    "geoPointValue",
    "arrayValue",
    "mapValue",
    "fieldReferenceValue",
];

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Firestore ProtoJSON Value object with exactly one type key")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let Some(key) = map.next_key::<String>()? else {
                    return Err(de::Error::custom(
                        "Firestore Value object must contain exactly one type key, got empty \
                         object",
                    ));
                };

                let vt = match key.as_str() {
                    "nullValue" => {
                        let _ = map.next_value::<NullValuePayload>()?;
                        ValueType::NullValue(0)
                    }
                    "booleanValue" => ValueType::BooleanValue(map.next_value::<bool>()?),
                    "integerValue" => ValueType::IntegerValue(map.next_value::<ProtoInt64>()?.0),
                    "doubleValue" => ValueType::DoubleValue(map.next_value::<ProtoDouble>()?.0),
                    "timestampValue" => ValueType::TimestampValue(map.next_value::<Timestamp>()?),
                    "stringValue" => ValueType::StringValue(map.next_value::<String>()?),
                    "bytesValue" => ValueType::BytesValue(map.next_value::<DecodedBytes>()?.0),
                    "referenceValue" => ValueType::ReferenceValue(map.next_value::<String>()?),
                    "geoPointValue" => ValueType::GeoPointValue(map.next_value::<LatLng>()?),
                    "arrayValue" => ValueType::ArrayValue(map.next_value::<ArrayValue>()?),
                    "mapValue" => ValueType::MapValue(map.next_value::<MapValue>()?),
                    "fieldReferenceValue" => {
                        ValueType::FieldReferenceValue(map.next_value::<String>()?)
                    }
                    other => return Err(de::Error::unknown_variant(other, VALUE_VARIANT_KEYS)),
                };

                if map.next_key::<IgnoredAny>()?.is_some() {
                    let _ = map.next_value::<IgnoredAny>()?;
                    return Err(de::Error::custom(
                        "Firestore Value object must contain at most one type key",
                    ));
                }

                Ok(Value {
                    value_type: Some(vt),
                })
            }
        }

        deserializer.deserialize_map(ValueVisitor)
    }
}

impl Serialize for Precondition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(usize::from(self.condition_type.is_some())))?;
        match &self.condition_type {
            Some(precondition::ConditionType::Exists(exists)) => {
                map.serialize_entry("exists", exists)?;
            }
            Some(precondition::ConditionType::UpdateTime(ts)) => {
                map.serialize_entry("updateTime", ts)?;
            }
            None => {}
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Precondition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct PreconditionHelper {
            exists:      Option<bool>,
            update_time: Option<Timestamp>,
        }

        let helper = PreconditionHelper::deserialize(deserializer)?;
        let condition_type = match (helper.exists, helper.update_time) {
            (Some(_), Some(_)) => {
                return Err(de::Error::custom(
                    "Precondition cannot specify both 'exists' and 'updateTime'",
                ));
            }
            (Some(exists), None) => Some(precondition::ConditionType::Exists(exists)),
            (None, Some(ts)) => Some(precondition::ConditionType::UpdateTime(ts)),
            (None, None) => None,
        };
        Ok(Precondition { condition_type })
    }
}

fn serialize_direction<S: Serializer>(val: &i32, s: S) -> Result<S::Ok, S::Error> {
    let name = match Direction::try_from(*val).unwrap_or(Direction::Unspecified) {
        Direction::Unspecified => "DIRECTION_UNSPECIFIED",
        Direction::Ascending => "ASCENDING",
        Direction::Descending => "DESCENDING",
    };
    s.serialize_str(name)
}

fn deserialize_direction<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    struct DirVisitor;
    impl Visitor<'_> for DirVisitor {
        type Value = i32;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a Direction enum string or integer")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
            match v {
                "DIRECTION_UNSPECIFIED" => Ok(Direction::Unspecified as i32),
                "ASCENDING" => Ok(Direction::Ascending as i32),
                "DESCENDING" => Ok(Direction::Descending as i32),
                other => Err(E::unknown_variant(
                    other,
                    &["DIRECTION_UNSPECIFIED", "ASCENDING", "DESCENDING"],
                )),
            }
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> {
            i32::try_from(v)
                .ok()
                .and_then(|n| Direction::try_from(n).ok())
                .map(|dir| dir as i32)
                .ok_or_else(|| E::custom(format!("unknown Direction enum number {v}")))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> {
            i64::try_from(v)
                .map_err(|_| E::custom(format!("unknown Direction enum number {v}")))
                .and_then(|n| self.visit_i64(n))
        }
    }
    d.deserialize_any(DirVisitor)
}

fn serialize_composite_op<S: Serializer>(val: &i32, s: S) -> Result<S::Ok, S::Error> {
    let name = match composite_filter::Operator::try_from(*val)
        .unwrap_or(composite_filter::Operator::Unspecified)
    {
        composite_filter::Operator::Unspecified => "OPERATOR_UNSPECIFIED",
        composite_filter::Operator::And => "AND",
        composite_filter::Operator::Or => "OR",
    };
    s.serialize_str(name)
}

fn deserialize_composite_op<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    struct OpVisitor;
    impl Visitor<'_> for OpVisitor {
        type Value = i32;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a CompositeFilter.Operator string or integer")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
            match v {
                "OPERATOR_UNSPECIFIED" => Ok(composite_filter::Operator::Unspecified as i32),
                "AND" => Ok(composite_filter::Operator::And as i32),
                "OR" => Ok(composite_filter::Operator::Or as i32),
                other => Err(E::unknown_variant(
                    other,
                    &["OPERATOR_UNSPECIFIED", "AND", "OR"],
                )),
            }
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> {
            i32::try_from(v)
                .ok()
                .and_then(|n| composite_filter::Operator::try_from(n).ok())
                .map(|op| op as i32)
                .ok_or_else(|| E::custom(format!("unknown CompositeFilter.Operator number {v}")))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> {
            i64::try_from(v)
                .map_err(|_| E::custom(format!("unknown CompositeFilter.Operator number {v}")))
                .and_then(|n| self.visit_i64(n))
        }
    }
    d.deserialize_any(OpVisitor)
}

fn serialize_field_op<S: Serializer>(val: &i32, s: S) -> Result<S::Ok, S::Error> {
    use field_filter::Operator;
    let name = match Operator::try_from(*val).unwrap_or(Operator::Unspecified) {
        Operator::Unspecified => "OPERATOR_UNSPECIFIED",
        Operator::LessThan => "LESS_THAN",
        Operator::LessThanOrEqual => "LESS_THAN_OR_EQUAL",
        Operator::GreaterThan => "GREATER_THAN",
        Operator::GreaterThanOrEqual => "GREATER_THAN_OR_EQUAL",
        Operator::Equal => "EQUAL",
        Operator::NotEqual => "NOT_EQUAL",
        Operator::ArrayContains => "ARRAY_CONTAINS",
        Operator::In => "IN",
        Operator::ArrayContainsAny => "ARRAY_CONTAINS_ANY",
        Operator::NotIn => "NOT_IN",
    };
    s.serialize_str(name)
}

fn deserialize_field_op<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    use field_filter::Operator;
    struct OpVisitor;
    impl Visitor<'_> for OpVisitor {
        type Value = i32;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a FieldFilter.Operator string or integer")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
            let op = match v {
                "OPERATOR_UNSPECIFIED" => Operator::Unspecified,
                "LESS_THAN" => Operator::LessThan,
                "LESS_THAN_OR_EQUAL" => Operator::LessThanOrEqual,
                "GREATER_THAN" => Operator::GreaterThan,
                "GREATER_THAN_OR_EQUAL" => Operator::GreaterThanOrEqual,
                "EQUAL" => Operator::Equal,
                "NOT_EQUAL" => Operator::NotEqual,
                "ARRAY_CONTAINS" => Operator::ArrayContains,
                "IN" => Operator::In,
                "ARRAY_CONTAINS_ANY" => Operator::ArrayContainsAny,
                "NOT_IN" => Operator::NotIn,
                other => {
                    return Err(E::unknown_variant(
                        other,
                        &[
                            "OPERATOR_UNSPECIFIED",
                            "LESS_THAN",
                            "LESS_THAN_OR_EQUAL",
                            "GREATER_THAN",
                            "GREATER_THAN_OR_EQUAL",
                            "EQUAL",
                            "NOT_EQUAL",
                            "ARRAY_CONTAINS",
                            "IN",
                            "ARRAY_CONTAINS_ANY",
                            "NOT_IN",
                        ],
                    ));
                }
            };
            Ok(op as i32)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> {
            i32::try_from(v)
                .ok()
                .and_then(|n| Operator::try_from(n).ok())
                .map(|op| op as i32)
                .ok_or_else(|| E::custom(format!("unknown FieldFilter.Operator number {v}")))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> {
            i64::try_from(v)
                .map_err(|_| E::custom(format!("unknown FieldFilter.Operator number {v}")))
                .and_then(|n| self.visit_i64(n))
        }
    }
    d.deserialize_any(OpVisitor)
}

fn serialize_unary_op<S: Serializer>(val: &i32, s: S) -> Result<S::Ok, S::Error> {
    use unary_filter::Operator;
    let name = match Operator::try_from(*val).unwrap_or(Operator::Unspecified) {
        Operator::Unspecified => "OPERATOR_UNSPECIFIED",
        Operator::IsNan => "IS_NAN",
        Operator::IsNull => "IS_NULL",
        Operator::IsNotNan => "IS_NOT_NAN",
        Operator::IsNotNull => "IS_NOT_NULL",
    };
    s.serialize_str(name)
}

fn deserialize_unary_op<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    use unary_filter::Operator;
    struct OpVisitor;
    impl Visitor<'_> for OpVisitor {
        type Value = i32;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a UnaryFilter.Operator string or integer")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
            let op = match v {
                "OPERATOR_UNSPECIFIED" => Operator::Unspecified,
                "IS_NAN" => Operator::IsNan,
                "IS_NULL" => Operator::IsNull,
                "IS_NOT_NAN" => Operator::IsNotNan,
                "IS_NOT_NULL" => Operator::IsNotNull,
                other => {
                    return Err(E::unknown_variant(
                        other,
                        &[
                            "OPERATOR_UNSPECIFIED",
                            "IS_NAN",
                            "IS_NULL",
                            "IS_NOT_NAN",
                            "IS_NOT_NULL",
                        ],
                    ));
                }
            };
            Ok(op as i32)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> {
            i32::try_from(v)
                .ok()
                .and_then(|n| Operator::try_from(n).ok())
                .map(|op| op as i32)
                .ok_or_else(|| E::custom(format!("unknown UnaryFilter.Operator number {v}")))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> {
            i64::try_from(v)
                .map_err(|_| E::custom(format!("unknown UnaryFilter.Operator number {v}")))
                .and_then(|n| self.visit_i64(n))
        }
    }
    d.deserialize_any(OpVisitor)
}

impl Serialize for Order {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OrderSer<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            field:     &'a Option<FieldReference>,
            #[serde(serialize_with = "serialize_direction")]
            direction: &'a i32,
        }
        OrderSer {
            field:     &self.field,
            direction: &self.direction,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Order {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct OrderDe {
            field:     FieldReference,
            #[serde(default, deserialize_with = "deserialize_direction")]
            direction: i32,
        }
        let helper = OrderDe::deserialize(deserializer)?;
        Ok(Order {
            field:     Some(helper.field),
            direction: helper.direction,
        })
    }
}

impl Serialize for CompositeFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CompositeFilterSer<'a> {
            #[serde(serialize_with = "serialize_composite_op")]
            op:      &'a i32,
            filters: &'a [Filter],
        }
        CompositeFilterSer {
            op:      &self.op,
            filters: &self.filters,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CompositeFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct CompositeFilterDe {
            #[serde(deserialize_with = "deserialize_composite_op")]
            op:      i32,
            #[serde(default)]
            filters: Vec<Filter>,
        }
        let helper = CompositeFilterDe::deserialize(deserializer)?;
        Ok(CompositeFilter {
            op:      helper.op,
            filters: helper.filters,
        })
    }
}

impl Serialize for FieldFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct FieldFilterSer<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            field: &'a Option<FieldReference>,
            #[serde(serialize_with = "serialize_field_op")]
            op:    &'a i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            value: &'a Option<Value>,
        }
        FieldFilterSer {
            field: &self.field,
            op:    &self.op,
            value: &self.value,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FieldFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct FieldFilterDe {
            field: FieldReference,
            #[serde(deserialize_with = "deserialize_field_op")]
            op:    i32,
            value: Value,
        }
        let helper = FieldFilterDe::deserialize(deserializer)?;
        Ok(FieldFilter {
            field: Some(helper.field),
            op:    helper.op,
            value: Some(helper.value),
        })
    }
}

impl Serialize for UnaryFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct UnaryFilterSer<'a> {
            #[serde(serialize_with = "serialize_unary_op")]
            op:    &'a i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            field: Option<&'a FieldReference>,
        }
        let field = self
            .operand_type
            .as_ref()
            .map(|unary_filter::OperandType::Field(f)| f);
        UnaryFilterSer {
            op: &self.op,
            field,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for UnaryFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct UnaryFilterDe {
            #[serde(deserialize_with = "deserialize_unary_op")]
            op:    i32,
            field: FieldReference,
        }
        let helper = UnaryFilterDe::deserialize(deserializer)?;
        Ok(UnaryFilter {
            op: helper.op,
            operand_type: Some(unary_filter::OperandType::Field(helper.field)),
        })
    }
}

impl Serialize for Filter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(ref ft) = self.filter_type else {
            return Err(serde::ser::Error::custom("Filter is missing filter_type"));
        };
        let mut map = serializer.serialize_map(Some(1))?;
        match ft {
            filter::FilterType::CompositeFilter(c) => map.serialize_entry("compositeFilter", c)?,
            filter::FilterType::FieldFilter(f) => map.serialize_entry("fieldFilter", f)?,
            filter::FilterType::UnaryFilter(u) => map.serialize_entry("unaryFilter", u)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct FilterDe {
            composite_filter: Option<CompositeFilter>,
            field_filter:     Option<FieldFilter>,
            unary_filter:     Option<UnaryFilter>,
        }
        let helper = FilterDe::deserialize(deserializer)?;
        let filter_type = match (
            helper.composite_filter,
            helper.field_filter,
            helper.unary_filter,
        ) {
            (Some(c), None, None) => filter::FilterType::CompositeFilter(c),
            (None, Some(f), None) => filter::FilterType::FieldFilter(f),
            (None, None, Some(u)) => filter::FilterType::UnaryFilter(u),
            (None, None, None) => {
                return Err(de::Error::custom(
                    "Filter must specify one of 'compositeFilter', 'fieldFilter', or 'unaryFilter'",
                ));
            }
            _ => {
                return Err(de::Error::custom(
                    "Filter cannot specify more than one filter type",
                ));
            }
        };
        Ok(Filter {
            filter_type: Some(filter_type),
        })
    }
}

impl Serialize for StructuredQuery {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct StructuredQuerySer<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            select:   &'a Option<Projection>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            from:     &'a Vec<CollectionSelector>,
            #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
            r#where:  &'a Option<Filter>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            order_by: &'a Vec<Order>,
            #[serde(skip_serializing_if = "Option::is_none")]
            start_at: &'a Option<Cursor>,
            #[serde(skip_serializing_if = "Option::is_none")]
            end_at:   &'a Option<Cursor>,
            #[serde(skip_serializing_if = "is_zero_i32")]
            offset:   &'a i32,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit:    Option<i32>,
        }
        if self.find_nearest.is_some() {
            return Err(serde::ser::Error::custom(
                "StructuredQuery.findNearest ProtoJSON serialization is not supported",
            ));
        }
        StructuredQuerySer {
            select:   &self.select,
            from:     &self.from,
            r#where:  &self.r#where,
            order_by: &self.order_by,
            start_at: &self.start_at,
            end_at:   &self.end_at,
            offset:   &self.offset,
            limit:    self.limit.map(|l| l.value),
        }
        .serialize(serializer)
    }
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

struct ProtoInt32(i32);

impl<'de> Deserialize<'de> for ProtoInt32 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Int32Visitor;
        impl Visitor<'_> for Int32Visitor {
            type Value = ProtoInt32;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a 32-bit signed integer or numeric string")
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ProtoInt32, E> {
                i32::try_from(v)
                    .map(ProtoInt32)
                    .map_err(|_| E::custom(format!("integer {v} out of i32 bounds")))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ProtoInt32, E> {
                i32::try_from(v)
                    .map(ProtoInt32)
                    .map_err(|_| E::custom(format!("integer {v} out of i32 bounds")))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<ProtoInt32, E> {
                v.parse::<i32>()
                    .map(ProtoInt32)
                    .map_err(|e| E::custom(format!("invalid 32-bit integer '{v}': {e}")))
            }
        }
        deserializer.deserialize_any(Int32Visitor)
    }
}

impl<'de> Deserialize<'de> for StructuredQuery {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct StructuredQueryDe {
            select: Option<Projection>,
            from: Vec<CollectionSelector>,
            #[serde(rename = "where")]
            r#where: Option<Filter>,
            order_by: Vec<Order>,
            start_at: Option<Cursor>,
            end_at: Option<Cursor>,
            offset: Option<ProtoInt32>,
            limit: Option<ProtoInt32>,
            find_nearest: Option<IgnoredAny>,
        }
        let helper = StructuredQueryDe::deserialize(deserializer)?;
        if helper.find_nearest.is_some() {
            return Err(de::Error::custom(
                "StructuredQuery.findNearest is not implemented",
            ));
        }
        let offset = helper.offset.map_or(0, |v| v.0);
        if offset < 0 {
            return Err(de::Error::custom("StructuredQuery.offset must be >= 0"));
        }
        let limit = match helper.limit {
            Some(ProtoInt32(n)) if n < 0 => {
                return Err(de::Error::custom("StructuredQuery.limit must be >= 0"));
            }
            Some(ProtoInt32(n)) => Some(Int32Value { value: n }),
            None => None,
        };
        Ok(StructuredQuery {
            select: helper.select,
            from: helper.from,
            r#where: helper.r#where,
            order_by: helper.order_by,
            start_at: helper.start_at,
            end_at: helper.end_at,
            offset,
            limit,
            find_nearest: None,
        })
    }
}

impl Serialize for RunQueryRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if !self.parent.is_empty() {
            map.serialize_entry("parent", &self.parent)?;
        }
        if let Some(run_query_request::QueryType::StructuredQuery(ref sq)) = self.query_type {
            map.serialize_entry("structuredQuery", sq)?;
        }
        if let Some(run_query_request::ConsistencySelector::ReadTime(ref rt)) =
            self.consistency_selector
        {
            map.serialize_entry("readTime", rt)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RunQueryRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct RunQueryRequestDe {
            parent: String,
            structured_query: Option<StructuredQuery>,
            read_time: Option<Timestamp>,
            transaction: Option<DecodedBytes>,
        }
        let helper = RunQueryRequestDe::deserialize(deserializer)?;
        let consistency_selector = match (helper.read_time, helper.transaction) {
            (Some(_), Some(_)) => {
                return Err(de::Error::custom(
                    "RunQueryRequest cannot specify both 'readTime' and 'transaction'",
                ));
            }
            (Some(rt), None) => Some(run_query_request::ConsistencySelector::ReadTime(rt)),
            (None, Some(tx)) => Some(run_query_request::ConsistencySelector::Transaction(
                tx.0.to_vec(),
            )),
            (None, None) => None,
        };
        Ok(RunQueryRequest {
            parent: helper.parent,
            query_type: helper
                .structured_query
                .map(run_query_request::QueryType::StructuredQuery),
            consistency_selector,
            explain_options: None,
        })
    }
}

impl Serialize for RunQueryResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if !self.transaction.is_empty() {
            map.serialize_entry("transaction", &Base64Bytes(&self.transaction))?;
        }
        if let Some(ref doc) = self.document {
            map.serialize_entry("document", doc)?;
        }
        if let Some(ref rt) = self.read_time {
            map.serialize_entry("readTime", rt)?;
        }
        if self.skipped_results != 0 {
            map.serialize_entry("skippedResults", &self.skipped_results)?;
        }
        if let Some(run_query_response::ContinuationSelector::Done(done)) =
            self.continuation_selector
        {
            map.serialize_entry("done", &done)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RunQueryResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default)]
        struct RunQueryResponseDe {
            transaction: Option<DecodedBytes>,
            document: Option<Document>,
            read_time: Option<Timestamp>,
            skipped_results: i32,
            done: Option<bool>,
        }
        let helper = RunQueryResponseDe::deserialize(deserializer)?;
        Ok(RunQueryResponse {
            transaction: helper.transaction.map_or_else(Vec::new, |b| b.0.to_vec()),
            document: helper.document,
            read_time: helper.read_time,
            skipped_results: helper.skipped_results,
            continuation_selector: helper
                .done
                .map(run_query_response::ContinuationSelector::Done),
            explain_metrics: None,
        })
    }
}

impl Serialize for ListCollectionIdsRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if !self.parent.is_empty() {
            map.serialize_entry("parent", &self.parent)?;
        }
        if self.page_size != 0 {
            map.serialize_entry("pageSize", &self.page_size)?;
        }
        if !self.page_token.is_empty() {
            map.serialize_entry("pageToken", &self.page_token)?;
        }
        if let Some(list_collection_ids_request::ConsistencySelector::ReadTime(ref rt)) =
            self.consistency_selector
        {
            map.serialize_entry("readTime", rt)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for ListCollectionIdsRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct ListCollectionIdsRequestDe {
            parent:     String,
            page_size:  Option<ProtoInt32>,
            page_token: String,
            read_time:  Option<Timestamp>,
        }
        let helper = ListCollectionIdsRequestDe::deserialize(deserializer)?;
        Ok(ListCollectionIdsRequest {
            parent: helper.parent,
            page_size: helper.page_size.map_or(0, |v| v.0),
            page_token: helper.page_token,
            consistency_selector: helper
                .read_time
                .map(list_collection_ids_request::ConsistencySelector::ReadTime),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googletest::prelude::*;
    use serde_json::json;
    use time::macros::datetime;

    use super::*;

    #[gtest]
    fn roundtrip_all_11_storable_value_variants() {
        let ts: Timestamp = datetime!(2026-11-10 15:30:45.123456789 UTC).into();
        let cases: Vec<(Value, serde_json::Value)> = vec![
            (Value::null(), json!({ "nullValue": null })),
            (Value::boolean(true), json!({ "booleanValue": true })),
            (
                Value::integer(i64::MIN),
                json!({ "integerValue": "-9223372036854775808" }),
            ),
            (
                Value::integer(i64::MAX),
                json!({ "integerValue": "9223372036854775807" }),
            ),
            (Value::double(42.5), json!({ "doubleValue": 42.5 })),
            (Value::double(f64::NAN), json!({ "doubleValue": "NaN" })),
            (
                Value::double(f64::INFINITY),
                json!({ "doubleValue": "Infinity" }),
            ),
            (
                Value::double(f64::NEG_INFINITY),
                json!({ "doubleValue": "-Infinity" }),
            ),
            (
                Value::timestamp(ts),
                json!({ "timestampValue": "2026-11-10T15:30:45.123456789Z" }),
            ),
            (Value::string("hello"), json!({ "stringValue": "hello" })),
            (
                Value::bytes(b"\x00\x01\x02\x03".as_slice()),
                json!({ "bytesValue": "AAECAw==" }),
            ),
            (
                Value::reference(
                    "projects/p/databases/(default)/documents/users/alice".to_string(),
                ),
                json!({ "referenceValue": "projects/p/databases/(default)/documents/users/alice" }),
            ),
            (
                Value {
                    value_type: Some(ValueType::GeoPointValue(LatLng {
                        latitude:  52.3676,
                        longitude: 4.9041,
                    })),
                },
                json!({ "geoPointValue": { "latitude": 52.3676, "longitude": 4.9041 } }),
            ),
            (
                Value::array(vec![Value::integer(1), Value::double(1.0)]),
                json!({
                    "arrayValue": {
                        "values": [
                            { "integerValue": "1" },
                            { "doubleValue": 1.0 }
                        ]
                    }
                }),
            ),
            (
                Value::map(HashMap::from([("nested".to_string(), Value::string("ok"))])),
                json!({
                    "mapValue": {
                        "fields": {
                            "nested": { "stringValue": "ok" }
                        }
                    }
                }),
            ),
        ];

        for (val, expected_json) in cases {
            let serialized = serde_json::to_value(&val).unwrap();
            expect_eq!(serialized, expected_json);
            let deserialized: Value = serde_json::from_value(serialized).unwrap();
            expect_true!(val.storage_eq(&deserialized));
        }
    }

    #[gtest]
    fn negative_zero_and_flexible_protojson_inputs() {
        let neg_zero = Value::double(-0.0);
        let text = serde_json::to_string(&neg_zero).unwrap();
        expect_eq!(text, r#"{"doubleValue":-0.0}"#);
        let parsed: Value = serde_json::from_str(&text).unwrap();
        expect_true!(neg_zero.storage_eq(&parsed));

        // Unpadded and URL-safe base64, JSON integer for integerValue, "NULL_VALUE" for nullValue
        let from_int: Value = serde_json::from_str(r#"{"integerValue": 42}"#).unwrap();
        expect_true!(from_int.storage_eq(&Value::integer(42)));

        let from_null_str: Value = serde_json::from_str(r#"{"nullValue": "NULL_VALUE"}"#).unwrap();
        expect_true!(from_null_str.storage_eq(&Value::null()));

        let from_unpadded_b64: Value = serde_json::from_str(r#"{"bytesValue": "AAECAw"}"#).unwrap();
        expect_true!(from_unpadded_b64.storage_eq(&Value::bytes(b"\x00\x01\x02\x03".as_slice())));

        // Empty arrayValue / mapValue / geoPointValue objects
        let empty_arr: Value = serde_json::from_str(r#"{"arrayValue": {}}"#).unwrap();
        expect_true!(empty_arr.storage_eq(&Value::array(vec![])));

        let empty_map: Value = serde_json::from_str(r#"{"mapValue": {}}"#).unwrap();
        expect_true!(empty_map.storage_eq(&Value::map(HashMap::new())));
    }

    #[gtest]
    fn rejects_invalid_value_envelopes() {
        expect_that!(serde_json::from_str::<Value>("{}"), err(anything()));
        expect_that!(
            serde_json::from_str::<Value>(r#"{"stringValue":"a","integerValue":"1"}"#),
            err(anything())
        );
        expect_that!(
            serde_json::from_str::<Value>(r#"{"unknownValue": 1}"#),
            err(anything())
        );
        expect_that!(
            serde_json::from_str::<Value>(r#"{"integerValue": "9223372036854775808"}"#),
            err(anything())
        );
    }

    #[gtest]
    fn document_and_structured_query_roundtrip() {
        let ts: Timestamp = datetime!(2026-11-10 12:00:00 UTC).into();
        let doc = Document {
            name: "projects/p/databases/(default)/documents/users/alice".to_string(),
            fields: HashMap::from([("age".to_string(), Value::integer(30))]),
            create_time: Some(ts),
            update_time: Some(ts),
        };
        let doc_json = serde_json::to_value(&doc).unwrap();
        expect_eq!(
            doc_json,
            json!({
                "name": "projects/p/databases/(default)/documents/users/alice",
                "fields": { "age": { "integerValue": "30" } },
                "createTime": "2026-11-10T12:00:00Z",
                "updateTime": "2026-11-10T12:00:00Z"
            })
        );
        let parsed_doc: Document = serde_json::from_value(doc_json).unwrap();
        expect_eq!(parsed_doc.name, doc.name);
        expect_eq!(parsed_doc.create_time, doc.create_time);
        expect_true!(parsed_doc.fields["age"].storage_eq(&Value::integer(30)));

        // Phantom document omits fields, createTime, and updateTime
        let phantom = Document {
            name: "projects/p/databases/(default)/documents/rooms/r1".to_string(),
            ..Default::default()
        };
        expect_eq!(
            serde_json::to_value(&phantom).unwrap(),
            json!({ "name": "projects/p/databases/(default)/documents/rooms/r1" })
        );

        // StructuredQuery roundtrip + missing filter field rejection
        let rq_json = json!({
            "structuredQuery": {
                "from": [{ "collectionId": "users", "allDescendants": false }],
                "where": {
                    "compositeFilter": {
                        "op": "AND",
                        "filters": [
                            {
                                "fieldFilter": {
                                    "field": { "fieldPath": "age" },
                                    "op": "GREATER_THAN_OR_EQUAL",
                                    "value": { "integerValue": "21" }
                                }
                            },
                            {
                                "unaryFilter": {
                                    "op": "IS_NOT_NULL",
                                    "field": { "fieldPath": "age" }
                                }
                            }
                        ]
                    }
                },
                "orderBy": [{ "field": { "fieldPath": "age" }, "direction": "DESCENDING" }],
                "limit": 10
            }
        });
        let rq: RunQueryRequest = serde_json::from_value(rq_json.clone()).unwrap();
        expect_eq!(serde_json::to_value(&rq).unwrap(), rq_json);

        // Missing field inside fieldFilter must fail deserialization instead of panicking later
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({
                "where": { "fieldFilter": { "op": "EQUAL", "value": { "stringValue": "x" } } }
            })),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({
                "where": {}
            })),
            err(anything())
        );
    }

    #[gtest]
    fn structured_query_run_query_and_precondition_validation_edges() {
        use crate::google::firestore::v1::structured_query::FindNearest;

        // Negative offset and limit are rejected
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({ "offset": -1 })),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({ "limit": -5 })),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({ "limit": 3_000_000_000_i64 })),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({ "limit": "not-an-i32" })),
            err(anything())
        );

        // findNearest is rejected on both deserialize and serialize
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({
                "findNearest": { "queryVector": { "mapValue": {} } }
            })),
            err(anything())
        );
        let sq_with_nearest = StructuredQuery {
            find_nearest: Some(FindNearest::default()),
            ..Default::default()
        };
        expect_that!(serde_json::to_string(&sq_with_nearest), err(anything()));

        // Conflicting readTime + transaction in RunQueryRequest is rejected
        expect_that!(
            serde_json::from_value::<RunQueryRequest>(json!({
                "parent": "projects/p/databases/(default)/documents",
                "readTime": "2026-11-10T12:00:00Z",
                "transaction": "AAEC"
            })),
            err(anything())
        );

        // Precondition empty, exists, updateTime, and conflicting fields
        let empty_pre: Precondition = serde_json::from_str("{}").unwrap();
        expect_eq!(empty_pre.condition_type, None);
        expect_eq!(serde_json::to_value(empty_pre).unwrap(), json!({}));

        let exists_pre: Precondition = serde_json::from_str(r#"{"exists": false}"#).unwrap();
        expect_eq!(
            exists_pre.condition_type,
            Some(precondition::ConditionType::Exists(false))
        );
        expect_eq!(
            serde_json::to_value(exists_pre).unwrap(),
            json!({ "exists": false })
        );

        let ts: Timestamp = datetime!(2026-11-10 12:00:00.500 UTC).into();
        let ut_pre = Precondition {
            condition_type: Some(precondition::ConditionType::UpdateTime(ts)),
        };
        let ut_round: Precondition =
            serde_json::from_str(&serde_json::to_string(&ut_pre).unwrap()).unwrap();
        expect_eq!(ut_round, ut_pre);

        expect_that!(
            serde_json::from_value::<Precondition>(json!({
                "exists": true,
                "updateTime": "2026-11-10T12:00:00Z"
            })),
            err(anything())
        );

        // Filter edge cases: multiple filter types, missing filter_type, missing UnaryFilter/Order
        // field
        expect_that!(
            serde_json::from_value::<Filter>(json!({
                "fieldFilter": {
                    "field": { "fieldPath": "a" },
                    "op": "EQUAL",
                    "value": { "integerValue": "1" }
                },
                "unaryFilter": {
                    "op": "IS_NULL",
                    "field": { "fieldPath": "a" }
                }
            })),
            err(anything())
        );
        expect_that!(
            serde_json::to_string(&Filter { filter_type: None }),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<UnaryFilter>(json!({ "op": "IS_NULL" })),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<Order>(json!({ "direction": "ASCENDING" })),
            err(anything())
        );
    }

    mod proptests {
        use proptest::prelude::*;

        use super::*;

        fn arb_valid_timestamp() -> impl Strategy<Value = Timestamp> {
            (
                MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS,
                0_i32..1_000_000_000_i32,
            )
                .prop_map(|(seconds, nanos)| Timestamp { seconds, nanos })
        }

        fn arb_protojson_f64() -> impl Strategy<Value = f64> {
            prop_oneof![
                Just(0.0),
                Just(-0.0),
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                (-1_000_000_i64..=1_000_000_i64).prop_map(|n| (n as f64) * 0.0625),
            ]
        }

        fn arb_storable_value() -> impl Strategy<Value = Value> {
            let leaf = prop_oneof![
                Just(Value::null()),
                any::<bool>().prop_map(Value::boolean),
                any::<i64>().prop_map(Value::integer),
                arb_protojson_f64().prop_map(Value::double),
                arb_valid_timestamp().prop_map(Value::timestamp),
                "[a-zA-Z0-9_/-]{0,16}".prop_map(Value::string),
                prop::collection::vec(any::<u8>(), 0..12)
                    .prop_map(|b| Value::bytes(Bytes::from(b))),
                "[a-z]{1,4}/[a-z0-9]{1,4}".prop_map(|tail| {
                    Value::reference(format!("projects/p/databases/(default)/documents/{tail}"))
                }),
                (-700_i32..=700_i32, -1400_i32..=1400_i32).prop_map(|(lat, lon)| {
                    Value {
                        value_type: Some(ValueType::GeoPointValue(LatLng {
                            latitude:  f64::from(lat) * 0.125,
                            longitude: f64::from(lon) * 0.125,
                        })),
                    }
                }),
            ];
            leaf.prop_recursive(2, 12, 3, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..3).prop_map(Value::array),
                    prop::collection::hash_map("[a-d]{1,3}", inner, 0..3).prop_map(Value::map),
                ]
            })
        }

        fn arb_filter() -> impl Strategy<Value = Filter> {
            let leaf = prop_oneof![
                (
                    "[a-d]{1,4}",
                    prop_oneof![
                        Just(field_filter::Operator::LessThan as i32),
                        Just(field_filter::Operator::LessThanOrEqual as i32),
                        Just(field_filter::Operator::GreaterThan as i32),
                        Just(field_filter::Operator::GreaterThanOrEqual as i32),
                        Just(field_filter::Operator::Equal as i32),
                        Just(field_filter::Operator::NotEqual as i32),
                        Just(field_filter::Operator::ArrayContains as i32),
                        Just(field_filter::Operator::In as i32),
                    ],
                    arb_storable_value(),
                )
                    .prop_map(|(field_path, op, val)| Filter {
                        filter_type: Some(filter::FilterType::FieldFilter(FieldFilter {
                            field: Some(FieldReference { field_path }),
                            op,
                            value: Some(val),
                        })),
                    }),
                (
                    "[a-d]{1,4}",
                    prop_oneof![
                        Just(unary_filter::Operator::IsNan as i32),
                        Just(unary_filter::Operator::IsNull as i32),
                        Just(unary_filter::Operator::IsNotNan as i32),
                        Just(unary_filter::Operator::IsNotNull as i32),
                    ],
                )
                    .prop_map(|(field_path, op)| Filter {
                        filter_type: Some(filter::FilterType::UnaryFilter(UnaryFilter {
                            op,
                            operand_type: Some(unary_filter::OperandType::Field(FieldReference {
                                field_path,
                            })),
                        })),
                    }),
            ];
            leaf.prop_recursive(2, 6, 3, |inner| {
                (
                    prop_oneof![
                        Just(composite_filter::Operator::And as i32),
                        Just(composite_filter::Operator::Or as i32),
                    ],
                    prop::collection::vec(inner, 1..=3),
                )
                    .prop_map(|(op, filters)| Filter {
                        filter_type: Some(filter::FilterType::CompositeFilter(CompositeFilter {
                            op,
                            filters,
                        })),
                    })
            })
        }

        fn arb_structured_query() -> impl Strategy<Value = StructuredQuery> {
            (
                prop::option::of(
                    prop::collection::vec("[a-d]{1,4}", 1..=3).prop_map(|paths| Projection {
                        fields: paths
                            .into_iter()
                            .map(|field_path| FieldReference { field_path })
                            .collect(),
                    }),
                ),
                prop::collection::vec(
                    ("[a-d]{1,4}", any::<bool>()).prop_map(|(collection_id, all_descendants)| {
                        CollectionSelector {
                            collection_id,
                            all_descendants,
                        }
                    }),
                    0..=2,
                ),
                prop::option::of(arb_filter()),
                prop::collection::vec(
                    (
                        "[a-d]{1,4}",
                        prop_oneof![
                            Just(Direction::Ascending as i32),
                            Just(Direction::Descending as i32),
                        ],
                    )
                        .prop_map(|(field_path, direction)| Order {
                            field: Some(FieldReference { field_path }),
                            direction,
                        }),
                    0..=2,
                ),
                prop::option::of(
                    (
                        prop::collection::vec(arb_storable_value(), 1..=2),
                        any::<bool>(),
                    )
                        .prop_map(|(values, before)| Cursor { values, before }),
                ),
                0_i32..=100_i32,
                prop::option::of(0_i32..=100_i32),
            )
                .prop_map(
                    |(select, from, r#where, order_by, start_at, offset, limit)| StructuredQuery {
                        select,
                        from,
                        r#where,
                        order_by,
                        start_at,
                        end_at: None,
                        offset,
                        limit: limit.map(|value| Int32Value { value }),
                        find_nearest: None,
                    },
                )
        }

        proptest! {
            #[test]
            fn proptest_document_protojson_roundtrip(
                col in "[a-d]{1,4}",
                doc_id in "[a-z0-9]{1,6}",
                fields in prop::collection::hash_map("[a-f]{1,4}", arb_storable_value(), 0..6),
                create_time in prop::option::of(arb_valid_timestamp()),
                update_time in prop::option::of(arb_valid_timestamp()),
            ) {
                let doc = Document {
                    name: format!("projects/p/databases/(default)/documents/{col}/{doc_id}"),
                    fields,
                    create_time,
                    update_time,
                };
                let json_str = serde_json::to_string(&doc).unwrap();
                let roundtripped: Document = serde_json::from_str(&json_str).unwrap();
                prop_assert_eq!(&roundtripped.name, &doc.name);
                prop_assert_eq!(roundtripped.create_time, doc.create_time);
                prop_assert_eq!(roundtripped.update_time, doc.update_time);
                prop_assert!(Value::fields_storage_eq(&roundtripped.fields, &doc.fields));
            }

            #[test]
            fn proptest_structured_query_protojson_roundtrip(sq in arb_structured_query()) {
                let json_str = serde_json::to_string(&sq).unwrap();
                let roundtripped: StructuredQuery = serde_json::from_str(&json_str).unwrap();
                let roundtripped_twice: StructuredQuery =
                    serde_json::from_str(&serde_json::to_string(&roundtripped).unwrap()).unwrap();
                prop_assert_eq!(&roundtripped, &sq);
                prop_assert_eq!(&roundtripped_twice, &roundtripped);
            }
        }
    }
}
