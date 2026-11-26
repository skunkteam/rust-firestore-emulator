use std::{collections::HashMap, fmt};

use base64::{
    Engine as _,
    display::Base64Display,
    prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD, BASE64_URL_SAFE, BASE64_URL_SAFE_NO_PAD},
};
use itertools::Itertools;
use prost::bytes::Bytes;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, IgnoredAny, MapAccess, Visitor},
    ser::SerializeMap,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    google::{
        firestore::v1::{
            AggregationResult, ArrayValue, Cursor, Document, ExplainOptions,
            ListCollectionIdsRequest, MapValue, Precondition, RequestOptions,
            RunAggregationQueryRequest, RunAggregationQueryResponse, RunQueryRequest,
            RunQueryResponse, StructuredAggregationQuery, StructuredQuery, TransactionOptions,
            Value, list_collection_ids_request, precondition, run_aggregation_query_request,
            run_query_request, run_query_response,
            structured_aggregation_query::{
                self, Aggregation,
                aggregation::{Avg, Count, Operator, Sum},
            },
            structured_query::{
                self, CollectionSelector, CompositeFilter, Direction, FieldFilter, FieldReference,
                Filter, Order, Projection, UnaryFilter, composite_filter, field_filter, filter,
                unary_filter,
            },
            value::ValueType,
        },
        protobuf::{Int32Value, Int64Value, Timestamp},
        r#type::LatLng,
    },
    timestamp_ext::{MAX_TIMESTAMP_SECONDS, MIN_TIMESTAMP_SECONDS},
};

pub(crate) fn deserialize_null_default<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(d).map(Option::unwrap_or_default)
}

pub(crate) fn serialize_sorted_map<S: Serializer>(
    fields: &HashMap<String, Value>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(fields.len()))?;
    for (k, v) in fields.iter().sorted_by_key(|(k, _)| *k) {
        map.serialize_entry(k, v)?;
    }
    map.end()
}

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
                let dt = OffsetDateTime::parse(v, &Rfc3339)
                    .map_err(|e| E::custom(format!("invalid RFC 3339 timestamp '{v}': {e}")))?;
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
                    .map_err(|_| E::custom(format!("integer {v} out of i64 bounds")))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse::<i64>()
                    .map(ProtoInt64)
                    .map_err(|e| E::custom(format!("invalid 64-bit integer '{v}': {e}")))
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
                        if !parsed.is_finite() {
                            return Err(E::custom(format!(
                                "invalid or out-of-range doubleValue '{other}'"
                            )));
                        }
                        Ok(ProtoDouble(parsed))
                    }
                }
            }
        }

        deserializer.deserialize_any(DoubleVisitor)
    }
}

struct ProtoDoubleRef(f64);

impl Serialize for ProtoDoubleRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_nan() {
            serializer.serialize_str("NaN")
        } else if self.0 == f64::INFINITY {
            serializer.serialize_str("Infinity")
        } else if self.0 == f64::NEG_INFINITY {
            serializer.serialize_str("-Infinity")
        } else {
            serializer.serialize_f64(self.0)
        }
    }
}

impl Serialize for LatLng {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct LatLngSer {
            latitude:  ProtoDoubleRef,
            longitude: ProtoDoubleRef,
        }
        LatLngSer {
            latitude:  ProtoDoubleRef(self.latitude),
            longitude: ProtoDoubleRef(self.longitude),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LatLng {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct LatLngDe {
            latitude:  Option<ProtoDouble>,
            longitude: Option<ProtoDouble>,
        }
        let helper = LatLngDe::deserialize(deserializer)?;
        let latitude = helper.latitude.map_or(0.0, |v| v.0);
        let longitude = helper.longitude.map_or(0.0, |v| v.0);
        if !(-90.0..=90.0).contains(&latitude) {
            return Err(de::Error::custom(format!(
                "LatLng latitude {latitude} must be within [-90.0, 90.0]"
            )));
        }
        if !(-180.0..=180.0).contains(&longitude) {
            return Err(de::Error::custom(format!(
                "LatLng longitude {longitude} must be within [-180.0, 180.0]"
            )));
        }
        Ok(LatLng {
            latitude,
            longitude,
        })
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
                map.serialize_entry("doubleValue", &ProtoDoubleRef(*d))?;
            }
            ValueType::TimestampValue(ts) => map.serialize_entry("timestampValue", ts)?,
            ValueType::StringValue(s) => map.serialize_entry("stringValue", s)?,
            ValueType::BytesValue(b) => map.serialize_entry("bytesValue", &Base64Bytes(b))?,
            ValueType::ReferenceValue(r) => map.serialize_entry("referenceValue", r)?,
            ValueType::GeoPointValue(g) => map.serialize_entry("geoPointValue", g)?,
            ValueType::ArrayValue(a) => map.serialize_entry("arrayValue", a)?,
            ValueType::MapValue(m) => map.serialize_entry("mapValue", m)?,
            ValueType::FieldReferenceValue(f) => map.serialize_entry("fieldReferenceValue", f)?,
            ValueType::VariableReferenceValue(v) => {
                map.serialize_entry("variableReferenceValue", v)?
            }
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
    "variableReferenceValue",
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
                    "variableReferenceValue" => {
                        ValueType::VariableReferenceValue(map.next_value::<String>()?)
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

macro_rules! proto_enum_serde {
    (
        ser = $ser_fn:ident,
        de = $de_fn:ident,
        ty = $enum_ty:path,
        default = $default_variant:ident,
        expecting = $expecting:literal,
        unknown_num = $unknown_num:literal,
        variants = [ $( $variant:ident => $wire:literal ),+ $(,)? ]
    ) => {
        fn $ser_fn<S: Serializer>(val: &i32, s: S) -> Result<S::Ok, S::Error> {
            use $enum_ty as ProtoEnum;
            let name = match ProtoEnum::try_from(*val).unwrap_or(ProtoEnum::$default_variant) {
                $( ProtoEnum::$variant => $wire, )+
            };
            s.serialize_str(name)
        }

        fn $de_fn<'de, D: Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
            use $enum_ty as ProtoEnum;
            struct EnumVisitor;
            impl Visitor<'_> for EnumVisitor {
                type Value = i32;

                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str($expecting)
                }

                fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
                    let parsed = match v {
                        $( $wire => ProtoEnum::$variant, )+
                        other => return Err(E::unknown_variant(other, &[ $( $wire ),+ ])),
                    };
                    Ok(parsed as i32)
                }

                fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> {
                    i32::try_from(v)
                        .ok()
                        .and_then(|n| ProtoEnum::try_from(n).ok())
                        .map(|parsed| parsed as i32)
                        .ok_or_else(|| E::custom(format!(concat!($unknown_num, " {}"), v)))
                }

                fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> {
                    i64::try_from(v)
                        .map_err(|_| E::custom(format!(concat!($unknown_num, " {}"), v)))
                        .and_then(|n| self.visit_i64(n))
                }
            }
            d.deserialize_any(EnumVisitor)
        }
    };
}

proto_enum_serde! {
    ser = serialize_direction,
    de = deserialize_direction,
    ty = Direction,
    default = Unspecified,
    expecting = "a Direction enum string or integer",
    unknown_num = "unknown Direction enum number",
    variants = [
        Unspecified => "DIRECTION_UNSPECIFIED",
        Ascending => "ASCENDING",
        Descending => "DESCENDING",
    ]
}

proto_enum_serde! {
    ser = serialize_composite_op,
    de = deserialize_composite_op,
    ty = composite_filter::Operator,
    default = Unspecified,
    expecting = "a CompositeFilter.Operator string or integer",
    unknown_num = "unknown CompositeFilter.Operator number",
    variants = [
        Unspecified => "OPERATOR_UNSPECIFIED",
        And => "AND",
        Or => "OR",
    ]
}

proto_enum_serde! {
    ser = serialize_field_op,
    de = deserialize_field_op,
    ty = field_filter::Operator,
    default = Unspecified,
    expecting = "a FieldFilter.Operator string or integer",
    unknown_num = "unknown FieldFilter.Operator number",
    variants = [
        Unspecified => "OPERATOR_UNSPECIFIED",
        LessThan => "LESS_THAN",
        LessThanOrEqual => "LESS_THAN_OR_EQUAL",
        GreaterThan => "GREATER_THAN",
        GreaterThanOrEqual => "GREATER_THAN_OR_EQUAL",
        Equal => "EQUAL",
        NotEqual => "NOT_EQUAL",
        ArrayContains => "ARRAY_CONTAINS",
        In => "IN",
        ArrayContainsAny => "ARRAY_CONTAINS_ANY",
        NotIn => "NOT_IN",
    ]
}

proto_enum_serde! {
    ser = serialize_unary_op,
    de = deserialize_unary_op,
    ty = unary_filter::Operator,
    default = Unspecified,
    expecting = "a UnaryFilter.Operator string or integer",
    unknown_num = "unknown UnaryFilter.Operator number",
    variants = [
        Unspecified => "OPERATOR_UNSPECIFIED",
        IsNan => "IS_NAN",
        IsNull => "IS_NULL",
        IsNotNan => "IS_NOT_NAN",
        IsNotNull => "IS_NOT_NULL",
    ]
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

struct ProtoDirection(i32);

impl<'de> Deserialize<'de> for ProtoDirection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_direction(deserializer).map(ProtoDirection)
    }
}

impl<'de> Deserialize<'de> for Order {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct OrderDe {
            field:     FieldReference,
            #[serde(default)]
            direction: Option<ProtoDirection>,
        }
        let helper = OrderDe::deserialize(deserializer)?;
        Ok(Order {
            field:     Some(helper.field),
            direction: helper
                .direction
                .map_or(Direction::Unspecified as i32, |d| d.0),
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
            #[serde(default, deserialize_with = "deserialize_null_default")]
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

/// Lossy, presence-only sentinel that accepts any JSON object and discards its contents.
///
/// Strictly intended for downstream `501 UNIMPLEMENTED` detection (e.g., `findNearest` on REST
/// `:runQuery`). Must be replaced with a typed Proto3 JSON codec before enabling REST execution
/// for any field that uses it.
struct UnsupportedMessageObject;

impl<'de> Deserialize<'de> for UnsupportedMessageObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;

        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = UnsupportedMessageObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(UnsupportedMessageObject)
            }
        }

        deserializer.deserialize_map(ObjectVisitor)
    }
}

impl<'de> Deserialize<'de> for StructuredQuery {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct StructuredQueryDe {
            select: Option<Projection>,
            #[serde(deserialize_with = "deserialize_null_default")]
            from: Vec<CollectionSelector>,
            #[serde(rename = "where")]
            r#where: Option<Filter>,
            #[serde(deserialize_with = "deserialize_null_default")]
            order_by: Vec<Order>,
            start_at: Option<Cursor>,
            end_at: Option<Cursor>,
            offset: Option<ProtoInt32>,
            limit: Option<ProtoInt32>,
            find_nearest: Option<UnsupportedMessageObject>,
        }
        let helper = StructuredQueryDe::deserialize(deserializer)?;
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
            find_nearest: helper
                .find_nearest
                .map(|_| structured_query::FindNearest::default()),
        })
    }
}

fn check_unsupported_query_serialize<E: serde::ser::Error>(
    msg_name: &str,
    has_explain_options: bool,
    has_request_options: bool,
    has_new_transaction: bool,
) -> Result<(), E> {
    if has_explain_options {
        return Err(E::custom(format!(
            "{msg_name}.explainOptions ProtoJSON serialization is not supported"
        )));
    }
    if has_request_options {
        return Err(E::custom(format!(
            "{msg_name}.requestOptions ProtoJSON serialization is not supported"
        )));
    }
    if has_new_transaction {
        return Err(E::custom(format!(
            "{msg_name}.newTransaction ProtoJSON serialization is not supported"
        )));
    }
    Ok(())
}

enum ParsedConsistency {
    ReadTime(Timestamp),
    Transaction(Vec<u8>),
    NewTransaction,
}

fn parse_consistency_selector<E: de::Error>(
    msg_name: &str,
    read_time: Option<Timestamp>,
    transaction: Option<DecodedBytes>,
    new_transaction: Option<UnsupportedMessageObject>,
) -> Result<Option<ParsedConsistency>, E> {
    match (read_time, transaction, new_transaction) {
        (Some(rt), None, None) => Ok(Some(ParsedConsistency::ReadTime(rt))),
        (None, Some(tx), None) => Ok(Some(ParsedConsistency::Transaction(tx.0.to_vec()))),
        (None, None, Some(_)) => Ok(Some(ParsedConsistency::NewTransaction)),
        (None, None, None) => Ok(None),
        _ => Err(de::Error::custom(format!(
            "{msg_name} cannot specify more than one of 'readTime', 'transaction', or \
             'newTransaction'"
        ))),
    }
}

impl Serialize for RunQueryRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        check_unsupported_query_serialize::<S::Error>(
            "RunQueryRequest",
            self.explain_options.is_some(),
            self.request_options.is_some(),
            matches!(
                self.consistency_selector,
                Some(run_query_request::ConsistencySelector::NewTransaction(_))
            ),
        )?;
        let mut map = serializer.serialize_map(None)?;
        if !self.parent.is_empty() {
            map.serialize_entry("parent", &self.parent)?;
        }
        if let Some(run_query_request::QueryType::StructuredQuery(ref sq)) = self.query_type {
            map.serialize_entry("structuredQuery", sq)?;
        }
        match &self.consistency_selector {
            Some(run_query_request::ConsistencySelector::Transaction(tx)) => {
                map.serialize_entry("transaction", &Base64Bytes(tx))?;
            }
            Some(run_query_request::ConsistencySelector::ReadTime(rt)) => {
                map.serialize_entry("readTime", rt)?;
            }
            Some(run_query_request::ConsistencySelector::NewTransaction(_)) | None => {}
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RunQueryRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct RunQueryRequestDe {
            #[serde(deserialize_with = "deserialize_null_default")]
            parent: String,
            structured_query: Option<StructuredQuery>,
            read_time: Option<Timestamp>,
            transaction: Option<DecodedBytes>,
            new_transaction: Option<UnsupportedMessageObject>,
            explain_options: Option<UnsupportedMessageObject>,
            request_options: Option<UnsupportedMessageObject>,
        }
        let helper = RunQueryRequestDe::deserialize(deserializer)?;
        let consistency_selector =
            parse_consistency_selector(
                "RunQueryRequest",
                helper.read_time,
                helper.transaction,
                helper.new_transaction,
            )?
            .map(|cs| match cs {
                ParsedConsistency::ReadTime(rt) => {
                    run_query_request::ConsistencySelector::ReadTime(rt)
                }
                ParsedConsistency::Transaction(tx) => {
                    run_query_request::ConsistencySelector::Transaction(tx)
                }
                ParsedConsistency::NewTransaction => {
                    run_query_request::ConsistencySelector::NewTransaction(
                        TransactionOptions::default(),
                    )
                }
            });
        Ok(RunQueryRequest {
            parent: helper.parent,
            query_type: helper
                .structured_query
                .map(run_query_request::QueryType::StructuredQuery),
            consistency_selector,
            explain_options: helper.explain_options.map(|_| ExplainOptions::default()),
            request_options: helper.request_options.map(|_| RequestOptions::default()),
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
            skipped_results: Option<ProtoInt32>,
            done: Option<bool>,
        }
        let helper = RunQueryResponseDe::deserialize(deserializer)?;
        Ok(RunQueryResponse {
            transaction: helper.transaction.map_or_else(Vec::new, |b| b.0.to_vec()),
            document: helper.document,
            read_time: helper.read_time,
            skipped_results: helper.skipped_results.map_or(0, |v| v.0),
            continuation_selector: helper
                .done
                .map(run_query_response::ContinuationSelector::Done),
            explain_metrics: None,
        })
    }
}

impl Serialize for Count {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct CountSer {
            #[serde(skip_serializing_if = "Option::is_none")]
            up_to: Option<String>,
        }
        CountSer {
            up_to: self.up_to.map(|v| v.value.to_string()),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Count {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct CountDe {
            up_to: Option<ProtoInt64>,
        }
        let helper = CountDe::deserialize(deserializer)?;
        Ok(Count {
            up_to: helper.up_to.map(|v| Int64Value { value: v.0 }),
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FieldAggregationSer<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    field: &'a Option<FieldReference>,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
struct FieldAggregationDe {
    field: Option<FieldReference>,
}

impl Serialize for Sum {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        FieldAggregationSer { field: &self.field }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Sum {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let helper = FieldAggregationDe::deserialize(deserializer)?;
        Ok(Sum {
            field: helper.field,
        })
    }
}

impl Serialize for Avg {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        FieldAggregationSer { field: &self.field }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Avg {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let helper = FieldAggregationDe::deserialize(deserializer)?;
        Ok(Avg {
            field: helper.field,
        })
    }
}

impl Serialize for Aggregation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(ref op) = self.operator else {
            return Err(serde::ser::Error::custom("Aggregation is missing operator"));
        };
        let len = 1 + usize::from(!self.alias.is_empty());
        let mut map = serializer.serialize_map(Some(len))?;
        match op {
            Operator::Count(c) => map.serialize_entry("count", c)?,
            Operator::Sum(s) => map.serialize_entry("sum", s)?,
            Operator::Avg(a) => map.serialize_entry("avg", a)?,
        }
        if !self.alias.is_empty() {
            map.serialize_entry("alias", &self.alias)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Aggregation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct AggregationDe {
            count: Option<Count>,
            sum:   Option<Sum>,
            avg:   Option<Avg>,
            #[serde(deserialize_with = "deserialize_null_default")]
            alias: String,
        }
        let helper = AggregationDe::deserialize(deserializer)?;
        let operator = match (helper.count, helper.sum, helper.avg) {
            (Some(c), None, None) => Operator::Count(c),
            (None, Some(s), None) => Operator::Sum(s),
            (None, None, Some(a)) => Operator::Avg(a),
            (None, None, None) => {
                return Err(de::Error::custom(
                    "Aggregation must specify one of 'count', 'sum', or 'avg'",
                ));
            }
            _ => {
                return Err(de::Error::custom(
                    "Aggregation cannot specify more than one operator",
                ));
            }
        };
        Ok(Aggregation {
            alias:    helper.alias,
            operator: Some(operator),
        })
    }
}

impl Serialize for StructuredAggregationQuery {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct StructuredAggregationQuerySer<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            structured_query: Option<&'a StructuredQuery>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            aggregations:     &'a Vec<Aggregation>,
        }
        let structured_query = self.query_type.as_ref().map(|qt| match qt {
            structured_aggregation_query::QueryType::StructuredQuery(sq) => sq,
        });
        StructuredAggregationQuerySer {
            structured_query,
            aggregations: &self.aggregations,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StructuredAggregationQuery {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct StructuredAggregationQueryDe {
            structured_query: Option<StructuredQuery>,
            #[serde(deserialize_with = "deserialize_null_default")]
            aggregations:     Vec<Aggregation>,
        }
        let helper = StructuredAggregationQueryDe::deserialize(deserializer)?;
        Ok(StructuredAggregationQuery {
            aggregations: helper.aggregations,
            query_type:   helper
                .structured_query
                .map(structured_aggregation_query::QueryType::StructuredQuery),
        })
    }
}

impl Serialize for RunAggregationQueryRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        check_unsupported_query_serialize::<S::Error>(
            "RunAggregationQueryRequest",
            self.explain_options.is_some(),
            self.request_options.is_some(),
            matches!(
                self.consistency_selector,
                Some(run_aggregation_query_request::ConsistencySelector::NewTransaction(_))
            ),
        )?;
        let mut map = serializer.serialize_map(None)?;
        if !self.parent.is_empty() {
            map.serialize_entry("parent", &self.parent)?;
        }
        if let Some(run_aggregation_query_request::QueryType::StructuredAggregationQuery(ref saq)) =
            self.query_type
        {
            map.serialize_entry("structuredAggregationQuery", saq)?;
        }
        match &self.consistency_selector {
            Some(run_aggregation_query_request::ConsistencySelector::Transaction(tx)) => {
                map.serialize_entry("transaction", &Base64Bytes(tx))?;
            }
            Some(run_aggregation_query_request::ConsistencySelector::ReadTime(rt)) => {
                map.serialize_entry("readTime", rt)?;
            }
            Some(run_aggregation_query_request::ConsistencySelector::NewTransaction(_)) | None => {}
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RunAggregationQueryRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default, deny_unknown_fields)]
        struct RunAggregationQueryRequestDe {
            #[serde(deserialize_with = "deserialize_null_default")]
            parent: String,
            structured_aggregation_query: Option<StructuredAggregationQuery>,
            read_time: Option<Timestamp>,
            transaction: Option<DecodedBytes>,
            new_transaction: Option<UnsupportedMessageObject>,
            explain_options: Option<UnsupportedMessageObject>,
            request_options: Option<UnsupportedMessageObject>,
        }
        let helper = RunAggregationQueryRequestDe::deserialize(deserializer)?;
        let consistency_selector = parse_consistency_selector(
            "RunAggregationQueryRequest",
            helper.read_time,
            helper.transaction,
            helper.new_transaction,
        )?
        .map(|cs| match cs {
            ParsedConsistency::ReadTime(rt) => {
                run_aggregation_query_request::ConsistencySelector::ReadTime(rt)
            }
            ParsedConsistency::Transaction(tx) => {
                run_aggregation_query_request::ConsistencySelector::Transaction(tx)
            }
            ParsedConsistency::NewTransaction => {
                run_aggregation_query_request::ConsistencySelector::NewTransaction(
                    TransactionOptions::default(),
                )
            }
        });
        Ok(RunAggregationQueryRequest {
            parent: helper.parent,
            query_type: helper
                .structured_aggregation_query
                .map(run_aggregation_query_request::QueryType::StructuredAggregationQuery),
            consistency_selector,
            explain_options: helper.explain_options.map(|_| ExplainOptions::default()),
            request_options: helper.request_options.map(|_| RequestOptions::default()),
        })
    }
}

impl Serialize for AggregationResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct AggregationResultSer<'a> {
            #[serde(
                skip_serializing_if = "HashMap::is_empty",
                serialize_with = "serialize_sorted_map"
            )]
            aggregate_fields: &'a HashMap<String, Value>,
        }
        AggregationResultSer {
            aggregate_fields: &self.aggregate_fields,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AggregationResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default)]
        struct AggregationResultDe {
            #[serde(deserialize_with = "deserialize_null_default")]
            aggregate_fields: HashMap<String, Value>,
        }
        let helper = AggregationResultDe::deserialize(deserializer)?;
        Ok(AggregationResult {
            aggregate_fields: helper.aggregate_fields,
        })
    }
}

impl Serialize for RunAggregationQueryResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.explain_metrics.is_some() {
            return Err(serde::ser::Error::custom(
                "RunAggregationQueryResponse.explainMetrics ProtoJSON serialization is not \
                 supported",
            ));
        }
        let mut map = serializer.serialize_map(None)?;
        if let Some(ref result) = self.result {
            map.serialize_entry("result", result)?;
        }
        if !self.transaction.is_empty() {
            map.serialize_entry("transaction", &Base64Bytes(&self.transaction))?;
        }
        if let Some(ref rt) = self.read_time {
            map.serialize_entry("readTime", rt)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RunAggregationQueryResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase", default)]
        struct RunAggregationQueryResponseDe {
            result:      Option<AggregationResult>,
            transaction: Option<DecodedBytes>,
            read_time:   Option<Timestamp>,
        }
        let helper = RunAggregationQueryResponseDe::deserialize(deserializer)?;
        Ok(RunAggregationQueryResponse {
            result: helper.result,
            transaction: helper.transaction.map_or_else(Vec::new, |b| b.0.to_vec()),
            read_time: helper.read_time,
            explain_metrics: None,
        })
    }
}

impl Serialize for ListCollectionIdsRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.request_options.is_some() {
            return Err(serde::ser::Error::custom(
                "ListCollectionIdsRequest.requestOptions ProtoJSON serialization is not supported",
            ));
        }
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
            #[serde(deserialize_with = "deserialize_null_default")]
            parent:     String,
            page_size:  Option<ProtoInt32>,
            #[serde(deserialize_with = "deserialize_null_default")]
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
            request_options: None,
        })
    }
}

#[cfg(test)]
mod tests;
