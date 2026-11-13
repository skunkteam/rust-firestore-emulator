use std::{cmp, collections::HashMap, iter::Sum, ops::Add};

use bytes::Bytes;
use itertools::Itertools;

use crate::google::{
    firestore::v1::{
        ArrayValue, Function, MapValue, Pipeline, Value, pipeline::Stage, value::ValueType,
    },
    protobuf::Timestamp,
    r#type::LatLng,
};

impl Value {
    pub fn reference(reference: String) -> Self {
        Self {
            value_type: Some(ValueType::ReferenceValue(reference)),
        }
    }

    pub fn array(values: Vec<Self>) -> Self {
        Self {
            value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
        }
    }

    pub fn map(fields: HashMap<String, Self>) -> Self {
        Self {
            value_type: Some(ValueType::MapValue(MapValue { fields })),
        }
    }

    pub fn null() -> Self {
        Self {
            value_type: Some(ValueType::NullValue(0)),
        }
    }

    pub fn integer(value: i64) -> Self {
        Self {
            value_type: Some(ValueType::IntegerValue(value)),
        }
    }

    pub fn boolean(value: bool) -> Self {
        Self {
            value_type: Some(ValueType::BooleanValue(value)),
        }
    }

    pub fn double(value: f64) -> Self {
        Self {
            value_type: Some(ValueType::DoubleValue(value)),
        }
    }

    pub fn string(value: impl Into<String>) -> Self {
        Self {
            value_type: Some(ValueType::StringValue(value.into())),
        }
    }

    pub fn timestamp(value: Timestamp) -> Self {
        Self {
            value_type: Some(ValueType::TimestampValue(value)),
        }
    }

    pub fn bytes(value: impl Into<Bytes>) -> Self {
        Self {
            value_type: Some(ValueType::BytesValue(value.into())),
        }
    }

    pub fn as_field_reference(&self) -> Option<&str> {
        match self.value_type() {
            ValueType::FieldReferenceValue(reference) => Some(reference),
            _ => None,
        }
    }

    pub fn as_function_value(&self) -> Option<&Function> {
        match self.value_type() {
            ValueType::FunctionValue(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_reference(&self) -> Option<&str> {
        match self.value_type() {
            ValueType::ReferenceValue(reference) => Some(reference),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&HashMap<String, Self>> {
        match self.value_type() {
            ValueType::MapValue(MapValue { fields }) => Some(fields),
            _ => None,
        }
    }

    pub fn as_map_mut(&mut self) -> Option<&mut HashMap<String, Self>> {
        match self.value_type_mut() {
            ValueType::MapValue(MapValue { fields }) => Some(fields),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self.value_type() {
            ValueType::StringValue(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_array(self) -> Option<Vec<Self>> {
        match self.into_value_type() {
            ValueType::ArrayValue(ArrayValue { values }) => Some(values),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Self]> {
        match self.value_type() {
            ValueType::ArrayValue(ArrayValue { values }) => Some(values),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self.value_type() {
            ValueType::BooleanValue(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self.value_type() {
            ValueType::DoubleValue(d) => Some(*d),
            ValueType::IntegerValue(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_integer(&self) -> Option<i64> {
        match self.value_type() {
            ValueType::IntegerValue(i) => Some(*i),
            _ => None,
        }
    }

    pub fn is_nan(&self) -> bool {
        matches!(self.value_type(), ValueType::DoubleValue(v) if v.is_nan())
    }

    pub fn is_number(&self) -> bool {
        matches!(
            self.value_type(),
            ValueType::IntegerValue(_) | ValueType::DoubleValue(_)
        )
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value_type(), ValueType::NullValue(_))
    }

    pub fn into_value_type(self) -> ValueType {
        self.value_type.expect("missing value_type in value")
    }

    pub fn value_type(&self) -> &ValueType {
        self.value_type
            .as_ref()
            .expect("missing value_type in value")
    }

    pub fn value_type_mut(&mut self) -> &mut ValueType {
        self.value_type
            .as_mut()
            .expect("missing value_type in value")
    }

    pub fn is_compatible_with(&self, other: &Self) -> bool {
        self.value_type_order() == other.value_type_order()
    }

    pub fn storage_eq(&self, other: &Self) -> bool {
        match (self.value_type.as_ref(), other.value_type.as_ref()) {
            (None, None) => true,
            (Some(ValueType::NullValue(a)), Some(ValueType::NullValue(b))) => a == b,
            (Some(ValueType::BooleanValue(a)), Some(ValueType::BooleanValue(b))) => a == b,
            (Some(ValueType::IntegerValue(a)), Some(ValueType::IntegerValue(b))) => a == b,
            (Some(ValueType::DoubleValue(a)), Some(ValueType::DoubleValue(b))) => {
                a.to_bits() == b.to_bits()
            }
            (Some(ValueType::TimestampValue(a)), Some(ValueType::TimestampValue(b))) => a == b,
            (Some(ValueType::StringValue(a)), Some(ValueType::StringValue(b))) => a == b,
            (Some(ValueType::BytesValue(a)), Some(ValueType::BytesValue(b))) => a == b,
            (Some(ValueType::ReferenceValue(a)), Some(ValueType::ReferenceValue(b))) => a == b,
            (Some(ValueType::GeoPointValue(a)), Some(ValueType::GeoPointValue(b))) => {
                a.latitude.to_bits() == b.latitude.to_bits()
                    && a.longitude.to_bits() == b.longitude.to_bits()
            }
            (Some(ValueType::ArrayValue(a)), Some(ValueType::ArrayValue(b))) => {
                a.values.len() == b.values.len()
                    && a.values.iter().zip(&b.values).all(|(x, y)| x.storage_eq(y))
            }
            (Some(ValueType::MapValue(a)), Some(ValueType::MapValue(b))) => {
                Self::fields_storage_eq(&a.fields, &b.fields)
            }
            (Some(ValueType::FieldReferenceValue(a)), Some(ValueType::FieldReferenceValue(b))) => {
                a == b
            }
            (Some(ValueType::FunctionValue(a)), Some(ValueType::FunctionValue(b))) => {
                a.name == b.name
                    && a.args.len() == b.args.len()
                    && a.args.iter().zip(&b.args).all(|(x, y)| x.storage_eq(y))
                    && Self::fields_storage_eq(&a.options, &b.options)
            }
            (Some(ValueType::PipelineValue(a)), Some(ValueType::PipelineValue(b))) => {
                a.stages.len() == b.stages.len()
                    && a.stages.iter().zip(&b.stages).all(|(x, y)| {
                        x.name == y.name
                            && x.args.len() == y.args.len()
                            && x.args.iter().zip(&y.args).all(|(xa, ya)| xa.storage_eq(ya))
                            && Self::fields_storage_eq(&x.options, &y.options)
                    })
            }
            _ => false,
        }
    }

    pub fn fields_storage_eq(a: &HashMap<String, Self>, b: &HashMap<String, Self>) -> bool {
        a.len() == b.len()
            && a.iter()
                .all(|(k, v)| b.get(k).is_some_and(|ov| v.storage_eq(ov)))
    }

    fn value_type_order(&self) -> usize {
        value_type_rank(self.value_type.as_ref())
    }
}

fn value_type_rank(vt: Option<&ValueType>) -> usize {
    // See: https://firebase.google.com/docs/firestore/manage-data/data-types#value_type_ordering
    match vt {
        None => 0,
        Some(ValueType::NullValue(_)) => 1,
        Some(ValueType::BooleanValue(_)) => 2,
        Some(ValueType::DoubleValue(value)) if value.is_nan() => 3,
        Some(ValueType::IntegerValue(_)) => 4,
        Some(ValueType::DoubleValue(_)) => 4,
        Some(ValueType::TimestampValue(_)) => 5,
        Some(ValueType::StringValue(_)) => 6,
        Some(ValueType::BytesValue(_)) => 7,
        Some(ValueType::ReferenceValue(_)) => 8,
        Some(ValueType::GeoPointValue(_)) => 9,
        Some(ValueType::ArrayValue(_)) => 10,
        Some(ValueType::MapValue(_)) => 11,
        // The following are not allowed to be used when writing documents.
        Some(ValueType::FieldReferenceValue(_)) => usize::MAX - 2,
        Some(ValueType::FunctionValue(_)) => usize::MAX - 1,
        Some(ValueType::PipelineValue(_)) => usize::MAX,
    }
}

fn cmp_f64(a: f64, b: f64) -> cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => cmp::Ordering::Equal,
        (true, false) => cmp::Ordering::Less,
        (false, true) => cmp::Ordering::Greater,
        (false, false) => {
            let a = if a == 0.0 { 0.0 } else { a };
            let b = if b == 0.0 { 0.0 } else { b };
            a.total_cmp(&b)
        }
    }
}

fn cmp_i64_f64(i: i64, d: f64) -> cmp::Ordering {
    // -2^63 is exactly representable as f64.
    const MIN_I64_AS_F64: f64 = i64::MIN as f64;
    // +2^63 is the first f64 strictly greater than i64::MAX (2^63 - 1).
    const EXCLUSIVE_MAX_I64_AS_F64: f64 = 9_223_372_036_854_775_808.0_f64;

    if d < MIN_I64_AS_F64 {
        return cmp::Ordering::Greater;
    }
    if d >= EXCLUSIVE_MAX_I64_AS_F64 {
        return cmp::Ordering::Less;
    }
    let d_floor = d.floor();
    let d_int = d_floor as i64;
    match i.cmp(&d_int) {
        cmp::Ordering::Equal => {
            if d == d_floor {
                cmp::Ordering::Equal
            } else {
                cmp::Ordering::Less
            }
        }
        ord => ord,
    }
}

fn cmp_value_type(a: &ValueType, b: &ValueType) -> cmp::Ordering {
    value_type_rank(Some(a))
        .cmp(&value_type_rank(Some(b)))
        .then_with(|| {
            // See: https://cloud.google.com/firestore/docs/concepts/data-types#data_types
            match (a, b) {
                (ValueType::NullValue(_), ValueType::NullValue(_)) => cmp::Ordering::Equal,
                (ValueType::BooleanValue(a), ValueType::BooleanValue(b)) => a.cmp(b),
                (ValueType::DoubleValue(a), ValueType::DoubleValue(b)) => cmp_f64(*a, *b),
                (ValueType::DoubleValue(a), ValueType::IntegerValue(b)) => {
                    cmp_i64_f64(*b, *a).reverse()
                }
                (ValueType::IntegerValue(a), ValueType::DoubleValue(b)) => cmp_i64_f64(*a, *b),
                (ValueType::IntegerValue(a), ValueType::IntegerValue(b)) => a.cmp(b),
                (ValueType::TimestampValue(a), ValueType::TimestampValue(b)) => a.cmp(b),
                (ValueType::StringValue(a), ValueType::StringValue(b)) => a.cmp(b),
                (ValueType::BytesValue(a), ValueType::BytesValue(b)) => a.cmp(b),
                (ValueType::ReferenceValue(a), ValueType::ReferenceValue(b)) => {
                    /// Datastore allowed numeric IDs where Firestore only allows strings.
                    /// Numeric IDs are exposed to Firestore as
                    /// `__idNUM__`, so this is the lowest possible
                    /// negative numeric value expressed in that format. It should be lower than
                    /// everything else.
                    ///
                    /// This constant is used to specify startAt/endAt values when querying for
                    /// all descendants in a single collection.
                    const REFERENCE_NAME_MIN_ID: &str = "__id-9223372036854775808__";

                    fn process_numeric_ids(s: &str) -> &str {
                        // TODO: Maybe support all numeric IDs? We need this one at least,
                        // because it is used by the JavaScript SDK,
                        // but we could try to support all numeric IDs here
                        // for fun and profit.
                        match s {
                            REFERENCE_NAME_MIN_ID => "",
                            _ => s,
                        }
                    }
                    let a = a.split('/').map(process_numeric_ids);
                    let b = b.split('/').map(process_numeric_ids);
                    a.cmp(b)
                }
                (ValueType::GeoPointValue(a), ValueType::GeoPointValue(b)) => a.cmp(b),
                (ValueType::ArrayValue(a), ValueType::ArrayValue(b)) => a.values.cmp(&b.values),
                (ValueType::MapValue(a), ValueType::MapValue(b)) => {
                    a.fields.iter().sorted().cmp(b.fields.iter().sorted())
                }
                (ValueType::FieldReferenceValue(a), ValueType::FieldReferenceValue(b)) => a.cmp(b),
                (ValueType::FunctionValue(a), ValueType::FunctionValue(b)) => a.cmp(b),
                (ValueType::PipelineValue(a), ValueType::PipelineValue(b)) => a.cmp(b),
                // Only the above types should need to be compared here, because of the type
                // ordering above.
                _ => unreachable!("logic error in Ord implementation of Value"),
            }
        })
}

impl PartialEq for ValueType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueType::MapValue(a), ValueType::MapValue(b)) => a.fields == b.fields,
            (ValueType::ArrayValue(a), ValueType::ArrayValue(b)) => a.values == b.values,
            (ValueType::StringValue(a), ValueType::StringValue(b)) => a == b,
            (ValueType::BytesValue(a), ValueType::BytesValue(b)) => a == b,
            _ => cmp_value_type(self, other).is_eq(),
        }
    }
}

impl Eq for ValueType {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (&self.value_type, &other.value_type) {
            (None, None) => true,
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match (&self.value_type, &other.value_type) {
            (None, None) => cmp::Ordering::Equal,
            (None, Some(_)) => cmp::Ordering::Less,
            (Some(_), None) => cmp::Ordering::Greater,
            (Some(a), Some(b)) => cmp_value_type(a, b),
        }
    }
}

// This is the implementation specific to: server side increment with the following rules:
//
// This must be an integer or a double value.
// If the field is not an integer or double, or if the field does not yet exist, the transformation
// will set the field to the given value. If either of the given value or the current field value
// are doubles, both values will be interpreted as doubles. Double arithmetic and representation of
// double values follow IEEE 754 semantics. If there is positive/negative integer overflow, the
// field is resolved to the largest magnitude positive/negative integer.
impl Add for Value {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        use ValueType::*;
        match (self.value_type(), rhs.value_type()) {
            (IntegerValue(a), IntegerValue(b)) => Self::integer(a.saturating_add(*b)),
            (IntegerValue(a), DoubleValue(b)) => Self::double(*a as f64 + b),
            (DoubleValue(a), IntegerValue(b)) => Self::double(a + *b as f64),
            (DoubleValue(a), DoubleValue(b)) => Self::double(a + b),
            _ => rhs,
        }
    }
}

// This is the implementation specific to: server side sum and avg aggregation with the following
// rules:
//
// * Only numeric values will be aggregated. All non-numeric values including `NULL` are skipped.
//
// * If the aggregated values contain `NaN`, returns `NaN`. Infinity math follows IEEE-754
//   standards.
//
// * If the aggregated value set is empty, returns 0.
//
// * Returns a 64-bit integer if all aggregated numbers are integers and the sum result does not
//   overflow. Otherwise, the result is returned as a double. Note that even if all the aggregated
//   values are integers, the result is returned as a double if it cannot fit within a 64-bit signed
//   integer. When this occurs, the returned value will lose precision.
//
// * When underflow occurs, floating-point aggregation is non-deterministic. This means that running
//   the same query repeatedly without any changes to the underlying values could produce slightly
//   different results each time. In those cases, values should be stored as integers over
//   floating-point numbers.
impl Sum for Value {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        use ValueType::*;
        let value_type = iter
            .map(|v| v.value_type().clone())
            .fold(IntegerValue(0), |a, b| match (a, b) {
                (IntegerValue(a), IntegerValue(b)) => a
                    .checked_add(b)
                    .map(IntegerValue)
                    .unwrap_or_else(|| DoubleValue(a as f64 + b as f64)),
                (IntegerValue(a), DoubleValue(b)) => DoubleValue(a as f64 + b),
                (DoubleValue(a), IntegerValue(b)) => DoubleValue(a + b as f64),
                (DoubleValue(a), DoubleValue(b)) => DoubleValue(a + b),
                (a @ (DoubleValue(_) | IntegerValue(_)), _) => a,
                (_, b @ (DoubleValue(_) | IntegerValue(_))) => b,
                _ => IntegerValue(0),
            });
        Self {
            value_type: Some(value_type),
        }
    }
}

impl Eq for Function {}

impl Ord for Function {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.args.cmp(&other.args))
            .then_with(|| {
                self.options
                    .iter()
                    .sorted()
                    .cmp(other.options.iter().sorted())
            })
    }
}

impl PartialOrd for Function {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Stage {}

impl Ord for Stage {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.args.cmp(&other.args))
            .then_with(|| {
                self.options
                    .iter()
                    .sorted()
                    .cmp(other.options.iter().sorted())
            })
    }
}

impl PartialOrd for Stage {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Pipeline {}

impl Ord for Pipeline {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.stages.cmp(&other.stages)
    }
}

impl PartialOrd for Pipeline {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LatLng {}

impl Ord for LatLng {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        cmp_f64(self.latitude, other.latitude)
            .then_with(|| cmp_f64(self.longitude, other.longitude))
    }
}

impl PartialOrd for LatLng {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::boolean(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::integer(i64::from(value))
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::double(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::string(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::string(value)
    }
}

impl From<Bytes> for Value {
    fn from(value: Bytes) -> Self {
        Self::bytes(value)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::bytes(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::bytes(Bytes::copy_from_slice(value))
    }
}

impl<const N: usize> From<&[u8; N]> for Value {
    fn from(value: &[u8; N]) -> Self {
        Self::bytes(Bytes::copy_from_slice(value))
    }
}

impl<const N: usize> From<[u8; N]> for Value {
    fn from(value: [u8; N]) -> Self {
        Self::bytes(Bytes::copy_from_slice(&value))
    }
}

impl From<Timestamp> for Value {
    fn from(value: Timestamp) -> Self {
        Self::timestamp(value)
    }
}

impl From<LatLng> for Value {
    fn from(value: LatLng) -> Self {
        Self {
            value_type: Some(ValueType::GeoPointValue(value)),
        }
    }
}

impl From<Vec<Value>> for Value {
    fn from(values: Vec<Value>) -> Self {
        Self::array(values)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(fields: HashMap<String, Value>) -> Self {
        Self::map(fields)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Self::null(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::HashMap};

    use googletest::prelude::*;
    use itertools::Itertools;

    use crate::google::{
        firestore::v1::{Function, Pipeline, Value, pipeline::Stage, value::ValueType},
        r#type::LatLng,
    };

    #[gtest]
    fn reference_ordering() {
        let ordered_refs = [
            "/cola",
            "/cola/__id-9223372036854775808__",
            "/cola/\0",
            "/cola/_",
            "/cola\0/doca",
            "/cola\0/doca/colb/__id-9223372036854775808__",
            "/cola\0/doca/colb/\0",
            "/cola\0/doca/colb/docb",
            "/cola\0/doca\0",
            "/cola\0/docb",
            "/colb/doca",
        ];
        for [a, b] in ordered_refs
            .into_iter()
            .map(str::to_string)
            .map(Value::reference)
            .array_windows()
        {
            expect_lt!(a, &b);
        }
    }

    #[gtest]
    fn partial_eq_and_ord_coherence_across_numeric_and_special_values() {
        let pos_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let neg_nan = f64::from_bits(0xfff8_0000_0000_0002);

        let equivalence_classes: Vec<Vec<Value>> = vec![
            vec![Value::default()],
            vec![Value::null()],
            vec![Value::boolean(false)],
            vec![Value::boolean(true)],
            vec![
                Value::double(f64::NAN),
                Value::double(pos_nan),
                Value::double(neg_nan),
            ],
            vec![Value::double(f64::NEG_INFINITY)],
            vec![Value::integer(i64::MIN), Value::double(i64::MIN as f64)],
            vec![Value::integer(-42), Value::double(-42.0)],
            vec![Value::double(-0.5)],
            vec![Value::integer(0), Value::double(0.0), Value::double(-0.0)],
            vec![Value::double(0.5)],
            vec![Value::integer(42), Value::double(42.0)],
            vec![Value::integer(i64::MAX - 1)],
            vec![Value::integer(i64::MAX)],
            vec![Value::double(i64::MAX as f64)], // +2^63
            vec![Value::double(f64::INFINITY)],
            vec![Value::string("abc")],
            vec![Value::bytes(&b"abc"[..])],
            vec![
                Value::reference(
                    "projects/p/databases/d/documents/c/__id-9223372036854775808__".to_string(),
                ),
                Value::reference("projects/p/databases/d/documents/c/".to_string()),
            ],
            vec![
                Value {
                    value_type: Some(ValueType::GeoPointValue(LatLng {
                        latitude:  -0.0,
                        longitude: 0.0,
                    })),
                },
                Value {
                    value_type: Some(ValueType::GeoPointValue(LatLng {
                        latitude:  0.0,
                        longitude: -0.0,
                    })),
                },
            ],
            vec![
                Value::array(vec![Value::integer(1), Value::double(f64::NAN)]),
                Value::array(vec![Value::double(1.0), Value::double(neg_nan)]),
            ],
            vec![
                Value::map(HashMap::from([("k".to_string(), Value::integer(7))])),
                Value::map(HashMap::from([("k".to_string(), Value::double(7.0))])),
            ],
            vec![Value {
                value_type: Some(ValueType::FieldReferenceValue("a".to_string())),
            }],
            vec![Value {
                value_type: Some(ValueType::FieldReferenceValue("b".to_string())),
            }],
            vec![
                Value {
                    value_type: Some(ValueType::FunctionValue(Function {
                        name:    "eq".to_string(),
                        args:    vec![Value::integer(1)],
                        options: HashMap::from([("opt".to_string(), Value::integer(2))]),
                    })),
                },
                Value {
                    value_type: Some(ValueType::FunctionValue(Function {
                        name:    "eq".to_string(),
                        args:    vec![Value::double(1.0)],
                        options: HashMap::from([("opt".to_string(), Value::double(2.0))]),
                    })),
                },
            ],
            vec![
                Value {
                    value_type: Some(ValueType::PipelineValue(Pipeline {
                        stages: vec![Stage {
                            name:    "where".to_string(),
                            args:    vec![Value::integer(3)],
                            options: HashMap::new(),
                        }],
                    })),
                },
                Value {
                    value_type: Some(ValueType::PipelineValue(Pipeline {
                        stages: vec![Stage {
                            name:    "where".to_string(),
                            args:    vec![Value::double(3.0)],
                            options: HashMap::new(),
                        }],
                    })),
                },
            ],
        ];

        for (i, class_a) in equivalence_classes.iter().enumerate() {
            for (j, class_b) in equivalence_classes.iter().enumerate() {
                let expected_ord = i.cmp(&j);
                for a in class_a {
                    for b in class_b {
                        expect_eq!(a.cmp(b), expected_ord, "failed cmp for {a:?} vs {b:?}");
                        expect_eq!(a == b, expected_ord == Ordering::Equal);
                        expect_eq!(a != b, expected_ord != Ordering::Equal);
                    }
                }
            }
        }
    }

    #[gtest]
    fn lat_lng_nan_transitivity() {
        let neg_nan = f64::from_bits(0xfff8_0000_0000_0001);
        let pos_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let a = LatLng {
            latitude:  neg_nan,
            longitude: 0.0,
        };
        let b = LatLng {
            latitude:  pos_nan,
            longitude: 0.0,
        };
        let c = LatLng {
            latitude:  0.0,
            longitude: 0.0,
        };
        expect_eq!(a.cmp(&b), Ordering::Equal);
        expect_eq!(a.cmp(&c), Ordering::Less);
        expect_eq!(b.cmp(&c), Ordering::Less);
    }

    #[gtest]
    fn storage_eq_distinguishes_representations() {
        expect_that!(Value::integer(1).storage_eq(&Value::integer(1)), eq(true));
        expect_that!(Value::integer(1).storage_eq(&Value::double(1.0)), eq(false));
        expect_that!(
            Value::double(0.0).storage_eq(&Value::double(-0.0)),
            eq(false)
        );
        expect_that!(
            Value::double(f64::NAN).storage_eq(&Value::double(f64::NAN)),
            eq(true)
        );

        // Nested array and map
        let map_int =
            HashMap::from([("nested".to_string(), Value::array(vec![Value::integer(5)]))]);
        let map_double =
            HashMap::from([("nested".to_string(), Value::array(vec![Value::double(5.0)]))]);
        expect_that!(map_int == map_double, eq(true));
        expect_that!(Value::fields_storage_eq(&map_int, &map_double), eq(false));
        expect_that!(Value::fields_storage_eq(&map_int, &map_int), eq(true));

        // ReferenceValue with sentinel vs empty segment
        let ref_sentinel = Value::reference(
            "projects/p/databases/d/documents/c/__id-9223372036854775808__".into(),
        );
        let ref_empty = Value::reference("projects/p/databases/d/documents/c/".into());
        expect_that!(ref_sentinel == ref_empty, eq(true));
        expect_that!(ref_sentinel.storage_eq(&ref_empty), eq(false));
    }

    #[gtest]
    fn from_conversions_preserve_exact_firestore_wire_types() {
        use bytes::Bytes;

        use crate::google::protobuf::Timestamp;

        expect_true!(Value::from(true).storage_eq(&Value::boolean(true)));
        expect_true!(Value::from(42_i32).storage_eq(&Value::integer(42)));
        expect_true!(Value::from(42_i64).storage_eq(&Value::integer(42)));
        expect_true!(Value::from(42.0_f64).storage_eq(&Value::double(42.0)));
        expect_false!(Value::from(42_i32).storage_eq(&Value::from(42.0_f64)));
        expect_true!(Value::from("abc").storage_eq(&Value::string("abc")));
        expect_true!(Value::from("abc".to_string()).storage_eq(&Value::string("abc")));
        expect_true!(
            Value::from(Bytes::from_static(b"xyz")).storage_eq(&Value::bytes(&b"xyz"[..]))
        );
        expect_true!(Value::from(vec![1_u8, 2, 3]).storage_eq(&Value::bytes(&[1_u8, 2, 3][..])));
        let slice: &[u8] = &[4, 5];
        expect_true!(Value::from(slice).storage_eq(&Value::bytes(&[4_u8, 5][..])));
        expect_true!(Value::from(b"hi").storage_eq(&Value::bytes(&b"hi"[..])));
        expect_true!(Value::from([9_u8, 8]).storage_eq(&Value::bytes(&[9_u8, 8][..])));
        let ts = Timestamp {
            seconds: 123,
            nanos:   456,
        };
        expect_true!(Value::from(ts).storage_eq(&Value::timestamp(ts)));
        let geo = LatLng {
            latitude:  52.0,
            longitude: 4.0,
        };
        expect_true!(Value::from(geo).storage_eq(&Value {
            value_type: Some(ValueType::GeoPointValue(geo)),
        }));
        expect_true!(
            Value::from(vec![Value::from(1_i32)])
                .storage_eq(&Value::array(vec![Value::integer(1)]))
        );
        let map = HashMap::from([("k".to_string(), Value::from("v"))]);
        expect_true!(Value::from(map.clone()).storage_eq(&Value::map(map)));
        expect_true!(Value::from(None::<i64>).storage_eq(&Value::null()));
        expect_true!(Value::from(Some(7_i32)).storage_eq(&Value::integer(7)));
    }

    mod proptests {
        use std::{cmp::Ordering, collections::HashMap};

        use bytes::Bytes;
        use proptest::prelude::*;

        use crate::google::{
            firestore::v1::{Value, value::ValueType},
            protobuf::Timestamp,
            r#type::LatLng,
        };

        fn arb_f64() -> impl Strategy<Value = f64> {
            prop_oneof![
                Just(0.0),
                Just(-0.0),
                Just(1.0),
                Just(-1.0),
                Just(f64::NAN),
                Just(f64::from_bits(0x7ff8_0000_0000_0001)),
                Just(f64::from_bits(0xfff8_0000_0000_0002)),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                Just(i64::MIN as f64),
                Just(i64::MAX as f64),
                Just(9_007_199_254_740_992.0),
                any::<f64>(),
            ]
        }

        fn arb_i64() -> impl Strategy<Value = i64> {
            prop_oneof![
                Just(0),
                Just(1),
                Just(-1),
                Just(i64::MIN),
                Just(i64::MIN + 1),
                Just(i64::MAX - 1),
                Just(i64::MAX),
                -1000_i64..=1000_i64,
                any::<i64>(),
            ]
        }

        fn arb_value() -> impl Strategy<Value = Value> {
            let leaf = prop_oneof![
                Just(Value::default()),
                Just(Value::null()),
                any::<bool>().prop_map(Value::boolean),
                arb_i64().prop_map(Value::integer),
                arb_f64().prop_map(Value::double),
                (
                    -62_135_596_800_i64..=253_402_300_799_i64,
                    0_i32..1_000_000_000_i32
                )
                    .prop_map(|(seconds, nanos)| Value::timestamp(Timestamp { seconds, nanos })),
                "[a-z0-9_/]{0,12}".prop_map(Value::string),
                prop::collection::vec(any::<u8>(), 0..8).prop_map(|v| Value::bytes(Bytes::from(v))),
                prop_oneof![
                    Just(Value::reference(
                        "projects/p/databases/(default)/documents/c/__id-9223372036854775808__"
                            .to_string(),
                    )),
                    Just(Value::reference(
                        "projects/p/databases/(default)/documents/c/".to_string()
                    )),
                    "[a-z]{1,4}/[a-z0-9]{1,4}".prop_map(|tail| {
                        Value::reference(format!("projects/p/databases/(default)/documents/{tail}"))
                    }),
                ],
                (arb_f64(), arb_f64()).prop_map(|(latitude, longitude)| Value {
                    value_type: Some(ValueType::GeoPointValue(LatLng {
                        latitude,
                        longitude,
                    })),
                }),
            ];
            leaf.prop_recursive(2, 16, 4, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..4).prop_map(Value::array),
                    prop::collection::hash_map("[a-d]{1,2}", inner, 0..4).prop_map(Value::map),
                ]
            })
        }

        proptest! {
            #[test]
            fn proptest_value_total_order_and_eq_laws(
                a in arb_value(),
                b in arb_value(),
                c in arb_value(),
            ) {
                // Reflexivity
                prop_assert_eq!(a.cmp(&a), Ordering::Equal);
                prop_assert_eq!(&a, &a);
                prop_assert!(a.storage_eq(&a));

                // Antisymmetry & PartialEq coherence
                let ab = a.cmp(&b);
                let ba = b.cmp(&a);
                prop_assert_eq!(ab, ba.reverse());
                prop_assert_eq!(a == b, ab == Ordering::Equal);
                prop_assert_eq!(a.storage_eq(&b), b.storage_eq(&a));
                if a.storage_eq(&b) {
                    prop_assert_eq!(&a, &b);
                }

                // Transitivity across arbitrary triples (sort the triple to force x <= y <= z)
                let mut triple = [a, b, c];
                triple.sort();
                prop_assert!(triple[0] <= triple[1]);
                prop_assert!(triple[1] <= triple[2]);
                prop_assert!(triple[0] <= triple[2]);
            }

            #[test]
            fn proptest_numeric_add_saturates_and_sum_promotes_overflow(
                a in arb_i64(),
                b in arb_i64(),
            ) {
                let va = Value::integer(a);
                let vb = Value::integer(b);

                // Increment (`Add`) saturates integer overflow to i64::MIN / i64::MAX.
                let inc = va.clone() + vb.clone();
                prop_assert!(inc.storage_eq(&Value::integer(a.saturating_add(b))));

                // Non-numeric receiver on `Add` is overwritten by rhs.
                let overwritten = Value::string("not-a-number") + vb.clone();
                prop_assert!(overwritten.storage_eq(&vb));

                // Aggregation (`Sum`) promotes integer overflow to f64 while skipping non-numbers.
                let summed: Value = [
                    Value::null(),
                    va,
                    Value::string("skip"),
                    vb,
                    Value::map(HashMap::new()),
                ]
                .into_iter()
                .sum();
                match a.checked_add(b) {
                    Some(exact) => prop_assert!(summed.storage_eq(&Value::integer(exact))),
                    None => prop_assert!(summed.storage_eq(&Value::double(a as f64 + b as f64))),
                }
            }
        }
    }
}
