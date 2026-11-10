use std::{cmp, collections::HashMap, iter::Sum, ops::Add};

use itertools::Itertools;
use tonic_prost::prost::bytes::Bytes;

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
}
