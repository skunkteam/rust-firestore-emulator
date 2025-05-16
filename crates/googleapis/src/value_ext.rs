use std::{cmp, collections::HashMap, iter::Sum, ops::Add};

use itertools::Itertools;
use prost::bytes::Bytes;

use crate::google::{
    firestore::v1::{ArrayValue, MapValue, Value, value::ValueType},
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

    pub fn double(value: f64) -> Self {
        Self {
            value_type: Some(ValueType::DoubleValue(value)),
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

    pub fn as_double(&self) -> Option<f64> {
        match self.value_type() {
            ValueType::DoubleValue(d) => Some(*d),
            ValueType::IntegerValue(i) => Some(*i as f64),
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

    fn value_type_order(&self) -> usize {
        // See: https://firebase.google.com/docs/firestore/manage-data/data-types#value_type_ordering
        match self.value_type() {
            ValueType::NullValue(_) => 1,
            ValueType::BooleanValue(_) => 2,
            ValueType::DoubleValue(value) if value.is_nan() => 3,
            ValueType::IntegerValue(_) => 4,
            ValueType::DoubleValue(_) => 4,
            ValueType::TimestampValue(_) => 5,
            ValueType::StringValue(_) => 6,
            ValueType::BytesValue(_) => 7,
            ValueType::ReferenceValue(_) => 8,
            ValueType::GeoPointValue(_) => 9,
            ValueType::ArrayValue(_) => 10,
            ValueType::MapValue(_) => 11,
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
        let type_order = self.value_type_order().cmp(&other.value_type_order());
        if type_order != cmp::Ordering::Equal {
            return type_order;
        }
        // See: https://cloud.google.com/firestore/docs/concepts/data-types#data_types
        match (self.value_type(), other.value_type()) {
            (ValueType::NullValue(_), ValueType::NullValue(_)) => cmp::Ordering::Equal,
            (ValueType::BooleanValue(a), ValueType::BooleanValue(b)) => a.cmp(b),
            (ValueType::DoubleValue(a), ValueType::DoubleValue(b)) => a.total_cmp(b),
            (ValueType::DoubleValue(a), ValueType::IntegerValue(b)) => a.total_cmp(&(*b as f64)),
            (ValueType::IntegerValue(a), ValueType::DoubleValue(b)) => (*a as f64).total_cmp(b),
            (ValueType::IntegerValue(a), ValueType::IntegerValue(b)) => a.cmp(b),
            (ValueType::TimestampValue(a), ValueType::TimestampValue(b)) => a.cmp(b),
            (ValueType::StringValue(a), ValueType::StringValue(b)) => a.cmp(b),
            (ValueType::BytesValue(a), ValueType::BytesValue(b)) => a.cmp(b),
            (ValueType::ReferenceValue(a), ValueType::ReferenceValue(b)) => {
                /// Datastore allowed numeric IDs where Firestore only allows strings. Numeric
                /// IDs are exposed to Firestore as `__idNUM__`, so this is the lowest possible
                /// negative numeric value expressed in that format. It should be lower than
                /// everything else.
                ///
                /// This constant is used to specify startAt/endAt values when querying for all
                /// descendants in a single collection.
                const REFERENCE_NAME_MIN_ID: &str = "__id-9223372036854775808__";

                fn process_numeric_ids(s: &str) -> &str {
                    // TODO: Maybe support all numeric IDs? We need this one at least, because it is
                    // used by the JavaScript SDK, but we could try to support all numeric IDs here
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
            // Only the above types should need to be compared here, because of the type ordering
            // above.
            _ => unreachable!("logic error in Ord implementation of Value"),
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

impl Eq for LatLng {}

impl Ord for LatLng {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.latitude
            .total_cmp(&other.latitude)
            .then_with(|| self.longitude.total_cmp(&other.longitude))
    }
}

impl PartialOrd for LatLng {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use itertools::Itertools;

    use crate::google::firestore::v1::Value;

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
        for (a, b) in ordered_refs
            .into_iter()
            .map(str::to_string)
            .map(Value::reference)
            .tuple_windows()
        {
            expect_that!(a, lt(&b));
        }
    }
}
