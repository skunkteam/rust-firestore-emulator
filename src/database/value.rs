use crate::{
    googleapis::google::{
        firestore::v1::{value::ValueType, ArrayValue, MapValue},
        r#type::LatLng,
    },
    utils::CmpTimestamp,
};
use std::{borrow::Cow, cmp, collections::HashMap};

pub use crate::googleapis::google::firestore::v1::Value;

impl Value {
    pub fn reference(reference: String) -> Self {
        Self {
            value_type: Some(ValueType::ReferenceValue(reference)),
        }
    }

    pub fn map(fields: HashMap<String, Self>) -> Self {
        Self {
            value_type: Some(ValueType::MapValue(MapValue { fields })),
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

    pub fn as_array(&self) -> Option<&[Self]> {
        match self.value_type() {
            ValueType::ArrayValue(ArrayValue { values }) => Some(values),
            _ => None,
        }
    }

    pub fn is_nan(&self) -> bool {
        matches!(self.value_type(), ValueType::DoubleValue(v) if v.is_nan())
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value_type(), ValueType::NullValue(_))
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
        if matches!(type_order, cmp::Ordering::Less | cmp::Ordering::Greater) {
            return type_order;
        }
        match (self.value_type(), other.value_type()) {
            (ValueType::NullValue(_), ValueType::NullValue(_)) => cmp::Ordering::Equal,
            (ValueType::BooleanValue(a), ValueType::BooleanValue(b)) => a.cmp(b),
            (ValueType::DoubleValue(a), ValueType::DoubleValue(b)) => a.total_cmp(b),
            (ValueType::DoubleValue(a), ValueType::IntegerValue(b)) => a.total_cmp(&(*b as f64)),
            (ValueType::IntegerValue(a), ValueType::DoubleValue(b)) => (*a as f64).total_cmp(b),
            (ValueType::IntegerValue(a), ValueType::IntegerValue(b)) => a.cmp(b),
            (ValueType::TimestampValue(a), ValueType::TimestampValue(b)) => {
                CmpTimestamp(a).cmp(&CmpTimestamp(b))
            }
            (ValueType::StringValue(a), ValueType::StringValue(b)) => a.cmp(b),
            (ValueType::BytesValue(a), ValueType::BytesValue(b)) => a.cmp(b),
            (ValueType::ReferenceValue(a), ValueType::ReferenceValue(b)) => {
                prep_ref_for_cmp(a).cmp(&prep_ref_for_cmp(b))
            }
            (ValueType::GeoPointValue(a), ValueType::GeoPointValue(b)) => a.cmp(b),
            (ValueType::ArrayValue(a), ValueType::ArrayValue(b)) => a.values.cmp(&b.values),
            (ValueType::MapValue(_a), ValueType::MapValue(_b)) => todo!("ordering for MapValues"),
            // Only the above types should need to be compared here, because of the type ordering above.
            _ => unreachable!("logic error in Ord implementation of Value"),
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

/// Datastore allowed numeric IDs where Firestore only allows strings. Numeric
/// IDs are exposed to Firestore as `__idNUM__`, so this is the lowest possible
/// negative numeric value expressed in that format.
///
/// This constant is used to specify startAt/endAt values when querying for all
/// descendants in a single collection.
const REFERENCE_NAME_MIN_ID: &str = "__id-9223372036854775808__";

fn prep_ref_for_cmp(path: &str) -> Cow<str> {
    (|| -> Option<Cow<str>> {
        let path = path.strip_suffix(REFERENCE_NAME_MIN_ID)?;
        let collection = path.strip_suffix('/')?;
        let result = match collection.strip_suffix('\0') {
            Some(collection) => Cow::Owned(collection.to_string() + "@"),
            None => Cow::Borrowed(path),
        };
        Some(result)
    })()
    .unwrap_or(Cow::Borrowed(path))
}
