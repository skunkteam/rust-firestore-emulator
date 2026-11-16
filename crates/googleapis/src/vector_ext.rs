use std::collections::HashMap;

use crate::google::firestore::v1::{Value, value::ValueType};

pub const VECTOR_MAP_TYPE_KEY: &str = "__type__";
pub const VECTOR_MAP_TYPE_SENTINEL: &str = "__vector__";
pub const VECTOR_MAP_VALUE_KEY: &str = "value";
pub const MAX_VECTOR_DIMENSIONS: usize = 2048;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorFormatError {
    ExtraneousEntries,
    MissingValueArray,
    EmptyVector,
    TooManyDimensions,
    NonDoubleElement,
    NanElement,
}

impl VectorFormatError {
    pub const fn as_error_message(self) -> &'static str {
        match self {
            Self::ExtraneousEntries => "Vector value map has extraneous entries.",
            Self::MissingValueArray => "Vector value map is missing 'value' array.",
            Self::EmptyVector => "Cannot have a zero length vector.",
            Self::TooManyDimensions => "Vectors must be at most 2048 dimensions.",
            Self::NonDoubleElement => "Vector must only contain values of type 'double'",
            Self::NanElement => "Vector cannot contain NaN values.",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorClassification<'a> {
    NotVectorMap,
    Valid(&'a [Value]),
    Invalid(VectorFormatError),
}

/// Classifies a Firestore `MapValue` field map as a non-vector map, a valid `VectorValue`
/// slice, or a malformed vector sentinel map with a typed diagnostic.
pub fn classify_vector_map(fields: &HashMap<String, Value>) -> VectorClassification<'_> {
    let Some(Value {
        value_type: Some(ValueType::StringValue(ty)),
    }) = fields.get(VECTOR_MAP_TYPE_KEY)
    else {
        return VectorClassification::NotVectorMap;
    };
    if ty != VECTOR_MAP_TYPE_SENTINEL {
        return VectorClassification::NotVectorMap;
    }

    if fields.len() > 2 {
        return VectorClassification::Invalid(VectorFormatError::ExtraneousEntries);
    }

    let Some(Value {
        value_type: Some(ValueType::ArrayValue(arr)),
    }) = fields.get(VECTOR_MAP_VALUE_KEY)
    else {
        return VectorClassification::Invalid(VectorFormatError::MissingValueArray);
    };

    if arr.values.is_empty() {
        return VectorClassification::Invalid(VectorFormatError::EmptyVector);
    }
    if arr.values.len() > MAX_VECTOR_DIMENSIONS {
        return VectorClassification::Invalid(VectorFormatError::TooManyDimensions);
    }

    for v in &arr.values {
        match v.value_type {
            Some(ValueType::DoubleValue(d)) => {
                if d.is_nan() {
                    return VectorClassification::Invalid(VectorFormatError::NanElement);
                }
            }
            _ => return VectorClassification::Invalid(VectorFormatError::NonDoubleElement),
        }
    }

    VectorClassification::Valid(&arr.values)
}

impl Value {
    pub fn vector(coords: impl IntoIterator<Item = f64>) -> Self {
        let values = coords.into_iter().map(Self::double).collect();
        Self::map(HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Self::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (VECTOR_MAP_VALUE_KEY.to_string(), Self::array(values)),
        ]))
    }

    pub fn is_vector(&self) -> bool {
        self.as_vector_values().is_some()
    }

    pub fn as_vector_values(&self) -> Option<&[Self]> {
        match self.value_type.as_ref() {
            Some(ValueType::MapValue(map)) => match classify_vector_map(&map.fields) {
                VectorClassification::Valid(values) => Some(values),
                VectorClassification::NotVectorMap | VectorClassification::Invalid(_) => None,
            },
            _ => None,
        }
    }

    pub fn iter_vector_f64(&self) -> Option<impl ExactSizeIterator<Item = f64> + Clone + '_> {
        self.as_vector_values().map(|values| {
            values.iter().map(|v| match v.value_type {
                Some(ValueType::DoubleValue(d)) => d,
                _ => unreachable!("valid vector values only contain DoubleValue"),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googletest::prelude::*;

    use super::*;

    #[gtest]
    fn valid_vectors_within_1_to_2048_dims_and_special_floats() {
        let one_d = Value::vector([42.5]);
        expect_true!(one_d.is_vector());
        expect_eq!(one_d.as_vector_values().map(<[Value]>::len), Some(1));
        expect_eq!(
            one_d.iter_vector_f64().map(Iterator::collect::<Vec<_>>),
            Some(vec![42.5])
        );

        let max_d = Value::vector((0..MAX_VECTOR_DIMENSIONS).map(|i| i as f64));
        expect_true!(max_d.is_vector());
        expect_eq!(
            max_d.iter_vector_f64().map(|it| it.len()),
            Some(MAX_VECTOR_DIMENSIONS)
        );

        let special = Value::vector([-0.0, f64::NEG_INFINITY, f64::INFINITY]);
        expect_true!(special.is_vector());
        let coords: Vec<f64> = special.iter_vector_f64().unwrap().collect();
        expect_eq!(coords[0].to_bits(), (-0.0_f64).to_bits());
        expect_eq!(coords[1], f64::NEG_INFINITY);
        expect_eq!(coords[2], f64::INFINITY);
    }

    #[gtest]
    fn non_vector_maps_and_none_value_type() {
        expect_false!(Value::default().is_vector());
        expect_that!(Value::default().as_vector_values(), none());
        expect_true!(Value::default().iter_vector_f64().is_none());

        let regular = Value::map(HashMap::from([("a".to_string(), Value::integer(1))]));
        expect_false!(regular.is_vector());
        expect_eq!(
            classify_vector_map(&HashMap::from([("a".to_string(), Value::integer(1))])),
            VectorClassification::NotVectorMap
        );

        let other_type =
            HashMap::from([(VECTOR_MAP_TYPE_KEY.to_string(), Value::string("__other__"))]);
        expect_eq!(
            classify_vector_map(&other_type),
            VectorClassification::NotVectorMap
        );

        let non_string_type = HashMap::from([(VECTOR_MAP_TYPE_KEY.to_string(), Value::integer(1))]);
        expect_eq!(
            classify_vector_map(&non_string_type),
            VectorClassification::NotVectorMap
        );
    }

    #[gtest]
    fn classifies_all_six_vector_format_errors_with_exact_messages() {
        let extraneous = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (
                VECTOR_MAP_VALUE_KEY.to_string(),
                Value::array(vec![Value::double(1.0)]),
            ),
            ("extra".to_string(), Value::integer(1)),
        ]);
        expect_eq!(
            classify_vector_map(&extraneous),
            VectorClassification::Invalid(VectorFormatError::ExtraneousEntries)
        );
        expect_eq!(
            VectorFormatError::ExtraneousEntries.as_error_message(),
            "Vector value map has extraneous entries."
        );
        expect_false!(Value::map(extraneous).is_vector());

        let missing_value = HashMap::from([(
            VECTOR_MAP_TYPE_KEY.to_string(),
            Value::string(VECTOR_MAP_TYPE_SENTINEL),
        )]);
        expect_eq!(
            classify_vector_map(&missing_value),
            VectorClassification::Invalid(VectorFormatError::MissingValueArray)
        );

        let non_array_value = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (VECTOR_MAP_VALUE_KEY.to_string(), Value::double(1.0)),
        ]);
        expect_eq!(
            classify_vector_map(&non_array_value),
            VectorClassification::Invalid(VectorFormatError::MissingValueArray)
        );
        expect_eq!(
            VectorFormatError::MissingValueArray.as_error_message(),
            "Vector value map is missing 'value' array."
        );

        let empty = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (VECTOR_MAP_VALUE_KEY.to_string(), Value::array(vec![])),
        ]);
        expect_eq!(
            classify_vector_map(&empty),
            VectorClassification::Invalid(VectorFormatError::EmptyVector)
        );
        expect_eq!(
            VectorFormatError::EmptyVector.as_error_message(),
            "Cannot have a zero length vector."
        );

        let too_many = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (
                VECTOR_MAP_VALUE_KEY.to_string(),
                Value::array(vec![Value::double(0.0); MAX_VECTOR_DIMENSIONS + 1]),
            ),
        ]);
        expect_eq!(
            classify_vector_map(&too_many),
            VectorClassification::Invalid(VectorFormatError::TooManyDimensions)
        );
        expect_eq!(
            VectorFormatError::TooManyDimensions.as_error_message(),
            "Vectors must be at most 2048 dimensions."
        );

        let non_double = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (
                VECTOR_MAP_VALUE_KEY.to_string(),
                Value::array(vec![Value::integer(1)]),
            ),
        ]);
        expect_eq!(
            classify_vector_map(&non_double),
            VectorClassification::Invalid(VectorFormatError::NonDoubleElement)
        );
        expect_eq!(
            VectorFormatError::NonDoubleElement.as_error_message(),
            "Vector must only contain values of type 'double'"
        );

        let nan_elem = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (
                VECTOR_MAP_VALUE_KEY.to_string(),
                Value::array(vec![Value::double(1.0), Value::double(f64::NAN)]),
            ),
        ]);
        expect_eq!(
            classify_vector_map(&nan_elem),
            VectorClassification::Invalid(VectorFormatError::NanElement)
        );
        expect_eq!(
            VectorFormatError::NanElement.as_error_message(),
            "Vector cannot contain NaN values."
        );
    }
}
