use std::collections::HashMap;

use googleapis::{
    VectorClassification, classify_vector_map,
    google::firestore::v1::{
        DocumentMask, Value, Write,
        document_transform::{FieldTransform, field_transform::TransformType},
        value::ValueType,
        write,
    },
};

use super::{field_path::FieldPath, reference};
use crate::{GenericDatabaseError, error::Result, unimplemented_collection, unimplemented_option};

pub(crate) fn validate_writable_fields(fields: &HashMap<String, Value>) -> Result<()> {
    for (key, value) in fields {
        if key.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "document field name cannot be empty",
            ));
        }
        if reference::is_reserved_name(key) {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "the name {key} is reserved"
            )));
        }
        validate_writable_value(value, false)?;
    }
    Ok(())
}

pub(crate) fn validate_writable_value(value: &Value, in_array: bool) -> Result<()> {
    match &value.value_type {
        None => Err(GenericDatabaseError::invalid_argument(
            "document field value is missing a value_type",
        )),
        Some(ValueType::FieldReferenceValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "fieldReferenceValue is not valid in stored documents",
        )),
        Some(ValueType::FunctionValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "functionValue is not valid in stored documents",
        )),
        Some(ValueType::PipelineValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "pipelineValue is not valid in stored documents",
        )),
        Some(ValueType::ArrayValue(arr)) => {
            if in_array {
                return Err(GenericDatabaseError::invalid_argument(
                    "nested arrays are not supported",
                ));
            }
            for item in &arr.values {
                validate_writable_value(item, true)?;
            }
            Ok(())
        }
        Some(ValueType::MapValue(map)) => match classify_vector_map(&map.fields) {
            VectorClassification::Valid(_) => Ok(()),
            VectorClassification::Invalid(err) => Err(GenericDatabaseError::invalid_argument(
                err.as_error_message(),
            )),
            VectorClassification::NotVectorMap => validate_writable_fields(&map.fields),
        },
        Some(
            ValueType::NullValue(_)
            | ValueType::BooleanValue(_)
            | ValueType::IntegerValue(_)
            | ValueType::DoubleValue(_)
            | ValueType::TimestampValue(_)
            | ValueType::StringValue(_)
            | ValueType::BytesValue(_)
            | ValueType::ReferenceValue(_)
            | ValueType::GeoPointValue(_),
        ) => Ok(()),
    }
}

pub(crate) fn validate_update_mask(mask: &DocumentMask) -> Result<()> {
    for raw_path in &mask.field_paths {
        let path: FieldPath = raw_path.parse()?;
        path.validate_non_reserved()?;
    }
    Ok(())
}

pub(crate) fn validate_field_transforms(transforms: &[FieldTransform]) -> Result<()> {
    for tf in transforms {
        let path: FieldPath = tf.field_path.parse()?;
        path.validate_non_reserved()?;
        let Some(transform_type) = &tf.transform_type else {
            return Err(GenericDatabaseError::invalid_argument(
                "empty transform_type in field_transform",
            ));
        };
        match transform_type {
            TransformType::SetToServerValue(_) => {}
            TransformType::AppendMissingElements(arr) | TransformType::RemoveAllFromArray(arr) => {
                for item in &arr.values {
                    validate_writable_value(item, true)?;
                }
            }
            TransformType::Increment(v) => {
                validate_writable_value(v, false)?;
                if !v.is_number() {
                    return Err(GenericDatabaseError::invalid_argument(
                        "increment value must be an integer or double",
                    ));
                }
            }
            TransformType::Maximum(v) | TransformType::Minimum(v) => {
                validate_writable_value(v, false)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_write(write: &Write) -> Result<()> {
    let Some(operation) = &write.operation else {
        return Err(GenericDatabaseError::not_implemented(
            "missing operation in write",
        ));
    };
    match operation {
        write::Operation::Update(doc) => {
            validate_writable_fields(&doc.fields)?;
            if let Some(mask) = &write.update_mask {
                validate_update_mask(mask)?;
            }
            validate_field_transforms(&write.update_transforms)?;
            Ok(())
        }
        write::Operation::Delete(_) => {
            unimplemented_option!(write.update_mask);
            unimplemented_collection!(write.update_transforms);
            Ok(())
        }
        write::Operation::Transform(_) => crate::unimplemented!("transform"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::{
        MAX_VECTOR_DIMENSIONS, VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY,
        google::firestore::v1::{
            ArrayValue, Document, DocumentTransform, Function, Pipeline,
            document_transform::field_transform::ServerValue,
        },
    };
    use googletest::prelude::*;

    use super::*;

    fn vector_map(fields: HashMap<String, Value>) -> Value {
        Value::map(fields)
    }

    #[gtest]
    fn accepts_valid_vectors_special_floats_arrays_and_four_underscores() {
        let fields = HashMap::from([
            ("____".to_string(), Value::integer(1)),
            (
                "emb".to_string(),
                Value::vector([-0.0, f64::NEG_INFINITY, f64::INFINITY]),
            ),
            (
                "arr_of_vecs".to_string(),
                Value::array(vec![Value::vector([1.0]), Value::vector([2.0, 3.0])]),
            ),
        ]);
        assert_that!(validate_writable_fields(&fields), ok(()));
    }

    #[gtest]
    fn rejects_all_invalid_vector_formats_in_nested_maps() {
        let invalid_cases = [
            (
                HashMap::from([
                    (
                        VECTOR_MAP_TYPE_KEY.to_string(),
                        Value::string(VECTOR_MAP_TYPE_SENTINEL),
                    ),
                    (
                        VECTOR_MAP_VALUE_KEY.to_string(),
                        Value::array(vec![Value::double(1.0)]),
                    ),
                    ("extra".to_string(), Value::integer(1)),
                ]),
                "Vector value map has extraneous entries.",
            ),
            (
                HashMap::from([(
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                )]),
                "Vector value map is missing 'value' array.",
            ),
            (
                HashMap::from([
                    (
                        VECTOR_MAP_TYPE_KEY.to_string(),
                        Value::string(VECTOR_MAP_TYPE_SENTINEL),
                    ),
                    (VECTOR_MAP_VALUE_KEY.to_string(), Value::array(vec![])),
                ]),
                "Cannot have a zero length vector.",
            ),
            (
                HashMap::from([
                    (
                        VECTOR_MAP_TYPE_KEY.to_string(),
                        Value::string(VECTOR_MAP_TYPE_SENTINEL),
                    ),
                    (
                        VECTOR_MAP_VALUE_KEY.to_string(),
                        Value::array(vec![Value::double(1.0); MAX_VECTOR_DIMENSIONS + 1]),
                    ),
                ]),
                "Vectors must be at most 2048 dimensions.",
            ),
            (
                HashMap::from([
                    (
                        VECTOR_MAP_TYPE_KEY.to_string(),
                        Value::string(VECTOR_MAP_TYPE_SENTINEL),
                    ),
                    (
                        VECTOR_MAP_VALUE_KEY.to_string(),
                        Value::array(vec![Value::integer(1)]),
                    ),
                ]),
                "Vector must only contain values of type 'double'",
            ),
            (
                HashMap::from([
                    (
                        VECTOR_MAP_TYPE_KEY.to_string(),
                        Value::string(VECTOR_MAP_TYPE_SENTINEL),
                    ),
                    (
                        VECTOR_MAP_VALUE_KEY.to_string(),
                        Value::array(vec![Value::double(f64::NAN)]),
                    ),
                ]),
                "Vector cannot contain NaN values.",
            ),
        ];

        for (map_fields, expected_msg) in invalid_cases {
            let doc_fields = HashMap::from([("emb".to_string(), vector_map(map_fields))]);
            assert_that!(
                validate_writable_fields(&doc_fields),
                err(eq(&GenericDatabaseError::invalid_argument(expected_msg)))
            );
        }
    }

    #[gtest]
    fn rejects_top_level_reserved_empty_keys_nested_arrays_none_and_query_variants() {
        let top_level_vector_sentinel = HashMap::from([
            (
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            ),
            (
                VECTOR_MAP_VALUE_KEY.to_string(),
                Value::array(vec![Value::double(1.0)]),
            ),
        ]);
        assert_that!(
            validate_writable_fields(&top_level_vector_sentinel),
            err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
        );

        let empty_key = HashMap::from([(String::new(), Value::integer(1))]);
        assert_that!(
            validate_writable_fields(&empty_key),
            err(eq(&GenericDatabaseError::invalid_argument(
                "document field name cannot be empty"
            )))
        );

        let reserved_key = HashMap::from([("__custom__".to_string(), Value::integer(1))]);
        assert_that!(
            validate_writable_fields(&reserved_key),
            err(eq(&GenericDatabaseError::invalid_argument(
                "the name __custom__ is reserved"
            )))
        );

        let nested_array =
            HashMap::from([("a".to_string(), Value::array(vec![Value::array(vec![])]))]);
        assert_that!(
            validate_writable_fields(&nested_array),
            err(eq(&GenericDatabaseError::invalid_argument(
                "nested arrays are not supported"
            )))
        );

        let missing_vt = HashMap::from([("a".to_string(), Value::default())]);
        assert_that!(
            validate_writable_fields(&missing_vt),
            err(eq(&GenericDatabaseError::invalid_argument(
                "document field value is missing a value_type"
            )))
        );

        let field_ref_val = HashMap::from([(
            "a".to_string(),
            Value {
                value_type: Some(ValueType::FieldReferenceValue("x".to_string())),
            },
        )]);
        assert_that!(
            validate_writable_fields(&field_ref_val),
            err(eq(&GenericDatabaseError::invalid_argument(
                "fieldReferenceValue is not valid in stored documents"
            )))
        );

        let fn_val = HashMap::from([(
            "a".to_string(),
            Value {
                value_type: Some(ValueType::FunctionValue(Function::default())),
            },
        )]);
        assert_that!(
            validate_writable_fields(&fn_val),
            err(eq(&GenericDatabaseError::invalid_argument(
                "functionValue is not valid in stored documents"
            )))
        );

        let pipe_val = HashMap::from([(
            "a".to_string(),
            Value {
                value_type: Some(ValueType::PipelineValue(Pipeline::default())),
            },
        )]);
        assert_that!(
            validate_writable_fields(&pipe_val),
            err(eq(&GenericDatabaseError::invalid_argument(
                "pipelineValue is not valid in stored documents"
            )))
        );
    }

    #[gtest]
    fn validates_update_mask_and_field_transforms() {
        for bad_path in ["__name__", "`__name__`", "emb.__type__", "a..b"] {
            assert_that!(
                validate_update_mask(&DocumentMask {
                    field_paths: vec![bad_path.to_string()],
                }),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
            assert_that!(
                validate_field_transforms(&[FieldTransform {
                    field_path:     bad_path.to_string(),
                    transform_type: Some(TransformType::SetToServerValue(
                        ServerValue::RequestTime as i32
                    )),
                }]),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }

        assert_that!(
            validate_field_transforms(&[FieldTransform {
                field_path:     "ts".to_string(),
                transform_type: None,
            }]),
            err(eq(&GenericDatabaseError::invalid_argument(
                "empty transform_type in field_transform"
            )))
        );

        let invalid_vec = Value::map(HashMap::from([(
            VECTOR_MAP_TYPE_KEY.to_string(),
            Value::string(VECTOR_MAP_TYPE_SENTINEL),
        )]));
        for tf_type in [
            TransformType::AppendMissingElements(ArrayValue {
                values: vec![invalid_vec.clone()],
            }),
            TransformType::RemoveAllFromArray(ArrayValue {
                values: vec![invalid_vec.clone()],
            }),
            TransformType::Increment(invalid_vec.clone()),
            TransformType::Maximum(invalid_vec.clone()),
            TransformType::Minimum(invalid_vec),
        ] {
            assert_that!(
                validate_field_transforms(&[FieldTransform {
                    field_path:     "f".to_string(),
                    transform_type: Some(tf_type),
                }]),
                err(eq(&GenericDatabaseError::invalid_argument(
                    "Vector value map is missing 'value' array."
                )))
            );
        }

        // Valid SetToServerValue, Maximum, Minimum with valid operands succeed validation.
        assert_that!(
            validate_field_transforms(&[
                FieldTransform {
                    field_path:     "ts".to_string(),
                    transform_type: Some(TransformType::SetToServerValue(
                        ServerValue::RequestTime as i32
                    )),
                },
                FieldTransform {
                    field_path:     "hi".to_string(),
                    transform_type: Some(TransformType::Maximum(Value::integer(1))),
                },
                FieldTransform {
                    field_path:     "lo".to_string(),
                    transform_type: Some(TransformType::Minimum(Value::integer(0))),
                },
            ]),
            ok(())
        );

        // Non-numeric Increment operands are rejected with InvalidArgument.
        for non_numeric in [
            Value::string("abc"),
            Value::boolean(true),
            Value::null(),
            Value::array(vec![Value::integer(1)]),
            Value::map(HashMap::from([("k".to_string(), Value::integer(1))])),
        ] {
            assert_that!(
                validate_field_transforms(&[FieldTransform {
                    field_path:     "cnt".to_string(),
                    transform_type: Some(TransformType::Increment(non_numeric)),
                }]),
                err(eq(&GenericDatabaseError::invalid_argument(
                    "increment value must be an integer or double"
                )))
            );
        }

        // Integer and double (including NaN/Infinity) Increment operands remain valid.
        for numeric in [
            Value::integer(1),
            Value::double(1.5),
            Value::double(f64::NAN),
            Value::double(f64::INFINITY),
        ] {
            assert_that!(
                validate_field_transforms(&[FieldTransform {
                    field_path:     "cnt".to_string(),
                    transform_type: Some(TransformType::Increment(numeric)),
                }]),
                ok(())
            );
        }
    }

    #[gtest]
    fn validate_write_preserves_none_delete_and_transform_errors() {
        assert_that!(
            validate_write(&Write::default()),
            err(eq(&GenericDatabaseError::not_implemented(
                "missing operation in write"
            )))
        );

        assert_that!(
            validate_write(&Write {
                operation: Some(write::Operation::Delete(
                    "projects/p/databases/(default)/documents/c/d".to_string()
                )),
                ..Default::default()
            }),
            ok(())
        );

        assert_that!(
            validate_write(&Write {
                update_mask: Some(DocumentMask {
                    field_paths: vec!["a".to_string()],
                }),
                operation: Some(write::Operation::Delete(
                    "projects/p/databases/(default)/documents/c/d".to_string()
                )),
                ..Default::default()
            }),
            err(matches_pattern!(GenericDatabaseError::NotImplemented(_)))
        );

        assert_that!(
            validate_write(&Write {
                update_transforms: vec![FieldTransform::default()],
                operation: Some(write::Operation::Delete(
                    "projects/p/databases/(default)/documents/c/d".to_string()
                )),
                ..Default::default()
            }),
            err(matches_pattern!(GenericDatabaseError::NotImplemented(_)))
        );

        assert_that!(
            validate_write(&Write {
                operation: Some(write::Operation::Transform(DocumentTransform::default())),
                ..Default::default()
            }),
            err(matches_pattern!(GenericDatabaseError::NotImplemented(_)))
        );

        assert_that!(
            validate_write(&Write {
                operation: Some(write::Operation::Update(Document {
                    name: "projects/p/databases/(default)/documents/c/d".to_string(),
                    fields: HashMap::from([("emb".to_string(), Value::vector([1.0, 2.0]))]),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ok(())
        );
    }
}
