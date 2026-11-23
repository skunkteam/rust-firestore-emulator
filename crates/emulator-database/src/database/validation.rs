use std::collections::HashMap;

use googleapis::{
    VectorClassification, classify_vector_map,
    google::{
        firestore::v1::{
            DocumentMask, Precondition, Value, Write,
            document_transform::{
                FieldTransform,
                field_transform::{ServerValue, TransformType},
            },
            precondition,
            value::ValueType,
            write,
        },
        protobuf::Timestamp,
        r#type::LatLng,
    },
};

use super::{
    document::DocumentPrecondition,
    field_path::FieldPath,
    reference::{self, DocumentRef, RootRef},
};
use crate::{GenericDatabaseError, error::Result, unimplemented_collection, unimplemented_option};

#[derive(Debug)]
pub(crate) struct ValidatedWrite {
    pub(crate) doc_ref:      DocumentRef,
    pub(crate) precondition: Option<DocumentPrecondition>,
    pub(crate) operation:    ValidatedWriteOperation,
}

#[derive(Debug)]
pub(crate) enum ValidatedWriteOperation {
    Update {
        fields: HashMap<String, Value>,
        update_mask: Option<Vec<FieldPath>>,
        update_transforms: Vec<ValidatedFieldTransform>,
    },
    Delete,
}

#[derive(Debug)]
pub(crate) struct ValidatedFieldTransform {
    pub(crate) field_path: FieldPath,
    pub(crate) op: ValidatedTransformOp,
}

#[derive(Debug)]
pub(crate) enum ValidatedTransformOp {
    SetToRequestTime,
    Increment(Value),
    AppendMissingElements(Vec<Value>),
    RemoveAllFromArray(Vec<Value>),
    Maximum,
    Minimum,
}

impl ValidatedWrite {
    pub(crate) fn parse(write: Write, expected_root: &RootRef) -> Result<Self> {
        let Some(operation) = write.operation else {
            return Err(GenericDatabaseError::not_implemented(
                "missing operation in write",
            ));
        };

        let raw_name = match &operation {
            write::Operation::Update(doc) => &doc.name,
            write::Operation::Delete(name) => name,
            write::Operation::Transform(trans) => &trans.document,
        };
        let doc_ref: DocumentRef = raw_name.parse()?;

        if &doc_ref.collection_ref.root_ref != expected_root {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "resource root {} does not match database {expected_root}",
                doc_ref.collection_ref.root_ref
            )));
        }

        if let Some(precondition) = &write.current_document {
            validate_precondition(precondition)?;
        }
        let precondition = write
            .current_document
            .and_then(|cd| cd.condition_type)
            .map(Into::into);

        let operation = match operation {
            write::Operation::Update(doc) => {
                validate_writable_fields(&doc.fields)?;
                let update_mask = write.update_mask.map(parse_update_mask).transpose()?;
                let update_transforms = parse_field_transforms(write.update_transforms)?;
                ValidatedWriteOperation::Update {
                    fields: doc.fields,
                    update_mask,
                    update_transforms,
                }
            }
            write::Operation::Delete(_) => {
                unimplemented_option!(write.update_mask);
                unimplemented_collection!(write.update_transforms);
                ValidatedWriteOperation::Delete
            }
            write::Operation::Transform(_) => crate::unimplemented!("transform"),
        };

        Ok(Self {
            doc_ref,
            precondition,
            operation,
        })
    }
}

pub(crate) fn validate_geo_point(geo: &LatLng) -> Result<()> {
    if !geo.latitude.is_finite() || !(-90.0..=90.0).contains(&geo.latitude) {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "latitude must be in the range [-90, 90], got {}",
            geo.latitude
        )));
    }
    if !geo.longitude.is_finite() || !(-180.0..=180.0).contains(&geo.longitude) {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "longitude must be in the range [-180, 180], got {}",
            geo.longitude
        )));
    }
    Ok(())
}

pub(crate) fn validate_timestamp(ts: &Timestamp) -> Result<()> {
    const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800; // 0001-01-01T00:00:00Z
    const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799; // 9999-12-31T23:59:59Z
    if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&ts.seconds) {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "Timestamp seconds {} out of range [{MIN_TIMESTAMP_SECONDS}, {MAX_TIMESTAMP_SECONDS}]",
            ts.seconds
        )));
    }
    if !(0..1_000_000_000).contains(&ts.nanos) {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "Timestamp nanos {} out of range [0, 999999999]",
            ts.nanos
        )));
    }
    Ok(())
}

/// Validates all field names and values in a document map before storage.
///
/// # Invariant (Compositional Checks Only)
/// Every check performed here and in [`validate_writable_value`] is purely compositional over
/// individual keys and values—validity of a merged map follows from validity of the existing stored
/// fields, the incoming update fields, and non-reserved [`FieldPath`] segments. Consequently,
/// `stage_write` skips re-validating the merged document when `update_transforms` is empty (guarded
/// by a `debug_assert!` in debug/test builds).
///
/// **WARNING**: If you add any non-compositional document-wide invariant here (such as Cloud
/// Firestore's 1 MiB total document size limit or 20-level nested map depth limit, which can be
/// violated by combining valid existing fields with valid masked updates), you MUST restore
/// unconditional post-merge validation in `commit.rs::FirestoreDatabase::stage_write`.
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
        Some(ValueType::VariableReferenceValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "variableReferenceValue is not valid in stored documents",
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
        Some(ValueType::GeoPointValue(geo)) => validate_geo_point(geo),
        Some(
            ValueType::NullValue(_)
            | ValueType::BooleanValue(_)
            | ValueType::IntegerValue(_)
            | ValueType::DoubleValue(_)
            | ValueType::TimestampValue(_)
            | ValueType::StringValue(_)
            | ValueType::BytesValue(_)
            | ValueType::ReferenceValue(_),
        ) => Ok(()),
    }
}

fn parse_update_mask(mask: DocumentMask) -> Result<Vec<FieldPath>> {
    mask.field_paths
        .into_iter()
        .map(|raw_path| {
            let path: FieldPath = raw_path.parse()?;
            path.validate_non_reserved()?;
            Ok(path)
        })
        .collect()
}

fn parse_field_transforms(transforms: Vec<FieldTransform>) -> Result<Vec<ValidatedFieldTransform>> {
    let mut parsed = Vec::with_capacity(transforms.len());
    for tf in transforms {
        let field_path: FieldPath = tf.field_path.parse()?;
        field_path.validate_non_reserved()?;
        let Some(transform_type) = tf.transform_type else {
            return Err(GenericDatabaseError::invalid_argument(
                "empty transform_type in field_transform",
            ));
        };
        let op = match transform_type {
            TransformType::SetToServerValue(code) => match ServerValue::try_from(code) {
                Ok(ServerValue::RequestTime) => ValidatedTransformOp::SetToRequestTime,
                Ok(ServerValue::Unspecified) => {
                    return Err(GenericDatabaseError::invalid_argument(
                        "ServerValue type must be specified for set_to_server_value \
                         transformation.",
                    ));
                }
                Err(_) => {
                    return Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid server_value: {code}"
                    )));
                }
            },
            TransformType::AppendMissingElements(arr) => {
                for item in &arr.values {
                    validate_writable_value(item, true)?;
                }
                ValidatedTransformOp::AppendMissingElements(arr.values)
            }
            TransformType::RemoveAllFromArray(arr) => {
                for item in &arr.values {
                    validate_writable_value(item, true)?;
                }
                ValidatedTransformOp::RemoveAllFromArray(arr.values)
            }
            TransformType::Increment(v) => {
                validate_writable_value(&v, false)?;
                if !v.is_number() {
                    return Err(GenericDatabaseError::invalid_argument(
                        "increment value must be an integer or double",
                    ));
                }
                ValidatedTransformOp::Increment(v)
            }
            TransformType::Maximum(v) => {
                validate_writable_value(&v, false)?;
                ValidatedTransformOp::Maximum
            }
            TransformType::Minimum(v) => {
                validate_writable_value(&v, false)?;
                ValidatedTransformOp::Minimum
            }
        };
        parsed.push(ValidatedFieldTransform { field_path, op });
    }
    Ok(parsed)
}

pub(crate) fn validate_precondition(precondition: &Precondition) -> Result<()> {
    if let Some(precondition::ConditionType::UpdateTime(time)) = precondition.condition_type {
        if time.seconds < 0 {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "Only timestamps past epoch are supported, got seconds={}",
                time.seconds
            )));
        }
        if !(0..1_000_000_000).contains(&time.nanos) {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "invalid timestamp: nanos ({}) must be in [0, 999_999_999]",
                time.nanos
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::{
        MAX_VECTOR_DIMENSIONS, VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY,
        google::{
            firestore::v1::{ArrayValue, Document, DocumentTransform, Function, Pipeline},
            protobuf::Timestamp,
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

        let var_ref_val = HashMap::from([(
            "a".to_string(),
            Value {
                value_type: Some(ValueType::VariableReferenceValue("x".to_string())),
            },
        )]);
        assert_that!(
            validate_writable_fields(&var_ref_val),
            err(eq(&GenericDatabaseError::invalid_argument(
                "variableReferenceValue is not valid in stored documents"
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

    fn test_root() -> RootRef {
        "projects/p/databases/(default)".parse().unwrap()
    }

    #[gtest]
    fn parses_update_mask_and_field_transforms() {
        for bad_path in ["__name__", "`__name__`", "emb.__type__", "a..b"] {
            assert_that!(
                parse_update_mask(DocumentMask {
                    field_paths: vec![bad_path.to_string()],
                }),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
            assert_that!(
                parse_field_transforms(vec![FieldTransform {
                    field_path:     bad_path.to_string(),
                    transform_type: Some(TransformType::SetToServerValue(
                        ServerValue::RequestTime as i32
                    )),
                }]),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }

        assert_that!(
            parse_field_transforms(vec![FieldTransform {
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
                parse_field_transforms(vec![FieldTransform {
                    field_path:     "f".to_string(),
                    transform_type: Some(tf_type),
                }]),
                err(eq(&GenericDatabaseError::invalid_argument(
                    "Vector value map is missing 'value' array."
                )))
            );
        }

        // Valid SetToServerValue, Maximum, Minimum with valid operands succeed pre-lock parsing.
        let parsed = parse_field_transforms(vec![
            FieldTransform {
                field_path:     "ts".to_string(),
                transform_type: Some(TransformType::SetToServerValue(
                    ServerValue::RequestTime as i32,
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
        ])
        .unwrap();
        assert!(matches!(
            parsed.as_slice(),
            [
                ValidatedFieldTransform {
                    op: ValidatedTransformOp::SetToRequestTime,
                    ..
                },
                ValidatedFieldTransform {
                    op: ValidatedTransformOp::Maximum,
                    ..
                },
                ValidatedFieldTransform {
                    op: ValidatedTransformOp::Minimum,
                    ..
                },
            ]
        ));

        // Non-numeric Increment operands are rejected with InvalidArgument.
        for non_numeric in [
            Value::string("abc"),
            Value::boolean(true),
            Value::null(),
            Value::array(vec![Value::integer(1)]),
            Value::map(HashMap::from([("k".to_string(), Value::integer(1))])),
        ] {
            assert_that!(
                parse_field_transforms(vec![FieldTransform {
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
                parse_field_transforms(vec![FieldTransform {
                    field_path:     "cnt".to_string(),
                    transform_type: Some(TransformType::Increment(numeric)),
                }]),
                ok(anything())
            );
        }
    }

    #[gtest]
    fn validated_write_parse_preserves_none_delete_and_transform_errors() {
        let root = test_root();
        let valid_doc = "projects/p/databases/(default)/documents/c/d".to_string();

        assert_that!(
            ValidatedWrite::parse(Write::default(), &root),
            err(eq(&GenericDatabaseError::not_implemented(
                "missing operation in write"
            )))
        );

        let deleted = ValidatedWrite::parse(
            Write {
                operation: Some(write::Operation::Delete(valid_doc.clone())),
                ..Default::default()
            },
            &root,
        )
        .unwrap();
        assert!(matches!(deleted.operation, ValidatedWriteOperation::Delete));

        assert_that!(
            ValidatedWrite::parse(
                Write {
                    update_mask: Some(DocumentMask {
                        field_paths: vec!["a".to_string()],
                    }),
                    operation: Some(write::Operation::Delete(valid_doc.clone())),
                    ..Default::default()
                },
                &root,
            ),
            err(matches_pattern!(GenericDatabaseError::NotImplemented(_)))
        );

        assert_that!(
            ValidatedWrite::parse(
                Write {
                    update_transforms: vec![FieldTransform::default()],
                    operation: Some(write::Operation::Delete(valid_doc.clone())),
                    ..Default::default()
                },
                &root,
            ),
            err(matches_pattern!(GenericDatabaseError::NotImplemented(_)))
        );

        // `DocumentTransform::default()` has an empty `document` path, so step (b)
        // `DocumentRef::from_str` triggers `InvalidReference` before step (e) `NotImplemented`;
        // a valid owned `DocumentTransform` path triggers `NotImplemented`.
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    operation: Some(write::Operation::Transform(DocumentTransform::default())),
                    ..Default::default()
                },
                &root,
            ),
            err(matches_pattern!(GenericDatabaseError::InvalidReference(
                _,
                _
            )))
        );
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    operation: Some(write::Operation::Transform(DocumentTransform {
                        document: valid_doc.clone(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                &root,
            ),
            err(matches_pattern!(GenericDatabaseError::NotImplemented(_)))
        );

        let updated = ValidatedWrite::parse(
            Write {
                operation: Some(write::Operation::Update(Document {
                    name: valid_doc,
                    fields: HashMap::from([("emb".to_string(), Value::vector([1.0, 2.0]))]),
                    ..Default::default()
                })),
                ..Default::default()
            },
            &root,
        )
        .unwrap();
        assert!(matches!(
            updated.operation,
            ValidatedWriteOperation::Update { .. }
        ));
    }

    #[gtest]
    fn validated_write_parse_preserves_five_step_validation_precedence() {
        let root = test_root();
        let valid_doc = "projects/p/databases/(default)/documents/c/d".to_string();
        let foreign_doc = "projects/other/databases/(default)/documents/c/d".to_string();
        let bad_precondition = Precondition {
            condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                seconds: -1,
                nanos:   0,
            })),
        };
        let bad_fields = HashMap::from([("__bad__".to_string(), Value::integer(1))]);
        let bad_mask = DocumentMask {
            field_paths: vec!["__name__".to_string()],
        };
        let bad_transforms = vec![FieldTransform {
            field_path:     "cnt".to_string(),
            transform_type: Some(TransformType::Increment(Value::string("bad"))),
        }];

        // Step (a): `operation: None` precedes invalid precondition/mask/transforms.
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    current_document: Some(bad_precondition),
                    update_mask: Some(bad_mask.clone()),
                    update_transforms: bad_transforms.clone(),
                    operation: None,
                },
                &root,
            ),
            err(eq(&GenericDatabaseError::not_implemented(
                "missing operation in write"
            )))
        );

        // Step (b): Invalid `DocumentRef` precedes ownership, precondition, and op checks.
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    current_document: Some(bad_precondition),
                    update_mask: Some(bad_mask.clone()),
                    update_transforms: bad_transforms.clone(),
                    operation: Some(write::Operation::Update(Document {
                        name: "projects/p/databases/(default)/documents/only_col".to_string(),
                        fields: bad_fields.clone(),
                        ..Default::default()
                    })),
                },
                &root,
            ),
            err(matches_pattern!(GenericDatabaseError::InvalidReference(
                _,
                _
            )))
        );

        // Step (c): `ensure_owns_doc` precedes precondition and op-specific validation.
        let ownership_err = ValidatedWrite::parse(
            Write {
                current_document: Some(bad_precondition),
                update_mask: Some(bad_mask.clone()),
                update_transforms: bad_transforms.clone(),
                operation: Some(write::Operation::Update(Document {
                    name: foreign_doc,
                    fields: bad_fields.clone(),
                    ..Default::default()
                })),
            },
            &root,
        )
        .unwrap_err();
        assert!(
            matches!(ownership_err, GenericDatabaseError::InvalidArgument(ref m) if m.contains("does not match database")),
            "expected root ownership error, got {ownership_err:?}"
        );

        // Step (d): `validate_precondition` precedes op-specific checks (Update, Delete,
        // Transform).
        for bad_pre_write in [
            Write {
                current_document: Some(bad_precondition),
                update_mask: Some(bad_mask.clone()),
                update_transforms: bad_transforms.clone(),
                operation: Some(write::Operation::Update(Document {
                    name: valid_doc.clone(),
                    fields: bad_fields.clone(),
                    ..Default::default()
                })),
            },
            Write {
                current_document: Some(bad_precondition),
                update_mask: Some(DocumentMask::default()),
                update_transforms: bad_transforms.clone(),
                operation: Some(write::Operation::Delete(valid_doc.clone())),
            },
            Write {
                current_document: Some(bad_precondition),
                operation: Some(write::Operation::Transform(DocumentTransform {
                    document: valid_doc.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        ] {
            let pre_err = ValidatedWrite::parse(bad_pre_write, &root).unwrap_err();
            assert!(
                matches!(pre_err, GenericDatabaseError::InvalidArgument(ref m) if m.contains("epoch")),
                "expected precondition timestamp error, got {pre_err:?}"
            );
        }

        // Step (e) Update: `fields` -> `update_mask` -> `update_transforms`.
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    update_mask: Some(bad_mask.clone()),
                    update_transforms: bad_transforms.clone(),
                    operation: Some(write::Operation::Update(Document {
                        name: valid_doc.clone(),
                        fields: bad_fields,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                &root,
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "the name __bad__ is reserved"
            )))
        );
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    update_mask: Some(bad_mask),
                    update_transforms: bad_transforms.clone(),
                    operation: Some(write::Operation::Update(Document {
                        name: valid_doc.clone(),
                        fields: HashMap::from([("ok".to_string(), Value::integer(1))]),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                &root,
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "the name __name__ is reserved"
            )))
        );
        assert_that!(
            ValidatedWrite::parse(
                Write {
                    update_mask: Some(DocumentMask {
                        field_paths: vec!["ok".to_string()],
                    }),
                    update_transforms: bad_transforms,
                    operation: Some(write::Operation::Update(Document {
                        name: valid_doc.clone(),
                        fields: HashMap::from([("ok".to_string(), Value::integer(1))]),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                &root,
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "increment value must be an integer or double"
            )))
        );

        // Step (e) Delete: `update_mask` precedes `update_transforms`.
        let del_mask_err = ValidatedWrite::parse(
            Write {
                update_mask: Some(DocumentMask::default()),
                update_transforms: vec![FieldTransform::default()],
                operation: Some(write::Operation::Delete(valid_doc.clone())),
                ..Default::default()
            },
            &root,
        )
        .unwrap_err();
        assert!(
            matches!(del_mask_err, GenericDatabaseError::NotImplemented(ref m) if m.contains("update_mask")),
            "expected update_mask error before update_transforms, got {del_mask_err:?}"
        );
        let del_tf_err = ValidatedWrite::parse(
            Write {
                update_mask: None,
                update_transforms: vec![FieldTransform::default()],
                operation: Some(write::Operation::Delete(valid_doc)),
                ..Default::default()
            },
            &root,
        )
        .unwrap_err();
        assert!(
            matches!(del_tf_err, GenericDatabaseError::NotImplemented(ref m) if m.contains("update_transforms")),
            "expected update_transforms error, got {del_tf_err:?}"
        );
    }

    #[gtest]
    fn validated_write_distinguishes_empty_mask_from_none_and_prevalidates_max_min() {
        let root = test_root();
        let valid_doc = "projects/p/databases/(default)/documents/c/d".to_string();

        // `update_mask: Some([])` MUST remain `Some(vec![])`, distinct from `None`.
        let empty_mask_write = ValidatedWrite::parse(
            Write {
                update_mask: Some(DocumentMask {
                    field_paths: vec![],
                }),
                update_transforms: vec![FieldTransform {
                    field_path:     "ts".to_string(),
                    transform_type: Some(TransformType::SetToServerValue(
                        ServerValue::RequestTime as i32,
                    )),
                }],
                operation: Some(write::Operation::Update(Document {
                    name: valid_doc.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            &root,
        )
        .unwrap();
        assert!(matches!(
            empty_mask_write.operation,
            ValidatedWriteOperation::Update {
                update_mask: Some(ref mask),
                ..
            } if mask.is_empty()
        ));

        let no_mask_write = ValidatedWrite::parse(
            Write {
                update_mask: None,
                update_transforms: vec![FieldTransform {
                    field_path:     "ts".to_string(),
                    transform_type: Some(TransformType::SetToServerValue(
                        ServerValue::RequestTime as i32,
                    )),
                }],
                operation: Some(write::Operation::Update(Document {
                    name: valid_doc.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            &root,
        )
        .unwrap();
        assert!(matches!(
            no_mask_write.operation,
            ValidatedWriteOperation::Update {
                update_mask: None,
                ..
            }
        ));

        // Overlapping / repeated mask paths preserve exact order and multiplicity.
        let repeated_mask_write = ValidatedWrite::parse(
            Write {
                update_mask: Some(DocumentMask {
                    field_paths: vec!["a".to_string(), "a.b".to_string(), "a".to_string()],
                }),
                operation: Some(write::Operation::Update(Document {
                    name: valid_doc.clone(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            &root,
        )
        .unwrap();
        assert_eq!(
            match repeated_mask_write.operation {
                ValidatedWriteOperation::Update {
                    update_mask: Some(mask),
                    ..
                } => mask,
                _ => panic!("expected Update with mask"),
            },
            vec![
                "a".parse::<FieldPath>().unwrap(),
                "a.b".parse::<FieldPath>().unwrap(),
                "a".parse::<FieldPath>().unwrap(),
            ]
        );

        // `Maximum` / `Minimum` with invalid operand fail pre-lock with `InvalidArgument`,
        // while valid operands parse to `ValidatedTransformOp::{Maximum, Minimum}`.
        for bad_tf in [
            TransformType::Maximum(Value::default()),
            TransformType::Minimum(Value::default()),
        ] {
            assert_that!(
                ValidatedWrite::parse(
                    Write {
                        update_transforms: vec![FieldTransform {
                            field_path:     "m".to_string(),
                            transform_type: Some(bad_tf),
                        }],
                        operation: Some(write::Operation::Update(Document {
                            name: valid_doc.clone(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    &root,
                ),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }

        let valid_max_min = ValidatedWrite::parse(
            Write {
                update_transforms: vec![
                    FieldTransform {
                        field_path:     "hi".to_string(),
                        transform_type: Some(TransformType::Maximum(Value::integer(10))),
                    },
                    FieldTransform {
                        field_path:     "lo".to_string(),
                        transform_type: Some(TransformType::Minimum(Value::integer(1))),
                    },
                ],
                operation: Some(write::Operation::Update(Document {
                    name: valid_doc,
                    ..Default::default()
                })),
                ..Default::default()
            },
            &root,
        )
        .unwrap();
        assert!(matches!(
            valid_max_min.operation,
            ValidatedWriteOperation::Update {
                ref update_transforms,
                ..
            } if matches!(
                update_transforms.as_slice(),
                [
                    ValidatedFieldTransform { op: ValidatedTransformOp::Maximum, .. },
                    ValidatedFieldTransform { op: ValidatedTransformOp::Minimum, .. },
                ]
            )
        ));
    }

    #[gtest]
    fn rejects_out_of_range_or_non_finite_geopoint_values() {
        use googleapis::google::r#type::LatLng;

        for (latitude, longitude) in [
            (90.0001, 0.0),
            (-90.0001, 0.0),
            (0.0, 180.0001),
            (0.0, -180.0001),
            (f64::NAN, 0.0),
            (0.0, f64::INFINITY),
        ] {
            let fields = HashMap::from([(
                "loc".to_string(),
                Value::from(LatLng {
                    latitude,
                    longitude,
                }),
            )]);
            assert_that!(
                validate_writable_fields(&fields),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }

    #[gtest]
    fn rejects_unspecified_or_unknown_server_value_and_invalid_precondition_timestamps() {
        let root = test_root();
        for bad_sv in [ServerValue::Unspecified as i32, 999] {
            assert_that!(
                parse_field_transforms(vec![FieldTransform {
                    field_path:     "ts".to_string(),
                    transform_type: Some(TransformType::SetToServerValue(bad_sv)),
                }]),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }

        // Empty precondition and epoch timestamp pass validation.
        assert_that!(validate_precondition(&Precondition::default()), ok(()));
        assert_that!(
            validate_precondition(&Precondition {
                condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                    seconds: 0,
                    nanos:   0,
                })),
            }),
            ok(())
        );

        // Negative seconds and out-of-range nanos fail validation with InvalidArgument.
        for bad_ts in [
            Timestamp {
                seconds: -1,
                nanos:   0,
            },
            Timestamp {
                seconds: 10,
                nanos:   -1,
            },
            Timestamp {
                seconds: 10,
                nanos:   1_000_000_000,
            },
        ] {
            let pre = Precondition {
                condition_type: Some(precondition::ConditionType::UpdateTime(bad_ts)),
            };
            assert_that!(
                validate_precondition(&pre),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
            assert_that!(
                ValidatedWrite::parse(
                    Write {
                        current_document: Some(pre),
                        operation: Some(write::Operation::Delete(
                            "projects/p/databases/(default)/documents/c/d".to_string(),
                        )),
                        ..Default::default()
                    },
                    &root,
                ),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }
}
