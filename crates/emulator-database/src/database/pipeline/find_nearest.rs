use std::str::FromStr;

use googleapis::{
    cmp_reference_paths,
    google::firestore::v1::{Document, Value, pipeline::Stage, value::ValueType},
};
use itertools::Itertools;

use crate::{
    GenericDatabaseError,
    database::{
        field_path::FieldReference,
        vector::{self, DistanceMeasure},
    },
    error::Result,
};

fn proto_oneof_screaming_name(v: &Value) -> &'static str {
    match v.value_type.as_ref() {
        None => "UNSET",
        Some(ValueType::NullValue(_)) => "NULL_VALUE",
        Some(ValueType::BooleanValue(_)) => "BOOLEAN_VALUE",
        Some(ValueType::IntegerValue(_)) => "INTEGER_VALUE",
        Some(ValueType::DoubleValue(_)) => "DOUBLE_VALUE",
        Some(ValueType::TimestampValue(_)) => "TIMESTAMP_VALUE",
        Some(ValueType::StringValue(_)) => "STRING_VALUE",
        Some(ValueType::BytesValue(_)) => "BYTES_VALUE",
        Some(ValueType::ReferenceValue(_)) => "REFERENCE_VALUE",
        Some(ValueType::GeoPointValue(_)) => "GEO_POINT_VALUE",
        Some(ValueType::ArrayValue(_)) => "ARRAY_VALUE",
        Some(ValueType::MapValue(_)) => "MAP_VALUE",
        Some(ValueType::FieldReferenceValue(_)) => "FIELD_REFERENCE_VALUE",
        // TODO: verify exact Cloud Firestore type name once pipeline variables are supported.
        Some(ValueType::VariableReferenceValue(_)) => "VARIABLE_REFERENCE_VALUE",
        Some(ValueType::FunctionValue(_)) => "FUNCTION_VALUE",
        Some(ValueType::PipelineValue(_)) => "PIPELINE_VALUE",
    }
}

fn proto_pascal_type_name(v: &Value) -> &'static str {
    match v.value_type.as_ref() {
        None => "Unset",
        Some(ValueType::NullValue(_)) => "NullValue",
        Some(ValueType::BooleanValue(_)) => "BooleanValue",
        Some(ValueType::IntegerValue(_)) => "IntegerValue",
        Some(ValueType::DoubleValue(_)) => "DoubleValue",
        Some(ValueType::TimestampValue(_)) => "TimestampValue",
        Some(ValueType::StringValue(_)) => "StringValue",
        Some(ValueType::BytesValue(_)) => "BytesValue",
        Some(ValueType::ReferenceValue(_)) => "ReferenceValue",
        Some(ValueType::GeoPointValue(_)) => "GeoPointValue",
        Some(ValueType::ArrayValue(_)) => "ArrayValue",
        Some(ValueType::MapValue(_)) => "MapValue",
        Some(ValueType::FieldReferenceValue(_)) => "FieldReferenceValue",
        // TODO: verify exact Cloud Firestore type name once pipeline variables are supported.
        Some(ValueType::VariableReferenceValue(_)) => "VariableReferenceValue",
        Some(ValueType::FunctionValue(_)) => "FunctionValue",
        Some(ValueType::PipelineValue(_)) => "PipelineValue",
    }
}

pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>> {
    if stage
        .args
        .iter()
        .chain(stage.options.values())
        .any(|v| matches!(v.value_type, Some(ValueType::VariableReferenceValue(_))))
    {
        crate::unimplemented!("variable_reference_value");
    }

    let [field_arg, vector_arg, measure_arg] = stage.args.as_slice() else {
        return Err(GenericDatabaseError::invalid_argument(
            "Stage 'find_nearest' expects 3 arguments (field, vector, distance_measure)",
        ));
    };

    let vector_field = match field_arg.value_type.as_ref() {
        Some(ValueType::FieldReferenceValue(s)) => FieldReference::from_str(s)?
            .into_non_reserved_field_path(
                "find_nearest field must be a non-reserved FieldReference",
            )?,
        _ => {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "Expected field to be FIELD_REFERENCE_VALUE, but was {}.",
                proto_oneof_screaming_name(field_arg)
            )));
        }
    };

    let query_vector = vector::extract_vector_coords(vector_arg, |v| {
        format!(
            "Expected vector to be VectorValue, but was {}.",
            proto_pascal_type_name(v)
        )
    })?;

    let distance_measure = match measure_arg.value_type.as_ref() {
        Some(ValueType::StringValue(s)) => DistanceMeasure::from_pipeline_str(s)?,
        _ => {
            return Err(GenericDatabaseError::invalid_argument(
                "find_nearest distance_measure must be 'euclidean', 'cosine', or 'dot_product'",
            ));
        }
    };

    vector::validate_query_vector_for_measure(&query_vector, distance_measure, true)?;

    let unknown: Vec<&str> = stage
        .options
        .keys()
        .map(String::as_str)
        .filter(|k| !matches!(*k, "distance_field" | "limit"))
        .sorted_unstable()
        .collect();
    if !unknown.is_empty() {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "Stage 'find_nearest(field: FieldReference, vector: Vector, distance_measure: \
             DistanceMeasure, limit: Int64?, distance_field: FieldPath?)' does not support \
             option(s) [{}].",
            unknown.join(", ")
        )));
    }

    let limit = match stage.options.get("limit") {
        None => None,
        Some(v) => match v.value_type.as_ref() {
            Some(ValueType::IntegerValue(n))
                if (1..=vector::MAX_FIND_NEAREST_LIMIT as i64).contains(n) =>
            {
                Some(*n as usize)
            }
            _ => {
                return Err(GenericDatabaseError::invalid_argument(
                    "find_nearest limit must be an integer between 1 and 1000",
                ));
            }
        },
    };

    let distance_field = match stage.options.get("distance_field") {
        None => None,
        Some(v) => match v.value_type.as_ref() {
            Some(ValueType::FieldReferenceValue(s)) => {
                Some(FieldReference::from_str(s)?.into_non_reserved_field_path(
                    "find_nearest distance_field must be a non-reserved FieldReference",
                )?)
            }
            _ => {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "Expected distance_field to be FIELD_REFERENCE_VALUE, but was {}.",
                    proto_oneof_screaming_name(v)
                )));
            }
        },
    };

    let mut candidates = Vec::new();
    for doc in docs {
        let Some(coords) = vector_field
            .get_value(&doc.fields)
            .and_then(|v| v.iter_vector_f64())
        else {
            continue;
        };
        if coords.len() != query_vector.len() {
            continue;
        }
        let dist =
            vector::compute_distance(distance_measure, query_vector.iter().copied(), coords)?;
        candidates.push((doc, dist));
    }

    vector::select_and_sort_nearest(
        &mut candidates,
        limit,
        |(doc_a, dist_a), (doc_b, dist_b)| {
            distance_measure
                .cmp_distance(*dist_a, *dist_b)
                .then_with(|| cmp_reference_paths(&doc_a.name, &doc_b.name))
        },
    );

    let result = candidates
        .into_iter()
        .map(|(mut doc, dist)| {
            if let Some(dist_path) = &distance_field {
                dist_path.set_value(&mut doc.fields, Value::double(dist));
            }
            doc
        })
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn find_nearest_rejects_variable_reference_value_as_not_implemented() {
        let var_val = Value {
            value_type: Some(ValueType::VariableReferenceValue("v".to_string())),
        };
        let stage_arg = Stage {
            name:    "find_nearest".to_string(),
            args:    vec![
                var_val.clone(),
                Value::vector([1.0, 0.0]),
                Value::string("euclidean"),
            ],
            options: HashMap::new(),
        };
        assert!(matches!(
            execute(stage_arg, vec![]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));

        let stage_opt = Stage {
            name:    "find_nearest".to_string(),
            args:    vec![
                Value {
                    value_type: Some(ValueType::FieldReferenceValue("emb".to_string())),
                },
                Value::vector([1.0, 0.0]),
                Value::string("euclidean"),
            ],
            options: HashMap::from([("limit".to_string(), var_val)]),
        };
        assert!(matches!(
            execute(stage_opt, vec![]),
            Err(GenericDatabaseError::NotImplemented(_))
        ));
    }

    #[test]
    fn find_nearest_preserves_pipeline_vector_and_limit_diagnostics() {
        let make_stage = |vector_arg: Value, measure: &str, limit: Option<Value>| Stage {
            name:    "find_nearest".to_string(),
            args:    vec![
                Value {
                    value_type: Some(ValueType::FieldReferenceValue("emb".to_string())),
                },
                vector_arg,
                Value::string(measure),
            ],
            options: limit
                .map(|l| HashMap::from([("limit".to_string(), l)]))
                .unwrap_or_default(),
        };

        assert_eq!(
            execute(make_stage(Value::integer(1), "euclidean", None), vec![]).unwrap_err(),
            GenericDatabaseError::invalid_argument(
                "Expected vector to be VectorValue, but was IntegerValue."
            )
        );
        assert_eq!(
            execute(
                make_stage(
                    Value::map(HashMap::from([("k".to_string(), Value::integer(1))])),
                    "euclidean",
                    None
                ),
                vec![]
            )
            .unwrap_err(),
            GenericDatabaseError::invalid_argument(
                "Expected vector to be VectorValue, but was MapValue."
            )
        );
        assert_eq!(
            execute(make_stage(Value::vector([]), "euclidean", None), vec![]).unwrap_err(),
            GenericDatabaseError::invalid_argument("Cannot have a zero length vector.")
        );
        assert_eq!(
            execute(
                make_stage(Value::vector([0.0, 0.0]), "cosine", None),
                vec![]
            )
            .unwrap_err(),
            GenericDatabaseError::invalid_argument("cosine_distance(...) result is NaN.")
        );
        for bad_limit in [Value::integer(0), Value::integer(1001), Value::double(1.0)] {
            assert_eq!(
                execute(
                    make_stage(Value::vector([1.0, 0.0]), "euclidean", Some(bad_limit)),
                    vec![]
                )
                .unwrap_err(),
                GenericDatabaseError::invalid_argument(
                    "find_nearest limit must be an integer between 1 and 1000"
                )
            );
        }
        assert!(
            execute(
                make_stage(
                    Value::vector([1.0, 0.0]),
                    "euclidean",
                    Some(Value::integer(1000))
                ),
                vec![]
            )
            .is_ok()
        );
    }
}
