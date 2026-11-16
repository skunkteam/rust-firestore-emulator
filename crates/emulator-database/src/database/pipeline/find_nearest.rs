use std::str::FromStr;

use googleapis::{
    VectorClassification, classify_vector_map, cmp_reference_paths,
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
        Some(ValueType::FunctionValue(_)) => "FunctionValue",
        Some(ValueType::PipelineValue(_)) => "PipelineValue",
    }
}

pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>> {
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

    let query_vector: Vec<f64> = match vector_arg.value_type.as_ref() {
        Some(ValueType::MapValue(map)) => match classify_vector_map(&map.fields) {
            VectorClassification::Valid(vals) => vals.iter().filter_map(Value::as_double).collect(),
            VectorClassification::Invalid(err) => {
                return Err(GenericDatabaseError::invalid_argument(
                    err.as_error_message(),
                ));
            }
            VectorClassification::NotVectorMap => {
                return Err(GenericDatabaseError::invalid_argument(
                    "Expected vector to be VectorValue, but was MapValue.",
                ));
            }
        },
        _ => {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "Expected vector to be VectorValue, but was {}.",
                proto_pascal_type_name(vector_arg)
            )));
        }
    };

    let distance_measure = match measure_arg.value_type.as_ref() {
        Some(ValueType::StringValue(s)) => DistanceMeasure::from_pipeline_str(s)?,
        _ => {
            return Err(GenericDatabaseError::invalid_argument(
                "find_nearest distance_measure must be 'euclidean', 'cosine', or 'dot_product'",
            ));
        }
    };

    if distance_measure == DistanceMeasure::Cosine {
        vector::compute_distance(
            DistanceMeasure::Cosine,
            query_vector.iter().copied(),
            query_vector.iter().copied(),
        )?;
    }

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
            Some(ValueType::IntegerValue(n)) if (1..=1000).contains(n) => Some(*n as usize),
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
