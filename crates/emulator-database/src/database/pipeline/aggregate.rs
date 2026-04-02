use std::collections::{BTreeMap, HashMap};

use googleapis::google::firestore::v1::{
    Document, Function, Value, pipeline::Stage, value::ValueType,
};
use itertools::Itertools;

use super::get_document_value;
use crate::{
    GenericDatabaseError,
    database::field_path::FieldReference,
    error::Result,
    pipeline::{expressions, get_exact_expr_args},
    unimplemented_collection,
};

pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "aggregate");
    unimplemented_collection!(options);
    if args.len() > 2 {
        return Err(GenericDatabaseError::invalid_argument(
            "aggregate stage expects at most 2 arguments",
        ));
    }

    let aggregations = args.first().and_then(|arg| arg.as_map()).ok_or_else(|| {
        GenericDatabaseError::invalid_argument(
            "aggregate stage expects a MapValue argument as first argument",
        )
    })?;

    let mut group_by_fields = args
        .get(1)
        .and_then(|v| v.as_map())
        .map(|v| v.iter().collect_vec())
        .unwrap_or_default();
    // Sort to ensure stable group key ordering
    group_by_fields.sort_by(|a, b| a.0.cmp(b.0));

    let mut buckets: BTreeMap<Vec<Value>, Vec<Document>> = BTreeMap::new();

    if group_by_fields.is_empty() {
        buckets.insert(vec![], docs);
    } else {
        for doc in docs {
            let mut key = Vec::with_capacity(group_by_fields.len());
            for (_, field_ref) in &group_by_fields {
                let val = expressions::evaluate(field_ref, &doc)?;
                key.push(val);
            }
            buckets.entry(key).or_default().push(doc);
        }
    }

    let mut aggregated_docs = Vec::new();

    for (group_key, bucket_docs) in buckets {
        let mut result_fields = HashMap::new();

        for (i, (alias, _)) in group_by_fields.iter().enumerate() {
            result_fields.insert(alias.to_string(), group_key[i].clone());
        }

        for (alias, value) in aggregations {
            let func = match value.value_type.as_ref() {
                Some(ValueType::FunctionValue(f)) => f,
                _ => {
                    return Err(GenericDatabaseError::invalid_argument(
                        "aggregation value must be a FunctionValue",
                    ));
                }
            };

            let result_val = match func.name.as_str() {
                "count" => Value::integer(bucket_docs.len() as i64),
                "sum" => {
                    let field_ref = extract_field_ref(func)?;
                    let values: Vec<Value> = bucket_docs
                        .iter()
                        .filter_map(|doc| get_document_value(&field_ref, doc))
                        .filter(|v| v.is_number())
                        .collect();
                    if values.is_empty() {
                        Value::null()
                    } else {
                        values.into_iter().sum()
                    }
                }
                "average" => {
                    let field_ref = extract_field_ref(func)?;
                    let values: Vec<Value> = bucket_docs
                        .iter()
                        .filter_map(|doc| get_document_value(&field_ref, doc))
                        .filter(|v| v.is_number())
                        .collect();
                    if values.is_empty() {
                        Value::null()
                    } else {
                        let len = values.len();
                        let sum: Value = values.into_iter().sum();
                        Value::double(sum.as_double().unwrap_or(0.0) / len as f64)
                    }
                }
                "minimum" => {
                    let field_ref = extract_field_ref(func)?;
                    bucket_docs
                        .iter()
                        .filter_map(|doc| get_document_value(&field_ref, doc))
                        .min()
                        .unwrap_or_else(Value::null)
                }
                "maximum" => {
                    let field_ref = extract_field_ref(func)?;
                    bucket_docs
                        .iter()
                        .filter_map(|doc| get_document_value(&field_ref, doc))
                        .max()
                        .unwrap_or_else(Value::null)
                }
                _ => {
                    return Err(GenericDatabaseError::not_implemented(format!(
                        "aggregation function '{}' not supported",
                        func.name
                    )));
                }
            };

            result_fields.insert(alias.clone(), result_val);
        }

        aggregated_docs.push(Document {
            name: String::new(),
            fields: result_fields,
            create_time: None,
            update_time: None,
        });
    }

    Ok(aggregated_docs)
}

// Ensure the helper fetches the field reference string and parses it into FieldReference
fn extract_field_ref(func: &Function) -> Result<FieldReference> {
    let [arg] = get_exact_expr_args(&func.args, &func.name)?;
    arg.as_field_reference()
        .ok_or_else(|| {
            GenericDatabaseError::invalid_argument(format!(
                "function '{}' expects a FieldReferenceValue argument",
                func.name
            ))
        })
        .and_then(|s| s.parse())
}
