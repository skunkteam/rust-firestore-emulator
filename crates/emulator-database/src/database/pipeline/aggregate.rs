use std::collections::{BTreeMap, HashMap};

use googleapis::google::firestore::v1::{Document, Value, pipeline::Stage, value::ValueType};
use itertools::Itertools;

use crate::{
    GenericDatabaseError,
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

    let mut group_by_fields = match args.get(1) {
        None => Vec::new(),
        Some(v) => v
            .as_map()
            .ok_or_else(|| {
                GenericDatabaseError::invalid_argument(
                    "aggregate stage expects a MapValue argument as second argument",
                )
            })?
            .iter()
            .collect_vec(),
    };
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

    let mut sorted_aggs = aggregations.iter().collect_vec();
    sorted_aggs.sort_by(|a, b| a.0.cmp(b.0));

    let mut aggregated_docs = Vec::new();

    for (group_key, bucket_docs) in buckets {
        let mut result_fields = HashMap::new();

        for (i, (alias, _)) in group_by_fields.iter().enumerate() {
            result_fields.insert(alias.to_string(), group_key[i].clone());
        }

        for &(alias, value) in &sorted_aggs {
            let func = match value.value_type.as_ref() {
                Some(ValueType::FunctionValue(f)) => f,
                _ => {
                    return Err(GenericDatabaseError::invalid_argument(
                        "aggregation value must be a FunctionValue",
                    ));
                }
            };
            unimplemented_collection!(func.options);

            let result_val = match func.name.as_str() {
                "count" => match func.args.as_slice() {
                    [] => Value::integer(bucket_docs.len() as i64),
                    [expr] => {
                        let mut count = 0_i64;
                        for doc in &bucket_docs {
                            if let Some(val) = expressions::maybe_evaluate(expr, doc)?
                                && !val.is_null()
                            {
                                count += 1;
                            }
                        }
                        Value::integer(count)
                    }
                    _ => {
                        return Err(GenericDatabaseError::invalid_argument(
                            "count expects 0 or 1 arguments",
                        ));
                    }
                },
                "sum" => {
                    let [expr] = get_exact_expr_args(&func.args, &func.name)?;
                    let values = collect_numeric_values(expr, &bucket_docs)?;
                    if values.is_empty() {
                        Value::null()
                    } else {
                        values.into_iter().sum()
                    }
                }
                "average" => {
                    let [expr] = get_exact_expr_args(&func.args, &func.name)?;
                    let values = collect_numeric_values(expr, &bucket_docs)?;
                    if values.is_empty() {
                        Value::null()
                    } else {
                        let len = values.len();
                        let sum: Value = values.into_iter().sum();
                        Value::double(sum.as_double().unwrap_or(0.0) / len as f64)
                    }
                }
                "minimum" | "maximum" => {
                    let [expr] = get_exact_expr_args(&func.args, &func.name)?;
                    let pick_max = func.name == "maximum";
                    let mut best: Option<Value> = None;
                    for doc in &bucket_docs {
                        if let Some(val) = expressions::maybe_evaluate(expr, doc)?
                            && !val.is_null()
                        {
                            best = Some(match best {
                                None => val,
                                Some(cur) => {
                                    if pick_max {
                                        cur.max(val)
                                    } else {
                                        cur.min(val)
                                    }
                                }
                            });
                        }
                    }
                    best.unwrap_or_else(Value::null)
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

fn collect_numeric_values(expr: &Value, docs: &[Document]) -> Result<Vec<Value>> {
    let mut values = Vec::new();
    for doc in docs {
        if let Some(val) = expressions::maybe_evaluate(expr, doc)?
            && val.is_number()
        {
            values.push(val);
        }
    }
    Ok(values)
}
