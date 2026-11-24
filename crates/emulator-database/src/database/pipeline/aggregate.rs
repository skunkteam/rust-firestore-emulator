use std::collections::{BTreeMap, HashMap};

use googleapis::google::firestore::v1::{Document, Value, pipeline::Stage, value::ValueType};

use super::parse_projection_paths;
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

    let group_by_map = match args.get(1) {
        None => None,
        Some(v) => Some(v.as_map().ok_or_else(|| {
            GenericDatabaseError::invalid_argument(
                "aggregate stage expects a MapValue argument as second argument",
            )
        })?),
    };

    let sorted_aggs = parse_projection_paths(aggregations)?;
    let group_by_fields = match group_by_map {
        None => Vec::new(),
        Some(map) => parse_projection_paths(map)?,
    };

    for (agg_path, _) in &sorted_aggs {
        for (grp_path, _) in &group_by_fields {
            if agg_path.starts_with(grp_path) || grp_path.starts_with(agg_path) {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "Conflicting field paths '{agg_path}' and '{grp_path}'"
                )));
            }
        }
    }

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

        for (i, (path, _)) in group_by_fields.iter().enumerate() {
            path.set_value(&mut result_fields, group_key[i].clone());
        }

        for &(ref path, value) in &sorted_aggs {
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
                        crate::database::sum_enterprise_checked(values)?
                    }
                }
                "average" => {
                    let [expr] = get_exact_expr_args(&func.args, &func.name)?;
                    let values = collect_numeric_values(expr, &bucket_docs)?;
                    if values.is_empty() {
                        Value::null()
                    } else {
                        let len = values.len();
                        let sum = crate::database::sum_enterprise_checked(values)?;
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

            path.set_value(&mut result_fields, result_val);
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

#[cfg(test)]
mod tests {
    use googleapis::google::firestore::v1::Function;

    use super::*;

    fn field_ref(path: &str) -> Value {
        Value {
            value_type: Some(ValueType::FieldReferenceValue(path.to_string())),
        }
    }

    fn fn_val(name: &str, args: Vec<Value>) -> Value {
        Value {
            value_type: Some(ValueType::FunctionValue(Function {
                name: name.to_string(),
                args,
                options: HashMap::new(),
            })),
        }
    }

    #[test]
    fn aggregate_parses_field_path_aliases_and_rejects_reserved_or_conflicting_paths() {
        let docs = vec![Document {
            name: "projects/p/databases/(default)/documents/c/d1".to_string(),
            fields: HashMap::from([
                ("g".to_string(), Value::string("k")),
                ("n".to_string(), Value::integer(5)),
            ]),
            create_time: None,
            update_time: None,
        }];

        let out = execute(
            Stage {
                name:    "aggregate".to_string(),
                args:    vec![
                    Value::map(HashMap::from([
                        ("`a.b`".to_string(), fn_val("count", vec![])),
                        ("stats.sum".to_string(), fn_val("sum", vec![field_ref("n")])),
                    ])),
                    Value::map(HashMap::from([("grp.key".to_string(), field_ref("g"))])),
                ],
                options: HashMap::new(),
            },
            docs.clone(),
        )
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields.get("a.b"), Some(&Value::integer(1)));
        assert_eq!(
            out[0]
                .fields
                .get("stats")
                .and_then(Value::as_map)
                .and_then(|m| m.get("sum")),
            Some(&Value::integer(5))
        );
        assert_eq!(
            out[0]
                .fields
                .get("grp")
                .and_then(Value::as_map)
                .and_then(|m| m.get("key")),
            Some(&Value::string("k"))
        );

        let reserved = execute(
            Stage {
                name:    "aggregate".to_string(),
                args:    vec![Value::map(HashMap::from([(
                    "__reserved__".to_string(),
                    fn_val("count", vec![]),
                )]))],
                options: HashMap::new(),
            },
            docs.clone(),
        );
        assert!(matches!(
            reserved,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        let conflicting = execute(
            Stage {
                name:    "aggregate".to_string(),
                args:    vec![
                    Value::map(HashMap::from([("a".to_string(), fn_val("count", vec![]))])),
                    Value::map(HashMap::from([("a.b".to_string(), field_ref("g"))])),
                ],
                options: HashMap::new(),
            },
            docs,
        );
        assert!(matches!(
            conflicting,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }
}
