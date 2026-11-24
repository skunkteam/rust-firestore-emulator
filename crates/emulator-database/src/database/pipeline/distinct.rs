use std::collections::{BTreeSet, HashMap};

use googleapis::google::firestore::v1::{Document, Value, pipeline::Stage};

use super::{expressions, single_map_stage};
use crate::error::Result;

/// Executes the `distinct` pipeline stage.
///
/// Filters the stream keeping only the first document producing a unique combination of
/// values extracted by the expressions mapping, returning exactly the evaluated mapping elements
/// natively.
pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>> {
    let distinct_map = single_map_stage(&stage, "distinct")?;
    let mut entries: Vec<(&String, &Value)> = distinct_map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut seen = BTreeSet::new();
    let mut distinct_docs = Vec::new();

    for doc in docs {
        let mut key = Vec::with_capacity(entries.len());
        let mut result_fields = HashMap::with_capacity(entries.len());

        for &(alias, expr) in &entries {
            let evaluated = expressions::evaluate(expr, &doc)?;

            key.push(evaluated.clone());
            result_fields.insert(alias.clone(), evaluated);
        }

        if seen.insert(key) {
            distinct_docs.push(Document {
                name: String::new(),
                fields: result_fields,
                create_time: None,
                update_time: None,
            });
        }
    }

    Ok(distinct_docs)
}

#[cfg(test)]
mod tests {
    use googleapis::google::firestore::v1::{Function, value::ValueType};

    use super::*;
    use crate::GenericDatabaseError;

    fn fn_val(name: &str, args: Vec<Value>) -> Value {
        Value {
            value_type: Some(ValueType::FunctionValue(Function {
                name: name.to_string(),
                args,
                options: HashMap::new(),
            })),
        }
    }

    fn doc() -> Document {
        Document {
            name: "projects/p/databases/(default)/documents/c/d1".to_string(),
            fields: HashMap::new(),
            create_time: None,
            update_time: None,
        }
    }

    #[test]
    fn distinct_reports_errors_deterministically_by_sorted_alias() {
        let div_zero = fn_val("divide", vec![Value::integer(1), Value::integer(0)]);
        let neg_sqrt = fn_val("sqrt", vec![Value::double(-1.0)]);
        let unknown = fn_val("unimplemented_fn", vec![Value::integer(1)]);

        for _ in 0..20 {
            let mut map_a_first = HashMap::new();
            map_a_first.insert("z_err".to_string(), unknown.clone());
            map_a_first.insert("m_ok".to_string(), Value::integer(42));
            map_a_first.insert("a_err".to_string(), div_zero.clone());

            let err = execute(
                Stage {
                    name:    "distinct".to_string(),
                    args:    vec![Value::map(map_a_first)],
                    options: HashMap::new(),
                },
                vec![doc()],
            )
            .unwrap_err();
            assert_eq!(
                err,
                GenericDatabaseError::invalid_argument("Division by zero")
            );

            let mut map_unimpl_first = HashMap::new();
            map_unimpl_first.insert("a_err".to_string(), unknown.clone());
            map_unimpl_first.insert("z_err".to_string(), div_zero.clone());

            let err = execute(
                Stage {
                    name:    "distinct".to_string(),
                    args:    vec![Value::map(map_unimpl_first)],
                    options: HashMap::new(),
                },
                vec![doc()],
            )
            .unwrap_err();
            assert!(
                matches!(err, GenericDatabaseError::NotImplemented(_)),
                "expected sorted alias 'a_err' (NotImplemented) first, got {err:?}"
            );

            let mut map_two_invalid = HashMap::new();
            map_two_invalid.insert("z_div".to_string(), div_zero.clone());
            map_two_invalid.insert("a_sqrt".to_string(), neg_sqrt.clone());

            let err = execute(
                Stage {
                    name:    "distinct".to_string(),
                    args:    vec![Value::map(map_two_invalid)],
                    options: HashMap::new(),
                },
                vec![doc()],
            )
            .unwrap_err();
            assert_ne!(
                err,
                GenericDatabaseError::invalid_argument("Division by zero")
            );
            assert!(matches!(err, GenericDatabaseError::InvalidArgument(_)));
        }
    }

    #[test]
    fn distinct_preserves_raw_alias_keys_and_rejects_vector_map() {
        let out = execute(
            Stage {
                name:    "distinct".to_string(),
                args:    vec![Value::map(HashMap::from([(
                    "a.b".to_string(),
                    Value::integer(7),
                )]))],
                options: HashMap::new(),
            },
            vec![doc()],
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields.get("a.b"), Some(&Value::integer(7)));

        let err = execute(
            Stage {
                name:    "distinct".to_string(),
                args:    vec![Value::vector([1.0, 2.0])],
                options: HashMap::new(),
            },
            vec![doc()],
        )
        .unwrap_err();
        assert_eq!(
            err,
            GenericDatabaseError::invalid_argument("distinct stage expects a MapValue argument")
        );
    }
}
