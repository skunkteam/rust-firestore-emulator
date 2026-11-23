use std::{collections::HashMap, sync::Arc};

use googleapis::google::{
    firestore::v1::{Document, StructuredPipeline, Value},
    protobuf::Timestamp,
};
use itertools::Itertools;

use crate::{
    FirestoreDatabase, GenericDatabaseError,
    database::field_path::{FieldPath, FieldReference},
    error::Result,
    read_consistency::ReadConsistency,
    reference::RootRef,
    required_option, unimplemented, unimplemented_collection,
};

mod add_fields;
mod aggregate;
mod collection;
mod distinct;
mod expressions;
mod filter;
mod find_nearest;
mod function;
mod limit;
mod offset;
mod remove_fields;
mod select;
mod sort;

pub async fn execute(
    database: &Arc<FirestoreDatabase>,
    root_ref: &RootRef,
    StructuredPipeline { pipeline, options }: StructuredPipeline,
    read_consistency: ReadConsistency,
) -> Result<(Timestamp, Vec<Document>)> {
    // These operators are only supported in Enterprise Edition.
    // We enforce this rule universally across the pipeline engine very early.
    if !database.enterprise_edition() {
        return Err(GenericDatabaseError::failed_precondition(
            "Pipeline APIs are only available in Enterprise Edition.",
        ));
    }

    required_option!(pipeline);
    unimplemented_collection!(options);

    let mut stages = pipeline.stages.into_iter();

    let first = stages
        .next()
        .ok_or_else(|| GenericDatabaseError::invalid_argument("pipeline stages cannot be empty"))?;

    let (read_time, mut current_docs) = match first.name.as_str() {
        "collection" => collection::execute(database, root_ref, first, read_consistency).await?,
        _ => unimplemented!(format!("pipeline stage {}", first.name)),
    };

    for stage in stages {
        current_docs = match stage.name.as_str() {
            "aggregate" => aggregate::execute(stage, current_docs)?,
            "where" => filter::execute(stage, current_docs)?,
            "limit" => limit::execute(stage, current_docs)?,
            "offset" => offset::execute(stage, current_docs)?,
            "sort" => sort::execute(stage, current_docs)?,
            "distinct" => distinct::execute(stage, current_docs)?,
            "add_fields" => add_fields::execute(stage, current_docs)?,
            "select" => select::execute(stage, current_docs)?,
            "remove_fields" => remove_fields::execute(stage, current_docs)?,
            "find_nearest" => find_nearest::execute(stage, current_docs)?,
            _ => unimplemented!(format!("pipeline stage {}", stage.name)),
        };
    }

    Ok((read_time, current_docs))
}

/// Extracts a value from a gRPC `Document` based on a resolved `FieldReference`.
///
/// This utility centralizes the logic for bridging Firestore's internal field resolution
/// capabilities directly into the stateless `Document` instances passed through pipeline arrays.
fn get_document_value(field_ref: &FieldReference, doc: &Document) -> Option<Value> {
    match field_ref {
        FieldReference::DocumentName => {
            (!doc.name.is_empty()).then(|| Value::reference(doc.name.clone()))
        }
        FieldReference::FieldPath(path) => path.get_value(&doc.fields).cloned(),
    }
}

fn get_exact_expr_args<'a, const N: usize>(
    args: &'a [Value],
    what: &str,
) -> Result<[&'a Value; N]> {
    args.iter().collect_array().ok_or_else(|| {
        GenericDatabaseError::invalid_argument(format!("{what} expects {N} arguments"))
    })
}

/// Parses and validates projection/add-fields target paths deterministically, rejecting
/// overlapping ancestor/descendant or duplicate-representation paths in the same stage.
pub(super) fn parse_projection_paths(
    projection_map: &HashMap<String, Value>,
) -> Result<Vec<(FieldPath, &Value)>> {
    let mut entries: Vec<(&String, FieldPath, &Value)> = Vec::with_capacity(projection_map.len());
    for (alias, expr) in projection_map {
        let path: FieldPath = alias.parse()?;
        path.validate_non_reserved()?;
        entries.push((alias, path, expr));
    }
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for i in 0..entries.len() {
        for j in (i + 1)..entries.len() {
            if entries[i].1.starts_with(&entries[j].1) || entries[j].1.starts_with(&entries[i].1) {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "Conflicting field paths '{}' and '{}'",
                    entries[i].0, entries[j].0
                )));
            }
        }
    }

    Ok(entries
        .into_iter()
        .map(|(_, path, expr)| (path, expr))
        .collect())
}

#[cfg(test)]
mod tests {
    use googleapis::google::firestore::v1::{
        Function, Pipeline, pipeline::Stage, value::ValueType,
    };

    use super::*;
    use crate::{FirestoreConfig, FirestoreProject, reference::DocumentRef};

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
    fn remove_fields_document_name_treats_name_as_absent_in_subsequent_stages() {
        let doc = Document {
            name: "projects/p/databases/(default)/documents/c/d1".to_string(),
            fields: HashMap::from([("x".to_string(), Value::integer(1))]),
            create_time: None,
            update_time: None,
        };

        let after_rm = remove_fields::execute(
            Stage {
                name:    "remove_fields".to_string(),
                args:    vec![field_ref("__name__")],
                options: HashMap::new(),
            },
            vec![doc],
        )
        .unwrap();

        let selected = select::execute(
            Stage {
                name:    "select".to_string(),
                args:    vec![Value::map(HashMap::from([
                    (
                        "name_absent".to_string(),
                        fn_val("is_absent", vec![field_ref("__name__")]),
                    ),
                    (
                        "eq_missing".to_string(),
                        fn_val("equal", vec![field_ref("__name__"), field_ref("missing")]),
                    ),
                    ("projected_name".to_string(), field_ref("__name__")),
                ]))],
                options: HashMap::new(),
            },
            after_rm,
        )
        .unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].fields["name_absent"], Value::boolean(true));
        assert_eq!(selected[0].fields["eq_missing"], Value::boolean(true));
        assert_eq!(selected[0].fields["projected_name"], Value::null());
    }

    #[tokio::test]
    async fn execute_propagates_snapshot_read_time_under_default_consistency_across_concurrent_writes()
     {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
                default_enterprise: true,
                ..Default::default()
            })));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/d1"
            .parse()
            .unwrap();

        db.update_doc(
            &doc_ref,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let collections_guard = db.collections.write().await;
        let mut fut = Box::pin(execute(
            &db,
            &root,
            StructuredPipeline {
                pipeline: Some(Pipeline {
                    stages: vec![Stage {
                        name:    "collection".to_string(),
                        args:    vec![Value::reference("/c".to_string())],
                        options: HashMap::new(),
                    }],
                }),
                options:  HashMap::new(),
            },
            ReadConsistency::Default,
        ));
        assert!(futures_util::poll!(&mut fut).is_pending());
        drop(collections_guard);

        let write_ts = db
            .update_doc(
                &doc_ref,
                HashMap::from([("v".to_string(), Value::integer(2))]),
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .update_time
            .unwrap();

        let (read_time, docs) = fut.await.unwrap();
        assert!(
            read_time < write_ts,
            "snapshot read_time {read_time:?} must precede concurrent write {write_ts:?}"
        );
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].fields.get("v"), Some(&Value::integer(1)));
        assert!(docs[0].update_time.unwrap() <= read_time);

        let collections_guard = db.collections.write().await;
        let mut filtered_empty_fut = Box::pin(execute(
            &db,
            &root,
            StructuredPipeline {
                pipeline: Some(Pipeline {
                    stages: vec![
                        Stage {
                            name:    "collection".to_string(),
                            args:    vec![Value::reference("/c".to_string())],
                            options: HashMap::new(),
                        },
                        Stage {
                            name:    "where".to_string(),
                            args:    vec![Value::boolean(false)],
                            options: HashMap::new(),
                        },
                    ],
                }),
                options:  HashMap::new(),
            },
            ReadConsistency::Default,
        ));
        assert!(futures_util::poll!(&mut filtered_empty_fut).is_pending());
        drop(collections_guard);

        let write_ts2 = db
            .update_doc(
                &doc_ref,
                HashMap::from([("v".to_string(), Value::integer(3))]),
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .update_time
            .unwrap();

        let (empty_read_time, empty_docs) = filtered_empty_fut.await.unwrap();
        assert!(empty_docs.is_empty());
        assert!(
            empty_read_time < write_ts2,
            "filtered-to-empty snapshot read_time {empty_read_time:?} must precede concurrent \
             write {write_ts2:?}"
        );
    }
}
