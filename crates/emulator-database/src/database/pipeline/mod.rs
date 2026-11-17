use std::{collections::HashMap, sync::Arc};

use googleapis::google::firestore::v1::{Document, StructuredPipeline};

use crate::{
    FirestoreDatabase, GenericDatabaseError, error::Result, read_consistency::ReadConsistency,
    reference::RootRef, required_option, unimplemented, unimplemented_collection,
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

use googleapis::google::firestore::v1::Value;
use itertools::Itertools;

use crate::database::field_path::{FieldPath, FieldReference};

pub async fn execute(
    database: &Arc<FirestoreDatabase>,
    root_ref: &RootRef,
    StructuredPipeline { pipeline, options }: StructuredPipeline,
    read_consistency: ReadConsistency,
) -> Result<Vec<Document>> {
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

    let mut current_docs = match first.name.as_str() {
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

    Ok(current_docs)
}

/// Extracts a value from a gRPC `Document` based on a resolved `FieldReference`.
///
/// This utility centralizes the logic for bridging Firestore's internal field resolution
/// capabilities directly into the stateless `Document` instances passed through pipeline arrays.
fn get_document_value(field_ref: &FieldReference, doc: &Document) -> Option<Value> {
    match field_ref {
        FieldReference::DocumentName => Some(Value::reference(doc.name.clone())),
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
