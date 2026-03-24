use std::cmp::Ordering;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::get_document_value;
use crate::{
    GenericDatabaseError, database::field_path::FieldReference, error::Result,
    unimplemented_collection,
};

/// Executes the `sort` pipeline stage.
///
/// This stage takes one or more sorting expression mappings to order the Document stream natively.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    mut docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "sort");
    unimplemented_collection!(options);

    let mut criteria = Vec::new();

    for arg in &args {
        let map = arg.as_map().ok_or_else(|| {
            GenericDatabaseError::invalid_argument("sort arguments must be MapValues")
        })?;
        let dir_val = map.get("direction").and_then(|v| v.as_string());
        let field_ref: FieldReference = map
            .get("expression")
            .and_then(|v| v.as_field_reference())
            .ok_or_else(|| {
                GenericDatabaseError::invalid_argument(
                    "sort expression must be a FieldReferenceValue",
                )
            })?
            .parse()?;

        let is_ascending = match dir_val {
            Some("descending") => false,
            Some("ascending") => true,
            _ => true,
        };

        criteria.push((field_ref, is_ascending));
    }

    docs.sort_by(|a, b| {
        for (field_ref, is_ascending) in &criteria {
            let val_a = get_document_value(field_ref, a);
            let val_b = get_document_value(field_ref, b);

            let ord = match (val_a, val_b) {
                (Some(ref a_val), Some(ref b_val)) => a_val.cmp(b_val),
                (Some(_), None) => Ordering::Greater,
                (None, Some(_)) => Ordering::Less,
                (None, None) => Ordering::Equal,
            };

            if ord.is_ne() {
                return if *is_ascending { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    });

    Ok(docs)
}
