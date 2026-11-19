use std::cmp::Ordering;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::expressions;
use crate::{GenericDatabaseError, error::Result, unimplemented_collection};

/// Executes the `sort` pipeline stage.
///
/// This stage takes one or more sorting expression mappings to order the Document stream natively.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "sort");
    unimplemented_collection!(options);

    if args.is_empty() {
        return Err(GenericDatabaseError::invalid_argument(
            "sort stage expects at least 1 ordering argument",
        ));
    }

    let mut criteria = Vec::with_capacity(args.len());

    for arg in &args {
        let map = arg.as_map().ok_or_else(|| {
            GenericDatabaseError::invalid_argument("sort arguments must be MapValues")
        })?;
        let is_ascending = match map.get("direction") {
            None => true,
            Some(v) => match v.as_string() {
                Some("ascending") => true,
                Some("descending") => false,
                _ => {
                    return Err(GenericDatabaseError::invalid_argument(
                        "sort direction must be 'ascending' or 'descending'",
                    ));
                }
            },
        };
        let expr = map.get("expression").ok_or_else(|| {
            GenericDatabaseError::invalid_argument("sort ordering must contain an 'expression'")
        })?;

        criteria.push((expr, is_ascending));
    }

    let mut keyed_docs = Vec::with_capacity(docs.len());
    for doc in docs {
        let mut keys = Vec::with_capacity(criteria.len());
        for (expr, _) in &criteria {
            keys.push(expressions::evaluate(expr, &doc)?);
        }
        keyed_docs.push((doc, keys));
    }

    keyed_docs.sort_by(|(_, keys_a), (_, keys_b)| {
        for ((_, is_ascending), (val_a, val_b)) in
            criteria.iter().zip(keys_a.iter().zip(keys_b.iter()))
        {
            let ord = val_a.cmp(val_b);
            if ord.is_ne() {
                return if *is_ascending { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    });

    Ok(keyed_docs.into_iter().map(|(doc, _)| doc).collect())
}
