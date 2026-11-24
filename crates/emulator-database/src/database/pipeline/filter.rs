use googleapis::google::firestore::v1::{Document, Value, pipeline::Stage};

use super::{expressions, single_arg_stage};
use crate::{GenericDatabaseError, error::Result};

pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>> {
    let arg = single_arg_stage(&stage, "where")?;

    let mut filtered_docs = Vec::new();
    for doc in docs {
        if evaluate_condition(arg, &doc)? {
            filtered_docs.push(doc);
        }
    }

    Ok(filtered_docs)
}

fn evaluate_condition(expr: &Value, doc: &Document) -> Result<bool> {
    let Some(result) = expressions::maybe_evaluate(expr, doc)? else {
        return Ok(false);
    };
    if result.is_null() {
        return Ok(false);
    }
    let is_match = result.as_boolean().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("where condition expects a boolean argument")
    })?;

    Ok(is_match)
}
