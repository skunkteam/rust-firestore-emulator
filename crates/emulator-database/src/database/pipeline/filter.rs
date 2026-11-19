use googleapis::google::firestore::v1::{Document, Value, pipeline::Stage};

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
    assert_eq!(name, "where");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "where")?;

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
