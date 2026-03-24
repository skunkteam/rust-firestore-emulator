use googleapis::google::firestore::v1::{Document, Function, pipeline::Stage};

use crate::{
    GenericDatabaseError,
    error::Result,
    pipeline::{function, get_exact_expr_args},
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

    let func = arg.as_function_value().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("where stage expects a FunctionValue argument")
    })?;

    let mut filtered_docs = Vec::new();
    for doc in docs {
        if evaluate_condition(func, &doc)? {
            filtered_docs.push(doc);
        }
    }

    Ok(filtered_docs)
}

fn evaluate_condition(func: &Function, doc: &Document) -> Result<bool> {
    let result = function::evaluate(func, doc)?;
    let is_match = result.and_then(|v| v.as_boolean()).ok_or_else(|| {
        GenericDatabaseError::invalid_argument("where condition expects a boolean argument")
    })?;

    Ok(is_match)
}
