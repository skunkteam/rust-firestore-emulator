use std::collections::{BTreeSet, HashMap};

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::expressions;
use crate::{
    GenericDatabaseError, error::Result, pipeline::get_exact_expr_args, unimplemented_collection,
};

/// Executes the `distinct` pipeline stage.
///
/// Filters the stream keeping only the first document producing a unique combination of
/// values extracted by the expressions mapping, returning exactly the evaluated mapping elements
/// natively.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "distinct");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "distinct")?;
    let distinct_map = arg.as_map().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("distinct stage expects a MapValue argument")
    })?;

    let mut seen = BTreeSet::new();
    let mut distinct_docs = Vec::new();

    for doc in docs {
        let mut key = Vec::with_capacity(distinct_map.len());
        let mut result_fields = HashMap::with_capacity(distinct_map.len());

        for (alias, expr) in distinct_map {
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
