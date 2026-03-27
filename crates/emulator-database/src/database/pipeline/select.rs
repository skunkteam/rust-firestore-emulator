use std::collections::HashMap;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::expressions;
use crate::{
    GenericDatabaseError, error::Result, pipeline::get_exact_expr_args, unimplemented_collection,
};

/// Executes the `select` pipeline stage.
///
/// Iterates over mappings executing syntax projections across the row and discarding all properties
/// omitting explicitly mapped elements in the evaluated MapValue scope.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    mut docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "select");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "select")?;

    let projection_map = arg.as_map().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("select stage expects a MapValue argument")
    })?;

    for doc in docs.iter_mut() {
        let mut selected_fields = HashMap::new();

        for (alias, expr) in projection_map {
            let evaluated_val = expressions::evaluate(expr, doc)?;
            selected_fields.insert(alias.clone(), evaluated_val);
        }

        doc.fields = selected_fields;
    }

    Ok(docs)
}
