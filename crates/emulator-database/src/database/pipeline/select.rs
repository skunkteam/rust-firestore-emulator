use std::collections::HashMap;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::{expressions, parse_projection_paths};
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
    let targets = parse_projection_paths(projection_map)?;

    for doc in docs.iter_mut() {
        let mut selected_fields = HashMap::new();

        for (path, expr) in &targets {
            let evaluated_val = expressions::evaluate(expr, doc)?;
            path.set_value(&mut selected_fields, evaluated_val);
        }

        doc.fields = selected_fields;
    }

    Ok(docs)
}
