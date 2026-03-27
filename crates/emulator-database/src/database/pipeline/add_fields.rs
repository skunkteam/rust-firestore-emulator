use std::collections::HashMap;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::expressions;
use crate::{
    GenericDatabaseError, error::Result, pipeline::get_exact_expr_args, unimplemented_collection,
};

/// Executes the `add_fields` pipeline stage.
///
/// This stage evaluates expressions mapped by aliases and appends or overwrites them into the
/// existing row stream.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "add_fields");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "add_fields")?;

    let projection_map = arg.as_map().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("add_fields stage expects a MapValue argument")
    })?;

    docs.into_iter()
        .map(|mut doc| {
            let mut add_fields_evals = HashMap::with_capacity(projection_map.len());

            for (alias, expr) in projection_map {
                let evaluated_val = expressions::evaluate(expr, &doc)?;
                add_fields_evals.insert(alias.clone(), evaluated_val);
            }

            for (alias, val) in add_fields_evals {
                // TODO: We only inject flat keys. Nested key mapping requires parsing string
                // tokens. Moet ook nog een test voor gemaakt worden om Cloud gedrag te testen.
                doc.fields.insert(alias, val);
            }

            Ok(doc)
        })
        .collect()
}
