use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::{expressions, parse_projection_paths};
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
    let targets = parse_projection_paths(projection_map)?;

    docs.into_iter()
        .map(|mut doc| {
            let mut add_fields_evals = Vec::with_capacity(targets.len());

            for (path, expr) in &targets {
                let evaluated_val = expressions::evaluate(expr, &doc)?;
                add_fields_evals.push((path, evaluated_val));
            }

            for (path, val) in add_fields_evals {
                path.set_value(&mut doc.fields, val);
            }

            Ok(doc)
        })
        .collect()
}
