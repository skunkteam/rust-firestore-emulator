use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::{expressions, parse_projection_paths, single_map_stage};
use crate::error::Result;

/// Executes the `add_fields` pipeline stage.
///
/// This stage evaluates expressions mapped by aliases and appends or overwrites them into the
/// existing row stream.
pub(super) fn execute(stage: Stage, docs: Vec<Document>) -> Result<Vec<Document>> {
    let projection_map = single_map_stage(&stage, "add_fields")?;
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
