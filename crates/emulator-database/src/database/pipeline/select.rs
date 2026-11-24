use std::collections::HashMap;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::{expressions, parse_projection_paths, single_map_stage};
use crate::error::Result;

/// Executes the `select` pipeline stage.
///
/// Iterates over mappings executing syntax projections across the row and discarding all properties
/// omitting explicitly mapped elements in the evaluated MapValue scope.
pub(super) fn execute(stage: Stage, mut docs: Vec<Document>) -> Result<Vec<Document>> {
    let projection_map = single_map_stage(&stage, "select")?;
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
