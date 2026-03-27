use googleapis::google::firestore::v1::{Document, pipeline::Stage};
use itertools::Itertools;

use crate::{GenericDatabaseError, error::Result, unimplemented_collection};

/// Executes the `remove_fields` pipeline stage.
///
/// Iterates across string argument targets dynamically stripping the nodes destructively out of the
/// maps.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    mut docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "remove_fields");
    unimplemented_collection!(options);

    let targets: Vec<_> = args
        .iter()
        .map(|arg| {
            arg.as_field_reference().ok_or_else(|| {
                GenericDatabaseError::invalid_argument(
                    "remove_fields stage arguments must be field references",
                )
            })
        })
        .try_collect()?;

    // Strip keys inline natively
    for doc in &mut docs {
        for target in &targets {
            doc.fields.remove(*target);
        }
    }

    Ok(docs)
}
