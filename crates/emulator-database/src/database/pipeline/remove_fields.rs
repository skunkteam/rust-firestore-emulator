use googleapis::google::firestore::v1::{Document, pipeline::Stage};
use itertools::Itertools;

use crate::{
    GenericDatabaseError, database::field_path::FieldReference, error::Result,
    unimplemented_collection,
};

/// Executes the `remove_fields` pipeline stage.
///
/// Iterates across field reference targets stripping matching paths (or `__name__`) from each
/// document.
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

    let targets: Vec<FieldReference> = args
        .iter()
        .map(|arg| {
            let raw = arg.as_field_reference().ok_or_else(|| {
                GenericDatabaseError::invalid_argument(
                    "remove_fields stage arguments must be field references",
                )
            })?;
            let field_ref: FieldReference = raw.parse()?;
            if let FieldReference::FieldPath(ref path) = field_ref {
                path.validate_non_reserved()?;
            }
            Ok::<_, GenericDatabaseError>(field_ref)
        })
        .try_collect()?;

    for doc in &mut docs {
        for target in &targets {
            match target {
                FieldReference::DocumentName => doc.name.clear(),
                FieldReference::FieldPath(path) => {
                    path.delete_value(&mut doc.fields);
                }
            }
        }
    }

    Ok(docs)
}
