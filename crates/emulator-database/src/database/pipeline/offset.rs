use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use crate::{
    GenericDatabaseError, error::Result, pipeline::get_exact_expr_args, unimplemented_collection,
};

/// Executes the `offset` pipeline stage.
///
/// This stage skips the first N documents in the pipeline stream and yields the remainder.
pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    mut docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "offset");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "offset")?;

    let offset = arg.as_integer().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("offset stage expects an IntegerValue")
    })?;

    let offset: usize = offset
        .try_into()
        .map_err(|_| GenericDatabaseError::invalid_argument("Offset must be >= 0"))?;

    if offset >= docs.len() {
        docs.clear();
    } else {
        docs.drain(0..offset);
    }

    Ok(docs)
}
