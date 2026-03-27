use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use crate::{
    GenericDatabaseError, error::Result, pipeline::get_exact_expr_args, unimplemented_collection,
};

pub(super) fn execute(
    Stage {
        name,
        args,
        options,
    }: Stage,
    mut docs: Vec<Document>,
) -> Result<Vec<Document>> {
    assert_eq!(name, "limit");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "limit")?;

    let limit = arg.as_integer().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("limit stage expects an IntegerValue")
    })?;

    let limit: usize = limit
        .try_into()
        .map_err(|_| GenericDatabaseError::invalid_argument("Limit must be >= 0"))?;

    docs.truncate(limit);
    Ok(docs)
}
