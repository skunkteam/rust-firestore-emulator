use std::sync::Arc;

use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use crate::{
    FirestoreDatabase, GenericDatabaseError,
    database::{query::QueryBuilder, reference::CollectionRef},
    error::Result,
    pipeline::get_exact_expr_args,
    read_consistency::ReadConsistency,
    reference::RootRef,
    unimplemented_collection,
};

pub(super) async fn execute(
    database: &Arc<FirestoreDatabase>,
    root_ref: &RootRef,
    Stage {
        name,
        args,
        options,
    }: Stage,
    read_consistency: ReadConsistency,
) -> Result<Vec<Document>> {
    assert_eq!(name, "collection");
    unimplemented_collection!(options);

    let [arg] = get_exact_expr_args(&args, "collection")?;

    let collection_path = arg.as_reference().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("collection stage expects a ReferenceValue argument")
    })?;

    let collection_ref = CollectionRef::new(
        root_ref.clone(),
        collection_path.strip_prefix('/').unwrap_or(collection_path),
    );

    // The enterprise edition check was successfully validated at the entrance of PipelineEngine.
    let mut query = QueryBuilder::from_collection(collection_ref)
        .enterprise_edition(true)
        .consistency(read_consistency)
        .build()?;

    let (_, docs) = database.run_query(&mut query).await?;

    Ok(docs)
}
