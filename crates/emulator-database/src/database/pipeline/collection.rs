use std::sync::Arc;

use googleapis::google::{
    firestore::v1::{Document, pipeline::Stage},
    protobuf::Timestamp,
};

use super::single_arg_stage;
use crate::{
    FirestoreDatabase, GenericDatabaseError,
    database::{query::QueryBuilder, reference::CollectionRef},
    error::Result,
    read_consistency::ReadConsistency,
    reference::RootRef,
};

pub(super) async fn execute(
    database: &Arc<FirestoreDatabase>,
    root_ref: &RootRef,
    stage: Stage,
    read_consistency: ReadConsistency,
) -> Result<(Timestamp, Vec<Document>)> {
    let arg = single_arg_stage(&stage, "collection")?;

    let collection_path = arg.as_reference().ok_or_else(|| {
        GenericDatabaseError::invalid_argument("collection stage expects a ReferenceValue argument")
    })?;

    let rel_path = collection_path.strip_prefix('/').unwrap_or(collection_path);
    if rel_path.is_empty() {
        return Err(GenericDatabaseError::InvalidReference(
            "collection",
            collection_path.to_string(),
        ));
    }
    let collection_ref: CollectionRef = format!("{root_ref}/{rel_path}").parse()?;

    // The enterprise edition check was successfully validated at the entrance of PipelineEngine.
    let mut query = QueryBuilder::from_collection(collection_ref)
        .enterprise_edition(true)
        .consistency(read_consistency)
        .build()?;

    database.run_query(&mut query).await
}
