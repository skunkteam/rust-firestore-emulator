use std::sync::Arc;

use googleapis::google::firestore::v1::{Document, StructuredPipeline};

use crate::{
    database::reference::Ref, error::Result, read_consistency::ReadConsistency, FirestoreDatabase,
    GenericDatabaseError,
};

mod collection;

pub async fn execute(
    database: &Arc<FirestoreDatabase>,
    root_ref: Ref,
    pipeline: StructuredPipeline,
    read_consistency: ReadConsistency,
) -> Result<Vec<Document>> {
    // These operators are only supported in Enterprise Edition. 
    // We enforce this rule universally across the pipeline engine very early.
    if !database.enterprise_edition() {
        return Err(GenericDatabaseError::failed_precondition(
            "Pipeline APIs are only available in Enterprise Edition.",
        ));
    }

    let pipeline_ops = pipeline.pipeline.ok_or_else(|| {
        GenericDatabaseError::invalid_argument("missing pipeline operations")
    })?;

    if pipeline_ops.stages.is_empty() {
        return Err(GenericDatabaseError::invalid_argument(
            "pipeline stages cannot be empty",
        ));
    }

    let mut current_docs = vec![];
    let mut first = true;

    for stage in pipeline_ops.stages {
        if first {
            first = false;
            // Basic pipeline support currently expects the first stage to be `collection`
            if stage.name == "collection" {
                current_docs = collection::execute(database, &root_ref, &stage, read_consistency).await?;
            } else {
                return Err(GenericDatabaseError::not_implemented(format!(
                    "pipeline stage {} is not supported as first stage yet",
                    stage.name
                )));
            }
        } else {
            return Err(GenericDatabaseError::not_implemented(format!(
                "pipeline stage {} is not supported yet",
                stage.name
            )));
        }
    }

    Ok(current_docs)
}
