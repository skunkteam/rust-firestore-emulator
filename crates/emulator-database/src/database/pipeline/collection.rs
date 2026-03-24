use std::sync::Arc;

use googleapis::google::firestore::v1::{
    structured_query::CollectionSelector, value::ValueType, Document,
};

use crate::{
    database::query::QueryBuilder,
    database::reference::{CollectionRef, Ref},
    error::Result,
    read_consistency::ReadConsistency,
    FirestoreDatabase, GenericDatabaseError,
};

pub(super) async fn execute(
    database: &Arc<FirestoreDatabase>,
    root_ref: &Ref,
    stage: &googleapis::google::firestore::v1::pipeline::Stage,
    read_consistency: ReadConsistency,
) -> Result<Vec<Document>> {
    let arg = stage.args.first().and_then(|v| v.value_type.as_ref()).ok_or_else(|| {
        GenericDatabaseError::invalid_argument("collection stage expects a ReferenceValue argument")
    })?;

    let collection_path = match arg {
        ValueType::ReferenceValue(rel_path) => rel_path,
        _ => {
            return Err(GenericDatabaseError::invalid_argument(
                "collection stage expects a ReferenceValue argument",
            ))
        }
    };

    // Replace the format-based string concatenation parsing with a direct Ref parse, or falling
    // back to extracting a CollectionRef for relative paths typically sent by tests.
    let collection_ref = if let Ok(parsed) = collection_path.parse::<Ref>() {
        parsed
    } else {
        let clean_path = collection_path.strip_prefix('/').unwrap_or(collection_path);
        Ref::Collection(CollectionRef::new(root_ref.root().clone(), clean_path))
    };

    let col_ref = collection_ref.as_collection().ok_or_else(|| {
        GenericDatabaseError::invalid_argument(
            "collection pipeline stage must refer to a collection",
        )
    })?;

    let full_path = &col_ref.collection_id;
    let leaf_collection_id = full_path
        .rsplit_once('/')
        .map(|(_, leaf)| leaf)
        .unwrap_or(full_path)
        .to_string();

    let parent_ref = col_ref.parent();

    // The enterprise edition check was successfully validated at the entrance of PipelineEngine.
    let mut query = QueryBuilder::from(
        parent_ref,
        vec![CollectionSelector {
            collection_id: leaf_collection_id,
            all_descendants: false,
        }],
    )
    .enterprise_edition(true)
    .consistency(read_consistency)
    .build()?;

    let (_, docs) = database.run_query(&mut query).await?;
    
    Ok(docs)
}
