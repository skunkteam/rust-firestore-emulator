use std::{collections::HashMap, ops::Deref, sync::Arc};

use string_cache::DefaultAtom;
use tokio::sync::RwLock;
use tracing::{instrument, Level};

use super::{
    document::DocumentMeta,
    reference::{CollectionRef, DocumentRef},
};
use crate::{error::Result, utils::RwLockHashMapExt, FirestoreProject};

#[derive(Debug)]
pub(crate) struct Collection {
    project:   &'static FirestoreProject,
    pub name:  CollectionRef,
    documents: RwLock<HashMap<DefaultAtom, Arc<DocumentMeta>>>,
}

impl Collection {
    #[instrument(level = Level::DEBUG, skip_all)]
    pub fn new(project: &'static FirestoreProject, name: CollectionRef) -> Self {
        Self {
            project,
            name,
            documents: Default::default(),
        }
    }

    pub(crate) async fn get_doc(self: &Arc<Self>, name: &DocumentRef) -> Arc<DocumentMeta> {
        debug_assert_eq!(self.name, name.collection_ref);
        Arc::clone(
            self.documents
                .get_or_insert(&name.document_id, || {
                    Arc::new(DocumentMeta::new(self.project, name.clone()))
                })
                .await
                .deref(),
        )
    }

    /// Get all documents in an allocated Vec, not using Iterator to keep the lock time minimal.
    pub(crate) async fn docs(&self) -> Vec<Arc<DocumentMeta>> {
        self.documents.read().await.values().cloned().collect()
    }

    /// Checks if this collection has a document with a current version.
    pub(crate) async fn has_doc(&self) -> Result<bool> {
        for doc in self.documents.read().await.values() {
            if doc.read().await?.exists() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
