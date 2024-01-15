use super::document::DocumentMeta;
use crate::utils::RwLockHashMapExt;
use std::{collections::HashMap, ops::Deref, sync::Arc};
use string_cache::DefaultAtom;
use tokio::sync::RwLock;
use tonic::Result;
use tracing::instrument;

pub struct Collection {
    pub name: DefaultAtom,
    documents: RwLock<HashMap<DefaultAtom, Arc<DocumentMeta>>>,
}

impl Collection {
    #[instrument(skip_all)]
    pub fn new(name: DefaultAtom) -> Self {
        Self {
            name,
            documents: Default::default(),
        }
    }

    pub async fn get_doc(self: &Arc<Self>, name: &DefaultAtom) -> Arc<DocumentMeta> {
        Arc::clone(
            self.documents
                .get_or_insert(name, || {
                    Arc::new(DocumentMeta::new(name.clone(), self.name.clone()))
                })
                .await
                .deref(),
        )
    }

    /// Get all documents in an allocated Vec, not using Iterator to keep the lock time minimal.
    pub async fn docs(&self) -> Vec<Arc<DocumentMeta>> {
        self.documents.read().await.values().cloned().collect()
    }

    /// Checks if this collection has a document with a current version.
    pub async fn has_doc(&self) -> Result<bool> {
        for doc in self.documents.read().await.values() {
            if doc.read().await?.exists() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
