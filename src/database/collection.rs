use super::document::{DocumentMeta, DocumentUpdate};
use crate::utils::RwLockHashMapExt;
use std::{collections::HashMap, ops::Deref, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tracing::instrument;

pub struct Collection {
    pub name: String,
    documents: RwLock<HashMap<String, Arc<DocumentMeta>>>,
    pub events: broadcast::Sender<DocumentUpdate>,
}

impl Collection {
    #[instrument(skip_all)]
    pub fn new(name: String) -> Self {
        Self {
            name,
            documents: Default::default(),
            events: broadcast::channel(512).0,
        }
    }

    pub async fn get_doc(self: &Arc<Self>, name: &str) -> Arc<DocumentMeta> {
        Arc::clone(
            self.documents
                .get_or_insert(name, || {
                    DocumentMeta::new(name.into(), Arc::downgrade(self)).into()
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
    pub async fn has_doc(&self) -> bool {
        for doc in self.documents.read().await.values() {
            if doc.exists().await {
                return true;
            }
        }
        false
    }
}
