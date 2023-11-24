use super::document::DocumentMeta;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub(crate) struct Collection {
    pub(crate) name: String,
    pub(crate) documents: RwLock<HashMap<String, Arc<DocumentMeta>>>,
}

impl Collection {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            documents: Default::default(),
        }
    }

    /// Get all documents in an allocated Vec, not using Iterator to keep the lock time minimal.
    pub(crate) async fn docs(&self) -> Vec<Arc<DocumentMeta>> {
        self.documents.read().await.values().cloned().collect()
    }
}
