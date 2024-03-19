use std::{collections::HashMap, ops::Deref, sync::Arc};

use string_cache::DefaultAtom;
use tokio::sync::RwLock;
use tracing::{instrument, Level};

use super::{
    document::DocumentMeta,
    reference::{CollectionRef, DocumentRef},
};
use crate::{error::Result, utils::RwLockHashMapExt, FirestoreConfig};

pub struct Collection {
    config:    &'static FirestoreConfig,
    pub name:  CollectionRef,
    documents: RwLock<HashMap<DefaultAtom, Arc<DocumentMeta>>>,
}

impl Collection {
    #[instrument(level = Level::TRACE, skip_all)]
    pub fn new(config: &'static FirestoreConfig, name: CollectionRef) -> Self {
        Self {
            config,
            name,
            documents: Default::default(),
        }
    }

    pub async fn get_doc(self: &Arc<Self>, name: &DocumentRef) -> Arc<DocumentMeta> {
        debug_assert_eq!(self.name, name.collection_ref);
        Arc::clone(
            self.documents
                .get_or_insert(&name.document_id, || {
                    Arc::new(DocumentMeta::new(self.config, name.clone()))
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
