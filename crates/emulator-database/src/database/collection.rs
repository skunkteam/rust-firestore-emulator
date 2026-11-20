use std::{collections::HashMap, ops::Deref, sync::Arc};

use string_cache::DefaultAtom;
use tokio::sync::RwLock;
use tracing::{Level, instrument};

use super::{
    document::DocumentMeta,
    reference::{CollectionRef, DocumentRef},
};
use crate::{error::Result, utils::RwLockHashMapExt};

#[derive(Debug)]
pub(crate) struct Collection {
    pub name:  CollectionRef,
    documents: RwLock<HashMap<DefaultAtom, Arc<DocumentMeta>>>,
}

impl Collection {
    #[instrument(level = Level::DEBUG, skip_all)]
    pub(super) fn new(name: CollectionRef) -> Self {
        Self {
            name,
            documents: Default::default(),
        }
    }

    pub(crate) async fn get_doc(self: &Arc<Self>, name: &DocumentRef) -> Arc<DocumentMeta> {
        debug_assert_eq!(self.name, name.collection_ref);
        Arc::clone(
            self.documents
                .get_or_insert(&name.document_id, || {
                    Arc::new(DocumentMeta::new(name.clone()))
                })
                .await
                .deref(),
        )
    }

    pub(crate) async fn peek_doc(&self, name: &DocumentRef) -> Option<Arc<DocumentMeta>> {
        debug_assert_eq!(self.name, name.collection_ref);
        self.documents
            .read()
            .await
            .get(&name.document_id)
            .map(Arc::clone)
    }

    /// Get all documents in an allocated Vec, not using Iterator to keep the lock time minimal.
    pub(crate) async fn docs(&self) -> Vec<Arc<DocumentMeta>> {
        self.documents.read().await.values().cloned().collect()
    }

    /// Checks if this collection has a document with a current version without entering lock wait
    /// queues.
    pub(crate) async fn has_doc(&self) -> Result<bool> {
        Ok(self
            .documents
            .read()
            .await
            .values()
            .any(|doc| doc.read_latest_uncontended(|c| c.exists())))
    }
}
