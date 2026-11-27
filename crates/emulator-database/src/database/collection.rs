use std::{collections::HashMap, ops::Deref, sync::Arc};

use googleapis::google::protobuf::Timestamp;
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

    pub(crate) fn from_docs(
        name: CollectionRef,
        documents: HashMap<DefaultAtom, Arc<DocumentMeta>>,
    ) -> Self {
        debug_assert!(
            documents
                .iter()
                .all(|(id, doc)| *id == doc.name.document_id && doc.name.collection_ref == name)
        );
        Self {
            name,
            documents: RwLock::new(documents),
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

    #[cfg(test)]
    pub(super) async fn documents_write(
        &self,
    ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<DefaultAtom, Arc<DocumentMeta>>> {
        self.documents.write().await
    }

    /// Checks if this collection has a document existing at `time` without acquiring `LockTable`
    /// locks.
    pub(crate) async fn has_doc_at(&self, time: Timestamp) -> bool {
        self.documents
            .read()
            .await
            .values()
            .any(|doc| doc.read_mvcc_at(time).is_some())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use googleapis::google::{firestore::v1::Value, protobuf::Timestamp};
    use string_cache::DefaultAtom;

    use super::Collection;
    use crate::{
        database::document::DocumentMeta,
        reference::{CollectionRef, DocumentRef},
    };

    #[tokio::test]
    async fn from_docs_populates_empty_and_non_empty_collections() {
        let col_ref: CollectionRef = "projects/p/databases/(default)/documents/items"
            .parse()
            .unwrap();

        let empty = Collection::from_docs(col_ref.clone(), HashMap::new());
        assert_eq!(empty.name, col_ref);
        assert!(!empty.has_doc().await.unwrap());
        assert!(empty.docs().await.is_empty());

        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/items/d1"
            .parse()
            .unwrap();
        let ts = Timestamp {
            seconds: 123,
            nanos:   456,
        };
        let meta = Arc::new(DocumentMeta::from_imported_version(
            doc_ref.clone(),
            HashMap::from([("v".to_string(), Value::integer(7))]),
            ts,
        ));
        let populated = Collection::from_docs(
            col_ref.clone(),
            HashMap::from([(DefaultAtom::from("d1"), Arc::clone(&meta))]),
        );

        assert!(populated.has_doc().await.unwrap());
        assert_eq!(populated.docs().await.len(), 1);
        let peeked = populated.peek_doc(&doc_ref).await.unwrap();
        assert!(Arc::ptr_eq(&peeked, &meta));
        let snap = peeked.read_mvcc_at(ts).unwrap();
        assert_eq!(snap.fields["v"].as_integer(), Some(7));
    }
}
