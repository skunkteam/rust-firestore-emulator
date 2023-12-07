use super::collection::Collection;
use crate::{
    googleapis::google::firestore::v1::{precondition, Document, Value},
    utils::timestamp_nanos,
};
use prost_types::Timestamp;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{Mutex, OwnedMutexGuard, RwLock},
    time::{error::Elapsed, timeout},
};
use tonic::{Code, Result, Status};
use tracing::{instrument, trace, Level};

const WAIT_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const TRY_LOCK_TIMEOUT: Duration = Duration::from_millis(10);

#[derive(Debug)]
pub struct DocumentMeta {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: String,
    versions: RwLock<Vec<DocumentVersion>>,
    txn_lock: Arc<Mutex<()>>,
    collection: Weak<Collection>,
}

impl DocumentMeta {
    pub fn new(name: String, collection: Weak<Collection>) -> Self {
        Self {
            name,
            versions: Default::default(),
            txn_lock: Default::default(),
            collection,
        }
    }

    pub async fn wait_lock(self: Arc<Self>) -> Result<DocumentGuard> {
        self.lock_with_timeout(WAIT_LOCK_TIMEOUT).await
    }

    pub async fn try_lock(self: Arc<Self>) -> Result<DocumentGuard> {
        self.lock_with_timeout(TRY_LOCK_TIMEOUT).await
    }

    async fn lock_with_timeout(self: Arc<Self>, duration: Duration) -> Result<DocumentGuard> {
        let guard = timeout(duration, Arc::clone(&self.txn_lock).lock_owned())
            .await
            .map_err(|_: Elapsed| Status::aborted("timeout waiting for lock on document"))?;
        Ok(DocumentGuard {
            doc: self,
            _guard: guard,
        })
    }

    pub async fn current_version(&self) -> Option<Arc<StoredDocumentVersion>> {
        self.versions
            .read()
            .await
            .last()
            .and_then(DocumentVersion::stored_document)
    }

    pub async fn version_at_time(
        &self,
        read_time: &Timestamp,
    ) -> Option<Arc<StoredDocumentVersion>> {
        self.versions
            .read()
            .await
            .iter()
            .rfind(|version| timestamp_nanos(version.update_time()) <= timestamp_nanos(read_time))
            .and_then(DocumentVersion::stored_document)
    }

    pub async fn exists(&self) -> bool {
        self.versions
            .read()
            .await
            .last()
            .is_some_and(|version| matches!(version, DocumentVersion::Stored(_)))
    }

    pub async fn create_time(&self) -> Option<Timestamp> {
        self.versions
            .read()
            .await
            .last()
            .and_then(DocumentVersion::create_time)
            .cloned()
    }

    pub async fn last_updated(&self) -> Option<Timestamp> {
        self.versions
            .read()
            .await
            .last()
            .map(DocumentVersion::update_time)
            .cloned()
    }
}

#[derive(Clone, Debug)]
pub enum DocumentVersion {
    Deleted(Arc<DeletedDocumentVersion>),
    Stored(Arc<StoredDocumentVersion>),
}

impl DocumentVersion {
    pub fn name(&self) -> &str {
        match self {
            DocumentVersion::Deleted(ver) => &ver.name,
            DocumentVersion::Stored(ver) => &ver.name,
        }
    }

    pub fn create_time(&self) -> Option<&Timestamp> {
        match self {
            DocumentVersion::Deleted(_) => None,
            DocumentVersion::Stored(ver) => Some(&ver.create_time),
        }
    }

    pub fn update_time(&self) -> &Timestamp {
        match self {
            DocumentVersion::Deleted(ver) => &ver.delete_time,
            DocumentVersion::Stored(ver) => &ver.update_time,
        }
    }

    fn stored_document(&self) -> Option<Arc<StoredDocumentVersion>> {
        match self {
            DocumentVersion::Deleted(_) => None,
            DocumentVersion::Stored(version) => Some(Arc::clone(version)),
        }
    }

    pub fn to_document(&self) -> Option<Document> {
        self.stored_document().map(|version| version.to_document())
    }
}

#[derive(Debug)]
pub struct StoredDocumentVersion {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: String,
    /// The time at which the document was created.
    ///
    /// This value increases monotonically when a document is deleted then
    /// recreated. It can also be compared to values from other documents and
    /// the `read_time` of a query.
    pub create_time: Timestamp,
    /// The time at which the document was last changed.
    ///
    /// This value is initially set to the `create_time` then increases
    /// monotonically with each change to the document. It can also be
    /// compared to values from other documents and the `read_time` of a query.
    pub update_time: Timestamp,
    /// The document's fields.
    ///
    /// The map keys represent field names.
    ///
    /// A simple field name contains only characters `a` to `z`, `A` to `Z`,
    /// `0` to `9`, or `_`, and must not start with `0` to `9`. For example,
    /// `foo_bar_17`.
    ///
    /// Field names matching the regular expression `__.*__` are reserved. Reserved
    /// field names are forbidden except in certain documented contexts. The map
    /// keys, represented as UTF-8, must not exceed 1,500 bytes and cannot be
    /// empty.
    ///
    /// Field paths may be used in other contexts to refer to structured fields
    /// defined here. For `map_value`, the field path is represented by the simple
    /// or quoted field names of the containing fields, delimited by `.`. For
    /// example, the structured field
    /// `"foo" : { map_value: { "x&y" : { string_value: "hello" }}}` would be
    /// represented by the field path `foo.x&y`.
    ///
    /// Within a field path, a quoted field name starts and ends with `` ` `` and
    /// may contain any character. Some characters, including `` ` ``, must be
    /// escaped using a `\`. For example, `` `x&y` `` represents `x&y` and
    /// `` `bak\`tik` `` represents `` bak`tik ``.
    pub fields: HashMap<String, Value>,
}

impl StoredDocumentVersion {
    pub fn to_document(&self) -> Document {
        Document {
            name: self.name.clone(),
            fields: self.fields.clone(),
            create_time: Some(self.create_time.clone()),
            update_time: Some(self.update_time.clone()),
        }
    }
}

#[derive(Debug)]
pub struct DeletedDocumentVersion {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: String,
    /// The time at which the document was deleted.
    pub delete_time: Timestamp,
}

pub struct DocumentGuard {
    doc: Arc<DocumentMeta>,
    _guard: OwnedMutexGuard<()>,
}

impl DocumentGuard {
    pub async fn check_precondition(&self, condition: DocumentPrecondition) -> Result<()> {
        match condition {
            DocumentPrecondition::Exists if !self.doc.exists().await => Err(
                Status::failed_precondition(format!("document not found: {}", self.doc.name)),
            ),
            DocumentPrecondition::NotExists if self.doc.exists().await => {
                Err(Status::already_exists(Code::AlreadyExists.description()))
            }
            DocumentPrecondition::UpdateTime(time)
                if self.doc.last_updated().await.as_ref() != Some(&time) =>
            {
                Err(Status::failed_precondition(format!(
                    "document has different update_time: {}",
                    self.doc.name
                )))
            }
            _ => Ok(()),
        }
    }

    pub async fn current_version(&self) -> Option<Arc<StoredDocumentVersion>> {
        self.doc.current_version().await
    }

    #[instrument(skip_all, fields(
        doc_name = self.doc.name,
        time = display(&update_time),
    ), level = Level::DEBUG)]
    pub async fn add_version(&self, fields: HashMap<String, Value>, update_time: Timestamp) {
        trace!(?fields);
        let create_time = self
            .doc
            .create_time()
            .await
            .unwrap_or_else(|| update_time.clone());
        let version = DocumentVersion::Stored(Arc::new(StoredDocumentVersion {
            name: self.doc.name.clone(),
            create_time,
            update_time,
            fields,
        }));
        self.doc.versions.write().await.push(version.clone());
        self.broadcast_event(DocumentUpdate::new(Arc::clone(&self.doc), version));
    }

    pub async fn delete(&self, delete_time: Timestamp) {
        let version = DocumentVersion::Deleted(Arc::new(DeletedDocumentVersion {
            name: self.doc.name.clone(),
            delete_time,
        }));
        self.doc.versions.write().await.push(version.clone());
        self.broadcast_event(DocumentUpdate::new(Arc::clone(&self.doc), version));
    }

    fn broadcast_event(&self, update: DocumentUpdate) {
        if let Some(collection) = self.doc.collection.upgrade() {
            let _ = collection.events.send(update);
        }
    }
}

pub enum DocumentPrecondition {
    NotExists,
    Exists,
    UpdateTime(Timestamp),
}

impl From<precondition::ConditionType> for DocumentPrecondition {
    fn from(value: precondition::ConditionType) -> Self {
        match value {
            precondition::ConditionType::Exists(false) => DocumentPrecondition::NotExists,
            precondition::ConditionType::Exists(true) => DocumentPrecondition::Exists,
            precondition::ConditionType::UpdateTime(time) => DocumentPrecondition::UpdateTime(time),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DocumentUpdate {
    pub doc_meta: Arc<DocumentMeta>,
    pub new_version: DocumentVersion,
}

impl DocumentUpdate {
    pub fn new(doc_meta: Arc<DocumentMeta>, new_version: DocumentVersion) -> Self {
        Self {
            doc_meta,
            new_version,
        }
    }
}
