use std::{
    collections::HashMap,
    fmt::{self, Debug},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use futures::Future;
use googleapis::google::{
    firestore::v1::{precondition, Document, Value},
    protobuf::Timestamp,
};
use string_cache::DefaultAtom;
use tokio::{
    sync::{
        oneshot, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock,
        RwLockReadGuard, Semaphore,
    },
    time::{error::Elapsed, timeout},
};
use tonic::{Code, Result, Status};
use tracing::{instrument, trace, Level};

use super::ReadConsistency;

const WAIT_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DocumentMeta {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: DefaultAtom,
    /// The collection name of the document, i.e. the full name of the document minus the last
    /// component.
    pub collection_name: DefaultAtom,
    contents: Arc<RwLock<DocumentContents>>,
    write_permit_shop: Arc<Semaphore>,
}

impl Debug for DocumentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentMeta")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DocumentMeta {
    pub fn new(name: DefaultAtom, collection_name: DefaultAtom) -> Self {
        Self {
            contents: Arc::new(RwLock::new(DocumentContents::new(
                name.clone(),
                collection_name.clone(),
            ))),
            name,
            collection_name,
            write_permit_shop: Arc::new(Semaphore::new(1)),
        }
    }

    pub async fn read(&self) -> Result<DocumentContentsReadGuard> {
        lock_timeout(self.contents.read()).await
    }

    pub async fn read_owned(self: &Arc<Self>) -> Result<OwnedDocumentContentsReadGuard> {
        let (mut tx, rx) = oneshot::channel();

        let write_permit_shop = Arc::clone(&self.write_permit_shop);
        tokio::spawn(async {
            tokio::select! {
                Ok(permit) = write_permit_shop.acquire_owned() => {
                    let _: Result<_, _> = tx.send(permit);
                }

                _ = tx.closed() => ()
            }
        });

        Ok(OwnedDocumentContentsReadGuard {
            meta: Arc::clone(self),
            guard: lock_timeout(Arc::clone(&self.contents).read_owned()).await?,
            write_permit: rx,
        })
    }

    async fn owned_write(&self) -> Result<OwnedDocumentContentsWriteGuard> {
        lock_timeout(Arc::clone(&self.contents).write_owned()).await
    }
}

pub struct DocumentContents {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: DefaultAtom,
    /// The collection name of the document, i.e. the full name of the document minus the last
    /// component.
    pub collection_name: DefaultAtom,
    versions: Vec<DocumentVersion>,
}

impl DocumentContents {
    pub fn new(name: DefaultAtom, collection_name: DefaultAtom) -> Self {
        Self {
            name,
            collection_name,
            versions: Default::default(),
        }
    }

    pub fn current_version(&self) -> Option<&Arc<StoredDocumentVersion>> {
        self.versions
            .last()
            .and_then(DocumentVersion::stored_document)
    }

    pub fn version_at_time(&self, read_time: &Timestamp) -> Option<&Arc<StoredDocumentVersion>> {
        self.versions
            .iter()
            .rfind(|version| (version.update_time()) <= (read_time))
            .and_then(DocumentVersion::stored_document)
    }

    pub fn version_for_consistency(
        &self,
        consistency: &ReadConsistency,
    ) -> Result<Option<&Arc<StoredDocumentVersion>>> {
        Ok(match consistency {
            ReadConsistency::Default => self.current_version(),
            ReadConsistency::ReadTime(time) => self.version_at_time(time),
            ReadConsistency::Transaction(_) => self.current_version(),
        })
    }

    pub fn exists(&self) -> bool {
        self.versions
            .last()
            .is_some_and(|version| matches!(version, DocumentVersion::Stored(_)))
    }

    pub fn create_time(&self) -> Option<Timestamp> {
        self.versions
            .last()
            .and_then(DocumentVersion::create_time)
            .cloned()
    }

    pub fn last_updated(&self) -> Option<Timestamp> {
        self.versions
            .last()
            .map(DocumentVersion::update_time)
            .cloned()
    }

    pub fn check_precondition(&self, condition: DocumentPrecondition) -> Result<()> {
        match condition {
            DocumentPrecondition::Exists if !self.exists() => Err(Status::failed_precondition(
                format!("document not found: {}", self.name),
            )),
            DocumentPrecondition::NotExists if self.exists() => {
                Err(Status::already_exists(Code::AlreadyExists.description()))
            }
            DocumentPrecondition::UpdateTime(time)
                if self.last_updated().as_ref() != Some(&time) =>
            {
                Err(Status::failed_precondition(format!(
                    "document has different update_time: {}",
                    self.name
                )))
            }
            _ => Ok(()),
        }
    }

    #[instrument(skip_all, fields(
        doc_name = self.name.deref(),
        time = display(&update_time),
    ), level = Level::DEBUG)]
    pub async fn add_version(
        &mut self,
        fields: HashMap<String, Value>,
        update_time: Timestamp,
    ) -> DocumentVersion {
        trace!(?fields);
        let create_time = self.create_time().unwrap_or_else(|| update_time.clone());
        let version = DocumentVersion::Stored(Arc::new(StoredDocumentVersion {
            name: self.name.clone(),
            collection_name: self.collection_name.clone(),
            create_time,
            update_time,
            fields,
        }));
        self.versions.push(version.clone());
        version
    }

    pub async fn delete(&mut self, delete_time: Timestamp) -> DocumentVersion {
        let version = DocumentVersion::Deleted(Arc::new(DeletedDocumentVersion {
            name: self.name.clone(),
            collection_name: self.collection_name.clone(),
            delete_time,
        }));
        self.versions.push(version.clone());
        version
    }
}

pub type DocumentContentsReadGuard<'a> = RwLockReadGuard<'a, DocumentContents>;

pub struct OwnedDocumentContentsReadGuard {
    meta: Arc<DocumentMeta>,
    guard: OwnedRwLockReadGuard<DocumentContents>,
    write_permit: oneshot::Receiver<OwnedSemaphorePermit>,
}

pub type OwnedDocumentContentsWriteGuard = OwnedRwLockWriteGuard<DocumentContents>;

impl OwnedDocumentContentsReadGuard {
    #[instrument(skip_all, err)]
    pub async fn upgrade(self) -> Result<OwnedDocumentContentsWriteGuard> {
        let check_time = self.guard.last_updated();
        let OwnedDocumentContentsReadGuard {
            meta,
            guard,
            write_permit,
        } = self;
        drop(guard);
        let write_permit = lock_timeout(write_permit).await?.unwrap();
        let owned_rw_lock_write_guard = lock_timeout(meta.owned_write()).await??;
        drop(write_permit);
        if check_time == owned_rw_lock_write_guard.last_updated() {
            Ok(owned_rw_lock_write_guard)
        } else {
            Err(Status::aborted("contention"))
        }
    }
}

impl Deref for OwnedDocumentContentsReadGuard {
    type Target = DocumentContents;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

#[derive(Clone, Debug)]
pub enum DocumentVersion {
    Deleted(Arc<DeletedDocumentVersion>),
    Stored(Arc<StoredDocumentVersion>),
}

impl DocumentVersion {
    pub fn name(&self) -> &DefaultAtom {
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

    pub fn stored_document(&self) -> Option<&Arc<StoredDocumentVersion>> {
        match self {
            DocumentVersion::Deleted(_) => None,
            DocumentVersion::Stored(version) => Some(version),
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
    pub name: DefaultAtom,
    /// The collection name of the document, i.e. the full name of the document minus the last
    /// component.
    pub collection_name: DefaultAtom,
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
            name: self.name.to_string(),
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
    pub name: DefaultAtom,
    /// The collection name of the document, i.e. the full name of the document minus the last
    /// component.
    pub collection_name: DefaultAtom,
    /// The time at which the document was deleted.
    pub delete_time: Timestamp,
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

async fn lock_timeout<F: Future>(future: F) -> Result<F::Output> {
    timeout(WAIT_LOCK_TIMEOUT, future)
        .await
        .map_err(|_: Elapsed| Status::aborted("timeout waiting for lock on document"))
}
