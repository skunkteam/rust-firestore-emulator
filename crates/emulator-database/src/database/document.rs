use std::{
    collections::HashMap,
    fmt::{self, Debug},
    ops::Deref,
    pin::pin,
    sync::Arc,
    time::Duration,
};

use fifo_rwlock::{FifoRwLock, OwnedReadGuard, OwnedWriteGuard, ReadGuard};
use futures::Future;
use googleapis::google::{
    firestore::v1::{precondition, Document, Value},
    protobuf::Timestamp,
};
use tokio::time::{error::Elapsed, sleep, timeout};
use tracing::{debug, instrument, trace, warn, Level};

use super::reference::DocumentRef;
use crate::{error::Result, FirestoreProject, GenericDatabaseError};

/// DocumentMeta is the representation of one document in Firestore. Or more accurately, one
/// document name or "spot" if you will, the document itself may or may not exist at any point in
/// time.
///
/// In order to write to a document, you can either upgrade an owned read lock or acquire a write
/// lock immediately. The moment you get a read lock, you are put in the queue for a write permit,
/// making sure that clients are rewarded a write lock based on the time they started reading the
/// document and not the time they expressed interest in writing to the document. This is relevant
/// for transactions that lock documents early on, if they wouldn't have the earliest rights to
/// write then we would have far more contention during transactions.
pub(crate) struct DocumentMeta {
    project:  &'static FirestoreProject,
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: DocumentRef,
    /// The actual contents (full history), protected by a RWLock.
    contents: Arc<FifoRwLock<DocumentContents>>,
}

impl Debug for DocumentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentMeta")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DocumentMeta {
    pub(crate) fn new(project: &'static FirestoreProject, name: DocumentRef) -> Self {
        Self {
            project,
            contents: Arc::new(FifoRwLock::new(DocumentContents::new(name.clone()))),
            name,
        }
    }

    /// Get a quick temporary read lock.
    pub(crate) async fn read(&self) -> Result<DocumentContentsReadGuard> {
        lock_timeout(
            self.contents.read(),
            self.project.timeouts.read,
            "read lock",
            &self.name,
        )
        .await
    }

    /// Get a read lock that can be held indefinitely. Can be upgraded to a write lock if needed.
    pub(crate) async fn read_owned(self: &Arc<Self>) -> Result<OwnedDocumentContentsReadGuard> {
        Ok(OwnedDocumentContentsReadGuard {
            project: self.project,
            meta:    Arc::clone(self),
            guard:   lock_timeout(
                self.contents.read_owned(),
                self.project.timeouts.read,
                "read lock",
                &self.name,
            )
            .await?,
        })
    }

    /// Get a write lock asap without any consistency guarantees.
    pub(crate) async fn write_owned(self: &Arc<Self>) -> Result<OwnedDocumentContentsWriteGuard> {
        // Using maximum timeout here, because there is no chance of failing because of contention,
        // the only way this fails is because of a timeout.

        let guard = lock_timeout(
            self.contents.write_owned(),
            self.project.timeouts.write,
            "write lock",
            &self.name,
        )
        .await?;

        Ok(OwnedDocumentContentsWriteGuard {
            guard,
            acquire_time: Timestamp::now(),
        })
    }
}

/// DocumentContents maintains the history of versions of a single document (document-name), this
/// includes "deleted versions". Note that all mutating methods are defined on
/// `OwnedDocumentContentsWriteGuard`, because these methods check whether the specified update-time
/// is on or after the time of acquiring the lock.
#[derive(Debug)]
pub(crate) struct DocumentContents {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub(crate) name: DocumentRef,
    versions: Vec<DocumentVersion>,
}

impl DocumentContents {
    pub(crate) fn new(name: DocumentRef) -> Self {
        Self {
            name,
            versions: Default::default(),
        }
    }

    pub(crate) fn current_version(&self) -> Option<&Arc<StoredDocumentVersion>> {
        self.versions
            .last()
            .and_then(DocumentVersion::stored_document)
    }

    pub(crate) fn version_at_time(
        &self,
        read_time: Timestamp,
    ) -> Option<&Arc<StoredDocumentVersion>> {
        self.versions
            .iter()
            .rfind(|version| (version.update_time()) <= (read_time))
            .and_then(DocumentVersion::stored_document)
    }

    pub(crate) fn exists(&self) -> bool {
        self.versions
            .last()
            .is_some_and(|version| matches!(version, DocumentVersion::Stored(_)))
    }

    pub(crate) fn create_time(&self) -> Option<Timestamp> {
        self.versions.last().and_then(DocumentVersion::create_time)
    }

    pub(crate) fn last_updated(&self) -> Option<Timestamp> {
        self.versions.last().map(DocumentVersion::update_time)
    }

    pub(crate) fn check_precondition(&self, condition: DocumentPrecondition) -> Result<()> {
        match condition {
            DocumentPrecondition::Exists if !self.exists() => {
                Err(GenericDatabaseError::failed_precondition(format!(
                    "document not found: {}",
                    self.name
                )))
            }
            DocumentPrecondition::NotExists if self.exists() => {
                Err(GenericDatabaseError::already_exists(format!(
                    "document already exists: {}",
                    self.name
                )))
            }
            DocumentPrecondition::UpdateTime(time)
                if self.last_updated().as_ref() != Some(&time) =>
            {
                Err(GenericDatabaseError::failed_precondition(format!(
                    "document has different update_time: {}",
                    self.name
                )))
            }
            _ => Ok(()),
        }
    }
}

pub(crate) type DocumentContentsReadGuard<'a> = ReadGuard<'a, DocumentContents>;

#[derive(Debug)]
pub(crate) struct OwnedDocumentContentsReadGuard {
    project: &'static FirestoreProject,
    meta:    Arc<DocumentMeta>,
    guard:   OwnedReadGuard<DocumentContents>,
}

impl OwnedDocumentContentsReadGuard {
    /// Try to upgrade this owned read lock to an owned write lock. Can result in a timeout or a
    /// contention error (GRPC Aborted).
    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub(crate) async fn upgrade(self) -> Result<OwnedDocumentContentsWriteGuard> {
        debug!(name = %self.meta.name);
        let OwnedDocumentContentsReadGuard {
            project,
            meta,
            guard,
        } = self;

        // Remember what the current last version is of the document, and then release the read
        // lock. We know that this current last version is also the version that our consumer has
        // seen, because they had a read lock all along.
        let check_time = guard.last_updated();

        let guard = lock_timeout(
            guard.upgrade(),
            project.timeouts.write,
            "write lock",
            &meta.name,
        )
        .await?;

        // Make sure nobody touched our document in the mean time, otherwise we have to report
        // contention to our client.
        if check_time != guard.last_updated() {
            return Err(GenericDatabaseError::aborted("contention"));
        }

        Ok(OwnedDocumentContentsWriteGuard {
            guard,
            acquire_time: Timestamp::now(),
        })
    }
}

impl Deref for OwnedDocumentContentsReadGuard {
    type Target = DocumentContents;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

#[derive(Debug)]
pub(crate) struct OwnedDocumentContentsWriteGuard {
    guard: OwnedWriteGuard<DocumentContents>,
    pub(crate) acquire_time: Timestamp,
}

impl OwnedDocumentContentsWriteGuard {
    /// Add a new version with the given `update_time`, if the given `fields` are identical to the
    /// last version, it will return [`Err`] with the update time ([`Timestamp`]) of the last
    /// version.
    #[instrument(level = Level::DEBUG, skip_all, fields(
        doc_name = %self.name,
        time = display(&update_time),
    ))]
    pub(crate) async fn maybe_update(
        &mut self,
        fields: HashMap<String, Value>,
        update_time: Timestamp,
    ) -> Result<(Timestamp, Option<DocumentVersion>)> {
        trace!(?fields);
        // Check if the fields are exactly equal to the last version, in that case, do not generate
        // a new version.
        if let Some(last) = self
            .versions
            .last()
            .and_then(DocumentVersion::stored_document)
        {
            trace!("fields are equal to previous version");
            if last.fields == fields {
                return Ok((last.update_time, None));
            }
        }
        let create_time = self.create_time().unwrap_or(update_time);
        let version = DocumentVersion::Stored(Arc::new(StoredDocumentVersion {
            name: self.name.clone(),
            create_time,
            update_time,
            fields,
        }));
        self.maybe_add_version(&version)?;
        return Ok((update_time, Some(version)));
    }

    pub(crate) async fn delete(&mut self, delete_time: Timestamp) -> Result<DocumentVersion> {
        let version = DocumentVersion::Deleted(Arc::new(DeletedDocumentVersion {
            name: self.name.clone(),
            delete_time,
        }));
        self.maybe_add_version(&version)?;
        Ok(version)
    }

    fn maybe_add_version(&mut self, version: &DocumentVersion) -> Result<()> {
        if version.update_time() < self.acquire_time {
            return Err(GenericDatabaseError::internal(
                "lock should always be acquired before claiming write/update time",
            ));
        }
        if let Some(last) = self.guard.versions.last_mut() {
            // This should never fail because of the check against the acquire time above.
            assert!(
                last.update_time() <= version.update_time(),
                "update time should be monotonically increasing"
            );
            // In transactions, we may get multiple updates to the same document, this should result
            // in only one added version.
            if last.update_time() == version.update_time() {
                last.clone_from(version);
                return Ok(());
            }
        }
        self.guard.versions.push(version.clone());
        Ok(())
    }
}

impl Deref for OwnedDocumentContentsWriteGuard {
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
    pub fn name(&self) -> &DocumentRef {
        match self {
            DocumentVersion::Deleted(ver) => &ver.name,
            DocumentVersion::Stored(ver) => &ver.name,
        }
    }

    pub fn create_time(&self) -> Option<Timestamp> {
        match self {
            DocumentVersion::Deleted(_) => None,
            DocumentVersion::Stored(ver) => Some(ver.create_time),
        }
    }

    pub fn update_time(&self) -> Timestamp {
        match self {
            DocumentVersion::Deleted(ver) => ver.delete_time,
            DocumentVersion::Stored(ver) => ver.update_time,
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
    pub name: DocumentRef,
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
            create_time: Some(self.create_time),
            update_time: Some(self.update_time),
        }
    }
}

#[derive(Debug)]
pub struct DeletedDocumentVersion {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: DocumentRef,
    /// The time at which the document was deleted.
    pub delete_time: Timestamp,
}

pub(crate) enum DocumentPrecondition {
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

async fn lock_timeout<F: Future>(
    future: F,
    time: Duration,
    kind: &str,
    doc_name: &DocumentRef,
) -> Result<F::Output> {
    let future_with_timeout = async {
        timeout(time, future).await.map_err(|_: Elapsed| {
            GenericDatabaseError::aborted("timeout waiting for lock on document")
        })
    };
    let mut future_with_timeout = pin!(future_with_timeout);
    tokio::select! {
        result = &mut future_with_timeout => return result,
        _ = sleep(Duration::from_secs(1)) => warn!("waiting more than 1 second on a {kind} for {doc_name}"),
    }
    future_with_timeout.await
}
