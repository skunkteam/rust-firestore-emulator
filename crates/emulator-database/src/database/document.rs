use std::{
    collections::{BTreeMap, HashMap},
    fmt::{self, Debug},
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use googleapis::google::{
    firestore::v1::{Document, Value, precondition},
    protobuf::Timestamp,
};

use super::{
    lock::{CommitSeal, ReadLease, WriteLease},
    reference::DocumentRef,
};
use crate::{
    GenericDatabaseError,
    error::{CommitError, Result},
};

#[derive(Debug)]
struct LockableDocument(RwLock<DocumentContents>);

impl LockableDocument {
    fn new() -> Self {
        Self(RwLock::new(DocumentContents::new()))
    }

    fn read(&self) -> RwLockReadGuard<'_, DocumentContents> {
        self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, DocumentContents> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// DocumentMeta is the representation of one document in Firestore. Or more accurately, one
/// document name or "spot" if you will; the document itself may or may not exist at any point in
/// time.
///
/// Physical access to [`DocumentContents`] is gated behind logical Wound-Wait lock capabilities
/// ([`ReadLease`], [`WriteLease`], [`CommitSeal`]) or queue-free MVCC / uncontended physical
/// snapshot accessors.
pub(crate) struct DocumentMeta {
    /// The resource name of the document, for example
    /// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
    pub name: DocumentRef,
    /// The actual contents (full history), protected by a synchronous RwLock.
    contents: LockableDocument,
}

impl Debug for DocumentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentMeta")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl DocumentMeta {
    pub(crate) fn new(name: DocumentRef) -> Self {
        Self {
            contents: LockableDocument::new(),
            name,
        }
    }

    pub(crate) fn read_with<R>(
        &self,
        lease: &ReadLease,
        f: impl FnOnce(&DocumentContents) -> R,
    ) -> Result<R> {
        if lease.doc_ref() != &self.name {
            return Err(GenericDatabaseError::internal(format!(
                "read lease for {} cannot access {}",
                lease.doc_ref(),
                self.name
            )));
        }
        let contents = self.contents.read();
        lease.check_active()?;
        Ok(f(&contents))
    }

    pub(crate) fn read_with_write_lease<R>(
        &self,
        lease: &WriteLease,
        f: impl FnOnce(&DocumentContents) -> R,
    ) -> std::result::Result<R, CommitError> {
        if lease.doc_ref() != &self.name {
            return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                "write lease for {} cannot access {}",
                lease.doc_ref(),
                self.name
            ))));
        }
        let contents = self.contents.read();
        lease.check_active()?;
        Ok(f(&contents))
    }

    pub(crate) fn read_mvcc_at(&self, read_time: Timestamp) -> Option<Arc<StoredDocumentVersion>> {
        self.contents.read().version_at_time(read_time).cloned()
    }

    pub(crate) fn read_latest_uncontended<R>(&self, f: impl FnOnce(&DocumentContents) -> R) -> R {
        f(&self.contents.read())
    }

    pub(crate) fn apply_sealed_commit<R>(
        seal: CommitSeal,
        docs: BTreeMap<DocumentRef, Option<Arc<DocumentMeta>>>,
        apply: impl FnOnce(
            Timestamp,
            &mut BTreeMap<DocumentRef, Option<&mut DocumentContents>>,
        ) -> std::result::Result<R, CommitError>,
    ) -> std::result::Result<R, CommitError> {
        seal.check_committing_active()?;
        if seal.len() != docs.len() {
            drop(seal);
            return Err(CommitError::Other(GenericDatabaseError::internal(
                "commit seal document count mismatch",
            )));
        }

        let metas: Vec<(DocumentRef, Option<Arc<DocumentMeta>>)> = docs.into_iter().collect();
        let mut guards: Vec<(&DocumentRef, Option<RwLockWriteGuard<'_, DocumentContents>>)> =
            Vec::with_capacity(metas.len());
        for (doc_ref, meta) in &metas {
            let guard = match meta {
                Some(meta) => {
                    if &meta.name != doc_ref {
                        let actual = &meta.name;
                        drop(guards);
                        drop(seal);
                        return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                            "document meta name {actual} does not match key {doc_ref}",
                        ))));
                    }
                    Some(meta.contents.write())
                }
                None => None,
            };
            guards.push((doc_ref, guard));
        }

        let commit_time = Timestamp::now();
        for &(doc_ref, _) in &guards {
            let Some(lease) = seal.get(doc_ref) else {
                let missing = doc_ref.clone();
                drop(guards);
                drop(seal);
                return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                    "missing write lease in CommitSeal for {missing}"
                ))));
            };
            if commit_time < lease.acquire_time() {
                drop(guards);
                drop(seal);
                return Err(CommitError::Other(GenericDatabaseError::internal(
                    "lock should always be acquired before claiming write/update time",
                )));
            }
        }

        let res = {
            let mut contents_map: BTreeMap<DocumentRef, Option<&mut DocumentContents>> = guards
                .iter_mut()
                .map(|(doc_ref, guard)| ((*doc_ref).clone(), guard.as_deref_mut()))
                .collect();
            apply(commit_time, &mut contents_map)
        };

        drop(guards);
        drop(seal);
        res
    }
}

/// DocumentContents maintains the history of versions of a single document (document-name), this
/// includes "deleted versions".
#[derive(Debug, Default)]
pub(crate) struct DocumentContents {
    versions: Vec<DocumentVersion>,
}

impl DocumentContents {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn latest_version(&self) -> Option<&DocumentVersion> {
        self.versions.last()
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
        let idx = self
            .versions
            .partition_point(|version| version.update_time() <= read_time);
        idx.checked_sub(1)
            .and_then(|i| self.versions[i].stored_document())
    }

    pub(crate) fn exists(&self) -> bool {
        self.versions
            .last()
            .is_some_and(|version| matches!(version, DocumentVersion::Stored(_)))
    }

    pub(crate) fn last_updated(&self) -> Option<Timestamp> {
        self.versions.last().map(DocumentVersion::update_time)
    }

    #[cfg(test)]
    pub(crate) fn version_count(&self) -> usize {
        self.versions.len()
    }

    pub(crate) fn maybe_add_version(&mut self, version: &DocumentVersion) -> Result<()> {
        if let Some(last) = self.versions.last_mut() {
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
        self.versions.push(version.clone());
        Ok(())
    }

    /// Add a new version with the given `update_time`; if `fields` equals the last stored version
    /// in storage representation, returns its existing `update_time` and `None`.
    #[cfg(test)]
    pub(crate) fn maybe_update(
        &mut self,
        name: &DocumentRef,
        fields: HashMap<String, Value>,
        update_time: Timestamp,
    ) -> Result<(Timestamp, Option<DocumentVersion>)> {
        let (ts, version) = stage_stored_version(name, self.versions.last(), fields, update_time);
        if let Some(ref version) = version {
            self.maybe_add_version(version)?;
        }
        Ok((ts, version))
    }
}

pub(crate) fn stage_stored_version(
    name: &DocumentRef,
    latest: Option<&DocumentVersion>,
    fields: HashMap<String, Value>,
    update_time: Timestamp,
) -> (Timestamp, Option<DocumentVersion>) {
    if let Some(last) = latest.and_then(DocumentVersion::stored_document)
        && Value::fields_storage_eq(&last.fields, &fields)
    {
        return (last.update_time, None);
    }
    let create_time = latest
        .and_then(DocumentVersion::create_time)
        .unwrap_or(update_time);
    let version = DocumentVersion::Stored(Arc::new(StoredDocumentVersion {
        name: name.clone(),
        create_time,
        update_time,
        fields,
    }));
    (update_time, Some(version))
}

pub(crate) fn check_version_precondition(
    name: &DocumentRef,
    latest: Option<&DocumentVersion>,
    condition: DocumentPrecondition,
) -> Result<()> {
    let current = latest.and_then(DocumentVersion::stored_document);
    match condition {
        DocumentPrecondition::Exists if current.is_none() => Err(GenericDatabaseError::not_found(
            format!("document not found: {name}"),
        )),
        DocumentPrecondition::NotExists if current.is_some() => Err(
            GenericDatabaseError::already_exists(format!("document already exists: {name}")),
        ),
        DocumentPrecondition::UpdateTime(time) if current.map(|v| v.update_time) != Some(time) => {
            Err(GenericDatabaseError::failed_precondition(format!(
                "document has different update_time: {name}"
            )))
        }
        _ => Ok(()),
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

#[derive(Clone, Copy, Debug)]
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    use googleapis::google::{firestore::v1::Value, protobuf::Timestamp};
    use googletest::prelude::*;

    use super::{DocumentContents, DocumentMeta};
    use crate::{
        database::{
            lock::{LockManager, LockPriority},
            timeouts::Timeouts,
        },
        reference::{DocumentRef, RootRef},
    };

    #[gtest]
    fn maybe_update_distinguishes_integer_and_double_and_preserves_nan_noop() {
        let name: DocumentRef = "projects/p/databases/d/documents/col/doc".parse().unwrap();
        let mut contents = DocumentContents::new();

        let initial_fields = HashMap::from([
            ("score".to_string(), Value::integer(1)),
            (
                "nested".to_string(),
                Value::map(HashMap::from([(
                    "items".to_string(),
                    Value::array(vec![Value::integer(5)]),
                )])),
            ),
            ("nan_val".to_string(), Value::double(f64::NAN)),
        ]);

        let t1 = Timestamp {
            seconds: 101,
            nanos:   0,
        };
        let (_, v1) = contents
            .maybe_update(&name, initial_fields.clone(), t1)
            .unwrap();
        expect_that!(v1, some(anything()));

        // Re-writing the exact same fields (including NaN) is a no-op.
        let t2 = Timestamp {
            seconds: 102,
            nanos:   0,
        };
        let (ts, v2) = contents.maybe_update(&name, initial_fields, t2).unwrap();
        expect_eq!(ts, t1);
        expect_that!(v2, none());

        // Changing top-level integer 1 to double 1.0 creates a new version.
        let top_level_double = HashMap::from([
            ("score".to_string(), Value::double(1.0)),
            (
                "nested".to_string(),
                Value::map(HashMap::from([(
                    "items".to_string(),
                    Value::array(vec![Value::integer(5)]),
                )])),
            ),
            ("nan_val".to_string(), Value::double(f64::NAN)),
        ]);
        let t3 = Timestamp {
            seconds: 103,
            nanos:   0,
        };
        let (ts, v3) = contents.maybe_update(&name, top_level_double, t3).unwrap();
        expect_eq!(ts, t3);
        expect_that!(v3, some(anything()));

        // Changing nested integer 5 to double 5.0 creates a new version.
        let nested_double = HashMap::from([
            ("score".to_string(), Value::double(1.0)),
            (
                "nested".to_string(),
                Value::map(HashMap::from([(
                    "items".to_string(),
                    Value::array(vec![Value::double(5.0)]),
                )])),
            ),
            ("nan_val".to_string(), Value::double(f64::NAN)),
        ]);
        let t4 = Timestamp {
            seconds: 104,
            nanos:   0,
        };
        let (ts, v4) = contents.maybe_update(&name, nested_double, t4).unwrap();
        expect_eq!(ts, t4);
        expect_that!(v4, some(anything()));
    }

    #[gtest]
    #[tokio::test]
    async fn capability_accessors_and_poison_recovery_work_cleanly() {
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let name: DocumentRef = "projects/p/databases/(default)/documents/col/doc"
            .parse()
            .unwrap();
        let meta = Arc::new(DocumentMeta::new(name.clone()));

        // Poison the inner RwLock deliberately via a panicked thread, then confirm subsequent
        // `read_latest_uncontended` and `apply_sealed_commit` recover cleanly.
        let poisoned_meta = Arc::clone(&meta);
        let _unused = std::thread::spawn(move || {
            let _guard = poisoned_meta.contents.write();
            panic!("deliberate poison test");
        })
        .join();
        expect_false!(meta.read_latest_uncontended(DocumentContents::exists));

        let locks = Arc::new(LockManager::new(root, Timeouts::FAST));
        let s = locks.new_session(
            None,
            crate::database::lock::SessionKind::PessimisticTxn,
            LockPriority::NextSeq,
            true,
            true,
        );
        let w = s.acquire_write(&name).await.unwrap();
        expect_that!(
            meta.read_with_write_lease(&w, DocumentContents::exists),
            ok(eq(&false))
        );

        let seal = s
            .seal_for_commit(BTreeMap::from([(name.clone(), w)]))
            .unwrap();
        let docs = BTreeMap::from([(name.clone(), Some(Arc::clone(&meta)))]);
        let commit_ts = DocumentMeta::apply_sealed_commit(seal, docs, |ts, map| {
            let contents = map.get_mut(&name).unwrap().as_deref_mut().unwrap();
            contents.maybe_update(
                &name,
                HashMap::from([("v".to_string(), Value::integer(7))]),
                ts,
            )?;
            Ok(ts)
        })
        .unwrap();

        let snap = meta.read_mvcc_at(commit_ts).unwrap();
        expect_eq!(snap.fields["v"].as_integer(), Some(7));

        let s2 = locks.new_session(
            None,
            crate::database::lock::SessionKind::PessimisticTxn,
            LockPriority::NextSeq,
            true,
            true,
        );
        let r = s2.acquire_read(&name).await.unwrap();
        expect_that!(
            meta.read_with(&r, |c| c.current_version().unwrap().fields["v"]
                .as_integer()),
            ok(some(eq(&7)))
        );
    }

    #[gtest]
    fn version_at_time_binary_search_handles_edge_cases() {
        use super::{DeletedDocumentVersion, DocumentVersion};

        let name: DocumentRef = "projects/p/databases/d/documents/col/mvcc".parse().unwrap();
        let mut contents = DocumentContents::new();

        let ts = |seconds, nanos| Timestamp { seconds, nanos };

        // Empty history returns `None` for any read_time.
        expect_that!(contents.version_at_time(ts(100, 0)), none());

        // Single version at `t1`: `< t1` -> `None`, `== t1` -> `v1`, `> t1` -> `v1`.
        let t1 = ts(100, 10);
        contents
            .maybe_update(
                &name,
                HashMap::from([("v".to_string(), Value::integer(1))]),
                t1,
            )
            .unwrap();
        expect_that!(contents.version_at_time(ts(100, 9)), none());
        expect_eq!(
            contents
                .version_at_time(t1)
                .map(|v| (v.create_time, v.fields["v"].as_integer())),
            Some((t1, Some(1)))
        );
        expect_eq!(
            contents
                .version_at_time(ts(100, 11))
                .map(|v| v.fields["v"].as_integer()),
            Some(Some(1))
        );

        // Update at `t2`, then overwrite at exact same timestamp `t2`.
        let t2 = ts(100, 20);
        contents
            .maybe_update(
                &name,
                HashMap::from([("v".to_string(), Value::integer(2))]),
                t2,
            )
            .unwrap();
        contents
            .maybe_update(
                &name,
                HashMap::from([("v".to_string(), Value::integer(22))]),
                t2,
            )
            .unwrap();

        // Delete at `t3`, then recreate at `t4`.
        let t3 = ts(101, 0);
        contents
            .maybe_add_version(&DocumentVersion::Deleted(Arc::new(
                DeletedDocumentVersion {
                    name: name.clone(),
                    delete_time: t3,
                },
            )))
            .unwrap();

        let t4 = ts(102, 5);
        contents
            .maybe_update(
                &name,
                HashMap::from([("v".to_string(), Value::integer(4))]),
                t4,
            )
            .unwrap();

        // Probe every boundary across `t1`, `t2`, `t3`, `t4`.
        expect_that!(contents.version_at_time(ts(100, 9)), none());
        expect_eq!(
            contents
                .version_at_time(ts(100, 19))
                .map(|v| v.fields["v"].as_integer()),
            Some(Some(1))
        );
        expect_eq!(
            contents
                .version_at_time(t2)
                .map(|v| (v.create_time, v.fields["v"].as_integer())),
            Some((t1, Some(22)))
        );
        expect_eq!(
            contents
                .version_at_time(ts(100, 999_999_999))
                .map(|v| v.fields["v"].as_integer()),
            Some(Some(22))
        );
        expect_that!(contents.version_at_time(t3), none());
        expect_that!(contents.version_at_time(ts(102, 4)), none());
        expect_eq!(
            contents
                .version_at_time(t4)
                .map(|v| (v.create_time, v.fields["v"].as_integer())),
            Some((t4, Some(4)))
        );
        expect_eq!(
            contents
                .version_at_time(ts(999, 0))
                .map(|v| (v.create_time, v.fields["v"].as_integer())),
            Some((t4, Some(4)))
        );
    }

    #[gtest]
    #[tokio::test]
    async fn apply_sealed_commit_supports_absent_docs_and_validates_all_leases() {
        use std::collections::BTreeSet;

        use tokio::time::{Duration, Instant};

        use crate::{GenericDatabaseError, error::CommitError};

        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let present_ref: DocumentRef = "projects/p/databases/(default)/documents/col/present"
            .parse()
            .unwrap();
        let absent_ref: DocumentRef = "projects/p/databases/(default)/documents/col/absent"
            .parse()
            .unwrap();
        let unsealed_ref: DocumentRef = "projects/p/databases/(default)/documents/col/unsealed"
            .parse()
            .unwrap();

        let locks = Arc::new(LockManager::new(root, Timeouts::FAST));
        let present_meta = Arc::new(DocumentMeta::new(present_ref.clone()));

        // Mixed `Some(meta)` + `None` commit succeeds and honors both lease `acquire_time`s.
        let s1 = locks.new_session(
            None,
            crate::database::lock::SessionKind::PessimisticTxn,
            LockPriority::NextSeq,
            true,
            true,
        );
        let leases = s1
            .acquire_writes_all(
                &BTreeSet::from([present_ref.clone(), absent_ref.clone()]),
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        let max_acquire = leases
            .values()
            .map(|l| l.acquire_time())
            .max()
            .expect("leases present");
        let seal = s1.seal_for_commit(leases).unwrap();
        let docs = BTreeMap::from([
            (present_ref.clone(), Some(Arc::clone(&present_meta))),
            (absent_ref.clone(), None),
        ]);
        let commit_ts = DocumentMeta::apply_sealed_commit(seal, docs, |ts, map| {
            expect_that!(map.get(&present_ref), some(some(anything())));
            expect_that!(map.get(&absent_ref), some(none()));
            Ok(ts)
        })
        .unwrap();
        expect_true!(commit_ts >= max_acquire);

        // Missing entry in `docs` still fails count check.
        let s2 = locks.new_session(
            None,
            crate::database::lock::SessionKind::PessimisticTxn,
            LockPriority::NextSeq,
            true,
            true,
        );
        let w2 = s2.acquire_write(&absent_ref).await.unwrap();
        let seal2 = s2
            .seal_for_commit(BTreeMap::from([(absent_ref.clone(), w2)]))
            .unwrap();
        let err =
            DocumentMeta::apply_sealed_commit(seal2, BTreeMap::new(), |_, _| Ok(())).unwrap_err();
        expect_true!(matches!(
            err,
            CommitError::Other(GenericDatabaseError::Internal(_))
        ));

        // Same count, mismatched key (`None` entry for an unsealed doc) fails lease lookup.
        let s3 = locks.new_session(
            None,
            crate::database::lock::SessionKind::PessimisticTxn,
            LockPriority::NextSeq,
            true,
            true,
        );
        let w3 = s3.acquire_write(&absent_ref).await.unwrap();
        let seal3 = s3
            .seal_for_commit(BTreeMap::from([(absent_ref.clone(), w3)]))
            .unwrap();
        let err = DocumentMeta::apply_sealed_commit(
            seal3,
            BTreeMap::from([(unsealed_ref, None)]),
            |_, _| Ok(()),
        )
        .unwrap_err();
        expect_true!(matches!(
            err,
            CommitError::Other(GenericDatabaseError::Internal(_))
        ));
    }
}
