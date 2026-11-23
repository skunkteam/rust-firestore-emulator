use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use googleapis::google::{
    firestore::v1::{Value, Write, WriteResult},
    protobuf::Timestamp,
};
use tokio::time::Instant;
use tracing::{Level, Span, debug, field::display, instrument};

use super::{
    FirestoreDatabase,
    document::{
        DeletedDocumentVersion, DocumentContents, DocumentMeta, DocumentPrecondition,
        DocumentVersion, StoredDocumentVersion, check_version_precondition, stage_stored_version,
    },
    event::DatabaseEvent,
    field_path::FieldPath,
    lock::{CommitSeal, LockPriority, ReadObservation, SessionKind},
    reference::DocumentRef,
    transaction::{CommitTake, ReadWriteTransaction, TransactionId},
    validation::{
        self, ValidatedFieldTransform, ValidatedTransformOp, ValidatedWrite,
        ValidatedWriteOperation,
    },
};
use crate::{
    GenericDatabaseError,
    error::{AbortReason, CommitError, Result},
    unimplemented,
};

struct ValidatedCommitWrites {
    target_docs: BTreeSet<DocumentRef>,
    writes:      Vec<ValidatedWrite>,
}

impl FirestoreDatabase {
    #[instrument(level = Level::DEBUG, skip_all, fields(
        count = writes.len(),
        txn_id,
    ), err)]
    pub async fn commit(
        self: &Arc<Self>,
        writes: Vec<Write>,
        transaction: Option<TransactionId>,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        let Some(transaction_id) = transaction else {
            return self.commit_non_transactional(writes).await;
        };

        Span::current().record("txn_id", display(transaction_id));
        let validated = self.validate_commit_writes(writes)?;

        match self
            .transactions
            .take_for_commit(transaction_id, !validated.writes.is_empty())?
        {
            CommitTake::ReadOnly(ro) => {
                debug!(txn_id = %ro.id, "committing empty read-only transaction");
                Ok((Timestamp::now(), Vec::new()))
            }
            CommitTake::ReadWrite((rw, commit_guard)) => {
                match self
                    .commit_read_write(&rw, validated.target_docs, validated.writes)
                    .await
                {
                    Ok(res) => {
                        commit_guard.finish_committed();
                        Ok(res)
                    }
                    Err(CommitError::Aborted(reason)) => {
                        commit_guard.abort_commit(Some(reason));
                        Err(reason.into())
                    }
                    Err(CommitError::Other(err)) => {
                        commit_guard.abort_commit(None);
                        Err(err)
                    }
                }
            }
        }
    }

    pub(super) async fn write_with_version(
        self: &Arc<Self>,
        write: Write,
    ) -> Result<(WriteResult, Option<Arc<StoredDocumentVersion>>)> {
        let validated = ValidatedWrite::parse(write, &self.name)?;
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }

        let doc_ref = validated.doc_ref.clone();
        let (_, mut results, stored) = self
            .apply_single_non_transactional(doc_ref, vec![validated])
            .await?;
        let result = results
            .pop()
            .ok_or_else(|| GenericDatabaseError::internal("missing single write result"))?;
        Ok((result, stored))
    }

    fn validate_commit_writes(&self, writes: Vec<Write>) -> Result<ValidatedCommitWrites> {
        let mut target_docs = BTreeSet::new();
        let mut parsed_writes = Vec::with_capacity(writes.len());
        for write in writes {
            let validated = ValidatedWrite::parse(write, &self.name)?;
            target_docs.insert(validated.doc_ref.clone());
            parsed_writes.push(validated);
        }
        Ok(ValidatedCommitWrites {
            target_docs,
            writes: parsed_writes,
        })
    }

    async fn apply_single_non_transactional(
        self: &Arc<Self>,
        doc_ref: DocumentRef,
        writes: Vec<ValidatedWrite>,
    ) -> Result<(
        Timestamp,
        Vec<WriteResult>,
        Option<Arc<StoredDocumentVersion>>,
    )> {
        self.transactions.reap_if_due();
        let session = self.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        let lease = session.acquire_write(&doc_ref).await?;
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        let seal = session.seal_for_commit(BTreeMap::from([(doc_ref, lease)]))?;
        self.apply_sealed_commit_writes(seal, writes)
            .await
            .map_err(Into::into)
    }

    async fn commit_non_transactional(
        self: &Arc<Self>,
        writes: Vec<Write>,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        let validated = self.validate_commit_writes(writes)?;
        if validated.target_docs.is_empty() {
            self.transactions.reap_if_due();
            return Ok((Timestamp::now(), Vec::new()));
        }

        if validated.target_docs.len() == 1 {
            let doc_ref = validated
                .target_docs
                .into_iter()
                .next()
                .expect("single target doc");
            let (time, results, _) = self
                .apply_single_non_transactional(doc_ref, validated.writes)
                .await?;
            return Ok((time, results));
        }

        self.transactions.reap_if_due();
        let batch_start = Instant::now();
        let deadline = batch_start + self.project.timeouts.write;
        let mut priority = LockPriority::NextSeq;
        let mut wound_retries: u32 = 0;

        loop {
            let session =
                self.locks
                    .new_session(None, SessionKind::WriteBatch, priority, true, true);
            let sealed = match session
                .acquire_writes_all_since(
                    &validated.target_docs,
                    batch_start,
                    deadline,
                    (wound_retries > 0).then_some(wound_retries),
                )
                .await
            {
                Ok(write_leases) => Arc::clone(&session).seal_for_commit(write_leases),
                Err(err) => Err(err),
            };
            match sealed {
                Ok(seal) => {
                    let (time, results, _) = self
                        .apply_sealed_commit_writes(seal, validated.writes)
                        .await?;
                    return Ok((time, results));
                }
                Err(CommitError::Aborted(AbortReason::Wounded {
                    wounder_txn_id,
                    wounder_seq,
                })) => {
                    let seq = session.root_seq();
                    if let Some(seq) = seq {
                        priority = LockPriority::Assigned(seq);
                    }
                    session.close();
                    if self.is_cleared() {
                        return Err(AbortReason::DatabaseCleared.into());
                    }
                    if Instant::now() >= deadline {
                        self.locks.record_write_batch_timeout(
                            session.session_id(),
                            seq,
                            u64::try_from(batch_start.elapsed().as_millis()).unwrap_or(u64::MAX),
                            wound_retries,
                        );
                        return Err(AbortReason::WriteTimeout.into());
                    }
                    if wound_retries == 0 {
                        tokio::task::yield_now().await;
                    } else {
                        let backoff =
                            Duration::from_millis((1u64 << (wound_retries - 1).min(4)).min(25));
                        tokio::time::sleep_until((Instant::now() + backoff).min(deadline)).await;
                    }
                    if self.is_cleared() {
                        return Err(AbortReason::DatabaseCleared.into());
                    }
                    if Instant::now() >= deadline {
                        self.locks.record_write_batch_timeout(
                            session.session_id(),
                            seq,
                            u64::try_from(batch_start.elapsed().as_millis()).unwrap_or(u64::MAX),
                            wound_retries,
                        );
                        return Err(AbortReason::WriteTimeout.into());
                    }
                    wound_retries = wound_retries.saturating_add(1);
                    self.locks.record_write_batch_retry(
                        session.session_id(),
                        seq,
                        wounder_txn_id,
                        wounder_seq,
                        wound_retries,
                    );
                }
                Err(other) => return Err(other.into()),
            }
        }
    }

    async fn commit_read_write(
        self: &Arc<Self>,
        rw: &ReadWriteTransaction,
        target_docs: BTreeSet<DocumentRef>,
        writes: Vec<ValidatedWrite>,
    ) -> std::result::Result<(Timestamp, Vec<WriteResult>), CommitError> {
        // Serialize optimistic validation-through-commit critical sections so concurrent
        // optimistic transactions cannot pass validation against each other's pre-commit state.
        let _optimistic_guard = if rw.is_optimistic() && !writes.is_empty() {
            Some(self.optimistic_commit_lock.lock().await)
        } else {
            None
        };

        let session = rw.session();
        let mut write_leases = BTreeMap::new();

        for doc_ref in target_docs {
            if rw.is_optimistic() {
                let lease = session.acquire_write(&doc_ref).await?;
                write_leases.insert(doc_ref, lease);
            } else {
                let (lease, observation) = session.acquire_or_upgrade_write(&doc_ref).await?;
                if let ReadObservation::Observed(expected_update_time) = observation {
                    let meta = self.peek_doc_meta(&doc_ref).await?;
                    let actual = match meta {
                        Some(meta) => {
                            meta.read_with_write_lease(&lease, DocumentContents::last_updated)?
                        }
                        None => {
                            lease.check_active()?;
                            None
                        }
                    };
                    if actual != expected_update_time {
                        drop(lease);
                        drop(write_leases);
                        self.locks.record_upgrade_abort(
                            session.session_id(),
                            session.txn_id(),
                            session.kind(),
                            session.root_seq(),
                            &doc_ref,
                        );
                        return Err(CommitError::Aborted(AbortReason::UpgradeContention));
                    }
                }
                write_leases.insert(doc_ref, lease);
            }
        }

        if rw.is_optimistic() && !writes.is_empty() {
            rw.check_concurrency_violations(&write_leases).await?;
        }
        if self.is_cleared() {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }

        let seal = Arc::clone(session).seal_for_commit(write_leases)?;
        let (time, results, _) = self.apply_sealed_commit_writes(seal, writes).await?;
        drop(_optimistic_guard);
        Ok((time, results))
    }

    async fn apply_sealed_commit_writes(
        self: &Arc<Self>,
        seal: CommitSeal,
        writes: Vec<ValidatedWrite>,
    ) -> std::result::Result<
        (
            Timestamp,
            Vec<WriteResult>,
            Option<Arc<StoredDocumentVersion>>,
        ),
        CommitError,
    > {
        // Track whether each target document's final write in this batch can persist a document
        // version. When a document is absent from storage (`peek_doc_meta` is `None`) and its
        // final write is `Delete`, staging is guaranteed to cancel any in-batch pending state
        // back to `None` (or fail a precondition), so we skip allocating phantom
        // `Collection`/`DocumentMeta` entries while still holding the exclusive `WriteLease` in
        // `seal`. Keep this as an exhaustive `match` so future `ValidatedWriteOperation` variants
        // (such as `Transform`) must explicitly declare their storage requirements at compile time.
        let mut last_write_persists: HashMap<&DocumentRef, bool> =
            HashMap::with_capacity(seal.len());
        for write in &writes {
            #[allow(clippy::match_like_matches_macro)]
            let persists = match write.operation {
                ValidatedWriteOperation::Update { .. } => true,
                ValidatedWriteOperation::Delete => false,
            };
            last_write_persists.insert(&write.doc_ref, persists);
        }

        let doc_refs: Vec<DocumentRef> = seal.doc_refs().cloned().collect();
        let mut docs = BTreeMap::new();
        for doc_ref in doc_refs {
            let meta = match self.peek_doc_meta(&doc_ref).await? {
                Some(meta) => Some(meta),
                None if last_write_persists.get(&doc_ref).copied().unwrap_or(false) => {
                    Some(self.get_doc_meta(&doc_ref).await?)
                }
                None => None,
            };
            docs.insert(doc_ref, meta);
        }

        DocumentMeta::apply_sealed_commit(seal, docs, |commit_time, contents_map| {
            if self.is_cleared() {
                return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
            }

            let mut staged_docs: BTreeMap<DocumentRef, StagedDocument<'_>> = contents_map
                .iter_mut()
                .map(|(name, doc)| {
                    (
                        name.clone(),
                        StagedDocument::new(name.clone(), doc.as_deref_mut()),
                    )
                })
                .collect();
            let mut staged_results: Vec<(DocumentRef, Vec<Value>)> =
                Vec::with_capacity(writes.len());

            for write in writes {
                let doc_ref = write.doc_ref.clone();
                let staged = staged_docs.get_mut(&doc_ref).ok_or_else(|| {
                    GenericDatabaseError::internal(format!("missing staged document for {doc_ref}"))
                })?;
                let transform_results = self.stage_write(write, staged, commit_time)?;
                staged_results.push((doc_ref, transform_results));
            }

            // Cloud Firestore populates every `WriteResult.update_time` in a commit from the
            // target document's post-commit stored version (`None` whenever the document does not
            // exist after commit, e.g. `Delete` or `[Update, Delete]`, and the final stored
            // `update_time` for `[Delete, Update]` or `[Update, revert-Update]`).
            let write_results: Vec<WriteResult> = staged_results
                .into_iter()
                .map(|(doc_ref, transform_results)| {
                    let update_time = staged_docs
                        .get(&doc_ref)
                        .and_then(StagedDocument::current_version)
                        .map(|v| v.update_time);
                    WriteResult {
                        update_time,
                        transform_results,
                    }
                })
                .collect();

            let mut updates = HashMap::new();
            let mut single_stored = None;
            let track_single = staged_docs.len() == 1;
            for (name, mut staged) in staged_docs {
                if let Some(version) = staged.pending {
                    let doc = staged.doc.as_deref_mut().ok_or_else(|| {
                        GenericDatabaseError::internal(format!(
                            "missing persisted storage slot for staged write on {name}"
                        ))
                    })?;
                    doc.maybe_add_version(&version)?;
                    updates.insert(name, version);
                }
                if track_single {
                    single_stored = staged
                        .doc
                        .as_deref()
                        .and_then(|doc| doc.current_version().cloned());
                }
            }

            if !updates.is_empty() {
                self.send_event(DatabaseEvent {
                    database: Arc::downgrade(self),
                    update_time: commit_time,
                    updates,
                });
            }

            Ok((commit_time, write_results, single_stored))
        })
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    fn stage_write(
        &self,
        write: ValidatedWrite,
        document: &mut StagedDocument<'_>,
        commit_time: Timestamp,
    ) -> Result<Vec<Value>> {
        let ValidatedWrite {
            doc_ref: _,
            precondition,
            operation,
        } = write;

        if let Some(condition) = precondition {
            document.check_precondition(condition)?;
        }
        let mut transform_results = vec![];

        match operation {
            ValidatedWriteOperation::Update {
                fields,
                update_mask,
                update_transforms,
            } => {
                debug!(name = %document.name, "Update");
                let mut fields = if let Some(mask) = update_mask {
                    apply_updates(
                        document.current_version().map(|v| &v.fields),
                        &mask,
                        &fields,
                    )
                } else {
                    fields
                };
                let has_transforms = !update_transforms.is_empty();
                for tf in update_transforms {
                    transform_results.push(apply_transform(&mut fields, tf, commit_time)?);
                }
                if has_transforms {
                    validation::validate_writable_fields(&fields)?;
                } else {
                    debug_assert!(validation::validate_writable_fields(&fields).is_ok());
                }
                document.stage_update(fields, commit_time);
            }
            ValidatedWriteOperation::Delete => {
                debug!(name = %document.name, "Delete");
                document.stage_delete(commit_time);
            }
        }
        Ok(transform_results)
    }
}

struct StagedDocument<'a> {
    name:    DocumentRef,
    doc:     Option<&'a mut DocumentContents>,
    pending: Option<DocumentVersion>,
}

impl<'a> StagedDocument<'a> {
    fn new(name: DocumentRef, doc: Option<&'a mut DocumentContents>) -> Self {
        Self {
            name,
            doc,
            pending: None,
        }
    }

    fn persisted_latest_version(&self) -> Option<&DocumentVersion> {
        self.doc
            .as_deref()
            .and_then(DocumentContents::latest_version)
    }

    fn persisted_current_version(&self) -> Option<&Arc<StoredDocumentVersion>> {
        self.doc
            .as_deref()
            .and_then(DocumentContents::current_version)
    }

    fn latest_version(&self) -> Option<&DocumentVersion> {
        self.pending
            .as_ref()
            .or_else(|| self.persisted_latest_version())
    }

    fn current_version(&self) -> Option<&Arc<StoredDocumentVersion>> {
        self.latest_version()
            .and_then(DocumentVersion::stored_document)
    }

    fn check_precondition(&self, condition: DocumentPrecondition) -> Result<()> {
        check_version_precondition(&self.name, self.latest_version(), condition)
    }

    fn stage_update(&mut self, fields: HashMap<String, Value>, commit_time: Timestamp) {
        let (_, version) =
            stage_stored_version(&self.name, self.latest_version(), fields, commit_time);
        if let Some(version) = version {
            if let (Some(orig), Some(staged)) =
                (self.persisted_current_version(), version.stored_document())
                && orig.create_time == staged.create_time
                && Value::fields_storage_eq(&orig.fields, &staged.fields)
            {
                self.pending = None;
                return;
            }
            self.pending = Some(version);
        }
    }

    fn stage_delete(&mut self, delete_time: Timestamp) {
        if self.persisted_current_version().is_none() {
            self.pending = None;
            return;
        }
        self.pending = Some(DocumentVersion::Deleted(Arc::new(DeletedDocumentVersion {
            name: self.name.clone(),
            delete_time,
        })));
    }
}

fn apply_updates(
    existing_fields: Option<&HashMap<String, Value>>,
    mask: &[FieldPath],
    updated_values: &HashMap<String, Value>,
) -> HashMap<String, Value> {
    let mut fields = existing_fields.cloned().unwrap_or_default();
    for field_path in mask {
        match field_path.get_value(updated_values) {
            Some(new_value) => field_path.set_value(&mut fields, new_value.clone()),
            None => {
                field_path.delete_value(&mut fields);
            }
        }
    }
    fields
}

fn apply_transform(
    fields: &mut HashMap<String, Value>,
    transform: ValidatedFieldTransform,
    commit_time: Timestamp,
) -> Result<Value> {
    let ValidatedFieldTransform { field_path, op } = transform;
    let result = match op {
        ValidatedTransformOp::SetToRequestTime => {
            let new_value = Value::timestamp(commit_time);
            field_path.set_value(fields, new_value.clone());
            new_value
        }
        ValidatedTransformOp::Increment(value) => field_path
            .transform_value(fields, |cur| cur.unwrap_or_else(Value::null) + value)
            .clone(),
        ValidatedTransformOp::Maximum => unimplemented!("TransformType::Maximum"),
        ValidatedTransformOp::Minimum => unimplemented!("TransformType::Minimum"),
        ValidatedTransformOp::AppendMissingElements(elements) => {
            field_path.transform_value(fields, |cur_value| {
                let mut array = cur_value.and_then(Value::into_array).unwrap_or_default();
                for el in elements {
                    if !array.contains(&el) {
                        array.push(el);
                    }
                }
                Value::array(array)
            });
            Value::null()
        }
        ValidatedTransformOp::RemoveAllFromArray(elements) => {
            field_path.transform_value(fields, |cur_value| {
                match cur_value.and_then(Value::into_array) {
                    Some(mut array) => {
                        array.retain(|v| !elements.contains(v));
                        Value::array(array)
                    }
                    None => Value::array(vec![]),
                }
            });
            Value::null()
        }
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::google::{
        firestore::v1::{
            Document, DocumentMask, Precondition, TransactionOptions, Value, Write,
            document_transform::{
                FieldTransform,
                field_transform::{ServerValue, TransformType},
            },
            precondition, transaction_options, write,
        },
        protobuf::Timestamp,
    };

    use crate::{
        FirestoreConfig, FirestoreProject, GenericDatabaseError,
        database::read_consistency::ReadConsistency,
        reference::{DocumentRef, RootRef},
    };

    async fn test_db() -> (
        &'static FirestoreProject,
        RootRef,
        std::sync::Arc<crate::FirestoreDatabase>,
    ) {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        (project, root, db)
    }

    #[tokio::test]
    async fn transform_only_empty_mask_preserves_existing_fields_while_none_overwrites() {
        let (_project, _root, db) = test_db().await;
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/d"
            .parse()
            .unwrap();

        db.update_doc(
            &doc_ref,
            HashMap::from([
                ("keep".to_string(), Value::string("yes")),
                ("count".to_string(), Value::integer(10)),
            ]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Empty `update_mask: Some([])` with transforms preserves existing fields and ignores
        // unmasked `doc.fields`.
        db.commit(
            vec![Write {
                update_mask: Some(DocumentMask {
                    field_paths: vec![],
                }),
                update_transforms: vec![FieldTransform {
                    field_path:     "count".to_string(),
                    transform_type: Some(TransformType::Increment(Value::integer(5))),
                }],
                operation: Some(write::Operation::Update(Document {
                    name: doc_ref.to_string(),
                    fields: HashMap::from([("ignored".to_string(), Value::integer(999))]),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            None,
        )
        .await
        .unwrap();

        let after_empty_mask = db
            .get_doc(&doc_ref, ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after_empty_mask.fields,
            HashMap::from([
                ("keep".to_string(), Value::string("yes")),
                ("count".to_string(), Value::integer(15)),
            ])
        );

        // `update_mask: None` overwrites the document with `doc.fields` before running transforms.
        db.commit(
            vec![Write {
                update_mask: None,
                update_transforms: vec![FieldTransform {
                    field_path:     "count".to_string(),
                    transform_type: Some(TransformType::Increment(Value::integer(1))),
                }],
                operation: Some(write::Operation::Update(Document {
                    name: doc_ref.to_string(),
                    fields: HashMap::new(),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            None,
        )
        .await
        .unwrap();

        let after_none_mask = db
            .get_doc(&doc_ref, ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after_none_mask.fields,
            HashMap::from([("count".to_string(), Value::integer(1))])
        );
    }

    #[tokio::test]
    async fn max_min_checks_precondition_before_not_implemented_and_rejects_bad_operand_prelock() {
        let (_project, _root, db) = test_db().await;
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/mm"
            .parse()
            .unwrap();

        let max_tf = |val: Value| {
            vec![FieldTransform {
                field_path:     "x".to_string(),
                transform_type: Some(TransformType::Maximum(val)),
            }]
        };
        let min_tf = |val: Value| {
            vec![FieldTransform {
                field_path:     "x".to_string(),
                transform_type: Some(TransformType::Minimum(val)),
            }]
        };

        // Missing doc with `Exists(true)` fails precondition (`NotFound`) before `NotImplemented`.
        let err = db
            .commit(
                vec![Write {
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::Exists(true)),
                    }),
                    update_mask: Some(DocumentMask::default()),
                    update_transforms: max_tf(Value::integer(10)),
                    operation: Some(write::Operation::Update(Document {
                        name: doc_ref.to_string(),
                        ..Default::default()
                    })),
                }],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::NotFound(_)));

        // Invalid operand on `Maximum` fails pre-lock with `InvalidArgument` even when
        // `Exists(true)` would fail under lock, and preserves an active transaction.
        let rw_id = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(
                    transaction_options::ReadWrite::default(),
                )),
            })
            .await
            .unwrap();
        let err = db
            .commit(
                vec![Write {
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::Exists(true)),
                    }),
                    update_mask: Some(DocumentMask::default()),
                    update_transforms: max_tf(Value::default()),
                    operation: Some(write::Operation::Update(Document {
                        name: doc_ref.to_string(),
                        ..Default::default()
                    })),
                }],
                Some(rw_id),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::InvalidArgument(_)));
        assert!(
            db.get_doc(&doc_ref, ReadConsistency::Transaction(rw_id))
                .await
                .is_ok(),
            "pre-lock operand rejection must keep active transaction open"
        );
        db.rollback(rw_id).await.unwrap();

        // Create doc, then check `Exists(false)` -> `AlreadyExists` and stale `UpdateTime` ->
        // `FailedPrecondition` before `NotImplemented`.
        db.update_doc(
            &doc_ref,
            HashMap::from([("x".to_string(), Value::integer(5))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let err = db
            .commit(
                vec![Write {
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::Exists(false)),
                    }),
                    update_mask: Some(DocumentMask::default()),
                    update_transforms: min_tf(Value::integer(1)),
                    operation: Some(write::Operation::Update(Document {
                        name: doc_ref.to_string(),
                        ..Default::default()
                    })),
                }],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::AlreadyExists(_)));

        let err = db
            .commit(
                vec![Write {
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                            seconds: 1,
                            nanos:   0,
                        })),
                    }),
                    update_mask: Some(DocumentMask::default()),
                    update_transforms: max_tf(Value::integer(1)),
                    operation: Some(write::Operation::Update(Document {
                        name: doc_ref.to_string(),
                        ..Default::default()
                    })),
                }],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::FailedPrecondition(_)));

        // When precondition passes, `Maximum`/`Minimum` returns `NotImplemented`.
        let err = db
            .commit(
                vec![Write {
                    current_document: Some(Precondition {
                        condition_type: Some(precondition::ConditionType::Exists(true)),
                    }),
                    update_mask: Some(DocumentMask::default()),
                    update_transforms: min_tf(Value::integer(1)),
                    operation: Some(write::Operation::Update(Document {
                        name: doc_ref.to_string(),
                        ..Default::default()
                    })),
                }],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::NotImplemented(_)));
    }

    #[tokio::test]
    async fn overlapping_and_repeated_mask_paths_apply_cleanly_without_losing_values() {
        let (_project, _root, db) = test_db().await;
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/overlap"
            .parse()
            .unwrap();

        db.update_doc(
            &doc_ref,
            HashMap::from([
                ("keep".to_string(), Value::boolean(true)),
                ("emb".to_string(), Value::vector([1.0, 2.0])),
                (
                    "a".to_string(),
                    Value::map(HashMap::from([
                        ("old".to_string(), Value::integer(1)),
                        ("sib".to_string(), Value::integer(2)),
                        (
                            "b".to_string(),
                            Value::map(HashMap::from([
                                ("c".to_string(), Value::integer(10)),
                                ("keep_b".to_string(), Value::integer(20)),
                            ])),
                        ),
                    ])),
                ),
            ]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Descendant-only mask preserves unmasked siblings (`a.sib`, `a.b.keep_b`), ignores
        // unmasked incoming fields (`a.ignored`), explicitly deletes absent `a.old`, and leaves
        // stored `VectorValue` `emb` intact when absent subpath `emb.value` is masked.
        let updated = db
            .update_doc(
                &doc_ref,
                HashMap::from([(
                    "a".to_string(),
                    Value::map(HashMap::from([
                        ("ignored".to_string(), Value::integer(99)),
                        (
                            "b".to_string(),
                            Value::map(HashMap::from([("c".to_string(), Value::integer(3))])),
                        ),
                    ])),
                )]),
                Some(DocumentMask {
                    field_paths: vec![
                        "a.b.c".to_string(),
                        "a.old".to_string(),
                        "a.b.c".to_string(),
                        "emb.value".to_string(),
                    ],
                }),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            updated.fields,
            HashMap::from([
                ("keep".to_string(), Value::boolean(true)),
                ("emb".to_string(), Value::vector([1.0, 2.0])),
                (
                    "a".to_string(),
                    Value::map(HashMap::from([
                        ("sib".to_string(), Value::integer(2)),
                        (
                            "b".to_string(),
                            Value::map(HashMap::from([
                                ("c".to_string(), Value::integer(3)),
                                ("keep_b".to_string(), Value::integer(20)),
                            ])),
                        ),
                    ])),
                ),
            ])
        );

        // Descendant-before-ancestor mask `["a.b.c", "a.b", "a", "emb.value"]`: ancestor `"a"`
        // overwrites existing siblings (`a.sib`, `a.b.keep_b`) while applying `a.new_sib`, and
        // `emb.value` still leaves `VectorValue` `emb` intact.
        let updated = db
            .update_doc(
                &doc_ref,
                HashMap::from([(
                    "a".to_string(),
                    Value::map(HashMap::from([
                        ("new_sib".to_string(), Value::integer(4)),
                        (
                            "b".to_string(),
                            Value::map(HashMap::from([("c".to_string(), Value::integer(5))])),
                        ),
                    ])),
                )]),
                Some(DocumentMask {
                    field_paths: vec![
                        "a.b.c".to_string(),
                        "a.b".to_string(),
                        "a".to_string(),
                        "emb.value".to_string(),
                    ],
                }),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            updated.fields,
            HashMap::from([
                ("keep".to_string(), Value::boolean(true)),
                ("emb".to_string(), Value::vector([1.0, 2.0])),
                (
                    "a".to_string(),
                    Value::map(HashMap::from([
                        ("new_sib".to_string(), Value::integer(4)),
                        (
                            "b".to_string(),
                            Value::map(HashMap::from([("c".to_string(), Value::integer(5))])),
                        ),
                    ])),
                ),
            ])
        );

        // Ancestor-before-descendant repeated mask `["a", "a.b", "a.b.c", "a"]` deletes
        // `a.new_sib`, while `["emb", "emb.value"]` writes `VectorValue` without corrupting its
        // sentinel `value` key.
        let updated = db
            .update_doc(
                &doc_ref,
                HashMap::from([
                    (
                        "a".to_string(),
                        Value::map(HashMap::from([(
                            "b".to_string(),
                            Value::map(HashMap::from([("c".to_string(), Value::integer(6))])),
                        )])),
                    ),
                    ("emb".to_string(), Value::vector([3.0, 4.0])),
                ]),
                Some(DocumentMask {
                    field_paths: vec![
                        "a".to_string(),
                        "a.b".to_string(),
                        "a.b.c".to_string(),
                        "a".to_string(),
                        "emb".to_string(),
                        "emb.value".to_string(),
                    ],
                }),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            updated.fields,
            HashMap::from([
                ("keep".to_string(), Value::boolean(true)),
                ("emb".to_string(), Value::vector([3.0, 4.0])),
                (
                    "a".to_string(),
                    Value::map(HashMap::from([(
                        "b".to_string(),
                        Value::map(HashMap::from([("c".to_string(), Value::integer(6))])),
                    )])),
                ),
            ])
        );

        // Writing a subpath onto a stored `VectorValue` replaces it atomically as a valid map
        // (satisfying post-merge `debug_assert!(validate_writable_fields(&fields).is_ok())`).
        let updated = db
            .update_doc(
                &doc_ref,
                HashMap::from([(
                    "emb".to_string(),
                    Value::map(HashMap::from([("nested".to_string(), Value::string("ok"))])),
                )]),
                Some(DocumentMask {
                    field_paths: vec!["emb.nested".to_string()],
                }),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            updated.fields,
            HashMap::from([
                ("keep".to_string(), Value::boolean(true)),
                (
                    "emb".to_string(),
                    Value::map(HashMap::from([("nested".to_string(), Value::string("ok"))])),
                ),
                (
                    "a".to_string(),
                    Value::map(HashMap::from([(
                        "b".to_string(),
                        Value::map(HashMap::from([("c".to_string(), Value::integer(6))])),
                    )])),
                ),
            ])
        );
    }

    #[tokio::test]
    async fn commit_preserves_multi_write_order_for_same_document_and_cleared_db_precedence() {
        let (project, root, db) = test_db().await;
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/seq"
            .parse()
            .unwrap();

        let (commit_ts, results) = db
            .commit(
                vec![
                    Write {
                        operation: Some(write::Operation::Update(Document {
                            name: doc_ref.to_string(),
                            fields: HashMap::from([("v".to_string(), Value::integer(1))]),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    Write {
                        update_mask: Some(DocumentMask::default()),
                        update_transforms: vec![
                            FieldTransform {
                                field_path:     "v".to_string(),
                                transform_type: Some(TransformType::Increment(Value::integer(4))),
                            },
                            FieldTransform {
                                field_path:     "ts".to_string(),
                                transform_type: Some(TransformType::SetToServerValue(
                                    ServerValue::RequestTime as i32,
                                )),
                            },
                        ],
                        operation: Some(write::Operation::Update(Document {
                            name: doc_ref.to_string(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    Write {
                        update_mask: Some(DocumentMask {
                            field_paths: vec!["tag".to_string()],
                        }),
                        operation: Some(write::Operation::Update(Document {
                            name: doc_ref.to_string(),
                            fields: HashMap::from([("tag".to_string(), Value::string("done"))]),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                None,
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(results[0].transform_results.is_empty());
        assert_eq!(
            results[1].transform_results,
            vec![Value::integer(5), Value::timestamp(commit_ts)]
        );
        assert!(results[2].transform_results.is_empty());

        let stored = db
            .get_doc(&doc_ref, ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.fields,
            HashMap::from([
                ("v".to_string(), Value::integer(5)),
                ("ts".to_string(), Value::timestamp(commit_ts)),
                ("tag".to_string(), Value::string("done")),
            ])
        );

        // Cleared-database ordering: `write_with_version` validates before `is_cleared()`, whereas
        // `commit_non_transactional` checks `is_cleared()` before validating writes.
        project.clear_database(&root).await;
        let invalid_write = Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_ref.to_string(),
                fields: HashMap::from([("__bad__".to_string(), Value::integer(1))]),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(matches!(
            db.write(invalid_write.clone()).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            db.commit(vec![invalid_write], None).await,
            Err(GenericDatabaseError::Aborted(_))
        ));
    }

    fn delete_write(doc_ref: &DocumentRef, precondition: Option<Precondition>) -> Write {
        Write {
            current_document: precondition,
            operation: Some(write::Operation::Delete(doc_ref.to_string())),
            ..Default::default()
        }
    }

    fn update_write(
        doc_ref: &DocumentRef,
        fields: HashMap<String, Value>,
        precondition: Option<Precondition>,
    ) -> Write {
        Write {
            current_document: precondition,
            operation: Some(write::Operation::Update(Document {
                name: doc_ref.to_string(),
                fields,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn rw_txn_opts() -> TransactionOptions {
        TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        }
    }

    #[tokio::test]
    async fn missing_and_already_deleted_doc_deletes_avoid_tombstones_allocations_and_events() {
        use tokio::sync::broadcast::error::TryRecvError;

        let (_project, _root, db) = test_db().await;
        let mut events = db.subscribe();
        let never_ref: DocumentRef = "projects/p/databases/(default)/documents/c/never"
            .parse()
            .unwrap();
        let live_ref: DocumentRef = "projects/p/databases/(default)/documents/c/live"
            .parse()
            .unwrap();

        // Repeated deletes of a never-existed document advance `commit_time`, return
        // `WriteResult.update_time == None`, emit no `DatabaseEvent`, and allocate no
        // `Collection` or `DocumentMeta`.
        let mut prev_commit_ts = None;
        for i in 0..5 {
            let precondition = (i == 2).then_some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(false)),
            });
            let (commit_ts, results) = db
                .commit(vec![delete_write(&never_ref, precondition)], None)
                .await
                .unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].update_time, None);
            if let Some(prev) = prev_commit_ts {
                assert!(
                    commit_ts > prev,
                    "commit_time must advance on consecutive no-op deletes"
                );
            }
            prev_commit_ts = Some(commit_ts);
        }
        db.delete_doc(&never_ref, None).await.unwrap();
        assert!(
            db.collections.read().await.is_empty(),
            "deleting never-existed docs must not allocate phantom collections or DocumentMeta"
        );
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

        // Preconditions on never-existed docs remain unchanged and allocate no storage.
        let err = db
            .delete_doc(
                &never_ref,
                Some(Precondition {
                    condition_type: Some(precondition::ConditionType::Exists(true)),
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::NotFound(_)));
        let err = db
            .delete_doc(
                &never_ref,
                Some(Precondition {
                    condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                        seconds: 100,
                        nanos:   0,
                    })),
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::FailedPrecondition(_)));
        assert!(db.collections.read().await.is_empty());
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

        // Create and delete `c/live`: the real delete emits an event, records a single tombstone,
        // and still returns `WriteResult.update_time == None`.
        let created = db
            .update_doc(
                &live_ref,
                HashMap::from([("v".to_string(), Value::integer(1))]),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let create_ts = created.update_time.unwrap();
        let create_ev = events.try_recv().expect("create event");
        assert!(create_ev.updates.contains_key(&live_ref));

        let (del_ts, del_results) = db
            .commit(vec![delete_write(&live_ref, None)], None)
            .await
            .unwrap();
        assert_eq!(del_results.len(), 1);
        assert_eq!(del_results[0].update_time, None);
        assert!(del_ts > create_ts);
        let del_ev = events.try_recv().expect("real delete event");
        assert!(matches!(
            del_ev.updates.get(&live_ref),
            Some(super::DocumentVersion::Deleted(_))
        ));

        let meta = db.peek_doc_meta(&live_ref).await.unwrap().unwrap();
        assert_eq!(
            meta.read_latest_uncontended(|c| (c.version_count(), c.last_updated())),
            (2, Some(del_ts))
        );

        // Repeated deletes on the already-deleted doc advance `commit_time`, keep
        // `WriteResult.update_time == None`, do NOT grow `versions`, do NOT advance
        // `last_updated()`, and emit NO `DatabaseEvent`.
        let mut prev_noop_ts = del_ts;
        for i in 0..5 {
            let precondition = (i == 1).then_some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(false)),
            });
            let (noop_ts, results) = db
                .commit(vec![delete_write(&live_ref, precondition)], None)
                .await
                .unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].update_time, None);
            assert!(noop_ts > prev_noop_ts);
            prev_noop_ts = noop_ts;
        }
        assert_eq!(
            meta.read_latest_uncontended(|c| (c.version_count(), c.last_updated())),
            (2, Some(del_ts))
        );
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

        // Preconditions on already-deleted docs remain unchanged.
        let err = db
            .delete_doc(
                &live_ref,
                Some(Precondition {
                    condition_type: Some(precondition::ConditionType::Exists(true)),
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::NotFound(_)));
        let err = db
            .delete_doc(
                &live_ref,
                Some(Precondition {
                    condition_type: Some(precondition::ConditionType::UpdateTime(del_ts)),
                }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::FailedPrecondition(_)));
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn multi_write_batches_cancel_absent_update_delete_and_preserve_recreate_create_time() {
        use tokio::sync::broadcast::error::TryRecvError;

        let (_project, _root, db) = test_db().await;
        let mut events = db.subscribe();
        let absent_ref: DocumentRef = "projects/p/databases/(default)/documents/c/absent"
            .parse()
            .unwrap();
        let reused_ref: DocumentRef = "projects/p/databases/(default)/documents/c/reused"
            .parse()
            .unwrap();
        let existing_ref: DocumentRef = "projects/p/databases/(default)/documents/c/existing"
            .parse()
            .unwrap();
        let real_ref: DocumentRef = "projects/p/databases/(default)/documents/c/real"
            .parse()
            .unwrap();
        let ghost_ref: DocumentRef = "projects/p/databases/(default)/documents/c/ghost"
            .parse()
            .unwrap();

        // 1. `[Update(D), Delete(D, Exists(true))]` on a never-existed doc evaluates transforms and
        //    intra-batch preconditions, cancels pending state to `None`, allocates no
        //    Collection/DocumentMeta, and emits no DatabaseEvent.
        let (ts, results) = db
            .commit(
                vec![
                    Write {
                        update_transforms: vec![FieldTransform {
                            field_path:     "ts".to_string(),
                            transform_type: Some(TransformType::SetToServerValue(
                                ServerValue::RequestTime as i32,
                            )),
                        }],
                        operation: Some(write::Operation::Update(Document {
                            name: absent_ref.to_string(),
                            fields: HashMap::from([("v".to_string(), Value::integer(1))]),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                    delete_write(
                        &absent_ref,
                        Some(Precondition {
                            condition_type: Some(precondition::ConditionType::Exists(true)),
                        }),
                    ),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].update_time, None);
        assert_eq!(results[0].transform_results, vec![Value::timestamp(ts)]);
        assert_eq!(results[1].update_time, None);
        assert!(db.collections.read().await.is_empty());
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

        // `[Update(D), Delete(D, Exists(false))]` sees staged Update -> AlreadyExists, zero alloc.
        let err = db
            .commit(
                vec![
                    update_write(
                        &absent_ref,
                        HashMap::from([("v".to_string(), Value::integer(1))]),
                        None,
                    ),
                    delete_write(
                        &absent_ref,
                        Some(Precondition {
                            condition_type: Some(precondition::ConditionType::Exists(false)),
                        }),
                    ),
                ],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::AlreadyExists(_)));
        assert!(db.collections.read().await.is_empty());

        // 2. `[Update(D), Delete(D)]` on an already-deleted doc cancels pending state to `None`
        //    without growing `versions`, advancing `last_updated()`, or emitting a `DatabaseEvent`.
        db.update_doc(
            &reused_ref,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let (del_ts, _) = db
            .commit(vec![delete_write(&reused_ref, None)], None)
            .await
            .unwrap();
        while events.try_recv().is_ok() {}

        let (_batch_ts, results) = db
            .commit(
                vec![
                    update_write(
                        &reused_ref,
                        HashMap::from([("v".to_string(), Value::integer(2))]),
                        None,
                    ),
                    delete_write(&reused_ref, None),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(results[0].update_time, None);
        assert_eq!(results[1].update_time, None);
        let reused_meta = db.peek_doc_meta(&reused_ref).await.unwrap().unwrap();
        assert_eq!(
            reused_meta.read_latest_uncontended(|c| (c.version_count(), c.last_updated())),
            (2, Some(del_ts))
        );
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

        // 3. `[Delete(D), Update(D, same_fields)]` on an existing doc MUST still advance
        //    `create_time` and `update_time`, report `Some(recreate_ts)` on both WriteResults, and
        //    emit a `DatabaseEvent`.
        let same_fields = HashMap::from([("k".to_string(), Value::string("same"))]);
        let initial = db
            .update_doc(&existing_ref, same_fields.clone(), None, None, None)
            .await
            .unwrap();
        let orig_create = initial.create_time.unwrap();
        while events.try_recv().is_ok() {}

        let (recreate_ts, results) = db
            .commit(
                vec![
                    delete_write(&existing_ref, None),
                    update_write(&existing_ref, same_fields.clone(), None),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(results[0].update_time, Some(recreate_ts));
        assert_eq!(results[1].update_time, Some(recreate_ts));
        let recreated = db
            .get_doc(&existing_ref, ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recreated.create_time, recreate_ts);
        assert_eq!(recreated.update_time, recreate_ts);
        assert!(recreated.create_time > orig_create);
        assert_eq!(recreated.fields, same_fields);
        let recreate_ev = events.try_recv().expect("recreate event");
        assert!(matches!(
            recreate_ev.updates.get(&existing_ref),
            Some(super::DocumentVersion::Stored(_))
        ));

        // 3b. `[Update(D, diff), Update(D, same_fields)]` reverts to the persisted state: both
        //     WriteResults report `Some(recreate_ts)` (unchanged update_time) and emit no event.
        let (_, revert_results) = db
            .commit(
                vec![
                    update_write(
                        &existing_ref,
                        HashMap::from([("k".to_string(), Value::string("diff"))]),
                        None,
                    ),
                    update_write(&existing_ref, same_fields.clone(), None),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(revert_results[0].update_time, Some(recreate_ts));
        assert_eq!(revert_results[1].update_time, Some(recreate_ts));
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));

        // 4. `[Delete(D, Exists(true)), Delete(D, Exists(true))]` fails atomically on the 2nd write
        //    leaving `existing_ref` intact; `[Delete(D), Delete(D)]` still deletes it once.
        let err = db
            .commit(
                vec![
                    delete_write(
                        &existing_ref,
                        Some(Precondition {
                            condition_type: Some(precondition::ConditionType::Exists(true)),
                        }),
                    ),
                    delete_write(
                        &existing_ref,
                        Some(Precondition {
                            condition_type: Some(precondition::ConditionType::Exists(true)),
                        }),
                    ),
                ],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::NotFound(_)));
        assert!(
            db.get_doc(&existing_ref, ReadConsistency::Default)
                .await
                .unwrap()
                .is_some()
        );

        let (_, del2_results) = db
            .commit(
                vec![
                    delete_write(&existing_ref, None),
                    delete_write(&existing_ref, None),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(del2_results[0].update_time, None);
        assert_eq!(del2_results[1].update_time, None);
        assert!(
            db.get_doc(&existing_ref, ReadConsistency::Default)
                .await
                .unwrap()
                .is_none()
        );
        let del2_ev = events.try_recv().expect("double-delete event");
        assert!(matches!(
            del2_ev.updates.get(&existing_ref),
            Some(super::DocumentVersion::Deleted(_))
        ));

        // 5. Mixed multi-doc batch `[Update(real), Delete(ghost)]` emits event only for `real` and
        //    does not allocate `DocumentMeta` for `ghost`.
        let (mixed_ts, mixed_results) = db
            .commit(
                vec![
                    update_write(
                        &real_ref,
                        HashMap::from([("ok".to_string(), Value::boolean(true))]),
                        None,
                    ),
                    delete_write(&ghost_ref, None),
                ],
                None,
            )
            .await
            .unwrap();
        assert_eq!(mixed_results[0].update_time, Some(mixed_ts));
        assert_eq!(mixed_results[1].update_time, None);
        assert!(db.peek_doc_meta(&ghost_ref).await.unwrap().is_none());
        let mixed_ev = events.try_recv().expect("mixed event");
        assert_eq!(mixed_ev.updates.len(), 1);
        assert!(mixed_ev.updates.contains_key(&real_ref));
    }

    #[tokio::test]
    async fn optimistic_txn_reading_missing_doc_ignores_concurrent_missing_doc_deletes() {
        let (_project, _root, db) = test_db().await;
        db.set_config(true, true);

        let never_ref: DocumentRef = "projects/p/databases/(default)/documents/opt/never"
            .parse()
            .unwrap();
        let deleted_ref: DocumentRef = "projects/p/databases/(default)/documents/opt/deleted"
            .parse()
            .unwrap();
        let other_ref: DocumentRef = "projects/p/databases/(default)/documents/opt/other"
            .parse()
            .unwrap();
        let existing_ref: DocumentRef = "projects/p/databases/(default)/documents/opt/existing"
            .parse()
            .unwrap();

        // Prepare an already-deleted document and a live control document.
        db.update_doc(
            &deleted_ref,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        db.delete_doc(&deleted_ref, None).await.unwrap();
        db.update_doc(
            &existing_ref,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Both never-existed and already-deleted docs: concurrent non-txn AND transactional
        // missing-doc deletes must NOT abort an optimistic txn that read the missing doc.
        for target_ref in [&never_ref, &deleted_ref] {
            // Subcase A: txn reads missing `target_ref`, concurrent deletes run, txn writes
            // `other`.
            let txn_a = db.new_txn(rw_txn_opts()).await.unwrap();
            assert!(
                db.get_doc(target_ref, ReadConsistency::Transaction(txn_a))
                    .await
                    .unwrap()
                    .is_none()
            );

            db.delete_doc(target_ref, None).await.unwrap();
            let txn_del = db.new_txn(rw_txn_opts()).await.unwrap();
            db.commit(vec![delete_write(target_ref, None)], Some(txn_del))
                .await
                .unwrap();

            db.commit(
                vec![update_write(
                    &other_ref,
                    HashMap::from([("from".to_string(), Value::string(target_ref.to_string()))]),
                    None,
                )],
                Some(txn_a),
            )
            .await
            .expect("missing-doc delete must not cause optimistic write-skew");

            // Subcase B: txn reads missing `target_ref`, concurrent delete runs, txn creates
            // `target_ref` itself.
            let txn_b = db.new_txn(rw_txn_opts()).await.unwrap();
            assert!(
                db.get_doc(target_ref, ReadConsistency::Transaction(txn_b))
                    .await
                    .unwrap()
                    .is_none()
            );
            db.delete_doc(target_ref, None).await.unwrap();
            db.commit(
                vec![update_write(
                    target_ref,
                    HashMap::from([("created".to_string(), Value::boolean(true))]),
                    None,
                )],
                Some(txn_b),
            )
            .await
            .expect("creating missing doc after concurrent no-op delete must succeed");
            // Reset back to deleted for clean state.
            db.delete_doc(target_ref, None).await.unwrap();
        }

        // Control 1: concurrent creation of a missing doc DOES abort optimistic txn.
        let control_missing: DocumentRef = "projects/p/databases/(default)/documents/opt/ctrl_miss"
            .parse()
            .unwrap();
        let txn_ctrl1 = db.new_txn(rw_txn_opts()).await.unwrap();
        assert!(
            db.get_doc(&control_missing, ReadConsistency::Transaction(txn_ctrl1))
                .await
                .unwrap()
                .is_none()
        );
        db.update_doc(
            &control_missing,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let err = db
            .commit(
                vec![update_write(
                    &other_ref,
                    HashMap::from([("v".to_string(), Value::integer(99))]),
                    None,
                )],
                Some(txn_ctrl1),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::Aborted(_)));

        // Control 2: concurrent deletion of an EXISTING doc DOES abort optimistic txn.
        let txn_ctrl2 = db.new_txn(rw_txn_opts()).await.unwrap();
        assert!(
            db.get_doc(&existing_ref, ReadConsistency::Transaction(txn_ctrl2))
                .await
                .unwrap()
                .is_some()
        );
        db.delete_doc(&existing_ref, None).await.unwrap();
        let err = db
            .commit(
                vec![update_write(
                    &other_ref,
                    HashMap::from([("v".to_string(), Value::integer(100))]),
                    None,
                )],
                Some(txn_ctrl2),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, GenericDatabaseError::Aborted(_)));
    }

    #[tokio::test]
    async fn pessimistic_txn_missing_doc_delete_still_blocks_and_wounds() {
        use std::time::Duration;

        let (_project, _root, db) = test_db().await;
        db.set_config(false, false);

        let never_ref: DocumentRef = "projects/p/databases/(default)/documents/pess/never"
            .parse()
            .unwrap();
        let deleted_ref: DocumentRef = "projects/p/databases/(default)/documents/pess/deleted"
            .parse()
            .unwrap();
        let unrelated_ref: DocumentRef = "projects/p/databases/(default)/documents/pess/unrelated"
            .parse()
            .unwrap();

        db.update_doc(
            &deleted_ref,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        db.delete_doc(&deleted_ref, None).await.unwrap();

        // 1. Non-txn `delete(D)` blocks while pessimistic Txn A holds read lock on missing `D`,
        //    then runs after A commits (deleting the doc A just created).
        for doc_ref in [&never_ref, &deleted_ref] {
            let txn_a = db.new_txn(rw_txn_opts()).await.unwrap();
            assert!(
                db.get_doc(doc_ref, ReadConsistency::Transaction(txn_a))
                    .await
                    .unwrap()
                    .is_none()
            );

            let db_bg = std::sync::Arc::clone(&db);
            let del_ref = doc_ref.clone();
            let del_handle = tokio::spawn(async move {
                db_bg.commit(vec![delete_write(&del_ref, None)], None).await
            });

            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                !del_handle.is_finished(),
                "non-txn missing-doc delete must block behind pessimistic read lock"
            );

            let (a_ts, _) = db
                .commit(
                    vec![update_write(
                        doc_ref,
                        HashMap::from([("v".to_string(), Value::integer(7))]),
                        None,
                    )],
                    Some(txn_a),
                )
                .await
                .unwrap();

            let (del_ts, del_results) = del_handle.await.unwrap().unwrap();
            assert!(del_ts > a_ts);
            assert_eq!(del_results[0].update_time, None);
            assert!(
                db.get_doc(doc_ref, ReadConsistency::Default)
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        // 2. Cloud probe b2(i): older A & younger B read missing D; B commits delete(D) (blocks
        //    behind A); A upgrades to write D -> A commits, B aborts.
        let d_b2_i: DocumentRef = "projects/p/databases/(default)/documents/pess/b2_i"
            .parse()
            .unwrap();
        let txn_a = db.new_txn(rw_txn_opts()).await.unwrap();
        let txn_b = db.new_txn(rw_txn_opts()).await.unwrap();
        assert!(
            db.get_doc(&d_b2_i, ReadConsistency::Transaction(txn_a))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_doc(&d_b2_i, ReadConsistency::Transaction(txn_b))
                .await
                .unwrap()
                .is_none()
        );
        let db_bg = std::sync::Arc::clone(&db);
        let d_bg = d_b2_i.clone();
        let b_handle = tokio::spawn(async move {
            db_bg
                .commit(vec![delete_write(&d_bg, None)], Some(txn_b))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!b_handle.is_finished());

        db.commit(
            vec![update_write(
                &d_b2_i,
                HashMap::from([("winner".to_string(), Value::string("A"))]),
                None,
            )],
            Some(txn_a),
        )
        .await
        .unwrap();
        let b_err = b_handle.await.unwrap().unwrap_err();
        assert!(matches!(b_err, GenericDatabaseError::Aborted(_)));

        // 3. Cloud probe b2(ii): older A & younger B read missing D; B commits delete(D) (blocks);
        //    A writes unrelated E and commits -> A commits, then B unblocks and commits cleanly.
        let d_b2_ii: DocumentRef = "projects/p/databases/(default)/documents/pess/b2_ii"
            .parse()
            .unwrap();
        let txn_a = db.new_txn(rw_txn_opts()).await.unwrap();
        let txn_b = db.new_txn(rw_txn_opts()).await.unwrap();
        assert!(
            db.get_doc(&d_b2_ii, ReadConsistency::Transaction(txn_a))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_doc(&d_b2_ii, ReadConsistency::Transaction(txn_b))
                .await
                .unwrap()
                .is_none()
        );
        let db_bg = std::sync::Arc::clone(&db);
        let d_bg = d_b2_ii.clone();
        let b_handle = tokio::spawn(async move {
            db_bg
                .commit(vec![delete_write(&d_bg, None)], Some(txn_b))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!b_handle.is_finished());

        db.commit(
            vec![update_write(
                &unrelated_ref,
                HashMap::from([("from".to_string(), Value::string("b2_ii"))]),
                None,
            )],
            Some(txn_a),
        )
        .await
        .unwrap();
        let (_, b_results) = b_handle.await.unwrap().unwrap();
        assert_eq!(b_results[0].update_time, None);
        assert!(db.peek_doc_meta(&d_b2_ii).await.unwrap().is_none());

        // 4. Cloud probe b2(iii): older A & younger B read missing D; older A commits delete(D) ->
        //    A commits and wounds younger B even when B writes unrelated E.
        let d_b2_iii: DocumentRef = "projects/p/databases/(default)/documents/pess/b2_iii"
            .parse()
            .unwrap();
        let txn_a = db.new_txn(rw_txn_opts()).await.unwrap();
        let txn_b = db.new_txn(rw_txn_opts()).await.unwrap();
        assert!(
            db.get_doc(&d_b2_iii, ReadConsistency::Transaction(txn_a))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_doc(&d_b2_iii, ReadConsistency::Transaction(txn_b))
                .await
                .unwrap()
                .is_none()
        );

        let (_, a_results) = db
            .commit(vec![delete_write(&d_b2_iii, None)], Some(txn_a))
            .await
            .unwrap();
        assert_eq!(a_results[0].update_time, None);
        assert!(db.peek_doc_meta(&d_b2_iii).await.unwrap().is_none());

        let b_err = db
            .commit(
                vec![update_write(
                    &unrelated_ref,
                    HashMap::from([("from".to_string(), Value::string("b2_iii"))]),
                    None,
                )],
                Some(txn_b),
            )
            .await
            .unwrap_err();
        assert!(matches!(b_err, GenericDatabaseError::Aborted(_)));
    }
}
