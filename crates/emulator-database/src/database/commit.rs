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
    lock::{CommitSeal, LockPriority, ReadObservation},
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
        let session = self
            .locks
            .new_session(None, LockPriority::NextSeq, false, false);
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
        let deadline = Instant::now() + self.project.timeouts.write;
        let mut priority = LockPriority::NextSeq;
        let mut wound_retries: u32 = 0;

        loop {
            let session = self.locks.new_session(None, priority, true, true);
            let sealed = match session
                .acquire_writes_all(&validated.target_docs, deadline)
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
                Err(CommitError::Aborted(AbortReason::Wounded { .. })) => {
                    if let Some(seq) = session.root_seq() {
                        priority = LockPriority::Assigned(seq);
                    }
                    session.close();
                    if self.is_cleared() {
                        return Err(AbortReason::DatabaseCleared.into());
                    }
                    if Instant::now() >= deadline {
                        return Err(AbortReason::WriteTimeout.into());
                    }
                    if wound_retries == 0 {
                        tokio::task::yield_now().await;
                    } else {
                        let backoff =
                            Duration::from_millis((1u64 << (wound_retries - 1).min(4)).min(25));
                        tokio::time::sleep_until((Instant::now() + backoff).min(deadline)).await;
                    }
                    wound_retries = wound_retries.saturating_add(1);
                    if self.is_cleared() {
                        return Err(AbortReason::DatabaseCleared.into());
                    }
                    if Instant::now() >= deadline {
                        return Err(AbortReason::WriteTimeout.into());
                    }
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
        let doc_refs: Vec<DocumentRef> = seal.doc_refs().cloned().collect();
        let mut docs = BTreeMap::new();
        for doc_ref in doc_refs {
            docs.insert(doc_ref.clone(), self.get_doc_meta(&doc_ref).await?);
        }

        DocumentMeta::apply_sealed_commit(seal, docs, |commit_time, contents_map| {
            if self.is_cleared() {
                return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
            }

            let mut staged_docs: BTreeMap<DocumentRef, StagedDocument<'_>> = contents_map
                .iter_mut()
                .map(|(name, doc)| (name.clone(), StagedDocument::new(name.clone(), doc)))
                .collect();
            let mut write_results = Vec::with_capacity(writes.len());

            for write in writes {
                let staged = staged_docs.get_mut(&write.doc_ref).ok_or_else(|| {
                    GenericDatabaseError::internal(format!(
                        "missing staged document for {}",
                        write.doc_ref
                    ))
                })?;
                write_results.push(self.stage_write(write, staged, commit_time)?);
            }

            let mut updates = HashMap::new();
            let mut single_stored = None;
            let track_single = staged_docs.len() == 1;
            for (name, staged) in staged_docs {
                if let Some(version) = staged.pending {
                    staged.doc.maybe_add_version(&version)?;
                    updates.insert(name, version);
                }
                if track_single {
                    single_stored = staged.doc.current_version().cloned();
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
    ) -> Result<WriteResult> {
        let ValidatedWrite {
            doc_ref: _,
            precondition,
            operation,
        } = write;

        if let Some(condition) = precondition {
            document.check_precondition(condition)?;
        }
        let mut transform_results = vec![];

        let update_time = match operation {
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
                document.stage_update(fields, commit_time)
            }
            ValidatedWriteOperation::Delete => {
                debug!(name = %document.name, "Delete");
                document.stage_delete(commit_time);
                commit_time
            }
        };
        Ok(WriteResult {
            update_time: Some(update_time),
            transform_results,
        })
    }
}

struct StagedDocument<'a> {
    name:    DocumentRef,
    doc:     &'a mut DocumentContents,
    pending: Option<DocumentVersion>,
}

impl<'a> StagedDocument<'a> {
    fn new(name: DocumentRef, doc: &'a mut DocumentContents) -> Self {
        Self {
            name,
            doc,
            pending: None,
        }
    }

    fn latest_version(&self) -> Option<&DocumentVersion> {
        self.pending.as_ref().or_else(|| self.doc.latest_version())
    }

    fn current_version(&self) -> Option<&Arc<StoredDocumentVersion>> {
        self.latest_version()
            .and_then(DocumentVersion::stored_document)
    }

    fn check_precondition(&self, condition: DocumentPrecondition) -> Result<()> {
        check_version_precondition(&self.name, self.latest_version(), condition)
    }

    fn stage_update(
        &mut self,
        fields: HashMap<String, Value>,
        commit_time: Timestamp,
    ) -> Timestamp {
        let (update_time, version) =
            stage_stored_version(&self.name, self.latest_version(), fields, commit_time);
        if let Some(version) = version {
            if let (Some(orig), Some(staged)) =
                (self.doc.current_version(), version.stored_document())
                && orig.create_time == staged.create_time
                && Value::fields_storage_eq(&orig.fields, &staged.fields)
            {
                self.pending = None;
                return orig.update_time;
            }
            self.pending = Some(version);
        }
        update_time
    }

    fn stage_delete(&mut self, delete_time: Timestamp) {
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

    fn test_db() -> (
        &'static FirestoreProject,
        RootRef,
        std::sync::Arc<crate::FirestoreDatabase>,
    ) {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let rt = tokio::runtime::Handle::current();
        let db = tokio::task::block_in_place(|| rt.block_on(project.database(&root)));
        (project, root, db)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transform_only_empty_mask_preserves_existing_fields_while_none_overwrites() {
        let (_project, _root, db) = test_db();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn max_min_checks_precondition_before_not_implemented_and_rejects_bad_operand_prelock() {
        let (_project, _root, db) = test_db();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn overlapping_and_repeated_mask_paths_apply_cleanly_without_losing_values() {
        let (_project, _root, db) = test_db();
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/overlap"
            .parse()
            .unwrap();

        db.update_doc(
            &doc_ref,
            HashMap::from([
                ("keep".to_string(), Value::boolean(true)),
                (
                    "a".to_string(),
                    Value::map(HashMap::from([("old".to_string(), Value::integer(1))])),
                ),
            ]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let updated = db
            .update_doc(
                &doc_ref,
                HashMap::from([(
                    "a".to_string(),
                    Value::map(HashMap::from([(
                        "b".to_string(),
                        Value::map(HashMap::from([("c".to_string(), Value::integer(3))])),
                    )])),
                )]),
                Some(DocumentMask {
                    field_paths: vec![
                        "a".to_string(),
                        "a.b".to_string(),
                        "a.b.c".to_string(),
                        "a".to_string(),
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
                (
                    "a".to_string(),
                    Value::map(HashMap::from([(
                        "b".to_string(),
                        Value::map(HashMap::from([("c".to_string(), Value::integer(3))])),
                    )])),
                ),
            ])
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_preserves_multi_write_order_for_same_document_and_cleared_db_precedence() {
        let (project, root, db) = test_db();
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
}
