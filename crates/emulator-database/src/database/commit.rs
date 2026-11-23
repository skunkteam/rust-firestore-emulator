use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use googleapis::google::{
    firestore::v1::{
        DocumentMask, Value, Write, WriteResult,
        document_transform::field_transform::{ServerValue, TransformType},
        write,
    },
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
    validation,
};
use crate::{
    GenericDatabaseError,
    error::{AbortReason, CommitError, Result},
    unimplemented, unimplemented_collection, unimplemented_option,
};

struct ValidatedCommitWrites {
    target_docs: BTreeSet<DocumentRef>,
    writes:      Vec<(DocumentRef, Write)>,
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
        let name = get_doc_name_from_write(&write)?;
        self.ensure_owns_doc(&name)?;
        validation::validate_write(&write)?;
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }

        let (_, mut results, stored) = self
            .apply_single_non_transactional(name.clone(), vec![(name, write)])
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
            let name = get_doc_name_from_write(&write)?;
            self.ensure_owns_doc(&name)?;
            validation::validate_write(&write)?;
            target_docs.insert(name.clone());
            parsed_writes.push((name, write));
        }
        Ok(ValidatedCommitWrites {
            target_docs,
            writes: parsed_writes,
        })
    }

    async fn apply_single_non_transactional(
        self: &Arc<Self>,
        doc_ref: DocumentRef,
        writes: Vec<(DocumentRef, Write)>,
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
        writes: Vec<(DocumentRef, Write)>,
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
        writes: Vec<(DocumentRef, Write)>,
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

            for (name, write) in writes {
                let staged = staged_docs.get_mut(&name).ok_or_else(|| {
                    GenericDatabaseError::internal(format!("missing staged document for {name}"))
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
        write: Write,
        document: &mut StagedDocument<'_>,
        commit_time: Timestamp,
    ) -> Result<WriteResult> {
        let Write {
            update_mask,
            update_transforms,
            current_document,
            operation,
        } = write;

        let operation = operation.ok_or(GenericDatabaseError::invalid_argument(
            "missing operation in write",
        ))?;
        let condition = current_document
            .and_then(|cd| cd.condition_type)
            .map(Into::into);
        if let Some(condition) = condition {
            document.check_precondition(condition)?;
        }
        let mut transform_results = vec![];

        use write::Operation::*;
        let update_time = match operation {
            Update(doc) => {
                debug!(name = %document.name, "Update");
                validation::validate_writable_fields(&doc.fields)?;
                if let Some(mask) = &update_mask {
                    validation::validate_update_mask(mask)?;
                }
                validation::validate_field_transforms(&update_transforms)?;

                let mut fields = if let Some(mask) = update_mask {
                    apply_updates(
                        document.current_version().map(|v| &v.fields),
                        mask,
                        &doc.fields,
                    )?
                } else {
                    doc.fields
                };
                for tf in update_transforms {
                    transform_results.push(apply_transform(
                        &mut fields,
                        tf.field_path,
                        tf.transform_type.ok_or_else(|| {
                            GenericDatabaseError::invalid_argument(
                                "empty transform_type in field_transform",
                            )
                        })?,
                        commit_time,
                    )?);
                }
                validation::validate_writable_fields(&fields)?;
                document.stage_update(fields, commit_time)
            }
            Delete(_) => {
                debug!(name = %document.name, "Delete");

                unimplemented_option!(update_mask);
                unimplemented_collection!(update_transforms);
                document.stage_delete(commit_time);
                commit_time
            }
            Transform(_) => unimplemented!("transform"),
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
    mask: DocumentMask,
    updated_values: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, GenericDatabaseError> {
    let mut fields = existing_fields.cloned().unwrap_or_default();
    for field_path in mask.field_paths {
        let field_path: FieldPath = field_path.parse()?;
        match field_path.get_value(updated_values) {
            Some(new_value) => field_path.set_value(&mut fields, new_value.clone()),
            None => {
                field_path.delete_value(&mut fields);
            }
        }
    }
    Ok(fields)
}

fn apply_transform(
    fields: &mut HashMap<String, Value>,
    path: String,
    transform: TransformType,
    commit_time: Timestamp,
) -> Result<Value> {
    let field_path: FieldPath = path.parse()?;
    let result = match transform {
        TransformType::SetToServerValue(code) => {
            match ServerValue::try_from(code).map_err(|_| {
                GenericDatabaseError::invalid_argument(format!("invalid server_value: {code}"))
            })? {
                ServerValue::RequestTime => {
                    let new_value = Value::timestamp(commit_time);
                    field_path.set_value(fields, new_value.clone());
                    new_value
                }
                ServerValue::Unspecified => {
                    return Err(GenericDatabaseError::invalid_argument(
                        "ServerValue type must be specified for set_to_server_value \
                         transformation.",
                    ));
                }
            }
        }
        TransformType::Increment(value) => field_path
            .transform_value(fields, |cur| cur.unwrap_or_else(Value::null) + value)
            .clone(),
        TransformType::Maximum(_) => unimplemented!("TransformType::Maximum"),
        TransformType::Minimum(_) => unimplemented!("TransformType::Minimum"),
        TransformType::AppendMissingElements(elements) => {
            field_path.transform_value(fields, |cur_value| {
                let mut array = cur_value.and_then(Value::into_array).unwrap_or_default();
                for el in elements.values {
                    if !array.contains(&el) {
                        array.push(el);
                    }
                }
                Value::array(array)
            });
            Value::null()
        }
        TransformType::RemoveAllFromArray(elements) => {
            field_path.transform_value(fields, |cur_value| {
                match cur_value.and_then(Value::into_array) {
                    Some(mut array) => {
                        array.retain(|v| !elements.values.contains(v));
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

fn get_doc_name_from_write(write: &Write) -> Result<DocumentRef> {
    let operation = write
        .operation
        .as_ref()
        .ok_or(GenericDatabaseError::not_implemented(
            "missing operation in write",
        ))?;
    use write::Operation::*;
    let name: &str = match operation {
        Update(doc) => &doc.name,
        Delete(name) => name,
        Transform(trans) => &trans.document,
    };
    name.parse()
}
