mod registry;

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, LazyLock, Weak},
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::Mutex;

#[cfg(test)]
pub(crate) use self::registry::MAX_CLOSED_TXNS;
pub(crate) use self::registry::{CommitTake, RunningTransactions, TransactionId};
use super::{
    FirestoreDatabase,
    document::{DocumentContents, StoredDocumentVersion},
    lock::{LockManager, LockPriority, LockSession, SessionKind, WriteLease},
    reference::DocumentRef,
};
use crate::{
    GenericDatabaseError,
    database::query::Query,
    error::{AbortReason, CommitError, Result},
};

#[derive(Debug)]
pub(crate) enum Transaction {
    ReadWrite(Arc<ReadWriteTransaction>),
    ReadOnly(Arc<ReadOnlyTransaction>),
}

impl Transaction {
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        match self {
            Transaction::ReadWrite(txn) => txn.read_doc(name).await,
            Transaction::ReadOnly(txn) => txn.read_doc(name).await,
        }
    }

    pub(crate) fn as_read_write(&self) -> Option<&ReadWriteTransaction> {
        if let Self::ReadWrite(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub(crate) fn read_time(&self) -> Option<Timestamp> {
        match self {
            Transaction::ReadWrite(txn) => match &txn.mode {
                ConcurrencyMode::Pessimistic(_) => None,
                ConcurrencyMode::Optimistic(state) => Some(*state.read_time),
            },
            Transaction::ReadOnly(txn) => Some(txn.read_time),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PessimisticState {
    database: Weak<FirestoreDatabase>,
    session:  Arc<LockSession>,
}

impl PessimisticState {
    /// Acquires a [`ReadLease`](super::lock::ReadLease) on the document, reads the latest version
    /// (if present) via a brief synchronous storage borrow, and retains the read hold inside
    /// [`LockManager`] along with the observed `last_updated` timestamp for upgrade validation.
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        let db = self
            .database
            .upgrade()
            .ok_or(AbortReason::DatabaseDropped)?;
        if db.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        db.ensure_owns_doc(name)?;
        let lease = self.session.acquire_read(name).await?;
        let meta = db.peek_doc_meta(name).await?;
        let (version, observed_update_time) = match meta {
            Some(meta) => {
                meta.read_with(&lease, |c| (c.current_version().cloned(), c.last_updated()))?
            }
            None => {
                lease.check_active()?;
                (None, None)
            }
        };
        self.session.retain_read(lease, observed_update_time)?;
        Ok(version)
    }

    pub(crate) fn session(&self) -> &Arc<LockSession> {
        &self.session
    }
}

#[derive(Debug)]
pub(crate) struct ExecutedQuery {
    pub(crate) query:    Query,
    pub(crate) snapshot: Vec<(DocumentRef, Timestamp)>,
}

#[derive(Debug)]
pub(crate) struct OptimisticState {
    database: Weak<FirestoreDatabase>,
    session: Arc<LockSession>,
    pub(crate) observed_docs: Mutex<HashSet<DocumentRef>>,
    pub(crate) executed_queries: Mutex<Vec<ExecutedQuery>>,
    pub(crate) read_time: LazyLock<Timestamp>,
}

impl OptimisticState {
    /// Reads the document snapshot at the transaction's pinned optimistic `read_time` without
    /// entering lock wait queues and records the path in `observed_docs` for commit-time write-skew
    /// validation.
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        let db = self
            .database
            .upgrade()
            .ok_or(AbortReason::DatabaseDropped)?;
        if db.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        db.ensure_owns_doc(name)?;
        let rt = *self.read_time;
        let version = db
            .peek_doc_meta(name)
            .await?
            .and_then(|meta| meta.read_mvcc_at(rt));
        self.observed_docs.lock().await.insert(name.clone());
        Ok(version)
    }

    pub(crate) async fn check_concurrency_violations(
        &self,
        held_write_leases: &BTreeMap<DocumentRef, WriteLease>,
    ) -> std::result::Result<(), CommitError> {
        let db = self
            .database
            .upgrade()
            .ok_or(AbortReason::DatabaseDropped)?;
        if db.is_cleared() {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        self.session.check_active()?;
        let rt = *self.read_time;

        // VERIFY OBSERVED DOCS (held targets via WriteLease; unheld docs queue-free)
        {
            let observed = self.observed_docs.lock().await;
            for name in observed.iter() {
                let meta = db.peek_doc_meta(name).await?;
                let check_time = if let Some(lease) = held_write_leases.get(name) {
                    match meta {
                        Some(meta) => {
                            meta.read_with_write_lease(lease, DocumentContents::last_updated)?
                        }
                        None => {
                            lease.check_active()?;
                            None
                        }
                    }
                } else {
                    meta.and_then(|m| m.read_latest_uncontended(DocumentContents::last_updated))
                };
                if check_time.is_some_and(|time| time > rt) {
                    return Err(CommitError::Aborted(AbortReason::OptimisticWriteSkew));
                }
            }
        }

        // VERIFY QUERIES
        {
            self.session.check_active()?;
            let mut executed_queries = self.executed_queries.lock().await;
            for ExecutedQuery { query, snapshot } in executed_queries.iter_mut() {
                let query_res = query.once_for_validation(&db, held_write_leases).await;
                self.session.check_active()?;
                let (_, result_docs) = match query_res {
                    Ok(res) => res,
                    Err(CommitError::Aborted(reason)) => return Err(CommitError::Aborted(reason)),
                    Err(CommitError::Other(err @ GenericDatabaseError::Internal(_))) => {
                        return Err(CommitError::Other(err));
                    }
                    Err(CommitError::Other(_)) => {
                        return Err(CommitError::Aborted(AbortReason::OptimisticWriteSkew));
                    }
                };
                if result_docs.len() != snapshot.len()
                    || result_docs
                        .iter()
                        .zip(snapshot.iter())
                        .any(|(m, (name, update_time))| {
                            m.version.name != *name
                                || m.version.update_time != *update_time
                                || m.version.update_time > rt
                        })
                {
                    return Err(CommitError::Aborted(AbortReason::OptimisticWriteSkew));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum ConcurrencyMode {
    Pessimistic(PessimisticState),
    Optimistic(Box<OptimisticState>),
}

impl ConcurrencyMode {
    fn new(
        id: TransactionId,
        database: Weak<FirestoreDatabase>,
        locks: &Arc<LockManager>,
        priority: LockPriority,
        optimistic_concurrency: bool,
    ) -> Self {
        let kind = if optimistic_concurrency {
            SessionKind::OptimisticTxn
        } else {
            SessionKind::PessimisticTxn
        };
        let session = locks.new_session(Some(id), kind, priority, true, true);
        if optimistic_concurrency {
            ConcurrencyMode::Optimistic(Box::new(OptimisticState {
                database,
                session,
                observed_docs: Default::default(),
                executed_queries: Default::default(),
                read_time: LazyLock::new(Timestamp::now),
            }))
        } else {
            ConcurrencyMode::Pessimistic(PessimisticState { database, session })
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReadWriteTransaction {
    pub(crate) id:   TransactionId,
    pub(crate) mode: ConcurrencyMode,
}

impl ReadWriteTransaction {
    fn new(
        id: TransactionId,
        database: Weak<FirestoreDatabase>,
        locks: &Arc<LockManager>,
        priority: LockPriority,
        optimistic_concurrency: bool,
    ) -> Self {
        ReadWriteTransaction {
            id,
            mode: ConcurrencyMode::new(id, database, locks, priority, optimistic_concurrency),
        }
    }

    pub(crate) fn session(&self) -> &Arc<LockSession> {
        match &self.mode {
            ConcurrencyMode::Pessimistic(state) => state.session(),
            ConcurrencyMode::Optimistic(state) => &state.session,
        }
    }

    /// Reads the document under the transaction's configured concurrency mode (pessimistic retained
    /// read lease or optimistic pinned MVCC snapshot).
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        tracing::debug!(txn_id = %self.id, %name, "reading document in read-write transaction");
        match &self.mode {
            ConcurrencyMode::Pessimistic(state) => state.read_doc(name).await,
            ConcurrencyMode::Optimistic(state) => state.read_doc(name).await,
        }
    }

    pub(crate) fn is_optimistic(&self) -> bool {
        matches!(self.mode, ConcurrencyMode::Optimistic(_))
    }

    pub(crate) async fn check_concurrency_violations(
        &self,
        held_write_leases: &BTreeMap<DocumentRef, WriteLease>,
    ) -> std::result::Result<(), CommitError> {
        match &self.mode {
            ConcurrencyMode::Pessimistic(_) => Ok(()),
            ConcurrencyMode::Optimistic(state) => {
                state.check_concurrency_violations(held_write_leases).await
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReadOnlyTransaction {
    pub(crate) id: TransactionId,
    pub(crate) database: Weak<FirestoreDatabase>,
    pub(crate) read_time: Timestamp,
}

impl ReadOnlyTransaction {
    pub(crate) fn new(
        id: TransactionId,
        database: Weak<FirestoreDatabase>,
        read_time: Timestamp,
    ) -> Self {
        Self {
            id,
            database,
            read_time,
        }
    }

    async fn read_doc(&self, name: &DocumentRef) -> Result<Option<Arc<StoredDocumentVersion>>> {
        let db = self
            .database
            .upgrade()
            .ok_or(AbortReason::DatabaseDropped)?;
        if db.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        tracing::debug!(
            txn_id = %self.id,
            %name,
            read_time = ?self.read_time,
            "reading document in read-only transaction"
        );
        Ok(db
            .peek_doc_meta(name)
            .await?
            .and_then(|meta| meta.read_mvcc_at(self.read_time)))
    }
}
