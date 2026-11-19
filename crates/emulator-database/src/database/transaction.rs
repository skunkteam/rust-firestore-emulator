use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::Display,
    sync::{
        Arc, LazyLock, PoisonError, RwLock, Weak,
        atomic::{self, AtomicUsize},
    },
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::{Mutex, MutexGuard};

use super::{
    FirestoreDatabase,
    document::{OwnedDocumentContentsReadGuard, OwnedDocumentContentsWriteGuard},
    read_consistency::validate_read_time,
    reference::DocumentRef,
};
use crate::{
    GenericDatabaseError,
    database::query::Query,
    document::{DocumentMeta, StoredDocumentVersion},
    error::Result,
};

pub(super) const MAX_CLOSED_TXNS: usize = 4_096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClosedOutcome {
    Committed,
    AbortedOrRolledBack,
}

#[derive(Clone, Copy, Debug)]
struct ClosedTransaction {
    outcome:    ClosedOutcome,
    read_only:  bool,
    generation: u64,
}

#[derive(Debug)]
enum TransactionSlot {
    Active(Arc<Transaction>),
    Committing { read_only: bool },
    Closed(ClosedTransaction),
}

#[derive(Debug, Default)]
struct RunningTransactionsState {
    slots: HashMap<TransactionId, TransactionSlot>,
    closed_order: VecDeque<(TransactionId, u64)>,
    closed_count: usize,
    next_closed_gen: u64,
}

impl RunningTransactionsState {
    fn record_closed(&mut self, id: TransactionId, outcome: ClosedOutcome, read_only: bool) {
        self.next_closed_gen = self.next_closed_gen.wrapping_add(1);
        let generation = self.next_closed_gen;
        let prev = self.slots.insert(
            id,
            TransactionSlot::Closed(ClosedTransaction {
                outcome,
                read_only,
                generation,
            }),
        );
        if !matches!(prev, Some(TransactionSlot::Closed(_))) {
            self.closed_count += 1;
        }
        self.closed_order.push_back((id, generation));
        while self.closed_count > MAX_CLOSED_TXNS {
            let Some((old_id, old_gen)) = self.closed_order.pop_front() else {
                break;
            };
            if matches!(
                self.slots.get(&old_id),
                Some(TransactionSlot::Closed(closed)) if closed.generation == old_gen
            ) {
                self.slots.remove(&old_id);
                self.closed_count -= 1;
            }
        }
        if self.closed_order.len() > MAX_CLOSED_TXNS * 2 {
            self.closed_order.retain(|(entry_id, entry_gen)| {
                matches!(
                    self.slots.get(entry_id),
                    Some(TransactionSlot::Closed(closed)) if closed.generation == *entry_gen
                )
            });
        }
    }
}

#[derive(Debug)]
pub(crate) struct RunningTransactions {
    database: Weak<FirestoreDatabase>,
    state:    Arc<RwLock<RunningTransactionsState>>,
}

impl RunningTransactions {
    pub(crate) fn new(database: Weak<FirestoreDatabase>) -> Self {
        Self {
            database,
            state: Default::default(),
        }
    }

    pub(crate) async fn get(&self, id: TransactionId) -> Result<Arc<Transaction>> {
        match self
            .state
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .get(&id)
        {
            Some(TransactionSlot::Active(txn)) => Ok(Arc::clone(txn)),
            Some(TransactionSlot::Committing { .. } | TransactionSlot::Closed(_)) | None => Err(
                GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {}", id.0)),
            ),
        }
    }

    pub(crate) async fn start_read_write(&self, optimistic_concurrency: bool) -> TransactionId {
        let id = TransactionId::new();
        let txn = Arc::new(Transaction::ReadWrite(ReadWriteTransaction::new(
            id,
            Weak::clone(&self.database),
            optimistic_concurrency,
        )));
        self.state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .insert(id, TransactionSlot::Active(txn));
        id
    }

    pub(crate) async fn start_read_only(
        &self,
        read_time: Option<Timestamp>,
    ) -> Result<TransactionId> {
        let read_time = match read_time {
            Some(time) => validate_read_time(time)?,
            None => Timestamp::now(),
        };
        let id = TransactionId::new();
        let txn = Arc::new(Transaction::ReadOnly(ReadOnlyTransaction::new(
            id,
            Weak::clone(&self.database),
            read_time,
        )));
        self.state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .slots
            .insert(id, TransactionSlot::Active(txn));
        Ok(id)
    }

    pub(crate) async fn start_read_write_with_id(
        &self,
        id: TransactionId,
        optimistic_concurrency: bool,
    ) -> Result<()> {
        id.check()?;
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        match state.slots.get(&id) {
            Some(TransactionSlot::Active(txn)) if txn.as_read_write().is_none() => {
                return Err(GenericDatabaseError::invalid_argument(
                    "Cannot retry a read-only transaction",
                ));
            }
            Some(
                TransactionSlot::Committing { read_only: true }
                | TransactionSlot::Closed(ClosedTransaction {
                    read_only: true, ..
                }),
            ) => {
                return Err(GenericDatabaseError::invalid_argument(
                    "Cannot retry a read-only transaction",
                ));
            }
            Some(TransactionSlot::Active(_) | TransactionSlot::Committing { .. }) => {
                return Err(GenericDatabaseError::failed_precondition(
                    "transaction_id already/still in use",
                ));
            }
            Some(TransactionSlot::Closed(_)) => {
                state.closed_count = state.closed_count.saturating_sub(1);
            }
            None => {}
        }
        let txn = Arc::new(Transaction::ReadWrite(ReadWriteTransaction::new(
            id,
            Weak::clone(&self.database),
            optimistic_concurrency,
        )));
        state.slots.insert(id, TransactionSlot::Active(txn));
        Ok(())
    }

    pub(crate) fn begin_commit(&self, id: TransactionId, has_writes: bool) -> Result<CommitGuard> {
        let mut closed_ro_txn = None;
        let result = {
            let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
            match state.slots.get(&id) {
                Some(TransactionSlot::Active(txn)) => {
                    let read_only = txn.as_read_write().is_none();
                    if has_writes && read_only {
                        let Some(TransactionSlot::Active(txn)) = state.slots.remove(&id) else {
                            unreachable!();
                        };
                        state.record_closed(id, ClosedOutcome::AbortedOrRolledBack, true);
                        closed_ro_txn = Some(txn);
                        Err(GenericDatabaseError::invalid_argument(
                            "Cannot modify entities in a read-only transaction",
                        ))
                    } else {
                        let Some(TransactionSlot::Active(txn)) = state
                            .slots
                            .insert(id, TransactionSlot::Committing { read_only })
                        else {
                            unreachable!();
                        };
                        Ok(CommitGuard {
                            txn,
                            reservation: CommitReservation {
                                state: Arc::clone(&self.state),
                                id,
                                read_only,
                                committed: false,
                            },
                        })
                    }
                }
                Some(TransactionSlot::Committing { .. } | TransactionSlot::Closed(_)) | None => {
                    Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {}",
                        id.0
                    )))
                }
            }
        };
        drop(closed_ro_txn);
        result
    }

    pub(crate) fn rollback_sync(&self, id: TransactionId) -> Result<()> {
        let mut dropped_txn = None;
        let result = {
            let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
            match state.slots.get(&id) {
                Some(TransactionSlot::Active(_)) => {
                    let Some(TransactionSlot::Active(txn)) = state.slots.remove(&id) else {
                        unreachable!();
                    };
                    let read_only = txn.as_read_write().is_none();
                    state.record_closed(id, ClosedOutcome::AbortedOrRolledBack, read_only);
                    dropped_txn = Some(txn);
                    Ok(())
                }
                Some(TransactionSlot::Committing { .. }) => Ok(()),
                Some(TransactionSlot::Closed(closed)) => match closed.outcome {
                    ClosedOutcome::Committed => Err(GenericDatabaseError::aborted(
                        "The referenced transaction has expired or is no longer valid.",
                    )),
                    ClosedOutcome::AbortedOrRolledBack => Ok(()),
                },
                None => Err(GenericDatabaseError::invalid_argument(format!(
                    "invalid transaction ID: {}",
                    id.0
                ))),
            }
        };
        drop(dropped_txn);
        result
    }
}

#[derive(Debug)]
pub(crate) struct CommitGuard {
    // Field order is load-bearing: `txn` drops first (releasing any held read locks) before
    // `reservation` drops and transitions the `TransactionId` slot to `Closed`.
    txn: Arc<Transaction>,
    reservation: CommitReservation,
}

impl CommitGuard {
    pub(crate) fn as_read_write(&self) -> Option<&ReadWriteTransaction> {
        self.txn.as_read_write()
    }

    pub(crate) fn finish_committed(mut self) {
        self.reservation.committed = true;
    }
}

#[derive(Debug)]
struct CommitReservation {
    state: Arc<RwLock<RunningTransactionsState>>,
    id: TransactionId,
    read_only: bool,
    committed: bool,
}

impl Drop for CommitReservation {
    fn drop(&mut self) {
        let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if matches!(
            state.slots.get(&self.id),
            Some(TransactionSlot::Committing { .. })
        ) {
            let outcome = if self.committed {
                ClosedOutcome::Committed
            } else {
                ClosedOutcome::AbortedOrRolledBack
            };
            state.record_closed(self.id, outcome, self.read_only);
        }
    }
}

#[derive(Debug)]
pub(crate) enum Transaction {
    ReadWrite(ReadWriteTransaction),
    ReadOnly(ReadOnlyTransaction),
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
    guards:   Mutex<HashMap<DocumentRef, Arc<OwnedDocumentContentsReadGuard>>>,
}

impl PessimisticState {
    /// Acquires a lock on the document (if needed) and returns the latest version of that document.
    /// The guard is stored with the transaction and can only be released by dropping the
    /// transaction, using [`Self::take_write_guard`] or [`Self::drop_read_guards`].
    ///
    /// If the guard should not be kept inside the transaction (if the lock is preliminary), then
    /// use the combination of [`RWTransactionQuerySupport::get_read_guard`] and
    /// [`RWTransactionQuerySupport::manage_read_guard`] on the result of [`Self::query_support`].
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        let mut guards = self.guards.lock().await;
        if let Some(guard) = guards.get(name) {
            return Ok(guard.current_version().cloned());
        }
        let guard: Arc<OwnedDocumentContentsReadGuard> = self.new_read_guard(name).await?.into();
        let version = guard.current_version().cloned();
        guards.insert(name.clone(), guard);
        Ok(version)
    }

    /// Returns a handle that can be used to temporarily lock documents in order to evaluate whether
    /// they should be included in a query result.
    pub(crate) async fn query_support(&self) -> PessimisticTransactionQuerySupport<'_> {
        PessimisticTransactionQuerySupport {
            guards_guard: self.guards.lock().await,
        }
    }

    /// Drops all read guards that are kept by this transaction. This does not include guards that
    /// have been upgraded to write guards, using [`Self::take_write_guard`].
    pub(crate) async fn drop_read_guards(&self) {
        self.guards.lock().await.clear();
    }

    /// Upgrade the read guard for the document with the given `name` to a write guard. Write guards
    /// are not kept with the transaction, unlike read guards.
    pub(crate) async fn take_write_guard(
        &self,
        name: &DocumentRef,
    ) -> Result<OwnedDocumentContentsWriteGuard> {
        let read_guard = {
            let mut guards = self.guards.lock().await;
            guards.remove(name)
        };
        let read_guard = match read_guard {
            Some(guard) => Arc::into_inner(guard).ok_or_else(|| {
                GenericDatabaseError::aborted("concurrent reads during txn commit in same txn")
            })?,
            None => self.new_read_guard(name).await?,
        };
        read_guard.upgrade().await
    }

    async fn new_read_guard(&self, name: &DocumentRef) -> Result<OwnedDocumentContentsReadGuard> {
        self.database
            .upgrade()
            .ok_or_else(|| GenericDatabaseError::aborted("database was dropped"))?
            .get_doc_meta(name)
            .await?
            .read_owned()
            .await
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
    pub(crate) observed_docs: Mutex<HashSet<DocumentRef>>,
    pub(crate) executed_queries: Mutex<Vec<ExecutedQuery>>,
    pub(crate) read_time: LazyLock<Timestamp>,
}

impl OptimisticState {
    /// Acquires a lock on the document (if needed) and returns the latest version of that document.
    /// The guard is stored with the transaction and can only be released by dropping the
    /// transaction, using [`Self::take_write_guard`] or [`Self::drop_read_guards`].
    ///
    /// If the guard should not be kept inside the transaction (if the lock is preliminary), then
    /// use the combination of [`RWTransactionQuerySupport::get_read_guard`] and
    /// [`RWTransactionQuerySupport::manage_read_guard`] on the result of [`Self::query_support`].
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        let db = self
            .database
            .upgrade()
            .ok_or_else(|| GenericDatabaseError::aborted("database was dropped"))?;
        let meta = db.get_doc_meta(name).await?;
        let read_guard = meta.read().await?;
        let rt = *self.read_time;
        let version = read_guard.version_at_time(rt).cloned();
        self.observed_docs.lock().await.insert(name.clone());
        Ok(version)
    }

    pub(crate) async fn get_write_guard(
        &self,
        name: &DocumentRef,
    ) -> Result<OwnedDocumentContentsWriteGuard> {
        self.database
            .upgrade()
            .ok_or_else(|| GenericDatabaseError::aborted("database was dropped"))?
            .get_doc_meta(name)
            .await?
            .write_owned()
            .await
    }

    pub(crate) async fn check_concurrency_violations(
        &self,
        held_write_guards: &BTreeMap<DocumentRef, OwnedDocumentContentsWriteGuard>,
    ) -> Result<()> {
        let db = self
            .database
            .upgrade()
            .ok_or_else(|| GenericDatabaseError::aborted("database was dropped"))?;
        let rt = *self.read_time;

        // VERIFY OBSERVED DOCS
        {
            let observed = self.observed_docs.lock().await;
            for name in observed.iter() {
                let check_time = if let Some(wg) = held_write_guards.get(name) {
                    wg.last_updated()
                } else {
                    let meta = db.get_doc_meta(name).await?;
                    let rg = meta.read().await?;
                    rg.last_updated()
                };
                if check_time.is_some_and(|time| time > rt) {
                    return Err(GenericDatabaseError::aborted(
                        "contention: document modified",
                    ));
                }
            }
        }

        // VERIFY QUERIES
        {
            let mut executed_queries = self.executed_queries.lock().await;
            for ExecutedQuery { query, snapshot } in executed_queries.iter_mut() {
                let (_, result_docs) = query
                    .once_for_validation(&db, held_write_guards)
                    .await
                    .map_err(|_| {
                        GenericDatabaseError::aborted(
                            "contention: concurrent modification invalidated query",
                        )
                    })?;
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
                    return Err(GenericDatabaseError::aborted(
                        "contention: query result changed",
                    ));
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
    fn new(database: Weak<FirestoreDatabase>, optimistic_concurrency: bool) -> Self {
        if optimistic_concurrency {
            ConcurrencyMode::Optimistic(Box::new(OptimisticState {
                database,
                observed_docs: Default::default(),
                executed_queries: Default::default(),
                read_time: LazyLock::new(Timestamp::now),
            }))
        } else {
            ConcurrencyMode::Pessimistic(PessimisticState {
                database,
                guards: Default::default(),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReadWriteTransaction {
    #[allow(dead_code)] // For logging
    pub(crate) id: TransactionId,
    pub(crate) mode: ConcurrencyMode,
}

impl ReadWriteTransaction {
    fn new(
        id: TransactionId,
        database: Weak<FirestoreDatabase>,
        optimistic_concurrency: bool,
    ) -> Self {
        ReadWriteTransaction {
            id,
            mode: ConcurrencyMode::new(database, optimistic_concurrency),
        }
    }

    /// Acquires a lock on the document (if needed) and returns the latest version of that document.
    /// The guard is stored with the transaction and can only be released by dropping the
    /// transaction, using [`Self::take_write_guard`] or [`Self::drop_read_guards`].
    ///
    /// If the guard should not be kept inside the transaction (if the lock is preliminary), then
    /// use the combination of [`RWTransactionQuerySupport::get_read_guard`] and
    /// [`RWTransactionQuerySupport::manage_read_guard`] on the result of [`Self::query_support`].
    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
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
        held_write_guards: &BTreeMap<DocumentRef, OwnedDocumentContentsWriteGuard>,
    ) -> Result<()> {
        match &self.mode {
            ConcurrencyMode::Pessimistic(_) => Ok(()),
            ConcurrencyMode::Optimistic(state) => {
                state.check_concurrency_violations(held_write_guards).await
            }
        }
    }

    pub(crate) async fn get_write_guard(
        &self,
        name: &DocumentRef,
    ) -> Result<OwnedDocumentContentsWriteGuard> {
        match &self.mode {
            ConcurrencyMode::Pessimistic(state) => state.take_write_guard(name).await,
            ConcurrencyMode::Optimistic(state) => state.get_write_guard(name).await,
        }
    }

    pub(crate) async fn prepare_commit(&self) {
        match &self.mode {
            ConcurrencyMode::Pessimistic(state) => state.drop_read_guards().await,
            ConcurrencyMode::Optimistic(_) => (),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PessimisticTransactionQuerySupport<'a> {
    pub(crate) guards_guard:
        MutexGuard<'a, HashMap<DocumentRef, Arc<OwnedDocumentContentsReadGuard>>>,
}

impl PessimisticTransactionQuerySupport<'_> {
    /// Acquires a read lock on the document or returns the already present guard in this
    /// transaction. It does not, however, keep the guard inside the transaction when newly
    /// acquired. If the guard should be stored with this transaction use
    /// [`Self::manage_read_guard`] to add it.
    pub(crate) async fn get_read_guard(
        &self,
        meta: &Arc<DocumentMeta>,
    ) -> Result<Arc<OwnedDocumentContentsReadGuard>> {
        if let Some(guard) = self.guards_guard.get(&meta.name) {
            return Ok(Arc::clone(guard));
        }
        Ok(meta.read_owned().await?.into())
    }

    /// Add the given read guard to this transaction, must be a guard that was returned by
    /// [`Self::get_read_guard`].
    pub(crate) fn manage_read_guard(&mut self, guard: Arc<OwnedDocumentContentsReadGuard>) {
        self.guards_guard.insert(guard.name.clone(), guard);
    }
}

#[derive(Debug)]
pub(crate) struct ReadOnlyTransaction {
    #[allow(dead_code)] // For logging
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
        let doc = self
            .database
            .upgrade()
            .ok_or_else(|| GenericDatabaseError::aborted("database was dropped"))?
            .get_doc_meta(name)
            .await?;
        let lock = doc.read().await?;
        Ok(lock.version_at_time(self.read_time).cloned())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(usize);

static NEXT_TXN_ID: AtomicUsize = AtomicUsize::new(1);

impl TransactionId {
    fn new() -> Self {
        Self(NEXT_TXN_ID.fetch_add(1, atomic::Ordering::Release))
    }

    /// Check if the given [`TransactionId`] could have been issued by the currently running
    /// instance. This doesn't guarantee that the given id is valid, but it prevents collisions with
    /// future IDs.
    fn check(self) -> Result<()> {
        if (1..NEXT_TXN_ID.load(atomic::Ordering::Acquire)).contains(&self.0) {
            Ok(())
        } else {
            Err(GenericDatabaseError::InvalidArgument(format!(
                "Invalid transaction ID, {self} has not been issued by this instance"
            )))
        }
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<Vec<u8>> for TransactionId {
    type Error = GenericDatabaseError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let arr = value.try_into().map_err(|value| {
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {value:?}"))
        })?;
        Ok(TransactionId(usize::from_ne_bytes(arr)))
    }
}

impl From<TransactionId> for Vec<u8> {
    fn from(val: TransactionId) -> Self {
        val.0.to_ne_bytes().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_id_check_rejects_zero_and_future_ids() {
        assert!(matches!(
            TransactionId(0).check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        let issued = TransactionId::new();
        assert!(issued.check().is_ok());

        let unissued = TransactionId(NEXT_TXN_ID.load(atomic::Ordering::Acquire) + 100);
        assert!(matches!(
            unissued.check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn start_read_only_validates_and_pins_read_time() {
        let txns = RunningTransactions::new(Weak::new());

        // Missing read_time is pinned at creation.
        let id = txns.start_read_only(None).await.unwrap();
        let pinned = txns.get(id).await.unwrap().read_time().unwrap();
        let later = Timestamp::now();
        assert!(later > pinned);
        assert_eq!(txns.get(id).await.unwrap().read_time(), Some(pinned));

        // Unix epoch is accepted.
        let epoch = Timestamp {
            seconds: 0,
            nanos:   0,
        };
        let epoch_id = txns.start_read_only(Some(epoch)).await.unwrap();
        assert_eq!(txns.get(epoch_id).await.unwrap().read_time(), Some(epoch));

        // Negative seconds and future timestamps are rejected.
        assert!(matches!(
            txns.start_read_only(Some(Timestamp {
                seconds: -1,
                nanos:   0,
            }))
            .await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            txns.start_read_only(Some(Timestamp {
                seconds: later.seconds + 60,
                nanos:   0,
            }))
            .await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn rollback_is_idempotent_for_closed_or_committing_and_aborts_after_commit() {
        let txns = RunningTransactions::new(Weak::new());
        let other_db_txns = RunningTransactions::new(Weak::new());

        // Unissued IDs and IDs issued by another database fail with InvalidArgument.
        assert!(matches!(
            txns.rollback_sync(TransactionId(0)),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        let other_id = other_db_txns.start_read_write(false).await;
        assert!(matches!(
            txns.rollback_sync(other_id),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Double rollback on an active transaction is idempotent Ok(()).
        let id_rb = txns.start_read_write(false).await;
        assert!(txns.rollback_sync(id_rb).is_ok());
        assert!(txns.rollback_sync(id_rb).is_ok());
        assert!(matches!(
            txns.get(id_rb).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Dropped (failed/cancelled) CommitGuard records AbortedOrRolledBack -> Rollback is Ok(()).
        let id_failed = txns.start_read_write(false).await;
        let guard = txns.begin_commit(id_failed, true).unwrap();
        // Rollback while Committing is Ok(()) and leaves Committing intact.
        assert!(txns.rollback_sync(id_failed).is_ok());
        drop(guard);
        assert!(txns.rollback_sync(id_failed).is_ok());
        assert!(txns.rollback_sync(id_failed).is_ok());

        // Succeeded CommitGuard records Committed -> Rollback returns Aborted (even on repeat).
        let id_committed = txns.start_read_write(false).await;
        let guard = txns.begin_commit(id_committed, true).unwrap();
        assert!(txns.rollback_sync(id_committed).is_ok());
        guard.finish_committed();
        assert!(matches!(
            txns.rollback_sync(id_committed),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            txns.rollback_sync(id_committed),
            Err(GenericDatabaseError::Aborted(_))
        ));
    }

    #[tokio::test]
    async fn retry_rejects_active_committing_and_closed_read_only_transactions() {
        let txns = RunningTransactions::new(Weak::new());

        // Active read-only transaction cannot be retried.
        let ro_active = txns.start_read_only(None).await.unwrap();
        assert!(matches!(
            txns.start_read_write_with_id(ro_active, false).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(txns.rollback_sync(ro_active).is_ok());
        // Rolled-back read-only transaction still cannot be retried.
        assert!(matches!(
            txns.start_read_write_with_id(ro_active, false).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Read-only transaction closed by write rejection cannot be read or retried,
        // while Rollback succeeds idempotently.
        let ro_write = txns.start_read_only(None).await.unwrap();
        assert!(matches!(
            txns.begin_commit(ro_write, true),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            txns.get(ro_write).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(txns.rollback_sync(ro_write).is_ok());
        assert!(matches!(
            txns.start_read_write_with_id(ro_write, false).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Read-only empty commit: committing rejects retry, finished commit aborts rollback
        // and still rejects retry.
        let ro_empty = txns.start_read_only(None).await.unwrap();
        let guard = txns.begin_commit(ro_empty, false).unwrap();
        assert!(matches!(
            txns.start_read_write_with_id(ro_empty, false).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        guard.finish_committed();
        assert!(matches!(
            txns.rollback_sync(ro_empty),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            txns.start_read_write_with_id(ro_empty, false).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn closed_transactions_evict_bounded_fifo_and_compact_retried_entries() {
        let txns = RunningTransactions::new(Weak::new());

        // Repeatedly retrying and closing one ID must keep `closed_order` bounded via compaction
        // and keep `closed_count == 1`.
        let reused = txns.start_read_write(false).await;
        txns.rollback_sync(reused).unwrap();
        for _ in 0..(MAX_CLOSED_TXNS * 2 + 16) {
            txns.start_read_write_with_id(reused, false).await.unwrap();
            txns.rollback_sync(reused).unwrap();
        }
        {
            let state = txns.state.read().unwrap();
            assert_eq!(state.closed_count, 1);
            assert!(state.closed_order.len() <= MAX_CLOSED_TXNS * 2);
        }

        // Closing MAX_CLOSED_TXNS + 10 distinct transactions evicts the oldest entries in FIFO
        // order while retaining the newest MAX_CLOSED_TXNS entries.
        let mut ids = Vec::with_capacity(MAX_CLOSED_TXNS + 10);
        for _ in 0..(MAX_CLOSED_TXNS + 10) {
            let id = txns.start_read_write(false).await;
            txns.rollback_sync(id).unwrap();
            ids.push(id);
        }
        {
            let state = txns.state.read().unwrap();
            assert_eq!(state.closed_count, MAX_CLOSED_TXNS);
            assert_eq!(state.slots.len(), MAX_CLOSED_TXNS);
        }

        // Oldest entries (including `reused` and the first 10 distinct IDs) were evicted.
        assert!(matches!(
            txns.rollback_sync(reused),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        for &evicted in &ids[..10] {
            assert!(matches!(
                txns.rollback_sync(evicted),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        // Newest MAX_CLOSED_TXNS entries remain idempotently rollback-able.
        assert!(txns.rollback_sync(ids[10]).is_ok());
        assert!(txns.rollback_sync(*ids.last().unwrap()).is_ok());
    }
}
