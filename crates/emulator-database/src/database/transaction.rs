use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    fmt::Display,
    sync::{
        Arc, LazyLock, Weak,
        atomic::{self, AtomicUsize},
    },
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::{Mutex, MutexGuard, RwLock};

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

#[derive(Debug)]
pub(crate) struct RunningTransactions {
    database: Weak<FirestoreDatabase>,
    txns:     RwLock<HashMap<TransactionId, Arc<Transaction>>>,
}

impl RunningTransactions {
    pub(crate) fn new(database: Weak<FirestoreDatabase>) -> Self {
        Self {
            database,
            txns: Default::default(),
        }
    }

    pub(crate) async fn get(&self, id: TransactionId) -> Result<Arc<Transaction>> {
        self.txns.read().await.get(&id).cloned().ok_or_else(|| {
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {}", id.0))
        })
    }

    pub(crate) async fn start_read_write(&self, optimistic_concurrency: bool) -> TransactionId {
        let id = TransactionId::new();
        let txn = Arc::new(Transaction::ReadWrite(ReadWriteTransaction::new(
            id,
            Weak::clone(&self.database),
            optimistic_concurrency,
        )));
        self.txns.write().await.insert(id, txn);
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
        self.txns.write().await.insert(id, txn);
        Ok(id)
    }

    pub(crate) async fn start_read_write_with_id(
        &self,
        id: TransactionId,
        optimistic_concurrency: bool,
    ) -> Result<()> {
        id.check()?;
        match self.txns.write().await.entry(id) {
            Entry::Occupied(_) => Err(GenericDatabaseError::failed_precondition(
                "transaction_id already/still in use",
            )),
            Entry::Vacant(e) => {
                let txn = Arc::new(Transaction::ReadWrite(ReadWriteTransaction::new(
                    id,
                    Weak::clone(&self.database),
                    optimistic_concurrency,
                )));
                e.insert(txn);
                Ok(())
            }
        }
    }

    pub(crate) async fn remove(&self, id: TransactionId) -> Result<Arc<Transaction>> {
        self.txns.write().await.remove(&id).ok_or_else(|| {
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {}", id.0))
        })
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
}
