use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Display,
    sync::{
        Arc, Weak,
        atomic::{self, AtomicUsize},
    },
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tracing::{Level, instrument};

use super::{
    FirestoreDatabase,
    document::{OwnedDocumentContentsReadGuard, OwnedDocumentContentsWriteGuard},
    reference::DocumentRef,
};
use crate::{
    GenericDatabaseError,
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

    pub(crate) async fn start_read_write(&self) -> TransactionId {
        let id = TransactionId::new();
        let txn = Arc::new(Transaction::ReadWrite(ReadWriteTransaction::new(
            id,
            Weak::clone(&self.database),
        )));
        self.txns.write().await.insert(id, txn);
        id
    }

    pub(crate) async fn start_read_only(&self, read_time: Option<Timestamp>) -> TransactionId {
        let id = TransactionId::new();
        let txn = Arc::new(Transaction::ReadOnly(ReadOnlyTransaction::new(
            id,
            Weak::clone(&self.database),
            read_time,
        )));
        self.txns.write().await.insert(id, txn);
        id
    }

    pub(crate) async fn start_read_write_with_id(&self, id: TransactionId) -> Result<()> {
        id.check()?;
        match self.txns.write().await.entry(id) {
            Entry::Occupied(_) => Err(GenericDatabaseError::failed_precondition(
                "transaction_id already/still in use",
            )),
            Entry::Vacant(e) => {
                let txn = Arc::new(Transaction::ReadWrite(ReadWriteTransaction::new(
                    id,
                    Weak::clone(&self.database),
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
            Transaction::ReadWrite(_) => None,
            Transaction::ReadOnly(txn) => txn.read_time,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReadWriteTransaction {
    #[allow(dead_code)] // For logging
    pub(crate) id: TransactionId,
    database: Weak<FirestoreDatabase>,
    guards: Mutex<HashMap<DocumentRef, Arc<OwnedDocumentContentsReadGuard>>>,
}

impl ReadWriteTransaction {
    fn new(id: TransactionId, database: Weak<FirestoreDatabase>) -> Self {
        ReadWriteTransaction {
            id,
            database,
            guards: Default::default(),
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
        Ok(self
            .managed_read_guard(name)
            .await?
            .current_version()
            .cloned())
    }

    /// Returns a handle that can be used to temporarily lock documents in order to evaluate whether
    /// they should be included in a query result.
    pub(crate) async fn query_support(&self) -> RWTransactionQuerySupport<'_> {
        RWTransactionQuerySupport {
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

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn managed_read_guard(
        &self,
        name: &DocumentRef,
    ) -> Result<Arc<OwnedDocumentContentsReadGuard>> {
        let mut guards = self.guards.lock().await;
        if let Some(guard) = guards.get(name) {
            return Ok(Arc::clone(guard));
        }
        let guard = self.new_read_guard(name).await?.into();
        guards.insert(name.clone(), Arc::clone(&guard));
        Ok(guard)
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
pub(crate) struct RWTransactionQuerySupport<'a> {
    guards_guard: MutexGuard<'a, HashMap<DocumentRef, Arc<OwnedDocumentContentsReadGuard>>>,
}

impl RWTransactionQuerySupport<'_> {
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
    /// [`Self::manage_read_guard`].
    pub(crate) fn manage_read_guard(&mut self, guard: Arc<OwnedDocumentContentsReadGuard>) {
        self.guards_guard.insert(guard.name.clone(), guard);
    }
}

#[derive(Debug)]
pub(crate) struct ReadOnlyTransaction {
    #[allow(dead_code)] // For logging
    pub(crate) id: TransactionId,
    pub(crate) database: Weak<FirestoreDatabase>,
    pub(crate) read_time: Option<Timestamp>,
}

impl ReadOnlyTransaction {
    pub(crate) fn new(
        id: TransactionId,
        database: Weak<FirestoreDatabase>,
        read_time: Option<Timestamp>,
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
        let version = match self.read_time {
            Some(read_time) => lock.version_at_time(read_time),
            None => lock.current_version(),
        };
        Ok(version.cloned())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(usize);

static NEXT_TXN_ID: AtomicUsize = AtomicUsize::new(1);

impl TransactionId {
    fn new() -> Self {
        Self(NEXT_TXN_ID.fetch_add(1, atomic::Ordering::Relaxed))
    }

    /// Check if the given [`TransactionId`] could have been issued by the currently running
    /// instance. This doesn't guarantee that the given id is valid, but it prevents collisions with
    /// future IDs.
    fn check(self) -> Result<()> {
        if self.0 < NEXT_TXN_ID.load(atomic::Ordering::Relaxed) {
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
