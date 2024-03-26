use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    sync::{
        atomic::{self, AtomicUsize},
        Arc, Weak,
    },
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::{Mutex, RwLock};
use tracing::{instrument, Level};

use super::{
    document::{OwnedDocumentContentsReadGuard, OwnedDocumentContentsWriteGuard},
    reference::DocumentRef,
    FirestoreDatabase,
};
use crate::{document::StoredDocumentVersion, error::Result, GenericDatabaseError};

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

    pub(crate) async fn read_doc(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        Ok(self.read_guard(name).await?.current_version().cloned())
    }

    pub(crate) async fn drop_remaining_guards(&self) {
        self.guards.lock().await.clear();
    }

    pub(crate) async fn take_write_guard(
        &self,
        name: &DocumentRef,
    ) -> Result<OwnedDocumentContentsWriteGuard> {
        let mut guards = self.guards.lock().await;
        let read_guard = match guards.remove(name) {
            Some(guard) => Arc::into_inner(guard).ok_or_else(|| {
                GenericDatabaseError::aborted("concurrent reads during txn commit in same txn")
            })?,
            None => self.new_read_guard(name).await?,
        };
        read_guard.upgrade().await
    }

    #[instrument(level = Level::DEBUG, skip_all)]
    async fn read_guard(&self, name: &DocumentRef) -> Result<Arc<OwnedDocumentContentsReadGuard>> {
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
