use super::{
    document::{OwnedDocumentContentsReadGuard, OwnedDocumentContentsWriteGuard},
    Database,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{
        atomic::{self, AtomicUsize},
        Arc, Weak,
    },
};
use tokio::sync::{Mutex, RwLock};
use tonic::{Result, Status};
use tracing::instrument;

pub struct RunningTransactions {
    pub(super) database: Weak<Database>,
    pub(super) map: RwLock<HashMap<TransactionId, Arc<Transaction>>>,
}

impl RunningTransactions {
    pub fn new(database: Weak<Database>) -> Self {
        Self {
            database,
            map: Default::default(),
        }
    }

    pub async fn get(&self, id: &TransactionId) -> Result<Arc<Transaction>> {
        self.map
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| Status::invalid_argument(format!("invalid transaction ID: {}", id.0)))
    }

    pub async fn start(&self) -> Arc<Transaction> {
        let mut lock = self.map.write().await;
        let id = loop {
            let id = TransactionId::new();
            if !lock.contains_key(&id) {
                break id;
            }
        };
        let txn = Arc::new(Transaction::new(id, Weak::clone(&self.database)));
        lock.insert(id, Arc::clone(&txn));
        txn
    }

    pub async fn start_with_id(&self, id: TransactionId) -> Result<Arc<Transaction>> {
        let mut lock = self.map.write().await;
        match lock.entry(id) {
            Entry::Occupied(_) => Err(Status::failed_precondition(
                "transaction_id already/still in use",
            )),
            Entry::Vacant(e) => {
                let txn = Arc::new(Transaction::new(id, Weak::clone(&self.database)));
                e.insert(Arc::clone(&txn));
                Ok(txn)
            }
        }
    }

    pub async fn stop(&self, id: &TransactionId) -> Result<()> {
        self.map
            .write()
            .await
            .remove(id)
            .ok_or_else(|| Status::invalid_argument(format!("invalid transaction ID: {}", id.0)))?;
        Ok(())
    }

    pub async fn clear(&self) {
        self.map.write().await.clear()
    }
}

pub struct Transaction {
    pub id: TransactionId,
    database: Weak<Database>,
    guards: Mutex<HashMap<String, Arc<OwnedDocumentContentsReadGuard>>>,
}

impl Transaction {
    fn new(id: TransactionId, database: Weak<Database>) -> Self {
        Transaction {
            id,
            database,
            guards: Default::default(),
        }
    }

    #[instrument(skip_all)]
    pub async fn read_doc(&self, name: &str) -> Result<Arc<OwnedDocumentContentsReadGuard>> {
        let mut guards = self.guards.lock().await;
        if let Some(guard) = guards.get(name) {
            return Ok(Arc::clone(guard));
        }
        let guard = self.new_read_guard(name).await?.into();
        guards.insert(name.to_string(), Arc::clone(&guard));
        Ok(guard)
    }

    pub async fn take_write_guard(&self, name: &str) -> Result<OwnedDocumentContentsWriteGuard> {
        let mut guards = self.guards.lock().await;
        let read_guard = match guards.remove(name) {
            Some(guard) => Arc::into_inner(guard)
                .ok_or_else(|| Status::aborted("concurrent reads during txn commit in same txn"))?,
            None => self.new_read_guard(name).await?,
        };
        read_guard.upgrade().await
    }

    async fn new_read_guard(&self, name: &str) -> Result<OwnedDocumentContentsReadGuard> {
        self.database
            .upgrade()
            .ok_or_else(|| Status::aborted("database was dropped"))?
            .get_doc_meta(name)
            .await?
            .read_owned()
            .await
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(pub(self) usize);

impl TransactionId {
    fn new() -> Self {
        static NEXT_TXN_ID: AtomicUsize = AtomicUsize::new(1);
        Self(NEXT_TXN_ID.fetch_add(1, atomic::Ordering::Relaxed))
    }
}

impl TryFrom<Vec<u8>> for TransactionId {
    type Error = Status;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let arr = value.try_into().map_err(|value| {
            Status::invalid_argument(format!("invalid transaction ID: {value:?}"))
        })?;
        Ok(TransactionId(usize::from_ne_bytes(arr)))
    }
}

impl From<TransactionId> for Vec<u8> {
    fn from(val: TransactionId) -> Self {
        val.0.to_ne_bytes().into()
    }
}
