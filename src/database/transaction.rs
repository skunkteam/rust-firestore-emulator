use super::document::{DocumentGuard, DocumentMeta};
use prost_types::Timestamp;
use std::{
    collections::{hash_map::Entry, HashMap},
    mem,
    sync::Arc,
    time::SystemTime,
};
use tokio::sync::{Mutex, RwLock};
use tonic::{Result, Status};
use tracing::{info, instrument};

#[derive(Default)]
pub struct RunningTransactions {
    map: RwLock<HashMap<TransactionId, Arc<Transaction>>>,
}

impl RunningTransactions {
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
            let id = TransactionId(rand::random());
            if !lock.contains_key(&id) {
                break id;
            }
        };
        let txn = Arc::new(Transaction::new(id));
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
                let txn = Arc::new(Transaction::new(id));
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

enum TransactionStatus {
    Valid(HashMap<String, DocumentGuard>),
    Invalid,
}

pub struct Transaction {
    pub id: TransactionId,
    pub start_time: Timestamp,
    status: Mutex<TransactionStatus>,
}

impl Transaction {
    fn new(id: TransactionId) -> Self {
        Transaction {
            id,
            start_time: SystemTime::now().into(),
            status: TransactionStatus::Valid(Default::default()).into(),
        }
    }

    #[instrument(skip_all)]
    pub async fn register_doc(&self, doc: &Arc<DocumentMeta>) {
        let mut status = self.status.lock().await;
        let TransactionStatus::Valid(guards) = &mut *status else {
            info!(?self.id, doc.name, "txn invalid");
            return;
        };
        if guards.contains_key(&doc.name) {
            info!(?self.id, doc.name, "lock already owned");
        } else if let Ok(new_lock) = Arc::clone(doc).try_lock().await {
            info!(?self.id, doc.name, "lock acquired");
            guards.insert(doc.name.clone(), new_lock);
        } else {
            info!(?self.id, doc.name, "lock unavailable, txn becomes invalid");
            *status = TransactionStatus::Invalid;
        }
    }

    pub async fn take_guards(&self) -> Result<HashMap<String, DocumentGuard>> {
        let status = mem::replace(&mut *self.status.lock().await, TransactionStatus::Invalid);
        match status {
            TransactionStatus::Valid(guards) => Ok(guards),
            TransactionStatus::Invalid => Err(Status::aborted("contention")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(usize);

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
