use super::document::{DocumentGuard, DocumentMeta};
use dashmap::{
    mapref::{
        entry::Entry,
        one::{Ref, RefMut},
    },
    DashMap,
};
use prost_types::Timestamp;
use std::sync::Arc;
use tonic::{Result, Status};

#[derive(Default)]
pub struct RunningTransactions {
    map: DashMap<TransactionId, Transaction>,
}

impl RunningTransactions {
    pub fn get(&self, id: &TransactionId) -> Result<Ref<TransactionId, Transaction>> {
        self.map
            .get(id)
            .ok_or_else(|| Status::invalid_argument(format!("invalid transaction ID: {}", id.0)))
    }

    pub fn start(&self) -> Ref<TransactionId, Transaction> {
        loop {
            let id = TransactionId(rand::random());
            match self.map.entry(id) {
                Entry::Occupied(_) => (),
                Entry::Vacant(entry) => return entry.insert(Default::default()).downgrade(),
            }
        }
    }

    pub fn stop(&self, id: &TransactionId) -> Result<()> {
        self.map
            .remove(id)
            .ok_or_else(|| Status::invalid_argument(format!("invalid transaction ID: {}", id.0)))?;
        Ok(())
    }
}

#[derive(Default)]
pub struct Transaction {
    start_time: Timestamp,
    locks: DashMap<String, DocumentGuard>,
}

impl Transaction {
    pub fn ensure_lock(&self, meta: &Arc<DocumentMeta>) -> Result<RefMut<String, DocumentGuard>> {
        let ref_mut = match self.locks.entry(meta.name.clone()) {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => entry.insert(meta.clone().try_lock()?),
        };
        Ok(ref_mut)
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
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
