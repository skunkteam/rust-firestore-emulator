use prost_types::Timestamp;
use std::{borrow::Borrow, cmp, collections::HashMap, hash::Hash};
use tokio::sync::{RwLock, RwLockReadGuard};
use tonic::async_trait;

#[macro_export]
macro_rules! unimplemented {
    ($name:expr) => {{
        use tonic::Status;
        let msg = format!("{} is not supported yet ({}:{})", $name, file!(), line!());
        eprintln!("{msg}");
        return Err(Status::unimplemented(msg));
    }};
}

#[macro_export]
macro_rules! unimplemented_option {
    ($val:expr) => {
        if $val.is_some() {
            $crate::unimplemented!(stringify!($val))
        }
    };
}

#[macro_export]
macro_rules! unimplemented_collection {
    ($val:expr) => {
        if !$val.is_empty() {
            $crate::unimplemented!(stringify!($val))
        }
    };
}

#[macro_export]
macro_rules! unimplemented_bool {
    ($val:expr) => {
        if $val {
            $crate::unimplemented!(stringify!($val))
        }
    };
}

#[derive(Eq, PartialEq)]
pub struct CmpTimestamp<'a>(pub &'a Timestamp);

impl<'a> PartialOrd for CmpTimestamp<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for CmpTimestamp<'a> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.0.seconds.cmp(&other.0.seconds) {
            result @ (cmp::Ordering::Less | cmp::Ordering::Greater) => result,
            cmp::Ordering::Equal => self.0.nanos.cmp(&other.0.nanos),
        }
    }
}

#[async_trait]
pub trait RwLockHashMapExt<Q: ?Sized, V> {
    async fn get_or_insert(
        &self,
        key: &Q,
        default: impl FnOnce() -> V + Send,
    ) -> RwLockReadGuard<V>;
}

#[async_trait]
impl<K, V, Q> RwLockHashMapExt<Q, V> for RwLock<HashMap<K, V>>
where
    K: Borrow<Q> + Eq + Hash + Sync + Send,
    Q: Eq + Hash + Sync + ?Sized,
    for<'a> &'a Q: Into<K>,
    V: Clone + Sync + Send,
{
    async fn get_or_insert(
        &self,
        key: &Q,
        default: impl FnOnce() -> V + Send,
    ) -> RwLockReadGuard<V> {
        let lock = self.read().await;
        if let Ok(guard) = RwLockReadGuard::try_map(lock, |lock| lock.get(key)) {
            return guard;
        };
        let mut lock = self.write().await;
        lock.entry(key.into()).or_insert_with(default);
        RwLockReadGuard::map(lock.downgrade(), |lock| &lock[key])
    }
}
