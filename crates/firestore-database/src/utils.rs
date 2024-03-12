use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash},
};

use async_trait::async_trait;
use tokio::sync::{RwLock, RwLockReadGuard};

#[macro_export]
macro_rules! unimplemented {
    ($name:expr) => {{
        use $crate::GenericDatabaseError;
        let msg = format!("{} is not supported yet ({}:{})", $name, file!(), line!());
        eprintln!("{msg}");
        return Err(GenericDatabaseError::not_implemented(msg));
    }};
}

#[macro_export]
macro_rules! required_option {
    ($var:ident) => {
        let Some($var) = $var else {
            return Err($crate::GenericDatabaseError::invalid_argument(concat!(
                "missing ",
                stringify!($var)
            )));
        };
    };
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

#[async_trait]
pub trait RwLockHashMapExt<Q: ?Sized, V> {
    async fn get_or_insert(
        &self,
        key: &Q,
        default: impl FnOnce() -> V + Send,
    ) -> RwLockReadGuard<V>;
}

#[async_trait]
impl<K, V, S> RwLockHashMapExt<K, V> for RwLock<HashMap<K, V, S>>
where
    K: Clone + Eq + Hash + Sync + Send,
    V: Clone + Sync + Send,
    S: BuildHasher + Sync + Send,
{
    async fn get_or_insert(
        &self,
        key: &K,
        default: impl FnOnce() -> V + Send,
    ) -> RwLockReadGuard<V> {
        let lock = self.read().await;
        if let Ok(guard) = RwLockReadGuard::try_map(lock, |lock| lock.get(key)) {
            return guard;
        };
        let mut lock = self.write().await;
        lock.entry(key.clone()).or_insert_with(default);
        RwLockReadGuard::map(lock.downgrade(), |lock| &lock[key])
    }
}
