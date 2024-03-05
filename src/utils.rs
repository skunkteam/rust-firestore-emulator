use prost_types::Timestamp;
use std::{borrow::Borrow, collections::HashMap, hash::Hash, sync::Mutex, time::SystemTime};
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
macro_rules! required_option {
    ($var:ident) => {
        let Some($var) = $var else {
            return Err(Status::invalid_argument(concat!(
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

const NANOS_PER_SECOND: i128 = 1_000_000_000;

pub fn timestamp_nanos(ts: &Timestamp) -> i128 {
    ts.seconds as i128 * NANOS_PER_SECOND + ts.nanos as i128
}

pub fn timestamp_from_nanos(nanos: i128) -> Timestamp {
    Timestamp {
        seconds: (nanos / NANOS_PER_SECOND) as _,
        nanos:   (nanos % NANOS_PER_SECOND) as _,
    }
}

/// Returns the current time as a `Timestamp` with the added guarantee that all calls will return a
/// strictly higher timestamp than all calls before that (for the execution of the program).
pub fn timestamp() -> Timestamp {
    static LAST: Mutex<Timestamp> = Mutex::new(Timestamp {
        seconds: 0,
        nanos:   0,
    });
    let mut last = LAST.lock().unwrap();
    let mut timestamp = SystemTime::now().into();
    if timestamp_nanos(&timestamp) <= timestamp_nanos(&last) {
        timestamp = Timestamp {
            seconds: last.seconds,
            nanos:   last.nanos + 1,
        }
    }
    last.clone_from(&timestamp);
    timestamp
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
