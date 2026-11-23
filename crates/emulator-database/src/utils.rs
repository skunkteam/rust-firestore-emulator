use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash},
};

use tokio::sync::{RwLock, RwLockReadGuard};

#[macro_export]
macro_rules! unimplemented {
    ($name:expr) => {{
        use $crate::GenericDatabaseError;
        let msg = format!("{} is not supported yet ({}:{})", $name, file!(), line!());
        $crate::__tracing::warn!("{msg}");
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

pub(crate) trait RwLockHashMapExt<Q: ?Sized, V> {
    async fn get_or_insert<'a>(
        &'a self,
        key: &Q,
        default: impl FnOnce() -> V + Send,
    ) -> RwLockReadGuard<'a, V>
    where
        V: 'a;
}

impl<K, V, S> RwLockHashMapExt<K, V> for RwLock<HashMap<K, V, S>>
where
    K: Clone + Eq + Hash + Sync + Send,
    V: Clone + Sync + Send,
    S: BuildHasher + Sync + Send,
{
    async fn get_or_insert<'a>(
        &'a self,
        key: &K,
        default: impl FnOnce() -> V + Send,
    ) -> RwLockReadGuard<'a, V>
    where
        V: 'a,
    {
        let lock = self.read().await;
        if let Ok(guard) = RwLockReadGuard::try_map(lock, |lock| lock.get(key)) {
            return guard;
        };
        let mut lock = self.write().await;
        lock.entry(key.clone()).or_insert_with(default);
        RwLockReadGuard::map(lock.downgrade(), |lock| &lock[key])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing::{
        Event, Level, Metadata,
        field::{Field, Visit},
        span,
    };

    use crate::GenericDatabaseError;

    struct MessageVisitor(Option<String>);

    impl Visit for MessageVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = Some(format!("{value:?}"));
            }
        }
    }

    struct WarnCollector {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl tracing::Subscriber for WarnCollector {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() <= Level::WARN
        }

        fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

        fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

        fn event(&self, event: &Event<'_>) {
            if *event.metadata().level() == Level::WARN {
                let mut visitor = MessageVisitor(None);
                event.record(&mut visitor);
                if let Some(msg) = visitor.0 {
                    self.messages.lock().unwrap().push(msg);
                }
            }
        }

        fn enter(&self, _span: &span::Id) {}

        fn exit(&self, _span: &span::Id) {}
    }

    fn trigger_unimplemented() -> Result<(), GenericDatabaseError> {
        crate::unimplemented!("demo_feature");
    }

    #[test]
    fn unimplemented_macro_returns_not_implemented_error() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let _tracing_guard = tracing::subscriber::set_default(WarnCollector {
            messages: Arc::clone(&messages),
        });

        let err = trigger_unimplemented().unwrap_err();
        assert!(
            matches!(
                err,
                GenericDatabaseError::NotImplemented(ref msg)
                    if msg.contains("demo_feature is not supported yet")
            ),
            "unexpected error: {err:?}"
        );

        let recorded = messages.lock().unwrap();
        assert!(
            recorded
                .iter()
                .any(|msg| msg.contains("demo_feature is not supported yet")),
            "expected a WARN tracing event containing 'demo_feature is not supported yet', got: \
             {recorded:?}"
        );
    }
}
