use std::{collections::HashMap, ops::Deref, sync::Arc};

use googleapis::google::firestore::v1::{ListenRequest, ListenResponse};
use tokio::sync::{RwLock, broadcast};
use tokio_stream::Stream;
use tracing::{Level, debug, instrument};

use crate::{
    ContentionSnapshot, ContentionSseEvent, ContentionSubscription, DatabaseContentionSnapshot,
    FirestoreDatabase, config::FirestoreConfig, database::lock::ContentionBroadcaster,
    error::Result, event::ProjectEvent, listener::Listener, reference::RootRef, timeouts::Timeouts,
    utils::RwLockHashMapExt,
};

const MAX_PROJECT_EVENT_BACKLOG: usize = 1024;

pub struct FirestoreProject {
    pub(crate) timeouts: Timeouts,
    pub(crate) default_enterprise: bool,
    databases: RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>,
    events: broadcast::Sender<ProjectEvent>,
    contention: ContentionBroadcaster,
}

// A lot of structs refer back to FirestoreProject, so make sure not to print them out in Debug to
// prevent stack overflows.
impl std::fmt::Debug for FirestoreProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreProject")
            .field("timeouts", &self.timeouts)
            .finish_non_exhaustive()
    }
}

impl FirestoreProject {
    pub fn new(config: FirestoreConfig) -> Self {
        let base = if config.long_contention_timeout {
            Timeouts::CLOUD
        } else {
            Timeouts::FAST
        };
        let txn_max = config.transaction_max_lifetime.unwrap_or(base.txn_max);
        let txn_idle = config
            .transaction_idle_timeout
            .unwrap_or(base.txn_idle)
            .min(txn_max);
        let timeouts = Timeouts {
            write: base.write,
            read: base.read,
            txn_idle,
            txn_max,
        };
        Self {
            timeouts,
            default_enterprise: config.default_enterprise,
            databases: Default::default(),
            events: broadcast::channel(MAX_PROJECT_EVENT_BACKLOG).0,
            contention: ContentionBroadcaster::new(),
        }
    }

    pub fn send_project_event(&self, event: ProjectEvent) {
        let _unused = self.events.send(event);
    }

    pub fn subscribe_project_events(&self) -> broadcast::Receiver<ProjectEvent> {
        self.events.subscribe()
    }

    pub fn subscribe_contention_events(&self) -> ContentionSubscription {
        self.contention.subscribe()
    }

    pub fn contention_subscriber_count(&self) -> usize {
        self.contention.subscriber_count()
    }

    pub(crate) fn contention_broadcaster(&self) -> ContentionBroadcaster {
        self.contention.clone()
    }

    pub async fn contention_snapshot(&self, database: Option<&RootRef>) -> ContentionSnapshot {
        match database {
            Some(root) => {
                let db = {
                    let databases = self.databases.read().await;
                    databases.get(root).cloned()
                };
                let databases = db
                    .map(|db| vec![db.locks.contention_snapshot()])
                    .unwrap_or_default();
                ContentionSnapshot { databases }
            }
            None => {
                let dbs: Vec<Arc<FirestoreDatabase>> = {
                    let databases = self.databases.read().await;
                    databases.values().cloned().collect()
                };
                let mut snapshots: Vec<DatabaseContentionSnapshot> = dbs
                    .into_iter()
                    .map(|db| db.locks.contention_snapshot())
                    .collect();
                snapshots.sort_by(|a, b| a.database.cmp(&b.database));
                ContentionSnapshot {
                    databases: snapshots,
                }
            }
        }
    }

    pub async fn clear_contention_history(&self, database: Option<&RootRef>) {
        match database {
            Some(root) => {
                let db = {
                    let databases = self.databases.read().await;
                    databases.get(root).cloned()
                };
                if let Some(db) = db {
                    db.locks.clear_contention_history();
                }
            }
            None => {
                let dbs: Vec<Arc<FirestoreDatabase>> = {
                    let databases = self.databases.read().await;
                    databases.values().cloned().collect()
                };
                for db in dbs {
                    db.locks.clear_contention_history();
                }
            }
        }
    }

    /// Drop the given database.
    ///
    /// All documents and collections will be lost. Active transactions will fail with ABORTED.
    ///
    /// Listeners that are still actively listening to this database will automatically reconnect
    /// to a new database with the same name, so all activity on the same database will still be
    /// observed by these listeners.
    #[instrument(level = Level::DEBUG)]
    pub async fn clear_database(&self, name: &RootRef) {
        let mut databases = self.databases.write().await;
        let Some(database) = databases.remove(name) else {
            debug!("database unknown");
            return;
        };
        let strong = Arc::strong_count(&database) - 1;
        let weak = Arc::weak_count(&database);
        debug!("database cleared, remaining references: strong: {strong}, weak: {weak}");
        database.mark_cleared();
        self.send_project_event(ProjectEvent::DatabaseCleared {
            database: name.clone(),
        });
        self.contention
            .send_if_subscribed(|| ContentionSseEvent::DatabaseCleared {
                database: name.clone(),
            });
        drop(database);
    }

    pub async fn database(&'static self, name: &RootRef) -> Arc<FirestoreDatabase> {
        Arc::clone(
            self.databases
                .get_or_insert(name, || {
                    let db = FirestoreDatabase::new(self, name.clone(), self.default_enterprise);
                    self.send_project_event(ProjectEvent::DatabaseCreated {
                        database: name.clone(),
                    });
                    db
                })
                .await
                .deref(),
        )
    }

    pub async fn database_names(&self) -> Vec<RootRef> {
        self.databases
            .read()
            .await
            .values()
            .map(|db| db.name.clone())
            .collect()
    }

    pub fn listen(
        &'static self,
        request_stream: impl Stream<Item = ListenRequest> + Send + Unpin + 'static,
    ) -> impl Stream<Item = Result<ListenResponse>> {
        Listener::start(self, request_stream)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use googleapis::google::firestore::v1::{Document, Value, Write, write};
    use tokio::sync::broadcast::error::RecvError;

    use super::*;
    use crate::{GenericDatabaseError, reference::DocumentRef};

    #[tokio::test]
    async fn clear_database_closes_events_and_suppresses_stale_inflight_emissions() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let mut proj_rx = project.subscribe_project_events();

        let db1 = project.database(&root).await;
        let mut db_rx = db1.subscribe();

        // Drain initial DatabaseCreated.
        assert!(matches!(
            proj_rx.try_recv().unwrap(),
            ProjectEvent::DatabaseCreated { .. }
        ));

        // Clear while `db1` still has an extra `Arc` held (simulating an in-flight RPC).
        project.clear_database(&root).await;

        // 1. DatabaseEvent channel closes immediately without waiting for `db1` Arc drop.
        assert!(matches!(db_rx.recv().await, Err(RecvError::Closed)));

        // 2. DatabaseCleared is emitted before any subsequent DatabaseCreated.
        assert!(matches!(
            proj_rx.try_recv().unwrap(),
            ProjectEvent::DatabaseCleared { .. }
        ));

        // 3. Stale writes/commits/config changes on cleared `db1` fail or emit no project events.
        let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/d"
            .parse()
            .unwrap();
        let write_err = db1
            .write(Write {
                operation: Some(write::Operation::Update(Document {
                    name: doc_ref.to_string(),
                    fields: HashMap::from([("k".to_string(), Value::integer(1))]),
                    ..Default::default()
                })),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(write_err, GenericDatabaseError::Aborted(_)));

        db1.set_enterprise_edition(true);
        db1.set_optimistic_concurrency(true);
        assert!(proj_rx.try_recv().is_err());

        // 4. Recreating the database emits DatabaseCreated strictly after DatabaseCleared.
        let db2 = project.database(&root).await;
        assert!(!Arc::ptr_eq(&db1, &db2));
        assert!(matches!(
            proj_rx.try_recv().unwrap(),
            ProjectEvent::DatabaseCreated { .. }
        ));
    }

    #[tokio::test]
    async fn clear_database_and_database_creation_preserve_event_order() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let mut proj_rx = project.subscribe_project_events();

        drop(project.database(&root).await);
        assert!(matches!(
            proj_rx.try_recv().unwrap(),
            ProjectEvent::DatabaseCreated { .. }
        ));

        let mut exists = true;
        for _ in 0..32 {
            let root_a = root.clone();
            let root_b = root.clone();
            tokio::join!(project.clear_database(&root_a), async move {
                let _db = project.database(&root_b).await;
            });
            while let Ok(ev) = proj_rx.try_recv() {
                match ev {
                    ProjectEvent::DatabaseCleared { .. } => {
                        assert!(exists, "DatabaseCleared emitted when DB was absent");
                        exists = false;
                    }
                    ProjectEvent::DatabaseCreated { .. } => {
                        assert!(!exists, "DatabaseCreated emitted while DB already existed");
                        exists = true;
                    }
                    _ => {}
                }
            }
        }
    }

    #[tokio::test]
    async fn clear_database_emits_contention_database_cleared_and_suppresses_stale_emissions() {
        use std::task::Poll;

        use futures_util::poll;
        use tokio::sync::broadcast::error::TryRecvError;

        use crate::database::lock::{ContentionSseEvent, LockPriority, SessionKind};

        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db1 = project.database(&root).await;

        let mut sub = project.subscribe_contention_events();
        assert_eq!(project.contention_subscriber_count(), 1);

        let d1: DocumentRef = "projects/p/databases/(default)/documents/c/d"
            .parse()
            .unwrap();
        let holder = db1.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        let held = holder.acquire_write(&d1).await.unwrap();
        let waiter = db1.locks.new_session(
            None,
            SessionKind::PointRead,
            LockPriority::NextSeq,
            false,
            false,
        );
        let mut wait_fut = Box::pin(waiter.acquire_read(&d1));
        assert!(matches!(poll!(&mut wait_fut), Poll::Pending));

        assert_eq!(
            sub.rx.try_recv().unwrap(),
            ContentionSseEvent::ContentionChanged {
                database: root.clone(),
            }
        );

        project.clear_database(&root).await;
        assert_eq!(
            sub.rx.try_recv().unwrap(),
            ContentionSseEvent::DatabaseCleared {
                database: root.clone(),
            }
        );

        // Waiter unblocked by DatabaseCleared; releasing/closing on cleared db1 must not emit stale
        // SSE.
        assert!(wait_fut.await.is_err());
        drop(held);
        holder.close();
        waiter.close();
        assert!(matches!(sub.rx.try_recv(), Err(TryRecvError::Empty)));

        // Snapshot on absent cleared DB returns empty without creating a DB.
        assert!(
            project
                .contention_snapshot(Some(&root))
                .await
                .databases
                .is_empty()
        );
        assert!(matches!(sub.rx.try_recv(), Err(TryRecvError::Empty)));

        drop(sub);
        assert_eq!(project.contention_subscriber_count(), 0);
    }
}
