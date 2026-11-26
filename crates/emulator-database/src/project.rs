use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use googleapis::google::{
    firestore::v1::{ListenRequest, ListenResponse},
    protobuf::Timestamp,
};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_stream::{Stream, StreamExt};
use tracing::{Level, debug, instrument};

use crate::{
    ContentionSnapshot, ContentionSseEvent, ContentionSubscription, DatabaseContentionSnapshot,
    DatabaseEdition, FirestoreDatabase, GenericDatabaseError,
    config::FirestoreConfig,
    database::lock::ContentionBroadcaster,
    error::Result,
    event::ProjectEvent,
    listener::Listener,
    reference::RootRef,
    snapshot::{
        ExportSnapshotPlan, ExportTarget, ImportSummary, ImportedDatabase, SnapshotError,
        codec::SnapshotDecoder,
    },
    timeouts::Timeouts,
};

const MAX_PROJECT_EVENT_BACKLOG: usize = 1024;

pub struct FirestoreProject {
    pub(crate) timeouts: Timeouts,
    pub(crate) default_enterprise: bool,
    databases: RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>,
    events: broadcast::Sender<ProjectEvent>,
    contention: ContentionBroadcaster,
    import_lock: Mutex<()>,
    next_import_id: AtomicU64,
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
            import_lock: Mutex::new(()),
            next_import_id: AtomicU64::new(1),
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
                let databases = self
                    .get_existing_database(root)
                    .await
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
                if let Some(db) = self.get_existing_database(root).await {
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
    /// Compatible listeners that are still actively listening to this database will automatically
    /// reconnect to a new or replacement database with the same name, so all activity on that
    /// database will still be observed. Query listeners whose query edition no longer matches the
    /// replacement (or project default) database edition terminate with `FAILED_PRECONDITION`
    /// without lazily re-creating an edition-mismatched database.
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

    pub async fn get_existing_database(&self, name: &RootRef) -> Option<Arc<FirestoreDatabase>> {
        self.databases.read().await.get(name).cloned()
    }

    async fn get_or_create_database_with(
        &'static self,
        name: &RootRef,
        required_edition: Option<DatabaseEdition>,
    ) -> std::result::Result<Arc<FirestoreDatabase>, DatabaseEdition> {
        if let Some(db) = self.databases.read().await.get(name) {
            return Ok(Arc::clone(db));
        }
        let mut databases = self.databases.write().await;
        if let Some(db) = databases.get(name) {
            return Ok(Arc::clone(db));
        }
        let default_edition = DatabaseEdition::from_enterprise(self.default_enterprise);
        if required_edition.is_some_and(|req| req != default_edition) {
            return Err(default_edition);
        }
        let db = FirestoreDatabase::new(self, name.clone(), self.default_enterprise);
        self.send_project_event(ProjectEvent::DatabaseCreated {
            database:  name.clone(),
            import_id: None,
        });
        databases.insert(name.clone(), Arc::clone(&db));
        Ok(db)
    }

    pub async fn database(&'static self, name: &RootRef) -> Arc<FirestoreDatabase> {
        self.get_or_create_database_with(name, None)
            .await
            .expect("unconditional database creation cannot mismatch edition")
    }

    pub(crate) async fn database_for_listener(
        &'static self,
        name: &RootRef,
        required_edition: Option<DatabaseEdition>,
    ) -> std::result::Result<Arc<FirestoreDatabase>, DatabaseEdition> {
        self.get_or_create_database_with(name, required_edition)
            .await
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

    pub async fn prepare_export(
        &self,
        targets: Option<&[RootRef]>,
    ) -> std::result::Result<ExportSnapshotPlan, GenericDatabaseError> {
        let databases = self.databases.read().await;
        let export_targets = match targets {
            Some(roots) => {
                let unique: BTreeSet<&RootRef> = roots.iter().collect();
                let mut resolved = Vec::with_capacity(unique.len());
                for root in unique {
                    let db = databases.get(root).ok_or_else(|| {
                        GenericDatabaseError::not_found(format!("database not found: {root}"))
                    })?;
                    resolved.push(ExportTarget {
                        database: Arc::clone(db),
                        settings: db.settings(),
                    });
                }
                resolved
            }
            None => {
                let mut all: Vec<ExportTarget> = databases
                    .values()
                    .map(|db| ExportTarget {
                        database: Arc::clone(db),
                        settings: db.settings(),
                    })
                    .collect();
                all.sort_by(|a, b| a.database.name.cmp(&b.database.name));
                all
            }
        };
        let snapshot_time = Timestamp::now();
        drop(databases);
        Ok(ExportSnapshotPlan::new(snapshot_time, export_targets))
    }

    pub async fn import_snapshot(
        &'static self,
        mut stream: impl Stream<Item = std::result::Result<Bytes, SnapshotError>> + Unpin,
    ) -> std::result::Result<ImportSummary, SnapshotError> {
        let _import_guard = self
            .import_lock
            .try_lock()
            .map_err(|_| SnapshotError::ImportInProgress)?;

        let mut decoder = SnapshotDecoder::new();
        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res?;
            decoder.feed(&chunk)?;
        }
        let prepared = decoder.finish()?;
        if prepared.is_empty() {
            return Ok(ImportSummary {
                databases: Vec::new(),
                import_id: None,
            });
        }

        let import_id = self.next_import_id.fetch_add(1, Ordering::Relaxed);
        let mut replaced_dbs = Vec::new();
        let mut summary_dbs = Vec::with_capacity(prepared.len());

        {
            let mut databases = self.databases.write().await;

            for db_spec in &prepared {
                if let Some(old_db) = databases.get(db_spec.name()) {
                    old_db.mark_cleared();
                }
            }

            let import_time = Timestamp::now();

            for db_spec in prepared {
                let (root, settings, document_count, new_db) =
                    db_spec.into_database(self, import_time);
                let old_db = databases.insert(root.clone(), new_db);
                let replaced = old_db.is_some();
                if let Some(old_db) = old_db {
                    replaced_dbs.push(old_db);
                    self.send_project_event(ProjectEvent::DatabaseReplaced {
                        database: root.clone(),
                        import_id,
                    });
                    self.contention
                        .send_if_subscribed(|| ContentionSseEvent::DatabaseCleared {
                            database: root.clone(),
                        });
                } else {
                    self.send_project_event(ProjectEvent::DatabaseCreated {
                        database:  root.clone(),
                        import_id: Some(import_id),
                    });
                }
                summary_dbs.push(ImportedDatabase {
                    name: root,
                    settings,
                    document_count,
                    replaced,
                });
            }
        }

        drop(replaced_dbs);
        drop(_import_guard);

        Ok(ImportSummary {
            databases: summary_dbs,
            import_id: Some(import_id),
        })
    }
}

#[cfg(test)]
mod tests;
