use std::{collections::HashMap, ops::Deref, sync::Arc};

use googleapis::google::firestore::v1::{ListenRequest, ListenResponse};
use tokio::sync::{RwLock, broadcast};
use tokio_stream::Stream;
use tracing::{Level, debug, instrument};

use crate::{
    FirestoreDatabase, config::FirestoreConfig, error::Result, event::ProjectEvent,
    listener::Listener, reference::RootRef, timeouts::Timeouts, utils::RwLockHashMapExt,
};

const MAX_PROJECT_EVENT_BACKLOG: usize = 1024;

pub struct FirestoreProject {
    pub(crate) timeouts: Timeouts,
    pub(crate) default_enterprise: bool,
    databases: RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>,
    events: broadcast::Sender<ProjectEvent>,
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
        let timeouts = if config.long_contention_timeout {
            Timeouts::CLOUD
        } else {
            Timeouts::FAST
        };
        Self {
            timeouts,
            default_enterprise: config.default_enterprise,
            databases: Default::default(),
            events: broadcast::channel(MAX_PROJECT_EVENT_BACKLOG).0,
        }
    }

    pub fn send_project_event(&self, event: ProjectEvent) {
        let _unused = self.events.send(event);
    }

    pub fn subscribe_project_events(&self) -> broadcast::Receiver<ProjectEvent> {
        self.events.subscribe()
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
        let Some(database) = self.databases.write().await.remove(name) else {
            debug!("database unknown");
            return;
        };
        let strong = Arc::strong_count(&database) - 1;
        let weak = Arc::weak_count(&database);
        debug!("database cleared, remaining references: strong: {strong}, weak: {weak}");
        self.send_project_event(ProjectEvent::DatabaseCleared {
            database: name.clone(),
        });
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
