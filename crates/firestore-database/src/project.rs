use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, OnceLock},
};

use googleapis::google::firestore::v1::{ListenRequest, ListenResponse};
use tokio::sync::RwLock;
use tokio_stream::Stream;

use crate::{
    config::FirestoreConfig, error::Result, listener::Listener, reference::RootRef,
    utils::RwLockHashMapExt, FirestoreDatabase,
};

pub struct FirestoreProject {
    config:    FirestoreConfig,
    databases: RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>,
}

impl std::fmt::Debug for FirestoreProject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreProject")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

static INSTANCE: OnceLock<FirestoreProject> = OnceLock::new();

impl FirestoreProject {
    pub fn init(config: FirestoreConfig) {
        INSTANCE
            .set(FirestoreProject {
                config,
                databases: Default::default(),
            })
            .expect("FirestoreProject already initialized");
    }

    pub fn get() -> &'static Self {
        INSTANCE
            .get()
            .expect("FirestoreProject not yet initialized")
    }

    pub async fn clear_database(&self, name: &RootRef) {
        self.databases.write().await.remove(name);
    }

    pub async fn database(&'static self, name: &RootRef) -> Arc<FirestoreDatabase> {
        Arc::clone(
            self.databases
                .get_or_insert(name, || FirestoreDatabase::new(&self.config, name.clone()))
                .await
                .deref(),
        )
    }

    pub async fn database_names(&self) -> Vec<String> {
        self.databases
            .read()
            .await
            .values()
            .map(|db| db.name.to_string())
            .collect()
    }

    pub fn listen(
        &'static self,
        request_stream: impl Stream<Item = ListenRequest> + Send + Unpin + 'static,
    ) -> impl Stream<Item = Result<ListenResponse>> {
        Listener::start(self, request_stream)
    }
}
