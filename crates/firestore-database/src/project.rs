use std::{collections::HashMap, ops::Deref, sync::Arc};

use googleapis::google::firestore::v1::{ListenRequest, ListenResponse};
use tokio::sync::RwLock;
use tokio_stream::Stream;

use crate::{
    config::FirestoreConfig, error::Result, listener::Listener, reference::RootRef,
    timeouts::Timeouts, utils::RwLockHashMapExt, FirestoreDatabase,
};

#[derive(Debug)]
pub struct FirestoreProject {
    pub(crate) timeouts: Timeouts,
    databases: RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>,
}

impl FirestoreProject {
    pub fn new(config: FirestoreConfig) -> Self {
        let timeouts = if config.long_contention_timeout {
            Timeouts::CLOUD
        } else {
            Timeouts::FAST
        };
        FirestoreProject {
            timeouts,
            databases: Default::default(),
        }
    }

    pub async fn clear_database(&self, name: &RootRef) {
        self.databases.write().await.remove(name);
    }

    pub async fn database(&'static self, name: &RootRef) -> Arc<FirestoreDatabase> {
        Arc::clone(
            self.databases
                .get_or_insert(name, || FirestoreDatabase::new(self, name.clone()))
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
