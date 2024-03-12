use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, OnceLock},
};

use googleapis::google::firestore::v1::{ListenRequest, ListenResponse};
use tokio::sync::RwLock;
use tokio_stream::Stream;

use crate::{
    error::Result, listener::Listener, reference::RootRef, utils::RwLockHashMapExt,
    FirestoreDatabase,
};

pub struct FirestoreProject {
    databases: RwLock<HashMap<RootRef, Arc<FirestoreDatabase>>>,
}

impl FirestoreProject {
    pub fn get() -> &'static Self {
        static INSTANCE: OnceLock<FirestoreProject> = OnceLock::new();
        INSTANCE.get_or_init(|| FirestoreProject {
            databases: Default::default(),
        })
    }

    pub async fn clear_database(&self, name: &RootRef) {
        self.databases.write().await.remove(name);
    }

    pub async fn database(&self, name: &RootRef) -> Arc<FirestoreDatabase> {
        Arc::clone(
            self.databases
                .get_or_insert(name, || FirestoreDatabase::new(name.clone()))
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
