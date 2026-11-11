use std::{collections::HashMap, sync::Weak};

use googleapis::google::protobuf::Timestamp;

use super::{
    document::DocumentVersion,
    reference::{DocumentRef, RootRef},
};
use crate::FirestoreDatabase;

#[derive(Debug)]
pub struct DatabaseEvent {
    pub database:    Weak<FirestoreDatabase>,
    pub update_time: Timestamp,
    pub updates:     HashMap<DocumentRef, DocumentVersion>,
}

#[derive(Clone, Debug)]
pub enum ProjectEvent {
    DatabaseCreated {
        database: RootRef,
    },
    DatabaseCleared {
        database: RootRef,
    },
    DatabaseConfigChanged {
        database: RootRef,
    },
    DocumentsChanged {
        database: RootRef,
        paths: Vec<String>,
        update_time: Timestamp,
    },
}
