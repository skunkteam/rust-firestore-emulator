use std::{collections::HashMap, sync::Weak};

use googleapis::google::protobuf::Timestamp;

use super::{document::DocumentVersion, reference::DocumentRef};
use crate::FirestoreDatabase;

#[derive(Debug)]
pub struct DatabaseEvent {
    pub database:    Weak<FirestoreDatabase>,
    pub update_time: Timestamp,
    pub updates:     HashMap<DocumentRef, DocumentVersion>,
}
