use std::collections::HashMap;

use googleapis::google::protobuf::Timestamp;

use super::{document::DocumentVersion, reference::DocumentRef};

pub struct DatabaseEvent {
    pub update_time: Timestamp,
    pub updates:     HashMap<DocumentRef, DocumentVersion>,
}
