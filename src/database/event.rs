use std::collections::HashMap;

use googleapis::Timestamp;
use string_cache::DefaultAtom;

use super::document::DocumentVersion;

pub struct DatabaseEvent {
    pub update_time: Timestamp,
    pub updates:     HashMap<DefaultAtom, DocumentVersion>,
}
