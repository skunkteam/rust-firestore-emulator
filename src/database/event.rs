use super::document::DocumentVersion;
use prost_types::Timestamp;
use std::collections::HashMap;
use string_cache::DefaultAtom;

pub struct DatabaseEvent {
    pub update_time: Timestamp,
    pub updates:     HashMap<DefaultAtom, DocumentVersion>,
}
