use std::collections::HashMap;

use googleapis::google::{firestore::v1::Value, protobuf::Timestamp};

use super::SnapshotError;
use crate::config::{DatabaseConcurrencyMode, DatabaseEdition};

pub(super) const TAG_SNAPSHOT_HEADER: u8 = 0x01;
pub(super) const TAG_DATABASE_START: u8 = 0x02;
pub(super) const TAG_DOCUMENT_RECORD: u8 = 0x03;
pub(super) const TAG_END_OF_SNAPSHOT: u8 = 0x04;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub(super) enum DatabaseEditionWire {
    Unspecified = 0,
    Standard = 1,
    Enterprise = 2,
}

impl From<DatabaseEdition> for DatabaseEditionWire {
    fn from(edition: DatabaseEdition) -> Self {
        match edition {
            DatabaseEdition::Standard => Self::Standard,
            DatabaseEdition::Enterprise => Self::Enterprise,
        }
    }
}

impl TryFrom<DatabaseEditionWire> for DatabaseEdition {
    type Error = SnapshotError;

    fn try_from(value: DatabaseEditionWire) -> Result<Self, Self::Error> {
        match value {
            DatabaseEditionWire::Standard => Ok(Self::Standard),
            DatabaseEditionWire::Enterprise => Ok(Self::Enterprise),
            DatabaseEditionWire::Unspecified => Err(SnapshotError::InvalidFormat(
                "database_edition must not be DATABASE_EDITION_UNSPECIFIED".to_string(),
            )),
        }
    }
}

pub(super) fn decode_database_edition(raw: i32) -> Result<DatabaseEdition, SnapshotError> {
    let wire = DatabaseEditionWire::try_from(raw).map_err(|_| {
        SnapshotError::InvalidFormat(format!("unknown database_edition discriminant: {raw}"))
    })?;
    DatabaseEdition::try_from(wire)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub(super) enum ConcurrencyModeWire {
    Unspecified = 0,
    Pessimistic = 1,
    Optimistic = 2,
}

impl From<DatabaseConcurrencyMode> for ConcurrencyModeWire {
    fn from(mode: DatabaseConcurrencyMode) -> Self {
        match mode {
            DatabaseConcurrencyMode::Pessimistic => Self::Pessimistic,
            DatabaseConcurrencyMode::Optimistic => Self::Optimistic,
        }
    }
}

impl TryFrom<ConcurrencyModeWire> for DatabaseConcurrencyMode {
    type Error = SnapshotError;

    fn try_from(value: ConcurrencyModeWire) -> Result<Self, Self::Error> {
        match value {
            ConcurrencyModeWire::Pessimistic => Ok(Self::Pessimistic),
            ConcurrencyModeWire::Optimistic => Ok(Self::Optimistic),
            ConcurrencyModeWire::Unspecified => Err(SnapshotError::InvalidFormat(
                "concurrency_mode must not be CONCURRENCY_MODE_UNSPECIFIED".to_string(),
            )),
        }
    }
}

pub(super) fn decode_concurrency_mode(raw: i32) -> Result<DatabaseConcurrencyMode, SnapshotError> {
    let wire = ConcurrencyModeWire::try_from(raw).map_err(|_| {
        SnapshotError::InvalidFormat(format!("unknown concurrency_mode discriminant: {raw}"))
    })?;
    DatabaseConcurrencyMode::try_from(wire)
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct SnapshotHeader {
    #[prost(string, tag = "1")]
    pub(super) emulator_version: String,
    #[prost(message, optional, tag = "2")]
    pub(super) snapshot_time:    Option<Timestamp>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub(super) struct DatabaseStart {
    #[prost(string, tag = "1")]
    pub(super) project_id: String,
    #[prost(string, tag = "2")]
    pub(super) database_id: String,
    #[prost(enumeration = "DatabaseEditionWire", tag = "3")]
    pub(super) database_edition: i32,
    #[prost(enumeration = "ConcurrencyModeWire", tag = "4")]
    pub(super) concurrency_mode: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct DocumentRecord {
    #[prost(string, tag = "1")]
    pub(super) document_path: String,
    #[prost(map = "string, message", tag = "2")]
    pub(super) fields: HashMap<String, Value>,
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub(super) struct EndOfSnapshot {
    #[prost(uint64, tag = "1")]
    pub(super) total_databases: u64,
    #[prost(uint64, tag = "2")]
    pub(super) total_documents: u64,
}
