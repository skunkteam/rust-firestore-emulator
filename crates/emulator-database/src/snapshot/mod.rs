use std::{io, sync::Arc};

use bytes::Bytes;
use googleapis::google::protobuf::Timestamp;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};

use self::codec::SnapshotEncoder;
use crate::{
    FirestoreDatabase, GenericDatabaseError, config::DatabaseSettings, reference::RootRef,
};

pub(crate) mod codec;
pub(crate) mod wire;

pub const SNAPSHOT_MAGIC: [u8; 8] = *b"FSEMUSNP";
pub const EOF_MAGIC: [u8; 8] = *b"FSEMUEOF";
pub const FORMAT_MAJOR: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SNAPSHOT_DATABASES: usize = 10_000;
pub const EXPORT_CHUNK_BYTES: usize = 64 * 1024;
const EXPORT_CHANNEL_CAPACITY: usize = 16;
const EXPORT_CANCEL_CHECK_INTERVAL: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    #[error("Invalid snapshot format: {0}")]
    InvalidFormat(String),
    #[error(
        "Unsupported snapshot version: found major {found_major}, supported major \
         {supported_major}"
    )]
    UnsupportedVersion {
        found_major:     u16,
        supported_major: u16,
    },
    #[error("Truncated snapshot: {0}")]
    Truncated(String),
    #[error("Snapshot frame too large: {len} bytes exceeds maximum {max} bytes")]
    FrameTooLarge { len: usize, max: usize },
    #[error(transparent)]
    Validation(#[from] GenericDatabaseError),
    #[error("A snapshot import is already in progress")]
    ImportInProgress,
    #[error("Snapshot stream error: {0}")]
    Stream(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportedDatabase {
    pub name: RootRef,
    pub settings: DatabaseSettings,
    pub document_count: u64,
    pub replaced: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportSummary {
    pub databases: Vec<ImportedDatabase>,
    pub import_id: Option<u64>,
}

impl ImportSummary {
    pub fn total_databases(&self) -> usize {
        self.databases.len()
    }

    pub fn total_documents(&self) -> u64 {
        self.databases.iter().map(|db| db.document_count).sum()
    }
}

#[derive(Debug)]
pub(crate) struct ExportTarget {
    pub(crate) database: Arc<FirestoreDatabase>,
    pub(crate) settings: DatabaseSettings,
}

#[derive(Debug)]
pub struct ExportSnapshotPlan {
    snapshot_time: Timestamp,
    targets: Vec<ExportTarget>,
}

impl ExportSnapshotPlan {
    pub(crate) fn new(snapshot_time: Timestamp, targets: Vec<ExportTarget>) -> Self {
        Self {
            snapshot_time,
            targets,
        }
    }

    pub fn snapshot_time(&self) -> Timestamp {
        self.snapshot_time
    }

    pub fn single_target(&self) -> Option<&RootRef> {
        (self.targets.len() == 1).then(|| &self.targets[0].database.name)
    }

    pub fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
        let (tx, rx) = mpsc::channel(EXPORT_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let snapshot_time = self.snapshot_time;
            let mut encoder = SnapshotEncoder::new(snapshot_time);

            for target in self.targets {
                if tx.is_closed() {
                    return;
                }
                if let Err(err) = encoder.start_database(&target.database.name, target.settings) {
                    let _unused = tx.send(Err(io::Error::other(err))).await;
                    return;
                }
                if !flush_full_chunks(&mut encoder, &tx).await {
                    return;
                }

                let mut collections = target.database.get_all_collections().await;
                collections.sort_by(|a, b| a.name.cmp(&b.name));

                for col in collections {
                    if tx.is_closed() {
                        return;
                    }
                    let mut docs = col.docs().await;
                    docs.sort_by(|a, b| a.name.cmp(&b.name));
                    for (idx, meta) in docs.into_iter().enumerate() {
                        if idx.is_multiple_of(EXPORT_CANCEL_CHECK_INTERVAL) && tx.is_closed() {
                            return;
                        }
                        let Some(stored) = meta.read_mvcc_at(snapshot_time) else {
                            continue;
                        };
                        if let Err(err) = encoder.write_document(&stored.name, &stored.fields) {
                            let _unused = tx.send(Err(io::Error::other(err))).await;
                            return;
                        }
                        if !flush_full_chunks(&mut encoder, &tx).await {
                            return;
                        }
                    }
                }
            }

            if tx.is_closed() {
                return;
            }
            if let Err(err) = encoder.finish() {
                let _unused = tx.send(Err(io::Error::other(err))).await;
                return;
            }
            while let Some(chunk) = encoder.take_chunk() {
                if tx.send(Ok(chunk)).await.is_err() {
                    return;
                }
            }
        });
        ReceiverStream::new(rx)
    }
}

async fn flush_full_chunks(
    encoder: &mut SnapshotEncoder,
    tx: &mpsc::Sender<io::Result<Bytes>>,
) -> bool {
    while encoder.has_full_chunk() {
        let Some(chunk) = encoder.take_chunk() else {
            break;
        };
        if tx.send(Ok(chunk)).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::google::firestore::v1::Value;
    use tokio_stream::StreamExt;

    use crate::{
        FirestoreConfig, FirestoreProject,
        reference::{CollectionRef, RootRef},
    };

    #[tokio::test]
    async fn dropping_export_stream_aborts_background_producer_promptly() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let col: CollectionRef = format!("{root}/items").parse().unwrap();
        let db = project.database(&root).await;

        for i in 0..300 {
            db.create_doc(
                &col,
                &format!("d{i:03}"),
                HashMap::from([("v".to_string(), Value::integer(i))]),
                None,
            )
            .await
            .unwrap();
        }

        let plan = project.prepare_export(None).await.unwrap();
        let mut stream = plan.into_stream();
        drop(stream.next().await);
        drop(stream);
        tokio::task::yield_now().await;
    }
}
