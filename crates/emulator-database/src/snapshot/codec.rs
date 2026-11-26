use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use googleapis::google::{firestore::v1::Value, protobuf::Timestamp};
use prost::Message;
use string_cache::DefaultAtom;

use super::{
    EOF_MAGIC, EXPORT_CHUNK_BYTES, FORMAT_MAJOR, MAX_FRAME_BYTES, MAX_SNAPSHOT_DATABASES,
    SNAPSHOT_MAGIC, SnapshotError,
    wire::{
        self, ConcurrencyModeWire, DatabaseEditionWire, DatabaseStart, DocumentRecord,
        EndOfSnapshot, SnapshotHeader, TAG_DATABASE_START, TAG_DOCUMENT_RECORD,
        TAG_END_OF_SNAPSHOT, TAG_SNAPSHOT_HEADER,
    },
};
use crate::{
    FirestoreDatabase, FirestoreProject, GenericDatabaseError,
    config::DatabaseSettings,
    database::{
        collection::Collection, document::DocumentMeta, validation::validate_writable_fields,
    },
    reference::{CollectionRef, DocumentRef, RootRef},
};

const FILE_HEADER_LEN: usize = SNAPSHOT_MAGIC.len() + size_of::<u16>();
const FRAME_PREFIX_LEN: usize = 1 + size_of::<u32>();
const TRAILER_LEN: usize = size_of::<u32>() + EOF_MAGIC.len();
const INITIAL_FRAME_BUF_CAP: usize = 64 * 1024;

type PreparedCollectionDocs = HashMap<DefaultAtom, (DocumentRef, HashMap<String, Value>)>;

#[derive(Debug)]
pub(crate) struct PreparedDatabase {
    name: RootRef,
    settings: DatabaseSettings,
    collections: HashMap<DefaultAtom, PreparedCollectionDocs>,
    document_count: u64,
}

impl PreparedDatabase {
    pub(crate) fn name(&self) -> &RootRef {
        &self.name
    }

    pub(crate) fn into_database(
        self,
        project: &'static FirestoreProject,
        import_time: Timestamp,
    ) -> (RootRef, DatabaseSettings, u64, Arc<FirestoreDatabase>) {
        let root = self.name;
        let settings = self.settings;
        let document_count = self.document_count;

        let mut collections = HashMap::with_capacity(self.collections.len());
        for (col_id, raw_docs) in self.collections {
            let col_ref = CollectionRef::new(root.clone(), col_id.clone());
            let mut doc_metas = HashMap::with_capacity(raw_docs.len());
            for (doc_id, (doc_ref, fields)) in raw_docs {
                doc_metas.insert(
                    doc_id,
                    Arc::new(DocumentMeta::from_imported_version(
                        doc_ref,
                        fields,
                        import_time,
                    )),
                );
            }
            collections.insert(col_id, Arc::new(Collection::from_docs(col_ref, doc_metas)));
        }

        let db = FirestoreDatabase::new_with_config_and_collections(
            project,
            root.clone(),
            settings,
            collections,
        );
        (root, settings, document_count, db)
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotEncoder {
    out_buf: BytesMut,
    hasher: Hasher,
    current_root: Option<RootRef>,
    total_databases: u64,
    total_documents: u64,
    finished: bool,
}

impl SnapshotEncoder {
    pub(crate) fn new(snapshot_time: Timestamp) -> Self {
        let mut encoder = Self {
            out_buf: BytesMut::with_capacity(EXPORT_CHUNK_BYTES),
            hasher: Hasher::new(),
            current_root: None,
            total_databases: 0,
            total_documents: 0,
            finished: false,
        };

        let mut file_header = [0u8; FILE_HEADER_LEN];
        file_header[..SNAPSHOT_MAGIC.len()].copy_from_slice(&SNAPSHOT_MAGIC);
        file_header[SNAPSHOT_MAGIC.len()..].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
        encoder.hasher.update(&file_header);
        encoder.out_buf.extend_from_slice(&file_header);

        let header = SnapshotHeader {
            emulator_version: env!("CARGO_PKG_VERSION").to_string(),
            snapshot_time:    Some(snapshot_time),
        };
        encoder
            .write_frame(TAG_SNAPSHOT_HEADER, &header)
            .expect("SnapshotHeader cannot exceed MAX_FRAME_BYTES");
        encoder
    }

    pub(crate) fn start_database(
        &mut self,
        root: &RootRef,
        settings: DatabaseSettings,
    ) -> Result<(), SnapshotError> {
        if self.finished {
            return Err(SnapshotError::InvalidFormat(
                "cannot start database after snapshot finish".to_string(),
            ));
        }

        let frame = DatabaseStart {
            project_id: root.project_id.to_string(),
            database_id: root.database_id.to_string(),
            database_edition: DatabaseEditionWire::from(settings.edition) as i32,
            concurrency_mode: ConcurrencyModeWire::from(settings.concurrency_mode) as i32,
        };
        self.write_frame(TAG_DATABASE_START, &frame)?;
        self.current_root = Some(root.clone());
        self.total_databases += 1;
        Ok(())
    }

    pub(crate) fn write_document(
        &mut self,
        doc_ref: &DocumentRef,
        fields: &HashMap<String, Value>,
    ) -> Result<(), SnapshotError> {
        if self.finished {
            return Err(SnapshotError::InvalidFormat(
                "cannot write document after snapshot finish".to_string(),
            ));
        }
        let Some(current_root) = &self.current_root else {
            return Err(SnapshotError::InvalidFormat(
                "cannot write document before starting a database".to_string(),
            ));
        };
        if &doc_ref.collection_ref.root_ref != current_root {
            return Err(SnapshotError::InvalidFormat(format!(
                "document {} does not belong to active database {}",
                doc_ref, current_root
            )));
        }

        let parts = [
            doc_ref.collection_ref.collection_id.as_ref(),
            "/",
            doc_ref.document_id.as_ref(),
        ];
        let encoded_len = wire::document_record_encoded_len_parts(&parts, fields);
        self.write_frame_raw(TAG_DOCUMENT_RECORD, encoded_len, |buf| {
            wire::encode_document_record_parts(&parts, fields, buf);
            Ok(())
        })?;
        self.total_documents += 1;
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), SnapshotError> {
        if self.finished {
            return Err(SnapshotError::InvalidFormat(
                "snapshot encoder already finished".to_string(),
            ));
        }
        self.finished = true;

        let eos = EndOfSnapshot {
            total_databases: self.total_databases,
            total_documents: self.total_documents,
        };
        self.write_frame(TAG_END_OF_SNAPSHOT, &eos)?;

        let checksum = self.hasher.clone().finalize();
        self.out_buf.put_u32_le(checksum);
        self.out_buf.extend_from_slice(&EOF_MAGIC);
        Ok(())
    }

    pub(super) fn has_full_chunk(&self) -> bool {
        self.out_buf.len() >= EXPORT_CHUNK_BYTES
    }

    pub(crate) fn take_chunk(&mut self) -> Option<Bytes> {
        if self.out_buf.is_empty() {
            return None;
        }
        let len = self.out_buf.len().min(EXPORT_CHUNK_BYTES);
        Some(self.out_buf.split_to(len).freeze())
    }

    #[cfg(test)]
    pub(crate) fn encode_to_writer(
        &mut self,
        writer: &mut impl std::io::Write,
    ) -> std::io::Result<()> {
        while let Some(chunk) = self.take_chunk() {
            writer.write_all(&chunk)?;
        }
        Ok(())
    }

    fn write_frame<M: Message>(&mut self, tag: u8, msg: &M) -> Result<(), SnapshotError> {
        let encoded_len = msg.encoded_len();
        self.write_frame_raw(tag, encoded_len, |buf| {
            msg.encode(buf).map_err(|err| {
                SnapshotError::InvalidFormat(format!("protobuf encode error: {err}"))
            })
        })
    }

    fn write_frame_raw(
        &mut self,
        tag: u8,
        payload_len: usize,
        encode_payload: impl FnOnce(&mut BytesMut) -> Result<(), SnapshotError>,
    ) -> Result<(), SnapshotError> {
        if payload_len > MAX_FRAME_BYTES {
            return Err(SnapshotError::FrameTooLarge {
                len: payload_len,
                max: MAX_FRAME_BYTES,
            });
        }

        let start = self.out_buf.len();
        self.out_buf.reserve(FRAME_PREFIX_LEN + payload_len);
        self.out_buf.put_u8(tag);
        self.out_buf.put_u32_le(payload_len as u32);

        if let Err(err) = encode_payload(&mut self.out_buf) {
            self.out_buf.truncate(start);
            return Err(err);
        }

        let actual_len = self.out_buf.len() - start - FRAME_PREFIX_LEN;
        if actual_len != payload_len {
            self.out_buf.truncate(start);
            return Err(SnapshotError::InvalidFormat(format!(
                "snapshot frame encoded length mismatch: expected {payload_len}, got {actual_len}"
            )));
        }

        self.hasher.update(&self.out_buf[start..]);
        Ok(())
    }
}

#[derive(Debug)]
enum DecodePhase {
    FileHeader,
    FramePrefix,
    FramePayload {
        tag: u8,
        payload_len: usize,
        buf: BytesMut,
    },
    Trailer,
    Done,
    Failed,
}

#[derive(Debug)]
pub(crate) struct SnapshotDecoder {
    phase: DecodePhase,
    seen_header: bool,
    small_buf: BytesMut,
    hasher: Hasher,
    seen_databases: HashSet<RootRef>,
    databases: Vec<PreparedDatabase>,
    current_db: Option<PreparedDatabase>,
    total_databases: u64,
    total_documents: u64,
}

impl Default for SnapshotDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotDecoder {
    pub(crate) fn new() -> Self {
        Self {
            phase: DecodePhase::FileHeader,
            seen_header: false,
            small_buf: BytesMut::with_capacity(TRAILER_LEN),
            hasher: Hasher::new(),
            seen_databases: HashSet::new(),
            databases: Vec::new(),
            current_db: None,
            total_databases: 0,
            total_documents: 0,
        }
    }

    fn failed_state_error() -> SnapshotError {
        SnapshotError::InvalidFormat("snapshot decoder is in a failed state".to_string())
    }

    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<(), SnapshotError> {
        if matches!(self.phase, DecodePhase::Failed) {
            return Err(Self::failed_state_error());
        }
        if let Err(err) = self.feed_inner(chunk) {
            self.phase = DecodePhase::Failed;
            self.small_buf.clear();
            return Err(err);
        }
        Ok(())
    }

    fn fill_small_buf(&mut self, chunk: &mut &[u8], target_len: usize) -> bool {
        let needed = target_len - self.small_buf.len();
        let take = chunk.len().min(needed);
        self.small_buf.extend_from_slice(&chunk[..take]);
        *chunk = &chunk[take..];
        self.small_buf.len() == target_len
    }

    fn finish_frame(&mut self, tag: u8, payload: &[u8]) -> Result<(), SnapshotError> {
        let is_end_of_snapshot = self.handle_frame(tag, payload)?;
        self.phase = if is_end_of_snapshot {
            DecodePhase::Trailer
        } else {
            DecodePhase::FramePrefix
        };
        Ok(())
    }

    fn feed_inner(&mut self, mut chunk: &[u8]) -> Result<(), SnapshotError> {
        while !chunk.is_empty() {
            match &mut self.phase {
                DecodePhase::FileHeader => {
                    if self.fill_small_buf(&mut chunk, FILE_HEADER_LEN) {
                        let magic = &self.small_buf[..SNAPSHOT_MAGIC.len()];
                        if magic != SNAPSHOT_MAGIC {
                            return Err(SnapshotError::InvalidFormat(
                                "invalid snapshot magic header".to_string(),
                            ));
                        }
                        let major = u16::from_le_bytes([
                            self.small_buf[SNAPSHOT_MAGIC.len()],
                            self.small_buf[SNAPSHOT_MAGIC.len() + 1],
                        ]);
                        if major != FORMAT_MAJOR {
                            return Err(SnapshotError::UnsupportedVersion {
                                found_major:     major,
                                supported_major: FORMAT_MAJOR,
                            });
                        }
                        self.hasher.update(&self.small_buf[..FILE_HEADER_LEN]);
                        self.small_buf.clear();
                        self.phase = DecodePhase::FramePrefix;
                    }
                }
                DecodePhase::FramePrefix => {
                    if self.fill_small_buf(&mut chunk, FRAME_PREFIX_LEN) {
                        let tag = self.small_buf[0];
                        let payload_len = u32::from_le_bytes([
                            self.small_buf[1],
                            self.small_buf[2],
                            self.small_buf[3],
                            self.small_buf[4],
                        ]) as usize;
                        if payload_len > MAX_FRAME_BYTES {
                            return Err(SnapshotError::FrameTooLarge {
                                len: payload_len,
                                max: MAX_FRAME_BYTES,
                            });
                        }
                        self.validate_frame_prefix(tag)?;
                        self.hasher.update(&self.small_buf[..FRAME_PREFIX_LEN]);
                        self.small_buf.clear();

                        if payload_len == 0 {
                            self.finish_frame(tag, &[])?;
                        } else {
                            self.phase = DecodePhase::FramePayload {
                                tag,
                                payload_len,
                                buf: BytesMut::with_capacity(
                                    payload_len.min(INITIAL_FRAME_BUF_CAP),
                                ),
                            };
                        }
                    }
                }
                DecodePhase::FramePayload {
                    tag,
                    payload_len,
                    buf,
                } => {
                    let needed = *payload_len - buf.len();
                    let take = chunk.len().min(needed);
                    self.hasher.update(&chunk[..take]);
                    buf.extend_from_slice(&chunk[..take]);
                    chunk = &chunk[take..];

                    if buf.len() == *payload_len {
                        let tag = *tag;
                        let payload = std::mem::take(buf).freeze();
                        self.finish_frame(tag, &payload)?;
                    }
                }
                DecodePhase::Trailer => {
                    if self.fill_small_buf(&mut chunk, TRAILER_LEN) {
                        let expected_crc = u32::from_le_bytes([
                            self.small_buf[0],
                            self.small_buf[1],
                            self.small_buf[2],
                            self.small_buf[3],
                        ]);
                        let actual_crc = self.hasher.clone().finalize();
                        if actual_crc != expected_crc {
                            return Err(SnapshotError::InvalidFormat(format!(
                                "snapshot checksum mismatch: expected {expected_crc:#010x}, got \
                                 {actual_crc:#010x}"
                            )));
                        }
                        let eof_magic = &self.small_buf[size_of::<u32>()..TRAILER_LEN];
                        if eof_magic != EOF_MAGIC {
                            return Err(SnapshotError::InvalidFormat(
                                "invalid snapshot EOF magic trailer".to_string(),
                            ));
                        }
                        self.small_buf.clear();
                        self.phase = DecodePhase::Done;
                    }
                }
                DecodePhase::Done => {
                    return Err(SnapshotError::InvalidFormat(format!(
                        "unexpected {} trailing byte(s) after snapshot EOF trailer",
                        chunk.len()
                    )));
                }
                DecodePhase::Failed => return Err(Self::failed_state_error()),
            }
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<PreparedDatabase>, SnapshotError> {
        match self.phase {
            DecodePhase::Done => {
                debug_assert!(self.current_db.is_none());
                Ok(self.databases)
            }
            DecodePhase::FileHeader => Err(SnapshotError::Truncated(
                "unexpected EOF while reading snapshot header".to_string(),
            )),
            DecodePhase::FramePrefix => Err(SnapshotError::Truncated(
                "unexpected EOF before EndOfSnapshot frame".to_string(),
            )),
            DecodePhase::FramePayload { .. } => Err(SnapshotError::Truncated(
                "unexpected EOF inside snapshot frame payload".to_string(),
            )),
            DecodePhase::Trailer => Err(SnapshotError::Truncated(
                "unexpected EOF inside snapshot trailer".to_string(),
            )),
            DecodePhase::Failed => Err(Self::failed_state_error()),
        }
    }

    fn validate_frame_prefix(&self, tag: u8) -> Result<(), SnapshotError> {
        match (self.seen_header, self.current_db.is_some(), tag) {
            (false, _, TAG_SNAPSHOT_HEADER) => Ok(()),
            (true, _, TAG_DATABASE_START) => {
                if self.total_databases >= MAX_SNAPSHOT_DATABASES as u64 {
                    return Err(SnapshotError::InvalidFormat(format!(
                        "snapshot exceeds maximum of {MAX_SNAPSHOT_DATABASES} databases"
                    )));
                }
                Ok(())
            }
            (true, true, TAG_DOCUMENT_RECORD) => Ok(()),
            (true, _, TAG_END_OF_SNAPSHOT) => Ok(()),
            (_, _, TAG_SNAPSHOT_HEADER) => Err(SnapshotError::InvalidFormat(
                "unexpected SnapshotHeader frame".to_string(),
            )),
            (_, _, TAG_DATABASE_START) => Err(SnapshotError::InvalidFormat(
                "unexpected DatabaseStart frame".to_string(),
            )),
            (true, false, TAG_DOCUMENT_RECORD) => Err(SnapshotError::InvalidFormat(
                "DocumentRecord appeared before any DatabaseStart frame".to_string(),
            )),
            (_, _, TAG_DOCUMENT_RECORD) => Err(SnapshotError::InvalidFormat(
                "unexpected DocumentRecord frame".to_string(),
            )),
            (_, _, TAG_END_OF_SNAPSHOT) => Err(SnapshotError::InvalidFormat(
                "unexpected EndOfSnapshot frame".to_string(),
            )),
            (_, _, other) => Err(SnapshotError::InvalidFormat(format!(
                "unknown snapshot frame tag: {other:#04x}"
            ))),
        }
    }

    fn handle_frame(&mut self, tag: u8, payload: &[u8]) -> Result<bool, SnapshotError> {
        match tag {
            TAG_SNAPSHOT_HEADER => {
                let _header = SnapshotHeader::decode(payload).map_err(|err| {
                    SnapshotError::InvalidFormat(format!("invalid SnapshotHeader payload: {err}"))
                })?;
                self.seen_header = true;
                Ok(false)
            }
            TAG_DATABASE_START => {
                if let Some(prev) = self.current_db.take() {
                    self.databases.push(prev);
                }
                let frame = DatabaseStart::decode(payload).map_err(|err| {
                    SnapshotError::InvalidFormat(format!("invalid DatabaseStart payload: {err}"))
                })?;
                let root = RootRef::try_new(&frame.project_id, &frame.database_id)?;
                if !self.seen_databases.insert(root.clone()) {
                    return Err(SnapshotError::InvalidFormat(format!(
                        "duplicate database in snapshot: {root}"
                    )));
                }
                let edition = wire::decode_database_edition(frame.database_edition)?;
                let concurrency_mode = wire::decode_concurrency_mode(frame.concurrency_mode)?;
                self.current_db = Some(PreparedDatabase {
                    name: root,
                    settings: DatabaseSettings {
                        edition,
                        concurrency_mode,
                    },
                    collections: HashMap::new(),
                    document_count: 0,
                });
                self.total_databases += 1;
                Ok(false)
            }
            TAG_DOCUMENT_RECORD => {
                let db = self
                    .current_db
                    .as_mut()
                    .expect("validate_frame_prefix ensures active database before DocumentRecord");
                let frame = DocumentRecord::decode(payload).map_err(|err| {
                    SnapshotError::InvalidFormat(format!("invalid DocumentRecord payload: {err}"))
                })?;
                if frame.document_path.is_empty()
                    || frame.document_path.starts_with('/')
                    || frame.document_path.ends_with('/')
                {
                    return Err(GenericDatabaseError::InvalidReference(
                        "document",
                        frame.document_path,
                    )
                    .into());
                }
                let full_path = format!("{}/{}", db.name, frame.document_path);
                let doc_ref: DocumentRef = full_path.parse()?;
                validate_writable_fields(&frame.fields)?;

                let col_docs = db
                    .collections
                    .entry(doc_ref.collection_ref.collection_id.clone())
                    .or_default();
                if col_docs.contains_key(&doc_ref.document_id) {
                    return Err(SnapshotError::InvalidFormat(format!(
                        "duplicate document in snapshot: {doc_ref}"
                    )));
                }
                col_docs.insert(doc_ref.document_id.clone(), (doc_ref, frame.fields));
                db.document_count += 1;
                self.total_documents += 1;
                Ok(false)
            }
            TAG_END_OF_SNAPSHOT => {
                if let Some(prev) = self.current_db.take() {
                    self.databases.push(prev);
                }
                let frame = EndOfSnapshot::decode(payload).map_err(|err| {
                    SnapshotError::InvalidFormat(format!("invalid EndOfSnapshot payload: {err}"))
                })?;
                if frame.total_databases != self.total_databases
                    || frame.total_documents != self.total_documents
                {
                    return Err(SnapshotError::InvalidFormat(format!(
                        "EndOfSnapshot totals mismatch: declared {} database(s) and {} \
                         document(s), decoded {} database(s) and {} document(s)",
                        frame.total_databases,
                        frame.total_documents,
                        self.total_databases,
                        self.total_documents
                    )));
                }
                Ok(true)
            }
            other => {
                unreachable!(
                    "validate_frame_prefix rejects unknown snapshot frame tag: {other:#04x}"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) use self::tests::encode_single_db_bytes;
