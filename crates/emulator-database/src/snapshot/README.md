# snapshot

## Overview
Pure in-memory, I/O-free `.fsemu` v1 binary snapshot subsystem for `emulator-database`. Defines the versioned length-delimited Proto3 container format (`wire.rs`), progressive chunked `SnapshotEncoder` / `SnapshotDecoder` state machines (`codec.rs`), domain summary/error types (`SnapshotError`, `ImportedDatabase`, `ImportSummary`), and `ExportSnapshotPlan` for queue-free point-in-time MVCC streaming.

## Interface
- `[mod.rs]`:
  - Format constants: `SNAPSHOT_MAGIC = *b"FSEMUSNP"`, `EOF_MAGIC = *b"FSEMUEOF"`, `FORMAT_MAJOR: u16 = 1`, `MAX_FRAME_BYTES: usize = 64 * 1024 * 1024`, `EXPORT_CHUNK_BYTES: usize = 64 * 1024`.
  - `[mod.rs:SnapshotError]`: `pub enum SnapshotError { InvalidFormat(String), UnsupportedVersion { found_major: u16, supported_major: u16 }, Truncated(String), FrameTooLarge { len: usize, max: usize }, Validation(GenericDatabaseError), ImportInProgress, Stream(String) }`.
  - `[mod.rs:ImportedDatabase]` & `[mod.rs:ImportSummary]`: `pub struct ImportedDatabase { pub name: RootRef, pub settings: DatabaseSettings, pub document_count: u64, pub replaced: bool }` and `pub struct ImportSummary { pub databases: Vec<ImportedDatabase> }` (`total_databases`, `total_documents`).
  - `[mod.rs:ExportTarget]` & `[mod.rs:ExportSnapshotPlan]`: `pub(crate) struct ExportTarget { pub(crate) database: Arc<FirestoreDatabase>, pub(crate) settings: DatabaseSettings }` and `pub struct ExportSnapshotPlan` (`new`, `snapshot_time`, `single_target`, `into_stream() -> impl Stream<Item = io::Result<Bytes>> + Send + 'static`).
- `[wire.rs]`:
  - Frame tags: `TAG_SNAPSHOT_HEADER = 0x01`, `TAG_DATABASE_START = 0x02`, `TAG_DOCUMENT_RECORD = 0x03`, `TAG_END_OF_SNAPSHOT = 0x04`.
  - Enums & decoders: `DatabaseEditionWire` (`decode_database_edition`) and `ConcurrencyModeWire` (`decode_concurrency_mode`) with `From` / `TryFrom` conversions rejecting `0` (`Unspecified`) and unknown discriminants with `SnapshotError::InvalidFormat`.
  - `prost::Message` payloads: `SnapshotHeader { emulator_version, snapshot_time }`, `DatabaseStart { project_id, database_id, database_edition, concurrency_mode }`, `DocumentRecord { document_path, fields }`, `EndOfSnapshot { total_databases, total_documents }`.
- `[codec.rs]`:
  - `[codec.rs:PreparedCollectionDocs]` & `[codec.rs:PreparedDatabase]`: `pub(crate) type PreparedCollectionDocs = HashMap<DefaultAtom, (DocumentRef, HashMap<String, Value>)>` and `pub(crate) struct PreparedDatabase { pub(crate) name: RootRef, pub(crate) settings: DatabaseSettings, pub(crate) collections: HashMap<DefaultAtom, PreparedCollectionDocs>, pub(crate) document_count: u64 }`.
  - `[codec.rs:SnapshotEncoder]`: I/O-free encoder (`new(snapshot_time)`, `start_database`, `write_document`, `finish`, `take_chunk`, `has_full_chunk`, `#[cfg(test)] encode_to_writer`).
  - `[codec.rs:SnapshotDecoder]`: I/O-free progressive decoder (`new`, `feed`, `finish`).

## Invariants
- **Binary Container Layout & Grammar (`[mod.rs]`, `[wire.rs]`, `[codec.rs]`)**: Every snapshot starts with a 10-byte file header (`SNAPSHOT_MAGIC` + `FORMAT_MAJOR` LE), followed by framed Proto3 records `[tag: u8][payload_len: u32 LE] + payload` matching `SnapshotHeader -> (DatabaseStart -> DocumentRecord*)* -> EndOfSnapshot`, and ends with a 12-byte EOF trailer (`[crc32: u32 LE][EOF_MAGIC]`) where IEEE CRC-32 (`crc32fast::Hasher`) covers all bytes from offset `0` up to the start of the trailer (including `EndOfSnapshot`). Trailing bytes after the trailer fail with `InvalidFormat`; EOF in any incomplete phase fails with `Truncated`; `format_major != 1` fails with `UnsupportedVersion`.
- **Progressive Bounded Frame Allocation (`[codec.rs:SnapshotDecoder]`, `[codec.rs:SnapshotEncoder]`)**: Both encoder and decoder reject frames exceeding `MAX_FRAME_BYTES` (`64 MiB`) with `SnapshotError::FrameTooLarge` before buffering/encoding; `SnapshotDecoder` caps initial frame `BytesMut` allocation at `payload_len.min(INITIAL_FRAME_BUF_CAP)` (`64 KiB`) and reads chunks progressively, preventing upfront multi-megabyte allocation on truncated frames. `SnapshotEncoder::take_chunk` slices output to at most `EXPORT_CHUNK_BYTES` (`64 KiB`).
- **Unified Path, Value & Duplication Checks (`[codec.rs:SnapshotDecoder::handle_frame]`)**: `SnapshotHeader` provenance metadata is ignored on import. `DatabaseStart` validates `project_id` and `database_id` through `RootRef::try_new`, rejects unspecified/unknown wire enum values, and rejects duplicate databases (`InvalidFormat`). `DocumentRecord.document_path` omits `projects/{p}/databases/{d}/documents/`, rejects empty or leading/trailing `'/'` paths, validates full paths via `DocumentRef::from_str`, validates all field keys and values via `validate_writable_fields` (preserving all 11 storable `Value` types including `-0.0`, `NaN`, `±Infinity`, `__vector__` sentinel maps, and `0001..9999` `TimestampValue` bounds), and rejects duplicate documents within a database while accepting unsorted database or document order. `EndOfSnapshot` verifies exact `total_databases` and `total_documents` totals; zero-database snapshots (`0` databases, `0` documents) decode to an empty `Vec`.
- **Deterministic Queue-Free MVCC Export (`[mod.rs:ExportSnapshotPlan::into_stream]`)**: Streams across `RootRef`-sorted targets without checking `is_cleared()`, sorting collections and documents canonically via `CollectionRef::Ord` and `DocumentRef::Ord`, reading `meta.read_mvcc_at(snapshot_time)` without Wound-Wait locks (omitting tombstones, post-`snapshot_time` versions, and metadata timestamps while preserving zero-field documents, subcollections under missing parent documents, and empty databases), and forwarding encoder errors as `io::Error::other(err)` on a bounded `mpsc::channel(16)`.

## Side Effects
- `[mod.rs:ExportSnapshotPlan::into_stream]`: Spawns a Tokio task (`tokio::spawn`) that reads MVCC document snapshots on cloned `Arc<FirestoreDatabase>` handles and streams `<= 64 KiB` `Bytes` chunks over a bounded `mpsc::channel(16)`.

## Verification
- Run snapshot codec unit/proptest and project export/import tests: `cargo nextest run -p emulator-database`
