# database

## Overview
Core database storage, addressing, query, and transaction primitives for `FirestoreDatabase`, including MVCC document versions with FIFO read/write locking, collection containers, resource references, field paths, projections, structured queries, pipelines, and pessimistic/optimistic concurrency transactions.

## Interface
- `[collection.rs:Collection]`: `pub(crate) struct Collection` (`new`, `get_doc`, `docs`, `has_doc`)
- `[document.rs:DocumentVersion]`: `pub enum DocumentVersion { Deleted(Arc<DeletedDocumentVersion>), Stored(Arc<StoredDocumentVersion>) }` (`name`, `create_time`, `update_time`, `stored_document`, `to_document`)
- `[document.rs:StoredDocumentVersion]`: `pub struct StoredDocumentVersion { pub name: DocumentRef, pub create_time: Timestamp, pub update_time: Timestamp, pub fields: HashMap<String, Value> }` (`to_document`)
- `[document.rs:DeletedDocumentVersion]`: `pub struct DeletedDocumentVersion { pub name: DocumentRef, pub delete_time: Timestamp }`
- `[document.rs:DocumentMeta]`, `[document.rs:DocumentContents]`, `[document.rs:DocumentPrecondition]`, `[document.rs:OwnedDocumentContentsReadGuard]`, `[document.rs:OwnedDocumentContentsWriteGuard]`: Crate-internal MVCC document storage, precondition validation, and FIFO read/write lock guards (`read`, `write`, `upgrade`, `current`, `get_at`, `check_precondition`, `maybe_add_version`, `delete`, `maybe_update`, `create`, `transform`).
- `[event.rs:DatabaseEvent]`: `pub struct DatabaseEvent { pub database: Weak<FirestoreDatabase>, pub update_time: Timestamp, pub updates: HashMap<DocumentRef, DocumentVersion> }`
- `[event.rs:ProjectEvent]`: `pub enum ProjectEvent { DatabaseCreated { database: RootRef }, DatabaseCleared { database: RootRef }, DatabaseConfigChanged { database: RootRef }, DocumentsChanged { database: RootRef, paths: Vec<String>, update_time: Timestamp } }`
- `[field_path.rs:DOC_NAME]`: `pub const DOC_NAME: &str = "__name__"`
- `[field_path.rs:FieldReference]`: `pub enum FieldReference { DocumentName, FieldPath(FieldPath) }` (`get_value`, `is_document_name`)
- `[field_path.rs:FieldPath]`: `pub struct FieldPath(Vec<String>)` (`get_value`, `set_value`, `delete_value`, `transform_value`, `try_transform_value`)
- `[projection.rs:Projection]` & `[projection.rs:Project]`: `pub struct Projection` and `pub trait Project { fn project(&self, version: &StoredDocumentVersion) -> Document; }`
- `[query.rs:QueryBuilder]` & `[query.rs:Query]`: `pub struct QueryBuilder` and `pub struct Query` (`from_structured`, `reset_on_update`, `reset_consistency_to_default`, `once`, `missing_docs`, `includes_document`, `project`)
- `[query.rs:Order]` & `[query.rs:Direction]`: `pub struct Order` and `pub enum Direction { Ascending, Descending }`
- `[read_consistency.rs:ReadConsistency]`: `pub enum ReadConsistency { Default, ReadTime(Timestamp), Transaction(TransactionId) }` (`is_transaction`)
- `[reference.rs:Ref]`, `[reference.rs:RootRef]`, `[reference.rs:CollectionRef]`, `[reference.rs:DocumentRef]`: Resource reference types and hierarchy helpers (`as_root`, `as_collection`, `as_document`, `root`, `is_parent_of`, `strip_prefix`, `strip_root_prefix`, `strip_collection_prefix`, `strip_document_prefix`, `parent`)
- `[timeouts.rs:Timeouts]`: `pub(crate) struct Timeouts { pub write: Duration, pub read: Duration }` (`CLOUD`, `FAST`)
- `[transaction.rs:TransactionId]`: `pub struct TransactionId(usize)`
- Subdirectories:
  - [pipeline](./pipeline/README.md): Enterprise Edition `StructuredPipeline` execution engine and expression/function evaluators.
  - [query](./query/README.md): Structured query filter AST and predicate evaluation (`Filter`, `CompositeFilter`, `FieldFilter`, `UnaryFilter`).

## Invariants
- `[document.rs:OwnedDocumentContentsWriteGuard::maybe_add_version]`: Every new version's `update_time` must be `>= self.acquire_time` (otherwise returns `GenericDatabaseError::Internal`) and monotonically non-decreasing (`assert!(last.update_time() <= version.update_time())`); updates with identical `update_time` overwrite the latest version in place, and `maybe_update` skips creating a version if `fields` are unchanged.
- `[document.rs:OwnedDocumentContentsReadGuard::upgrade]`: Upgrading an owned read lock to a write lock verifies that `last_updated()` did not change while waiting; otherwise returns `GenericDatabaseError::Aborted("contention")`.
- `[reference.rs:Ref::from_str]`: Resource paths must match `projects/{project_id}/databases/{database_id}[/documents/...]` where collection/document IDs are non-empty, not `.` or `..`, not matching `__.*__` (length >= 5), and at most 1,500 bytes.
- `[field_path.rs:FieldPath::from_str]`: Field paths must be non-empty, and backtick-quoted segments must wrap entire segments with a matching closing backtick and valid `\` escape sequences.
- `[query.rs:Query::validate]`: `Query.parent` must be `Ref::Root` or `Ref::Document` (never `Ref::Collection`), and `from` must be non-empty; in Standard Edition (`!enterprise_edition`), inequality filter fields populate `order_by` if empty, at most one `ARRAY_CONTAINS`/`ARRAY_CONTAINS_ANY` filter is allowed, and `__name__` is appended to `order_by` when absent (or when `all_descendants` is enabled).
- `[transaction.rs:TransactionId::check]`: Retried transaction IDs must be strictly less than `NEXT_TXN_ID` and not currently registered in `RunningTransactions`.

## Side Effects
- `[document.rs:lock_timeout]`: Acquires asynchronous FIFO read/write guards (`fifo_rwlock::FifoRwLock`) on `DocumentContents` bounded by `Timeouts` (`CLOUD`: 15s write / 16s read; `FAST`: 2s write / 5s read), logging a `tracing::warn!` after 1 second and aborting with `GenericDatabaseError::Aborted` on timeout.
- `[collection.rs:Collection::get_doc]`: Lazily inserts `Arc<DocumentMeta>` entries into the collection's `RwLock<HashMap<DefaultAtom, Arc<DocumentMeta>>>`.
- `[transaction.rs:RunningTransactions]`, `[transaction.rs:PessimisticState]`, `[transaction.rs:OptimisticState]`: Generates monotonically increasing `TransactionId` values via `NEXT_TXN_ID` (`AtomicUsize`), tracks active transactions in `RwLock<HashMap<TransactionId, Arc<Transaction>>>`, retains `OwnedDocumentContentsReadGuard` locks in pessimistic mode, and records `observed_docs` and `executed_queries` in `Mutex` state in optimistic mode.

## Verification
- Run: `cargo test -p emulator-database`
