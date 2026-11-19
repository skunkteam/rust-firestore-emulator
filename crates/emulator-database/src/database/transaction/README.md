# transaction

## Overview
Transaction registry and concurrency-mode state machines for `FirestoreDatabase`, managing 16-byte rotated transaction tokens, a 5-state `TxnSlot` lifecycle with isolated `4,096`-entry retry-tombstone and `Closed` rings, RAII commit slot guards, Cloud-compliant `Rollback`/`ReadOnly` validation semantics, and per-mode document/query isolation (`PessimisticState`, `OptimisticState`, `ReadOnlyTransaction`).

## Interface
- `[registry.rs:TransactionId]`: `pub struct TransactionId` — 16-byte little-endian `(lineage_id: u64, attempt_gen: u64)` wire token (`Display` `{lineage_id}:{attempt_gen}`, `TryFrom<Vec<u8>>`, `Into<Vec<u8>>`, `check`).
- `[registry.rs:ClosedOutcome]`: `pub(crate) enum ClosedOutcome { Committed, AbortedOrRolledBack }` — terminal outcome retained on `Closed` slots for Cloud-compliant rollback and retry behavior.
- `[registry.rs:CommitTake]`: `pub(crate) enum CommitTake { ReadOnly(Arc<ReadOnlyTransaction>), ReadWrite((Arc<ReadWriteTransaction>, CommitSlotGuard)) }` — typed slot handoff returned by `RunningTransactions::take_for_commit`.
- `[registry.rs:CommitSlotGuard]`: `pub(crate) struct CommitSlotGuard` (`finish_committed`, `abort_commit`, `Drop`) — RAII guard owning the `Committing` state of a `ReadWrite` transaction across asynchronous commit execution.
- `[registry.rs:RunningTransactions]`: Synchronous `std::sync::Mutex`-backed transaction registry (`new`, `clear_all`, `start_read_write`, `start_read_write_with_id`, `start_read_only`, `get_active`, `take_for_commit`, `rollback`) paired with `[registry.rs:tests]` unit tests.
- `[../transaction.rs:Transaction]`: `pub(crate) enum Transaction { ReadWrite(Arc<ReadWriteTransaction>), ReadOnly(Arc<ReadOnlyTransaction>) }` (`read_doc`, `as_read_write`, `read_time`) re-exporting `TransactionId`, `CommitTake`, and `RunningTransactions`.
- `[../transaction.rs:ReadWriteTransaction]`, `[../transaction.rs:PessimisticState]`, `[../transaction.rs:OptimisticState]`, `[../transaction.rs:ReadOnlyTransaction]`: Concurrency execution state holding `Arc<LockSession>` (`PessimisticState`, `OptimisticState`) or pinned `Timestamp` MVCC read-only snapshots (`ReadOnlyTransaction`), plus queue-free optimistic write-skew validation (`check_concurrency_violations`, `record_query_observation`, `remove_query_observation`).

## Invariants
- **5-State `TxnSlot` Machine & Isolated `4,096`-Entry Rings (`[registry.rs:TxnSlot]`, `[registry.rs:MAX_RETRY_TOMBSTONES]`, `[registry.rs:MAX_CLOSED_TXNS]`)**: Each `lineage_id` maps to `{ current_attempt, slot }` across `Active`, `ReadWrite`-only `Committing`, `Wounded { root_seq, reason }`, `AbortedLineage { root_seq }`, and `Closed { outcome, read_only }`. Two isolated generation-tagged `VecDeque<(u64, u64)>` rings (each capacity `4,096`) bound `Wounded`/`AbortedLineage` retry tombstones separately from `Closed` slots so routine commits/rollbacks never evict un-retried wounded lineages.
- **Cloud-Compliant `Rollback` & `ReadOnly` Lifecycle (`[registry.rs:RunningTransactions::rollback]`, `[registry.rs:RunningTransactions::take_for_commit]`, `[registry.rs:RunningTransactions::start_read_write_with_id]`)**:
  - Stale generations (`id.attempt_gen < current_attempt`) return idempotent `Ok(())` on `rollback` even when `current_attempt` is already `Active` or `Committed`.
  - Current-generation `rollback` closes `Active` slots to `Closed { AbortedOrRolledBack }`, leaves in-flight `Committing` commits untouched and returns `Ok(())`, returns idempotent `Ok(())` on `Wounded`/`AbortedLineage` **without stripping `root_seq`**, returns idempotent `Ok(())` on `Closed { AbortedOrRolledBack }`, and returns `Aborted` repeatedly on `Closed { Committed }` (including empty RW/RO commits).
  - `take_for_commit(id, has_writes)` transitions `Active(ReadOnly)` with `has_writes == true` directly to `Closed { AbortedOrRolledBack, read_only: true }` (`InvalidArgument`), and with `has_writes == false` directly to `Closed { Committed, read_only: true }`.
  - `start_read_write_with_id` rejects `ReadOnly` slots (`Active` or `Closed { read_only: true }`) with `InvalidArgument`, rejects `Active(ReadWrite)` / `Committing` with `FailedPrecondition`, rejects future generations with `InvalidArgument`, inherits `root_seq` (`Assigned(seq)`) on `Wounded` / `AbortedLineage`, and rotates `attempt_gen` on valid `Closed { read_only: false }` and `Entry::Vacant` retries with fresh `LockPriority::Lazy`.
- **Fail-Closed Database Clear & Cancellation Safety (`[registry.rs:CommitSlotGuard]`, `[registry.rs:is_cleared]`)**: `db.is_cleared()` fails closed with `AbortReason::DatabaseCleared` across all entry points and deletes slots immediately (also on `DatabaseDropped`) without retaining `Closed` or retry tombstones; `CommitSlotGuard::drop` closes the `LockSession` and cleans up abandoned `Committing` slots.
- **Queue-Free `OptimisticState` & `ReadOnlyTransaction` Reads (`[../transaction.rs:OptimisticState::read_doc]`, `[../transaction.rs:OptimisticState::check_concurrency_violations]`, `[../transaction.rs:ReadOnlyTransaction::read_doc]`)**: Optimistic reads and MVCC snapshots read via `DocumentMeta::read_mvcc_at` without entering `LockTable` queues; optimistic commit validation checks held target `WriteLease`s via `read_with_write_lease` and unheld `observed_docs` / `executed_queries` queue-free before mapping skew failures to `AbortReason::OptimisticWriteSkew`.

## Side Effects
- **Process-Global Lineage Counter (`[registry.rs:NEXT_LINEAGE_ID]`)**: Allocates monotonically increasing non-zero `lineage_id` values (`AtomicU64`) across transactions.
- **Lock Session Lifecycle**: Spawns, promotes (`Active` -> `Wounded` / `AbortedLineage` / `Closed`), and closes `Arc<LockSession>` handles stored inside `ReadWriteTransaction` variants without holding `Mutex<RunningTxnMap>` across `.await`.

## Verification
- Run unit tests: `cargo nextest run -p emulator-database database::transaction`
- Run wire lifecycle conformance & Rust integration suites: `just test` and `just sdk-test`
