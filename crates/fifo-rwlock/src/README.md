# fifo-rwlock Source (`src`)

## Overview
Implements `FifoRwLock<T>`, a strictly First-In, First-Out (FIFO) asynchronous reader-writer lock for Tokio with support for in-place read-to-write lock upgrades and atomic write-to-read lock downgrades. All read, write, and upgrade requests share a single FIFO queue where granted entries remain in place until their RAII guard is dropped.

## Interface
- `[lib.rs:FifoRwLock]` - `pub struct FifoRwLock<T>`: Asynchronous FIFO reader-writer lock wrapping `std::sync::Mutex<FifoRwLockQueue>` and `UnsafeCell<T>`. Implements `Send` (where `T: Send`) and `Sync` (where `T: Send + Sync`).
- `[lib.rs:FifoRwLock::new]` - `pub fn new(data: T) -> Self`: Constructs a new `FifoRwLock<T>` protecting `data`.
- `[lib.rs:FifoRwLock::read]` - `pub async fn read(&self) -> ReadGuard<'_, T>`: Asynchronously acquires a shared read lock in FIFO order.
- `[lib.rs:FifoRwLock::read_owned]` - `pub async fn read_owned(self: &Arc<Self>) -> OwnedReadGuard<T>`: Asynchronously acquires an `Arc`-owned shared read lock in FIFO order.
- `[lib.rs:FifoRwLock::write_owned]` - `pub async fn write_owned(self: &Arc<Self>) -> OwnedWriteGuard<T>`: Asynchronously acquires an `Arc`-owned exclusive write lock in FIFO order.
- `[read_guard.rs:ReadGuard]` - `pub struct ReadGuard<'a, T>`: Borrowed RAII guard providing shared immutable access via `Deref<Target = T>`. Implements `Send` (where `T: Sync`) and `Sync` (where `T: Send + Sync`).
- `[owned_read_guard.rs:OwnedReadGuard]` - `pub struct OwnedReadGuard<T>`: Owned RAII guard providing shared immutable access via `Deref<Target = T>`. Implements `Send` and `Sync` (where `T: Send + Sync`).
- `[owned_read_guard.rs:OwnedReadGuard::upgrade]` - `pub async fn upgrade(self) -> OwnedWriteGuard<T>`: Consumes the read guard and requests an upgrade to an `OwnedWriteGuard<T>` at the read guard's original FIFO queue position.
- `[owned_write_guard.rs:OwnedWriteGuard]` - `pub struct OwnedWriteGuard<T>`: Owned RAII guard providing exclusive mutable access via `Deref<Target = T>` and `DerefMut<Target = T>`. Implements `Send` and `Sync` (where `T: Send + Sync`).
- `[owned_write_guard.rs:OwnedWriteGuard::downgrade]` - `pub fn downgrade(self) -> OwnedReadGuard<T>`: Atomically and immediately downgrades the exclusive write guard into an `OwnedReadGuard<T>`.

## Invariants
- `[queue.rs:FifoRwLockQueue]` - All requests (`read`, `read_owned`, `write_owned`, and `upgrade`) are tracked in a single `VecDeque<QueueEntry>` with monotonically increasing `next_id` identifiers; granted entries remain in the queue until their guard is dropped.
- `[queue.rs:FifoRwLockQueue::upgrade_request]` - Upgrading an `OwnedReadGuard` transitions its existing `QueueEntryState::ReadGranted` entry to `QueueEntryState::WriteRequested` in-place (preserving its original FIFO priority) and decrements `active_readers`. Upgrades are **not atomic**: writers queued earlier than the original read request may mutate `T` before the upgrade resolves.
- `[queue.rs:FifoRwLockQueue::downgrade_request]` - Downgrading an `OwnedWriteGuard` is atomic and immediate, transitioning `QueueEntryState::WriteGranted` to `QueueEntryState::ReadGranted` (`writer_active = false`, `active_readers += 1`) and waking eligible queued readers.
- `[queue.rs:FifoRwLockQueue::check_and_notify]` - A `WriteRequested` entry is granted only when `writer_active` is `false`, `active_readers == 0`, and it is the first ungranted entry in the queue. A `ReadRequested` entry is granted only when `writer_active` is `false` and no preceding `WriteRequested` entry is waiting.
- `[lib.rs:FifoRwLock]` - The internal `std::sync::Mutex<FifoRwLockQueue>` (`state`) is never held across an `.await` boundary.
- `[queue.rs:QueueEntryHandle::drop]` & `[queue.rs:OwnedQueueEntryHandle::drop]` - All lock acquisition and upgrade futures are cancellation-safe: dropping a pending future or active guard invokes `FifoRwLockQueue::release`, removing the queue entry, updating `active_readers` / `writer_active`, and notifying subsequent waiting tasks.

## Side Effects
- `[lib.rs:FifoRwLock]` - Synchronizes shared in-memory state (`UnsafeCell<T>`) across asynchronous tasks using `std::sync::Mutex<FifoRwLockQueue>` and `tokio::sync::oneshot` channels.
- `[queue.rs:FifoRwLockQueue::check_and_notify]` & `[queue.rs:FifoRwLockQueue::release]` - Mutates the internal lock queue on request, upgrade, downgrade, or drop and signals waiting tasks via `oneshot::Sender::send(())`.

## Verification
- Run: `cargo test -p fifo-rwlock`
