//! A strictly FIFO asynchronous reader-writer lock with upgrade support.
//!
//! This module provides `FifoRwLock`, a synchronization primitive that allows multiple readers or a
//! single writer to access shared data.
//!
//! ## FIFO Ordering and Comparison with `tokio::sync::RwLock`
//!
//! Unlike `std::sync::RwLock` (whose fairness is OS-dependent) and `tokio::sync::RwLock` (which is
//! fair and write-preferring), this `FifoRwLock` enforces a strict First-In, First-Out (FIFO)
//! ordering for *all* requests (read, write, and attempts to upgrade a read lock). All requests
//! enter a single queue, and their position in this queue determines their priority.
//!
//! If the `upgrade` feature is never used, the behavior related to granting read and write locks
//! should be similar to a strictly fair `RwLock`. However, the internal mechanism of maintaining
//! granted locks in the queue to handle upgrade priorities is a key difference.
//!
//! ## Upgrade Mechanism
//!
//! An `OwnedReadGuard` can be "upgraded" to an `OwnedWriteGuard` using the `upgrade()` method.
//! Crucially, this upgrade attempt **retains the original request's position in the FIFO queue**.
//!
//! For example:
//! 1. Task R1 requests and gets an `OwnedReadGuard`.
//! 2. Task W2 requests a `OwnedWriteGuard` (is queued behind R1).
//! 3. Task R1 decides to `upgrade` its `OwnedReadGuard`.
//!
//! In this scenario, R1's upgraded request (now effectively a write request) will be prioritized
//! over W2's request because R1's original read request was earlier in the queue. The upgrade will
//! complete once R1's request is at the front of the queue and any other concurrent readers (that
//! might have acquired their locks alongside R1) have released their locks.
//!
//! **Important Note on Upgrades:** The `upgrade()` operation is **not atomic** in the sense that
//! the data might be modified by other writers (who had higher priority in the queue) between the
//! time `upgrade()` is called and the time the `OwnedWriteGuard` is actually granted. Always
//! re-check state or perform idempotent operations after an upgrade.
//!
//! ## Downgrade Mechanism
//!
//! An `OwnedWriteGuard` can be "downgraded" to a `ReadGuard` using the `downgrade()` method. This
//! operation is **atomic and immediate**. The `OwnedWriteGuard` is consumed, the lock state is
//! changed to allow readers, and a `ReadGuard` is returned. Other waiting readers (if any) may also
//! be granted access concurrently.

use std::{
    cell::UnsafeCell,
    sync::{Arc, Mutex as SyncMutex},
};

use tokio::sync::oneshot;

mod owned_read_guard;
mod owned_write_guard;
mod queue;
mod read_guard;
pub use owned_read_guard::OwnedReadGuard;
pub use owned_write_guard::OwnedWriteGuard;
use queue::FifoRwLockQueue;
pub use read_guard::ReadGuard;

#[cfg(test)]
mod tests;

/// A strictly FIFO asynchronous reader-writer lock with upgrade support.
///
/// This lock guarantees that requests (read, write, or upgrade) are granted access to the
/// underlying data based on the order in which the first request was made. It allows multiple
/// concurrent readers or a single exclusive writer.
///
/// See the [module-level documentation](self) for a detailed explanation of its behavior,
/// especially regarding FIFO ordering and the upgrade mechanism.
#[derive(Debug)]
pub struct FifoRwLock<T> {
    /// The internal state of the lock, including the request queue and active lock counts. This is
    /// protected by a standard library `Mutex` to allow its use in `Drop` implementations and to
    /// ensure atomicity of state changes. This `Mutex` should never be held across an `.await`
    /// point directly to avoid blocking Tokio's runtime. Operations requiring asynchronous waiting
    /// release this mutex before awaiting and re-acquire it afterwards.
    state: SyncMutex<FifoRwLockQueue>,
    /// The actual data protected by the lock. `UnsafeCell` is used to allow obtaining
    /// mutable pointers (`*mut T`) from an immutable reference (`&self`) when a write lock
    /// is granted, while ensuring that Rust's borrowing rules are upheld dynamically by the lock
    /// logic.
    data:  UnsafeCell<T>,
}

impl<T> FifoRwLock<T> {
    /// Creates a new `FifoRwLock` protecting the given `data`.
    pub fn new(data: T) -> Self {
        FifoRwLock {
            state: SyncMutex::new(FifoRwLockQueue::new()),
            data:  UnsafeCell::new(data),
        }
    }

    /// Acquires a read lock asynchronously.
    ///
    /// This method adds a `ReadRequested` entry to the FIFO queue and waits until the lock can be
    /// granted according to the FIFO rules (no active writer, no preceding write requests).
    ///
    /// Returns a [`ReadGuard`] when successful. The future returned by this method is
    /// cancellation-safe: if it's dropped before the lock is acquired, the request is removed from
    /// the queue.
    pub async fn read(&self) -> ReadGuard<T> {
        // Create a oneshot channel. The sender (`tx`) is given to the queue entry, and the receiver
        // (`rx`) is awaited by this task.
        let (tx, rx) = oneshot::channel();

        let queue_handle = self.state.lock().unwrap().add_read_request(self, tx);

        // Asynchronously wait for the `listener` to send a signal, indicating the lock is granted.
        // This is cancellation-safe: if this future is dropped, `queue_handle` is dropped, which
        // calls `FifoRwLockQueue::release` to remove the entry from the queue.
        rx.await.expect(
            "FifoRwLock read listener was dropped prematurely, indicates a bug in lock logic or \
             task management.",
        );

        let data = self.data.get();
        ReadGuard {
            _queue_handle: queue_handle,
            data,
        }
    }

    /// Acquires a read lock asynchronously.
    ///
    /// This method adds a `ReadRequested` entry to the FIFO queue and waits until the lock can be
    /// granted according to the FIFO rules (no active writer, no preceding write requests).
    ///
    /// Returns an [`OwnedReadGuard`] when successful. The future returned by this method is
    /// cancellation-safe: if it's dropped before the lock is acquired, the request is removed from
    /// the queue.
    pub async fn read_owned(self: &Arc<Self>) -> OwnedReadGuard<T> {
        // Create a oneshot channel. The sender (`tx`) is given to the queue entry, and the receiver
        // (`rx`) is awaited by this task.
        let (tx, rx) = oneshot::channel();

        let queue_handle = self.state.lock().unwrap().add_owned_read_request(self, tx);

        // Asynchronously wait for the `listener` to send a signal, indicating the lock is granted.
        // This is cancellation-safe: if this future is dropped, `queue_handle` is dropped, which
        // calls `FifoRwLockQueue::release` to remove the entry from the queue.
        rx.await.expect(
            "FifoRwLock read listener was dropped prematurely, indicates a bug in lock logic or \
             task management.",
        );

        let data = self.data.get();
        OwnedReadGuard { queue_handle, data }
    }

    /// Acquires a write lock asynchronously.
    ///
    /// This method adds a `WriteRequested` entry to the FIFO queue and waits until the lock can be
    /// granted (request is at the front of the queue, no active readers, no active writer).
    ///
    /// Returns an [`OwnedWriteGuard`] when successful. The future returned by this method is
    /// cancellation-safe: if it's dropped before the lock is acquired, the request is removed from
    /// the queue.
    pub async fn write_owned(self: &Arc<Self>) -> OwnedWriteGuard<T> {
        // Create a oneshot channel. The sender (`tx`) is given to the queue entry, and the receiver
        // (`rx`) is awaited by this task.
        let (tx, rx) = oneshot::channel();

        let queue_handle = self.state.lock().unwrap().add_owned_write_request(self, tx);

        // Asynchronously wait for the `listener` to send a signal, indicating the lock is granted.
        // This is cancellation-safe: if this future is dropped, `queue_handle` is dropped, which
        // calls `FifoRwLockQueue::release` to remove the entry from the queue.
        rx.await.expect(
            "FifoRwLock write listener was dropped prematurely, indicates a bug in lock logic or \
             task management.",
        );

        let data = self.data.get();
        OwnedWriteGuard { queue_handle, data }
    }
}

// --- Unsafe Send/Sync Implementations ---
// Inspired by:
// https://github.com/tokio-rs/tokio/blob/4cbcb687f429f2a4cee948b0462e23f86ef95822/tokio/src/sync/rwlock.rs

// These are necessary because `FifoRwLock` contains `UnsafeCell<T>`, which is not `Sync`, and
// guards contain raw pointers, which are not `Send` or `Sync` by default. We assert that our lock
// logic correctly synchronizes access, making these operations safe.

// As long as T: Send + Sync, it's fine to send and share FifoRwLock<T> between threads.
// If T were not Send, sending and sharing a FifoRwLock<T> would be bad, since you can access T
// through FifoRwLock<T>.
unsafe impl<T> Send for FifoRwLock<T> where T: Send {}
unsafe impl<T> Sync for FifoRwLock<T> where T: Send + Sync {}
// NB: These impls need to be explicit since we're storing a raw pointer.
// Safety: Stores a raw pointer to `T`, so if `T` is `Sync`, the lock guard over
// `T` is `Send`.
unsafe impl<T> Send for ReadGuard<'_, T> where T: Sync {}
unsafe impl<T> Sync for ReadGuard<'_, T> where T: Send + Sync {}
// T is required to be `Send` because an OwnedReadGuard can be used to drop the value held in
// the Fifo, unlike ReadGuard.
unsafe impl<T> Send for OwnedReadGuard<T> where T: Send + Sync {}
unsafe impl<T> Sync for OwnedReadGuard<T> where T: Send + Sync {}
unsafe impl<T> Sync for OwnedWriteGuard<T> where T: Send + Sync {}
// Safety: Stores a raw pointer to `T`, so if `T` is `Sync`, the lock guard over
// `T` is `Send` - but since this is also provides mutable access, we need to
// make sure that `T` is `Send` since its value can be sent across thread
// boundaries.
unsafe impl<T> Send for OwnedWriteGuard<T> where T: Send + Sync {}

#[test]
fn bounds() {
    use tokio_test::{assert_ready, task::spawn};

    fn check_send<T: Send>() {}
    fn check_sync<T: Sync>() {}
    fn check_unpin<T: Unpin>() {}
    // This has to take a value, since the async fn's return type is unnameable.
    fn check_send_sync_val<T: Send + Sync>(_t: T) {}

    check_send::<FifoRwLock<u32>>();
    check_sync::<FifoRwLock<u32>>();
    check_unpin::<FifoRwLock<u32>>();

    check_send::<ReadGuard<'_, u32>>();
    check_sync::<ReadGuard<'_, u32>>();
    check_unpin::<ReadGuard<'_, u32>>();

    check_send::<OwnedReadGuard<u32>>();
    check_sync::<OwnedReadGuard<u32>>();
    check_unpin::<OwnedReadGuard<u32>>();

    check_send::<OwnedWriteGuard<u32>>();
    check_sync::<OwnedWriteGuard<u32>>();
    check_unpin::<OwnedWriteGuard<u32>>();

    let rwlock = Arc::new(FifoRwLock::new(0));
    check_send_sync_val(rwlock.read_owned());
    check_send_sync_val(rwlock.write_owned());
    let read = assert_ready!(spawn(rwlock.read_owned()).poll());
    check_send_sync_val(read.upgrade());
}
