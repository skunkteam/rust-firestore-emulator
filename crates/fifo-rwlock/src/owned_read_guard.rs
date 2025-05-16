use std::ops::Deref;

use tokio::sync::oneshot;

use crate::{owned_write_guard::OwnedWriteGuard, queue::OwnedQueueEntryHandle};

/// RAII guard that provides shared read access to the data protected by a `FifoRwLock`.
///
/// When this guard is dropped, the read lock is released, and its entry is removed from the lock's
/// internal queue. This guard can be upgraded to an [`OwnedWriteGuard`] using the
/// [`upgrade`](Self::upgrade) method.
#[derive(Debug)]
pub struct OwnedReadGuard<T> {
    /// The handle to this guard's entry in the FIFO queue. When dropped, it signals the lock
    /// to release this read access.
    pub(crate) queue_handle: OwnedQueueEntryHandle<T>,
    /// A raw pointer providing immutable access to the locked data.
    ///
    /// SAFETY: This pointer is valid because the `FifoRwLock` ensures the data (held in
    /// `UnsafeCell`) outlives the guard, and the lock logic prevents mutable access while this
    /// read guard exists.
    pub(crate) data: *const T,
}

impl<T> OwnedReadGuard<T> {
    /// Attempts to upgrade this `OwnedReadGuard` to a `OwnedWriteGuard`.
    ///
    /// This method consumes the `OwnedReadGuard`. The upgrade request is placed in the FIFO queue
    /// at the **original position** of the `OwnedReadGuard`. The returned future completes when
    /// the write lock is granted.
    ///
    /// **Behavior and Priority:**
    /// - The upgraded request honors its original queue position. It might gain write access before
    ///   an `OwnedWriteGuard` that was requested *after* this `OwnedReadGuard` but *before* the
    ///   call to `upgrade()`.
    /// - The upgrade will only complete when this request is at the front of the queue, no other
    ///   writer is active, and all *other* concurrent readers (if any) have released their locks.
    ///
    /// **Atomicity:** This operation is **not atomic**. Other writers with higher priority (i.e.,
    /// earlier in the queue) may modify the data between the call to `upgrade()` and the
    /// `OwnedWriteGuard` being granted. Callers should be aware of this and re-verify state or use
    /// idempotent operations if necessary.
    ///
    /// # Returns
    ///
    /// A future that resolves to an `OwnedWriteGuard`. This future is cancellation-safe.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use fifo_rwlock::FifoRwLock;
    /// # #[tokio::main]
    /// # async fn main() {
    /// let lock = Arc::new(FifoRwLock::new(10));
    ///
    /// let read_guard = lock.read_owned().await;
    /// assert_eq!(*read_guard, 10);
    ///
    /// let mut write_guard = read_guard.upgrade().await;
    /// // We should always check here that our value is what we expect, because it may have been
    /// // changed since requesting the upgrade unless we perform an idempotent operation.
    /// *write_guard += 10;
    ///
    /// // Note that `downgrade` **is** an atomic operation.
    /// let read_guard = write_guard.downgrade();
    /// assert_eq!(*read_guard, 20);
    /// # }
    /// ```
    pub async fn upgrade(self) -> OwnedWriteGuard<T> {
        let (tx, rx) = oneshot::channel();
        let queue_handle = self.queue_handle;

        queue_handle
            .rw_lock
            .state
            .lock()
            .unwrap()
            .upgrade_request(queue_handle.id, tx);

        rx.await
            .expect("FifoRwLock upgrade listener was dropped, indicates a bug.");

        OwnedWriteGuard {
            data: queue_handle.rw_lock.data.get(),
            queue_handle,
        }
    }
}

impl<T> Deref for OwnedReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: The existence of an `OwnedReadGuard` implies shared read access is permitted
        // by the lock logic. The `data` pointer, obtained from `UnsafeCell::get()`,
        // is valid as `FifoRwLock` ensures the data outlives any guards.
        unsafe { &*self.data }
    }
}
