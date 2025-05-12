use std::ops::{Deref, DerefMut};

use crate::{owned_read_guard::OwnedReadGuard, queue::OwnedQueueEntryHandle};

/// RAII guard that provides exclusive write access to the data protected by a `FifoRwLock`.
///
/// When this guard is dropped, the write lock is released, and its entry is removed from the lock's
/// internal queue. This guard can be downgraded to an [`OwnedReadGuard`] using the
/// [`downgrade`](Self::downgrade) method.
#[derive(Debug)]
pub struct OwnedWriteGuard<T> {
    /// The handle to this guard's entry in the FIFO queue. When dropped, it signals the lock
    /// to release this write access.
    pub(crate) queue_handle: OwnedQueueEntryHandle<T>,
    /// A raw pointer providing mutable access to the locked data.
    ///
    /// SAFETY: This pointer is valid because the `FifoRwLock` ensures the data outlives the
    /// guard, and the lock logic guarantees exclusive access.
    pub(crate) data: *mut T,
}

impl<T> OwnedWriteGuard<T> {
    /// Downgrades this `OwnedWriteGuard` to an `OwnedReadGuard`.
    ///
    /// This operation is **atomic and immediate**. It consumes the `OwnedWriteGuard`, transitions
    /// the lock state to allow readers (none were allowed due to this writer), and returns an
    /// `OwnedReadGuard`.
    ///
    /// Other waiting readers may be granted access concurrently after a downgrade.
    ///
    /// # Returns
    ///
    /// An `OwnedReadGuard` providing shared read access.
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
    /// let mut write_guard = lock.write_owned().await;
    /// *write_guard += 10;
    ///
    /// // Note that `downgrade` is an atomic operation and is always granted.
    /// let read_guard = write_guard.downgrade();
    /// assert_eq!(*read_guard, 20);
    /// # }
    pub fn downgrade(self) -> OwnedReadGuard<T> {
        let Self { data, queue_handle } = self;

        queue_handle
            .rw_lock
            .state
            .lock()
            .unwrap()
            .downgrade_request(queue_handle.id);

        OwnedReadGuard { queue_handle, data }
    }
}

impl<T> Deref for OwnedWriteGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: An `OwnedWriteGuard` implies exclusive access is permitted.
        unsafe { &*self.data }
    }
}

impl<T> DerefMut for OwnedWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: An `OwnedWriteGuard` implies exclusive mutable access is permitted.
        unsafe { &mut *self.data }
    }
}
