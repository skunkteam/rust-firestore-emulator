use std::ops::Deref;

use crate::queue::QueueEntryHandle;

/// RAII guard that provides shared read access to the data protected by a `FifoRwLock`.
///
/// When this guard is dropped, the read lock is released, and its entry is removed from the lock's
/// internal queue.
#[derive(Debug)]
pub struct ReadGuard<'a, T> {
    /// The handle to this guard's entry in the FIFO queue. When dropped, it signals the lock
    /// to release this read access.
    pub(crate) _queue_handle: QueueEntryHandle<'a, T>,
    /// A raw pointer providing immutable access to the locked data.
    ///
    /// SAFETY: This pointer is valid because the `FifoRwLock` ensures the data (held in
    /// `UnsafeCell`) outlives the guard, and the lock logic prevents mutable access while this
    /// read guard exists.
    pub(crate) data: *const T,
}

impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // SAFETY: The existence of a `ReadGuard` implies shared read access is permitted
        // by the lock logic. The `data` pointer, obtained from `UnsafeCell::get()`,
        // is valid as `FifoRwLock` ensures the data outlives any guards.
        unsafe { &*self.data }
    }
}
