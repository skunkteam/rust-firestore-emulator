use std::{
    collections::VecDeque,
    fmt::{self, Debug},
    mem,
    sync::Arc,
};

use tokio::sync::oneshot;

use crate::FifoRwLock;

/// Holds the core state of the `FifoRwLock`.
pub(crate) struct FifoRwLockQueue {
    /// The FIFO queue of all pending and granted lock requests. Entries are added to the back and
    /// processed from the front. Granted entries remain in the queue until their corresponding
    /// guard is dropped.
    queue: VecDeque<QueueEntry>,
    /// The number of currently active `(Owned)ReadGuard`s.
    active_readers: usize,
    /// `true` if an `OwnedWriteGuard` is currently active, `false` otherwise.
    writer_active: bool,
    /// A counter to generate unique sequential IDs for each `QueueEntry`.
    next_id: usize,
}

/// Represents a request (read or write) in the lock's queue.
struct QueueEntry {
    /// A unique ID for this queue entry, assigned sequentially.
    id:    usize,
    /// The current state of this queue entry (e.g., requested, granted).
    state: QueueEntryState,
}

/// RAII handle representing a request's presence in the `FifoRwLock`'s queue.
///
/// When this handle is dropped (e.g., if the task awaiting the lock is cancelled), it automatically
/// removes its corresponding entry from the lock's queue via the `FifoRwLockState::release` method.
/// This ensures that cancelled requests don't indefinitely block other requests.
///
/// It depends on lifecycle bounds to keep the lock alive as long as there are pending requests or
/// active guards associated with it.
#[derive(Debug)]
pub(crate) struct QueueEntryHandle<'a, T> {
    /// The unique ID of the `QueueEntry` this handle corresponds to.
    pub(crate) id:      usize,
    /// A reference to the `FifoRwLock` this handle belongs to.
    pub(crate) rw_lock: &'a FifoRwLock<T>,
}

/// RAII handle representing a request's presence in the `FifoRwLock`'s queue.
///
/// When this handle is dropped (e.g., if the task awaiting the lock is cancelled), it automatically
/// removes its corresponding entry from the lock's queue via the `FifoRwLockState::release` method.
/// This ensures that cancelled requests don't indefinitely block other requests.
///
/// It holds an `Arc<FifoRwLock<T>>` to keep the lock alive as long as there are pending requests or
/// active guards associated with it.
#[derive(Debug)]
pub(crate) struct OwnedQueueEntryHandle<T> {
    /// The unique ID of the `QueueEntry` this handle corresponds to.
    pub(crate) id:      usize,
    /// A reference to the `FifoRwLock` this handle belongs to.
    pub(crate) rw_lock: Arc<FifoRwLock<T>>,
}

/// Represents the specific state of a `QueueEntry`.
enum QueueEntryState {
    /// The task has requested a read lock and is waiting. The `listener` is a `oneshot::Sender`
    /// used to notify the task when the lock is granted.
    ReadRequested { listener: oneshot::Sender<()> },
    /// The read lock has been granted, and an `(Owned)ReadGuard` exists.
    ReadGranted,
    /// The task has requested a write lock (either initially or via upgrade) and is waiting. The
    /// `listener` is used to notify the task when the lock is granted.
    WriteRequested { listener: oneshot::Sender<()> },
    /// The write lock has been granted, and an `OwnedWriteGuard` exists.
    WriteGranted,
}

impl FifoRwLockQueue {
    pub(crate) fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            active_readers: 0,
            writer_active: false,
            next_id: 0,
        }
    }

    /// Adds a new read request to the FIFO queue and returns a handle for it.
    pub(crate) fn add_read_request<'a, T>(
        &mut self,
        rw_lock: &'a FifoRwLock<T>,
        listener: oneshot::Sender<()>,
    ) -> QueueEntryHandle<'a, T> {
        // Return a handle that, when dropped, will call `self.release(id)`.
        QueueEntryHandle {
            id: self.add_request(QueueEntryState::ReadRequested { listener }),
            rw_lock,
        }
    }

    /// Adds a new read request to the FIFO queue and returns a handle for it.
    pub(crate) fn add_owned_read_request<T>(
        &mut self,
        rw_lock: &Arc<FifoRwLock<T>>,
        listener: oneshot::Sender<()>,
    ) -> OwnedQueueEntryHandle<T> {
        // Return a handle that, when dropped, will call `self.release(id)`.
        OwnedQueueEntryHandle {
            id:      self.add_request(QueueEntryState::ReadRequested { listener }),
            rw_lock: rw_lock.clone(),
        }
    }

    /// Adds a new write request to the FIFO queue and returns a handle for it.
    pub(crate) fn add_owned_write_request<T>(
        &mut self,
        rw_lock: &Arc<FifoRwLock<T>>,
        listener: oneshot::Sender<()>,
    ) -> OwnedQueueEntryHandle<T> {
        // Return a handle that, when dropped, will call `self.release(id)`.
        OwnedQueueEntryHandle {
            id:      self.add_request(QueueEntryState::WriteRequested { listener }),
            rw_lock: rw_lock.clone(),
        }
    }

    /// Adds a new request to the FIFO queue and returns its ID.
    #[must_use]
    fn add_request(&mut self, state: QueueEntryState) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        let entry = QueueEntry { id, state };
        self.queue.push_back(entry);

        // After adding, check if any requests (including potentially this new one) can be granted.
        self.check_and_notify();

        id
    }

    /// Changes a `ReadGranted` queue entry to `WriteRequested` for an upgrade attempt.
    pub(crate) fn upgrade_request(&mut self, queue_id: usize, listener: oneshot::Sender<()>) {
        // Find the existing queue entry for the read lock being upgraded.
        let entry = self
            .queue
            .iter_mut()
            .find(|e| e.id == queue_id)
            .expect("unknown queue ID");
        assert!(
            matches!(entry.state, QueueEntryState::ReadGranted),
            "queue entry is not ReadGranted"
        );
        self.active_readers -= 1;

        // Change the entry's state to WriteRequested, using the new listener for the upgrade.
        // The entry remains in its original position in the queue.
        *entry = QueueEntry {
            id:    queue_id,
            state: QueueEntryState::WriteRequested { listener },
        };

        // An effective reader release occurred, so check if other tasks can proceed.
        self.check_and_notify();
    }

    /// Changes a `WriteGranted` queue entry to `ReadGranted` for a downgrade.
    /// This is an atomic operation from the caller's perspective.
    pub(crate) fn downgrade_request(&mut self, queue_id: usize) {
        let entry = self
            .queue
            .iter_mut()
            .find(|e| e.id == queue_id)
            .expect("unknown queue ID");
        assert!(
            matches!(entry.state, QueueEntryState::WriteGranted),
            "queue entry is not WriteGranted"
        );
        self.writer_active = false;
        self.active_readers += 1;

        // Change the entry's state to ReadGranted.
        *entry = QueueEntry {
            id:    queue_id,
            state: QueueEntryState::ReadGranted,
        };

        // A writer released, potentially allowing many readers or the next writer.
        self.check_and_notify();
    }

    /// Removes a request from the queue, called when a `(Owned)QueueEntryHandle` is dropped.
    fn release(&mut self, queue_id: usize) {
        let idx = self
            .queue
            .iter()
            .position(|e| e.id == queue_id)
            .expect("unknown queue ID");

        // Remove the entry and get its state to update active counts.
        match self.queue.remove(idx).unwrap().state {
            QueueEntryState::ReadRequested { .. } | QueueEntryState::WriteRequested { .. } => {
                // If the request was only 'Requested' (i.e., listener not yet triggered),
                // no active counts need to be changed. The listener's `rx` side will simply
                // get a `RecvError` when it's awaited, or the future will be dropped.
            }
            QueueEntryState::ReadGranted => self.active_readers -= 1,
            QueueEntryState::WriteGranted => self.writer_active = false,
        }

        // After any release, check if other waiting tasks can now proceed.
        self.check_and_notify();
    }

    /// Iterates through the queue and notifies tasks whose requests can now be granted. This is the
    /// core scheduling logic of the lock.
    fn check_and_notify(&mut self) {
        // If a writer is already active, no other request (read or write) can be granted.
        if self.writer_active {
            return;
        }

        for entry in &mut self.queue {
            debug_assert!(!self.writer_active); // invariant

            match &entry.state {
                QueueEntryState::ReadRequested { .. } => {
                    // Since `self.writer_active` is false (checked above and invariant throughout
                    // this for loop), and by iterating from the front, we ensure no *preceding*
                    // entry was a WriteRequest that hasn't been granted yet (otherwise we would
                    // have broken out of this loop). So, this read request can be granted.
                    self.active_readers += 1;
                    // Transition the entry's state to ReadGranted and take its listener.
                    let listener = entry.state.take_listener(QueueEntryState::ReadGranted);
                    // Notify the waiting task by sending on the oneshot channel. If `send` fails,
                    // it means the receiver (`rx`) was dropped (task cancelled). This is fine;
                    // `(Owned)QueueEntryHandle::drop` will clean up the queue entry.
                    let _unused = listener.send(());
                    // Continue the loop: other subsequent ReadRequests might also be grantable.
                }
                QueueEntryState::WriteRequested { .. } => {
                    // Current entry is a WriteRequest. It can only be granted if there are NO
                    // active readers.
                    if self.active_readers != 0 {
                        // Readers are active; this writer (and any subsequent requests) must wait.
                        break;
                    }
                    // No active readers, and no active writer (invariant). This write request
                    // (being the first ungranted writer encountered) can proceed.
                    self.writer_active = true;
                    let listener = entry.state.take_listener(QueueEntryState::WriteGranted);
                    // Notify the waiting task by sending on the oneshot channel. If `send` fails,
                    // it means the receiver (`rx`) was dropped (task cancelled). This is fine;
                    // `(Owned)QueueEntryHandle::drop` will clean up the queue entry.
                    let _unused = listener.send(());
                    // A writer is now active. No other requests can be granted.
                    break;
                }
                QueueEntryState::ReadGranted => {
                    // This entry is already a granted read lock.
                    // Sanity check: if we have granted readers, active_readers should be > 0.
                    assert_ne!(
                        self.active_readers, 0,
                        "invalid state, found ReadGranted entry but active_readers is 0."
                    );
                    // Continue, as other readers might be grantable or a writer might be waiting.
                }
                QueueEntryState::WriteGranted => {
                    // This entry is already a granted write lock. This should not happen if
                    // `self.writer_active` was false at the start of the loop. This indicates an
                    // inconsistent state.
                    panic!("invalid state, writer_active is false but found a WriteGranted entry.");
                }
            }
        }
    }
}

impl Debug for FifoRwLockQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.queue.fmt(f)
    }
}

impl Debug for QueueEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.id + 1;
        match &self.state {
            QueueEntryState::ReadRequested { .. } => write!(f, "R{id} (pending)"),
            QueueEntryState::ReadGranted => write!(f, "R{id}"),
            QueueEntryState::WriteRequested { .. } => write!(f, "W{id} (pending)"),
            QueueEntryState::WriteGranted => write!(f, "W{id}"),
        }
    }
}

impl QueueEntryState {
    /// Replaces the current `QueueEntryState` with `new_state` and returns the listener
    /// from the previous state if it was `ReadRequested` or `WriteRequested`.
    /// Panics if called on an already granted state.
    fn take_listener(&mut self, new_state: Self) -> oneshot::Sender<()> {
        match mem::replace(self, new_state) {
            Self::ReadRequested { listener } | Self::WriteRequested { listener } => listener,
            Self::ReadGranted | Self::WriteGranted => {
                panic!("take_listener called on an already granted state")
            }
        }
    }
}

/// When a `QueueEntryHandle` is dropped, it signals the `FifoRwLockState` to release (remove) the
/// corresponding entry from the queue. This is crucial for cancellation safety.
impl<T> Drop for QueueEntryHandle<'_, T> {
    fn drop(&mut self) {
        self.rw_lock.state.lock().unwrap().release(self.id);
    }
}

/// When a `OwnedQueueEntryHandle` is dropped, it signals the `FifoRwLockState` to release (remove)
/// the corresponding entry from the queue. This is crucial for cancellation safety.
impl<T> Drop for OwnedQueueEntryHandle<T> {
    fn drop(&mut self) {
        self.rw_lock.state.lock().unwrap().release(self.id);
    }
}
