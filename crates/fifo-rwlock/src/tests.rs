// Note: `tokio_test` is used for fine-grained polling in some tests.
// For multithreaded tests, `#[tokio::test(flavor = "multi_thread")]` is used.

use std::{hint::black_box, sync::Arc};

use tokio_test::{assert_pending, assert_ready, task::spawn};

use super::*;

/// Helper function to get a snapshot of the lock's internal state for assertions.
fn get_queue<T>(lock: &FifoRwLock<T>) -> String {
    format!("{:?}", lock.state.lock().unwrap())
}

// --- Basic Lock/Unlock Tests ---

#[test]
fn test_single_reader() {
    // Purpose: Verify basic read lock acquisition and release.
    let lock = FifoRwLock::new(10);

    let mut r1_future = spawn(lock.read());
    let r1 = assert_ready!(r1_future.poll());

    assert_eq!(get_queue(&lock), "[R1]");

    assert_eq!(*r1, 10, "Read initial value through guard");
    drop(r1); // Explicitly drop the guard to release the lock.

    assert_eq!(get_queue(&lock), "[]");
}

#[test]
fn test_single_reader_owned() {
    // Purpose: Verify basic read lock acquisition and release.
    let lock = Arc::new(FifoRwLock::new(10));

    let mut r1_future = spawn(lock.read_owned());
    let r1 = assert_ready!(r1_future.poll());

    assert_eq!(get_queue(&lock), "[R1]");

    assert_eq!(*r1, 10, "Read initial value through guard");
    drop(r1); // Explicitly drop the guard to release the lock.

    assert_eq!(get_queue(&lock), "[]");
}

#[test]
fn test_single_writer() {
    // Purpose: Verify basic write lock acquisition, data modification, and release.
    let lock = Arc::new(FifoRwLock::new(10));

    let mut w1_future = spawn(lock.write_owned());
    let mut w1 = assert_ready!(w1_future.poll());

    assert_eq!(get_queue(&lock), "[W1]");

    assert_eq!(*w1, 10, "Read initial value via write guard");
    *w1 = 20;
    assert_eq!(*w1, 20, "Read modified value via write guard");
    drop(w1);

    assert_eq!(get_queue(&lock), "[]");

    // Verify modification by acquiring a new read lock.
    let mut read_guard_future = spawn(lock.read_owned());
    let read_guard = assert_ready!(read_guard_future.poll());
    assert_eq!(*read_guard, 20, "Verify written value with a new read lock");
}

// --- Concurrency Tests ---

#[test]
fn multiple_readers() {
    // Purpose: Verify that multiple readers can acquire the lock concurrently.
    let lock = Arc::new(FifoRwLock::new(10));

    let mut r1_future = spawn(lock.read());
    let mut r2_future = spawn(lock.read_owned());

    let r1 = assert_ready!(r1_future.poll(), "Reader 1 should acquire immediately");
    let r2 = assert_ready!(
        r2_future.poll(),
        "Reader 2 should acquire immediately as well"
    );

    assert_eq!(get_queue(&lock), "[R1, R2]");

    assert_eq!(*r1, 10, "Reader 1 sees correct value");
    assert_eq!(*r2, 10, "Reader 2 sees correct value");
    drop((r1, r2));

    assert_eq!(get_queue(&lock), "[]");
}

#[test]
fn only_a_single_writer_at_a_time() {
    // Purpose: Verify that only one writer can acquire the lock, and a second writer waits.
    let lock = Arc::new(FifoRwLock::new(10));

    let mut w1_future = spawn(lock.write_owned());
    let mut w1 = assert_ready!(w1_future.poll(), "Writer 1 acquires lock");

    let mut w2_future = spawn(lock.write_owned());
    assert_pending!(w2_future.poll(), "Writer 2 should be pending");

    assert_eq!(get_queue(&lock), "[W1, W2 (pending)]");

    assert_eq!(*w1, 10);
    *w1 = 20;
    drop(w1);

    // After W1 releases, W2 should be able to acquire the lock.
    assert_eq!(get_queue(&lock), "[W2]");

    let mut w2 = assert_ready!(w2_future.poll(), "Writer 2 acquires lock");
    assert_eq!(*w2, 20, "Writer 2 sees value modified by W1");
    *w2 = 30;
    drop(w2);

    assert_eq!(get_queue(&lock), "[]");
}

// --- FIFO and Blocking Tests ---

#[test]
fn write_waits_on_earlier_readers() {
    // Scenario: R1 (granted) -> W2 (waits) -> R3 (waits for W2).
    let lock = Arc::new(FifoRwLock::new(10));

    // R1 requests and gets read access.
    let mut r1_future = spawn(lock.read_owned());
    let r1 = assert_ready!(r1_future.poll());
    assert_eq!(get_queue(&lock), "[R1]");

    // W2 requests write access, should wait for R1.
    let mut w2_future = spawn(lock.write_owned());
    assert_pending!(w2_future.poll(), "W2 should wait for R1");
    assert_eq!(get_queue(&lock), "[R1, W2 (pending)]");

    // R3 requests read access, should wait for W2 (due to FIFO).
    let mut r3_future = spawn(lock.read());
    assert_pending!(r3_future.poll(), "R3 should wait for W2");
    assert_eq!(get_queue(&lock), "[R1, W2 (pending), R3 (pending)]");

    // R1 releases.
    assert_eq!(*r1, 10);
    drop(r1);
    assert_eq!(get_queue(&lock), "[W2, R3 (pending)]");

    let mut w2 = assert_ready!(w2_future.poll(), "W2 acquires after R1 releases");
    *w2 = 20;
    assert_pending!(r3_future.poll(), "R3 still waits for W2 to release");
    drop(w2);
    assert_eq!(get_queue(&lock), "[R3]");

    // R3 should now be granted.
    let r3 = assert_ready!(r3_future.poll(), "R3 acquires after W2 releases");
    assert_eq!(*r3, 20, "R3 sees value modified by W2");
    drop(r3);
    assert_eq!(get_queue(&lock), "[]");
}

// --- Upgrade and Downgrade Tests ---

#[test]
fn upgrade_gets_priority_over_existing_writerequests() {
    // Scenario: R1, R2 (concurrent reads) -> W3 (queues) -> R1 upgrades.
    // R1's upgrade should get priority over W3 due to original queue order.
    let lock = Arc::new(FifoRwLock::new(10));

    let r1 = assert_ready!(spawn(lock.read_owned()).poll());
    let r2 = assert_ready!(spawn(lock.read()).poll());
    assert_eq!(get_queue(&lock), "[R1, R2]");

    let mut w3_future = spawn(lock.write_owned());
    assert_pending!(w3_future.poll(), "W3 waits for R1, R2");
    assert_eq!(get_queue(&lock), "[R1, R2, W3 (pending)]");

    let mut w1_future = spawn(r1.upgrade());
    assert_pending!(w1_future.poll(), "R1 upgrade waits for R2");
    // After upgrade initiated: R1's queue entry is now WriteRequested. R2 is still active.
    // R1 is no longer an "active reader" in the count, but its request is pending as write.
    assert_eq!(get_queue(&lock), "[W1 (pending), R2, W3 (pending)]");

    // R2 still works.
    assert_eq!(*r2, 10);
    drop(r2);

    // Now R1's upgrade should proceed as it's effectively the first writer in queue order.
    assert_eq!(get_queue(&lock), "[W1, W3 (pending)]");

    // W3 is still not awarded a write guard because R1's upgrade had priority.
    assert_pending!(w3_future.poll(), "W3 still waits for R1's upgraded write");
    let mut w1 = assert_ready!(w1_future.poll(), "R1 upgrade completes");
    *w1 = 20;
    drop(w1);

    assert_eq!(get_queue(&lock), "[W3]");

    // Finally, W3 gets its turn.
    let w3 = assert_ready!(w3_future.poll(), "W3 finally acquires");
    assert_eq!(*w3, 20, "W3 sees value from R1's upgrade");
    drop(w3);

    assert_eq!(get_queue(&lock), "[]");
}

#[test]
fn upgrade_has_to_wait_on_upgrades_with_higher_priority() {
    // Scenario: R1, R2, R3, R4 read. R3 upgrades, R2 upgrades.
    // R2's upgrade should get priority over R3's due to original queue order.
    let lock = Arc::new(FifoRwLock::new(10));

    let r1 = assert_ready!(spawn(lock.read()).poll());
    let r2 = assert_ready!(spawn(lock.read_owned()).poll());
    let r3 = assert_ready!(spawn(lock.read_owned()).poll());
    let r4 = assert_ready!(spawn(lock.read()).poll());
    assert_eq!(get_queue(&lock), "[R1, R2, R3, R4]");

    // R3 initiates upgrade. Waits for R1, R2, R4.
    let mut w3_future = spawn(r3.upgrade());
    assert_pending!(w3_future.poll(), "R3 upgrade waits");
    // R3 is no longer an active reader.
    assert_eq!(
        get_queue(&lock),
        "[R1, R2, W3 (pending), R4]",
        "R3 upgrading to W3, 3 other readers active"
    );

    // R2 initiates upgrade. Waits for R1, R4. Its original position is before R3's.
    let mut w2_future = spawn(r2.upgrade());
    assert_pending!(w2_future.poll(), "R2 upgrade waits");
    // R2 is no longer an active reader.
    assert_eq!(
        get_queue(&lock),
        "[R1, W2 (pending), W3 (pending), R4]",
        "R2 also upgrading to R2, 2 other readers active"
    );

    drop(r1);

    assert_pending!(w2_future.poll(), "R2 upgrade still waits for R4");
    assert_pending!(w3_future.poll(), "R3 upgrade still waits for R2 & R4");
    assert_eq!(get_queue(&lock), "[W2 (pending), W3 (pending), R4]");

    drop(r4);

    assert_eq!(get_queue(&lock), "[W2, W3 (pending)]");

    let mut w2 = assert_ready!(w2_future.poll(), "R2 upgrade completes");
    assert_pending!(w3_future.poll(), "R3 upgrade waits for R2's upgraded write");
    *w2 = 20;
    drop(w2); // R2's upgraded write releases.

    assert_eq!(get_queue(&lock), "[W3]");

    // R3's upgrade now proceeds.
    let w3 = assert_ready!(w3_future.poll(), "R3 upgrade completes");
    assert_eq!(*w3, 20, "R3 sees value from R2's upgrade");
    drop(w3);
    assert_eq!(get_queue(&lock), "[]");
}

#[test]
fn downgrade_allows_other_readers() {
    // Scenario: W1 -> R2 (waits) -> W3 (waits) -> R4 (waits).
    // W1 downgrades. R2 should get access. R4 still waits for W3.
    let lock = Arc::new(FifoRwLock::new(10));

    let mut w1_future = spawn(lock.write_owned());
    let mut w1 = assert_ready!(w1_future.poll());

    let mut r2_future = spawn(lock.read());
    assert_pending!(r2_future.poll(), "R2 waits for W1");
    let mut w3_future = spawn(lock.write_owned());
    assert_pending!(w3_future.poll(), "W3 waits for R2 (which waits for W1)");
    let mut r4_future = spawn(lock.read());
    assert_pending!(r4_future.poll(), "R4 waits for W3");

    assert_eq!(
        get_queue(&lock),
        "[W1, R2 (pending), W3 (pending), R4 (pending)]"
    );

    *w1 += 10;

    let r1 = w1.downgrade();
    // After downgrade: W1 is now a reader (R1). R2 (next in queue) also becomes a reader.
    // W3 is still queued. R4 is still queued behind W3.
    assert_eq!(get_queue(&lock), "[R1, R2, W3 (pending), R4 (pending)]");

    let r2 = assert_ready!(r2_future.poll(), "R2 acquires after W1 downgrades");
    assert_pending!(w3_future.poll(), "W3 still waits");
    assert_pending!(r4_future.poll(), "R4 still waits");

    assert_eq!(*r1, 20, "Downgraded W1 sees modified value");
    assert_eq!(*r2, 20, "R2 sees modified value");

    drop((r1, r2, w3_future));
    assert_eq!(get_queue(&lock), "[R4]");

    let r4 = assert_ready!(r4_future.poll(), "R4 finally acquires");
    assert_eq!(*r4, 20, "R4 sees value from W1");
}

// --- Cancellation Tests (Dropping Futures) ---

#[test]
fn dropping_write_request_will_remove_request_from_queue() {
    // Scenario: R1 (active) -> W2 (queued) -> R3 (queued).
    // W2's future is dropped. R3 should then get access after R1.
    let lock = Arc::new(FifoRwLock::new(10));

    let _r1 = assert_ready!(spawn(lock.read_owned()).poll());

    let mut w2_future = spawn(lock.write_owned()); // W2 queues
    assert_pending!(w2_future.poll());
    let mut r3_future = spawn(lock.read()); // R3 queues behind W2
    assert_pending!(r3_future.poll());

    assert_eq!(get_queue(&lock), "[R1, W2 (pending), R3 (pending)]");

    drop(w2_future);

    assert_eq!(
        get_queue(&lock),
        "[R1, R3]",
        "R3 is granted read access now]"
    );

    let r3 = assert_ready!(r3_future.poll());
    assert_eq!(*r3, 10);
}

#[test]
fn dropping_read_request_will_remove_request_from_queue() {
    // Scenario: W1 (active) -> R2 (queued).
    // R2's future is dropped. Queue should only contain W1.
    let lock = Arc::new(FifoRwLock::new(10));

    let _w1 = assert_ready!(spawn(lock.write_owned()).poll());
    let mut r2_fut = spawn(lock.read_owned());
    assert_pending!(r2_fut.poll());

    assert_eq!(get_queue(&lock), "[W1, R2 (pending)]");

    drop(r2_fut);

    assert_eq!(get_queue(&lock), "[W1]");
}

// --- Multithreaded Stress Test ---

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multithreaded() {
    use futures::stream::{self, StreamExt};
    use tokio::sync::Barrier;

    let num_iterations = 1000; // Number of operations per task
    let num_writers_per_type = 2; // Number of tasks performing each type of write operation

    let barrier = Arc::new(Barrier::new(num_writers_per_type * 4 + 1)); // +1 for the main thread
    let rwlock = Arc::new(FifoRwLock::new(0_u32));

    let mut expected_sum = 0;

    // Spawn tasks that increment by 2
    for _ in 0..num_writers_per_type {
        let rwlock = rwlock.clone();
        let barrier = barrier.clone();
        expected_sum += 2 * num_iterations;
        tokio::spawn(async move {
            stream::iter(0..num_iterations)
                .for_each(|_| {
                    let rwlock = rwlock.clone();
                    async move {
                        let mut guard = rwlock.write_owned().await;
                        *guard += 2;
                    }
                })
                .await;
            barrier.wait().await;
        });
    }

    // Spawn tasks that increment by 3
    for _ in 0..num_writers_per_type {
        let rwlock = rwlock.clone();
        let barrier = barrier.clone();
        expected_sum += 3 * num_iterations;
        tokio::spawn(async move {
            stream::iter(0..num_iterations)
                .for_each(|_| {
                    let rwlock = rwlock.clone();
                    async move {
                        let mut guard = rwlock.write_owned().await;
                        *guard += 3;
                    }
                })
                .await;
            barrier.wait().await;
        });
    }

    // Spawn tasks that increment by 5 (via upgrade)
    for _ in 0..num_writers_per_type {
        let rwlock = rwlock.clone();
        let barrier = barrier.clone();
        expected_sum += 5 * num_iterations;
        tokio::spawn(async move {
            stream::iter(0..num_iterations)
                .for_each(|_| {
                    let rwlock = rwlock.clone();
                    async move {
                        let read_guard = rwlock.read_owned().await;
                        // In a real scenario, might do some reading here.
                        let _ = *read_guard;
                        let mut write_guard = read_guard.upgrade().await;
                        *write_guard += 5;
                    }
                })
                .await;
            barrier.wait().await;
        });
    }
    // Spawn tasks that read
    for _ in 0..num_writers_per_type {
        // Same number of reader tasks for contention
        let rwlock = rwlock.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            stream::iter(0..num_iterations)
                .for_each(|_| {
                    let rwlock = rwlock.clone();
                    async move {
                        let guard = rwlock.read().await;
                        let value = *guard; // Perform a read
                        black_box(value);
                    }
                })
                .await;
            barrier.wait().await;
        });
    }

    barrier.wait().await;

    let g = rwlock.read_owned().await;
    assert_eq!(*g, expected_sum);
}
