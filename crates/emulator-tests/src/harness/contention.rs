use std::{fmt, future::Future, time::Duration};

use tokio::time::advance;
use tokio_test::task::Spawn;
use tonic::{Code, Status};

use super::client::TestDb;

/// Two-actor virtual-time coordinator for deterministic lock-contention, FIFO fairness, upgrade
/// deadlock, and `Timeouts::FAST` / `Timeouts::CLOUD` boundary tests under
/// `#[tokio::test(start_paused = true)]`.
#[derive(Clone, Debug)]
pub(crate) struct ContentionProbe {
    pub(crate) actor_a: TestDb,
    pub(crate) actor_b: TestDb,
}

impl ContentionProbe {
    pub(crate) fn new(db: &TestDb) -> Self {
        Self {
            actor_a: db.clone(),
            actor_b: db.clone(),
        }
    }

    pub(crate) fn spawn_op<F>(fut: F) -> SpawnedOp<F>
    where
        F: Future,
    {
        SpawnedOp {
            task:      tokio_test::task::spawn(fut),
            completed: None,
        }
    }

    /// Advances Tokio's paused virtual clock in bounded steps (`<= 250ms` per tick) and yields
    /// runtime tasks between steps so intermediate timers (such as `document.rs`'s `1s` slow-lock
    /// warning `tokio::select!` branch) wake, re-poll, and re-arm the outer lock timeout cleanly.
    pub(crate) async fn advance_and_poll<F1, F2>(
        total: Duration,
        first: &mut SpawnedOp<F1>,
        second: Option<&mut SpawnedOp<F2>>,
    ) where
        F1: Future,
        F2: Future,
    {
        const STEP: Duration = Duration::from_millis(250);
        let mut remaining = total;

        first.drive_settled().await;
        let mut second = second;
        if let Some(op2) = second.as_deref_mut() {
            op2.drive_settled().await;
        }

        while !remaining.is_zero() {
            let delta = remaining.min(STEP);
            advance(delta).await;
            remaining -= delta;
            tokio::task::yield_now().await;
            first.drive_settled().await;
            if let Some(op2) = second.as_deref_mut() {
                op2.drive_settled().await;
            }
        }
    }
}

pub(crate) struct SpawnedOp<F: Future> {
    task:      Spawn<F>,
    completed: Option<F::Output>,
}

impl<F: Future> fmt::Debug for SpawnedOp<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpawnedOp")
            .field("is_completed", &self.completed.is_some())
            .field("is_woken", &self.task.is_woken())
            .finish()
    }
}

impl<F: Future> SpawnedOp<F> {
    /// Polls the spawned future repeatedly across cooperative yields until either it resolves
    /// (caching the output in `self.completed`) or remains quiescently `Poll::Pending`.
    pub(crate) async fn drive_settled(&mut self) {
        if self.completed.is_some() {
            return;
        }
        for _ in 0..8 {
            if let std::task::Poll::Ready(out) = self.task.poll() {
                self.completed = Some(out);
                return;
            }
            tokio::task::yield_now().await;
            if !self.task.is_woken() {
                break;
            }
        }
    }

    pub(crate) async fn assert_pending(&mut self) {
        self.drive_settled().await;
        assert!(
            self.completed.is_none(),
            "expected operation to remain Poll::Pending (blocked on lock)"
        );
    }

    pub(crate) async fn expect_ready(&mut self) -> F::Output {
        if let Some(out) = self.completed.take() {
            return out;
        }
        for _ in 0..16 {
            if let std::task::Poll::Ready(out) = self.task.poll() {
                return out;
            }
            tokio::task::yield_now().await;
        }
        panic!("expected spawned operation to resolve to Poll::Ready, got Poll::Pending");
    }
}

impl<F, T> SpawnedOp<F>
where
    F: Future<Output = Result<T, Status>>,
    T: fmt::Debug,
{
    pub(crate) async fn expect_aborted(&mut self) -> Status {
        let err = self
            .expect_ready()
            .await
            .expect_err("expected lock timeout or contention to abort operation");
        assert_eq!(
            err.code(),
            Code::Aborted,
            "expected Code::Aborted, got {:?}: {}",
            err.code(),
            err.message()
        );
        err
    }
}
