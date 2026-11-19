mod leases;
mod table;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use googleapis::google::protobuf::Timestamp;
use tokio::{
    sync::oneshot,
    time::{Instant, error::Elapsed, sleep, timeout, timeout_at},
};

use self::{
    leases::WaiterGuard,
    table::{AcquireOutcome, LockTable, WoundEvent},
};
pub(crate) use self::{
    leases::{CommitSeal, ReadLease, WriteLease},
    table::ReadObservation,
};
use crate::{
    database::{
        reference::{DocumentRef, RootRef},
        timeouts::Timeouts,
        transaction::TransactionId,
    },
    error::{AbortReason, CommitError},
};

pub(super) const STATE_ACTIVE: u8 = 0;
pub(super) const STATE_COMMITTING: u8 = 1;
pub(super) const STATE_WOUNDED: u8 = 2;
pub(super) const STATE_TIMED_OUT: u8 = 3;
pub(super) const STATE_CLEARED: u8 = 4;
pub(super) const STATE_CLOSED: u8 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LockPriority {
    /// Transaction priority is unassigned (`0`) at creation and allocated monotonically on first
    /// lock acquisition inside [`LockTable`].
    Lazy,
    /// Inherit an existing lineage priority sequence across transaction retries.
    Assigned(u64),
    /// Eagerly allocate the next monotonic sequence at session creation (used for both
    /// multi-document non-transactional `WriteBatch` commits with `can_wound = true, woundable =
    /// true` and single-document non-transactional writes / `Default` point reads with
    /// `can_wound = false, woundable = false`).
    NextSeq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionAbortState {
    Active,
    Committing,
    Wounded(AbortReason),
    TimedOut(AbortReason),
    Cleared(AbortReason),
    Closed,
}

/// Database-scoped Wound-Wait lock manager coordinating read/write leases, priority wait queues,
/// and cross-document preemption without `unsafe` code or storage callbacks.
#[derive(Debug)]
pub(crate) struct LockManager {
    database: RootRef,
    timeouts: Timeouts,
    table: Mutex<LockTable>,
    next_session_id: AtomicU64,
    next_seq: AtomicU64,
    cleared: AtomicBool,
}

impl LockManager {
    pub(crate) fn new(database: RootRef, timeouts: Timeouts) -> Self {
        Self {
            database,
            timeouts,
            table: Mutex::new(LockTable::default()),
            next_session_id: AtomicU64::new(1),
            next_seq: AtomicU64::new(1),
            cleared: AtomicBool::new(false),
        }
    }

    pub(crate) fn new_session(
        self: &Arc<Self>,
        txn_id: Option<TransactionId>,
        priority: LockPriority,
        can_wound: bool,
        woundable: bool,
    ) -> Arc<LockSession> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let initial_seq = match priority {
            LockPriority::Lazy => 0,
            LockPriority::Assigned(seq) => {
                assert!(
                    (1..u64::MAX).contains(&seq),
                    "invalid assigned lock seq: {seq}"
                );
                seq
            }
            LockPriority::NextSeq => {
                let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
                assert!(seq < u64::MAX, "lock priority sequence exhausted");
                seq
            }
        };

        let session = Arc::new(LockSession {
            manager: Arc::clone(self),
            session_id,
            txn_id,
            seq: AtomicU64::new(initial_seq),
            state: AtomicU8::new(STATE_ACTIVE),
            abort_reason: Mutex::new(None),
        });

        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        table.register_session(
            session_id,
            Arc::downgrade(&session),
            txn_id,
            initial_seq,
            can_wound,
            woundable,
        );
        drop(table);

        session
    }

    pub(crate) fn abort_all(&self, reason: AbortReason) {
        if reason == AbortReason::DatabaseCleared {
            self.cleared.store(true, Ordering::Release);
        }
        let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        table.abort_all(reason);
        drop(table);
        tracing::debug!(
            target: "emulator_database::lock",
            database = %self.database,
            reason = %reason,
            "aborted all lock sessions"
        );
    }

    pub(crate) fn is_cleared(&self) -> bool {
        self.cleared.load(Ordering::Acquire)
    }
}

/// Per-transaction or single-operation lock session handle.
#[derive(Debug)]
pub(crate) struct LockSession {
    manager: Arc<LockManager>,
    session_id: u64,
    txn_id: Option<TransactionId>,
    seq: AtomicU64,
    state: AtomicU8,
    abort_reason: Mutex<Option<AbortReason>>,
}

impl LockSession {
    #[cfg(test)]
    pub(crate) fn session_id(&self) -> u64 {
        self.session_id
    }

    pub(crate) fn root_seq(&self) -> Option<u64> {
        let seq = self.seq.load(Ordering::Acquire);
        (1..u64::MAX).contains(&seq).then_some(seq)
    }

    #[cfg(test)]
    pub(crate) fn is_wounded(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_WOUNDED
    }

    pub(crate) fn abort_state(&self) -> SessionAbortState {
        match self.state.load(Ordering::Acquire) {
            STATE_ACTIVE => SessionAbortState::Active,
            STATE_COMMITTING => SessionAbortState::Committing,
            STATE_WOUNDED => {
                SessionAbortState::Wounded(self.read_abort_reason(AbortReason::Wounded {
                    wounder_txn_id: None,
                    wounder_seq:    0,
                }))
            }
            STATE_TIMED_OUT => {
                SessionAbortState::TimedOut(self.read_abort_reason(AbortReason::WriteTimeout))
            }
            STATE_CLEARED => {
                SessionAbortState::Cleared(self.read_abort_reason(AbortReason::DatabaseCleared))
            }
            _ => SessionAbortState::Closed,
        }
    }

    pub(crate) fn check_active(&self) -> Result<(), AbortReason> {
        match self.abort_state() {
            SessionAbortState::Active => Ok(()),
            SessionAbortState::Wounded(reason)
            | SessionAbortState::TimedOut(reason)
            | SessionAbortState::Cleared(reason) => Err(reason),
            SessionAbortState::Committing | SessionAbortState::Closed => {
                Err(AbortReason::DatabaseCleared)
            }
        }
    }

    pub(super) fn check_committing_active(&self) -> Result<(), AbortReason> {
        match self.abort_state() {
            SessionAbortState::Active | SessionAbortState::Committing => Ok(()),
            SessionAbortState::Wounded(reason)
            | SessionAbortState::TimedOut(reason)
            | SessionAbortState::Cleared(reason) => Err(reason),
            SessionAbortState::Closed => Err(AbortReason::DatabaseCleared),
        }
    }

    pub(crate) fn retain_read(
        &self,
        lease: ReadLease,
        observed_update_time: Option<Timestamp>,
    ) -> Result<(), AbortReason> {
        if !std::ptr::eq(Arc::as_ptr(lease.session()), self) {
            drop(lease);
            return Err(AbortReason::DatabaseCleared);
        }
        if let Err(reason) = lease.check_active() {
            drop(lease);
            return Err(reason);
        }
        let doc_ref = lease.doc_ref().clone();
        let res = {
            let mut table = self
                .manager
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            table.retain_read(self.session_id, &doc_ref, observed_update_time)
        };
        match res {
            Ok(()) => {
                let _unused = lease.disarm();
                Ok(())
            }
            Err(reason) => {
                drop(lease);
                Err(reason)
            }
        }
    }

    pub(crate) fn close(&self) {
        let mut table = self
            .manager
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        table.close_session(self.session_id, None, STATE_CLOSED);
    }

    pub(crate) fn seal_for_commit(
        self: &Arc<Self>,
        writes: BTreeMap<DocumentRef, WriteLease>,
    ) -> Result<CommitSeal, AbortReason> {
        let mut write_docs = BTreeSet::new();
        for (doc_ref, lease) in &writes {
            if !std::ptr::eq(Arc::as_ptr(lease.session()), Arc::as_ptr(self))
                || lease.doc_ref() != doc_ref
            {
                drop(writes);
                return Err(AbortReason::DatabaseCleared);
            }
            write_docs.insert(doc_ref.clone());
        }

        let res = {
            let mut table = self
                .manager
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            table.seal_for_commit(self.session_id, &write_docs)
        };
        if let Err(reason) = res {
            drop(writes);
            return Err(reason);
        }

        tracing::debug!(
            target: "emulator_database::lock",
            database = %self.manager.database,
            txn_id = self.txn_id.map(tracing::field::display),
            session_id = self.session_id,
            root_seq = self.root_seq(),
            write_count = writes.len(),
            "sealed session for commit"
        );

        Ok(CommitSeal::new(Arc::clone(self), writes))
    }

    pub(crate) async fn acquire_read(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
    ) -> Result<ReadLease, CommitError> {
        let outcome = {
            let mut table = self
                .manager
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            table.try_acquire_read(self.session_id, doc_ref, &self.manager.next_seq)?
        };
        match outcome {
            AcquireOutcome::Granted { wounds, .. } => {
                self.emit_wound_logs(&wounds);
                Ok(ReadLease::new(Arc::clone(self), doc_ref.clone()))
            }
            AcquireOutcome::Wait {
                wait_id,
                rx,
                wounds,
                ..
            } => {
                self.emit_wound_logs(&wounds);
                self.await_single_waiter(
                    doc_ref,
                    "read lock",
                    wait_id,
                    rx,
                    self.manager.timeouts.read,
                    AbortReason::ReadTimeout,
                )
                .await?;
                Ok(ReadLease::new(Arc::clone(self), doc_ref.clone()))
            }
        }
    }

    pub(crate) async fn acquire_write(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
    ) -> Result<WriteLease, CommitError> {
        let (lease, _) = self.acquire_or_upgrade_write(doc_ref).await?;
        Ok(lease)
    }

    pub(crate) async fn acquire_or_upgrade_write(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
    ) -> Result<(WriteLease, ReadObservation), CommitError> {
        let outcome = {
            let mut table = self
                .manager
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            table.try_acquire_or_upgrade_write(self.session_id, doc_ref, &self.manager.next_seq)?
        };
        match outcome {
            AcquireOutcome::Granted {
                observation,
                wounds,
            } => {
                self.emit_wound_logs(&wounds);
                let acquire_time = Timestamp::now();
                Ok((
                    WriteLease::new(Arc::clone(self), doc_ref.clone(), acquire_time),
                    observation,
                ))
            }
            AcquireOutcome::Wait {
                wait_id,
                rx,
                observation,
                wounds,
            } => {
                self.emit_wound_logs(&wounds);
                self.await_single_waiter(
                    doc_ref,
                    "write lock",
                    wait_id,
                    rx,
                    self.manager.timeouts.write,
                    AbortReason::WriteTimeout,
                )
                .await?;
                let acquire_time = Timestamp::now();
                Ok((
                    WriteLease::new(Arc::clone(self), doc_ref.clone(), acquire_time),
                    observation,
                ))
            }
        }
    }

    pub(crate) async fn acquire_writes_all(
        self: &Arc<Self>,
        docs: &BTreeSet<DocumentRef>,
        deadline: Instant,
    ) -> Result<BTreeMap<DocumentRef, WriteLease>, CommitError> {
        if docs.is_empty() {
            self.check_active().map_err(CommitError::Aborted)?;
            return Ok(BTreeMap::new());
        }

        let batch = {
            let mut table = self
                .manager
                .table
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            table.try_acquire_or_queue_writes(self.session_id, docs, &self.manager.next_seq)?
        };
        self.emit_wound_logs(&batch.wounds);

        let now_ts = Timestamp::now();
        let mut leases = BTreeMap::new();
        for doc_ref in batch.granted_docs {
            leases.insert(
                doc_ref.clone(),
                WriteLease::new(Arc::clone(self), doc_ref, now_ts),
            );
        }

        if batch.pending.is_empty() {
            return Ok(leases);
        }

        struct BatchWaiter {
            doc_ref: DocumentRef,
            guard:   WaiterGuard,
            rx:      oneshot::Receiver<Result<(), CommitError>>,
        }

        let start = Instant::now();
        let waiters: Vec<BatchWaiter> = batch
            .pending
            .into_iter()
            .map(|pending| BatchWaiter {
                guard:   WaiterGuard::new(Arc::clone(self), pending.wait_id),
                doc_ref: pending.doc_ref,
                rx:      pending.rx,
            })
            .collect();

        let first_doc = waiters[0].doc_ref.clone();
        let wait_all = async {
            for BatchWaiter { doc_ref, guard, rx } in waiters {
                match rx.await {
                    Ok(Ok(())) => {
                        let consumed = {
                            let mut table = self
                                .manager
                                .table
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner);
                            table.consume_wait_grant(self.session_id, guard.wait_id())
                        };
                        match consumed {
                            Ok(()) => {
                                guard.disarm();
                                leases.insert(
                                    doc_ref.clone(),
                                    WriteLease::new(Arc::clone(self), doc_ref, Timestamp::now()),
                                );
                            }
                            Err(reason) => return Err(CommitError::Aborted(reason)),
                        }
                    }
                    Ok(Err(err)) => {
                        guard.disarm();
                        return Err(err);
                    }
                    Err(_) => {
                        return Err(CommitError::Aborted(
                            self.check_active()
                                .err()
                                .unwrap_or(AbortReason::DatabaseDropped),
                        ));
                    }
                }
            }
            Ok(())
        };

        let timed = self
            .await_with_slow_warning(
                &first_doc,
                "write lock",
                start,
                timeout_at(deadline, wait_all),
            )
            .await;

        match timed {
            Ok(Ok(())) => {
                tracing::debug!(
                    target: "emulator_database::lock",
                    database = %self.manager.database,
                    txn_id = self.txn_id.map(tracing::field::display),
                    session_id = self.session_id,
                    root_seq = self.root_seq(),
                    doc_name = %first_doc,
                    wait_ms = start.elapsed().as_millis() as u64,
                    "acquired batch write locks after wait"
                );
                Ok(leases)
            }
            Ok(Err(err)) => {
                drop(leases);
                Err(err)
            }
            Err(_elapsed) => {
                self.close_timed_out(AbortReason::WriteTimeout);
                for lease in leases.values_mut() {
                    lease.disarm();
                }
                drop(leases);
                tracing::debug!(
                    target: "emulator_database::lock",
                    database = %self.manager.database,
                    txn_id = self.txn_id.map(tracing::field::display),
                    session_id = self.session_id,
                    root_seq = self.root_seq(),
                    doc_name = %first_doc,
                    wait_ms = start.elapsed().as_millis() as u64,
                    reason = %AbortReason::WriteTimeout,
                    "batch write lock timed out"
                );
                Err(CommitError::Aborted(AbortReason::WriteTimeout))
            }
        }
    }

    async fn await_single_waiter(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
        kind: &'static str,
        wait_id: u64,
        rx: oneshot::Receiver<Result<(), CommitError>>,
        timeout_duration: Duration,
        timeout_reason: AbortReason,
    ) -> Result<(), CommitError> {
        let start = Instant::now();
        let guard = WaiterGuard::new(Arc::clone(self), wait_id);
        let awaited = self
            .await_with_slow_warning(doc_ref, kind, start, timeout(timeout_duration, rx))
            .await;

        match awaited {
            Ok(Ok(Ok(()))) => {
                let consumed = {
                    let mut table = self
                        .manager
                        .table
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    table.consume_wait_grant(self.session_id, guard.wait_id())
                };
                match consumed {
                    Ok(()) => {
                        guard.disarm();
                        tracing::debug!(
                            target: "emulator_database::lock",
                            database = %self.manager.database,
                            txn_id = self.txn_id.map(tracing::field::display),
                            session_id = self.session_id,
                            root_seq = self.root_seq(),
                            doc_name = %doc_ref,
                            lock_kind = kind,
                            wait_ms = start.elapsed().as_millis() as u64,
                            "lock waiter granted"
                        );
                        Ok(())
                    }
                    Err(reason) => Err(CommitError::Aborted(reason)),
                }
            }
            Ok(Ok(Err(err))) => {
                guard.disarm();
                tracing::debug!(
                    target: "emulator_database::lock",
                    database = %self.manager.database,
                    txn_id = self.txn_id.map(tracing::field::display),
                    session_id = self.session_id,
                    root_seq = self.root_seq(),
                    doc_name = %doc_ref,
                    lock_kind = kind,
                    wait_ms = start.elapsed().as_millis() as u64,
                    reason = %err_display(&err),
                    "lock waiter cancelled"
                );
                Err(err)
            }
            Ok(Err(_)) => Err(CommitError::Aborted(
                self.check_active()
                    .err()
                    .unwrap_or(AbortReason::DatabaseDropped),
            )),
            Err(_elapsed) => {
                guard.disarm();
                self.close_timed_out(timeout_reason);
                tracing::debug!(
                    target: "emulator_database::lock",
                    database = %self.manager.database,
                    txn_id = self.txn_id.map(tracing::field::display),
                    session_id = self.session_id,
                    root_seq = self.root_seq(),
                    doc_name = %doc_ref,
                    lock_kind = kind,
                    wait_ms = start.elapsed().as_millis() as u64,
                    reason = %timeout_reason,
                    "lock wait timed out"
                );
                Err(CommitError::Aborted(timeout_reason))
            }
        }
    }

    async fn await_with_slow_warning<F, Out>(
        &self,
        doc_ref: &DocumentRef,
        kind: &'static str,
        start: Instant,
        timeout_fut: F,
    ) -> Result<Out, Elapsed>
    where
        F: Future<Output = Result<Out, Elapsed>>,
    {
        tokio::pin!(timeout_fut);
        tokio::select! {
            res = &mut timeout_fut => res,
            _ = sleep(Duration::from_secs(1)) => {
                tracing::warn!(
                    target: "emulator_database::lock",
                    database = %self.manager.database,
                    txn_id = self.txn_id.map(tracing::field::display),
                    session_id = self.session_id,
                    root_seq = self.root_seq(),
                    doc_name = %doc_ref,
                    lock_kind = kind,
                    wait_ms = start.elapsed().as_millis() as u64,
                    "waiting more than 1 second on a {kind} for {doc_ref}"
                );
                timeout_fut.await
            }
        }
    }

    pub(super) fn publish_seq(&self, seq: u64) {
        self.seq.store(seq, Ordering::Release);
    }

    pub(super) fn publish_state(&self, new_state: u8) {
        self.state.store(new_state, Ordering::Release);
    }

    pub(super) fn publish_abort(&self, new_state: u8, reason: AbortReason) {
        *self
            .abort_reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(reason);
        self.state.store(new_state, Ordering::Release);
    }

    pub(super) fn release_read_lease(&self, doc_ref: &DocumentRef) {
        let mut table = self
            .manager
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        table.release_read_lease(self.session_id, doc_ref);
    }

    pub(super) fn release_write_lease(&self, doc_ref: &DocumentRef) {
        let mut table = self
            .manager
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        table.release_write_lease(self.session_id, doc_ref);
    }

    pub(super) fn revoke_waiter(&self, wait_id: u64) {
        let mut table = self
            .manager
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        table.revoke_waiter(self.session_id, wait_id);
    }

    fn close_timed_out(&self, reason: AbortReason) {
        let mut table = self
            .manager
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        table.close_session(self.session_id, Some(reason), STATE_TIMED_OUT);
    }

    fn read_abort_reason(&self, fallback: AbortReason) -> AbortReason {
        self.abort_reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unwrap_or(fallback)
    }

    fn emit_wound_logs(&self, wounds: &[WoundEvent]) {
        for w in wounds {
            tracing::debug!(
                target: "emulator_database::lock",
                database = %self.manager.database,
                txn_id = w.victim_txn_id.map(tracing::field::display),
                session_id = w.victim_session_id,
                root_seq = w.victim_seq,
                wounder_txn_id = self.txn_id.map(tracing::field::display),
                wounder_seq = self.root_seq(),
                doc_name = %w.doc_ref,
                reason = %w.reason,
                "wounded conflicting lock holder"
            );
        }
    }
}

impl Drop for LockSession {
    fn drop(&mut self) {
        self.close();
    }
}

fn err_display(err: &CommitError) -> String {
    match err {
        CommitError::Aborted(reason) => reason.to_string(),
        CommitError::Other(other) => other.to_string(),
    }
}
