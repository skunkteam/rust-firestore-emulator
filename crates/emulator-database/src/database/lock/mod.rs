mod contention;
mod leases;
mod table;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use googleapis::google::protobuf::Timestamp;
use tokio::{
    sync::{Notify, oneshot},
    time::{Instant, sleep, sleep_until},
};

pub use self::contention::{
    ContendedDocumentSnapshot, ContentionEvent, ContentionEventKind, ContentionLockMode,
    ContentionSessionState, ContentionSnapshot, ContentionSseEvent, ContentionSubscriberGuard,
    ContentionSubscription, ContentionSummary, DatabaseContentionSnapshot, MAX_CONTENTION_HISTORY,
    MAX_SNAPSHOT_CONTENDED_DOCS, MAX_SNAPSHOT_DOC_PARTICIPANTS, ReaderSnapshot, SessionKind,
    WaiterSnapshot, WriterSnapshot,
};
pub(crate) use self::{
    contention::ContentionBroadcaster,
    leases::{CommitSeal, ReadLease, WriteLease},
    table::ReadObservation,
};
use self::{
    contention::{ContentionHistory, TimeoutRecord},
    leases::WaiterGuard,
    table::{
        AcquireOutcome, CloseOutcome, ExpiredSessionOutcome, LockTable, LockTableGarbage,
        WoundEvent,
    },
};
use crate::{
    GenericDatabaseError,
    database::{
        FirestoreDatabase,
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
pub(super) const STATE_EXPIRED: u8 = 6;

fn is_terminal_state(state: u8) -> bool {
    matches!(
        state,
        STATE_WOUNDED | STATE_TIMED_OUT | STATE_CLEARED | STATE_CLOSED | STATE_EXPIRED
    )
}

pub(super) fn transaction_closed() -> CommitError {
    CommitError::Other(GenericDatabaseError::invalid_argument("transaction closed"))
}

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
    Expired(AbortReason),
    Closed,
}

/// Shared lock-free status block held by both [`LockSession`] and `SessionRecord` so `LockTable`
/// can publish sequence and abort transitions without upgrading weak session handles under lock.
///
/// Intentionally has no `Drop` implementation.
#[derive(Debug)]
pub(super) struct SessionSharedState {
    seq: AtomicU64,
    state: AtomicU8,
    accepting_reads: AtomicBool,
    abort_reason: Mutex<Option<AbortReason>>,
}

impl SessionSharedState {
    fn new(initial_seq: u64) -> Self {
        Self {
            seq: AtomicU64::new(initial_seq),
            state: AtomicU8::new(STATE_ACTIVE),
            accepting_reads: AtomicBool::new(true),
            abort_reason: Mutex::new(None),
        }
    }

    pub(super) fn root_seq(&self) -> Option<u64> {
        let seq = self.seq.load(Ordering::Acquire);
        (1..u64::MAX).contains(&seq).then_some(seq)
    }

    pub(super) fn state_code(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    pub(super) fn is_accepting_reads(&self) -> bool {
        self.accepting_reads.load(Ordering::Acquire)
    }

    pub(super) fn mark_commit_started(&self) {
        self.accepting_reads.store(false, Ordering::Release);
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

    fn read_abort_reason(&self, fallback: AbortReason) -> AbortReason {
        self.abort_reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unwrap_or(fallback)
    }

    pub(super) fn abort_state(&self) -> SessionAbortState {
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
            STATE_EXPIRED => {
                SessionAbortState::Expired(self.read_abort_reason(AbortReason::TransactionExpired))
            }
            _ => SessionAbortState::Closed,
        }
    }
}

/// Database-scoped Wound-Wait lock manager coordinating read/write leases, priority wait queues,
/// and cross-document preemption without `unsafe` code or storage callbacks.
#[derive(Debug)]
pub(crate) struct LockManager {
    database: RootRef,
    timeouts: Timeouts,
    table: Mutex<LockTable>,
    history: Mutex<ContentionHistory>,
    broadcaster: Option<ContentionBroadcaster>,
    next_session_id: AtomicU64,
    next_seq: AtomicU64,
    cleared: AtomicBool,
    expiry_notify: Arc<Notify>,
    reaper: OnceLock<Weak<FirestoreDatabase>>,
}

impl LockManager {
    #[cfg(test)]
    pub(crate) fn new(database: RootRef, timeouts: Timeouts) -> Self {
        Self::new_inner(database, timeouts, None)
    }

    pub(crate) fn with_broadcaster(
        database: RootRef,
        timeouts: Timeouts,
        broadcaster: ContentionBroadcaster,
    ) -> Self {
        Self::new_inner(database, timeouts, Some(broadcaster))
    }

    fn new_inner(
        database: RootRef,
        timeouts: Timeouts,
        broadcaster: Option<ContentionBroadcaster>,
    ) -> Self {
        Self {
            database,
            timeouts,
            table: Mutex::new(LockTable::default()),
            history: Mutex::new(ContentionHistory::default()),
            broadcaster,
            next_session_id: AtomicU64::new(1),
            next_seq: AtomicU64::new(1),
            cleared: AtomicBool::new(false),
            expiry_notify: Arc::new(Notify::new()),
            reaper: OnceLock::new(),
        }
    }

    pub(crate) fn set_reaper(&self, db: Weak<FirestoreDatabase>) {
        drop(self.reaper.set(db));
    }

    pub(crate) fn expiry_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.expiry_notify)
    }

    pub(crate) fn contention_snapshot(&self) -> DatabaseContentionSnapshot {
        let now = Instant::now();
        let (contended_documents, total_contended_documents, truncated) = {
            let table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            table.contention_snapshot(now)
        };
        let (summary, recent_events) = {
            let history = self.history.lock().unwrap_or_else(PoisonError::into_inner);
            history.snapshot()
        };
        DatabaseContentionSnapshot {
            database: self.database.clone(),
            summary,
            contended_documents,
            total_contended_documents,
            truncated,
            recent_events,
        }
    }

    fn update_history_and_notify(&self, update: impl FnOnce(&mut ContentionHistory) -> bool) {
        if self.is_cleared() {
            return;
        }
        let mut history = self.history.lock().unwrap_or_else(PoisonError::into_inner);
        if history.cleared {
            return;
        }
        if update(&mut history)
            && let Some(broadcaster) = &self.broadcaster
        {
            broadcaster.send_changed_if_subscribed(&self.database);
        }
    }

    pub(crate) fn clear_contention_history(&self) {
        self.update_history_and_notify(ContentionHistory::clear);
    }

    pub(crate) fn close_expired_sessions(&self, sessions: &[Arc<LockSession>]) {
        if !sessions
            .iter()
            .any(|s| s.shared.state_code() == STATE_ACTIVE)
        {
            return;
        }
        let mut garbage = LockTableGarbage::default();
        let mut outcomes = Vec::new();
        {
            let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            for session in sessions {
                if session.shared.state_code() == STATE_ACTIVE
                    && let Some(outcome) = table.close_expired(session.session_id, &mut garbage)
                {
                    outcomes.push(outcome);
                }
            }
        }
        drop(garbage);
        self.record_expired_outcomes(&outcomes);
    }

    fn record_expired_outcomes(&self, outcomes: &[ExpiredSessionOutcome]) {
        if self.is_cleared() || outcomes.is_empty() {
            return;
        }
        if outcomes.iter().any(|o| o.had_resources) {
            self.update_history_and_notify(|history| {
                for outcome in outcomes {
                    if outcome.had_resources {
                        history.record_expired(outcome);
                    }
                }
                true
            });
        } else if outcomes.iter().any(|o| o.notify_state_change) {
            self.notify_contention_changed();
        }
    }

    fn notify_contention_changed(&self) {
        if self
            .broadcaster
            .as_ref()
            .is_some_and(ContentionBroadcaster::has_subscribers)
        {
            self.update_history_and_notify(|_| true);
        }
    }

    fn record_wounds_and_maybe_notify(
        &self,
        wounds: &[WoundEvent],
        wounder_session_id: u64,
        wounder_txn_id: Option<TransactionId>,
        wounder_kind: SessionKind,
        wounder_seq: Option<u64>,
        notify_state_change: bool,
    ) {
        if wounds.is_empty() {
            if notify_state_change {
                self.notify_contention_changed();
            }
            return;
        }
        self.update_history_and_notify(|history| {
            for wound in wounds {
                history.record_wound(
                    wound,
                    wounder_session_id,
                    wounder_txn_id,
                    wounder_kind,
                    wounder_seq,
                );
            }
            true
        });
    }

    fn record_timeout_and_notify(&self, record: TimeoutRecord) {
        self.update_history_and_notify(|history| {
            history.record_timeout(record);
            true
        });
    }

    pub(crate) fn record_upgrade_abort(
        &self,
        session_id: u64,
        txn_id: Option<TransactionId>,
        session_kind: SessionKind,
        seq: Option<u64>,
        doc_ref: &DocumentRef,
    ) {
        self.update_history_and_notify(|history| {
            history.record_upgrade_abort(
                session_id,
                txn_id,
                session_kind,
                seq,
                doc_ref.to_string(),
            );
            true
        });
    }

    pub(crate) fn record_write_batch_retry(
        &self,
        session_id: u64,
        seq: Option<u64>,
        wounder_txn_id: Option<TransactionId>,
        wounder_seq: u64,
        attempt: u32,
    ) {
        self.update_history_and_notify(|history| {
            history.record_write_batch_retry(session_id, seq, wounder_txn_id, wounder_seq, attempt);
            true
        });
    }

    pub(crate) fn record_write_batch_timeout(
        &self,
        session_id: u64,
        seq: Option<u64>,
        wait_ms: u64,
        attempt: u32,
    ) {
        self.record_timeout_and_notify(TimeoutRecord {
            kind: ContentionEventKind::WriteTimeout,
            session_id,
            txn_id: None,
            session_kind: SessionKind::WriteBatch,
            seq,
            doc_name: None,
            wait_ms,
            attempt: (attempt > 0).then_some(attempt),
        });
    }

    fn reap_and_next_expiry(&self) -> Option<Instant> {
        self.reaper
            .get()
            .and_then(Weak::upgrade)
            .and_then(|db| db.transactions.reap_and_next_pessimistic_expiry())
    }

    pub(crate) fn new_session(
        self: &Arc<Self>,
        txn_id: Option<TransactionId>,
        kind: SessionKind,
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

        let shared = Arc::new(SessionSharedState::new(initial_seq));
        if self.is_cleared() {
            shared.publish_abort(STATE_CLEARED, AbortReason::DatabaseCleared);
        } else {
            let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            table.register_session(
                session_id,
                Arc::clone(&shared),
                txn_id,
                kind,
                initial_seq,
                can_wound,
                woundable,
            );
        }

        Arc::new(LockSession {
            manager: Arc::clone(self),
            session_id,
            txn_id,
            kind,
            shared,
        })
    }

    pub(crate) fn abort_all(&self, reason: AbortReason) {
        if reason == AbortReason::DatabaseCleared {
            self.cleared.store(true, Ordering::Release);
        } else if self.is_cleared() {
            return;
        }
        let mut garbage = LockTableGarbage::default();
        let had_contended = {
            let mut table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
            table.abort_all(reason, &mut garbage)
        };
        drop(garbage);
        if reason == AbortReason::DatabaseCleared {
            let mut history = self.history.lock().unwrap_or_else(PoisonError::into_inner);
            let _unused = history.clear();
            history.cleared = true;
        } else if had_contended && !self.is_cleared() {
            self.notify_contention_changed();
        }
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

    #[cfg(test)]
    pub(crate) fn table_counts(&self) -> (usize, usize) {
        let table = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        table.assert_contended_docs_consistent();
        table.counts()
    }
}

#[derive(Debug)]
struct BatchWaiter {
    doc_ref: DocumentRef,
    guard:   Option<WaiterGuard>,
    rx:      oneshot::Receiver<Result<(), CommitError>>,
}

/// Per-transaction or single-operation lock session handle.
#[derive(Debug)]
pub(crate) struct LockSession {
    manager: Arc<LockManager>,
    session_id: u64,
    txn_id: Option<TransactionId>,
    kind: SessionKind,
    shared: Arc<SessionSharedState>,
}

impl LockSession {
    pub(crate) fn session_id(&self) -> u64 {
        self.session_id
    }

    pub(crate) fn txn_id(&self) -> Option<TransactionId> {
        self.txn_id
    }

    pub(crate) fn kind(&self) -> SessionKind {
        self.kind
    }

    pub(crate) fn root_seq(&self) -> Option<u64> {
        self.shared.root_seq()
    }

    #[cfg(test)]
    pub(crate) fn is_wounded(&self) -> bool {
        self.shared.state_code() == STATE_WOUNDED
    }

    /// Rejects new and in-flight reads once commit reservation begins, while keeping
    /// `STATE_ACTIVE` and `woundable = true` until [`Self::seal_for_commit`].
    pub(crate) fn mark_commit_started(&self) {
        self.shared.mark_commit_started();
    }

    pub(crate) fn abort_state(&self) -> SessionAbortState {
        self.shared.abort_state()
    }

    pub(crate) fn check_active(&self) -> Result<(), CommitError> {
        match self.abort_state() {
            SessionAbortState::Active => Ok(()),
            SessionAbortState::Wounded(reason)
            | SessionAbortState::TimedOut(reason)
            | SessionAbortState::Cleared(reason)
            | SessionAbortState::Expired(reason) => Err(CommitError::Aborted(reason)),
            SessionAbortState::Committing | SessionAbortState::Closed => Err(transaction_closed()),
        }
    }

    pub(crate) fn check_read_active(&self) -> Result<(), CommitError> {
        self.check_active()?;
        if !self.shared.is_accepting_reads() {
            return Err(transaction_closed());
        }
        Ok(())
    }

    pub(crate) fn check_committing_active(&self) -> Result<(), CommitError> {
        match self.abort_state() {
            SessionAbortState::Active | SessionAbortState::Committing => Ok(()),
            SessionAbortState::Wounded(reason)
            | SessionAbortState::TimedOut(reason)
            | SessionAbortState::Cleared(reason)
            | SessionAbortState::Expired(reason) => Err(CommitError::Aborted(reason)),
            SessionAbortState::Closed => Err(transaction_closed()),
        }
    }

    pub(crate) fn retain_read(
        &self,
        lease: ReadLease,
        observed_update_time: Option<Timestamp>,
    ) -> Result<(), CommitError> {
        if !std::ptr::eq(Arc::as_ptr(lease.session()), self) {
            drop(lease);
            return Err(CommitError::Other(GenericDatabaseError::internal(
                "read lease does not belong to lock session",
            )));
        }
        if let Err(err) = lease.check_active() {
            drop(lease);
            return Err(err);
        }
        let res = {
            let mut table = self.lock_table();
            table.retain_read(
                self.session_id,
                &self.shared,
                lease.doc_ref(),
                observed_update_time,
            )
        };
        match res {
            Ok(notify) => {
                lease.disarm();
                if notify {
                    self.manager.notify_contention_changed();
                }
                Ok(())
            }
            Err(err) => {
                drop(lease);
                Err(err)
            }
        }
    }

    pub(crate) fn retain_reads(
        &self,
        leases: impl IntoIterator<Item = (ReadLease, Option<Timestamp>)>,
    ) -> Result<(), CommitError> {
        let entries: Vec<(ReadLease, Option<Timestamp>)> = leases.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }
        for (lease, _) in &entries {
            if !std::ptr::eq(Arc::as_ptr(lease.session()), self) {
                drop(entries);
                return Err(CommitError::Other(GenericDatabaseError::internal(
                    "read lease does not belong to lock session",
                )));
            }
        }
        if let Err(err) = self.check_read_active() {
            drop(entries);
            return Err(err);
        }

        let res = {
            let mut table = self.lock_table();
            table.retain_reads(self.session_id, &self.shared, &entries)
        };
        match res {
            Ok(notify) => {
                for (lease, _) in entries {
                    lease.disarm();
                }
                if notify {
                    self.manager.notify_contention_changed();
                }
                Ok(())
            }
            Err(err) => {
                drop(entries);
                Err(err)
            }
        }
    }

    pub(crate) fn close(&self) {
        if is_terminal_state(self.shared.state_code()) {
            return;
        }
        let mut garbage = LockTableGarbage::default();
        let outcome = {
            let mut table = self.lock_table();
            table.close_session(self.session_id, None, STATE_CLOSED, &mut garbage)
        };
        drop(garbage);
        if outcome.notify_state_change {
            self.manager.notify_contention_changed();
        }
    }

    #[cfg(test)]
    pub(crate) fn close_expired(&self) {
        if self.shared.state_code() != STATE_ACTIVE {
            return;
        }
        let mut garbage = LockTableGarbage::default();
        let outcome = {
            let mut table = self.lock_table();
            table.close_expired(self.session_id, &mut garbage)
        };
        drop(garbage);
        if let Some(outcome) = outcome {
            self.manager
                .record_expired_outcomes(std::slice::from_ref(&outcome));
        }
    }

    pub(crate) fn seal_for_commit(
        self: Arc<Self>,
        writes: BTreeMap<DocumentRef, WriteLease>,
    ) -> Result<CommitSeal, CommitError> {
        for (doc_ref, lease) in &writes {
            if !std::ptr::eq(Arc::as_ptr(lease.session()), Arc::as_ptr(&self))
                || lease.doc_ref() != doc_ref
            {
                drop(writes);
                return Err(CommitError::Other(GenericDatabaseError::internal(
                    "write lease does not match sealing session or target document",
                )));
            }
        }
        if let Err(err) = self.check_active() {
            drop(writes);
            return Err(err);
        }

        let mut garbage = LockTableGarbage::default();
        let res = {
            let mut table = self.lock_table();
            table.seal_for_commit(self.session_id, &self.shared, &writes, &mut garbage)
        };
        drop(garbage);
        let notify = match res {
            Ok(notify) => notify,
            Err(err) => {
                drop(writes);
                return Err(err);
            }
        };
        if notify {
            self.manager.notify_contention_changed();
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

        Ok(CommitSeal::new(self, writes))
    }

    pub(crate) fn commit_empty(&self) -> Result<Timestamp, CommitError> {
        self.check_active()?;

        let mut garbage = LockTableGarbage::default();
        let res = {
            let mut table = self.lock_table();
            table.commit_empty(self.session_id, &self.shared, &mut garbage)
        };
        drop(garbage);
        let (commit_time, notify) = res?;
        if notify {
            self.manager.notify_contention_changed();
        }

        tracing::debug!(
            target: "emulator_database::lock",
            database = %self.manager.database,
            txn_id = self.txn_id.map(tracing::field::display),
            session_id = self.session_id,
            root_seq = self.root_seq(),
            "committed empty session"
        );

        Ok(commit_time)
    }

    pub(crate) async fn acquire_read(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
    ) -> Result<ReadLease, CommitError> {
        self.check_read_active()?;
        let mut garbage = LockTableGarbage::default();
        let outcome = {
            let mut table = self.lock_table();
            table.try_acquire_read(
                self.session_id,
                &self.shared,
                doc_ref,
                &self.manager.next_seq,
                &mut garbage,
            )
        };
        drop(garbage);
        match outcome? {
            AcquireOutcome::Granted {
                wounds,
                notify_contention,
                ..
            } => {
                self.emit_wound_logs(&wounds);
                self.manager.record_wounds_and_maybe_notify(
                    &wounds,
                    self.session_id,
                    self.txn_id,
                    self.kind,
                    self.root_seq(),
                    notify_contention,
                );
                Ok(ReadLease::new(Arc::clone(self), doc_ref.clone()))
            }
            AcquireOutcome::Wait {
                wait_id,
                rx,
                wounds,
                notify_contention,
                ..
            } => {
                self.emit_wound_logs(&wounds);
                self.manager.record_wounds_and_maybe_notify(
                    &wounds,
                    self.session_id,
                    self.txn_id,
                    self.kind,
                    self.root_seq(),
                    notify_contention,
                );
                self.await_single_waiter(
                    doc_ref,
                    "read lock",
                    wait_id,
                    rx,
                    self.manager.timeouts.read,
                    AbortReason::ReadTimeout,
                    true,
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
        self.check_active()?;
        let mut garbage = LockTableGarbage::default();
        let outcome = {
            let mut table = self.lock_table();
            table.try_acquire_or_upgrade_write(
                self.session_id,
                &self.shared,
                doc_ref,
                &self.manager.next_seq,
                &mut garbage,
            )
        };
        drop(garbage);
        match outcome? {
            AcquireOutcome::Granted {
                observation,
                wounds,
                notify_contention,
            } => {
                self.emit_wound_logs(&wounds);
                self.manager.record_wounds_and_maybe_notify(
                    &wounds,
                    self.session_id,
                    self.txn_id,
                    self.kind,
                    self.root_seq(),
                    notify_contention,
                );
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
                notify_contention,
            } => {
                self.emit_wound_logs(&wounds);
                self.manager.record_wounds_and_maybe_notify(
                    &wounds,
                    self.session_id,
                    self.txn_id,
                    self.kind,
                    self.root_seq(),
                    notify_contention,
                );
                self.await_single_waiter(
                    doc_ref,
                    "write lock",
                    wait_id,
                    rx,
                    self.manager.timeouts.write,
                    AbortReason::WriteTimeout,
                    false,
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

    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) async fn acquire_writes_all(
        self: &Arc<Self>,
        docs: &BTreeSet<DocumentRef>,
        deadline: Instant,
    ) -> Result<BTreeMap<DocumentRef, WriteLease>, CommitError> {
        self.acquire_writes_all_since(docs, Instant::now(), deadline, None)
            .await
    }

    pub(crate) async fn acquire_writes_all_since(
        self: &Arc<Self>,
        docs: &BTreeSet<DocumentRef>,
        batch_start: Instant,
        deadline: Instant,
        attempt: Option<u32>,
    ) -> Result<BTreeMap<DocumentRef, WriteLease>, CommitError> {
        self.check_active()?;
        if docs.is_empty() {
            return Ok(BTreeMap::new());
        }

        let mut garbage = LockTableGarbage::default();
        let batch = {
            let mut table = self.lock_table();
            table.try_acquire_or_queue_writes(
                self.session_id,
                &self.shared,
                docs,
                &self.manager.next_seq,
                &mut garbage,
            )
        };
        drop(garbage);
        let batch = batch?;
        self.emit_wound_logs(&batch.wounds);
        self.manager.record_wounds_and_maybe_notify(
            &batch.wounds,
            self.session_id,
            self.txn_id,
            self.kind,
            self.root_seq(),
            batch.notify_contention,
        );

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

        let start = Instant::now();
        let mut waiters: Vec<BatchWaiter> = batch
            .pending
            .into_iter()
            .map(|pending| BatchWaiter {
                guard:   Some(WaiterGuard::new(Arc::clone(self), pending.wait_id)),
                doc_ref: pending.doc_ref,
                rx:      pending.rx,
            })
            .collect();

        let slow_warn = sleep(Duration::from_secs(1));
        tokio::pin!(slow_warn);
        let mut warned = false;
        let deadline_fut = sleep_until(deadline);
        tokio::pin!(deadline_fut);

        let mut batch_err: Option<CommitError> = None;
        let mut timed_out = false;
        let mut last_waited_doc = waiters[0].doc_ref.clone();

        for waiter in &mut waiters {
            last_waited_doc = waiter.doc_ref.clone();
            let Some(rx_res) = self
                .await_waiter_rx(
                    &mut waiter.rx,
                    &waiter.doc_ref,
                    "write lock",
                    start,
                    deadline,
                    deadline_fut.as_mut(),
                    slow_warn.as_mut(),
                    &mut warned,
                )
                .await
            else {
                timed_out = true;
                break;
            };
            let wait_id = waiter
                .guard
                .as_ref()
                .expect("batch waiter guard present")
                .wait_id();
            match rx_res {
                Ok(Ok(())) => {
                    let (consumed, notify) = {
                        let mut table = self.lock_table();
                        table.consume_wait_grant(self.session_id, &self.shared, wait_id)
                    };
                    if let Some(guard) = waiter.guard.take() {
                        guard.disarm();
                    }
                    if notify {
                        self.manager.notify_contention_changed();
                    }
                    match consumed {
                        Ok(()) => {
                            let doc_ref = waiter.doc_ref.clone();
                            leases.insert(
                                doc_ref.clone(),
                                WriteLease::new(Arc::clone(self), doc_ref, Timestamp::now()),
                            );
                        }
                        Err(err) => {
                            batch_err = Some(err);
                            break;
                        }
                    }
                }
                Ok(Err(err)) => {
                    if let Some(guard) = waiter.guard.take() {
                        guard.disarm();
                    }
                    batch_err = Some(err);
                    break;
                }
                Err(_) => {
                    if let Some(guard) = waiter.guard.take() {
                        guard.disarm();
                    }
                    batch_err = Some(match self.check_active() {
                        Err(err) => err,
                        Ok(()) => CommitError::Aborted(AbortReason::DatabaseDropped),
                    });
                    break;
                }
            }
        }

        if timed_out {
            // Strip the session in one lock first, then disarm remaining guards/leases so teardown
            // never triggers T + P individual Mutex<LockTable> re-acquisitions.
            let close_outcome = self.close_timed_out(AbortReason::WriteTimeout);
            Self::disarm_batch(&mut waiters, &mut leases);
            let final_err = match self.check_active() {
                Err(err) => err,
                Ok(()) => CommitError::Aborted(AbortReason::WriteTimeout),
            };
            let wait_ms = u64::try_from(batch_start.elapsed().as_millis()).unwrap_or(u64::MAX);
            if close_outcome.actually_closed {
                self.manager.record_timeout_and_notify(TimeoutRecord {
                    kind: ContentionEventKind::WriteTimeout,
                    session_id: self.session_id,
                    txn_id: self.txn_id,
                    session_kind: self.kind,
                    seq: self.root_seq(),
                    doc_name: Some(last_waited_doc.to_string()),
                    wait_ms,
                    attempt,
                });
            }
            tracing::debug!(
                target: "emulator_database::lock",
                database = %self.manager.database,
                txn_id = self.txn_id.map(tracing::field::display),
                session_id = self.session_id,
                root_seq = self.root_seq(),
                doc_name = %last_waited_doc,
                wait_ms,
                reason = %err_display(&final_err),
                "batch write lock timed out"
            );
            return Err(final_err);
        }

        if let Some(err) = batch_err {
            if self.shared.state_code() != STATE_ACTIVE {
                Self::disarm_batch(&mut waiters, &mut leases);
            }
            return Err(err);
        }

        tracing::debug!(
            target: "emulator_database::lock",
            database = %self.manager.database,
            txn_id = self.txn_id.map(tracing::field::display),
            session_id = self.session_id,
            root_seq = self.root_seq(),
            doc_name = %last_waited_doc,
            wait_ms = start.elapsed().as_millis() as u64,
            "acquired batch write locks after wait"
        );
        Ok(leases)
    }

    #[allow(clippy::too_many_arguments)]
    async fn await_single_waiter(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
        kind: &'static str,
        wait_id: u64,
        mut rx: oneshot::Receiver<Result<(), CommitError>>,
        timeout_duration: Duration,
        timeout_reason: AbortReason,
        is_read: bool,
    ) -> Result<(), CommitError> {
        let start = Instant::now();
        let deadline = start + timeout_duration;
        let guard = WaiterGuard::new(Arc::clone(self), wait_id);
        let slow_warn = sleep(Duration::from_secs(1));
        tokio::pin!(slow_warn);
        let mut warned = false;
        let deadline_fut = sleep_until(deadline);
        tokio::pin!(deadline_fut);
        let awaited = self
            .await_waiter_rx(
                &mut rx,
                doc_ref,
                kind,
                start,
                deadline,
                deadline_fut,
                slow_warn,
                &mut warned,
            )
            .await;

        match awaited {
            Some(Ok(Ok(()))) => {
                let (consumed, notify) = {
                    let mut table = self.lock_table();
                    table.consume_wait_grant(self.session_id, &self.shared, guard.wait_id())
                };
                guard.disarm();
                if notify {
                    self.manager.notify_contention_changed();
                }
                match consumed {
                    Ok(()) => {
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
                    Err(err) => Err(err),
                }
            }
            Some(Ok(Err(err))) => {
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
            Some(Err(_)) => {
                guard.disarm();
                let check = if is_read {
                    self.check_read_active()
                } else {
                    self.check_active()
                };
                Err(match check {
                    Err(err) => err,
                    Ok(()) => CommitError::Aborted(AbortReason::DatabaseDropped),
                })
            }
            None => {
                guard.disarm();
                let close_outcome = self.close_timed_out(timeout_reason);
                let final_err = match self.check_active() {
                    Err(err) => err,
                    Ok(()) => CommitError::Aborted(timeout_reason),
                };
                let wait_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                if close_outcome.actually_closed {
                    let event_kind = if is_read {
                        ContentionEventKind::ReadTimeout
                    } else {
                        ContentionEventKind::WriteTimeout
                    };
                    self.manager.record_timeout_and_notify(TimeoutRecord {
                        kind: event_kind,
                        session_id: self.session_id,
                        txn_id: self.txn_id,
                        session_kind: self.kind,
                        seq: self.root_seq(),
                        doc_name: Some(doc_ref.to_string()),
                        wait_ms,
                        attempt: None,
                    });
                }
                tracing::debug!(
                    target: "emulator_database::lock",
                    database = %self.manager.database,
                    txn_id = self.txn_id.map(tracing::field::display),
                    session_id = self.session_id,
                    root_seq = self.root_seq(),
                    doc_name = %doc_ref,
                    lock_kind = kind,
                    wait_ms,
                    reason = %err_display(&final_err),
                    "lock wait timed out"
                );
                Err(final_err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn await_waiter_rx(
        &self,
        rx: &mut oneshot::Receiver<Result<(), CommitError>>,
        doc_ref: &DocumentRef,
        kind: &'static str,
        start: Instant,
        deadline: Instant,
        mut deadline_fut: Pin<&mut tokio::time::Sleep>,
        mut slow_warn: Pin<&mut tokio::time::Sleep>,
        warned: &mut bool,
    ) -> Option<Result<Result<(), CommitError>, oneshot::error::RecvError>> {
        loop {
            let notified = self.manager.expiry_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let next_expiry = self.manager.reap_and_next_expiry();
            let expiry_sleep = next_expiry.filter(|&exp| exp < deadline).map(sleep_until);
            let has_expiry = expiry_sleep.is_some();
            tokio::pin!(expiry_sleep);

            let wake_slow = !*warned;
            enum WaitTick {
                Rx(Result<Result<(), CommitError>, oneshot::error::RecvError>),
                TimedOut,
                Expiry,
                SlowWarn,
            }

            let tick = tokio::select! {
                biased;
                res = &mut *rx => WaitTick::Rx(res),
                _ = &mut deadline_fut => WaitTick::TimedOut,
                _ = async {
                    if let Some(fut) = expiry_sleep.as_mut().as_pin_mut() {
                        fut.await;
                    }
                }, if has_expiry => WaitTick::Expiry,
                _ = &mut notified => WaitTick::Expiry,
                _ = &mut slow_warn, if wake_slow => WaitTick::SlowWarn,
            };

            match tick {
                WaitTick::Rx(res) => return Some(res),
                WaitTick::TimedOut => {
                    let _ = self.manager.reap_and_next_expiry();
                    match rx.try_recv() {
                        Ok(res) => return Some(Ok(res)),
                        Err(oneshot::error::TryRecvError::Closed) => return Some(rx.await),
                        Err(oneshot::error::TryRecvError::Empty) => return None,
                    }
                }
                WaitTick::Expiry => {}
                WaitTick::SlowWarn => {
                    *warned = true;
                    self.emit_slow_wait_warning(doc_ref, kind, start);
                }
            }
        }
    }

    fn emit_slow_wait_warning(&self, doc_ref: &DocumentRef, kind: &'static str, start: Instant) {
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
    }

    pub(super) fn release_read_lease(&self, doc_ref: &DocumentRef) {
        if is_terminal_state(self.shared.state_code()) {
            return;
        }
        let notify = {
            self.lock_table()
                .release_read_lease(self.session_id, doc_ref)
        };
        if notify {
            self.manager.notify_contention_changed();
        }
    }

    pub(super) fn release_write_lease(&self, doc_ref: &DocumentRef) {
        if is_terminal_state(self.shared.state_code()) {
            return;
        }
        let notify = {
            self.lock_table()
                .release_write_lease(self.session_id, doc_ref)
        };
        if notify {
            self.manager.notify_contention_changed();
        }
    }

    pub(super) fn revoke_waiter(&self, wait_id: u64) {
        if is_terminal_state(self.shared.state_code()) {
            return;
        }
        let notify = { self.lock_table().revoke_waiter(self.session_id, wait_id) };
        if notify {
            self.manager.notify_contention_changed();
        }
    }

    fn close_timed_out(&self, reason: AbortReason) -> CloseOutcome {
        if self.shared.state_code() != STATE_ACTIVE {
            return CloseOutcome {
                actually_closed:     false,
                notify_state_change: false,
            };
        }
        let mut garbage = LockTableGarbage::default();
        let outcome = {
            let mut table = self.lock_table();
            table.close_session(self.session_id, Some(reason), STATE_TIMED_OUT, &mut garbage)
        };
        drop(garbage);
        outcome
    }

    fn lock_table(&self) -> std::sync::MutexGuard<'_, LockTable> {
        self.manager
            .table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn disarm_batch(waiters: &mut [BatchWaiter], leases: &mut BTreeMap<DocumentRef, WriteLease>) {
        for waiter in waiters {
            if let Some(guard) = waiter.guard.take() {
                guard.disarm();
            }
        }
        for lease in leases.values_mut() {
            lease.disarm();
        }
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
