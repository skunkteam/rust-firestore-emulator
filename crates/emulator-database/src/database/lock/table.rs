use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::oneshot;

use super::{
    ReadLease, STATE_CLEARED, STATE_COMMITTING, STATE_EXPIRED, STATE_TIMED_OUT, STATE_WOUNDED,
    SessionAbortState, SessionSharedState, WriteLease,
    contention::{
        ContendedDocumentSnapshot, ContentionSessionState, MAX_SNAPSHOT_CONTENDED_DOCS,
        MAX_SNAPSHOT_DOC_PARTICIPANTS, ReaderSnapshot, SessionKind, WaiterSnapshot, WriterSnapshot,
    },
    transaction_closed,
};
use crate::{
    GenericDatabaseError,
    database::{reference::DocumentRef, transaction::TransactionId},
    error::{AbortReason, CommitError},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReadObservation {
    NeverRead,
    Observed(Option<Timestamp>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LockMode {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SessionState {
    Active,
    Committing,
    Wounded,
    TimedOut,
    Expired,
}

#[derive(Clone, Copy, Debug)]
enum CloseDisposition {
    Normal(u8),
    TimedOut(AbortReason),
    Expired,
}

type ParkedWaiterMap = HashMap<u64, (DocumentRef, (u64, u64))>;

#[derive(Debug, Clone)]
pub(super) struct WoundEvent {
    pub(super) victim_session_id: u64,
    pub(super) victim_txn_id: Option<TransactionId>,
    pub(super) victim_kind: SessionKind,
    pub(super) victim_seq: u64,
    pub(super) doc_ref: DocumentRef,
    pub(super) reason: AbortReason,
}

#[derive(Debug, Clone)]
pub(super) struct ExpiredSessionOutcome {
    pub(super) session_id: u64,
    pub(super) txn_id: Option<TransactionId>,
    pub(super) seq: Option<u64>,
    pub(super) kind: SessionKind,
    pub(super) doc_ref: Option<DocumentRef>,
    pub(super) had_resources: bool,
    pub(super) notify_state_change: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CloseOutcome {
    pub(super) actually_closed:     bool,
    pub(super) notify_state_change: bool,
}

#[derive(Debug)]
struct Waiter {
    wait_id: u64,
    session_id: u64,
    mode: LockMode,
    enqueued_at: tokio::time::Instant,
    tx: oneshot::Sender<Result<(), CommitError>>,
}

#[derive(Debug)]
struct ReadHold {
    active_leases: usize,
    retained:      bool,
    observation:   Option<Option<Timestamp>>,
}

#[derive(Debug, Default)]
struct DocLockState {
    readers: HashMap<u64, ReadHold>,
    writer:  Option<u64>,
    waiters: BTreeMap<(u64, u64), Waiter>,
}

impl DocLockState {
    fn is_empty(&self) -> bool {
        self.readers.is_empty() && self.writer.is_none() && self.waiters.is_empty()
    }
}

#[derive(Debug)]
struct SessionRecord {
    shared: Arc<SessionSharedState>,
    txn_id: Option<TransactionId>,
    seq: u64,
    can_wound: bool,
    woundable: bool,
    kind: SessionKind,
    state: SessionState,
    abort_reason: Option<AbortReason>,
    held_reads: HashSet<DocumentRef>,
    held_writes: HashSet<DocumentRef>,
    parked_waiters: ParkedWaiterMap,
    promoted_grants: HashMap<u64, (DocumentRef, LockMode)>,
}

/// Deferred heap allocations collected while holding `Mutex<LockTable>` so deallocations run
/// strictly after the mutex guard is released.
#[derive(Debug, Default)]
pub(super) struct LockTableGarbage {
    sessions: HashMap<u64, SessionRecord>,
    docs: HashMap<DocumentRef, DocLockState>,
    contended_docs: BTreeSet<DocumentRef>,
    records: Vec<SessionRecord>,
    doc_sets: Vec<HashSet<DocumentRef>>,
    doc_vecs: Vec<Vec<DocumentRef>>,
    waiter_maps: Vec<ParkedWaiterMap>,
    grant_maps: Vec<HashMap<u64, (DocumentRef, LockMode)>>,
}

#[derive(Debug)]
pub(super) enum AcquireOutcome {
    Granted {
        observation: ReadObservation,
        wounds: Vec<WoundEvent>,
        notify_contention: bool,
    },
    Wait {
        wait_id: u64,
        rx: oneshot::Receiver<Result<(), CommitError>>,
        observation: ReadObservation,
        wounds: Vec<WoundEvent>,
        notify_contention: bool,
    },
}

#[derive(Debug)]
pub(super) struct PendingWait {
    pub(super) doc_ref: DocumentRef,
    pub(super) wait_id: u64,
    pub(super) rx:      oneshot::Receiver<Result<(), CommitError>>,
}

#[derive(Debug)]
pub(super) struct BatchAcquireOutcome {
    pub(super) granted_docs: Vec<DocumentRef>,
    pub(super) pending: Vec<PendingWait>,
    pub(super) wounds: Vec<WoundEvent>,
    pub(super) notify_contention: bool,
}

#[derive(Debug, Default)]
pub(super) struct LockTable {
    docs: HashMap<DocumentRef, DocLockState>,
    contended_docs: BTreeSet<DocumentRef>,
    sessions: HashMap<u64, SessionRecord>,
    next_arrival: u64,
    next_wait_id: u64,
    cleared: bool,
}

impl LockTable {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn register_session(
        &mut self,
        session_id: u64,
        shared: Arc<SessionSharedState>,
        txn_id: Option<TransactionId>,
        kind: SessionKind,
        seq: u64,
        can_wound: bool,
        woundable: bool,
    ) {
        if self.cleared {
            shared.publish_abort(STATE_CLEARED, AbortReason::DatabaseCleared);
            return;
        }
        self.sessions.insert(
            session_id,
            SessionRecord {
                shared,
                txn_id,
                seq,
                can_wound,
                woundable,
                kind,
                state: SessionState::Active,
                abort_reason: None,
                held_reads: HashSet::new(),
                held_writes: HashSet::new(),
                parked_waiters: HashMap::new(),
                promoted_grants: HashMap::new(),
            },
        );
    }

    pub(super) fn try_acquire_read(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        doc_ref: &DocumentRef,
        alloc_seq: &AtomicU64,
        garbage: &mut LockTableGarbage,
    ) -> Result<AcquireOutcome, CommitError> {
        let (txn_id, can_wound) = {
            let rec = self.active_session_mut(session_id, shared)?;
            if !rec.shared.is_accepting_reads() {
                return Err(transaction_closed());
            }
            (rec.txn_id, rec.can_wound)
        };
        let (already_reader, already_writer) = self.docs.get(doc_ref).map_or((false, false), |d| {
            (
                d.readers.contains_key(&session_id),
                d.writer == Some(session_id),
            )
        });

        if already_writer {
            return Err(transaction_closed());
        }

        if already_reader {
            let mut notify_contention = false;
            if let Some(doc) = self.docs.get_mut(doc_ref)
                && let Some(hold) = doc.readers.get_mut(&session_id)
            {
                hold.active_leases += 1;
                notify_contention = !doc.waiters.is_empty();
            }
            return Ok(AcquireOutcome::Granted {
                observation: ReadObservation::NeverRead,
                wounds: Vec::new(),
                notify_contention,
            });
        }

        let seq = self.ensure_session_seq(session_id, alloc_seq);
        let wounds = if can_wound {
            self.wound_and_wake_others(
                &[(doc_ref, LockMode::Read)],
                session_id,
                seq,
                txn_id,
                garbage,
            )
        } else {
            Vec::new()
        };

        let arrival = self.next_arrival_order();
        let can_grant = self.docs.get(doc_ref).is_none_or(|d| {
            d.writer.is_none()
                && !d
                    .waiters
                    .range(..(seq, arrival))
                    .any(|(_, w)| w.mode == LockMode::Write)
        });

        if can_grant {
            let doc = self.docs.entry(doc_ref.clone()).or_default();
            doc.readers.insert(
                session_id,
                ReadHold {
                    active_leases: 1,
                    retained:      false,
                    observation:   None,
                },
            );
            let has_waiters = !doc.waiters.is_empty();
            let should_wake = !wounds.is_empty() && has_waiters;
            let mut notify_contention = !wounds.is_empty() || has_waiters;
            self.sessions
                .get_mut(&session_id)
                .expect("session registered")
                .held_reads
                .insert(doc_ref.clone());
            if should_wake && self.wake_waiters(doc_ref) {
                notify_contention = true;
            }
            return Ok(AcquireOutcome::Granted {
                observation: ReadObservation::NeverRead,
                wounds,
                notify_contention,
            });
        }

        let (wait_id, rx) = self.park_waiter(session_id, doc_ref, seq, arrival, LockMode::Read);
        if !wounds.is_empty() && self.docs.get(doc_ref).is_some_and(|d| d.writer.is_none()) {
            self.wake_waiters(doc_ref);
        }

        Ok(AcquireOutcome::Wait {
            wait_id,
            rx,
            observation: ReadObservation::NeverRead,
            wounds,
            notify_contention: true,
        })
    }

    pub(super) fn try_acquire_or_upgrade_write(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        doc_ref: &DocumentRef,
        alloc_seq: &AtomicU64,
        garbage: &mut LockTableGarbage,
    ) -> Result<AcquireOutcome, CommitError> {
        let (txn_id, can_wound) = {
            let rec = self.active_session_mut(session_id, shared)?;
            (rec.txn_id, rec.can_wound)
        };

        let (already_writer, observation) =
            self.docs
                .get(doc_ref)
                .map_or((false, ReadObservation::NeverRead), |d| {
                    let obs = d
                        .readers
                        .get(&session_id)
                        .and_then(|hold| hold.observation)
                        .map_or(ReadObservation::NeverRead, ReadObservation::Observed);
                    (d.writer == Some(session_id), obs)
                });
        if already_writer {
            return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                "duplicate exclusive write lease requested by session {session_id} on {doc_ref}"
            ))));
        }

        let seq = self.ensure_session_seq(session_id, alloc_seq);
        let wounds = if can_wound {
            self.wound_and_wake_others(
                &[(doc_ref, LockMode::Write)],
                session_id,
                seq,
                txn_id,
                garbage,
            )
        } else {
            Vec::new()
        };

        let arrival = self.next_arrival_order();
        let can_grant = self.docs.get(doc_ref).is_none_or(|d| {
            d.writer.is_none()
                && !d.readers.keys().any(|&reader_id| reader_id != session_id)
                && d.waiters.range(..(seq, arrival)).next().is_none()
        });

        if can_grant {
            let doc = self.docs.entry(doc_ref.clone()).or_default();
            doc.readers.remove(&session_id);
            doc.writer = Some(session_id);
            let notify_contention = !wounds.is_empty() || !doc.waiters.is_empty();
            let rec = self.sessions.get_mut(&session_id).expect("session exists");
            rec.held_reads.remove(doc_ref);
            rec.held_writes.insert(doc_ref.clone());
            return Ok(AcquireOutcome::Granted {
                observation,
                wounds,
                notify_contention,
            });
        }

        // Release our existing read hold (if upgrading) before entering the wait queue so another
        // holder with higher priority can finish cleanly without upgrade deadlock.
        let released_read = if let Some(doc) = self.docs.get_mut(doc_ref) {
            doc.readers.remove(&session_id).is_some()
        } else {
            false
        };
        if released_read {
            self.sessions
                .get_mut(&session_id)
                .expect("session exists")
                .held_reads
                .remove(doc_ref);
        }

        let (wait_id, rx) = self.park_waiter(session_id, doc_ref, seq, arrival, LockMode::Write);
        if (released_read || !wounds.is_empty())
            && self.docs.get(doc_ref).is_some_and(|d| d.writer.is_none())
        {
            self.wake_waiters(doc_ref);
        }

        Ok(AcquireOutcome::Wait {
            wait_id,
            rx,
            observation,
            wounds,
            notify_contention: true,
        })
    }

    pub(super) fn try_acquire_or_queue_writes(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        docs: &BTreeSet<DocumentRef>,
        alloc_seq: &AtomicU64,
        garbage: &mut LockTableGarbage,
    ) -> Result<BatchAcquireOutcome, CommitError> {
        let (txn_id, can_wound) = {
            let rec = self.active_session_mut(session_id, shared)?;
            (rec.txn_id, rec.can_wound)
        };

        for doc_ref in docs {
            if self.docs.get(doc_ref).is_some_and(|d| {
                d.writer == Some(session_id) || d.readers.contains_key(&session_id)
            }) {
                return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                    "session {session_id} already holds lock on batch target {doc_ref}"
                ))));
            }
        }

        let seq = self.ensure_session_seq(session_id, alloc_seq);

        let wounds = if can_wound {
            let targets: Vec<(&DocumentRef, LockMode)> =
                docs.iter().map(|d| (d, LockMode::Write)).collect();
            self.wound_and_wake_others(&targets, session_id, seq, txn_id, garbage)
        } else {
            Vec::new()
        };

        let mut granted_docs = Vec::new();
        let mut pending = Vec::new();
        let mut waiters_to_wake = Vec::new();
        let mut notify_contention = !wounds.is_empty();

        for doc_ref in docs {
            let arrival = self.next_arrival_order();
            let can_grant = self.docs.get(doc_ref).is_none_or(|d| {
                d.writer.is_none()
                    && d.readers.is_empty()
                    && d.waiters.range(..(seq, arrival)).next().is_none()
            });

            if can_grant {
                let doc = self.docs.entry(doc_ref.clone()).or_default();
                doc.writer = Some(session_id);
                if !doc.waiters.is_empty() {
                    notify_contention = true;
                }
                self.sessions
                    .get_mut(&session_id)
                    .expect("session exists")
                    .held_writes
                    .insert(doc_ref.clone());
                granted_docs.push(doc_ref.clone());
            } else {
                let (wait_id, rx) =
                    self.park_waiter(session_id, doc_ref, seq, arrival, LockMode::Write);
                notify_contention = true;
                pending.push(PendingWait {
                    doc_ref: doc_ref.clone(),
                    wait_id,
                    rx,
                });
                if !wounds.is_empty() && self.docs.get(doc_ref).is_some_and(|d| d.writer.is_none())
                {
                    waiters_to_wake.push(doc_ref.clone());
                }
            }
        }

        for doc_ref in &waiters_to_wake {
            if self.wake_waiters(doc_ref) {
                notify_contention = true;
            }
        }
        if !waiters_to_wake.is_empty() {
            garbage.doc_vecs.push(waiters_to_wake);
        }

        Ok(BatchAcquireOutcome {
            granted_docs,
            pending,
            wounds,
            notify_contention,
        })
    }

    pub(super) fn retain_read(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        doc_ref: &DocumentRef,
        observed_update_time: Option<Timestamp>,
    ) -> Result<bool, CommitError> {
        self.check_read_retaining_session(session_id, shared)?;
        let Some(doc) = self.docs.get_mut(doc_ref) else {
            return Err(transaction_closed());
        };
        let Some(hold) = doc.readers.get_mut(&session_id) else {
            return Err(transaction_closed());
        };
        hold.active_leases = hold.active_leases.saturating_sub(1);
        hold.retained = true;
        // Preserve the transaction's earliest read observation on this document.
        hold.observation.get_or_insert(observed_update_time);
        Ok(!doc.waiters.is_empty())
    }

    pub(super) fn retain_reads(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        leases: &[(ReadLease, Option<Timestamp>)],
    ) -> Result<bool, CommitError> {
        self.check_read_retaining_session(session_id, shared)?;
        for (lease, _) in leases {
            if !self
                .docs
                .get(lease.doc_ref())
                .is_some_and(|d| d.readers.contains_key(&session_id))
            {
                return Err(transaction_closed());
            }
        }
        let mut notify_contention = false;
        for (lease, observed_update_time) in leases {
            let doc = self
                .docs
                .get_mut(lease.doc_ref())
                .expect("validated read doc exists");
            let hold = doc
                .readers
                .get_mut(&session_id)
                .expect("validated read hold exists");
            hold.active_leases = hold.active_leases.saturating_sub(1);
            hold.retained = true;
            // Preserve the transaction's earliest read observation on this document.
            hold.observation.get_or_insert(*observed_update_time);
            if !doc.waiters.is_empty() {
                notify_contention = true;
            }
        }
        Ok(notify_contention)
    }

    pub(super) fn release_read_lease(&mut self, session_id: u64, doc_ref: &DocumentRef) -> bool {
        let mut wake_doc = false;
        let mut remove_doc = false;
        let mut notify = false;
        if let Some(doc) = self.docs.get_mut(doc_ref) {
            if let Some(hold) = doc.readers.get_mut(&session_id) {
                let prev_leases = hold.active_leases;
                hold.active_leases = hold.active_leases.saturating_sub(1);
                let lease_decremented = hold.active_leases != prev_leases;
                let removed_hold = if hold.active_leases == 0 && !hold.retained {
                    doc.readers.remove(&session_id);
                    if let Some(rec) = self.sessions.get_mut(&session_id) {
                        rec.held_reads.remove(doc_ref);
                    }
                    true
                } else {
                    false
                };
                if !doc.waiters.is_empty() && (removed_hold || lease_decremented) {
                    notify = true;
                }
                if removed_hold {
                    wake_doc = doc.writer.is_none() && !doc.waiters.is_empty();
                    remove_doc = doc.is_empty();
                }
            } else {
                remove_doc = doc.is_empty();
            }
        }
        if remove_doc {
            self.docs.remove(doc_ref);
        } else if wake_doc && self.wake_waiters(doc_ref) {
            notify = true;
        }
        notify
    }

    pub(super) fn release_write_lease(&mut self, session_id: u64, doc_ref: &DocumentRef) -> bool {
        let mut wake_doc = false;
        let mut remove_doc = false;
        let mut notify = false;
        if let Some(doc) = self.docs.get_mut(doc_ref)
            && doc.writer == Some(session_id)
        {
            doc.writer = None;
            if let Some(rec) = self.sessions.get_mut(&session_id) {
                rec.held_writes.remove(doc_ref);
            }
            wake_doc = !doc.waiters.is_empty();
            notify = wake_doc;
            remove_doc = doc.is_empty();
        }
        if remove_doc {
            self.docs.remove(doc_ref);
        } else if wake_doc && self.wake_waiters(doc_ref) {
            notify = true;
        }
        notify
    }

    pub(super) fn consume_wait_grant(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        wait_id: u64,
    ) -> (Result<(), CommitError>, bool) {
        if self.cleared {
            return (
                Err(CommitError::Aborted(AbortReason::DatabaseCleared)),
                false,
            );
        }
        let Some(rec) = self.sessions.get_mut(&session_id) else {
            return (Err(missing_session_error(shared)), false);
        };
        if rec.state != SessionState::Active {
            return (Err(inactive_record_error(rec)), false);
        }
        let Some((doc_ref, mode)) = rec.promoted_grants.remove(&wait_id) else {
            return (Err(transaction_closed()), false);
        };
        if mode == LockMode::Read && !rec.shared.is_accepting_reads() {
            let mut wake_doc = false;
            let mut remove_doc = false;
            let mut notify = false;
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                rollback_promoted_grant(doc, rec, session_id, &doc_ref, mode);
                notify = !doc.waiters.is_empty();
                wake_doc = doc.writer.is_none() && !doc.waiters.is_empty();
                remove_doc = doc.is_empty();
            }
            if remove_doc {
                self.docs.remove(&doc_ref);
            } else if wake_doc && self.wake_waiters(&doc_ref) {
                notify = true;
            }
            return (Err(transaction_closed()), notify);
        }
        (Ok(()), false)
    }

    pub(super) fn revoke_waiter(&mut self, session_id: u64, wait_id: u64) -> bool {
        let parked = self
            .sessions
            .get_mut(&session_id)
            .and_then(|rec| rec.parked_waiters.remove(&wait_id));
        if let Some((doc_ref, key)) = parked {
            let mut wake_doc = false;
            let mut remove_doc = false;
            let mut removed_waiter = false;
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                removed_waiter = doc.waiters.remove(&key).is_some();
                if removed_waiter && doc.waiters.is_empty() {
                    self.contended_docs.remove(&doc_ref);
                }
                wake_doc = doc.writer.is_none() && !doc.waiters.is_empty();
                remove_doc = doc.is_empty();
            }
            if remove_doc {
                self.docs.remove(&doc_ref);
            } else if wake_doc {
                self.wake_waiters(&doc_ref);
            }
            return removed_waiter;
        }

        let promoted = self
            .sessions
            .get_mut(&session_id)
            .and_then(|rec| rec.promoted_grants.remove(&wait_id));
        if let Some((doc_ref, mode)) = promoted {
            return match mode {
                LockMode::Read => self.release_read_lease(session_id, &doc_ref),
                LockMode::Write => self.release_write_lease(session_id, &doc_ref),
            };
        }
        false
    }

    pub(super) fn seal_for_commit(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        write_docs: &BTreeMap<DocumentRef, WriteLease>,
        garbage: &mut LockTableGarbage,
    ) -> Result<bool, CommitError> {
        if self.cleared {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        {
            let Some(rec) = self.sessions.get(&session_id) else {
                return Err(missing_session_error(shared));
            };
            if rec.state != SessionState::Active {
                return Err(inactive_record_error(rec));
            }
            for doc_ref in write_docs.keys() {
                if self.docs.get(doc_ref).and_then(|d| d.writer) != Some(session_id) {
                    return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                        "write lease for {doc_ref} is not held by session {session_id}"
                    ))));
                }
            }
        }

        let mut notify_contention = write_docs.keys().any(|doc_ref| {
            self.docs
                .get(doc_ref)
                .is_some_and(|d| !d.waiters.is_empty())
        });
        let mut docs_to_wake = Vec::new();
        if let Some(rec) = self.sessions.get_mut(&session_id) {
            rec.state = SessionState::Committing;
            rec.woundable = false;
            rec.shared.publish_state(STATE_COMMITTING);

            let promoted = std::mem::take(&mut rec.promoted_grants);
            for (doc_ref, mode) in promoted.values() {
                if let Some(doc) = self.docs.get_mut(doc_ref) {
                    match mode {
                        LockMode::Read => {
                            let prev_leases =
                                doc.readers.get(&session_id).map_or(0, |h| h.active_leases);
                            let remove_hold = if let Some(hold) = doc.readers.get_mut(&session_id) {
                                hold.active_leases = hold.active_leases.saturating_sub(1);
                                hold.active_leases == 0 && !hold.retained
                            } else {
                                false
                            };
                            if !doc.waiters.is_empty() && (remove_hold || prev_leases > 0) {
                                notify_contention = true;
                            }
                            if remove_hold {
                                doc.readers.remove(&session_id);
                                rec.held_reads.remove(doc_ref);
                                record_doc_wake_or_prune(
                                    &mut self.docs,
                                    doc_ref,
                                    &mut docs_to_wake,
                                );
                            }
                        }
                        LockMode::Write => {
                            if !write_docs.contains_key(doc_ref) {
                                if doc.writer == Some(session_id) {
                                    doc.writer = None;
                                    if !doc.waiters.is_empty() {
                                        notify_contention = true;
                                    }
                                }
                                rec.held_writes.remove(doc_ref);
                                record_doc_wake_or_prune(
                                    &mut self.docs,
                                    doc_ref,
                                    &mut docs_to_wake,
                                );
                            }
                        }
                    }
                }
            }
            garbage.grant_maps.push(promoted);

            let held_reads = std::mem::take(&mut rec.held_reads);
            for doc_ref in &held_reads {
                if let Some(doc) = self.docs.get_mut(doc_ref)
                    && doc.readers.remove(&session_id).is_some()
                {
                    if !doc.waiters.is_empty() {
                        notify_contention = true;
                    }
                    if doc.writer.is_none() && !doc.waiters.is_empty() {
                        docs_to_wake.push(doc_ref.clone());
                    } else if doc.is_empty() {
                        self.docs.remove(doc_ref);
                    }
                }
            }
            garbage.doc_sets.push(held_reads);

            let parked = std::mem::take(&mut rec.parked_waiters);
            for (doc_ref, key) in parked.values() {
                if let Some(doc) = self.docs.get_mut(doc_ref) {
                    if let Some(waiter) = doc.waiters.remove(key) {
                        let _unused = waiter.tx.send(Err(transaction_closed()));
                        notify_contention = true;
                        if doc.waiters.is_empty() {
                            self.contended_docs.remove(doc_ref);
                        }
                    }
                    if doc.writer.is_none() && !doc.waiters.is_empty() {
                        docs_to_wake.push(doc_ref.clone());
                    } else if doc.is_empty() {
                        self.docs.remove(doc_ref);
                    }
                }
            }
            garbage.waiter_maps.push(parked);
        }

        for (idx, doc_ref) in docs_to_wake.iter().enumerate() {
            if !docs_to_wake[..idx].contains(doc_ref) && self.wake_waiters(doc_ref) {
                notify_contention = true;
            }
        }
        if !docs_to_wake.is_empty() {
            garbage.doc_vecs.push(docs_to_wake);
        }
        Ok(notify_contention)
    }

    pub(super) fn close_session(
        &mut self,
        session_id: u64,
        timeout_reason: Option<AbortReason>,
        closed_state_code: u8,
        garbage: &mut LockTableGarbage,
    ) -> CloseOutcome {
        let disposition = match timeout_reason {
            Some(reason) => CloseDisposition::TimedOut(reason),
            None => CloseDisposition::Normal(closed_state_code),
        };
        match self.close_session_inner(session_id, disposition, garbage) {
            Some((_, notify_state_change)) => CloseOutcome {
                actually_closed: true,
                notify_state_change,
            },
            None => CloseOutcome {
                actually_closed:     false,
                notify_state_change: false,
            },
        }
    }

    pub(super) fn close_expired(
        &mut self,
        session_id: u64,
        garbage: &mut LockTableGarbage,
    ) -> Option<ExpiredSessionOutcome> {
        self.close_session_inner(session_id, CloseDisposition::Expired, garbage)
            .map(|(expired, _)| expired.expect("expired disposition yields outcome"))
    }

    fn close_session_inner(
        &mut self,
        session_id: u64,
        disposition: CloseDisposition,
        garbage: &mut LockTableGarbage,
    ) -> Option<(Option<ExpiredSessionOutcome>, bool)> {
        let rec = self.sessions.get(&session_id)?;
        if !matches!(disposition, CloseDisposition::Normal(_)) && rec.state != SessionState::Active
        {
            return None;
        }

        let mut rec = self
            .sessions
            .remove(&session_id)
            .expect("session verified present");

        let mut expired_outcome = None;
        let cancel_err = match disposition {
            CloseDisposition::TimedOut(reason) => {
                rec.state = SessionState::TimedOut;
                rec.abort_reason = Some(reason);
                rec.shared.publish_abort(STATE_TIMED_OUT, reason);
                CommitError::Aborted(reason)
            }
            CloseDisposition::Expired => {
                let had_resources = !rec.held_reads.is_empty()
                    || !rec.held_writes.is_empty()
                    || !rec.promoted_grants.is_empty()
                    || !rec.parked_waiters.is_empty();
                let doc_ref = if had_resources {
                    rec.parked_waiters
                        .values()
                        .map(|(doc, _)| doc)
                        .min()
                        .or_else(|| rec.held_writes.iter().min())
                        .or_else(|| rec.held_reads.iter().min())
                        .or_else(|| rec.promoted_grants.values().map(|(doc, _)| doc).min())
                        .cloned()
                } else {
                    None
                };
                expired_outcome = Some(ExpiredSessionOutcome {
                    session_id,
                    txn_id: rec.txn_id,
                    seq: (rec.seq != 0).then_some(rec.seq),
                    kind: rec.kind,
                    doc_ref,
                    had_resources,
                    notify_state_change: false,
                });
                let reason = AbortReason::TransactionExpired;
                rec.state = SessionState::Expired;
                rec.abort_reason = Some(reason);
                rec.shared.publish_abort(STATE_EXPIRED, reason);
                CommitError::Aborted(reason)
            }
            CloseDisposition::Normal(closed_state_code) => {
                if rec.state == SessionState::Active || rec.state == SessionState::Committing {
                    rec.shared.publish_state(closed_state_code);
                }
                transaction_closed()
            }
        };

        let mut affected_docs = Vec::new();
        let mut notify_state_change = false;
        self.strip_session_resources(
            session_id,
            &mut rec,
            &cancel_err,
            &mut affected_docs,
            &mut notify_state_change,
            garbage,
        );

        for (idx, doc_ref) in affected_docs.iter().enumerate() {
            if !affected_docs[..idx].contains(doc_ref) && self.wake_waiters(doc_ref) {
                notify_state_change = true;
            }
        }
        if !affected_docs.is_empty() {
            garbage.doc_vecs.push(affected_docs);
        }
        garbage.records.push(rec);
        if let Some(outcome) = expired_outcome.as_mut() {
            outcome.notify_state_change = notify_state_change;
        }
        Some((expired_outcome, notify_state_change))
    }

    pub(super) fn abort_all(
        &mut self,
        reason: AbortReason,
        garbage: &mut LockTableGarbage,
    ) -> bool {
        if reason == AbortReason::DatabaseCleared {
            self.cleared = true;
        }
        let had_contended = !self.contended_docs.is_empty();
        garbage.contended_docs = std::mem::take(&mut self.contended_docs);
        let sessions = std::mem::take(&mut self.sessions);
        let mut docs = std::mem::take(&mut self.docs);
        for rec in sessions.values() {
            rec.shared.publish_abort(STATE_CLEARED, reason);
        }
        for doc in docs.values_mut() {
            for (_, waiter) in std::mem::take(&mut doc.waiters) {
                let _unused = waiter.tx.send(Err(CommitError::Aborted(reason)));
            }
        }
        garbage.sessions = sessions;
        garbage.docs = docs;
        had_contended
    }

    fn active_session_mut(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
    ) -> Result<&mut SessionRecord, CommitError> {
        if self.cleared {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        let Some(rec) = self.sessions.get_mut(&session_id) else {
            return Err(missing_session_error(shared));
        };
        if rec.state != SessionState::Active {
            return Err(inactive_record_error(rec));
        }
        Ok(rec)
    }

    fn check_read_retaining_session(
        &self,
        session_id: u64,
        shared: &SessionSharedState,
    ) -> Result<(), CommitError> {
        if self.cleared {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        let Some(rec) = self.sessions.get(&session_id) else {
            return Err(missing_session_error(shared));
        };
        if rec.state != SessionState::Active {
            return Err(inactive_record_error(rec));
        }
        if !rec.shared.is_accepting_reads() {
            return Err(transaction_closed());
        }
        Ok(())
    }

    fn ensure_session_seq(&mut self, session_id: u64, alloc_seq: &AtomicU64) -> u64 {
        let rec = self
            .sessions
            .get_mut(&session_id)
            .expect("session registered");
        if rec.seq == 0 {
            let allocated = alloc_seq.fetch_add(1, Ordering::Relaxed);
            assert!(allocated < u64::MAX, "lock priority sequence exhausted");
            rec.seq = allocated;
            rec.shared.publish_seq(allocated);
        }
        rec.seq
    }

    fn next_arrival_order(&mut self) -> u64 {
        let order = self.next_arrival;
        self.next_arrival += 1;
        order
    }

    fn next_wait_id(&mut self) -> u64 {
        let id = self.next_wait_id;
        self.next_wait_id += 1;
        id
    }

    fn park_waiter(
        &mut self,
        session_id: u64,
        doc_ref: &DocumentRef,
        seq: u64,
        arrival: u64,
        mode: LockMode,
    ) -> (u64, oneshot::Receiver<Result<(), CommitError>>) {
        let wait_id = self.next_wait_id();
        let (tx, rx) = oneshot::channel();
        let key = (seq, arrival);
        let doc = self.docs.entry(doc_ref.clone()).or_default();
        let was_empty = doc.waiters.is_empty();
        doc.waiters.insert(
            key,
            Waiter {
                wait_id,
                session_id,
                mode,
                enqueued_at: tokio::time::Instant::now(),
                tx,
            },
        );
        if was_empty {
            self.contended_docs.insert(doc_ref.clone());
        }
        self.sessions
            .get_mut(&session_id)
            .expect("session registered")
            .parked_waiters
            .insert(wait_id, (doc_ref.clone(), key));
        (wait_id, rx)
    }

    fn wound_and_wake_others(
        &mut self,
        targets: &[(&DocumentRef, LockMode)],
        requester_id: u64,
        requester_seq: u64,
        requester_txn_id: Option<TransactionId>,
        garbage: &mut LockTableGarbage,
    ) -> Vec<WoundEvent> {
        let (affected_docs, wounds) = self.wound_holders_on(
            targets,
            requester_id,
            requester_seq,
            requester_txn_id,
            garbage,
        );
        for (idx, other) in affected_docs.iter().enumerate() {
            if !affected_docs[..idx].contains(other)
                && !targets.iter().any(|(target, _)| *target == other)
            {
                self.wake_waiters(other);
            }
        }
        if !affected_docs.is_empty() {
            garbage.doc_vecs.push(affected_docs);
        }
        wounds
    }

    fn wound_holders_on(
        &mut self,
        targets: &[(&DocumentRef, LockMode)],
        requester_id: u64,
        requester_seq: u64,
        requester_txn_id: Option<TransactionId>,
        garbage: &mut LockTableGarbage,
    ) -> (Vec<DocumentRef>, Vec<WoundEvent>) {
        let mut victims: BTreeMap<u64, DocumentRef> = BTreeMap::new();
        for &(doc_ref, req_mode) in targets {
            let Some(doc) = self.docs.get(doc_ref) else {
                continue;
            };
            if let Some(writer_id) = doc.writer
                && writer_id != requester_id
            {
                victims.entry(writer_id).or_insert_with(|| doc_ref.clone());
            }
            if req_mode == LockMode::Write {
                for &reader_id in doc.readers.keys() {
                    if reader_id != requester_id {
                        victims.entry(reader_id).or_insert_with(|| doc_ref.clone());
                    }
                }
            }
        }

        let eligible: Vec<(u64, DocumentRef)> = victims
            .into_iter()
            .filter(|(vid, _)| {
                self.sessions.get(vid).is_some_and(|rec| {
                    rec.state == SessionState::Active && rec.woundable && rec.seq > requester_seq
                })
            })
            .collect();

        if eligible.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let reason = AbortReason::Wounded {
            wounder_txn_id: requester_txn_id,
            wounder_seq:    requester_seq,
        };
        let mut affected_docs = Vec::new();
        let mut events = Vec::with_capacity(eligible.len());
        let mut notify_state_change = false;

        for (victim_id, trigger_doc) in eligible {
            // Remove the victim record completely so dead Wounded records do not accumulate in
            // `self.sessions`; `SessionSharedState` retains `STATE_WOUNDED` + `abort_reason`.
            let Some(mut rec) = self.sessions.remove(&victim_id) else {
                continue;
            };
            rec.state = SessionState::Wounded;
            rec.abort_reason = Some(reason);
            rec.shared.publish_abort(STATE_WOUNDED, reason);
            events.push(WoundEvent {
                victim_session_id: victim_id,
                victim_txn_id: rec.txn_id,
                victim_kind: rec.kind,
                victim_seq: rec.seq,
                doc_ref: trigger_doc,
                reason,
            });
            let cancel_err = CommitError::Aborted(reason);
            self.strip_session_resources(
                victim_id,
                &mut rec,
                &cancel_err,
                &mut affected_docs,
                &mut notify_state_change,
                garbage,
            );
            garbage.records.push(rec);
        }

        (affected_docs, events)
    }

    fn strip_session_resources(
        &mut self,
        session_id: u64,
        rec: &mut SessionRecord,
        cancel_err: &CommitError,
        affected_docs: &mut Vec<DocumentRef>,
        notify_state_change: &mut bool,
        garbage: &mut LockTableGarbage,
    ) {
        let held_reads = std::mem::take(&mut rec.held_reads);
        for doc_ref in &held_reads {
            if let Some(doc) = self.docs.get_mut(doc_ref)
                && doc.readers.remove(&session_id).is_some()
            {
                if !doc.waiters.is_empty() {
                    *notify_state_change = true;
                }
                if doc.writer.is_none() && !doc.waiters.is_empty() {
                    affected_docs.push(doc_ref.clone());
                } else if doc.is_empty() {
                    self.docs.remove(doc_ref);
                }
            }
        }
        if !held_reads.is_empty() || held_reads.capacity() > 0 {
            garbage.doc_sets.push(held_reads);
        }

        let held_writes = std::mem::take(&mut rec.held_writes);
        for doc_ref in &held_writes {
            if let Some(doc) = self.docs.get_mut(doc_ref)
                && doc.writer == Some(session_id)
            {
                doc.writer = None;
                if !doc.waiters.is_empty() {
                    *notify_state_change = true;
                    affected_docs.push(doc_ref.clone());
                } else if doc.is_empty() {
                    self.docs.remove(doc_ref);
                }
            }
        }
        if !held_writes.is_empty() || held_writes.capacity() > 0 {
            garbage.doc_sets.push(held_writes);
        }

        let promoted_grants = std::mem::take(&mut rec.promoted_grants);
        for (doc_ref, mode) in promoted_grants.values() {
            if let Some(doc) = self.docs.get_mut(doc_ref) {
                let changed = match mode {
                    LockMode::Read => doc.readers.remove(&session_id).is_some(),
                    LockMode::Write => {
                        if doc.writer == Some(session_id) {
                            doc.writer = None;
                            true
                        } else {
                            false
                        }
                    }
                };
                if changed {
                    if !doc.waiters.is_empty() {
                        *notify_state_change = true;
                    }
                    if doc.writer.is_none() && !doc.waiters.is_empty() {
                        affected_docs.push(doc_ref.clone());
                    } else if doc.is_empty() {
                        self.docs.remove(doc_ref);
                    }
                }
            }
        }
        if !promoted_grants.is_empty() || promoted_grants.capacity() > 0 {
            garbage.grant_maps.push(promoted_grants);
        }

        let parked_waiters = std::mem::take(&mut rec.parked_waiters);
        for (doc_ref, key) in parked_waiters.values() {
            if let Some(doc) = self.docs.get_mut(doc_ref) {
                if let Some(waiter) = doc.waiters.remove(key) {
                    let _unused = waiter.tx.send(Err(cancel_err.clone()));
                    *notify_state_change = true;
                    if doc.waiters.is_empty() {
                        self.contended_docs.remove(doc_ref);
                    }
                }
                if doc.writer.is_none() && !doc.waiters.is_empty() {
                    affected_docs.push(doc_ref.clone());
                } else if doc.is_empty() {
                    self.docs.remove(doc_ref);
                }
            }
        }
        if !parked_waiters.is_empty() || parked_waiters.capacity() > 0 {
            garbage.waiter_maps.push(parked_waiters);
        }
    }

    fn wake_waiters(&mut self, doc_ref: &DocumentRef) -> bool {
        let Self {
            docs,
            contended_docs,
            sessions,
            ..
        } = self;
        let mut any_popped = false;
        let remove_empty = {
            let Some(doc) = docs.get_mut(doc_ref) else {
                return false;
            };
            loop {
                let Some((_, first)) = doc.waiters.first_key_value() else {
                    if any_popped {
                        contended_docs.remove(doc_ref);
                    }
                    break doc.is_empty();
                };
                let eligible = match first.mode {
                    LockMode::Read => doc.writer.is_none(),
                    LockMode::Write => doc.writer.is_none() && doc.readers.is_empty(),
                };
                if !eligible {
                    return any_popped;
                }

                let (_, waiter) = doc.waiters.pop_first().expect("waiter existed");
                any_popped = true;
                let Some(rec) = sessions.get_mut(&waiter.session_id) else {
                    continue;
                };
                rec.parked_waiters.remove(&waiter.wait_id);

                if rec.state != SessionState::Active {
                    let _unused = waiter.tx.send(Err(inactive_record_error(rec)));
                    continue;
                }
                if waiter.mode == LockMode::Read && !rec.shared.is_accepting_reads() {
                    let _unused = waiter.tx.send(Err(transaction_closed()));
                    continue;
                }

                match waiter.mode {
                    LockMode::Read => {
                        let hold = doc.readers.entry(waiter.session_id).or_insert(ReadHold {
                            active_leases: 0,
                            retained:      false,
                            observation:   None,
                        });
                        hold.active_leases += 1;
                        rec.held_reads.insert(doc_ref.clone());
                    }
                    LockMode::Write => {
                        doc.writer = Some(waiter.session_id);
                        rec.held_writes.insert(doc_ref.clone());
                    }
                }
                rec.promoted_grants
                    .insert(waiter.wait_id, (doc_ref.clone(), waiter.mode));

                if waiter.tx.send(Ok(())).is_err() {
                    // Receiver already dropped before wake completed: revoke our unconsumed
                    // promotion immediately and keep scanning the queue.
                    rec.promoted_grants.remove(&waiter.wait_id);
                    rollback_promoted_grant(doc, rec, waiter.session_id, doc_ref, waiter.mode);
                }
            }
        };
        if remove_empty {
            docs.remove(doc_ref);
        }
        any_popped
    }

    pub(super) fn contention_snapshot(
        &self,
        now: tokio::time::Instant,
    ) -> (Vec<ContendedDocumentSnapshot>, usize, bool) {
        let total_contended = self.contended_docs.len();
        let truncated = total_contended > MAX_SNAPSHOT_CONTENDED_DOCS;
        let mut contended_documents =
            Vec::with_capacity(total_contended.min(MAX_SNAPSHOT_CONTENDED_DOCS));

        for doc_ref in self.contended_docs.iter().take(MAX_SNAPSHOT_CONTENDED_DOCS) {
            let Some(doc) = self.docs.get(doc_ref) else {
                continue;
            };
            let writer = doc.writer.and_then(|session_id| {
                let rec = self.sessions.get(&session_id)?;
                Some(WriterSnapshot {
                    session_id,
                    txn_id: rec.txn_id.map(|id| id.to_string()),
                    kind: rec.kind,
                    seq: (rec.seq != 0).then_some(rec.seq),
                    state: session_snapshot_state(rec.state),
                    woundable: rec.woundable,
                })
            });

            let reader_count = doc.readers.len();
            let mut reader_entries: Vec<(u64, u64, &ReadHold, &SessionRecord)> = doc
                .readers
                .iter()
                .filter_map(|(&session_id, hold)| {
                    let rec = self.sessions.get(&session_id)?;
                    let sort_seq = if rec.seq == 0 { u64::MAX } else { rec.seq };
                    Some((sort_seq, session_id, hold, rec))
                })
                .collect();
            reader_entries.sort_by_key(|&(sort_seq, session_id, _, _)| (sort_seq, session_id));
            reader_entries.truncate(MAX_SNAPSHOT_DOC_PARTICIPANTS);
            let readers: Vec<ReaderSnapshot> = reader_entries
                .into_iter()
                .map(|(_, session_id, hold, rec)| ReaderSnapshot {
                    session_id,
                    txn_id: rec.txn_id.map(|id| id.to_string()),
                    kind: rec.kind,
                    seq: (rec.seq != 0).then_some(rec.seq),
                    state: session_snapshot_state(rec.state),
                    woundable: rec.woundable,
                    retained: hold.retained,
                    active_leases: hold.active_leases,
                })
                .collect();

            let waiter_count = doc.waiters.len();
            let waiters: Vec<WaiterSnapshot> = doc
                .waiters
                .iter()
                .take(MAX_SNAPSHOT_DOC_PARTICIPANTS)
                .filter_map(|(&(seq, _arrival), waiter)| {
                    let rec = self.sessions.get(&waiter.session_id)?;
                    let waited_ms = u64::try_from(
                        now.saturating_duration_since(waiter.enqueued_at)
                            .as_millis(),
                    )
                    .unwrap_or(u64::MAX);
                    Some(WaiterSnapshot {
                        session_id: waiter.session_id,
                        txn_id: rec.txn_id.map(|id| id.to_string()),
                        kind: rec.kind,
                        seq,
                        mode: waiter.mode.into(),
                        waited_ms,
                    })
                })
                .collect();

            contended_documents.push(ContendedDocumentSnapshot {
                document: doc_ref.to_string(),
                writer,
                readers,
                reader_count,
                waiters,
                waiter_count,
            });
        }

        (contended_documents, total_contended, truncated)
    }

    #[cfg(test)]
    pub(super) fn assert_contended_docs_consistent(&self) {
        let expected: BTreeSet<DocumentRef> = self
            .docs
            .iter()
            .filter(|(_, doc)| !doc.waiters.is_empty())
            .map(|(doc_ref, _)| doc_ref.clone())
            .collect();
        assert_eq!(self.contended_docs, expected);
    }

    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize) {
        (self.docs.len(), self.sessions.len())
    }
}

fn session_snapshot_state(state: SessionState) -> ContentionSessionState {
    match state {
        SessionState::Active => ContentionSessionState::Active,
        SessionState::Committing => ContentionSessionState::Committing,
        SessionState::Wounded | SessionState::TimedOut | SessionState::Expired => {
            debug_assert!(false, "terminal session should not reside in LockTable");
            ContentionSessionState::Committing
        }
    }
}

fn missing_session_error(shared: &SessionSharedState) -> CommitError {
    match shared.abort_state() {
        SessionAbortState::Wounded(reason)
        | SessionAbortState::TimedOut(reason)
        | SessionAbortState::Cleared(reason)
        | SessionAbortState::Expired(reason) => CommitError::Aborted(reason),
        SessionAbortState::Active | SessionAbortState::Committing | SessionAbortState::Closed => {
            transaction_closed()
        }
    }
}

fn inactive_record_error(rec: &SessionRecord) -> CommitError {
    match rec.state {
        SessionState::Active => unreachable!("checked active"),
        SessionState::Committing => transaction_closed(),
        SessionState::Wounded => {
            CommitError::Aborted(rec.abort_reason.unwrap_or(AbortReason::Wounded {
                wounder_txn_id: None,
                wounder_seq:    0,
            }))
        }
        SessionState::TimedOut => {
            CommitError::Aborted(rec.abort_reason.unwrap_or(AbortReason::WriteTimeout))
        }
        SessionState::Expired => {
            CommitError::Aborted(rec.abort_reason.unwrap_or(AbortReason::TransactionExpired))
        }
    }
}

fn record_doc_wake_or_prune(
    docs: &mut HashMap<DocumentRef, DocLockState>,
    doc_ref: &DocumentRef,
    docs_to_wake: &mut Vec<DocumentRef>,
) {
    if let Some(doc) = docs.get(doc_ref) {
        if doc.writer.is_none() && !doc.waiters.is_empty() {
            docs_to_wake.push(doc_ref.clone());
        } else if doc.is_empty() {
            docs.remove(doc_ref);
        }
    }
}

fn rollback_promoted_grant(
    doc: &mut DocLockState,
    rec: &mut SessionRecord,
    session_id: u64,
    doc_ref: &DocumentRef,
    mode: LockMode,
) {
    match mode {
        LockMode::Read => {
            let remove_hold = if let Some(hold) = doc.readers.get_mut(&session_id) {
                hold.active_leases = hold.active_leases.saturating_sub(1);
                hold.active_leases == 0 && !hold.retained
            } else {
                false
            };
            if remove_hold {
                doc.readers.remove(&session_id);
                rec.held_reads.remove(doc_ref);
            }
        }
        LockMode::Write => {
            if doc.writer == Some(session_id) {
                doc.writer = None;
            }
            rec.held_writes.remove(doc_ref);
        }
    }
}
