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
    SessionAbortState, SessionSharedState, WriteLease, transaction_closed,
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
    pub(super) victim_seq: u64,
    pub(super) doc_ref: DocumentRef,
    pub(super) reason: AbortReason,
}

#[derive(Debug)]
struct Waiter {
    wait_id: u64,
    session_id: u64,
    mode: LockMode,
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
        wounds:      Vec<WoundEvent>,
    },
    Wait {
        wait_id: u64,
        rx: oneshot::Receiver<Result<(), CommitError>>,
        observation: ReadObservation,
        wounds: Vec<WoundEvent>,
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
}

#[derive(Debug, Default)]
pub(super) struct LockTable {
    docs: HashMap<DocumentRef, DocLockState>,
    sessions: HashMap<u64, SessionRecord>,
    next_arrival: u64,
    next_wait_id: u64,
    cleared: bool,
}

impl LockTable {
    pub(super) fn register_session(
        &mut self,
        session_id: u64,
        shared: Arc<SessionSharedState>,
        txn_id: Option<TransactionId>,
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
            if let Some(doc) = self.docs.get_mut(doc_ref)
                && let Some(hold) = doc.readers.get_mut(&session_id)
            {
                hold.active_leases += 1;
            }
            return Ok(AcquireOutcome::Granted {
                observation: ReadObservation::NeverRead,
                wounds:      Vec::new(),
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
            let should_wake = !wounds.is_empty() && !doc.waiters.is_empty();
            self.sessions
                .get_mut(&session_id)
                .expect("session registered")
                .held_reads
                .insert(doc_ref.clone());
            if should_wake {
                self.wake_waiters(doc_ref);
            }
            return Ok(AcquireOutcome::Granted {
                observation: ReadObservation::NeverRead,
                wounds,
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
            let rec = self.sessions.get_mut(&session_id).expect("session exists");
            rec.held_reads.remove(doc_ref);
            rec.held_writes.insert(doc_ref.clone());
            return Ok(AcquireOutcome::Granted {
                observation,
                wounds,
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

        for doc_ref in docs {
            let arrival = self.next_arrival_order();
            let can_grant = self.docs.get(doc_ref).is_none_or(|d| {
                d.writer.is_none()
                    && d.readers.is_empty()
                    && d.waiters.range(..(seq, arrival)).next().is_none()
            });

            if can_grant {
                self.docs.entry(doc_ref.clone()).or_default().writer = Some(session_id);
                self.sessions
                    .get_mut(&session_id)
                    .expect("session exists")
                    .held_writes
                    .insert(doc_ref.clone());
                granted_docs.push(doc_ref.clone());
            } else {
                let (wait_id, rx) =
                    self.park_waiter(session_id, doc_ref, seq, arrival, LockMode::Write);
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
            self.wake_waiters(doc_ref);
        }
        if !waiters_to_wake.is_empty() {
            garbage.doc_vecs.push(waiters_to_wake);
        }

        Ok(BatchAcquireOutcome {
            granted_docs,
            pending,
            wounds,
        })
    }

    pub(super) fn retain_read(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        doc_ref: &DocumentRef,
        observed_update_time: Option<Timestamp>,
    ) -> Result<(), CommitError> {
        self.check_read_retaining_session(session_id, shared)?;
        let Some(hold) = self
            .docs
            .get_mut(doc_ref)
            .and_then(|d| d.readers.get_mut(&session_id))
        else {
            return Err(transaction_closed());
        };
        hold.active_leases = hold.active_leases.saturating_sub(1);
        hold.retained = true;
        // Preserve the transaction's earliest read observation on this document.
        hold.observation.get_or_insert(observed_update_time);
        Ok(())
    }

    pub(super) fn retain_reads(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        leases: &[(ReadLease, Option<Timestamp>)],
    ) -> Result<(), CommitError> {
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
        for (lease, observed_update_time) in leases {
            let hold = self
                .docs
                .get_mut(lease.doc_ref())
                .and_then(|d| d.readers.get_mut(&session_id))
                .expect("validated read hold exists");
            hold.active_leases = hold.active_leases.saturating_sub(1);
            hold.retained = true;
            // Preserve the transaction's earliest read observation on this document.
            hold.observation.get_or_insert(*observed_update_time);
        }
        Ok(())
    }

    pub(super) fn release_read_lease(&mut self, session_id: u64, doc_ref: &DocumentRef) {
        let mut wake_doc = false;
        let mut remove_doc = false;
        if let Some(doc) = self.docs.get_mut(doc_ref) {
            if let Some(hold) = doc.readers.get_mut(&session_id) {
                hold.active_leases = hold.active_leases.saturating_sub(1);
                if hold.active_leases == 0 && !hold.retained {
                    doc.readers.remove(&session_id);
                    if let Some(rec) = self.sessions.get_mut(&session_id) {
                        rec.held_reads.remove(doc_ref);
                    }
                    wake_doc = doc.writer.is_none() && !doc.waiters.is_empty();
                    remove_doc = doc.is_empty();
                }
            } else {
                remove_doc = doc.is_empty();
            }
        }
        if remove_doc {
            self.docs.remove(doc_ref);
        } else if wake_doc {
            self.wake_waiters(doc_ref);
        }
    }

    pub(super) fn release_write_lease(&mut self, session_id: u64, doc_ref: &DocumentRef) {
        let mut wake_doc = false;
        let mut remove_doc = false;
        if let Some(doc) = self.docs.get_mut(doc_ref)
            && doc.writer == Some(session_id)
        {
            doc.writer = None;
            if let Some(rec) = self.sessions.get_mut(&session_id) {
                rec.held_writes.remove(doc_ref);
            }
            wake_doc = !doc.waiters.is_empty();
            remove_doc = doc.is_empty();
        }
        if remove_doc {
            self.docs.remove(doc_ref);
        } else if wake_doc {
            self.wake_waiters(doc_ref);
        }
    }

    pub(super) fn consume_wait_grant(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        wait_id: u64,
    ) -> Result<(), CommitError> {
        if self.cleared {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        let Some(rec) = self.sessions.get_mut(&session_id) else {
            return Err(missing_session_error(shared));
        };
        if rec.state != SessionState::Active {
            return Err(inactive_record_error(rec));
        }
        let Some((doc_ref, mode)) = rec.promoted_grants.remove(&wait_id) else {
            return Err(transaction_closed());
        };
        if mode == LockMode::Read && !rec.shared.is_accepting_reads() {
            let mut wake_doc = false;
            let mut remove_doc = false;
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                rollback_promoted_grant(doc, rec, session_id, &doc_ref, mode);
                wake_doc = doc.writer.is_none() && !doc.waiters.is_empty();
                remove_doc = doc.is_empty();
            }
            if remove_doc {
                self.docs.remove(&doc_ref);
            } else if wake_doc {
                self.wake_waiters(&doc_ref);
            }
            return Err(transaction_closed());
        }
        Ok(())
    }

    pub(super) fn revoke_waiter(&mut self, session_id: u64, wait_id: u64) {
        let parked = self
            .sessions
            .get_mut(&session_id)
            .and_then(|rec| rec.parked_waiters.remove(&wait_id));
        if let Some((doc_ref, key)) = parked {
            let mut wake_doc = false;
            let mut remove_doc = false;
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                doc.waiters.remove(&key);
                wake_doc = doc.writer.is_none() && !doc.waiters.is_empty();
                remove_doc = doc.is_empty();
            }
            if remove_doc {
                self.docs.remove(&doc_ref);
            } else if wake_doc {
                self.wake_waiters(&doc_ref);
            }
            return;
        }

        let promoted = self
            .sessions
            .get_mut(&session_id)
            .and_then(|rec| rec.promoted_grants.remove(&wait_id));
        if let Some((doc_ref, mode)) = promoted {
            match mode {
                LockMode::Read => self.release_read_lease(session_id, &doc_ref),
                LockMode::Write => self.release_write_lease(session_id, &doc_ref),
            }
        }
    }

    pub(super) fn seal_for_commit(
        &mut self,
        session_id: u64,
        shared: &SessionSharedState,
        write_docs: &BTreeMap<DocumentRef, WriteLease>,
        garbage: &mut LockTableGarbage,
    ) -> Result<(), CommitError> {
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
                            let remove_hold = if let Some(hold) = doc.readers.get_mut(&session_id) {
                                hold.active_leases = hold.active_leases.saturating_sub(1);
                                hold.active_leases == 0 && !hold.retained
                            } else {
                                false
                            };
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
            if !docs_to_wake[..idx].contains(doc_ref) {
                self.wake_waiters(doc_ref);
            }
        }
        if !docs_to_wake.is_empty() {
            garbage.doc_vecs.push(docs_to_wake);
        }
        Ok(())
    }

    pub(super) fn close_session(
        &mut self,
        session_id: u64,
        timeout_reason: Option<AbortReason>,
        closed_state_code: u8,
        garbage: &mut LockTableGarbage,
    ) {
        let disposition = match timeout_reason {
            Some(reason) => CloseDisposition::TimedOut(reason),
            None => CloseDisposition::Normal(closed_state_code),
        };
        self.close_session_inner(session_id, disposition, garbage);
    }

    pub(super) fn close_expired(&mut self, session_id: u64, garbage: &mut LockTableGarbage) {
        self.close_session_inner(session_id, CloseDisposition::Expired, garbage);
    }

    fn close_session_inner(
        &mut self,
        session_id: u64,
        disposition: CloseDisposition,
        garbage: &mut LockTableGarbage,
    ) {
        let Some(rec) = self.sessions.get(&session_id) else {
            return;
        };
        if !matches!(disposition, CloseDisposition::Normal(_)) && rec.state != SessionState::Active
        {
            return;
        }

        let mut rec = self
            .sessions
            .remove(&session_id)
            .expect("session verified present");

        let cancel_err = match disposition {
            CloseDisposition::TimedOut(reason) => {
                rec.state = SessionState::TimedOut;
                rec.abort_reason = Some(reason);
                rec.shared.publish_abort(STATE_TIMED_OUT, reason);
                CommitError::Aborted(reason)
            }
            CloseDisposition::Expired => {
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
        self.strip_session_resources(
            session_id,
            &mut rec,
            &cancel_err,
            &mut affected_docs,
            garbage,
        );

        for (idx, doc_ref) in affected_docs.iter().enumerate() {
            if !affected_docs[..idx].contains(doc_ref) {
                self.wake_waiters(doc_ref);
            }
        }
        if !affected_docs.is_empty() {
            garbage.doc_vecs.push(affected_docs);
        }
        garbage.records.push(rec);
    }

    pub(super) fn abort_all(&mut self, reason: AbortReason, garbage: &mut LockTableGarbage) {
        if reason == AbortReason::DatabaseCleared {
            self.cleared = true;
        }
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
        self.docs
            .entry(doc_ref.clone())
            .or_default()
            .waiters
            .insert(
                key,
                Waiter {
                    wait_id,
                    session_id,
                    mode,
                    tx,
                },
            );
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
        garbage: &mut LockTableGarbage,
    ) {
        let held_reads = std::mem::take(&mut rec.held_reads);
        for doc_ref in &held_reads {
            if let Some(doc) = self.docs.get_mut(doc_ref)
                && doc.readers.remove(&session_id).is_some()
            {
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

    fn wake_waiters(&mut self, doc_ref: &DocumentRef) {
        let Self { docs, sessions, .. } = self;
        let remove_empty = {
            let Some(doc) = docs.get_mut(doc_ref) else {
                return;
            };
            loop {
                let Some((_, first)) = doc.waiters.first_key_value() else {
                    break doc.is_empty();
                };
                let eligible = match first.mode {
                    LockMode::Read => doc.writer.is_none(),
                    LockMode::Write => doc.writer.is_none() && doc.readers.is_empty(),
                };
                if !eligible {
                    return;
                }

                let (_, waiter) = doc.waiters.pop_first().expect("waiter existed");
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
    }

    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize) {
        (self.docs.len(), self.sessions.len())
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
