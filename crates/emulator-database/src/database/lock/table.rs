use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use googleapis::google::protobuf::Timestamp;
use tokio::sync::oneshot;

use super::{LockSession, STATE_CLEARED, STATE_COMMITTING, STATE_TIMED_OUT, STATE_WOUNDED};
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
    Cleared,
}

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
    session: Weak<LockSession>,
    txn_id: Option<TransactionId>,
    seq: u64,
    can_wound: bool,
    woundable: bool,
    state: SessionState,
    abort_reason: Option<AbortReason>,
    held_reads: HashSet<DocumentRef>,
    held_writes: HashSet<DocumentRef>,
    parked_waiters: HashMap<u64, (DocumentRef, (u64, u64))>,
    promoted_grants: HashMap<u64, (DocumentRef, LockMode)>,
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
        session: Weak<LockSession>,
        txn_id: Option<TransactionId>,
        seq: u64,
        can_wound: bool,
        woundable: bool,
    ) {
        let (state, abort_reason) = if self.cleared {
            if let Some(strong) = session.upgrade() {
                strong.publish_abort(STATE_CLEARED, AbortReason::DatabaseCleared);
            }
            (SessionState::Cleared, Some(AbortReason::DatabaseCleared))
        } else {
            (SessionState::Active, None)
        };
        self.sessions.insert(
            session_id,
            SessionRecord {
                session,
                txn_id,
                seq,
                can_wound,
                woundable,
                state,
                abort_reason,
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
        doc_ref: &DocumentRef,
        alloc_seq: &AtomicU64,
    ) -> Result<AcquireOutcome, CommitError> {
        let (txn_id, can_wound) = {
            let rec = self.active_session_mut(session_id)?;
            (rec.txn_id, rec.can_wound)
        };
        let (already_reader, already_writer) = self.docs.get(doc_ref).map_or((false, false), |d| {
            (
                d.readers.contains_key(&session_id),
                d.writer == Some(session_id),
            )
        });

        if already_writer {
            return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                "session {session_id} already holds exclusive write lease on {doc_ref}"
            ))));
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
            let (mut other_docs, wounds) =
                self.wound_holders_on(&[(doc_ref, LockMode::Read)], session_id, seq, txn_id);
            other_docs.remove(doc_ref);
            for other in other_docs {
                self.wake_waiters(&other);
            }
            wounds
        } else {
            Vec::new()
        };

        let arrival = self.next_arrival_order();
        let can_grant = {
            let doc = self.docs.get(doc_ref);
            let writer_free = doc.is_none_or(|d| d.writer.is_none());
            let earlier_write_waiter = doc.is_some_and(|d| {
                d.waiters
                    .range(..(seq, arrival))
                    .any(|(_, w)| w.mode == LockMode::Write)
            });
            writer_free && !earlier_write_waiter
        };

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
            self.sessions
                .get_mut(&session_id)
                .expect("session registered")
                .held_reads
                .insert(doc_ref.clone());
            self.wake_waiters(doc_ref);
            return Ok(AcquireOutcome::Granted {
                observation: ReadObservation::NeverRead,
                wounds,
            });
        }

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
                    mode: LockMode::Read,
                    tx,
                },
            );
        self.sessions
            .get_mut(&session_id)
            .expect("session registered")
            .parked_waiters
            .insert(wait_id, (doc_ref.clone(), key));
        self.wake_waiters(doc_ref);

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
        doc_ref: &DocumentRef,
        alloc_seq: &AtomicU64,
    ) -> Result<AcquireOutcome, CommitError> {
        let (txn_id, can_wound) = {
            let rec = self.active_session_mut(session_id)?;
            (rec.txn_id, rec.can_wound)
        };

        if self
            .docs
            .get(doc_ref)
            .is_some_and(|d| d.writer == Some(session_id))
        {
            return Err(CommitError::Other(GenericDatabaseError::internal(format!(
                "duplicate exclusive write lease requested by session {session_id} on {doc_ref}"
            ))));
        }

        let observation = self
            .docs
            .get(doc_ref)
            .and_then(|d| d.readers.get(&session_id))
            .and_then(|hold| hold.observation)
            .map_or(ReadObservation::NeverRead, ReadObservation::Observed);

        let seq = self.ensure_session_seq(session_id, alloc_seq);
        let wounds = if can_wound {
            let (mut other_docs, wounds) =
                self.wound_holders_on(&[(doc_ref, LockMode::Write)], session_id, seq, txn_id);
            other_docs.remove(doc_ref);
            for other in other_docs {
                self.wake_waiters(&other);
            }
            wounds
        } else {
            Vec::new()
        };

        let arrival = self.next_arrival_order();
        let can_grant = {
            let doc = self.docs.get(doc_ref);
            let writer_free = doc.is_none_or(|d| d.writer.is_none());
            let other_readers =
                doc.is_some_and(|d| d.readers.keys().any(|&reader_id| reader_id != session_id));
            let earlier_waiter =
                doc.is_some_and(|d| d.waiters.range(..(seq, arrival)).next().is_some());
            writer_free && !other_readers && !earlier_waiter
        };

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
                    mode: LockMode::Write,
                    tx,
                },
            );
        self.sessions
            .get_mut(&session_id)
            .expect("session exists")
            .parked_waiters
            .insert(wait_id, (doc_ref.clone(), key));
        self.wake_waiters(doc_ref);

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
        docs: &BTreeSet<DocumentRef>,
        alloc_seq: &AtomicU64,
    ) -> Result<BatchAcquireOutcome, CommitError> {
        let (txn_id, can_wound) = {
            let rec = self.active_session_mut(session_id)?;
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
            let (mut other_docs, wounds) = self.wound_holders_on(&targets, session_id, seq, txn_id);
            for target in docs {
                other_docs.remove(target);
            }
            for other in other_docs {
                self.wake_waiters(&other);
            }
            wounds
        } else {
            Vec::new()
        };

        let mut granted_docs = Vec::new();
        let mut pending = Vec::new();
        let mut waiters_to_wake = Vec::new();

        for doc_ref in docs {
            let arrival = self.next_arrival_order();
            let can_grant = {
                let doc = self.docs.get(doc_ref);
                let free = doc.is_none_or(|d| d.writer.is_none() && d.readers.is_empty());
                let earlier_waiter =
                    doc.is_some_and(|d| d.waiters.range(..(seq, arrival)).next().is_some());
                free && !earlier_waiter
            };

            if can_grant {
                self.docs.entry(doc_ref.clone()).or_default().writer = Some(session_id);
                self.sessions
                    .get_mut(&session_id)
                    .expect("session exists")
                    .held_writes
                    .insert(doc_ref.clone());
                granted_docs.push(doc_ref.clone());
            } else {
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
                            mode: LockMode::Write,
                            tx,
                        },
                    );
                self.sessions
                    .get_mut(&session_id)
                    .expect("session exists")
                    .parked_waiters
                    .insert(wait_id, (doc_ref.clone(), key));
                pending.push(PendingWait {
                    doc_ref: doc_ref.clone(),
                    wait_id,
                    rx,
                });
                waiters_to_wake.push(doc_ref.clone());
            }
        }

        for doc_ref in waiters_to_wake {
            self.wake_waiters(&doc_ref);
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
        doc_ref: &DocumentRef,
        observed_update_time: Option<Timestamp>,
    ) -> Result<(), AbortReason> {
        if self.cleared {
            return Err(AbortReason::DatabaseCleared);
        }
        let Some(rec) = self.sessions.get(&session_id) else {
            return Err(AbortReason::DatabaseCleared);
        };
        if rec.state != SessionState::Active {
            return Err(rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared));
        }

        let Some(doc) = self.docs.get_mut(doc_ref) else {
            return Err(AbortReason::DatabaseCleared);
        };
        let Some(hold) = doc.readers.get_mut(&session_id) else {
            return Err(AbortReason::DatabaseCleared);
        };
        hold.active_leases = hold.active_leases.saturating_sub(1);
        hold.retained = true;
        // Preserve the transaction's earliest read observation on this document.
        hold.observation.get_or_insert(observed_update_time);
        Ok(())
    }

    pub(super) fn release_read_lease(&mut self, session_id: u64, doc_ref: &DocumentRef) {
        let mut wake_doc = false;
        let mut prune_doc = false;
        if let Some(doc) = self.docs.get_mut(doc_ref) {
            if let Some(hold) = doc.readers.get_mut(&session_id) {
                hold.active_leases = hold.active_leases.saturating_sub(1);
                if hold.active_leases == 0 && !hold.retained {
                    doc.readers.remove(&session_id);
                    if let Some(rec) = self.sessions.get_mut(&session_id) {
                        rec.held_reads.remove(doc_ref);
                    }
                    wake_doc = true;
                }
            }
            prune_doc = doc.is_empty();
        }
        if wake_doc {
            self.wake_waiters(doc_ref);
        } else if prune_doc {
            self.docs.remove(doc_ref);
        }
    }

    pub(super) fn release_write_lease(&mut self, session_id: u64, doc_ref: &DocumentRef) {
        let mut wake_doc = false;
        if let Some(doc) = self.docs.get_mut(doc_ref)
            && doc.writer == Some(session_id)
        {
            doc.writer = None;
            if let Some(rec) = self.sessions.get_mut(&session_id) {
                rec.held_writes.remove(doc_ref);
            }
            wake_doc = true;
        }
        if wake_doc {
            self.wake_waiters(doc_ref);
        }
    }

    pub(super) fn consume_wait_grant(
        &mut self,
        session_id: u64,
        wait_id: u64,
    ) -> Result<(), AbortReason> {
        if self.cleared {
            return Err(AbortReason::DatabaseCleared);
        }
        let Some(rec) = self.sessions.get_mut(&session_id) else {
            return Err(AbortReason::DatabaseCleared);
        };
        if rec.state != SessionState::Active {
            return Err(rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared));
        }
        if rec.promoted_grants.remove(&wait_id).is_some() {
            Ok(())
        } else {
            Err(rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared))
        }
    }

    pub(super) fn revoke_waiter(&mut self, session_id: u64, wait_id: u64) {
        let parked = self
            .sessions
            .get_mut(&session_id)
            .and_then(|rec| rec.parked_waiters.remove(&wait_id));
        if let Some((doc_ref, key)) = parked {
            let mut wake_doc = false;
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                doc.waiters.remove(&key);
                wake_doc = true;
            }
            if wake_doc {
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
        write_docs: &BTreeSet<DocumentRef>,
    ) -> Result<(), AbortReason> {
        if self.cleared {
            return Err(AbortReason::DatabaseCleared);
        }
        {
            let Some(rec) = self.sessions.get(&session_id) else {
                return Err(AbortReason::DatabaseCleared);
            };
            if rec.state != SessionState::Active {
                return Err(rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared));
            }
            for doc_ref in write_docs {
                if self.docs.get(doc_ref).and_then(|d| d.writer) != Some(session_id) {
                    return Err(rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared));
                }
            }
        }

        let mut docs_to_wake = BTreeSet::new();
        if let Some(rec) = self.sessions.get_mut(&session_id) {
            rec.state = SessionState::Committing;
            if let Some(strong) = rec.session.upgrade() {
                strong.publish_state(STATE_COMMITTING);
            }
            for doc_ref in rec.held_reads.drain() {
                if let Some(doc) = self.docs.get_mut(&doc_ref) {
                    doc.readers.remove(&session_id);
                }
                docs_to_wake.insert(doc_ref);
            }
        }

        for doc_ref in docs_to_wake {
            self.wake_waiters(&doc_ref);
        }
        Ok(())
    }

    pub(super) fn close_session(
        &mut self,
        session_id: u64,
        timeout_reason: Option<AbortReason>,
        closed_state_code: u8,
    ) {
        let Some(mut rec) = self.sessions.remove(&session_id) else {
            return;
        };

        if let Some(reason) = timeout_reason {
            rec.state = SessionState::TimedOut;
            rec.abort_reason = Some(reason);
            if let Some(strong) = rec.session.upgrade() {
                strong.publish_abort(STATE_TIMED_OUT, reason);
            }
        } else if (rec.state == SessionState::Active || rec.state == SessionState::Committing)
            && let Some(strong) = rec.session.upgrade()
        {
            strong.publish_state(closed_state_code);
        }

        let mut affected_docs = BTreeSet::new();
        let cancel_err = timeout_reason.map(CommitError::Aborted).unwrap_or_else(|| {
            CommitError::Other(GenericDatabaseError::invalid_argument("transaction closed"))
        });
        self.strip_session_resources(session_id, &mut rec, cancel_err, &mut affected_docs);

        for doc_ref in affected_docs {
            self.wake_waiters(&doc_ref);
        }
    }

    pub(super) fn abort_all(&mut self, reason: AbortReason) {
        if reason == AbortReason::DatabaseCleared {
            self.cleared = true;
        }
        let session_ids: Vec<u64> = self.sessions.keys().copied().collect();
        for sid in session_ids {
            if let Some(mut rec) = self.sessions.remove(&sid) {
                rec.state = SessionState::Cleared;
                rec.abort_reason = Some(reason);
                if let Some(strong) = rec.session.upgrade() {
                    strong.publish_abort(STATE_CLEARED, reason);
                }
                let mut ignored = BTreeSet::new();
                self.strip_session_resources(
                    sid,
                    &mut rec,
                    CommitError::Aborted(reason),
                    &mut ignored,
                );
            }
        }
        self.docs.clear();
    }

    fn active_session_mut(&mut self, session_id: u64) -> Result<&mut SessionRecord, CommitError> {
        if self.cleared {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        let Some(rec) = self.sessions.get_mut(&session_id) else {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        };
        if rec.state != SessionState::Active {
            return Err(CommitError::Aborted(
                rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared),
            ));
        }
        Ok(rec)
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
            if let Some(strong) = rec.session.upgrade() {
                strong.publish_seq(allocated);
            }
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

    fn wound_holders_on(
        &mut self,
        targets: &[(&DocumentRef, LockMode)],
        requester_id: u64,
        requester_seq: u64,
        requester_txn_id: Option<TransactionId>,
    ) -> (BTreeSet<DocumentRef>, Vec<WoundEvent>) {
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
            return (BTreeSet::new(), Vec::new());
        }

        let reason = AbortReason::Wounded {
            wounder_txn_id: requester_txn_id,
            wounder_seq:    requester_seq,
        };
        let mut affected_docs = BTreeSet::new();
        let mut events = Vec::with_capacity(eligible.len());

        for (victim_id, trigger_doc) in eligible {
            // Temporarily remove victim from map to satisfy borrow checker cleanly while mutating
            // `self.docs`, then re-insert as a Wounded record so subsequent lookups see Wounded.
            let Some(mut rec) = self.sessions.remove(&victim_id) else {
                continue;
            };
            rec.state = SessionState::Wounded;
            rec.abort_reason = Some(reason);
            if let Some(strong) = rec.session.upgrade() {
                strong.publish_abort(STATE_WOUNDED, reason);
            }
            events.push(WoundEvent {
                victim_session_id: victim_id,
                victim_txn_id: rec.txn_id,
                victim_seq: rec.seq,
                doc_ref: trigger_doc,
                reason,
            });
            self.strip_session_resources(
                victim_id,
                &mut rec,
                CommitError::Aborted(reason),
                &mut affected_docs,
            );
            self.sessions.insert(victim_id, rec);
        }

        (affected_docs, events)
    }

    fn strip_session_resources(
        &mut self,
        session_id: u64,
        rec: &mut SessionRecord,
        cancel_err: CommitError,
        affected_docs: &mut BTreeSet<DocumentRef>,
    ) {
        for doc_ref in rec.held_reads.drain() {
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                doc.readers.remove(&session_id);
                if doc.is_empty() {
                    self.docs.remove(&doc_ref);
                }
            }
            affected_docs.insert(doc_ref);
        }

        for doc_ref in rec.held_writes.drain() {
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                if doc.writer == Some(session_id) {
                    doc.writer = None;
                }
                if doc.is_empty() {
                    self.docs.remove(&doc_ref);
                }
            }
            affected_docs.insert(doc_ref);
        }

        for (_, (doc_ref, mode)) in rec.promoted_grants.drain() {
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                match mode {
                    LockMode::Read => {
                        doc.readers.remove(&session_id);
                    }
                    LockMode::Write => {
                        if doc.writer == Some(session_id) {
                            doc.writer = None;
                        }
                    }
                }
                if doc.is_empty() {
                    self.docs.remove(&doc_ref);
                }
            }
            affected_docs.insert(doc_ref);
        }

        for (_, (doc_ref, key)) in rec.parked_waiters.drain() {
            if let Some(doc) = self.docs.get_mut(&doc_ref) {
                if let Some(waiter) = doc.waiters.remove(&key) {
                    let _unused = waiter
                        .tx
                        .send(Err(CommitError::from_abort_or_clone(&cancel_err)));
                }
                if doc.is_empty() {
                    self.docs.remove(&doc_ref);
                }
            }
            affected_docs.insert(doc_ref);
        }
    }

    fn wake_waiters(&mut self, doc_ref: &DocumentRef) {
        loop {
            let next_key = {
                let Some(doc) = self.docs.get(doc_ref) else {
                    return;
                };
                let Some((&key, first)) = doc.waiters.first_key_value() else {
                    if doc.is_empty() {
                        self.docs.remove(doc_ref);
                    }
                    return;
                };
                let eligible = match first.mode {
                    LockMode::Read => doc.writer.is_none(),
                    LockMode::Write => doc.writer.is_none() && doc.readers.is_empty(),
                };
                if !eligible {
                    return;
                }
                key
            };

            let waiter = self
                .docs
                .get_mut(doc_ref)
                .and_then(|d| d.waiters.remove(&next_key))
                .expect("waiter existed");

            let Some(rec) = self.sessions.get_mut(&waiter.session_id) else {
                continue;
            };
            rec.parked_waiters.remove(&waiter.wait_id);

            if rec.state != SessionState::Active {
                let reason = rec.abort_reason.unwrap_or(AbortReason::DatabaseCleared);
                let _unused = waiter.tx.send(Err(CommitError::Aborted(reason)));
                continue;
            }

            let doc = self.docs.get_mut(doc_ref).expect("doc exists");
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
                // Receiver already dropped before wake completed: revoke our unconsumed promotion
                // immediately and keep scanning the queue.
                rec.promoted_grants.remove(&waiter.wait_id);
                match waiter.mode {
                    LockMode::Read => {
                        let remove_hold =
                            if let Some(hold) = doc.readers.get_mut(&waiter.session_id) {
                                hold.active_leases = hold.active_leases.saturating_sub(1);
                                hold.active_leases == 0 && !hold.retained
                            } else {
                                false
                            };
                        if remove_hold {
                            doc.readers.remove(&waiter.session_id);
                            rec.held_reads.remove(doc_ref);
                        }
                    }
                    LockMode::Write => {
                        if doc.writer == Some(waiter.session_id) {
                            doc.writer = None;
                        }
                        rec.held_writes.remove(doc_ref);
                    }
                }
            }
        }
    }
}

impl CommitError {
    fn from_abort_or_clone(err: &CommitError) -> Self {
        match err {
            CommitError::Aborted(reason) => CommitError::Aborted(*reason),
            CommitError::Other(other) => CommitError::Other(other.clone()),
        }
    }
}
