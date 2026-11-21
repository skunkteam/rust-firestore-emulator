use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, VecDeque},
    fmt::Display,
    sync::{
        Arc, PoisonError, RwLock, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use googleapis::google::protobuf::Timestamp;
use tokio::{sync::Notify, time::Instant};

use super::{ReadOnlyTransaction, ReadWriteTransaction, Transaction};
use crate::{
    FirestoreDatabase, GenericDatabaseError,
    database::{
        lock::{LockManager, LockPriority, LockSession, SessionAbortState},
        read_consistency::validate_read_time,
        timeouts::Timeouts,
    },
    error::{AbortReason, EXPIRED_TRANSACTION_MSG, Result},
};

pub(crate) const MAX_RETRY_TOMBSTONES: usize = 4_096;
pub(crate) const MAX_CLOSED_TXNS: usize = 4_096;
pub(crate) const MAX_EXPIRED_TXNS: usize = 4_096;

static NEXT_INSTANCE_ID: AtomicU32 = AtomicU32::new(1);

fn duration_to_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// 16-byte rotated transaction wire token encoding `(lineage_id, attempt_gen)`.
///
/// Every fresh `BeginTransaction` allocates a unique non-zero `lineage_id` packing
/// `(instance_id << 32) | local_seq` with `attempt_gen == 1`. Each
/// `BeginTransaction(retry_transaction)` on that lineage increments `attempt_gen`, rotating the
/// wire bytes while preserving the underlying lineage priority inside [`RunningTransactions`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId {
    lineage_id:  u64,
    attempt_gen: u64,
}

impl TransactionId {
    const LOCAL_SEQ_MASK: u64 = 0xFFFF_FFFF;

    fn from_instance_seq(instance_id: u32, local_seq: u32) -> Self {
        debug_assert!(instance_id != 0);
        debug_assert!(local_seq != 0);
        Self {
            lineage_id:  (u64::from(instance_id) << 32) | u64::from(local_seq),
            attempt_gen: 1,
        }
    }

    fn instance_id(self) -> u32 {
        (self.lineage_id >> 32) as u32
    }

    fn local_seq(self) -> u64 {
        self.lineage_id & Self::LOCAL_SEQ_MASK
    }

    pub(crate) fn with_attempt(self, attempt_gen: u64) -> Self {
        Self {
            lineage_id: self.lineage_id,
            attempt_gen,
        }
    }

    #[cfg(test)]
    pub(crate) fn lineage_id(self) -> u64 {
        self.lineage_id
    }

    #[cfg(test)]
    pub(crate) fn attempt_gen(self) -> u64 {
        self.attempt_gen
    }

    pub(crate) fn from_parts(lineage_id: u64, attempt_gen: u64) -> Self {
        Self {
            lineage_id,
            attempt_gen,
        }
    }

    /// Check if the given [`TransactionId`] could have been issued by the currently running
    /// process. This validates non-zero lineage/attempt fields and `instance_id` / `local_seq`
    /// bounds without binding to a specific database instance.
    pub(crate) fn check(self) -> Result<()> {
        let instance_id = self.instance_id();
        let local_seq = self.local_seq();
        if self.lineage_id != 0
            && self.attempt_gen >= 1
            && (1..NEXT_INSTANCE_ID.load(Ordering::Acquire)).contains(&instance_id)
            && local_seq >= 1
        {
            Ok(())
        } else {
            Err(GenericDatabaseError::InvalidArgument(format!(
                "Invalid transaction ID, {self} has not been issued by this instance"
            )))
        }
    }

    /// Check if the given [`TransactionId`] belongs to `instance_id` and has a `local_seq`
    /// already issued by that registry instance (`1..next_local_seq`).
    pub(crate) fn check_for_instance(self, instance_id: u32, next_local_seq: u64) -> Result<()> {
        self.check()?;
        if instance_id != 0
            && self.instance_id() == instance_id
            && (1..next_local_seq).contains(&self.local_seq())
        {
            Ok(())
        } else {
            Err(GenericDatabaseError::InvalidArgument(format!(
                "Invalid transaction ID, {self} has not been issued by this instance"
            )))
        }
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.lineage_id, self.attempt_gen)
    }
}

impl TryFrom<&[u8]> for TransactionId {
    type Error = GenericDatabaseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 16] = value.try_into().map_err(|_| {
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {value:?}"))
        })?;
        let lineage_id = u64::from_le_bytes(bytes[..8].try_into().expect("8-byte slice"));
        let attempt_gen = u64::from_le_bytes(bytes[8..].try_into().expect("8-byte slice"));
        if lineage_id == 0 || attempt_gen == 0 {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "invalid transaction ID: {bytes:?}"
            )));
        }
        Ok(Self {
            lineage_id,
            attempt_gen,
        })
    }
}

impl TryFrom<Vec<u8>> for TransactionId {
    type Error = GenericDatabaseError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

impl From<TransactionId> for Vec<u8> {
    fn from(val: TransactionId) -> Self {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&val.lineage_id.to_le_bytes());
        out.extend_from_slice(&val.attempt_gen.to_le_bytes());
        out
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClosedOutcome {
    Committed,
    AbortedOrRolledBack,
}

#[derive(Debug)]
struct ActiveEntry {
    txn: Arc<Transaction>,
    unused_expiry_ns: u64,
    used_min_expiry_ns: u64,
    max_expiry_ns: u64,
    last_activity_ns: AtomicU64,
    used: AtomicBool,
    heap_deadline_ns: u64,
}

impl ActiveEntry {
    fn new(txn: Arc<Transaction>, created_ns: u64, timeouts: &Timeouts) -> Self {
        let max_ns = duration_to_ns(timeouts.txn_max);
        let idle_ns = duration_to_ns(timeouts.txn_idle).min(max_ns);
        let used_min_ns = duration_to_ns(timeouts.txn_used_min).min(max_ns);
        let unused_expiry_ns = created_ns.saturating_add(idle_ns);
        Self {
            txn,
            unused_expiry_ns,
            used_min_expiry_ns: created_ns.saturating_add(used_min_ns),
            max_expiry_ns: created_ns.saturating_add(max_ns),
            last_activity_ns: AtomicU64::new(created_ns),
            used: AtomicBool::new(false),
            heap_deadline_ns: unused_expiry_ns,
        }
    }

    fn effective_expiry(&self, idle_ns: u64) -> u64 {
        if !self.used.load(Ordering::Acquire) {
            return self.unused_expiry_ns;
        }
        let last_ns = self.last_activity_ns.load(Ordering::Acquire);
        self.max_expiry_ns
            .min(self.used_min_expiry_ns.max(last_ns.saturating_add(idle_ns)))
    }

    fn record_activity(&self, now_ns: u64) {
        self.last_activity_ns.fetch_max(now_ns, Ordering::AcqRel);
        self.used.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExpiryKey {
    deadline_ns: u64,
    lineage_id:  u64,
    attempt_gen: u64,
}

#[derive(Debug)]
enum TxnSlot {
    Active(ActiveEntry),
    Committing,
    Wounded {
        root_seq: Option<u64>,
        reason: AbortReason,
        expiry_ns: u64,
        max_expiry_ns: u64,
        ring_gen: u64,
    },
    AbortedLineage {
        root_seq:      Option<u64>,
        max_expiry_ns: u64,
        ring_gen:      u64,
    },
    Expired {
        read_only:     bool,
        root_seq:      Option<u64>,
        max_expiry_ns: u64,
        ring_gen:      u64,
    },
    Closed {
        outcome:   ClosedOutcome,
        read_only: bool,
        ring_gen:  u64,
    },
}

#[derive(Debug)]
struct LineageEntry {
    current_attempt: u64,
    slot: TxnSlot,
}

#[derive(Debug, Default)]
struct BoundedRing {
    order:    VecDeque<(u64, u64)>,
    count:    usize,
    next_gen: u64,
}

impl BoundedRing {
    fn push(&mut self, lineage_id: u64) -> u64 {
        self.next_gen = self.next_gen.wrapping_add(1).max(1);
        let gen_id = self.next_gen;
        self.count += 1;
        self.order.push_back((lineage_id, gen_id));
        gen_id
    }

    fn decrement(&mut self) {
        self.count = self.count.saturating_sub(1);
    }

    fn clear(&mut self) {
        self.order.clear();
        self.count = 0;
    }

    fn evict(
        &mut self,
        max_len: usize,
        slots: &mut HashMap<u64, LineageEntry>,
        matches_ring: impl Fn(&TxnSlot, u64) -> bool,
    ) {
        while self.count > max_len {
            let Some((old_id, old_gen)) = self.order.pop_front() else {
                break;
            };
            let matches_live = slots
                .get(&old_id)
                .is_some_and(|e| matches_ring(&e.slot, old_gen));
            if matches_live {
                slots.remove(&old_id);
                self.count -= 1;
            }
        }
        if self.order.len() > max_len.saturating_mul(2) {
            self.order.retain(|(entry_id, entry_gen)| {
                slots
                    .get(entry_id)
                    .is_some_and(|e| matches_ring(&e.slot, *entry_gen))
            });
        }
    }
}

#[derive(Debug, Default)]
struct RunningTxnMap {
    slots: HashMap<u64, LineageEntry>,
    active_count: usize,
    expiry_heap: BinaryHeap<Reverse<ExpiryKey>>,
    retry_ring: BoundedRing,
    closed_ring: BoundedRing,
    expired_rw_ring: BoundedRing,
    expired_ro_ring: BoundedRing,
}

impl RunningTxnMap {
    fn decrement_slot_counters(&mut self, slot: &TxnSlot) {
        match slot {
            TxnSlot::Active(_) => {
                self.active_count = self.active_count.saturating_sub(1);
            }
            TxnSlot::Wounded { .. } | TxnSlot::AbortedLineage { .. } => {
                self.retry_ring.decrement();
            }
            TxnSlot::Closed { .. } => {
                self.closed_ring.decrement();
            }
            TxnSlot::Expired {
                read_only: false, ..
            } => {
                self.expired_rw_ring.decrement();
            }
            TxnSlot::Expired {
                read_only: true, ..
            } => {
                self.expired_ro_ring.decrement();
            }
            TxnSlot::Committing => {}
        }
    }

    fn remove_slot(&mut self, lineage_id: u64) -> Option<Arc<Transaction>> {
        let entry = self.slots.remove(&lineage_id)?;
        self.decrement_slot_counters(&entry.slot);
        if self.active_count == 0 {
            self.expiry_heap.clear();
        }
        match entry.slot {
            TxnSlot::Active(active) => Some(active.txn),
            _ => None,
        }
    }

    fn set_slot(
        &mut self,
        lineage_id: u64,
        current_attempt: u64,
        mut new_slot: TxnSlot,
    ) -> Option<Arc<Transaction>> {
        let dropped = if let Some(prev) = self.slots.remove(&lineage_id) {
            self.decrement_slot_counters(&prev.slot);
            match prev.slot {
                TxnSlot::Active(active) => Some(active.txn),
                _ => None,
            }
        } else {
            None
        };

        match &mut new_slot {
            TxnSlot::Wounded { ring_gen, .. } | TxnSlot::AbortedLineage { ring_gen, .. } => {
                *ring_gen = self.retry_ring.push(lineage_id);
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                self.retry_ring
                    .evict(MAX_RETRY_TOMBSTONES, &mut self.slots, |slot, gen_id| {
                        matches!(
                            slot,
                            TxnSlot::Wounded { ring_gen, .. }
                                | TxnSlot::AbortedLineage { ring_gen, .. }
                                if *ring_gen == gen_id
                        )
                    });
            }
            TxnSlot::Closed { ring_gen, .. } => {
                *ring_gen = self.closed_ring.push(lineage_id);
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                self.closed_ring
                    .evict(MAX_CLOSED_TXNS, &mut self.slots, |slot, gen_id| {
                        matches!(slot, TxnSlot::Closed { ring_gen, .. } if *ring_gen == gen_id)
                    });
            }
            TxnSlot::Expired {
                read_only: false,
                ring_gen,
                ..
            } => {
                *ring_gen = self.expired_rw_ring.push(lineage_id);
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                self.expired_rw_ring
                    .evict(MAX_EXPIRED_TXNS, &mut self.slots, |slot, gen_id| {
                        matches!(
                            slot,
                            TxnSlot::Expired {
                                read_only: false,
                                ring_gen,
                                ..
                            } if *ring_gen == gen_id
                        )
                    });
            }
            TxnSlot::Expired {
                read_only: true,
                ring_gen,
                ..
            } => {
                *ring_gen = self.expired_ro_ring.push(lineage_id);
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                self.expired_ro_ring
                    .evict(MAX_EXPIRED_TXNS, &mut self.slots, |slot, gen_id| {
                        matches!(
                            slot,
                            TxnSlot::Expired {
                                read_only: true,
                                ring_gen,
                                ..
                            } if *ring_gen == gen_id
                        )
                    });
            }
            TxnSlot::Active(active) => {
                self.active_count += 1;
                self.expiry_heap.push(Reverse(ExpiryKey {
                    deadline_ns: active.heap_deadline_ns,
                    lineage_id,
                    attempt_gen: current_attempt,
                }));
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                if self.expiry_heap.len() > self.active_count.saturating_mul(2).max(MAX_CLOSED_TXNS)
                {
                    self.compact_expiry_heap();
                }
            }
            TxnSlot::Committing => {
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
            }
        }
        if self.active_count == 0 {
            self.expiry_heap.clear();
        }
        dropped
    }

    fn compact_expiry_heap(&mut self) {
        let slots = &self.slots;
        self.expiry_heap.retain(|Reverse(key)| {
            matches!(
                slots.get(&key.lineage_id),
                Some(LineageEntry {
                    current_attempt,
                    slot: TxnSlot::Active(active),
                }) if *current_attempt == key.attempt_gen
                    && active.heap_deadline_ns == key.deadline_ns
            )
        });
    }

    fn refresh_next_expiry(&mut self) -> u64 {
        if self.active_count == 0 {
            self.expiry_heap.clear();
            return u64::MAX;
        }
        while let Some(&Reverse(top)) = self.expiry_heap.peek() {
            let matches_live = matches!(
                self.slots.get(&top.lineage_id),
                Some(LineageEntry {
                    current_attempt,
                    slot: TxnSlot::Active(active),
                }) if *current_attempt == top.attempt_gen
                    && active.heap_deadline_ns == top.deadline_ns
            );
            if matches_live {
                return top.deadline_ns;
            }
            self.expiry_heap.pop();
        }
        u64::MAX
    }

    fn clear_all(&mut self) -> Vec<Arc<Transaction>> {
        let mut active_txns = Vec::new();
        for (_, entry) in self.slots.drain() {
            if let TxnSlot::Active(active) = entry.slot {
                active_txns.push(active.txn);
            }
        }
        self.active_count = 0;
        self.expiry_heap.clear();
        self.retry_ring.clear();
        self.closed_ring.clear();
        self.expired_rw_ring.clear();
        self.expired_ro_ring.clear();
        active_txns
    }
}

#[derive(Debug)]
pub(crate) enum CommitTake {
    ReadWrite((Arc<ReadWriteTransaction>, CommitSlotGuard)),
    ReadOnly(Arc<ReadOnlyTransaction>),
}

/// RAII reservation guard keeping a `ReadWrite` transaction slot in [`TxnSlot::Committing`] from
/// [`RunningTransactions::take_for_commit`] until commit completion or cancellation/drop.
#[derive(Debug)]
pub(crate) struct CommitSlotGuard {
    database: Weak<FirestoreDatabase>,
    locks: Arc<LockManager>,
    state: Arc<RwLock<RunningTxnMap>>,
    id: TransactionId,
    session: Arc<LockSession>,
    expiry_ns: u64,
    max_expiry_ns: u64,
    armed: bool,
}

impl CommitSlotGuard {
    fn finalize_slot(&self, cleared: bool, next_slot: impl FnOnce() -> TxnSlot) {
        let dropped = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if matches!(
                map.slots.get(&self.id.lineage_id),
                Some(LineageEntry {
                    current_attempt,
                    slot: TxnSlot::Committing,
                }) if *current_attempt == self.id.attempt_gen
            ) {
                if cleared {
                    map.remove_slot(self.id.lineage_id)
                } else {
                    map.set_slot(self.id.lineage_id, self.id.attempt_gen, next_slot())
                }
            } else {
                None
            }
        };
        drop(dropped);
    }

    pub(crate) fn finish_committed(mut self) {
        self.armed = false;
        let cleared = is_cleared(&self.database, &self.locks);
        self.finalize_slot(cleared, || TxnSlot::Closed {
            outcome:   ClosedOutcome::Committed,
            read_only: false,
            ring_gen:  0,
        });
    }

    pub(crate) fn abort_commit(mut self, abort_reason: Option<AbortReason>) {
        self.armed = false;
        let root_seq = self.session.root_seq();
        if matches!(abort_reason, Some(AbortReason::TransactionExpired)) {
            self.session.close_expired();
        } else {
            self.session.close();
        }

        let cleared = is_cleared(&self.database, &self.locks)
            || matches!(
                abort_reason,
                Some(AbortReason::DatabaseCleared | AbortReason::DatabaseDropped)
            );
        self.finalize_slot(cleared, || match abort_reason {
            Some(reason @ AbortReason::Wounded { .. }) => TxnSlot::Wounded {
                root_seq,
                reason,
                expiry_ns: self.expiry_ns,
                max_expiry_ns: self.max_expiry_ns,
                ring_gen: 0,
            },
            Some(
                AbortReason::UpgradeContention
                | AbortReason::OptimisticWriteSkew
                | AbortReason::WriteTimeout
                | AbortReason::ReadTimeout,
            ) => TxnSlot::AbortedLineage {
                root_seq,
                max_expiry_ns: self.max_expiry_ns,
                ring_gen: 0,
            },
            Some(AbortReason::TransactionExpired) => TxnSlot::Expired {
                read_only: false,
                root_seq,
                max_expiry_ns: self.max_expiry_ns,
                ring_gen: 0,
            },
            Some(AbortReason::DatabaseCleared | AbortReason::DatabaseDropped) => {
                unreachable!("handled above")
            }
            None => TxnSlot::Closed {
                outcome:   ClosedOutcome::AbortedOrRolledBack,
                read_only: false,
                ring_gen:  0,
            },
        });
    }
}

impl Drop for CommitSlotGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.session.close();
        let cleared = is_cleared(&self.database, &self.locks);
        self.finalize_slot(cleared, || TxnSlot::Closed {
            outcome:   ClosedOutcome::AbortedOrRolledBack,
            read_only: false,
            ring_gen:  0,
        });
    }
}

#[derive(Debug)]
pub(crate) struct RunningTransactions {
    instance_id: u32,
    next_local_seq: AtomicU64,
    database: Weak<FirestoreDatabase>,
    locks: Arc<LockManager>,
    timeouts: Timeouts,
    epoch: Instant,
    next_expiry_ns: AtomicU64,
    expiry_notify: Arc<Notify>,
    state: Arc<RwLock<RunningTxnMap>>,
}

impl RunningTransactions {
    pub(crate) fn new(
        database: Weak<FirestoreDatabase>,
        locks: Arc<LockManager>,
        timeouts: Timeouts,
    ) -> Self {
        let instance_id = NEXT_INSTANCE_ID
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                cur.checked_add(1)
            })
            .expect("transaction registry instance_id exhausted");
        let expiry_notify = locks.expiry_notify();
        Self {
            instance_id,
            next_local_seq: AtomicU64::new(1),
            database,
            locks,
            timeouts,
            epoch: Instant::now(),
            next_expiry_ns: AtomicU64::new(u64::MAX),
            expiry_notify,
            state: Default::default(),
        }
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(
            Instant::now()
                .saturating_duration_since(self.epoch)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX)
    }

    fn instant_from_ns(&self, ns: u64) -> Option<Instant> {
        if ns == u64::MAX {
            return None;
        }
        self.epoch.checked_add(Duration::from_nanos(ns))
    }

    fn new_lineage_id(&self) -> TransactionId {
        let local_seq = self.next_local_seq.fetch_add(1, Ordering::AcqRel);
        assert!(
            local_seq <= u64::from(u32::MAX),
            "transaction lineage local sequence exhausted"
        );
        TransactionId::from_instance_seq(self.instance_id, local_seq as u32)
    }

    fn check_id(&self, id: TransactionId) -> Result<()> {
        id.check_for_instance(
            self.instance_id,
            self.next_local_seq.load(Ordering::Acquire),
        )
    }

    pub(crate) fn clear_all(&self) {
        let txns = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            let txns = map.clear_all();
            self.next_expiry_ns.store(u64::MAX, Ordering::Release);
            txns
        };
        drop(txns);
    }

    pub(crate) fn reap_if_due(&self) {
        let _ = self.reap_and_next_expiry();
    }

    pub(crate) fn reap_and_next_expiry(&self) -> Option<Instant> {
        let now_ns = self.now_ns();
        let cached_next = self.next_expiry_ns.load(Ordering::Acquire);
        if now_ns < cached_next {
            return self.instant_from_ns(cached_next);
        }

        let mut sessions_to_expire = Vec::new();
        let mut txns_to_drop = Vec::new();
        let next_ns = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                txns_to_drop = map.clear_all();
                self.next_expiry_ns.store(u64::MAX, Ordering::Release);
                u64::MAX
            } else {
                let write_now_ns = self.now_ns();
                self.reap_due_locked(
                    &mut map,
                    write_now_ns,
                    &mut sessions_to_expire,
                    &mut txns_to_drop,
                );
                self.next_expiry_ns.load(Ordering::Acquire)
            }
        };
        finish_write_cleanup(sessions_to_expire, Vec::new(), txns_to_drop);
        self.instant_from_ns(next_ns)
    }

    fn reap_due_locked(
        &self,
        map: &mut RunningTxnMap,
        now_ns: u64,
        sessions_to_expire: &mut Vec<Arc<LockSession>>,
        txns_to_drop: &mut Vec<Arc<Transaction>>,
    ) {
        let idle_ns = duration_to_ns(self.timeouts.txn_idle);
        while let Some(&Reverse(top)) = map.expiry_heap.peek() {
            if top.deadline_ns > now_ns {
                break;
            }
            map.expiry_heap.pop();

            let Some(entry) = map.slots.get(&top.lineage_id) else {
                continue;
            };
            if entry.current_attempt != top.attempt_gen {
                continue;
            }
            let TxnSlot::Active(active) = &entry.slot else {
                continue;
            };
            if active.heap_deadline_ns != top.deadline_ns {
                continue;
            }

            let true_deadline = active.effective_expiry(idle_ns);
            if true_deadline > now_ns {
                let Some(TxnSlot::Active(active_mut)) =
                    map.slots.get_mut(&top.lineage_id).map(|e| &mut e.slot)
                else {
                    unreachable!("entry verified Active");
                };
                active_mut.heap_deadline_ns = true_deadline;
                map.expiry_heap.push(Reverse(ExpiryKey {
                    deadline_ns: true_deadline,
                    lineage_id:  top.lineage_id,
                    attempt_gen: top.attempt_gen,
                }));
                continue;
            }

            let id = TransactionId::from_parts(top.lineage_id, top.attempt_gen);
            let max_expiry_ns = active.max_expiry_ns;
            let dropped = match active_abort_promotion(&active.txn) {
                Some(promoted) => {
                    let (dropped, _) = promote_active_slot(
                        map,
                        id,
                        promoted,
                        true_deadline,
                        max_expiry_ns,
                        now_ns,
                    );
                    dropped
                }
                None => {
                    let (read_only, root_seq) =
                        extract_expired_info(active.txn.as_ref(), sessions_to_expire);
                    expire_active_slot(map, id, read_only, root_seq, max_expiry_ns)
                }
            };
            if let Some(txn) = dropped {
                txns_to_drop.push(txn);
            }
        }
        let next = map.refresh_next_expiry();
        self.next_expiry_ns.store(next, Ordering::Release);
    }

    fn insert_new_active(&self, id: TransactionId, txn: Arc<Transaction>) -> Result<TransactionId> {
        let mut sessions_to_expire = Vec::new();
        let mut txns_to_drop = Vec::new();
        let should_notify;
        let res = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                txns_to_drop = map.clear_all();
                self.next_expiry_ns.store(u64::MAX, Ordering::Release);
                txns_to_drop.push(txn);
                should_notify = false;
                Err(AbortReason::DatabaseCleared.into())
            } else {
                let now_ns = self.now_ns();
                if now_ns >= self.next_expiry_ns.load(Ordering::Acquire) {
                    self.reap_due_locked(
                        &mut map,
                        now_ns,
                        &mut sessions_to_expire,
                        &mut txns_to_drop,
                    );
                }
                let pre_insert_next = self.next_expiry_ns.load(Ordering::Acquire);
                let active = ActiveEntry::new(txn, now_ns, &self.timeouts);
                if let Some(dropped) =
                    map.set_slot(id.lineage_id, id.attempt_gen, TxnSlot::Active(active))
                {
                    txns_to_drop.push(dropped);
                }
                let new_next = map.refresh_next_expiry();
                self.next_expiry_ns.store(new_next, Ordering::Release);
                should_notify = new_next < pre_insert_next;
                Ok(id)
            }
        };
        finish_write_cleanup(sessions_to_expire, Vec::new(), txns_to_drop);
        if should_notify {
            self.expiry_notify.notify_waiters();
        }
        res
    }

    pub(crate) fn start_read_write(&self, optimistic_concurrency: bool) -> Result<TransactionId> {
        if is_cleared(&self.database, &self.locks) {
            return Err(AbortReason::DatabaseCleared.into());
        }
        let id = self.new_lineage_id();
        let txn = Arc::new(Transaction::ReadWrite(Arc::new(ReadWriteTransaction::new(
            id,
            Weak::clone(&self.database),
            &self.locks,
            LockPriority::Lazy,
            optimistic_concurrency,
        ))));
        self.insert_new_active(id, txn)
    }

    pub(crate) fn start_read_only(&self, read_time: Option<Timestamp>) -> Result<TransactionId> {
        if is_cleared(&self.database, &self.locks) {
            return Err(AbortReason::DatabaseCleared.into());
        }
        let read_time = match read_time {
            Some(time) => validate_read_time(time)?,
            None => Timestamp::now(),
        };
        let id = self.new_lineage_id();
        let txn = Arc::new(Transaction::ReadOnly(Arc::new(ReadOnlyTransaction::new(
            id,
            Weak::clone(&self.database),
            read_time,
        ))));
        self.insert_new_active(id, txn)
    }

    pub(crate) fn get_active(&self, id: TransactionId) -> Result<Arc<Transaction>> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = {
                let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
                let dropped = map.remove_slot(id.lineage_id);
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                dropped
            };
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let idle_ns = duration_to_ns(self.timeouts.txn_idle);
        // Read-lock fast path for un-wounded, un-expired active transactions; only upgrade to
        // `.write()` when an active transaction needs abort/expiry promotion.
        {
            let map = self.state.read().unwrap_or_else(PoisonError::into_inner);
            let now_ns = self.now_ns();
            match map.slots.get(&id.lineage_id) {
                Some(entry) if entry.current_attempt == id.attempt_gen => match &entry.slot {
                    TxnSlot::Active(active) => {
                        if active_abort_promotion(&active.txn).is_none()
                            && now_ns < active.effective_expiry(idle_ns)
                        {
                            active.record_activity(now_ns);
                            let txn = Arc::clone(&active.txn);
                            drop(map);
                            if now_ns >= self.next_expiry_ns.load(Ordering::Acquire) {
                                self.reap_if_due();
                            }
                            return Ok(txn);
                        }
                    }
                    TxnSlot::Expired { .. } => {
                        return Err(AbortReason::TransactionExpired.into());
                    }
                    TxnSlot::Wounded {
                        reason, expiry_ns, ..
                    } => {
                        return Err(if now_ns >= *expiry_ns {
                            AbortReason::TransactionExpired.into()
                        } else {
                            (*reason).into()
                        });
                    }
                    TxnSlot::Committing
                    | TxnSlot::AbortedLineage { .. }
                    | TxnSlot::Closed { .. } => {
                        return Err(GenericDatabaseError::invalid_argument(format!(
                            "invalid transaction ID: {id}"
                        )));
                    }
                },
                _ => {
                    if is_cleared(&self.database, &self.locks) {
                        return Err(AbortReason::DatabaseCleared.into());
                    }
                    return Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    )));
                }
            }
        }

        let mut sessions_to_expire = Vec::new();
        let mut txns_to_drop = Vec::new();
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                txns_to_drop = map.clear_all();
                self.next_expiry_ns.store(u64::MAX, Ordering::Release);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                let now_ns = self.now_ns();
                if now_ns >= self.next_expiry_ns.load(Ordering::Acquire) {
                    self.reap_due_locked(
                        &mut map,
                        now_ns,
                        &mut sessions_to_expire,
                        &mut txns_to_drop,
                    );
                }
                let res = match map.slots.get(&id.lineage_id) {
                    Some(entry) if entry.current_attempt == id.attempt_gen => match &entry.slot {
                        TxnSlot::Active(active) => match active_abort_promotion(&active.txn) {
                            Some(promoted) => {
                                let expiry_ns = active.effective_expiry(idle_ns);
                                let max_expiry_ns = active.max_expiry_ns;
                                let (dropped, err) = promote_active_slot(
                                    &mut map,
                                    id,
                                    promoted,
                                    expiry_ns,
                                    max_expiry_ns,
                                    now_ns,
                                );
                                if let Some(txn) = dropped {
                                    txns_to_drop.push(txn);
                                }
                                Err(err)
                            }
                            None if now_ns >= active.effective_expiry(idle_ns) => {
                                let max_expiry_ns = active.max_expiry_ns;
                                let (read_only, root_seq) = extract_expired_info(
                                    active.txn.as_ref(),
                                    &mut sessions_to_expire,
                                );
                                if let Some(dropped) = expire_active_slot(
                                    &mut map,
                                    id,
                                    read_only,
                                    root_seq,
                                    max_expiry_ns,
                                ) {
                                    txns_to_drop.push(dropped);
                                }
                                Err(AbortReason::TransactionExpired.into())
                            }
                            None => {
                                active.record_activity(now_ns);
                                Ok(Arc::clone(&active.txn))
                            }
                        },
                        TxnSlot::Expired { .. } => Err(AbortReason::TransactionExpired.into()),
                        TxnSlot::Wounded {
                            reason, expiry_ns, ..
                        } => Err(if now_ns >= *expiry_ns {
                            AbortReason::TransactionExpired.into()
                        } else {
                            (*reason).into()
                        }),
                        TxnSlot::Committing
                        | TxnSlot::AbortedLineage { .. }
                        | TxnSlot::Closed { .. } => Err(GenericDatabaseError::invalid_argument(
                            format!("invalid transaction ID: {id}"),
                        )),
                    },
                    _ => Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    ))),
                };
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                res
            }
        };
        finish_write_cleanup(sessions_to_expire, Vec::new(), txns_to_drop);
        result
    }

    pub(crate) fn take_for_commit(
        &self,
        id: TransactionId,
        has_writes: bool,
    ) -> Result<CommitTake> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = {
                let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
                let dropped = map.remove_slot(id.lineage_id);
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                dropped
            };
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let idle_ns = duration_to_ns(self.timeouts.txn_idle);
        let mut sessions_to_expire = Vec::new();
        let mut txns_to_drop = Vec::new();
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                txns_to_drop = map.clear_all();
                self.next_expiry_ns.store(u64::MAX, Ordering::Release);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                let now_ns = self.now_ns();
                if now_ns >= self.next_expiry_ns.load(Ordering::Acquire) {
                    self.reap_due_locked(
                        &mut map,
                        now_ns,
                        &mut sessions_to_expire,
                        &mut txns_to_drop,
                    );
                }
                let res = match map.slots.get(&id.lineage_id) {
                    Some(entry) if entry.current_attempt == id.attempt_gen => match &entry.slot {
                        TxnSlot::Active(active) => {
                            let expiry_ns = active.effective_expiry(idle_ns);
                            let max_expiry_ns = active.max_expiry_ns;
                            if let Some(promoted) = active_abort_promotion(&active.txn) {
                                let (dropped, err) = promote_active_slot(
                                    &mut map,
                                    id,
                                    promoted,
                                    expiry_ns,
                                    max_expiry_ns,
                                    now_ns,
                                );
                                if let Some(txn) = dropped {
                                    txns_to_drop.push(txn);
                                }
                                Err(err)
                            } else if now_ns >= expiry_ns {
                                let (read_only, root_seq) = extract_expired_info(
                                    active.txn.as_ref(),
                                    &mut sessions_to_expire,
                                );
                                if let Some(dropped) = expire_active_slot(
                                    &mut map,
                                    id,
                                    read_only,
                                    root_seq,
                                    max_expiry_ns,
                                ) {
                                    txns_to_drop.push(dropped);
                                }
                                Err(AbortReason::TransactionExpired.into())
                            } else {
                                match active.txn.as_ref() {
                                    Transaction::ReadOnly(ro) => {
                                        let ro = Arc::clone(ro);
                                        let (outcome, ret) = if has_writes {
                                            (
                                                ClosedOutcome::AbortedOrRolledBack,
                                                Err(GenericDatabaseError::invalid_argument(
                                                    "Cannot modify entities in a read-only \
                                                     transaction",
                                                )),
                                            )
                                        } else {
                                            (ClosedOutcome::Committed, Ok(CommitTake::ReadOnly(ro)))
                                        };
                                        if let Some(dropped) = map.set_slot(
                                            id.lineage_id,
                                            id.attempt_gen,
                                            TxnSlot::Closed {
                                                outcome,
                                                read_only: true,
                                                ring_gen: 0,
                                            },
                                        ) {
                                            txns_to_drop.push(dropped);
                                        }
                                        ret
                                    }
                                    Transaction::ReadWrite(rw) => {
                                        let rw = Arc::clone(rw);
                                        let session = Arc::clone(rw.session());
                                        // Disallow new reads on this session immediately while
                                        // keeping `STATE_ACTIVE` and `woundable = true` until
                                        // `seal_for_commit`.
                                        session.mark_commit_started();
                                        if let Some(dropped) = map.set_slot(
                                            id.lineage_id,
                                            id.attempt_gen,
                                            TxnSlot::Committing,
                                        ) {
                                            txns_to_drop.push(dropped);
                                        }
                                        let guard = CommitSlotGuard {
                                            database: Weak::clone(&self.database),
                                            locks: Arc::clone(&self.locks),
                                            state: Arc::clone(&self.state),
                                            id,
                                            session,
                                            expiry_ns,
                                            max_expiry_ns,
                                            armed: true,
                                        };
                                        Ok(CommitTake::ReadWrite((rw, guard)))
                                    }
                                }
                            }
                        }
                        TxnSlot::Expired { .. } => Err(AbortReason::TransactionExpired.into()),
                        TxnSlot::Wounded {
                            reason, expiry_ns, ..
                        } => Err(if now_ns >= *expiry_ns {
                            AbortReason::TransactionExpired.into()
                        } else {
                            (*reason).into()
                        }),
                        TxnSlot::Committing
                        | TxnSlot::AbortedLineage { .. }
                        | TxnSlot::Closed { .. } => Err(GenericDatabaseError::invalid_argument(
                            format!("invalid transaction ID: {id}"),
                        )),
                    },
                    _ => Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    ))),
                };
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                res
            }
        };
        finish_write_cleanup(sessions_to_expire, Vec::new(), txns_to_drop);
        result
    }

    pub(crate) fn rollback(&self, id: TransactionId) -> Result<()> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = {
                let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
                let dropped = map.remove_slot(id.lineage_id);
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                dropped
            };
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let idle_ns = duration_to_ns(self.timeouts.txn_idle);
        let mut sessions_to_expire = Vec::new();
        let mut sessions_to_close = Vec::new();
        let mut txns_to_drop = Vec::new();
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                txns_to_drop = map.clear_all();
                self.next_expiry_ns.store(u64::MAX, Ordering::Release);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                let now_ns = self.now_ns();
                if now_ns >= self.next_expiry_ns.load(Ordering::Acquire) {
                    self.reap_due_locked(
                        &mut map,
                        now_ns,
                        &mut sessions_to_expire,
                        &mut txns_to_drop,
                    );
                }
                let res = match map.slots.get(&id.lineage_id) {
                    None => Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    ))),
                    Some(entry) if id.attempt_gen > entry.current_attempt => {
                        Err(GenericDatabaseError::invalid_argument(format!(
                            "invalid transaction ID: {id}"
                        )))
                    }
                    // Stale generation (`id.attempt_gen < current_attempt`): return idempotent
                    // Ok(()) immediately without inspecting or mutating the active/committed newer
                    // attempt.
                    Some(entry) if id.attempt_gen < entry.current_attempt => Ok(()),
                    Some(entry) => match &entry.slot {
                        TxnSlot::Active(active) => {
                            let expiry_ns = active.effective_expiry(idle_ns);
                            let max_expiry_ns = active.max_expiry_ns;
                            match active_abort_promotion(&active.txn) {
                                Some(Promotion::Clear) => {
                                    if let Some(dropped) = map.remove_slot(id.lineage_id) {
                                        txns_to_drop.push(dropped);
                                    }
                                    Err(AbortReason::DatabaseCleared.into())
                                }
                                Some(Promotion::Wounded { root_seq, reason }) => {
                                    if let Some(dropped) = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::Wounded {
                                            root_seq,
                                            reason,
                                            expiry_ns,
                                            max_expiry_ns,
                                            ring_gen: 0,
                                        },
                                    ) {
                                        txns_to_drop.push(dropped);
                                    }
                                    rollback_expiry_result(now_ns, max_expiry_ns)
                                }
                                Some(Promotion::AbortedLineage { root_seq }) => {
                                    if let Some(dropped) = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::AbortedLineage {
                                            root_seq,
                                            max_expiry_ns,
                                            ring_gen: 0,
                                        },
                                    ) {
                                        txns_to_drop.push(dropped);
                                    }
                                    rollback_expiry_result(now_ns, max_expiry_ns)
                                }
                                Some(Promotion::Expired { root_seq }) => {
                                    if let Some(dropped) = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::Expired {
                                            read_only: false,
                                            root_seq,
                                            max_expiry_ns,
                                            ring_gen: 0,
                                        },
                                    ) {
                                        txns_to_drop.push(dropped);
                                    }
                                    rollback_expiry_result(now_ns, max_expiry_ns)
                                }
                                Some(Promotion::Closed) => {
                                    if let Some(dropped) = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::Closed {
                                            outcome:   ClosedOutcome::AbortedOrRolledBack,
                                            read_only: false,
                                            ring_gen:  0,
                                        },
                                    ) {
                                        txns_to_drop.push(dropped);
                                    }
                                    Ok(())
                                }
                                None if now_ns >= expiry_ns => {
                                    let (read_only, root_seq) = extract_expired_info(
                                        active.txn.as_ref(),
                                        &mut sessions_to_expire,
                                    );
                                    if let Some(dropped) = expire_active_slot(
                                        &mut map,
                                        id,
                                        read_only,
                                        root_seq,
                                        max_expiry_ns,
                                    ) {
                                        txns_to_drop.push(dropped);
                                    }
                                    rollback_expiry_result(now_ns, max_expiry_ns)
                                }
                                None => {
                                    let read_only = match active.txn.as_ref() {
                                        Transaction::ReadWrite(rw) => {
                                            sessions_to_close.push(Arc::clone(rw.session()));
                                            false
                                        }
                                        Transaction::ReadOnly(_) => true,
                                    };
                                    if let Some(dropped) = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::Closed {
                                            outcome: ClosedOutcome::AbortedOrRolledBack,
                                            read_only,
                                            ring_gen: 0,
                                        },
                                    ) {
                                        txns_to_drop.push(dropped);
                                    }
                                    Ok(())
                                }
                            }
                        }
                        TxnSlot::Expired { max_expiry_ns, .. }
                        | TxnSlot::Wounded { max_expiry_ns, .. }
                        | TxnSlot::AbortedLineage { max_expiry_ns, .. } => {
                            rollback_expiry_result(now_ns, *max_expiry_ns)
                        }
                        TxnSlot::Committing
                        | TxnSlot::Closed {
                            outcome: ClosedOutcome::AbortedOrRolledBack,
                            ..
                        } => Ok(()),
                        TxnSlot::Closed {
                            outcome: ClosedOutcome::Committed,
                            ..
                        } => Err(GenericDatabaseError::aborted(EXPIRED_TRANSACTION_MSG)),
                    },
                };
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                res
            }
        };
        finish_write_cleanup(sessions_to_expire, sessions_to_close, txns_to_drop);
        result
    }

    pub(crate) fn start_read_write_with_id(
        &self,
        id: TransactionId,
        optimistic_concurrency: bool,
    ) -> Result<TransactionId> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = {
                let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
                let dropped = map.remove_slot(id.lineage_id);
                self.next_expiry_ns
                    .store(map.refresh_next_expiry(), Ordering::Release);
                dropped
            };
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let idle_ns = duration_to_ns(self.timeouts.txn_idle);
        let mut sessions_to_expire = Vec::new();
        let mut txns_to_drop = Vec::new();
        let mut should_notify = false;
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                txns_to_drop = map.clear_all();
                self.next_expiry_ns.store(u64::MAX, Ordering::Release);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                let now_ns = self.now_ns();
                if now_ns >= self.next_expiry_ns.load(Ordering::Acquire) {
                    self.reap_due_locked(
                        &mut map,
                        now_ns,
                        &mut sessions_to_expire,
                        &mut txns_to_drop,
                    );
                }
                let pre_insert_next = self.next_expiry_ns.load(Ordering::Acquire);

                let plan = match map.slots.get(&id.lineage_id) {
                    Some(entry) if id.attempt_gen > entry.current_attempt => {
                        Err(GenericDatabaseError::invalid_argument(format!(
                            "invalid transaction ID: {id}"
                        )))
                    }
                    Some(entry) => {
                        let cur = entry.current_attempt;
                        match &entry.slot {
                            TxnSlot::Active(active) => match active.txn.as_ref() {
                                Transaction::ReadOnly(_) => {
                                    Err(GenericDatabaseError::invalid_argument(
                                        "Cannot retry a read-only transaction",
                                    ))
                                }
                                Transaction::ReadWrite(rw) => {
                                    match active_abort_promotion(&active.txn) {
                                        Some(Promotion::Clear) => {
                                            if let Some(dropped) = map.remove_slot(id.lineage_id) {
                                                txns_to_drop.push(dropped);
                                            }
                                            Err(AbortReason::DatabaseCleared.into())
                                        }
                                        Some(
                                            Promotion::Wounded { root_seq, .. }
                                            | Promotion::AbortedLineage { root_seq }
                                            | Promotion::Expired { root_seq },
                                        ) => next_attempt_gen(cur, id).map(|next_attempt| {
                                            (
                                                next_attempt,
                                                root_seq.map_or(
                                                    LockPriority::Lazy,
                                                    LockPriority::Assigned,
                                                ),
                                            )
                                        }),
                                        Some(Promotion::Closed) => next_attempt_gen(cur, id)
                                            .map(|next_attempt| (next_attempt, LockPriority::Lazy)),
                                        None if now_ns >= active.effective_expiry(idle_ns) => {
                                            let session = Arc::clone(rw.session());
                                            let root_seq = session.root_seq();
                                            sessions_to_expire.push(session);
                                            next_attempt_gen(cur, id).map(|next_attempt| {
                                                (
                                                    next_attempt,
                                                    root_seq.map_or(
                                                        LockPriority::Lazy,
                                                        LockPriority::Assigned,
                                                    ),
                                                )
                                            })
                                        }
                                        None => Err(GenericDatabaseError::failed_precondition(
                                            "transaction_id already/still in use",
                                        )),
                                    }
                                }
                            },
                            TxnSlot::Closed {
                                read_only: true, ..
                            }
                            | TxnSlot::Expired {
                                read_only: true, ..
                            } => Err(GenericDatabaseError::invalid_argument(
                                "Cannot retry a read-only transaction",
                            )),
                            TxnSlot::Committing => Err(GenericDatabaseError::failed_precondition(
                                "transaction_id already/still in use",
                            )),
                            TxnSlot::Wounded { root_seq, .. }
                            | TxnSlot::AbortedLineage { root_seq, .. }
                            | TxnSlot::Expired {
                                read_only: false,
                                root_seq,
                                ..
                            } => {
                                let root_seq = *root_seq;
                                next_attempt_gen(cur, id).map(|next_attempt| {
                                    (
                                        next_attempt,
                                        root_seq.map_or(LockPriority::Lazy, LockPriority::Assigned),
                                    )
                                })
                            }
                            TxnSlot::Closed {
                                read_only: false, ..
                            } => next_attempt_gen(cur, id)
                                .map(|next_attempt| (next_attempt, LockPriority::Lazy)),
                        }
                    }
                    None => next_attempt_gen(id.attempt_gen, id)
                        .map(|next_attempt| (next_attempt, LockPriority::Lazy)),
                };

                let res = match plan {
                    Ok((next_attempt, priority)) => {
                        let new_id = id.with_attempt(next_attempt);
                        let txn =
                            Arc::new(Transaction::ReadWrite(Arc::new(ReadWriteTransaction::new(
                                new_id,
                                Weak::clone(&self.database),
                                &self.locks,
                                priority,
                                optimistic_concurrency,
                            ))));
                        let active = ActiveEntry::new(txn, now_ns, &self.timeouts);
                        if let Some(prev) =
                            map.set_slot(id.lineage_id, next_attempt, TxnSlot::Active(active))
                        {
                            txns_to_drop.push(prev);
                        }
                        Ok(new_id)
                    }
                    Err(err) => Err(err),
                };

                let new_next = map.refresh_next_expiry();
                self.next_expiry_ns.store(new_next, Ordering::Release);
                should_notify = res.is_ok() && new_next < pre_insert_next;
                res
            }
        };
        finish_write_cleanup(sessions_to_expire, Vec::new(), txns_to_drop);
        if should_notify {
            self.expiry_notify.notify_waiters();
        }
        result
    }
}

fn finish_write_cleanup(
    sessions_to_expire: Vec<Arc<LockSession>>,
    sessions_to_close: Vec<Arc<LockSession>>,
    txns_to_drop: Vec<Arc<Transaction>>,
) {
    for session in sessions_to_expire {
        session.close_expired();
    }
    for session in sessions_to_close {
        session.close();
    }
    drop(txns_to_drop);
}

fn rollback_expiry_result(now_ns: u64, max_expiry_ns: u64) -> Result<()> {
    if now_ns >= max_expiry_ns {
        Err(AbortReason::TransactionExpired.into())
    } else {
        Ok(())
    }
}

fn extract_expired_info(
    txn: &Transaction,
    sessions_to_expire: &mut Vec<Arc<LockSession>>,
) -> (bool, Option<u64>) {
    match txn {
        Transaction::ReadWrite(rw) => {
            let session = Arc::clone(rw.session());
            let root_seq = session.root_seq();
            sessions_to_expire.push(session);
            (false, root_seq)
        }
        Transaction::ReadOnly(_) => (true, None),
    }
}

fn expire_active_slot(
    map: &mut RunningTxnMap,
    id: TransactionId,
    read_only: bool,
    root_seq: Option<u64>,
    max_expiry_ns: u64,
) -> Option<Arc<Transaction>> {
    map.set_slot(
        id.lineage_id,
        id.attempt_gen,
        TxnSlot::Expired {
            read_only,
            root_seq,
            max_expiry_ns,
            ring_gen: 0,
        },
    )
}

fn next_attempt_gen(attempt_gen: u64, id: TransactionId) -> Result<u64> {
    attempt_gen.checked_add(1).ok_or_else(|| {
        GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {id}"))
    })
}

fn promote_active_slot(
    map: &mut RunningTxnMap,
    id: TransactionId,
    promoted: Promotion,
    expiry_ns: u64,
    max_expiry_ns: u64,
    now_ns: u64,
) -> (Option<Arc<Transaction>>, GenericDatabaseError) {
    match promoted {
        Promotion::Clear => (
            map.remove_slot(id.lineage_id),
            AbortReason::DatabaseCleared.into(),
        ),
        Promotion::Wounded { root_seq, reason } => (
            map.set_slot(
                id.lineage_id,
                id.attempt_gen,
                TxnSlot::Wounded {
                    root_seq,
                    reason,
                    expiry_ns,
                    max_expiry_ns,
                    ring_gen: 0,
                },
            ),
            if now_ns >= expiry_ns {
                AbortReason::TransactionExpired.into()
            } else {
                reason.into()
            },
        ),
        Promotion::AbortedLineage { root_seq } => (
            map.set_slot(
                id.lineage_id,
                id.attempt_gen,
                TxnSlot::AbortedLineage {
                    root_seq,
                    max_expiry_ns,
                    ring_gen: 0,
                },
            ),
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {id}")),
        ),
        Promotion::Expired { root_seq } => (
            map.set_slot(
                id.lineage_id,
                id.attempt_gen,
                TxnSlot::Expired {
                    read_only: false,
                    root_seq,
                    max_expiry_ns,
                    ring_gen: 0,
                },
            ),
            AbortReason::TransactionExpired.into(),
        ),
        Promotion::Closed => (
            map.set_slot(
                id.lineage_id,
                id.attempt_gen,
                TxnSlot::Closed {
                    outcome:   ClosedOutcome::AbortedOrRolledBack,
                    read_only: false,
                    ring_gen:  0,
                },
            ),
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {id}")),
        ),
    }
}

enum Promotion {
    Wounded {
        root_seq: Option<u64>,
        reason:   AbortReason,
    },
    AbortedLineage {
        root_seq: Option<u64>,
    },
    Expired {
        root_seq: Option<u64>,
    },
    Clear,
    Closed,
}

fn active_abort_promotion(txn: &Transaction) -> Option<Promotion> {
    let rw = txn.as_read_write()?;
    let session = rw.session();
    match session.abort_state() {
        SessionAbortState::Active | SessionAbortState::Committing => None,
        SessionAbortState::Wounded(reason) => Some(Promotion::Wounded {
            root_seq: session.root_seq(),
            reason,
        }),
        SessionAbortState::TimedOut(_) => Some(Promotion::AbortedLineage {
            root_seq: session.root_seq(),
        }),
        SessionAbortState::Expired(_) => Some(Promotion::Expired {
            root_seq: session.root_seq(),
        }),
        SessionAbortState::Cleared(_) => Some(Promotion::Clear),
        SessionAbortState::Closed => Some(Promotion::Closed),
    }
}

fn is_cleared(database: &Weak<FirestoreDatabase>, locks: &LockManager) -> bool {
    locks.is_cleared() || database.upgrade().is_some_and(|db| db.is_cleared())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FirestoreConfig, FirestoreProject,
        database::{ReadConsistency, timeouts::Timeouts},
        reference::RootRef,
    };

    fn test_txns(timeouts: Timeouts) -> RunningTransactions {
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let locks = Arc::new(LockManager::new(root, timeouts));
        RunningTransactions::new(Weak::new(), locks, timeouts)
    }

    #[test]
    fn transaction_id_wire_encoding_and_check_validate_lineage_and_attempt() {
        let txns = test_txns(Timeouts::FAST);

        // 16-byte round-trip (`Vec<u8>` and `&[u8]`)
        let issued = txns.start_read_write(false).unwrap();
        assert!(issued.check().is_ok());
        assert!(txns.check_id(issued).is_ok());
        assert_eq!(issued.attempt_gen(), 1);
        let wire: Vec<u8> = issued.into();
        assert_eq!(wire.len(), 16);
        assert_eq!(TransactionId::try_from(wire.as_slice()).unwrap(), issued);
        let decoded = TransactionId::try_from(wire).unwrap();
        assert_eq!(decoded, issued);

        // Zero lineage / zero instance / zero local_seq / zero attempt / wrong lengths rejected.
        assert!(matches!(
            TransactionId::from_parts(0, 1).check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            TransactionId::from_parts(1, 1).check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            TransactionId::from_parts(u64::from(issued.instance_id()) << 32, 1).check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            TransactionId::from_parts(issued.lineage_id(), 0).check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            TransactionId::try_from(vec![0u8; 8]),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            TransactionId::try_from([0u8; 8].as_slice()),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            TransactionId::try_from(vec![0u8; 16]),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Future instance ID and unissued local_seq rejected.
        let future_instance =
            TransactionId::from_instance_seq(NEXT_INSTANCE_ID.load(Ordering::Acquire) + 100, 1);
        assert!(matches!(
            future_instance.check(),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        let unissued_local = TransactionId::from_instance_seq(txns.instance_id, 999);
        assert!(matches!(
            txns.check_id(unissued_local),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }

    #[test]
    fn start_read_only_validates_and_pins_read_time() {
        let txns = test_txns(Timeouts::FAST);

        let id = txns.start_read_only(None).unwrap();
        let pinned = txns.get_active(id).unwrap().read_time().unwrap();
        let later = Timestamp::now();
        assert!(later > pinned);
        assert_eq!(txns.get_active(id).unwrap().read_time(), Some(pinned));

        let epoch = Timestamp {
            seconds: 0,
            nanos:   0,
        };
        let epoch_id = txns.start_read_only(Some(epoch)).unwrap();
        assert_eq!(txns.get_active(epoch_id).unwrap().read_time(), Some(epoch));

        assert!(matches!(
            txns.start_read_only(Some(Timestamp {
                seconds: -1,
                nanos:   0,
            })),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            txns.start_read_only(Some(Timestamp {
                seconds: later.seconds + 60,
                nanos:   0,
            })),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
    }

    #[test]
    fn rollback_is_idempotent_for_closed_or_committing_and_aborts_after_commit() {
        let txns = test_txns(Timeouts::FAST);
        let other_db_txns = test_txns(Timeouts::FAST);

        assert!(matches!(
            txns.rollback(TransactionId::from_parts(0, 1)),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        let other_id = other_db_txns.start_read_write(false).unwrap();
        assert!(matches!(
            txns.rollback(other_id),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Double rollback on active transaction is idempotent Ok(()).
        let id_rb = txns.start_read_write(false).unwrap();
        assert!(txns.rollback(id_rb).is_ok());
        assert!(txns.rollback(id_rb).is_ok());
        assert!(matches!(
            txns.get_active(id_rb),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Dropped (cancelled) CommitSlotGuard records AbortedOrRolledBack -> Rollback is Ok(()).
        let id_failed = txns.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((_, guard)) = txns.take_for_commit(id_failed, true).unwrap()
        else {
            panic!("expected ReadWrite");
        };
        // Rollback while Committing leaves commit untouched and returns Ok(()).
        assert!(txns.rollback(id_failed).is_ok());
        drop(guard);
        assert!(txns.rollback(id_failed).is_ok());
        assert!(txns.rollback(id_failed).is_ok());

        // Succeeded CommitSlotGuard records Committed -> Rollback returns Aborted (even repeated).
        let id_committed = txns.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((_, guard)) = txns.take_for_commit(id_committed, true).unwrap()
        else {
            panic!("expected ReadWrite");
        };
        assert!(txns.rollback(id_committed).is_ok());
        guard.finish_committed();
        assert!(matches!(
            txns.rollback(id_committed),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            txns.rollback(id_committed),
            Err(GenericDatabaseError::Aborted(_))
        ));
    }

    #[test]
    fn retry_rotates_token_preserves_stale_rollback_idempotency_and_rejects_read_only() {
        let txns = test_txns(Timeouts::FAST);

        // Read-only active/closed cannot be retried (`InvalidArgument`).
        let ro_active = txns.start_read_only(None).unwrap();
        assert!(matches!(
            txns.start_read_write_with_id(ro_active, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        txns.rollback(ro_active).unwrap();
        assert!(matches!(
            txns.start_read_write_with_id(ro_active, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Read-only with writes closes to AbortedOrRolledBack + rejects retry.
        let ro_write = txns.start_read_only(None).unwrap();
        assert!(matches!(
            txns.take_for_commit(ro_write, true),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            txns.get_active(ro_write),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(txns.rollback(ro_write).is_ok());
        assert!(matches!(
            txns.start_read_write_with_id(ro_write, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Read-only empty commit transitions immediately to Committed -> Aborted on rollback.
        let ro_empty = txns.start_read_only(None).unwrap();
        let CommitTake::ReadOnly(_) = txns.take_for_commit(ro_empty, false).unwrap() else {
            panic!("expected ReadOnly");
        };
        assert!(matches!(
            txns.rollback(ro_empty),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            txns.start_read_write_with_id(ro_empty, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Wound -> Rollback preserves tombstone + root_seq -> retry rotates bytes -> stale
        // rollback(attempt_1) returns Ok(()) even after attempt_2 is Committed!
        let attempt_1 = txns.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((rw1, g1)) = txns.take_for_commit(attempt_1, true).unwrap()
        else {
            panic!();
        };
        let _unused = rw1;
        g1.abort_commit(Some(AbortReason::Wounded {
            wounder_txn_id: None,
            wounder_seq:    1,
        }));
        // Delayed fire-and-forget rollback before retry must preserve Wounded tombstone.
        assert!(txns.rollback(attempt_1).is_ok());
        assert!(matches!(
            txns.get_active(attempt_1),
            Err(GenericDatabaseError::Aborted(_))
        ));

        let attempt_2 = txns.start_read_write_with_id(attempt_1, false).unwrap();
        assert_eq!(attempt_2.lineage_id(), attempt_1.lineage_id());
        assert_eq!(attempt_2.attempt_gen(), 2);
        assert_ne!(Vec::from(attempt_2), Vec::from(attempt_1));

        // Delayed rollback(attempt_1) while attempt_2 is Active is an idempotent no-op.
        assert!(txns.rollback(attempt_1).is_ok());
        assert!(txns.get_active(attempt_2).is_ok());

        // Commit attempt_2: stale rollback(attempt_1) STILL returns Ok(())!
        let CommitTake::ReadWrite((_, g2)) = txns.take_for_commit(attempt_2, true).unwrap() else {
            panic!();
        };
        g2.finish_committed();
        assert!(txns.rollback(attempt_1).is_ok());
        assert!(matches!(
            txns.rollback(attempt_2),
            Err(GenericDatabaseError::Aborted(_))
        ));
    }

    #[test]
    fn retry_tombstone_and_closed_rings_are_isolated_and_clear_deletes_immediately() {
        let txns = test_txns(Timeouts::FAST);

        // Create a wounded retry tombstone, then churn MAX_CLOSED_TXNS + 32 normal commits.
        let wounded_id = txns.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((_, wg)) = txns.take_for_commit(wounded_id, true).unwrap() else {
            panic!();
        };
        wg.abort_commit(Some(AbortReason::Wounded {
            wounder_txn_id: None,
            wounder_seq:    1,
        }));

        let mut closed_ids = Vec::with_capacity(MAX_CLOSED_TXNS + 32);
        for _ in 0..(MAX_CLOSED_TXNS + 32) {
            let id = txns.start_read_write(false).unwrap();
            txns.rollback(id).unwrap();
            closed_ids.push(id);
        }

        // Oldest 32 closed IDs were evicted, while `wounded_id` stayed completely intact!
        for &evicted in &closed_ids[..32] {
            assert!(matches!(
                txns.rollback(evicted),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            txns.get_active(wounded_id),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(txns.start_read_write_with_id(wounded_id, false).is_ok());

        // Churn MAX_RETRY_TOMBSTONES + 10 wounded tombstones: oldest 10 retry tombstones evict
        // cleanly.
        let mut retry_ids = Vec::with_capacity(MAX_RETRY_TOMBSTONES + 10);
        for _ in 0..(MAX_RETRY_TOMBSTONES + 10) {
            let id = txns.start_read_write(false).unwrap();
            let CommitTake::ReadWrite((_, g)) = txns.take_for_commit(id, true).unwrap() else {
                panic!();
            };
            g.abort_commit(Some(AbortReason::UpgradeContention));
            retry_ids.push(id);
        }
        for &evicted in &retry_ids[..10] {
            assert!(matches!(
                txns.rollback(evicted),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        assert!(txns.start_read_write_with_id(retry_ids[10], false).is_ok());
    }

    #[tokio::test]
    async fn database_cleared_deletes_slots_immediately_without_retaining_closed_or_retry() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let active_id = db.transactions.start_read_write(false).unwrap();
        let closed_id = db.transactions.start_read_write(false).unwrap();
        db.transactions.rollback(closed_id).unwrap();

        project.clear_database(&root).await;

        // Cleared database deletes slots without keeping Closed or retry tombstones.
        assert!(matches!(
            db.transactions.get_active(active_id),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            db.transactions.rollback(closed_id),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(db.transactions.state.read().unwrap().slots.is_empty());
    }

    #[test]
    fn retry_rejects_attempt_gen_u64_max_without_overflow_while_preserving_evicted_retry() {
        let txns = test_txns(Timeouts::FAST);

        // Active / Closed slots with `u64::MAX` attempt_gen return `InvalidArgument`.
        let active = txns.start_read_write(false).unwrap();
        let active_max = active.with_attempt(u64::MAX);
        assert!(matches!(
            txns.start_read_write_with_id(active_max, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        txns.rollback(active).unwrap();
        assert!(matches!(
            txns.start_read_write_with_id(active_max, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Direct `current_attempt == u64::MAX` in `Closed` and `Wounded` slots rejects `cur + 1`.
        let closed_sat = txns.start_read_write(false).unwrap();
        let closed_sat_max = closed_sat.with_attempt(u64::MAX);
        let dropped = txns.state.write().unwrap().set_slot(
            closed_sat.lineage_id(),
            u64::MAX,
            TxnSlot::Closed {
                outcome:   ClosedOutcome::AbortedOrRolledBack,
                read_only: false,
                ring_gen:  0,
            },
        );
        drop(dropped);
        assert!(matches!(
            txns.start_read_write_with_id(closed_sat_max, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        let wounded_sat = txns.start_read_write(false).unwrap();
        let wounded_sat_max = wounded_sat.with_attempt(u64::MAX);
        let dropped = txns.state.write().unwrap().set_slot(
            wounded_sat.lineage_id(),
            u64::MAX,
            TxnSlot::Wounded {
                root_seq: Some(1),
                reason: AbortReason::Wounded {
                    wounder_txn_id: None,
                    wounder_seq:    1,
                },
                expiry_ns: u64::MAX,
                max_expiry_ns: u64::MAX,
                ring_gen: 0,
            },
        );
        drop(dropped);
        assert!(matches!(
            txns.start_read_write_with_id(wounded_sat_max, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Evicted (`Entry::Vacant`) `u64::MAX` also returns `InvalidArgument` cleanly.
        let evicted_rw = txns.start_read_write(false).unwrap();
        txns.rollback(evicted_rw).unwrap();
        for _ in 0..(MAX_CLOSED_TXNS + 32) {
            let churn = txns.start_read_write(false).unwrap();
            txns.rollback(churn).unwrap();
        }
        assert!(matches!(
            txns.rollback(evicted_rw),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(matches!(
            txns.start_read_write_with_id(evicted_rw.with_attempt(u64::MAX), false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Same-DB evicted valid RW token still retries cleanly with rotated `attempt_gen = 2`.
        let rotated = txns.start_read_write_with_id(evicted_rw, false).unwrap();
        assert_eq!(rotated.lineage_id(), evicted_rw.lineage_id());
        assert_eq!(rotated.attempt_gen(), 2);
        txns.rollback(rotated).unwrap();
    }

    #[tokio::test]
    async fn retry_rejects_cross_db_and_post_clear_tokens_and_start_fails_closed_when_cleared() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root_a: RootRef = "projects/p/databases/db-a".parse().unwrap();
        let root_b: RootRef = "projects/p/databases/db-b".parse().unwrap();
        let db_a = project.database(&root_a).await;
        let db_b = project.database(&root_b).await;

        let rw_active_a = db_a.transactions.start_read_write(false).unwrap();
        let rw_closed_a = db_a.transactions.start_read_write(false).unwrap();
        db_a.transactions.rollback(rw_closed_a).unwrap();
        let ro_a = db_a.transactions.start_read_only(None).unwrap();

        for cross_id in [rw_active_a, rw_closed_a, ro_a] {
            assert!(matches!(
                db_b.transactions.start_read_write_with_id(cross_id, false),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(matches!(
                db_b.transactions.get_active(cross_id),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(matches!(
                db_b.transactions.take_for_commit(cross_id, false),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(matches!(
                db_b.transactions.rollback(cross_id),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }

        // Clearing db_a makes `start_*` fail closed on cleared `db_a` without inserting slots, and
        // makes recreated `db_a_recreated` reject pre-clear tokens as `InvalidArgument`.
        project.clear_database(&root_a).await;
        assert!(matches!(
            db_a.transactions.start_read_write(false),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            db_a.transactions.start_read_only(None),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(matches!(
            db_a.transactions
                .start_read_write_with_id(rw_closed_a, false),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(db_a.transactions.state.read().unwrap().slots.is_empty());

        let db_a_recreated = project.database(&root_a).await;
        for pre_clear_id in [rw_active_a, rw_closed_a, ro_a] {
            assert!(matches!(
                db_a_recreated
                    .transactions
                    .start_read_write_with_id(pre_clear_id, false),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn take_for_commit_marks_commit_started_before_sealing() {
        use crate::error::CommitError;

        let txns = test_txns(Timeouts::FAST);
        let id = txns.start_read_write(false).unwrap();

        let CommitTake::ReadWrite((rw, guard)) = txns.take_for_commit(id, true).unwrap() else {
            panic!("expected ReadWrite");
        };
        assert!(matches!(
            rw.session().check_read_active(),
            Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
        ));
        assert!(rw.session().check_active().is_ok());
        assert_eq!(rw.session().abort_state(), SessionAbortState::Active);

        drop(guard);
    }

    fn expired_err() -> GenericDatabaseError {
        GenericDatabaseError::aborted(EXPIRED_TRANSACTION_MSG)
    }

    fn ro_opts() -> googleapis::google::firestore::v1::TransactionOptions {
        use googleapis::google::firestore::v1::{TransactionOptions, transaction_options};
        TransactionOptions {
            mode: Some(transaction_options::Mode::ReadOnly(Default::default())),
        }
    }

    fn doc_ref(name: &str) -> crate::database::reference::DocumentRef {
        format!("projects/p/databases/(default)/documents/c/{name}")
            .parse()
            .unwrap()
    }

    fn upsert_write(name: &str, v: i64) -> googleapis::google::firestore::v1::Write {
        use googleapis::google::firestore::v1::{Document, Value, Write, write};
        Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_ref(name).to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(v))]),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_defaults_unused_vs_used_activity_refresh_and_max_lifetime_cap() {
        use tokio::time::advance;

        // Both Timeouts::FAST and Timeouts::CLOUD share identical Cloud transaction TTL defaults.
        for timeouts in [Timeouts::FAST, Timeouts::CLOUD] {
            assert_eq!(timeouts.txn_idle, Duration::from_secs(60));
            assert_eq!(timeouts.txn_used_min, Duration::from_secs(120));
            assert_eq!(timeouts.txn_max, Duration::from_secs(270));

            let txns = test_txns(timeouts);

            let unused_rw = txns.start_read_write(false).unwrap();
            let unused_ro = txns.start_read_only(None).unwrap();
            let used_idle_rw = txns.start_read_write(false).unwrap();
            let used_idle_ro = txns.start_read_only(None).unwrap();
            let refreshed_rw = txns.start_read_write(false).unwrap();
            let periodic_rw = txns.start_read_write(false).unwrap();

            // First `get_active` marks transactions as used (`max(120s, last_activity + 60s)`).
            assert!(txns.get_active(used_idle_rw).is_ok());
            assert!(txns.get_active(used_idle_ro).is_ok());
            assert!(txns.get_active(refreshed_rw).is_ok());
            assert!(txns.get_active(periodic_rw).is_ok());

            // t = 35s: refresh `refreshed_rw` and `periodic_rw` -> `35 + 60 = 95s < 120s` min
            // window.
            advance(Duration::from_secs(35)).await;
            assert!(txns.get_active(refreshed_rw).is_ok());
            assert!(txns.get_active(periodic_rw).is_ok());

            // t = 59.999s: unused RW/RO are still active.
            advance(Duration::from_secs(24) + Duration::from_millis(999)).await;
            assert!(
                txns.take_for_commit(txns.start_read_only(None).unwrap(), false)
                    .is_ok()
            );
            let active_before_60 = txns.state.read().unwrap().active_count;
            assert_eq!(active_before_60, 6);

            // t = 60.000s: unused RW/RO expire immediately; used txns remain alive.
            advance(Duration::from_millis(1)).await;
            assert_eq!(txns.get_active(unused_rw).unwrap_err(), expired_err());
            assert_eq!(txns.get_active(unused_ro).unwrap_err(), expired_err());
            assert!(txns.get_active(used_idle_rw).is_ok());
            assert!(txns.get_active(used_idle_ro).is_ok());
            // Re-pin `used_idle_rw`? Wait: calling `get_active` at 60s refreshed `used_idle_*` to
            // `60 + 60 = 120s` too; test a fresh no-refresh used txn separately below!

            // Test a pure `used_once_at_0` txn without touching it between t=0 and t=119.999s.
            let used_once_base = txns.start_read_write(false).unwrap();
            assert!(txns.get_active(used_once_base).is_ok());
            // Keep `periodic_rw` alive with reads every 35s, and check `refreshed_rw` at t=95s.
            advance(Duration::from_secs(30)).await; // total 90s; used_once_base at 30s
            assert!(txns.get_active(periodic_rw).is_ok());

            // At t = 120s (`used_idle_rw/ro` had last activity at 60s -> expire at 120s!).
            advance(Duration::from_secs(30)).await; // total 120s; used_once_base at 60s
            assert_eq!(txns.get_active(used_idle_rw).unwrap_err(), expired_err());
            assert_eq!(txns.get_active(used_idle_ro).unwrap_err(), expired_err());
            assert_eq!(txns.get_active(refreshed_rw).unwrap_err(), expired_err());
            assert!(txns.get_active(periodic_rw).is_ok()); // t = 120s -> next expiry 180s

            // Verify `used_once_base` survives without activity until `119.999s` from its
            // creation (t=60 -> t=179.999s), and `periodic_rw` survives with reads every <=35s up
            // to 269.999s, then expires at 270s!
            advance(Duration::from_secs(35)).await; // total 155s
            assert!(txns.get_active(periodic_rw).is_ok());
            advance(Duration::from_secs(24) + Duration::from_millis(999)).await; // total 179.999s
            // `used_once_base` created at t=60s, used at t=60s: still alive at +119.999s!
            {
                let map = txns.state.read().unwrap();
                let idle_ns = duration_to_ns(timeouts.txn_idle);
                let now_ns = txns.now_ns();
                let Some(LineageEntry {
                    slot: TxnSlot::Active(active),
                    ..
                }) = map.slots.get(&used_once_base.lineage_id())
                else {
                    panic!("used_once_base should still be active before +120s");
                };
                assert!(now_ns < active.effective_expiry(idle_ns));
            }
            advance(Duration::from_millis(1)).await; // total 180s -> +120s for used_once_base
            assert_eq!(txns.get_active(used_once_base).unwrap_err(), expired_err());

            assert!(txns.get_active(periodic_rw).is_ok()); // t = 180s
            advance(Duration::from_secs(35)).await; // t = 215s
            assert!(txns.get_active(periodic_rw).is_ok());
            advance(Duration::from_secs(35)).await; // t = 250s
            assert!(txns.get_active(periodic_rw).is_ok());
            advance(Duration::from_secs(19) + Duration::from_millis(999)).await; // t = 269.999s
            assert!(txns.get_active(periodic_rw).is_ok());
            // Last read was 19.999s ago (< 60s), yet hard max lifetime caps at t = 270.000s!
            advance(Duration::from_millis(1)).await; // t = 270.000s
            assert_eq!(txns.get_active(periodic_rw).unwrap_err(), expired_err());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn activity_beyond_120s_extends_up_to_270s_cap() {
        use tokio::time::advance;

        let txns = test_txns(Timeouts::FAST);
        let rw = txns.start_read_write(false).unwrap();
        assert!(txns.get_active(rw).is_ok());

        // Read at t = 90s pushes expiry to 90 + 60 = 150s (past 120s).
        advance(Duration::from_secs(90)).await;
        assert!(txns.get_active(rw).is_ok());
        advance(Duration::from_secs(59) + Duration::from_millis(999)).await; // t = 149.999s
        // Peek directly without refreshing `last_activity_ns` to verify exact 150s boundary.
        {
            let map = txns.state.read().unwrap();
            let idle_ns = duration_to_ns(Timeouts::FAST.txn_idle);
            let TxnSlot::Active(active) = &map.slots.get(&rw.lineage_id()).unwrap().slot else {
                panic!("expected active");
            };
            assert!(txns.now_ns() < active.effective_expiry(idle_ns));
        }
        advance(Duration::from_millis(1)).await; // t = 150.000s
        assert_eq!(txns.get_active(rw).unwrap_err(), expired_err());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_rw_and_ro_error_codes_rollback_windows_and_retry_lineage() {
        use tokio::time::advance;

        let txns = test_txns(Timeouts::FAST);

        let rw_idle = txns.start_read_write(false).unwrap();
        let rw_unretried = txns.start_read_write(false).unwrap();
        let rw_post_rb = txns.start_read_write(false).unwrap();
        let ro_empty = txns.start_read_only(None).unwrap();
        let ro_writes = txns.start_read_only(None).unwrap();
        let ro_post_rb = txns.start_read_only(None).unwrap();

        advance(Duration::from_secs(61)).await;

        // Expired RW and RO return `10 ABORTED: The referenced transaction has expired or is no
        // longer valid.` across get_active and commit (empty & non-empty, preceding RO
        // Cannot modify entities!).
        assert_eq!(txns.get_active(rw_idle).unwrap_err(), expired_err());
        assert_eq!(
            txns.take_for_commit(rw_idle, false).unwrap_err(),
            expired_err()
        );
        assert_eq!(
            txns.take_for_commit(rw_idle, true).unwrap_err(),
            expired_err()
        );
        assert_eq!(txns.get_active(rw_unretried).unwrap_err(), expired_err());

        assert_eq!(txns.get_active(ro_empty).unwrap_err(), expired_err());
        assert_eq!(
            txns.take_for_commit(ro_empty, false).unwrap_err(),
            expired_err()
        );
        assert_eq!(
            txns.take_for_commit(ro_writes, true).unwrap_err(),
            expired_err()
        );

        // Rollback before max lifetime (270s) is idempotent Ok(()), keeping slot Expired.
        assert!(txns.rollback(rw_post_rb).is_ok());
        assert!(txns.rollback(rw_post_rb).is_ok());
        assert_eq!(txns.get_active(rw_post_rb).unwrap_err(), expired_err());
        assert_eq!(
            txns.take_for_commit(rw_post_rb, true).unwrap_err(),
            expired_err()
        );

        assert!(txns.rollback(ro_post_rb).is_ok());
        assert!(txns.rollback(ro_post_rb).is_ok());
        assert_eq!(txns.get_active(ro_post_rb).unwrap_err(), expired_err());

        // Retrying expired RO always fails with InvalidArgument("Cannot retry a read-only
        // transaction"), both before and after rollback.
        for ro in [ro_empty, ro_writes, ro_post_rb] {
            assert!(matches!(
                txns.start_read_write_with_id(ro, false),
                Err(GenericDatabaseError::InvalidArgument(ref msg))
                    if msg == "Cannot retry a read-only transaction"
            ));
        }

        // Retrying expired RW (pre-rollback and post-rollback) succeeds with rotated token.
        let rw_retry_1 = txns.start_read_write_with_id(rw_idle, false).unwrap();
        assert_eq!(rw_retry_1.lineage_id(), rw_idle.lineage_id());
        assert_eq!(rw_retry_1.attempt_gen(), 2);
        assert!(txns.get_active(rw_retry_1).is_ok());

        let rw_retry_2 = txns.start_read_write_with_id(rw_post_rb, false).unwrap();
        assert_eq!(rw_retry_2.lineage_id(), rw_post_rb.lineage_id());
        assert_eq!(rw_retry_2.attempt_gen(), 2);

        // Past max lifetime (t >= 270s): Rollback on unretried expired txns returns 10 ABORTED,
        // stale attempt-1 rollback on retried `rw_idle` remains idempotent Ok(()), while RW retry
        // STILL succeeds and gets a fresh TTL window!
        let rw_past_max = txns.start_read_write(false).unwrap();
        let ro_past_max = txns.start_read_only(None).unwrap();
        advance(Duration::from_secs(210)).await; // t = 271s (rw_past_max & ro_past_max at +210s)
        assert!(txns.rollback(rw_idle).is_ok());
        assert_eq!(txns.rollback(rw_unretried).unwrap_err(), expired_err());
        assert_eq!(txns.rollback(ro_empty).unwrap_err(), expired_err());
        assert_eq!(txns.rollback(ro_post_rb).unwrap_err(), expired_err());
        // Still rejects RO retry past 270s.
        assert!(matches!(
            txns.start_read_write_with_id(ro_empty, false),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        // Still allows RW retry past 270s, and fresh attempt lives a full new TTL window!
        let rw_reborn = txns.start_read_write_with_id(rw_post_rb, false).unwrap();
        assert_eq!(rw_reborn.attempt_gen(), 3);
        assert!(txns.get_active(rw_reborn).is_ok());

        // Rollback of rw_past_max at +275s (after advancing 65s more) returns 10 ABORTED.
        advance(Duration::from_secs(65)).await;
        assert_eq!(txns.rollback(rw_past_max).unwrap_err(), expired_err());
        assert_eq!(txns.rollback(ro_past_max).unwrap_err(), expired_err());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_rw_retry_inherits_root_seq_and_closes_unreaped_old_session() {
        use tokio::time::advance;

        let project: &'static FirestoreProject = Box::leak(Box::new(FirestoreProject::new(
            FirestoreConfig::default()
                .with_transaction_timeouts(Duration::from_secs(2), Duration::from_secs(10)),
        )));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        let doc = doc_ref("prio");

        // Older RW acquires read lock (gets root_seq = 1), then expires at t = 4s (`2 * 2s`).
        let older_1 = db
            .new_txn(googleapis::google::firestore::v1::TransactionOptions::READ_WRITE)
            .await
            .unwrap();
        assert!(
            db.get_doc(&doc, ReadConsistency::Transaction(older_1))
                .await
                .is_ok()
        );

        // Advance to t = 5s (older_1 expired, NOT yet reaped if nothing ran!).
        advance(Duration::from_secs(5)).await;

        // Start a younger RW at t = 5s and acquire a read lock on `doc`? First let's retry
        // `older_1` BEFORE any reap ran -> must close older_1's session (releasing its read lock)
        // AND inherit root_seq = 1!
        let older_2 = db
            .transactions
            .start_read_write_with_id(older_1, false)
            .unwrap();
        assert_eq!(older_2.attempt_gen(), 2);

        // Younger RW acquires a read lock on `doc` (gets root_seq = 2 > 1).
        let younger = db
            .new_txn(googleapis::google::firestore::v1::TransactionOptions::READ_WRITE)
            .await
            .unwrap();
        assert!(
            db.get_doc(&doc, ReadConsistency::Transaction(younger))
                .await
                .is_ok()
        );

        // `older_2` writes to `doc` -> because `older_2` inherited `root_seq = 1 < 2`, it wounds
        // `younger` and commits cleanly!
        assert!(
            db.commit(vec![upsert_write("prio", 42)], Some(older_2))
                .await
                .is_ok()
        );
        assert!(matches!(
            db.transactions.get_active(younger),
            Err(GenericDatabaseError::Aborted(ref msg)) if msg.contains("wounded")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn wounded_and_aborted_lineage_expiry_precedence_and_committing_immunity() {
        use tokio::time::advance;

        let txns = test_txns(Timeouts::FAST);

        // 1. Wounded early-promoted: wounded before expiry_ns -> returns Wounded, after expiry_ns
        //    -> returns TransactionExpired, rollback < 270s Ok, >= 270s TransactionExpired, retry
        //    inherits root_seq.
        let w_early = txns.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((rw_e, g_e)) = txns.take_for_commit(w_early, true).unwrap()
        else {
            panic!();
        };
        drop(rw_e);
        g_e.abort_commit(Some(AbortReason::Wounded {
            wounder_txn_id: None,
            wounder_seq:    1,
        }));
        assert!(matches!(
            txns.get_active(w_early),
            Err(GenericDatabaseError::Aborted(ref msg)) if msg.contains("wounded")
        ));
        advance(Duration::from_secs(65)).await;
        assert_eq!(txns.get_active(w_early).unwrap_err(), expired_err());
        assert_eq!(
            txns.take_for_commit(w_early, false).unwrap_err(),
            expired_err()
        );
        assert!(txns.rollback(w_early).is_ok());

        // 2. Wounded while Active and reaped late (precedence: Wounded > Expired!): simulate an
        //    active session with seq = 2 wounded by LockManager while slot is still Active, then
        //    reap past `effective_expiry`.
        let txns2 = test_txns(Timeouts::FAST);
        let d_p = doc_ref("prec_doc");
        let seq1_holder = txns2
            .locks
            .new_session(None, LockPriority::NextSeq, true, true); // seq = 1
        let victim_id = txns2.start_read_write(false).unwrap();
        let victim_txn = txns2.get_active(victim_id).unwrap();
        let victim_sess = Arc::clone(victim_txn.as_read_write().unwrap().session());
        drop(victim_txn);
        let vr = victim_sess.acquire_read(&d_p).await.unwrap(); // lazy seq = 2
        let expected_seq = victim_sess.root_seq().unwrap();
        assert!(expected_seq > 1);
        let hw = seq1_holder.acquire_write(&d_p).await.unwrap(); // wounds victim!
        assert!(victim_sess.is_wounded());
        drop(vr);
        drop(hw);
        drop(seq1_holder);
        drop(victim_sess);

        // Advance past 120s (victim was used) so `reap_if_due()` sweeps it; precedence MUST promote
        // to `TxnSlot::Wounded` (keeping `root_seq`), NOT overwrite with `Expired`!
        advance(Duration::from_secs(125)).await;
        txns2.reap_if_due();
        {
            let map = txns2.state.read().unwrap();
            assert!(matches!(
                map.slots.get(&victim_id.lineage_id()).map(|e| &e.slot),
                Some(TxnSlot::Wounded { root_seq, .. }) if *root_seq == Some(expected_seq)
            ));
        }
        assert_eq!(txns2.get_active(victim_id).unwrap_err(), expired_err());
        assert!(txns2.rollback(victim_id).is_ok());

        // Past max_expiry_ns (t >= 270s from creation): Wounded rollback returns TransactionExpired
        // while retry still inherits `expected_seq`.
        advance(Duration::from_secs(150)).await;
        assert_eq!(txns2.rollback(victim_id).unwrap_err(), expired_err());
        let retried = txns2.start_read_write_with_id(victim_id, false).unwrap();
        let retried_txn = txns2.get_active(retried).unwrap();
        assert_eq!(
            retried_txn.as_read_write().unwrap().session().root_seq(),
            Some(expected_seq)
        );
        drop(retried_txn);

        // 3. AbortedLineage: get_active -> InvalidArgument; rollback < 270s Ok, >= 270s
        //    TransactionExpired; retry inherits root_seq.
        let ab = txns2.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((_, gab)) = txns2.take_for_commit(ab, true).unwrap() else {
            panic!();
        };
        gab.abort_commit(Some(AbortReason::UpgradeContention));
        assert!(matches!(
            txns2.get_active(ab),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert!(txns2.rollback(ab).is_ok());
        advance(Duration::from_secs(275)).await;
        assert_eq!(txns2.rollback(ab).unwrap_err(), expired_err());
        assert!(txns2.start_read_write_with_id(ab, false).is_ok());

        // 4. `TxnSlot::Committing` is completely immune to reaping even past 270s!
        let committing_id = txns2.start_read_write(false).unwrap();
        advance(Duration::from_secs(50)).await;
        let CommitTake::ReadWrite((c_rw, c_guard)) =
            txns2.take_for_commit(committing_id, true).unwrap()
        else {
            panic!();
        };
        advance(Duration::from_secs(300)).await;
        txns2.reap_if_due();
        assert_eq!(c_rw.session().abort_state(), SessionAbortState::Active);
        assert!(c_rw.session().check_active().is_ok());
        c_guard.finish_committed();
        assert_eq!(txns2.rollback(committing_id).unwrap_err(), expired_err());

        // 5. `CommitSlotGuard::abort_commit(Some(AbortReason::TransactionExpired))` preserves
        //    `root_seq`!
        let exp_abort_id = txns2.start_read_write(false).unwrap();
        let exp_txn = txns2.get_active(exp_abort_id).unwrap();
        let lease = exp_txn
            .as_read_write()
            .unwrap()
            .session()
            .acquire_read(&doc_ref("abort_exp_seq"))
            .await
            .unwrap();
        let seq_before = exp_txn
            .as_read_write()
            .unwrap()
            .session()
            .root_seq()
            .unwrap();
        drop(lease);
        drop(exp_txn);
        let CommitTake::ReadWrite((_, g_exp)) = txns2.take_for_commit(exp_abort_id, true).unwrap()
        else {
            panic!();
        };
        g_exp.abort_commit(Some(AbortReason::TransactionExpired));
        let exp_retried = txns2.start_read_write_with_id(exp_abort_id, false).unwrap();
        let exp_retried_txn = txns2.get_active(exp_retried).unwrap();
        assert_eq!(
            exp_retried_txn
                .as_read_write()
                .unwrap()
                .session()
                .root_seq(),
            Some(seq_before)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bug2_expired_rw_lock_release_unblocks_arriving_and_parked_writers_and_ro_query_gc() {
        use googleapis::google::firestore::v1::{
            TransactionOptions, structured_query::CollectionSelector,
        };
        use tokio::time::advance;

        use crate::database::query::QueryBuilder;

        // Configure `idle = 2s` (`used_min = 4s`, `max = 20s`) with `long_contention_timeout =
        // true` (`write = 15s`, `read = 16s`) so lock contention waits exceed transaction TTL!
        let project: &'static FirestoreProject = Box::leak(Box::new(FirestoreProject::new(
            FirestoreConfig {
                long_contention_timeout: true,
                ..Default::default()
            }
            .with_transaction_timeouts(Duration::from_secs(2), Duration::from_secs(20)),
        )));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let d1 = doc_ref("b2_d1");
        let d2 = doc_ref("b2_d2");

        // A. Arriving non-tx write, WriteBatch, and Default read succeed immediately after holder
        //    expires without any transaction API calls.
        let holder_arr = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        drop(
            db.get_doc(&d1, ReadConsistency::Transaction(holder_arr))
                .await
                .unwrap(),
        );
        advance(Duration::from_secs(5)).await; // holder_arr expired at 4s!
        assert!(db.write(upsert_write("b2_d1", 1)).await.is_ok());
        assert!(
            db.commit(
                vec![upsert_write("b2_d1", 2), upsert_write("b2_d2", 3)],
                None
            )
            .await
            .is_ok()
        );
        assert!(db.get_doc(&d1, ReadConsistency::Default).await.is_ok());

        // B. Already-parked non-tx single write, multi-doc WriteBatch, and younger RW transaction
        //    all wake and succeed right when `holder_parked` expires at t + 4s (well before 15s
        //    lock timeout!).
        let holder_parked = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        drop(
            db.get_doc(&d1, ReadConsistency::Transaction(holder_parked))
                .await
                .unwrap(),
        );
        drop(
            db.get_doc(&d2, ReadConsistency::Transaction(holder_parked))
                .await
                .unwrap(),
        );

        let db_w = Arc::clone(&db);
        let nontx_w = tokio::spawn(async move { db_w.write(upsert_write("b2_d1", 10)).await });
        let db_b = Arc::clone(&db);
        let batch_w = tokio::spawn(async move {
            db_b.commit(
                vec![upsert_write("b2_d1", 11), upsert_write("b2_d2", 12)],
                None,
            )
            .await
        });
        let younger_rw = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        // Read d2 to keep younger_rw used, then start commit (moves to Committing before waiting).
        drop(
            db.get_doc(&d2, ReadConsistency::Transaction(younger_rw))
                .await
                .unwrap(),
        );
        let db_y = Arc::clone(&db);
        let young_commit = tokio::spawn(async move {
            db_y.commit(vec![upsert_write("b2_d1", 13)], Some(younger_rw))
                .await
        });

        tokio::task::yield_now().await;
        // All 3 writers are parked; advance to t = 3.9s (holder_parked still alive).
        advance(Duration::from_millis(3_900)).await;
        tokio::task::yield_now().await;
        assert!(!nontx_w.is_finished());
        assert!(!batch_w.is_finished());
        assert!(!young_commit.is_finished());

        // Advance past 4s: `holder_parked` expires, releases locks, and unblocks all 3 writers!
        advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;
        assert!(nontx_w.await.unwrap().is_ok());
        assert!(batch_w.await.unwrap().is_ok());
        assert!(young_commit.await.unwrap().is_ok());

        // C. Waiter parked behind used txn (4s) re-arms via `notify_waiters()` when a shorter
        //    unused txn starts at t = 0.5s (expires at 2.5s).
        let long_holder = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        drop(
            db.get_doc(&d1, ReadConsistency::Transaction(long_holder))
                .await
                .unwrap(),
        );
        let db_wait = Arc::clone(&db);
        let waiter_task =
            tokio::spawn(async move { db_wait.write(upsert_write("b2_d1", 20)).await });
        tokio::task::yield_now().await;
        advance(Duration::from_millis(500)).await;
        // Starts unused txn expiring at 0.5 + 2.0 = 2.5s (< 4.0s), lowering `next_expiry_ns`.
        let shorter_unused = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        advance(Duration::from_millis(2_100)).await; // t = 2.6s: shorter_unused reaped!
        tokio::task::yield_now().await;
        assert!(!waiter_task.is_finished());
        assert_eq!(
            db.transactions.get_active(shorter_unused).unwrap_err(),
            expired_err()
        );
        // Advance to t = 4.1s: long_holder expires and wakes waiter_task!
        advance(Duration::from_millis(1_500)).await;
        tokio::task::yield_now().await;
        assert!(waiter_task.await.unwrap().is_ok());

        // D. Mid-RPC `Query::once` expires while blocked waiting for a read lock.
        let lock_holder = db
            .locks
            .new_session(None, LockPriority::NextSeq, false, false);
        let held_w = lock_holder.acquire_write(&d1).await.unwrap();
        let query_txn = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        let mut q = QueryBuilder::from(
            root.clone().into(),
            vec![CollectionSelector {
                collection_id:   "c".to_string(),
                all_descendants: false,
            }],
        )
        .consistency(ReadConsistency::Transaction(query_txn))
        .build()
        .unwrap();
        let db_q = Arc::clone(&db);
        let query_task = tokio::spawn(async move { db_q.run_query(&mut q).await });
        tokio::task::yield_now().await;
        advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;
        assert_eq!(query_task.await.unwrap().unwrap_err(), expired_err());
        drop(held_w);
        drop(lock_holder);

        // E. Optimistic RW expiry closes `LockSession` so `LockTable::sessions` never leaks.
        let opt_rw = db.transactions.start_read_write(true).unwrap();
        drop(
            db.get_doc(&d1, ReadConsistency::Transaction(opt_rw))
                .await
                .unwrap(),
        );
        assert_eq!(db.locks.table_counts(), (0, 1));
        advance(Duration::from_secs(5)).await;
        db.transactions.reap_if_due();
        assert_eq!(db.locks.table_counts(), (0, 0));

        // F. Queue-free RO-only workload GC via `Query::once(Default|ReadTime)` &
        //    `get_doc(ReadTime)`.
        let ro1 = db.new_txn(ro_opts()).await.unwrap();
        let ro2 = db.new_txn(ro_opts()).await.unwrap();
        assert_eq!(db.transactions.state.read().unwrap().active_count, 2);
        advance(Duration::from_secs(3)).await;

        let mut q_def = QueryBuilder::from(
            root.clone().into(),
            vec![CollectionSelector {
                collection_id:   "c".to_string(),
                all_descendants: false,
            }],
        )
        .build()
        .unwrap();
        db.run_query(&mut q_def).await.unwrap();
        assert_eq!(db.transactions.state.read().unwrap().active_count, 0);
        assert_eq!(
            db.transactions.next_expiry_ns.load(Ordering::Acquire),
            u64::MAX
        );
        assert_eq!(db.transactions.get_active(ro1).unwrap_err(), expired_err());
        assert_eq!(db.transactions.get_active(ro2).unwrap_err(), expired_err());

        let ro3 = db.new_txn(ro_opts()).await.unwrap();
        advance(Duration::from_secs(3)).await;
        drop(
            db.get_doc(&d1, ReadConsistency::ReadTime(Timestamp::now()))
                .await
                .unwrap(),
        );
        assert_eq!(db.transactions.state.read().unwrap().active_count, 0);
        assert_eq!(db.transactions.get_active(ro3).unwrap_err(), expired_err());
        assert_eq!(db.locks.table_counts(), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn five_thousand_ro_leak_gc_isolated_rings_and_bounded_expiry_heap() {
        use tokio::time::advance;

        let txns = test_txns(Timeouts::FAST);

        // Seed one survivor in each of the 3 non-RO rings: retry, closed, expired_rw.
        let wounded_id = txns.start_read_write(false).unwrap();
        let CommitTake::ReadWrite((_, wg)) = txns.take_for_commit(wounded_id, true).unwrap() else {
            panic!();
        };
        wg.abort_commit(Some(AbortReason::Wounded {
            wounder_txn_id: None,
            wounder_seq:    1,
        }));

        let closed_id = txns.start_read_write(false).unwrap();
        txns.rollback(closed_id).unwrap();

        let expired_rw_id = txns.start_read_write(false).unwrap();
        advance(Duration::from_secs(61)).await;
        txns.reap_if_due();
        assert_eq!(txns.get_active(expired_rw_id).unwrap_err(), expired_err());

        // Churn 5,000 abandoned RO transactions and let them expire.
        let mut ro_ids = Vec::with_capacity(5_000);
        for _ in 0..5_000 {
            ro_ids.push(txns.start_read_only(None).unwrap());
        }
        let ro_weaks: Vec<Weak<Transaction>> = {
            let map = txns.state.read().unwrap();
            assert_eq!(map.active_count, 5_000);
            ro_ids
                .iter()
                .map(|id| match &map.slots.get(&id.lineage_id()).unwrap().slot {
                    TxnSlot::Active(active) => Arc::downgrade(&active.txn),
                    _ => panic!("expected Active RO slot"),
                })
                .collect()
        };

        advance(Duration::from_secs(61)).await;
        txns.reap_if_due();

        assert!(
            ro_weaks.iter().all(|w| w.strong_count() == 0),
            "all 5,000 abandoned RO Arc<Transaction> handles must be dropped on expiry"
        );

        {
            let map = txns.state.read().unwrap();
            assert_eq!(map.active_count, 0);
            assert!(map.expiry_heap.is_empty());
            assert_eq!(map.expired_ro_ring.count, MAX_EXPIRED_TXNS);
            assert_eq!(map.expired_rw_ring.count, 1);
            assert_eq!(map.retry_ring.count, 1);
            assert_eq!(map.closed_ring.count, 1);
        }
        assert_eq!(txns.next_expiry_ns.load(Ordering::Acquire), u64::MAX);

        // Oldest RO tombstones were evicted by `expired_ro_ring` while newest stay Expired.
        assert!(matches!(
            txns.get_active(ro_ids[0]),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        assert_eq!(
            txns.get_active(*ro_ids.last().unwrap()).unwrap_err(),
            expired_err()
        );

        // Wounded, Closed, and Expired RW survivors stayed 100% intact across 5,000 RO churn!
        assert!(matches!(
            txns.get_active(wounded_id),
            Err(GenericDatabaseError::Aborted(_))
        ));
        assert!(txns.rollback(closed_id).is_ok());
        assert_eq!(txns.get_active(expired_rw_id).unwrap_err(), expired_err());
        let retried_rw = txns.start_read_write_with_id(expired_rw_id, false).unwrap();
        txns.rollback(retried_rw).unwrap();

        // Churn MAX_EXPIRED_TXNS + 20 expired RW txns -> evicts oldest 20 while retry/closed stay
        // intact.
        let mut rw_expired_ids = Vec::with_capacity(MAX_EXPIRED_TXNS + 20);
        for _ in 0..(MAX_EXPIRED_TXNS + 20) {
            rw_expired_ids.push(txns.start_read_write(false).unwrap());
        }
        advance(Duration::from_secs(61)).await;
        txns.reap_if_due();
        for &evicted in &rw_expired_ids[..20] {
            assert!(matches!(
                txns.rollback(evicted),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        assert_eq!(
            txns.get_active(rw_expired_ids[20]).unwrap_err(),
            expired_err()
        );
        let retried_wounded = txns.start_read_write_with_id(wounded_id, false).unwrap();
        txns.rollback(retried_wounded).unwrap();
        assert!(txns.rollback(closed_id).is_ok());

        // Bounded `expiry_heap` compaction: keep 1 active txn while churning 10,000 committed txns.
        let anchor = txns.start_read_write(false).unwrap();
        assert!(txns.get_active(anchor).is_ok()); // anchor deadline +120s
        for _ in 0..10_000 {
            let short = txns.start_read_write(false).unwrap();
            txns.rollback(short).unwrap();
        }
        {
            let map = txns.state.read().unwrap();
            assert_eq!(map.active_count, 1);
            assert!(
                map.expiry_heap.len() <= MAX_CLOSED_TXNS,
                "expiry_heap grew unbounded: {}",
                map.expiry_heap.len()
            );
        }
        txns.rollback(anchor).unwrap();
        assert!(txns.state.read().unwrap().expiry_heap.is_empty());
        assert_eq!(txns.next_expiry_ns.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn config_zero_and_max_durations_without_panics_and_sync_no_runtime() {
        // 1. Synchronous (no Tokio runtime) RunningTransactions lifecycle works cleanly.
        let txns = test_txns(Timeouts::FAST);
        let rw = txns.start_read_write(false).unwrap();
        let ro = txns.start_read_only(None).unwrap();
        assert!(txns.get_active(rw).is_ok());
        assert!(txns.get_active(ro).is_ok());
        txns.reap_if_due();
        let CommitTake::ReadOnly(_) = txns.take_for_commit(ro, false).unwrap() else {
            panic!();
        };
        assert!(txns.rollback(rw).is_ok());
        let retried = txns.start_read_write_with_id(rw, false).unwrap();
        assert!(txns.rollback(retried).is_ok());

        // 2. Duration::ZERO & Duration::MAX config edge cases never panic or underflow.
        let zero_both =
            FirestoreConfig::default().with_transaction_timeouts(Duration::ZERO, Duration::ZERO);
        let proj_zero_both = FirestoreProject::new(zero_both);
        assert_eq!(proj_zero_both.timeouts.txn_idle, Duration::ZERO);
        assert_eq!(proj_zero_both.timeouts.txn_used_min, Duration::ZERO);
        assert_eq!(proj_zero_both.timeouts.txn_max, Duration::ZERO);
        let txns_zb = test_txns(proj_zero_both.timeouts);
        let z_rw = txns_zb.start_read_write(false).unwrap();
        assert_eq!(txns_zb.get_active(z_rw).unwrap_err(), expired_err());
        assert_eq!(txns_zb.rollback(z_rw).unwrap_err(), expired_err());
        assert!(txns_zb.start_read_write_with_id(z_rw, false).is_ok());

        let zero_idle = FirestoreConfig::default()
            .with_transaction_idle_timeout(Duration::ZERO)
            .with_transaction_max_lifetime(Duration::from_secs(10));
        let proj_zi = FirestoreProject::new(zero_idle);
        assert_eq!(proj_zi.timeouts.txn_idle, Duration::ZERO);
        assert_eq!(proj_zi.timeouts.txn_used_min, Duration::ZERO);
        assert_eq!(proj_zi.timeouts.txn_max, Duration::from_secs(10));
        let txns_zi = test_txns(proj_zi.timeouts);
        let zi_ro = txns_zi.start_read_only(None).unwrap();
        assert_eq!(txns_zi.get_active(zi_ro).unwrap_err(), expired_err());
        assert!(txns_zi.rollback(zi_ro).is_ok());

        let zero_max = FirestoreConfig::default().with_transaction_max_lifetime(Duration::ZERO);
        let proj_zm = FirestoreProject::new(zero_max);
        assert_eq!(proj_zm.timeouts.txn_idle, Duration::ZERO);
        assert_eq!(proj_zm.timeouts.txn_used_min, Duration::ZERO);
        assert_eq!(proj_zm.timeouts.txn_max, Duration::ZERO);

        let max_both =
            FirestoreConfig::default().with_transaction_timeouts(Duration::MAX, Duration::MAX);
        let proj_max = FirestoreProject::new(max_both);
        assert_eq!(proj_max.timeouts.txn_idle, Duration::MAX);
        assert_eq!(proj_max.timeouts.txn_used_min, Duration::MAX);
        assert_eq!(proj_max.timeouts.txn_max, Duration::MAX);
        let txns_max = test_txns(proj_max.timeouts);
        let m_rw = txns_max.start_read_write(false).unwrap();
        assert!(txns_max.get_active(m_rw).is_ok());
        assert!(txns_max.reap_and_next_expiry().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_multithreaded_expiry_and_lock_release_stress() {
        use googleapis::google::firestore::v1::{
            TransactionOptions, structured_query::CollectionSelector,
        };

        use crate::database::query::QueryBuilder;

        let project: &'static FirestoreProject = Box::leak(Box::new(FirestoreProject::new(
            FirestoreConfig::default()
                .with_transaction_timeouts(Duration::from_millis(4), Duration::from_millis(12)),
        )));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let mut handles = Vec::new();
        for worker in 0..8usize {
            let db = Arc::clone(&db);
            let root = root.clone();
            handles.push(tokio::spawn(async move {
                for iter in 0..40usize {
                    let doc = doc_ref(&format!("stress_{}", (worker + iter) % 4));
                    match iter % 4 {
                        0 => {
                            if let Ok(id) = db.new_txn(TransactionOptions::READ_WRITE).await {
                                drop(db.get_doc(&doc, ReadConsistency::Transaction(id)).await);
                                if iter % 8 == 0 {
                                    tokio::time::sleep(Duration::from_millis(6)).await;
                                }
                                drop(
                                    db.commit(
                                        vec![upsert_write("stress_0", iter as i64)],
                                        Some(id),
                                    )
                                    .await,
                                );
                                drop(db.rollback(id).await);
                            }
                        }
                        1 => {
                            if let Ok(id) = db.new_txn(ro_opts()).await {
                                drop(db.get_doc(&doc, ReadConsistency::Transaction(id)).await);
                                tokio::time::sleep(Duration::from_millis(5)).await;
                                drop(db.get_doc(&doc, ReadConsistency::Transaction(id)).await);
                            }
                        }
                        2 => {
                            drop(
                                db.write(upsert_write(
                                    &format!("stress_{}", iter % 4),
                                    iter as i64,
                                ))
                                .await,
                            );
                            drop(db.get_doc(&doc, ReadConsistency::Default).await);
                        }
                        _ => {
                            let mut q = QueryBuilder::from(
                                root.clone().into(),
                                vec![CollectionSelector {
                                    collection_id:   "c".to_string(),
                                    all_descendants: false,
                                }],
                            )
                            .build()
                            .unwrap();
                            drop(db.run_query(&mut q).await);
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
        db.transactions.reap_if_due();

        assert_eq!(db.transactions.state.read().unwrap().active_count, 0);
        assert_eq!(
            db.transactions.next_expiry_ns.load(Ordering::Acquire),
            u64::MAX
        );
        assert_eq!(db.locks.table_counts(), (0, 0));
    }
}
