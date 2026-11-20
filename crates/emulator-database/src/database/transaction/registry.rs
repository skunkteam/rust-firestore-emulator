use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::{
        Arc, PoisonError, RwLock, Weak,
        atomic::{self, AtomicU32, AtomicU64},
    },
};

use googleapis::google::protobuf::Timestamp;

use super::{ReadOnlyTransaction, ReadWriteTransaction, Transaction};
use crate::{
    FirestoreDatabase, GenericDatabaseError,
    database::{
        lock::{LockManager, LockPriority, LockSession, SessionAbortState},
        read_consistency::validate_read_time,
    },
    error::{AbortReason, Result},
};

pub(crate) const MAX_RETRY_TOMBSTONES: usize = 4_096;
pub(crate) const MAX_CLOSED_TXNS: usize = 4_096;

static NEXT_INSTANCE_ID: AtomicU32 = AtomicU32::new(1);

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

    #[cfg(test)]
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
            && (1..NEXT_INSTANCE_ID.load(atomic::Ordering::Acquire)).contains(&instance_id)
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
enum TxnSlot {
    Active(Arc<Transaction>),
    Committing,
    Wounded {
        root_seq: Option<u64>,
        reason:   AbortReason,
        ring_gen: u64,
    },
    AbortedLineage {
        root_seq: Option<u64>,
        ring_gen: u64,
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
struct RunningTxnMap {
    slots: HashMap<u64, LineageEntry>,
    retry_order: VecDeque<(u64, u64)>,
    retry_count: usize,
    next_retry_gen: u64,
    closed_order: VecDeque<(u64, u64)>,
    closed_count: usize,
    next_closed_gen: u64,
}

impl RunningTxnMap {
    fn decrement_slot_counters(&mut self, slot: &TxnSlot) {
        match slot {
            TxnSlot::Wounded { .. } | TxnSlot::AbortedLineage { .. } => {
                self.retry_count = self.retry_count.saturating_sub(1);
            }
            TxnSlot::Closed { .. } => {
                self.closed_count = self.closed_count.saturating_sub(1);
            }
            TxnSlot::Active(_) | TxnSlot::Committing => {}
        }
    }

    fn remove_slot(&mut self, lineage_id: u64) -> Option<Arc<Transaction>> {
        let entry = self.slots.remove(&lineage_id)?;
        self.decrement_slot_counters(&entry.slot);
        match entry.slot {
            TxnSlot::Active(txn) => Some(txn),
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
                TxnSlot::Active(txn) => Some(txn),
                _ => None,
            }
        } else {
            None
        };

        match &mut new_slot {
            TxnSlot::Wounded { ring_gen, .. } | TxnSlot::AbortedLineage { ring_gen, .. } => {
                self.next_retry_gen = self.next_retry_gen.wrapping_add(1).max(1);
                let gen_id = self.next_retry_gen;
                *ring_gen = gen_id;
                self.retry_count += 1;
                self.retry_order.push_back((lineage_id, gen_id));
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                self.evict_retry_ring();
            }
            TxnSlot::Closed { ring_gen, .. } => {
                self.next_closed_gen = self.next_closed_gen.wrapping_add(1).max(1);
                let gen_id = self.next_closed_gen;
                *ring_gen = gen_id;
                self.closed_count += 1;
                self.closed_order.push_back((lineage_id, gen_id));
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
                self.evict_closed_ring();
            }
            TxnSlot::Active(_) | TxnSlot::Committing => {
                self.slots.insert(
                    lineage_id,
                    LineageEntry {
                        current_attempt,
                        slot: new_slot,
                    },
                );
            }
        }
        dropped
    }

    fn evict_retry_ring(&mut self) {
        while self.retry_count > MAX_RETRY_TOMBSTONES {
            let Some((old_id, old_gen)) = self.retry_order.pop_front() else {
                break;
            };
            let matches_live = matches!(
                self.slots.get(&old_id).map(|e| &e.slot),
                Some(TxnSlot::Wounded { ring_gen, .. } | TxnSlot::AbortedLineage { ring_gen, .. })
                    if *ring_gen == old_gen
            );
            if matches_live {
                self.slots.remove(&old_id);
                self.retry_count -= 1;
            }
        }
        if self.retry_order.len() > MAX_RETRY_TOMBSTONES * 2 {
            self.retry_order.retain(|(entry_id, entry_gen)| {
                matches!(
                    self.slots.get(entry_id).map(|e| &e.slot),
                    Some(
                        TxnSlot::Wounded { ring_gen, .. }
                            | TxnSlot::AbortedLineage { ring_gen, .. }
                    ) if *ring_gen == *entry_gen
                )
            });
        }
    }

    fn evict_closed_ring(&mut self) {
        while self.closed_count > MAX_CLOSED_TXNS {
            let Some((old_id, old_gen)) = self.closed_order.pop_front() else {
                break;
            };
            let matches_live = matches!(
                self.slots.get(&old_id).map(|e| &e.slot),
                Some(TxnSlot::Closed { ring_gen, .. }) if *ring_gen == old_gen
            );
            if matches_live {
                self.slots.remove(&old_id);
                self.closed_count -= 1;
            }
        }
        if self.closed_order.len() > MAX_CLOSED_TXNS * 2 {
            self.closed_order.retain(|(entry_id, entry_gen)| {
                matches!(
                    self.slots.get(entry_id).map(|e| &e.slot),
                    Some(TxnSlot::Closed { ring_gen, .. }) if *ring_gen == *entry_gen
                )
            });
        }
    }

    fn clear_all(&mut self) -> Vec<Arc<Transaction>> {
        let mut active_txns = Vec::new();
        for (_, entry) in self.slots.drain() {
            if let TxnSlot::Active(txn) = entry.slot {
                active_txns.push(txn);
            }
        }
        self.retry_order.clear();
        self.retry_count = 0;
        self.closed_order.clear();
        self.closed_count = 0;
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
        self.session.close();

        let cleared = is_cleared(&self.database, &self.locks)
            || matches!(
                abort_reason,
                Some(AbortReason::DatabaseCleared | AbortReason::DatabaseDropped)
            );
        self.finalize_slot(cleared, || match abort_reason {
            Some(reason @ AbortReason::Wounded { .. }) => TxnSlot::Wounded {
                root_seq,
                reason,
                ring_gen: 0,
            },
            Some(
                AbortReason::UpgradeContention
                | AbortReason::OptimisticWriteSkew
                | AbortReason::WriteTimeout
                | AbortReason::ReadTimeout,
            ) => TxnSlot::AbortedLineage {
                root_seq,
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
    state: Arc<RwLock<RunningTxnMap>>,
}

impl RunningTransactions {
    pub(crate) fn new(database: Weak<FirestoreDatabase>, locks: Arc<LockManager>) -> Self {
        let instance_id = NEXT_INSTANCE_ID
            .fetch_update(atomic::Ordering::AcqRel, atomic::Ordering::Acquire, |cur| {
                cur.checked_add(1)
            })
            .expect("transaction registry instance_id exhausted");
        Self {
            instance_id,
            next_local_seq: AtomicU64::new(1),
            database,
            locks,
            state: Default::default(),
        }
    }

    fn new_lineage_id(&self) -> TransactionId {
        let local_seq = self.next_local_seq.fetch_add(1, atomic::Ordering::AcqRel);
        assert!(
            local_seq <= u64::from(u32::MAX),
            "transaction lineage local sequence exhausted"
        );
        TransactionId::from_instance_seq(self.instance_id, local_seq as u32)
    }

    fn check_id(&self, id: TransactionId) -> Result<()> {
        id.check_for_instance(
            self.instance_id,
            self.next_local_seq.load(atomic::Ordering::Acquire),
        )
    }

    pub(crate) fn clear_all(&self) {
        let txns = self
            .state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear_all();
        drop(txns);
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
        let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if is_cleared(&self.database, &self.locks) {
            drop(map);
            drop(txn);
            return Err(AbortReason::DatabaseCleared.into());
        }
        let dropped = map.set_slot(id.lineage_id, id.attempt_gen, TxnSlot::Active(txn));
        drop(map);
        drop(dropped);
        Ok(id)
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
        let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
        if is_cleared(&self.database, &self.locks) {
            drop(map);
            drop(txn);
            return Err(AbortReason::DatabaseCleared.into());
        }
        let dropped = map.set_slot(id.lineage_id, id.attempt_gen, TxnSlot::Active(txn));
        drop(map);
        drop(dropped);
        Ok(id)
    }

    pub(crate) fn get_active(&self, id: TransactionId) -> Result<Arc<Transaction>> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = self
                .state
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove_slot(id.lineage_id);
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        // Read-lock fast path for un-wounded active transactions; only upgrade to `.write()` when
        // an active `LockSession` needs lazy abort-state promotion into `RunningTxnMap`.
        {
            let map = self.state.read().unwrap_or_else(PoisonError::into_inner);
            match map.slots.get(&id.lineage_id) {
                Some(entry) if entry.current_attempt == id.attempt_gen => match &entry.slot {
                    TxnSlot::Active(txn) => {
                        if active_abort_promotion(txn).is_none() {
                            return Ok(Arc::clone(txn));
                        }
                    }
                    TxnSlot::Wounded { reason, .. } => return Err((*reason).into()),
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

        let mut dropped_txn = None;
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                dropped_txn = map.remove_slot(id.lineage_id);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                match map.slots.get(&id.lineage_id) {
                    Some(entry) if entry.current_attempt == id.attempt_gen => match &entry.slot {
                        TxnSlot::Active(txn) => match active_abort_promotion(txn) {
                            None => Ok(Arc::clone(txn)),
                            Some(promoted) => {
                                let (dropped, err) = promote_active_slot(&mut map, id, promoted);
                                dropped_txn = dropped;
                                Err(err)
                            }
                        },
                        TxnSlot::Wounded { reason, .. } => Err((*reason).into()),
                        TxnSlot::Committing
                        | TxnSlot::AbortedLineage { .. }
                        | TxnSlot::Closed { .. } => Err(GenericDatabaseError::invalid_argument(
                            format!("invalid transaction ID: {id}"),
                        )),
                    },
                    _ => Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    ))),
                }
            }
        };
        drop(dropped_txn);
        result
    }

    pub(crate) fn take_for_commit(
        &self,
        id: TransactionId,
        has_writes: bool,
    ) -> Result<CommitTake> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = self
                .state
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove_slot(id.lineage_id);
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let mut dropped_txn = None;
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                dropped_txn = map.remove_slot(id.lineage_id);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                match map.slots.get(&id.lineage_id) {
                    Some(entry) if entry.current_attempt == id.attempt_gen => match &entry.slot {
                        TxnSlot::Active(txn) => match txn.as_ref() {
                            Transaction::ReadOnly(ro) => {
                                let ro = Arc::clone(ro);
                                if has_writes {
                                    dropped_txn = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::Closed {
                                            outcome:   ClosedOutcome::AbortedOrRolledBack,
                                            read_only: true,
                                            ring_gen:  0,
                                        },
                                    );
                                    Err(GenericDatabaseError::invalid_argument(
                                        "Cannot modify entities in a read-only transaction",
                                    ))
                                } else {
                                    dropped_txn = map.set_slot(
                                        id.lineage_id,
                                        id.attempt_gen,
                                        TxnSlot::Closed {
                                            outcome:   ClosedOutcome::Committed,
                                            read_only: true,
                                            ring_gen:  0,
                                        },
                                    );
                                    Ok(CommitTake::ReadOnly(ro))
                                }
                            }
                            Transaction::ReadWrite(rw) => {
                                let rw = Arc::clone(rw);
                                match active_abort_promotion(txn) {
                                    Some(promoted) => {
                                        let (dropped, err) =
                                            promote_active_slot(&mut map, id, promoted);
                                        dropped_txn = dropped;
                                        Err(err)
                                    }
                                    None => {
                                        let session = Arc::clone(rw.session());
                                        // Disallow new reads on this session immediately while
                                        // keeping `STATE_ACTIVE` and `woundable = true` until
                                        // `seal_for_commit`.
                                        session.mark_commit_started();
                                        dropped_txn = map.set_slot(
                                            id.lineage_id,
                                            id.attempt_gen,
                                            TxnSlot::Committing,
                                        );
                                        let guard = CommitSlotGuard {
                                            database: Weak::clone(&self.database),
                                            locks: Arc::clone(&self.locks),
                                            state: Arc::clone(&self.state),
                                            id,
                                            session,
                                            armed: true,
                                        };
                                        Ok(CommitTake::ReadWrite((rw, guard)))
                                    }
                                }
                            }
                        },
                        TxnSlot::Wounded { reason, .. } => Err((*reason).into()),
                        TxnSlot::Committing
                        | TxnSlot::AbortedLineage { .. }
                        | TxnSlot::Closed { .. } => Err(GenericDatabaseError::invalid_argument(
                            format!("invalid transaction ID: {id}"),
                        )),
                    },
                    _ => Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    ))),
                }
            }
        };
        drop(dropped_txn);
        result
    }

    pub(crate) fn rollback(&self, id: TransactionId) -> Result<()> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = self
                .state
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove_slot(id.lineage_id);
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let mut close_session: Option<Arc<LockSession>> = None;
        let mut dropped_txn: Option<Arc<Transaction>> = None;
        let result = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                dropped_txn = map.remove_slot(id.lineage_id);
                Err(AbortReason::DatabaseCleared.into())
            } else {
                match map.slots.get(&id.lineage_id) {
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
                        TxnSlot::Active(txn) => match active_abort_promotion(txn) {
                            Some(Promotion::Clear) => {
                                dropped_txn = map.remove_slot(id.lineage_id);
                                Err(AbortReason::DatabaseCleared.into())
                            }
                            Some(Promotion::Wounded { root_seq, reason }) => {
                                dropped_txn = map.set_slot(
                                    id.lineage_id,
                                    id.attempt_gen,
                                    TxnSlot::Wounded {
                                        root_seq,
                                        reason,
                                        ring_gen: 0,
                                    },
                                );
                                Ok(())
                            }
                            Some(Promotion::AbortedLineage { root_seq }) => {
                                dropped_txn = map.set_slot(
                                    id.lineage_id,
                                    id.attempt_gen,
                                    TxnSlot::AbortedLineage {
                                        root_seq,
                                        ring_gen: 0,
                                    },
                                );
                                Ok(())
                            }
                            None | Some(Promotion::Closed) => {
                                let read_only = match txn.as_ref() {
                                    Transaction::ReadWrite(rw) => {
                                        close_session = Some(Arc::clone(rw.session()));
                                        false
                                    }
                                    Transaction::ReadOnly(_) => true,
                                };
                                dropped_txn = map.set_slot(
                                    id.lineage_id,
                                    id.attempt_gen,
                                    TxnSlot::Closed {
                                        outcome: ClosedOutcome::AbortedOrRolledBack,
                                        read_only,
                                        ring_gen: 0,
                                    },
                                );
                                Ok(())
                            }
                        },
                        TxnSlot::Committing
                        | TxnSlot::Wounded { .. }
                        | TxnSlot::AbortedLineage { .. }
                        | TxnSlot::Closed {
                            outcome: ClosedOutcome::AbortedOrRolledBack,
                            ..
                        } => Ok(()),
                        TxnSlot::Closed {
                            outcome: ClosedOutcome::Committed,
                            ..
                        } => Err(GenericDatabaseError::aborted(
                            "The referenced transaction has expired or is no longer valid.",
                        )),
                    },
                }
            }
        };
        if let Some(session) = close_session {
            session.close();
        }
        drop(dropped_txn);
        result
    }

    pub(crate) fn start_read_write_with_id(
        &self,
        id: TransactionId,
        optimistic_concurrency: bool,
    ) -> Result<TransactionId> {
        self.check_id(id)?;
        if is_cleared(&self.database, &self.locks) {
            let dropped = self
                .state
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove_slot(id.lineage_id);
            drop(dropped);
            return Err(AbortReason::DatabaseCleared.into());
        }

        let (dropped_txn, result) = {
            let mut map = self.state.write().unwrap_or_else(PoisonError::into_inner);
            if is_cleared(&self.database, &self.locks) {
                let cleared_txn = map.remove_slot(id.lineage_id);
                drop(map);
                drop(cleared_txn);
                return Err(AbortReason::DatabaseCleared.into());
            }

            let (next_attempt, priority) = match map.slots.get(&id.lineage_id) {
                Some(entry) if id.attempt_gen > entry.current_attempt => {
                    return Err(GenericDatabaseError::invalid_argument(format!(
                        "invalid transaction ID: {id}"
                    )));
                }
                Some(entry) => {
                    let cur = entry.current_attempt;
                    match &entry.slot {
                        TxnSlot::Active(txn) => match txn.as_ref() {
                            Transaction::ReadOnly(_) => {
                                return Err(GenericDatabaseError::invalid_argument(
                                    "Cannot retry a read-only transaction",
                                ));
                            }
                            Transaction::ReadWrite(_) => match active_abort_promotion(txn) {
                                Some(Promotion::Clear) => {
                                    let cleared_txn = map.remove_slot(id.lineage_id);
                                    drop(map);
                                    drop(cleared_txn);
                                    return Err(AbortReason::DatabaseCleared.into());
                                }
                                Some(
                                    Promotion::Wounded { root_seq, .. }
                                    | Promotion::AbortedLineage { root_seq },
                                ) => (
                                    next_attempt_gen(cur, id)?,
                                    root_seq.map_or(LockPriority::Lazy, LockPriority::Assigned),
                                ),
                                Some(Promotion::Closed) => {
                                    (next_attempt_gen(cur, id)?, LockPriority::Lazy)
                                }
                                None => {
                                    return Err(GenericDatabaseError::failed_precondition(
                                        "transaction_id already/still in use",
                                    ));
                                }
                            },
                        },
                        TxnSlot::Closed {
                            read_only: true, ..
                        } => {
                            return Err(GenericDatabaseError::invalid_argument(
                                "Cannot retry a read-only transaction",
                            ));
                        }
                        TxnSlot::Committing => {
                            return Err(GenericDatabaseError::failed_precondition(
                                "transaction_id already/still in use",
                            ));
                        }
                        TxnSlot::Wounded { root_seq, .. }
                        | TxnSlot::AbortedLineage { root_seq, .. } => (
                            next_attempt_gen(cur, id)?,
                            root_seq.map_or(LockPriority::Lazy, LockPriority::Assigned),
                        ),
                        TxnSlot::Closed {
                            read_only: false, ..
                        } => (next_attempt_gen(cur, id)?, LockPriority::Lazy),
                    }
                }
                None => (next_attempt_gen(id.attempt_gen, id)?, LockPriority::Lazy),
            };

            let new_id = id.with_attempt(next_attempt);
            let txn = Arc::new(Transaction::ReadWrite(Arc::new(ReadWriteTransaction::new(
                new_id,
                Weak::clone(&self.database),
                &self.locks,
                priority,
                optimistic_concurrency,
            ))));
            let prev = map.set_slot(id.lineage_id, next_attempt, TxnSlot::Active(txn));
            (prev, Ok(new_id))
        };
        drop(dropped_txn);
        result
    }
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
                    ring_gen: 0,
                },
            ),
            reason.into(),
        ),
        Promotion::AbortedLineage { root_seq } => (
            map.set_slot(
                id.lineage_id,
                id.attempt_gen,
                TxnSlot::AbortedLineage {
                    root_seq,
                    ring_gen: 0,
                },
            ),
            GenericDatabaseError::invalid_argument(format!("invalid transaction ID: {id}")),
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
        FirestoreConfig, FirestoreProject, database::timeouts::Timeouts, reference::RootRef,
    };

    fn test_locks() -> Arc<LockManager> {
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        Arc::new(LockManager::new(root, Timeouts::FAST))
    }

    #[test]
    fn transaction_id_wire_encoding_and_check_validate_lineage_and_attempt() {
        let txns = RunningTransactions::new(Weak::new(), test_locks());

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
        let future_instance = TransactionId::from_instance_seq(
            NEXT_INSTANCE_ID.load(atomic::Ordering::Acquire) + 100,
            1,
        );
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
        let locks = test_locks();
        let txns = RunningTransactions::new(Weak::new(), locks);

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
        let locks = test_locks();
        let txns = RunningTransactions::new(Weak::new(), Arc::clone(&locks));
        let other_db_txns = RunningTransactions::new(Weak::new(), test_locks());

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
        let locks = test_locks();
        let txns = RunningTransactions::new(Weak::new(), locks);

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
        let locks = test_locks();
        let txns = RunningTransactions::new(Weak::new(), locks);

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
        let txns = RunningTransactions::new(Weak::new(), test_locks());

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
                reason:   AbortReason::Wounded {
                    wounder_txn_id: None,
                    wounder_seq:    1,
                },
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

        let locks = test_locks();
        let txns = RunningTransactions::new(Weak::new(), locks);
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
}
