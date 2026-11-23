use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use googleapis::google::protobuf::Timestamp;
use serde::Serialize;
use tokio::sync::broadcast;

use super::table::{ExpiredSessionOutcome, LockMode, WoundEvent};
use crate::database::{reference::RootRef, transaction::TransactionId};

/// Maximum number of contention events retained per database in the ring buffer.
pub const MAX_CONTENTION_HISTORY: usize = 256;

/// Maximum number of contended documents included in a single contention snapshot.
pub const MAX_SNAPSHOT_CONTENDED_DOCS: usize = 256;

/// Maximum number of readers or waiters included per contended document in a snapshot.
pub const MAX_SNAPSHOT_DOC_PARTICIPANTS: usize = 64;

const MAX_CONTENTION_SSE_BACKLOG: usize = 512;

/// Operation classification stored on each lock session for contention inspection.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SessionKind {
    PessimisticTxn,
    OptimisticTxn,
    WriteBatch,
    SingleWrite,
    PointRead,
}

const _: () = assert!(size_of::<SessionKind>() == 1);

/// Lock mode requested by a queued waiter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContentionLockMode {
    Read,
    Write,
}

impl From<LockMode> for ContentionLockMode {
    fn from(mode: LockMode) -> Self {
        match mode {
            LockMode::Read => Self::Read,
            LockMode::Write => Self::Write,
        }
    }
}

/// Lifecycle state of an active lock-holding session in a contention snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContentionSessionState {
    Active,
    Committing,
}

/// Classification of historical lock contention and abort events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContentionEventKind {
    Wounded,
    ReadTimeout,
    WriteTimeout,
    UpgradeAbort,
    WriteBatchRetry,
    TransactionExpired,
}

/// Cumulative per-database contention counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentionSummary {
    pub wounds_total: u64,
    pub timeouts_total: u64,
    pub upgrade_aborts_total: u64,
    pub write_batch_retries_total: u64,
}

/// Snapshot of an exclusive write lock holder on a contended document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriterSnapshot {
    pub session_id: u64,
    pub txn_id: Option<String>,
    pub kind: SessionKind,
    pub seq: Option<u64>,
    pub state: ContentionSessionState,
    pub woundable: bool,
}

/// Snapshot of a shared read lock holder on a contended document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReaderSnapshot {
    pub session_id: u64,
    pub txn_id: Option<String>,
    pub kind: SessionKind,
    pub seq: Option<u64>,
    pub state: ContentionSessionState,
    pub woundable: bool,
    pub retained: bool,
    pub active_leases: usize,
}

/// Snapshot of a queued lock waiter on a contended document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WaiterSnapshot {
    pub session_id: u64,
    pub txn_id: Option<String>,
    pub kind: SessionKind,
    pub seq: u64,
    pub mode: ContentionLockMode,
    pub waited_ms: u64,
}

/// Snapshot of a single document currently experiencing lock queue contention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContendedDocumentSnapshot {
    pub document: String,
    pub writer: Option<WriterSnapshot>,
    pub readers: Vec<ReaderSnapshot>,
    /// Total number of active/retained read holders before `readers` is capped at
    /// [`MAX_SNAPSHOT_DOC_PARTICIPANTS`].
    pub reader_count: usize,
    pub waiters: Vec<WaiterSnapshot>,
    /// Total number of queued waiters before `waiters` is capped at
    /// [`MAX_SNAPSHOT_DOC_PARTICIPANTS`].
    pub waiter_count: usize,
}

/// Recorded historical contention event in a database's bounded ring buffer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentionEvent {
    pub id: u64,
    pub kind: ContentionEventKind,
    pub timestamp: Timestamp,
    pub doc_name: Option<String>,
    pub session_id: u64,
    pub txn_id: Option<String>,
    pub session_kind: SessionKind,
    pub seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wounder_session_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wounder_txn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wounder_kind: Option<SessionKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wounder_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
}

impl ContentionEvent {
    fn new(
        kind: ContentionEventKind,
        session_id: u64,
        txn_id: Option<TransactionId>,
        session_kind: SessionKind,
        seq: Option<u64>,
        doc_name: Option<String>,
    ) -> Self {
        Self {
            id: 0,
            kind,
            timestamp: Timestamp::now(),
            doc_name,
            session_id,
            txn_id: txn_id.map(|id| id.to_string()),
            session_kind,
            seq,
            wounder_session_id: None,
            wounder_txn_id: None,
            wounder_kind: None,
            wounder_seq: None,
            wait_ms: None,
            attempt: None,
        }
    }
}

/// Point-in-time contention snapshot for a single database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseContentionSnapshot {
    pub database: RootRef,
    pub summary: ContentionSummary,
    pub contended_documents: Vec<ContendedDocumentSnapshot>,
    /// Total number of contended documents in the database before `contended_documents` is capped
    /// at [`MAX_SNAPSHOT_CONTENDED_DOCS`].
    pub total_contended_documents: usize,
    /// Whether `contended_documents` was truncated at the document level because
    /// `total_contended_documents > MAX_SNAPSHOT_CONTENDED_DOCS` (does not reflect per-document
    /// reader/waiter truncation at [`MAX_SNAPSHOT_DOC_PARTICIPANTS`]).
    pub truncated: bool,
    pub recent_events: Vec<ContentionEvent>,
}

/// Response payload returned by `GET /emulator/v1/contention`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentionSnapshot {
    pub databases: Vec<DatabaseContentionSnapshot>,
}

/// Lightweight invalidation event emitted on `/emulator/v1/contention/events`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentionSseEvent {
    ContentionChanged { database: RootRef },
    DatabaseCleared { database: RootRef },
}

/// RAII guard decrementing the lock-free SSE subscriber count when an SSE stream drops.
#[derive(Debug)]
pub struct ContentionSubscriberGuard {
    subscribers: Arc<AtomicUsize>,
}

impl Drop for ContentionSubscriberGuard {
    fn drop(&mut self) {
        self.subscribers.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Active subscription handle bundling the broadcast receiver and its subscriber count guard.
#[derive(Debug)]
pub struct ContentionSubscription {
    pub rx:    broadcast::Receiver<ContentionSseEvent>,
    pub guard: ContentionSubscriberGuard,
}

/// Lock-free gated broadcast channel for contention invalidation SSE events.
#[derive(Clone, Debug)]
pub(crate) struct ContentionBroadcaster {
    sender:      broadcast::Sender<ContentionSseEvent>,
    subscribers: Arc<AtomicUsize>,
}

impl Default for ContentionBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentionBroadcaster {
    pub(crate) fn new() -> Self {
        Self {
            sender:      broadcast::channel(MAX_CONTENTION_SSE_BACKLOG).0,
            subscribers: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn subscribe(&self) -> ContentionSubscription {
        // Increment before subscribing so producers see the subscriber before missing any send.
        self.subscribers.fetch_add(1, Ordering::AcqRel);
        ContentionSubscription {
            rx:    self.sender.subscribe(),
            guard: ContentionSubscriberGuard {
                subscribers: Arc::clone(&self.subscribers),
            },
        }
    }

    pub(crate) fn subscriber_count(&self) -> usize {
        self.subscribers.load(Ordering::Acquire)
    }

    pub(crate) fn has_subscribers(&self) -> bool {
        self.subscribers.load(Ordering::Acquire) > 0
    }

    pub(crate) fn send_if_subscribed(&self, make_event: impl FnOnce() -> ContentionSseEvent) {
        if self.has_subscribers() {
            let _unused = self.sender.send(make_event());
        }
    }

    pub(crate) fn send_changed_if_subscribed(&self, database: &RootRef) {
        if self.has_subscribers() {
            let _unused = self.sender.send(ContentionSseEvent::ContentionChanged {
                database: database.clone(),
            });
        }
    }
}

/// Metadata describing a lock wait timeout recorded in [`ContentionHistory`].
pub(super) struct TimeoutRecord {
    pub(super) kind: ContentionEventKind,
    pub(super) session_id: u64,
    pub(super) txn_id: Option<TransactionId>,
    pub(super) session_kind: SessionKind,
    pub(super) seq: Option<u64>,
    pub(super) doc_name: Option<String>,
    pub(super) wait_ms: u64,
    pub(super) attempt: Option<u32>,
}

/// Bounded per-database history ring and summary counters protected by its own leaf mutex.
#[derive(Debug, Default)]
pub(super) struct ContentionHistory {
    summary: ContentionSummary,
    events: VecDeque<ContentionEvent>,
    next_event_id: u64,
    pub(super) cleared: bool,
}

impl ContentionHistory {
    fn push_event(&mut self, mut event: ContentionEvent) {
        self.next_event_id += 1;
        event.id = self.next_event_id;
        if self.events.len() >= MAX_CONTENTION_HISTORY {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    pub(super) fn record_wound(
        &mut self,
        wound: &WoundEvent,
        wounder_session_id: u64,
        wounder_txn_id: Option<TransactionId>,
        wounder_kind: SessionKind,
        wounder_seq: Option<u64>,
    ) {
        self.summary.wounds_total += 1;
        let mut event = ContentionEvent::new(
            ContentionEventKind::Wounded,
            wound.victim_session_id,
            wound.victim_txn_id,
            wound.victim_kind,
            (wound.victim_seq != 0).then_some(wound.victim_seq),
            Some(wound.doc_ref.to_string()),
        );
        event.wounder_session_id = Some(wounder_session_id);
        event.wounder_txn_id = wounder_txn_id.map(|id| id.to_string());
        event.wounder_kind = Some(wounder_kind);
        event.wounder_seq = wounder_seq;
        self.push_event(event);
    }

    pub(super) fn record_timeout(&mut self, record: TimeoutRecord) {
        debug_assert!(matches!(
            record.kind,
            ContentionEventKind::ReadTimeout | ContentionEventKind::WriteTimeout
        ));
        self.summary.timeouts_total += 1;
        let mut event = ContentionEvent::new(
            record.kind,
            record.session_id,
            record.txn_id,
            record.session_kind,
            record.seq,
            record.doc_name,
        );
        event.wait_ms = Some(record.wait_ms);
        event.attempt = record.attempt;
        self.push_event(event);
    }

    pub(super) fn record_upgrade_abort(
        &mut self,
        session_id: u64,
        txn_id: Option<TransactionId>,
        session_kind: SessionKind,
        seq: Option<u64>,
        doc_name: String,
    ) {
        self.summary.upgrade_aborts_total += 1;
        let event = ContentionEvent::new(
            ContentionEventKind::UpgradeAbort,
            session_id,
            txn_id,
            session_kind,
            seq,
            Some(doc_name),
        );
        self.push_event(event);
    }

    pub(super) fn record_write_batch_retry(
        &mut self,
        session_id: u64,
        seq: Option<u64>,
        wounder_txn_id: Option<TransactionId>,
        wounder_seq: u64,
        attempt: u32,
    ) {
        self.summary.write_batch_retries_total += 1;
        let mut event = ContentionEvent::new(
            ContentionEventKind::WriteBatchRetry,
            session_id,
            None,
            SessionKind::WriteBatch,
            seq,
            None,
        );
        event.wounder_txn_id = wounder_txn_id.map(|id| id.to_string());
        event.wounder_seq = (wounder_seq != 0).then_some(wounder_seq);
        event.attempt = Some(attempt);
        self.push_event(event);
    }

    pub(super) fn record_expired(&mut self, outcome: &ExpiredSessionOutcome) {
        let event = ContentionEvent::new(
            ContentionEventKind::TransactionExpired,
            outcome.session_id,
            outcome.txn_id,
            outcome.kind,
            outcome.seq,
            outcome.doc_ref.as_ref().map(ToString::to_string),
        );
        self.push_event(event);
    }

    pub(super) fn snapshot(&self) -> (ContentionSummary, Vec<ContentionEvent>) {
        (self.summary, self.events.iter().rev().cloned().collect())
    }

    pub(super) fn clear(&mut self) -> bool {
        let had_data = !self.events.is_empty() || self.summary != ContentionSummary::default();
        self.events.clear();
        self.summary = ContentionSummary::default();
        had_data
    }
}
