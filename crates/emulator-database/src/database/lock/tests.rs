use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex as StdMutex, atomic::AtomicU64},
    time::Duration,
};

use futures_util::poll;
use googleapis::google::protobuf::Timestamp;
use googletest::prelude::*;
use tokio::{
    sync::Barrier,
    time::{Instant, advance},
};

use super::{
    table::{AcquireOutcome, LockTable, LockTableGarbage},
    *,
};
use crate::{
    GenericDatabaseError,
    database::{
        reference::{DocumentRef, RootRef},
        timeouts::Timeouts,
    },
    error::{AbortReason, CommitError},
};

fn db_root() -> RootRef {
    "projects/p/databases/(default)".parse().unwrap()
}

fn doc(name: &str) -> DocumentRef {
    format!("projects/p/databases/(default)/documents/c/{name}")
        .parse()
        .unwrap()
}

fn mgr(timeouts: Timeouts) -> Arc<LockManager> {
    Arc::new(LockManager::new(db_root(), timeouts))
}

#[gtest]
#[tokio::test]
async fn lazy_first_lock_priority_assigned_next_seq_and_nontx_tiers() {
    let locks = mgr(Timeouts::FAST);
    let doc_seed = doc("seed");
    let doc_a = doc("a");

    // 1. Lazy session starts with seq == 0 (`root_seq() == None`).
    let s_early = locks.new_session(None, LockPriority::Lazy, true, true);
    expect_eq!(s_early.root_seq(), None);
    let s_late = locks.new_session(None, LockPriority::Lazy, true, true);
    expect_eq!(s_late.root_seq(), None);

    // `s_late` acquires the first lock -> gets lower seq than idle `s_early`.
    let late_r = s_late.acquire_read(&doc_a).await.unwrap();
    expect_eq!(s_late.root_seq(), Some(1));
    s_late.retain_read(late_r, None).unwrap();

    let early_r = s_early.acquire_read(&doc_a).await.unwrap();
    expect_eq!(s_early.root_seq(), Some(2));
    s_early.retain_read(early_r, None).unwrap();

    // `s_early` cannot wound `s_late`, so it releases Read and blocks; `s_late` bypasses it!
    let early_up = s_early.clone();
    let doc_a_clone = doc_a.clone();
    let mut early_fut =
        Box::pin(async move { early_up.acquire_or_upgrade_write(&doc_a_clone).await });
    assert!(poll!(&mut early_fut).is_pending());

    let (late_w, obs) = s_late.acquire_or_upgrade_write(&doc_a).await.unwrap();
    expect_eq!(obs, ReadObservation::Observed(None));
    drop(late_w);
    drop(s_late);

    let (early_w, obs2) = early_fut.await.unwrap();
    expect_eq!(obs2, ReadObservation::Observed(None));
    drop(early_w);
    drop(s_early);

    // 2. Assigned / NextSeq tiers: non-wounding single-doc/Default NextSeq preserves monotonic
    //    priority after earlier sessions while remaining non-woundable, and Assigned preserves
    //    older seq.
    let s_older = locks.new_session(None, LockPriority::NextSeq, true, true);
    let old_seq = s_older.root_seq().unwrap();
    let s_nontx = locks.new_session(None, LockPriority::NextSeq, false, false);
    expect_true!(s_nontx.root_seq().unwrap() > old_seq);

    let r_nontx = s_nontx.acquire_read(&doc_seed).await.unwrap();
    // `s_nontx` has `woundable = false` so `s_older` queues behind it (`Pending`), not wounding.
    let mut old_fut = Box::pin(s_older.acquire_write(&doc_seed));
    assert!(poll!(&mut old_fut).is_pending());
    drop(r_nontx);
    let _w = old_fut.await.unwrap();
    drop(_w);

    let s_retry = locks.new_session(None, LockPriority::Assigned(old_seq), true, true);
    expect_eq!(s_retry.root_seq(), Some(old_seq));
    expect_true!(s_retry.session_id() > s_nontx.session_id());

    // 3. Monotonic NextSeq single-doc write waiter blocks a younger Lazy transaction reader.
    let holder = locks.new_session(None, LockPriority::Lazy, true, true);
    let holder_r = holder.acquire_read(&doc_a).await.unwrap();
    let queued_nontx_w = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut queued_w_fut = Box::pin(queued_nontx_w.acquire_write(&doc_a));
    assert!(poll!(&mut queued_w_fut).is_pending());

    let younger_lazy = locks.new_session(None, LockPriority::Lazy, true, true);
    let mut younger_r_fut = Box::pin(younger_lazy.acquire_read(&doc_a));
    assert!(
        poll!(&mut younger_r_fut).is_pending(),
        "younger Lazy reader must wait behind earlier NextSeq non-wounding write waiter"
    );

    drop(holder_r);
    drop(holder);
    let granted_w = queued_w_fut.await.unwrap();
    assert!(poll!(&mut younger_r_fut).is_pending());
    drop(granted_w);
    drop(queued_nontx_w);
    let _yr = younger_r_fut.await.unwrap();
    drop(_yr);
    drop(younger_lazy);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn read_and_write_wounding_cancels_multi_doc_waiters_and_respects_committing_seal() {
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("d1");
    let d2 = doc("d2");
    let d3 = doc("d3");

    // Sequence order: older (1) < mid (2) < young (3)
    let older = locks.new_session(None, LockPriority::NextSeq, true, true);
    let mid = locks.new_session(None, LockPriority::NextSeq, true, true);
    let young = locks.new_session(None, LockPriority::NextSeq, true, true);

    // `young` holds Write on `d3`, and `mid` holds Write on `d2` (blocking `young` on `d2`) and
    // Read on `d1`.
    let mid_r_d1 = mid.acquire_read(&d1).await.unwrap();
    mid.retain_read(mid_r_d1, None).unwrap();
    let mid_w_d2 = mid.acquire_write(&d2).await.unwrap();

    // `young` tries to read `d2` (blocks behind `mid_w_d2`) while holding nothing yet? No, let
    // `young` hold `d3` and block on `d2` while `unrelated` waits behind `young` on `d3`.
    let young_w_d3 = young.acquire_write(&d3).await.unwrap();
    let young_wait_d2 = young.clone();
    let d2_clone = d2.clone();
    let mut young_wait_fut = Box::pin(async move { young_wait_d2.acquire_read(&d2_clone).await });
    assert!(poll!(&mut young_wait_fut).is_pending());

    let nontx = locks.new_session(None, LockPriority::NextSeq, false, false);
    let d3_clone = d3.clone();
    let mut nontx_d3_fut = Box::pin(async move { nontx.acquire_read(&d3_clone).await });
    assert!(poll!(&mut nontx_d3_fut).is_pending());

    // `older` requests Write on `d1`: wounds `mid` across `d1`, `d2`, and clears `d2` so `young`
    // wakes up on `d2`!
    let (older_w_d1, obs) = older.acquire_or_upgrade_write(&d1).await.unwrap();
    expect_eq!(obs, ReadObservation::NeverRead);
    expect_true!(mid.is_wounded());
    assert!(matches!(
        mid.check_active(),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));

    // `young` was woken immediately on `d2` (other affected doc woke first!).
    let young_r_d2 = young_wait_fut.await.unwrap();
    expect_false!(young.is_wounded());

    // Now `older` reads `d3`: `Read` wounds younger `Write` holder `young`, cancelling `young`
    // and unblocking `nontx` on `d3`!
    let older_r_d3 = older.acquire_read(&d3).await.unwrap();
    expect_true!(young.is_wounded());
    assert!(matches!(
        young_w_d3.check_active(),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    expect_true!(young_w_d3.is_wounded());
    assert!(matches!(
        young_r_d2.check_active(),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    expect_true!(young_r_d2.is_wounded());

    drop(older_r_d3);
    let _nontx_r_d3 = nontx_d3_fut.await.unwrap();
    drop(mid_w_d2);

    // Seal `older` on `d1`: `Committing` is immune from wounding even by an older session!
    let oldest = locks.new_session(None, LockPriority::Assigned(1), true, true);
    // Notice `older` got seq=1 above! Allocate an even older? Let's seal `mid2` vs `older`:
    // Actually `older` has seq=1, give `even_younger` seq=2 and seal it, then test `older` (1)
    // trying to acquire Read/Write against sealed `even_younger`!
    drop(older_w_d1);
    drop(older);

    let seal_holder = locks.new_session(None, LockPriority::NextSeq, true, true);
    let extra_r = seal_holder.acquire_read(&d2).await.unwrap();
    seal_holder.retain_read(extra_r, None).unwrap();
    let w_d1 = seal_holder.acquire_write(&d1).await.unwrap();
    let mut map = BTreeMap::new();
    map.insert(d1.clone(), w_d1);
    let seal = seal_holder.seal_for_commit(map).unwrap();
    expect_eq!(seal.len(), 1);
    expect_false!(seal.is_empty());
    expect_that!(seal.get(&d1), some(anything()));
    expect_that!(seal.check_committing_active(), ok(()));
    expect_eq!(seal.doc_refs().count(), 1);

    // `seal_holder` was un-upgraded on `d2`, so sealing released `d2` cleanly!
    let check_d2 = oldest.acquire_write(&d2).await.unwrap();
    drop(check_d2);

    // `oldest` (seq=1) cannot wound `Committing` `seal_holder` on `d1`, so it queues cleanly.
    let d1_clone = d1.clone();
    let oldest_clone = oldest.clone();
    let mut blocked_by_seal = Box::pin(async move { oldest_clone.acquire_write(&d1_clone).await });
    assert!(poll!(&mut blocked_by_seal).is_pending());

    drop(seal);
    let w_after_seal = blocked_by_seal.await.unwrap();
    drop(w_after_seal);
}

#[gtest]
#[tokio::test]
async fn reentrant_reads_first_observation_preservation_and_release() {
    let locks = mgr(Timeouts::FAST);
    let d = doc("reentrant");
    let s1 = locks.new_session(None, LockPriority::NextSeq, true, true);

    let t1 = Timestamp {
        seconds: 10,
        nanos:   5,
    };
    let t2 = Timestamp {
        seconds: 20,
        nanos:   0,
    };

    let r1 = s1.acquire_read(&d).await.unwrap();
    let r2 = s1.acquire_read(&d).await.unwrap();
    expect_eq!(r1.doc_ref(), &d);
    expect_that!(r1.check_active(), ok(()));

    s1.retain_read(r1, Some(t1)).unwrap();
    // Second retain must preserve earlier `t1` observation!
    s1.retain_read(r2, Some(t2)).unwrap();

    let ts_before = Timestamp::now();
    let (w, obs) = s1.acquire_or_upgrade_write(&d).await.unwrap();
    expect_eq!(obs, ReadObservation::Observed(Some(t1)));
    expect_eq!(w.doc_ref(), &d);
    expect_true!(w.acquire_time() >= ts_before);

    // Duplicate exclusive write returns Internal, while reading while holding write returns
    // InvalidArgument("transaction closed") rather than Internal 500.
    assert!(matches!(
        s1.acquire_write(&d).await,
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
    ));
    assert!(matches!(
        s1.acquire_read(&d).await,
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
    ));
    drop(w);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn timeouts_slow_wait_warnings_waiter_guard_revocation_and_clear() {
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("t1");
    let d2 = doc("t2");

    // 1. Hold write on `d1`, test WriteTimeout at 2s and ReadTimeout at 5s.
    let holder = locks.new_session(None, LockPriority::NextSeq, true, true);
    let holder_w = holder.acquire_write(&d1).await.unwrap();

    let blocked_writer = locks.new_session(None, LockPriority::NextSeq, true, true);
    let blocked_w_r = blocked_writer.acquire_read(&d2).await.unwrap();
    blocked_writer.retain_read(blocked_w_r, None).unwrap();

    let bw_clone = blocked_writer.clone();
    let d1_w = d1.clone();
    let mut bw_fut = Box::pin(async move { bw_clone.acquire_write(&d1_w).await });
    assert!(poll!(&mut bw_fut).is_pending());

    advance(Duration::from_millis(1_999)).await;
    tokio::task::yield_now().await;
    assert!(poll!(&mut bw_fut).is_pending());

    advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        bw_fut.await,
        Err(CommitError::Aborted(AbortReason::WriteTimeout))
    ));
    // Timed-out session released all its other held locks (`d2`) immediately!
    let other_w_d2 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let d2_w = other_w_d2.acquire_write(&d2).await.unwrap();
    drop(d2_w);

    // 2. Cancelled Write waiter wakes downstream Read waiter; cancelled already-promoted
    //    WaiterGuard revokes its grant cleanly.
    let w_cancelled = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut w_cancel_fut = Box::pin(w_cancelled.acquire_write(&d1));
    assert!(poll!(&mut w_cancel_fut).is_pending());

    let r_downstream = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut r_downstream_fut = Box::pin(r_downstream.acquire_read(&d1));
    assert!(poll!(&mut r_downstream_fut).is_pending());

    // Drop `w_cancel_fut` -> drops `WaiterGuard` for ungranted waiter.
    drop(w_cancel_fut);
    // Drop `holder_w` -> promotes `r_downstream`, WaiterGuard not consumed yet.
    drop(holder_w);
    // Drop `r_downstream_fut` BEFORE polling it to completion -> revokes already-promoted grant!
    drop(r_downstream_fut);

    // Next writer acquires `d1` immediately because promoted grant was revoked cleanly!
    let verify = locks.new_session(None, LockPriority::NextSeq, true, true);
    let vw = verify.acquire_write(&d1).await.unwrap();
    drop(vw);

    // 3. Multi-doc `acquire_writes_all` cancellation, timeout, and `abort_all(DatabaseCleared)`.
    let h_d1 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let h_d1_w = h_d1.acquire_write(&d1).await.unwrap();

    let batch_s = locks.new_session(None, LockPriority::NextSeq, true, true);
    let targets = BTreeSet::from([d1.clone(), d2.clone()]);
    let deadline = Instant::now() + Timeouts::FAST.write;
    let mut batch_fut = Box::pin(batch_s.acquire_writes_all(&targets, deadline));
    assert!(poll!(&mut batch_fut).is_pending());
    // Dropping in-flight batch future releases granted `d2` and revokes parked `d1`.
    drop(batch_fut);
    drop(batch_s);

    let probe_d2 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let p2_w = probe_d2.acquire_write(&d2).await.unwrap();
    drop(p2_w);

    // Empty `acquire_writes_all` succeeds cleanly.
    let empty_s = locks.new_session(None, LockPriority::NextSeq, true, true);
    expect_true!(
        empty_s
            .acquire_writes_all(&BTreeSet::new(), deadline)
            .await
            .unwrap()
            .is_empty()
    );

    // Sticky `DatabaseCleared` aborts parked waiters even on `woundable = false` sessions.
    let nontx_waiter = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut cleared_fut = Box::pin(nontx_waiter.acquire_read(&d1));
    assert!(poll!(&mut cleared_fut).is_pending());

    locks.abort_all(AbortReason::DatabaseCleared);
    expect_true!(locks.is_cleared());
    assert!(matches!(
        cleared_fut.await,
        Err(CommitError::Aborted(AbortReason::DatabaseCleared))
    ));
    assert!(matches!(
        h_d1_w.check_active(),
        Err(CommitError::Aborted(AbortReason::DatabaseCleared))
    ));
    let post_clear = locks.new_session(None, LockPriority::Lazy, true, true);
    assert!(matches!(
        post_clear.acquire_read(&d1).await,
        Err(CommitError::Aborted(AbortReason::DatabaseCleared))
    ));

    // Real `LockTable::wake_waiters` closed-rx promotion rollback + downstream wakeup + pruning.
    let mut table = LockTable::default();
    let alloc_seq = AtomicU64::new(1);
    let d_roll = doc("closed_rx_roll");
    let register = |table: &mut LockTable, sid: u64, seq: u64| {
        let shared = Arc::new(SessionSharedState::new(seq));
        table.register_session(sid, Arc::clone(&shared), None, seq, false, false);
        shared
    };

    // (a) Closed Read waiter rolls back `doc.readers` & `held_reads`, waking downstream Write.
    let s_hold = register(&mut table, 1, 1);
    let s_closed_r = register(&mut table, 2, 2);
    let s_down_w = register(&mut table, 3, 3);
    let mut g = LockTableGarbage::default();
    assert!(matches!(
        table.try_acquire_or_upgrade_write(1, &s_hold, &d_roll, &alloc_seq, &mut g),
        Ok(AcquireOutcome::Granted { .. })
    ));
    let rx_closed_r = match table.try_acquire_read(2, &s_closed_r, &d_roll, &alloc_seq, &mut g) {
        Ok(AcquireOutcome::Wait { rx, .. }) => rx,
        other => panic!("expected Wait for closed reader, got {other:?}"),
    };
    let (down_w_id, mut rx_down_w) =
        match table.try_acquire_or_upgrade_write(3, &s_down_w, &d_roll, &alloc_seq, &mut g) {
            Ok(AcquireOutcome::Wait { wait_id, rx, .. }) => (wait_id, rx),
            other => panic!("expected Wait for downstream writer, got {other:?}"),
        };
    drop(rx_closed_r);
    table.release_write_lease(1, &d_roll);
    assert!(matches!(rx_down_w.try_recv(), Ok(Ok(()))));
    expect_that!(table.consume_wait_grant(3, &s_down_w, down_w_id), ok(()));
    table.release_write_lease(3, &d_roll);
    expect_eq!(table.counts().0, 0);

    // (b) Closed Write waiter rolls back `doc.writer` & `held_writes`, waking downstream Read.
    let s_closed_w = register(&mut table, 4, 4);
    let s_down_r = register(&mut table, 5, 5);
    assert!(matches!(
        table.try_acquire_or_upgrade_write(1, &s_hold, &d_roll, &alloc_seq, &mut g),
        Ok(AcquireOutcome::Granted { .. })
    ));
    let rx_closed_w =
        match table.try_acquire_or_upgrade_write(4, &s_closed_w, &d_roll, &alloc_seq, &mut g) {
            Ok(AcquireOutcome::Wait { rx, .. }) => rx,
            other => panic!("expected Wait for closed writer, got {other:?}"),
        };
    let (down_r_id, mut rx_down_r) =
        match table.try_acquire_read(5, &s_down_r, &d_roll, &alloc_seq, &mut g) {
            Ok(AcquireOutcome::Wait { wait_id, rx, .. }) => (wait_id, rx),
            other => panic!("expected Wait for downstream reader, got {other:?}"),
        };
    drop(rx_closed_w);
    table.release_write_lease(1, &d_roll);
    assert!(matches!(rx_down_r.try_recv(), Ok(Ok(()))));
    expect_that!(table.consume_wait_grant(5, &s_down_r, down_r_id), ok(()));
    table.release_read_lease(5, &d_roll);
    expect_eq!(table.counts().0, 0);

    // (c) Closed Write waiter with no downstream waiter prunes empty `DocLockState` immediately.
    assert!(matches!(
        table.try_acquire_or_upgrade_write(1, &s_hold, &d_roll, &alloc_seq, &mut g),
        Ok(AcquireOutcome::Granted { .. })
    ));
    let rx_closed_solo =
        match table.try_acquire_or_upgrade_write(4, &s_closed_w, &d_roll, &alloc_seq, &mut g) {
            Ok(AcquireOutcome::Wait { rx, .. }) => rx,
            other => panic!("expected Wait for solo closed writer, got {other:?}"),
        };
    drop(rx_closed_solo);
    table.release_write_lease(1, &d_roll);
    expect_eq!(table.counts().0, 0);
}

struct DebugLogSink;

impl tracing::Subscriber for DebugLogSink {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, _event: &tracing::Event<'_>) {}

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn error_paths_timeouts_states_and_batch_wait_branches() {
    let _tracing_guard = tracing::subscriber::set_default(DebugLogSink);
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("ep_d1");
    let d2 = doc("ep_d2");

    // 1. Cross-session `retain_read` (Internal mismatch) and wounded `retain_read` failures.
    let s1 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let s2 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r1 = s1.acquire_read(&d1).await.unwrap();
    assert!(matches!(
        s2.retain_read(r1, None),
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
    ));
    let r2 = s2.acquire_read(&d1).await.unwrap();
    // `s1` (older) writes `d1`, wounding `s2`.
    let w1 = s1.acquire_write(&d1).await.unwrap();
    assert!(matches!(
        s2.retain_read(r2, None),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));

    // 2. `seal_for_commit` validation failures (session mismatch, doc mismatch, and wounded).
    let s3 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let w3_d2 = s3.acquire_write(&d2).await.unwrap();
    assert!(matches!(
        s1.clone()
            .seal_for_commit(BTreeMap::from([(d2.clone(), w3_d2)])),
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
    ));

    let w_mismatch = s1.acquire_write(&d2).await.unwrap();
    assert!(matches!(
        s1.clone()
            .seal_for_commit(BTreeMap::from([(d1.clone(), w_mismatch)])),
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
    ));
    drop(w1);

    // Wounded session cannot seal.
    let younger_s = locks.new_session(None, LockPriority::NextSeq, true, true);
    let _yr = younger_s.acquire_read(&d2).await.unwrap();
    let older_w = s1.acquire_write(&d2).await.unwrap();
    assert!(matches!(
        younger_s.seal_for_commit(BTreeMap::new()),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(_yr);
    drop(older_w);

    // 3. Multi-target `CommitSeal` & `check_committing_active` across states + `ReadTimeout`.
    let ts_before = Timestamp::now();
    let multi = locks.new_session(None, LockPriority::NextSeq, true, true);
    let mw1 = multi.acquire_write(&d1).await.unwrap();
    let mw2 = multi.acquire_write(&d2).await.unwrap();
    expect_true!(mw1.acquire_time() >= ts_before);
    let mut map = BTreeMap::new();
    map.insert(d1.clone(), mw1);
    map.insert(d2.clone(), mw2);
    let seal = multi.clone().seal_for_commit(map).unwrap();
    expect_eq!(seal.len(), 2);
    expect_that!(seal.get(&d1), some(anything()));
    expect_that!(seal.get(&d2), some(anything()));
    expect_that!(seal.get(&doc("other")), none());
    expect_eq!(
        seal.doc_refs().cloned().collect::<Vec<_>>(),
        vec![d1.clone(), d2.clone()]
    );
    expect_eq!(multi.abort_state(), SessionAbortState::Committing);
    expect_that!(multi.check_committing_active(), ok(()));
    expect_eq!(
        multi.check_active(),
        Err(CommitError::Other(GenericDatabaseError::invalid_argument(
            "transaction closed"
        )))
    );

    // Blocked reader times out with `ReadTimeout` at 5s while `seal` holds `d1`.
    let r_timeout = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut r_fut = Box::pin(r_timeout.acquire_read(&d1));
    assert!(poll!(&mut r_fut).is_pending());
    advance(Duration::from_millis(5_001)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        r_fut.await,
        Err(CommitError::Aborted(AbortReason::ReadTimeout))
    ));
    expect_eq!(
        r_timeout.abort_state(),
        SessionAbortState::TimedOut(AbortReason::ReadTimeout)
    );
    expect_eq!(
        r_timeout.check_committing_active(),
        Err(CommitError::Aborted(AbortReason::ReadTimeout))
    );

    drop(seal);
    expect_eq!(multi.abort_state(), SessionAbortState::Closed);
    expect_eq!(
        multi.check_committing_active(),
        Err(CommitError::Other(GenericDatabaseError::invalid_argument(
            "transaction closed"
        )))
    );

    // 4. `acquire_read` and `acquire_or_upgrade_write` wounded while waiting.
    let holder = locks.new_session(None, LockPriority::NextSeq, true, true);
    let h_w = holder.acquire_write(&d1).await.unwrap();

    let wait_r_sess = locks.new_session(None, LockPriority::NextSeq, true, true);
    let mut wait_r_fut = Box::pin(wait_r_sess.acquire_read(&d1));
    assert!(poll!(&mut wait_r_fut).is_pending());
    // Older `holder` wounds `wait_r_sess` via `d2`!
    let _yr_d2 = wait_r_sess.acquire_read(&d2).await.unwrap();
    let h_w2 = holder.acquire_write(&d2).await.unwrap();
    assert!(matches!(
        wait_r_fut.await,
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    assert!(matches!(
        wait_r_sess.check_active(),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    assert!(matches!(
        wait_r_sess.check_committing_active(),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(_yr_d2);
    drop(h_w2);

    // Grant-then-wounded race on single waiter (`consume_wait_grant` rejects after `Ok(())` on
    // `rx`). Sequence order: `holder` < `race_holder` < `race_w_sess`, so `race_w_sess` waits on
    // `race_holder` and `holder` can still wound `race_w_sess`.
    drop(h_w);
    let race_holder = locks.new_session(None, LockPriority::NextSeq, true, true);
    let race_hw = race_holder.acquire_write(&d1).await.unwrap();
    let race_w_sess = locks.new_session(None, LockPriority::NextSeq, true, true);
    let _race_r_d2 = race_w_sess.acquire_read(&d2).await.unwrap();
    let mut race_fut = Box::pin(race_w_sess.acquire_read(&d1));
    assert!(poll!(&mut race_fut).is_pending());
    // Release `race_hw` -> sends `Ok(())` on `race_fut`'s oneshot; before polling `race_fut`, wound
    // `race_w_sess` via `holder`.
    drop(race_hw);
    drop(race_holder);
    let _wound_race = holder.acquire_write(&d2).await.unwrap();
    assert!(matches!(
        race_fut.await,
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(_race_r_d2);
    drop(_wound_race);
    let h_w = holder.acquire_write(&d1).await.unwrap();

    // `acquire_writes_all`: wait -> succeed after release; wait -> wounded; wait -> WriteTimeout.
    let targets = BTreeSet::from([d1.clone(), d2.clone()]);
    let b_ok = locks.new_session(None, LockPriority::NextSeq, true, true);
    let deadline_ok = Instant::now() + Timeouts::FAST.write;
    let mut b_ok_fut = Box::pin(b_ok.acquire_writes_all(&targets, deadline_ok));
    assert!(poll!(&mut b_ok_fut).is_pending());
    drop(h_w);
    let granted = b_ok_fut.await.unwrap();
    expect_eq!(granted.len(), 2);
    drop(granted);
    drop(b_ok);

    // Batch wait -> wounded via d2 while waiting on d1.
    let blocker = locks.new_session(None, LockPriority::NextSeq, true, true);
    let bw_d1 = blocker.acquire_write(&d1).await.unwrap();
    let b_wounded = locks.new_session(None, LockPriority::NextSeq, true, true);
    let deadline_w = Instant::now() + Timeouts::FAST.write;
    let mut b_wounded_fut = Box::pin(b_wounded.acquire_writes_all(&targets, deadline_w));
    assert!(poll!(&mut b_wounded_fut).is_pending());
    // `blocker` (older) reads `d2`, wounding `b_wounded`'s granted `d2` lease & parked `d1`.
    let _br_d2 = blocker.acquire_read(&d2).await.unwrap();
    assert!(matches!(
        b_wounded_fut.await,
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(_br_d2);

    // Batch wait -> `WriteTimeout` at `deadline`.
    let b_to = locks.new_session(None, LockPriority::NextSeq, true, true);
    let deadline_to = Instant::now() + Timeouts::FAST.write;
    let mut b_to_fut = Box::pin(b_to.acquire_writes_all(&targets, deadline_to));
    assert!(poll!(&mut b_to_fut).is_pending());
    advance(Duration::from_millis(1_100)).await;
    tokio::task::yield_now().await;
    assert!(poll!(&mut b_to_fut).is_pending());
    advance(Duration::from_millis(901)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        b_to_fut.await,
        Err(CommitError::Aborted(AbortReason::WriteTimeout))
    ));
    drop(bw_d1);
    drop(blocker);

    // 5. `abort_all(DatabaseDropped)` while a `Committing` seal is active.
    let drop_s = locks.new_session(None, LockPriority::NextSeq, true, true);
    let dw = drop_s.acquire_write(&d1).await.unwrap();
    let drop_seal = drop_s
        .seal_for_commit(BTreeMap::from([(d1.clone(), dw)]))
        .unwrap();
    locks.abort_all(AbortReason::DatabaseDropped);
    expect_false!(locks.is_cleared());
    assert!(matches!(
        drop_seal.check_committing_active(),
        Err(CommitError::Aborted(AbortReason::DatabaseDropped))
    ));
}

#[gtest]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn c1_multithread_concurrent_wound_abort_all_and_session_drop_no_deadlock() {
    let d1 = doc("c1_d1");
    let d2 = doc("c1_d2");

    for round in 0..32 {
        let locks = mgr(Timeouts::FAST);
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::with_capacity(8);

        // 4 victim tasks acquiring locks and immediately dropping their last `Arc<LockSession>`
        // concurrently with `wound_holders_on` and `abort_all`.
        for idx in 0..4 {
            let l = Arc::clone(&locks);
            let b = Arc::clone(&barrier);
            let target = if idx % 2 == 0 { d1.clone() } else { d2.clone() };
            handles.push(tokio::spawn(async move {
                let s = l.new_session(None, LockPriority::NextSeq, true, true);
                let r = s.acquire_read(&target).await;
                b.wait().await;
                drop(r);
                drop(s);
            }));
        }

        // 2 older wounders triggering `wound_holders_on` across `d1` and `d2`.
        for idx in 0..2 {
            let l = Arc::clone(&locks);
            let b = Arc::clone(&barrier);
            let target = if idx == 0 { d1.clone() } else { d2.clone() };
            handles.push(tokio::spawn(async move {
                let s = l.new_session(None, LockPriority::Assigned(1), true, true);
                b.wait().await;
                let _res = s.acquire_write(&target).await;
            }));
        }

        // 2 tasks racing `abort_all` (even rounds `DatabaseCleared`, odd rounds `DatabaseDropped`).
        for task_idx in 0..2 {
            let l = Arc::clone(&locks);
            let b = Arc::clone(&barrier);
            let clear_mode = round % 2 == 0 && task_idx == 0;
            handles.push(tokio::spawn(async move {
                b.wait().await;
                if clear_mode {
                    l.abort_all(AbortReason::DatabaseCleared);
                } else {
                    l.abort_all(AbortReason::DatabaseDropped);
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
        expect_eq!(locks.table_counts(), (0, 0));
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn h2_m1_mark_commit_started_keeps_woundable_and_rejects_reads_without_stalling_writes() {
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("mcs_d1");
    let d2 = doc("mcs_d2");

    // 1. `mark_commit_started` preserves `Active` + `woundable = true` while rejecting reads.
    let holder = locks.new_session(None, LockPriority::NextSeq, true, true);
    let s = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r_held = s.acquire_read(&d1).await.unwrap();
    let r_for_retain = s.acquire_read(&d1).await.unwrap();
    s.retain_read(r_for_retain, None).unwrap();
    let w_held = s.acquire_write(&d2).await.unwrap();

    s.mark_commit_started();
    expect_eq!(s.abort_state(), SessionAbortState::Active);
    expect_that!(s.check_active(), ok(()));
    expect_eq!(
        s.check_read_active(),
        Err(CommitError::Other(GenericDatabaseError::invalid_argument(
            "transaction closed"
        )))
    );
    expect_eq!(
        r_held.check_active(),
        Err(CommitError::Other(GenericDatabaseError::invalid_argument(
            "transaction closed"
        )))
    );
    expect_that!(w_held.check_active(), ok(()));
    assert!(matches!(
        s.retain_read(r_held, None),
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
    ));
    assert!(matches!(
        s.acquire_read(&d2).await,
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
    ));

    // Older `holder` can still wound `s` because `woundable = true` until `seal_for_commit`!
    let hw = holder.acquire_write(&d1).await.unwrap();
    expect_true!(s.is_wounded());
    assert!(matches!(
        w_held.check_active(),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(w_held);
    drop(hw);

    // 2. Queued Read waiter whose session calls `mark_commit_started` is rejected in `wake_waiters`
    //    so a queued Commit Write waiter behind it wakes immediately instead of stalling.
    let blocker = locks.new_session(None, LockPriority::NextSeq, false, false);
    let bw = blocker.acquire_write(&d1).await.unwrap();

    let commit_sess = locks.new_session(None, LockPriority::NextSeq, true, true);
    let mut read_wait = Box::pin(commit_sess.acquire_read(&d1));
    assert!(poll!(&mut read_wait).is_pending());

    commit_sess.mark_commit_started();
    let mut write_wait = Box::pin(commit_sess.acquire_write(&d1));
    assert!(poll!(&mut write_wait).is_pending());

    drop(bw);
    assert!(matches!(
        read_wait.await,
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
    ));
    let granted_w = write_wait.await.unwrap();
    drop(granted_w);
    drop(commit_sess);

    // 3. Promoted-but-unconsumed Read grant when `mark_commit_started` runs fails in
    //    `consume_wait_grant` and releases `doc.readers` cleanly for downstream writers.
    let bw2 = blocker.acquire_write(&d1).await.unwrap();
    let s_promoted = locks.new_session(None, LockPriority::NextSeq, true, true);
    let mut prom_read = Box::pin(s_promoted.acquire_read(&d1));
    assert!(poll!(&mut prom_read).is_pending());
    drop(bw2);
    // Do NOT poll `prom_read` yet; call `mark_commit_started()` first!
    s_promoted.mark_commit_started();
    assert!(matches!(
        prom_read.await,
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
    ));
    // `d1` must now be free for a subsequent writer!
    let verify_w = blocker.acquire_write(&d1).await.unwrap();
    drop(verify_w);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn h2_m1_seal_for_commit_drains_dangling_waiters_and_promoted_grants() {
    let locks = mgr(Timeouts::FAST);
    let d_write = doc("seal_w");
    let d_retained = doc("seal_ret");
    let d_parked = doc("seal_park");
    let d_promoted = doc("seal_prom");

    let blocker = locks.new_session(None, LockPriority::NextSeq, false, false);
    let bw_parked = blocker.acquire_write(&d_parked).await.unwrap();
    let bw_promoted = blocker.acquire_write(&d_promoted).await.unwrap();

    let s = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r_ret = s.acquire_read(&d_retained).await.unwrap();
    s.retain_read(r_ret, None).unwrap();
    let w_lease = s.acquire_write(&d_write).await.unwrap();

    let mut parked_fut = Box::pin(s.acquire_read(&d_parked));
    assert!(poll!(&mut parked_fut).is_pending());

    let mut promoted_fut = Box::pin(s.acquire_read(&d_promoted));
    assert!(poll!(&mut promoted_fut).is_pending());

    // Release `bw_promoted` -> promotes `s`'s Read waiter on `d_promoted` before `seal_for_commit`.
    drop(bw_promoted);

    let seal = s
        .clone()
        .seal_for_commit(BTreeMap::from([(d_write.clone(), w_lease)]))
        .unwrap();

    // Both dangling `acquire_read` futures must fail with `InvalidArgument("transaction closed")`,
    // NEVER `AbortReason::DatabaseCleared`!
    assert!(matches!(
        parked_fut.await,
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(msg)))
            if msg == "transaction closed"
    ));
    assert!(matches!(
        promoted_fut.await,
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(msg)))
            if msg == "transaction closed"
    ));

    // Un-upgraded retained read and promoted grant are both released so writers succeed
    // immediately.
    drop(bw_parked);
    let probe = locks.new_session(None, LockPriority::NextSeq, false, false);
    drop(probe.acquire_write(&d_retained).await.unwrap());
    drop(probe.acquire_write(&d_parked).await.unwrap());
    drop(probe.acquire_write(&d_promoted).await.unwrap());
    drop(seal);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn h2_m1_closed_vs_cleared_and_sibling_timeout_preserve_exact_errors() {
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("cc_d1");
    let d2 = doc("cc_d2");

    // 1. Explicit `close()` -> every follow-up method returns `InvalidArgument("transaction
    //    closed")`.
    let s_closed = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r_c = s_closed.acquire_read(&d1).await.unwrap();
    let r_c2 = s_closed.acquire_read(&d1).await.unwrap();
    let w_c = s_closed.acquire_write(&d2).await.unwrap();
    s_closed.close();
    expect_eq!(locks.table_counts(), (0, 0));

    let closed_err =
        CommitError::Other(GenericDatabaseError::invalid_argument("transaction closed"));
    expect_eq!(s_closed.check_active(), Err(closed_err.clone()));
    expect_eq!(s_closed.check_read_active(), Err(closed_err.clone()));
    expect_eq!(s_closed.check_committing_active(), Err(closed_err.clone()));
    expect_eq!(r_c.check_active(), Err(closed_err.clone()));
    expect_eq!(w_c.check_active(), Err(closed_err.clone()));
    expect_eq!(s_closed.retain_read(r_c, None), Err(closed_err.clone()));
    expect_eq!(
        s_closed.retain_reads([(r_c2, None)]),
        Err(closed_err.clone())
    );
    expect_eq!(s_closed.acquire_read(&d1).await.unwrap_err(), closed_err);
    expect_eq!(s_closed.acquire_write(&d1).await.unwrap_err(), closed_err);
    expect_eq!(
        s_closed
            .acquire_writes_all(
                &BTreeSet::from([d1.clone()]),
                Instant::now() + Timeouts::FAST.write
            )
            .await
            .unwrap_err(),
        closed_err
    );
    expect_eq!(
        s_closed
            .clone()
            .seal_for_commit(BTreeMap::from([(d2.clone(), w_c)]))
            .unwrap_err(),
        closed_err
    );

    // 2. Sibling op timeout preserves `ReadTimeout` (NOT `DatabaseCleared`) on concurrent ops.
    let holder = locks.new_session(None, LockPriority::NextSeq, false, false);
    let hw = holder.acquire_write(&d1).await.unwrap();

    let s_sib = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r_sib_held = s_sib.acquire_read(&d2).await.unwrap();
    let r_sib_retain = s_sib.acquire_read(&d2).await.unwrap();
    let mut blocked_read = Box::pin(s_sib.acquire_read(&d1));
    assert!(poll!(&mut blocked_read).is_pending());
    advance(Duration::from_millis(5_001)).await;
    tokio::task::yield_now().await;
    expect_eq!(
        blocked_read.await.unwrap_err(),
        CommitError::Aborted(AbortReason::ReadTimeout)
    );
    expect_eq!(
        r_sib_held.check_active(),
        Err(CommitError::Aborted(AbortReason::ReadTimeout))
    );
    expect_eq!(
        s_sib.retain_read(r_sib_retain, None),
        Err(CommitError::Aborted(AbortReason::ReadTimeout))
    );
    expect_eq!(
        s_sib.acquire_write(&d2).await.unwrap_err(),
        CommitError::Aborted(AbortReason::ReadTimeout)
    );
    expect_eq!(
        s_sib.clone().seal_for_commit(BTreeMap::new()).unwrap_err(),
        CommitError::Aborted(AbortReason::ReadTimeout)
    );
    drop(r_sib_held);
    drop(hw);
    drop(holder);

    // 3. Wounded session does not leak `SessionRecord` in `LockTable::sessions`, while keeping
    //    `Wounded` across subsequent calls.
    let young = locks.new_session(None, LockPriority::Assigned(10), true, true);
    let yr = young.acquire_read(&d1).await.unwrap();
    let older = locks.new_session(None, LockPriority::Assigned(1), true, true);
    let ow = older.acquire_write(&d1).await.unwrap();
    expect_true!(young.is_wounded());
    // Only `older` remains registered in `LockTable::sessions`.
    expect_eq!(locks.table_counts(), (1, 1));
    assert!(matches!(
        young.retain_read(yr, None),
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(ow);
    drop(older);
    expect_eq!(locks.table_counts(), (0, 0));

    // 4. `DatabaseCleared` skips registering new sessions in `self.sessions` while marking Cleared.
    locks.abort_all(AbortReason::DatabaseCleared);
    let post_clear = locks.new_session(None, LockPriority::NextSeq, true, true);
    expect_eq!(locks.table_counts(), (0, 0));
    expect_eq!(
        post_clear.abort_state(),
        SessionAbortState::Cleared(AbortReason::DatabaseCleared)
    );
    expect_eq!(
        post_clear.check_active(),
        Err(CommitError::Aborted(AbortReason::DatabaseCleared))
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn l1_timeout_vs_wound_race_preserves_wounded_state() {
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("l1_d1");
    let d2 = doc("l1_d2");

    // Advance clock to ReadTimeout deadline while `young` is waiting on `d1`, then wound `young`
    // via `d2` before `wait_fut` executes `close_timed_out`. `Wounded` must win over `TimedOut`!
    let older = locks.new_session(None, LockPriority::NextSeq, true, true);
    let nontx = locks.new_session(None, LockPriority::NextSeq, false, false);
    let nw = nontx.acquire_write(&d1).await.unwrap();

    let young = locks.new_session(None, LockPriority::NextSeq, true, true);
    let _yr_d2 = young.acquire_read(&d2).await.unwrap();
    let mut wait_fut = Box::pin(young.acquire_read(&d1));
    assert!(poll!(&mut wait_fut).is_pending());

    advance(Duration::from_millis(5_001)).await;
    // Wound `young` via `d2` before polling `wait_fut` after timer expiration.
    let ow = older.acquire_write(&d2).await.unwrap();
    expect_true!(young.is_wounded());
    assert!(matches!(
        wait_fut.await,
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    assert!(matches!(
        young.abort_state(),
        SessionAbortState::Wounded(AbortReason::Wounded { .. })
    ));
    drop(_yr_d2);
    drop(ow);
    drop(nw);

    // Direct `LockTable::close_session` with `timeout_reason` must never strip `Committing`!
    let mut table = LockTable::default();
    let alloc_seq = AtomicU64::new(1);
    let mut g = LockTableGarbage::default();
    let s_committing = Arc::new(SessionSharedState::new(1));
    table.register_session(1, Arc::clone(&s_committing), None, 1, true, true);
    assert!(matches!(
        table.try_acquire_or_upgrade_write(1, &s_committing, &d1, &alloc_seq, &mut g),
        Ok(AcquireOutcome::Granted { .. })
    ));
    let w_lease = WriteLease::new(
        locks.new_session(None, LockPriority::NextSeq, false, false),
        d1.clone(),
        Timestamp::now(),
    );
    let sealed = BTreeMap::from([(d1.clone(), w_lease)]);
    expect_that!(
        table.seal_for_commit(1, &s_committing, &sealed, &mut g),
        ok(())
    );
    table.close_session(1, Some(AbortReason::WriteTimeout), STATE_TIMED_OUT, &mut g);
    expect_eq!(s_committing.abort_state(), SessionAbortState::Committing);
    expect_eq!(table.counts(), (1, 1));
    // Disarm dummy lease before dropping.
    for mut l in sealed.into_values() {
        l.disarm();
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn partial_reentrant_retain_drop_and_batch_retain_reads_atomicity() {
    let locks = mgr(Timeouts::FAST);
    let da = doc("pr_a");
    let db = doc("pr_b");
    let t1 = Timestamp {
        seconds: 100,
        nanos:   1,
    };
    let t2 = Timestamp {
        seconds: 200,
        nanos:   2,
    };

    // 1. Partial re-entrant retain/drop: retain `r1`, drop `r2` without retaining (`Query`
    //    filter-out path) -> `da` stays retained and keeps `t1` observation.
    let s1 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r1 = s1.acquire_read(&da).await.unwrap();
    let r2 = s1.acquire_read(&da).await.unwrap();
    s1.retain_read(r1, Some(t1)).unwrap();
    drop(r2);

    let probe = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut blocked = Box::pin(probe.acquire_write(&da));
    assert!(poll!(&mut blocked).is_pending());

    let (w_up, obs) = s1.acquire_or_upgrade_write(&da).await.unwrap();
    expect_eq!(obs, ReadObservation::Observed(Some(t1)));
    assert!(poll!(&mut blocked).is_pending());
    drop(w_up);
    drop(blocked.await.unwrap());

    // 2. Batch `retain_reads` retains multiple leases in one lock and preserves observations.
    let ra = s1.acquire_read(&da).await.unwrap();
    let rb = s1.acquire_read(&db).await.unwrap();
    s1.retain_reads([(ra, Some(t1)), (rb, Some(t2))]).unwrap();

    let mut blocked_a = Box::pin(probe.acquire_write(&da));
    let mut blocked_b = Box::pin(probe.acquire_write(&db));
    assert!(poll!(&mut blocked_a).is_pending());
    assert!(poll!(&mut blocked_b).is_pending());

    let (wa, obsa) = s1.acquire_or_upgrade_write(&da).await.unwrap();
    let (wb, obsb) = s1.acquire_or_upgrade_write(&db).await.unwrap();
    expect_eq!(obsa, ReadObservation::Observed(Some(t1)));
    expect_eq!(obsb, ReadObservation::Observed(Some(t2)));
    drop(wa);
    drop(wb);
    drop(blocked_a.await.unwrap());
    drop(blocked_b.await.unwrap());

    // 3. `retain_reads` error atomicity: mixed session lease rejects with `Internal` and drops all
    //    leases cleanly so `da` is NOT left retained.
    let s_other = locks.new_session(None, LockPriority::NextSeq, true, true);
    let ra_ok = s1.acquire_read(&da).await.unwrap();
    let rb_foreign = s_other.acquire_read(&db).await.unwrap();
    assert!(matches!(
        s1.retain_reads([(ra_ok, Some(t1)), (rb_foreign, Some(t2))]),
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
    ));
    drop(probe.acquire_write(&da).await.unwrap());

    // Also verify `retain_reads` fails atomically after `mark_commit_started()` and unlocks `da`.
    let ra_pre = s1.acquire_read(&da).await.unwrap();
    s1.mark_commit_started();
    assert!(matches!(
        s1.retain_reads([(ra_pre, Some(t1))]),
        Err(CommitError::Other(GenericDatabaseError::InvalidArgument(_)))
    ));
    drop(probe.acquire_write(&da).await.unwrap());
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn parked_upgrade_write_wounded_timed_out_and_releases_read_to_wake_earlier_writer() {
    let locks = mgr(Timeouts::FAST);
    let d1 = doc("up_d1");
    let d2 = doc("up_d2");

    // 1. Parked `UpgradeWrite` releases its read hold before parking so an earlier `NextSeq` write
    //    waiter wakes immediately.
    let s_lazy = locks.new_session(None, LockPriority::Lazy, true, true);
    let s_early_w = locks.new_session(None, LockPriority::NextSeq, false, false); // seq = 1
    let r_lazy = s_lazy.acquire_read(&d1).await.unwrap(); // lazy allocates seq = 2
    s_lazy.retain_read(r_lazy, None).unwrap();

    let mut early_w_fut = Box::pin(s_early_w.acquire_write(&d1));
    assert!(poll!(&mut early_w_fut).is_pending());

    let mut lazy_up_fut = Box::pin(s_lazy.acquire_or_upgrade_write(&d1));
    assert!(poll!(&mut lazy_up_fut).is_pending());

    // `s_early_w` must be woken immediately when `s_lazy` released its read hold to park!
    let early_w = early_w_fut.await.unwrap();
    drop(early_w);
    let (lazy_w, obs) = lazy_up_fut.await.unwrap();
    expect_eq!(obs, ReadObservation::Observed(None));
    drop(lazy_w);

    // 2. Parked `UpgradeWrite` is not wounded by `s_older` upgrading `d1` (since `s_young` released
    //    its `d1` read hold when parking), and IS wounded cross-doc via `d2`.
    let s_older = locks.new_session(None, LockPriority::NextSeq, true, true);
    let s_young = locks.new_session(None, LockPriority::NextSeq, true, true);
    let ro = s_older.acquire_read(&d1).await.unwrap();
    s_older.retain_read(ro, None).unwrap();
    let ry_d1 = s_young.acquire_read(&d1).await.unwrap();
    s_young.retain_read(ry_d1, None).unwrap();
    let ry_d2 = s_young.acquire_read(&d2).await.unwrap();
    s_young.retain_read(ry_d2, None).unwrap();

    let mut young_up_fut = Box::pin(s_young.acquire_or_upgrade_write(&d1));
    assert!(poll!(&mut young_up_fut).is_pending());

    let (older_w, _) = s_older.acquire_or_upgrade_write(&d1).await.unwrap();
    expect_false!(s_young.is_wounded());
    assert!(poll!(&mut young_up_fut).is_pending());

    let older_w_d2 = s_older.acquire_write(&d2).await.unwrap();
    assert!(matches!(
        young_up_fut.await,
        Err(CommitError::Aborted(AbortReason::Wounded { .. }))
    ));
    drop(older_w);
    drop(older_w_d2);
    drop(s_older);
    drop(s_young);

    // 3. Parked `UpgradeWrite` times out with `WriteTimeout` and releases other held locks (`d2`).
    let r_nontx = locks.new_session(None, LockPriority::NextSeq, false, false);
    let rn = r_nontx.acquire_read(&d1).await.unwrap();

    let s_up_to = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r1 = s_up_to.acquire_read(&d1).await.unwrap();
    s_up_to.retain_read(r1, None).unwrap();
    let r2 = s_up_to.acquire_read(&d2).await.unwrap();
    s_up_to.retain_read(r2, None).unwrap();

    let mut up_to_fut = Box::pin(s_up_to.acquire_or_upgrade_write(&d1));
    assert!(poll!(&mut up_to_fut).is_pending());
    advance(Duration::from_millis(2_001)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        up_to_fut.await,
        Err(CommitError::Aborted(AbortReason::WriteTimeout))
    ));
    // `d2` must have been released upon timeout!
    drop(r_nontx.acquire_write(&d2).await.unwrap());
    drop(rn);
}

#[derive(Clone)]
struct WarnDocCollector {
    warned_docs: Arc<StdMutex<Vec<String>>>,
}

struct DocNameVisitor(Option<String>);

impl tracing::field::Visit for DocNameVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "doc_name" {
            self.0 = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "doc_name" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

impl tracing::Subscriber for WarnDocCollector {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::WARN
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut visitor = DocNameVisitor(None);
            event.record(&mut visitor);
            if let Some(doc_name) = visitor.0 {
                self.warned_docs.lock().unwrap().push(doc_name);
            }
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn l2_acquire_writes_all_slow_warning_names_active_blocked_doc() {
    let warned_docs = Arc::new(StdMutex::new(Vec::new()));
    let _tracing_guard = tracing::subscriber::set_default(WarnDocCollector {
        warned_docs: Arc::clone(&warned_docs),
    });

    let locks = mgr(Timeouts::FAST);
    let d1 = doc("warn_d1");
    let d2 = doc("warn_d2");

    let holder1 = locks.new_session(None, LockPriority::NextSeq, false, false);
    let holder2 = locks.new_session(None, LockPriority::NextSeq, false, false);
    let w1 = holder1.acquire_write(&d1).await.unwrap();
    let w2 = holder2.acquire_write(&d2).await.unwrap();

    let batch = locks.new_session(None, LockPriority::NextSeq, true, true);
    let targets = BTreeSet::from([d1.clone(), d2.clone()]);
    let deadline = Instant::now() + Timeouts::FAST.write;
    let mut batch_fut = Box::pin(batch.acquire_writes_all(&targets, deadline));
    assert!(poll!(&mut batch_fut).is_pending());

    // Unblock `d1` at 200ms while `d2` stays blocked past 1s!
    advance(Duration::from_millis(200)).await;
    drop(w1);
    tokio::task::yield_now().await;
    assert!(poll!(&mut batch_fut).is_pending());

    advance(Duration::from_millis(900)).await;
    tokio::task::yield_now().await;
    assert!(poll!(&mut batch_fut).is_pending());

    let logged = warned_docs.lock().unwrap().clone();
    expect_eq!(logged, vec![d2.to_string()]);

    drop(w2);
    drop(batch_fut.await.unwrap());
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn close_expired_strips_locks_surfaces_expired_error_and_respects_committing() {
    let locks = mgr(Timeouts::FAST);
    let d_read = doc("exp_read");
    let d_retained = doc("exp_retained");
    let d_write = doc("exp_write");
    let d_blocked = doc("exp_blocked");
    let d_promoted = doc("exp_promoted");

    // 1. Active session holding active ReadLease, retained read, WriteLease, and parked waiter:
    //    `close_expired()` strips all locks, cancels its own parked waiter with
    //    `TransactionExpired`, wakes other parked waiters, and surfaces `TransactionExpired` on
    //    every subsequent session/lease method.
    let holder = locks.new_session(None, LockPriority::NextSeq, false, false);
    let blocker_w = holder.acquire_write(&d_blocked).await.unwrap();

    let s_exp = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r_active = s_exp.acquire_read(&d_read).await.unwrap();
    let r_to_retain = s_exp.acquire_read(&d_retained).await.unwrap();
    s_exp.retain_read(r_to_retain, None).unwrap();
    let r_retain_fail = s_exp.acquire_read(&d_read).await.unwrap();
    let w_held = s_exp.acquire_write(&d_write).await.unwrap();
    let mut own_wait = Box::pin(s_exp.acquire_read(&d_blocked));
    assert!(poll!(&mut own_wait).is_pending());

    let other = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut other_w_read = Box::pin(other.acquire_write(&d_read));
    let mut other_w_ret = Box::pin(other.acquire_write(&d_retained));
    let mut other_w_write = Box::pin(other.acquire_write(&d_write));
    assert!(poll!(&mut other_w_read).is_pending());
    assert!(poll!(&mut other_w_ret).is_pending());
    assert!(poll!(&mut other_w_write).is_pending());

    s_exp.close_expired();

    let exp_err = CommitError::Aborted(AbortReason::TransactionExpired);
    expect_eq!(
        s_exp.abort_state(),
        SessionAbortState::Expired(AbortReason::TransactionExpired)
    );
    expect_eq!(own_wait.await.unwrap_err(), exp_err);
    expect_eq!(r_active.check_active(), Err(exp_err.clone()));
    expect_eq!(w_held.check_active(), Err(exp_err.clone()));
    expect_eq!(s_exp.check_active(), Err(exp_err.clone()));
    expect_eq!(s_exp.check_read_active(), Err(exp_err.clone()));
    expect_eq!(s_exp.check_committing_active(), Err(exp_err.clone()));
    expect_eq!(s_exp.retain_read(r_retain_fail, None), Err(exp_err.clone()));
    expect_eq!(s_exp.acquire_read(&d_read).await.unwrap_err(), exp_err);
    expect_eq!(s_exp.acquire_write(&d_read).await.unwrap_err(), exp_err);
    expect_eq!(
        s_exp
            .acquire_writes_all(
                &BTreeSet::from([d_read.clone()]),
                Instant::now() + Timeouts::FAST.write,
            )
            .await
            .unwrap_err(),
        exp_err
    );
    expect_eq!(
        s_exp
            .clone()
            .seal_for_commit(BTreeMap::from([(d_write.clone(), w_held)]))
            .unwrap_err(),
        exp_err
    );
    // Dropping armed ReadLease after `close_expired` must preserve `STATE_EXPIRED`.
    drop(r_active);
    expect_eq!(
        s_exp.abort_state(),
        SessionAbortState::Expired(AbortReason::TransactionExpired)
    );

    // Waiting writers on all 3 stripped docs wake immediately and acquire their locks.
    drop(other_w_read.await.unwrap());
    drop(other_w_ret.await.unwrap());
    drop(other_w_write.await.unwrap());

    drop(blocker_w);
    drop(holder);
    drop(other);
    expect_eq!(locks.table_counts(), (0, 0));

    // 2. Promoted-waiter-then-expired race before `consume_wait_grant` surfaces
    //    `TransactionExpired` and rolls back the promoted grant for the next waiter.
    let h2 = locks.new_session(None, LockPriority::NextSeq, false, false);
    let h2_w = h2.acquire_write(&d_promoted).await.unwrap();

    let s_prom_exp = locks.new_session(None, LockPriority::NextSeq, true, true);
    let mut prom_fut = Box::pin(s_prom_exp.acquire_read(&d_promoted));
    assert!(poll!(&mut prom_fut).is_pending());

    let s_after = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut after_fut = Box::pin(s_after.acquire_write(&d_promoted));
    assert!(poll!(&mut after_fut).is_pending());

    // Release `h2_w` -> promotes `s_prom_exp` into `promoted_grants` and sends `Ok(())`,
    // then expire `s_prom_exp` BEFORE `prom_fut` polls `consume_wait_grant`.
    drop(h2_w);
    s_prom_exp.close_expired();

    expect_eq!(prom_fut.await.unwrap_err(), exp_err);
    drop(after_fut.await.unwrap());
    drop(h2);
    drop(s_after);
    expect_eq!(locks.table_counts(), (0, 0));

    // 3. `LockTable::close_expired` ignores `SessionState::Committing` and terminal states.
    let mut table = LockTable::default();
    let alloc_seq = AtomicU64::new(1);
    let mut g = LockTableGarbage::default();
    let s_committing = Arc::new(SessionSharedState::new(1));
    table.register_session(1, Arc::clone(&s_committing), None, 1, true, true);
    assert!(matches!(
        table.try_acquire_or_upgrade_write(1, &s_committing, &d_write, &alloc_seq, &mut g),
        Ok(AcquireOutcome::Granted { .. })
    ));
    let dummy_lease = WriteLease::new(
        locks.new_session(None, LockPriority::NextSeq, false, false),
        d_write.clone(),
        Timestamp::now(),
    );
    let mut sealed = BTreeMap::from([(d_write.clone(), dummy_lease)]);
    expect_that!(
        table.seal_for_commit(1, &s_committing, &sealed, &mut g),
        ok(())
    );
    table.close_expired(1, &mut g);
    expect_eq!(s_committing.abort_state(), SessionAbortState::Committing);
    expect_eq!(table.counts(), (1, 1));
    for lease in sealed.values_mut() {
        lease.disarm();
    }
    drop(sealed);
    // Clean up dummy session from `locks` so `locks.table_counts()` is clean too.
    expect_eq!(locks.table_counts(), (0, 0));

    // 4. `LockManager::close_expired_sessions` strips multiple active sessions under a single lock
    //    while skipping non-active sessions.
    let s1 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let s2 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let s_closed = locks.new_session(None, LockPriority::NextSeq, true, true);
    let l1 = s1.acquire_read(&d_read).await.unwrap();
    let l2 = s2.acquire_read(&d_retained).await.unwrap();
    s1.retain_read(l1, None).unwrap();
    s2.retain_read(l2, None).unwrap();
    s_closed.close();
    let wait_other = locks.new_session(None, LockPriority::NextSeq, false, false);
    let mut w1 = Box::pin(wait_other.acquire_write(&d_read));
    let mut w2 = Box::pin(wait_other.acquire_write(&d_retained));
    assert!(poll!(&mut w1).is_pending());
    assert!(poll!(&mut w2).is_pending());

    locks.close_expired_sessions(&[s1, s2, s_closed]);
    drop(w1.await.unwrap());
    drop(w2.await.unwrap());
    drop(wait_other);
    expect_eq!(locks.table_counts(), (0, 0));
}
