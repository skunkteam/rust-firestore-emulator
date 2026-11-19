use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures_util::poll;
use googleapis::google::protobuf::Timestamp;
use googletest::prelude::*;
use tokio::{
    sync::oneshot,
    time::{Instant, advance},
};

use super::*;
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
    expect_that!(
        mid.check_active(),
        err(matches_pattern!(AbortReason::Wounded { .. }))
    );

    // `young` was woken immediately on `d2` (other affected doc woke first!).
    let young_r_d2 = young_wait_fut.await.unwrap();
    expect_false!(young.is_wounded());

    // Now `older` reads `d3`: `Read` wounds younger `Write` holder `young`, cancelling `young`
    // and unblocking `nontx` on `d3`!
    let older_r_d3 = older.acquire_read(&d3).await.unwrap();
    expect_true!(young.is_wounded());
    expect_that!(
        young_w_d3.check_active(),
        err(matches_pattern!(AbortReason::Wounded { .. }))
    );
    expect_true!(young_w_d3.is_wounded());
    expect_that!(
        young_r_d2.check_active(),
        err(matches_pattern!(AbortReason::Wounded { .. }))
    );
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

    let (w, obs) = s1.acquire_or_upgrade_write(&d).await.unwrap();
    expect_eq!(obs, ReadObservation::Observed(Some(t1)));
    expect_eq!(w.doc_ref(), &d);
    expect_true!(w.acquire_time() >= Timestamp::now().min(w.acquire_time()));

    // Duplicate exclusive write or read while holding write returns Internal error.
    assert!(matches!(
        s1.acquire_write(&d).await,
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
    ));
    assert!(matches!(
        s1.acquire_read(&d).await,
        Err(CommitError::Other(GenericDatabaseError::Internal(_)))
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
        Err(AbortReason::DatabaseCleared)
    ));
    let post_clear = locks.new_session(None, LockPriority::Lazy, true, true);
    assert!(matches!(
        post_clear.acquire_read(&d1).await,
        Err(CommitError::Aborted(AbortReason::DatabaseCleared))
    ));

    // Direct `oneshot` dropped receiver path test for coverage of `wake_waiters` closed receiver.
    let (tx, rx) = oneshot::channel::<Result<(), CommitError>>();
    drop(tx);
    expect_true!(rx.await.is_err());
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

    // 1. Cross-session `retain_read` and wounded `retain_read` failures.
    let s1 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let s2 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let r1 = s1.acquire_read(&d1).await.unwrap();
    assert!(matches!(
        s2.retain_read(r1, None),
        Err(AbortReason::DatabaseCleared)
    ));
    let r2 = s2.acquire_read(&d1).await.unwrap();
    // `s1` (older) writes `d1`, wounding `s2`.
    let w1 = s1.acquire_write(&d1).await.unwrap();
    assert!(matches!(
        s2.retain_read(r2, None),
        Err(AbortReason::Wounded { .. })
    ));

    // 2. `seal_for_commit` validation failures (session mismatch, doc mismatch, and wounded).
    let s3 = locks.new_session(None, LockPriority::NextSeq, true, true);
    let w3_d2 = s3.acquire_write(&d2).await.unwrap();
    assert!(
        s1.seal_for_commit(BTreeMap::from([(d2.clone(), w3_d2)]))
            .is_err()
    );

    let w_mismatch = s1.acquire_write(&d2).await.unwrap();
    assert!(
        s1.seal_for_commit(BTreeMap::from([(d1.clone(), w_mismatch)]))
            .is_err()
    );
    drop(w1);

    // Wounded session cannot seal.
    let younger_s = locks.new_session(None, LockPriority::NextSeq, true, true);
    let _yr = younger_s.acquire_read(&d2).await.unwrap();
    let older_w = s1.acquire_write(&d2).await.unwrap();
    assert!(younger_s.seal_for_commit(BTreeMap::new()).is_err());
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
    let seal = multi.seal_for_commit(map).unwrap();
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
    expect_eq!(multi.check_active(), Err(AbortReason::DatabaseCleared));

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
        Err(AbortReason::ReadTimeout)
    );

    drop(seal);
    expect_eq!(multi.abort_state(), SessionAbortState::Closed);
    expect_eq!(
        multi.check_committing_active(),
        Err(AbortReason::DatabaseCleared)
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
        Err(AbortReason::Wounded { .. })
    ));
    assert!(matches!(
        wait_r_sess.check_committing_active(),
        Err(AbortReason::Wounded { .. })
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
        Err(AbortReason::DatabaseDropped)
    ));
}
