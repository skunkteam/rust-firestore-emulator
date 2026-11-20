use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use googleapis::{
    fields,
    google::firestore::v1::{
        TransactionOptions,
        structured_query::{CollectionSelector, field_filter::Operator},
        transaction_options::{self, ReadOnly},
    },
    val,
};
use googletest::prelude::*;
use http::{Method, StatusCode};
use tokio::sync::Barrier;
use tonic::Code;

use crate::harness::{
    builders::{QuerySpec, WriteBatchSpec, field_filter},
    contention::ContentionProbe,
    env::TestEnv,
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
};

#[gtest]
#[tokio::test(start_paused = true)]
async fn query_cursor_and_limit_lock_only_returned_window() {
    let mut db = TestEnv::new().build_db(); // STANDARD -> PESSIMISTIC, Timeouts::FAST

    for i in 1..=5i64 {
        db.put(&format!("locks/d{i}"), fields!({ "k": i }))
            .await
            .unwrap();
    }

    let probe = ContentionProbe::new(&db);
    let mut txn_a = probe.actor_a.clone().begin_rw_txn().await.unwrap();

    // Query starts inclusive at k=2, ends inclusive at k=4, with limit(2) -> returns [d2, d3].
    let out = txn_a
        .query(
            QuerySpec::collection("locks")
                .order_asc("k")
                .start_at(vec![val!(2i64)], true)
                .end_at(vec![val!(4i64)], false)
                .limit(2),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&out.documents), vec!["d2", "d3"]);

    // Outside cursor (`d1`, `d5`) and beyond `limit` (`d4`) are NOT locked: Actor B writes
    // immediately!
    let mut b_free = probe.actor_b.clone();
    b_free
        .put("locks/d1", fields!({ "k": 100i64 }))
        .await
        .expect("d1 before start_at cursor must be unlocked");
    b_free
        .put("locks/d4", fields!({ "k": 400i64 }))
        .await
        .expect("d4 truncated by limit(2) must be unlocked");
    b_free
        .put("locks/d5", fields!({ "k": 500i64 }))
        .await
        .expect("d5 after end_at cursor must be unlocked");

    // Returned `d2` IS read-locked by Txn A: Actor B's write blocks until Txn A rolls back.
    let mut actor_b_d2 = probe.actor_b.clone();
    let mut blocked_write = ContentionProbe::spawn_op(async move {
        actor_b_d2.put("locks/d2", fields!({ "k": 200i64 })).await
    });
    blocked_write.assert_pending().await;

    txn_a.rollback().await.unwrap();
    let updated_d2 = blocked_write
        .expect_ready()
        .await
        .expect("write unblocks immediately once Txn A releases read lock");
    assert_doc_wire_eq(&updated_d2, "locks/d2", &fields!({ "k": 200i64 }));
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn fifo_wakeup_order_and_read_to_write_upgrade_contention_abort() {
    let mut db = TestEnv::new().build_db();
    db.put("contend/x", fields!({ "step": 0i64 }))
        .await
        .unwrap();

    // 1. Two pessimistic transactions both read `contend/x` (Txn 1 first, Txn 2 second). When both
    //    commit writes to `contend/x`, Txn 1 has earlier upgrade priority and commits, while Txn 2
    //    immediately aborts with `Code::Aborted` ("contention") upon seeing `last_updated` changed!
    let mut txn1 = db.begin_rw_txn().await.unwrap();
    let _unused = txn1.get("contend/x").await.unwrap();

    let mut txn2 = db.begin_rw_txn().await.unwrap();
    let _unused = txn2.get("contend/x").await.unwrap();

    txn2.put("contend/x", fields!({ "step": 2i64 }));
    let mut commit2 = ContentionProbe::spawn_op(async move { txn2.commit().await });
    // Txn 2 releases its read lock and waits in the upgrade queue behind Txn 1's read lock.
    commit2.assert_pending().await;

    txn1.put("contend/x", fields!({ "step": 1i64 }));
    txn1.commit()
        .await
        .expect("Txn 1 holds earlier upgrade slot and commits cleanly");

    // Txn 2 wakes immediately and detects contention (`check_time != guard.last_updated()`) ->
    // Aborted!
    commit2.expect_aborted().await;
    assert_doc_wire_eq(
        &db.get("contend/x").await.unwrap(),
        "contend/x",
        &fields!({ "step": 1i64 }),
    );

    // 2. FIFO writer/reader wakeup order: while Txn H holds read lock on `contend/x`, queue Writer
    //    1 then Reader 2 (pessimistic txn read) then Writer 3, and record exact completion order!
    let holder = {
        let mut t = db.begin_rw_txn().await.unwrap();
        let _unused = t.get("contend/x").await.unwrap();
        t
    };

    let order_log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    let mut w1_db = db.clone();
    let log1 = Arc::clone(&order_log);
    let mut op_w1 = ContentionProbe::spawn_op(async move {
        let res = w1_db.put("contend/x", fields!({ "step": 10i64 })).await;
        log1.lock().unwrap().push("w1");
        res
    });
    op_w1.assert_pending().await;

    let mut r2_db = db.clone();
    let log2 = Arc::clone(&order_log);
    let mut op_r2 = ContentionProbe::spawn_op(async move {
        let doc = r2_db.get("contend/x").await?;
        log2.lock().unwrap().push("r2");
        Ok::<_, tonic::Status>(doc)
    });
    op_r2.assert_pending().await;

    let mut w3_db = db.clone();
    let log3 = Arc::clone(&order_log);
    let mut op_w3 = ContentionProbe::spawn_op(async move {
        let res = w3_db.put("contend/x", fields!({ "step": 30i64 })).await;
        log3.lock().unwrap().push("w3");
        res
    });
    op_w3.assert_pending().await;

    // Release holder -> W1 runs first (writes 10), then R2 reads 10 (ahead of W3!), then W3 writes
    // 30!
    holder.rollback().await.unwrap();

    let _unused = op_w1.expect_ready().await.unwrap();
    let r2_doc = op_r2.expect_ready().await.unwrap();
    let _unused = op_w3.expect_ready().await.unwrap();

    assert_doc_wire_eq(&r2_doc, "contend/x", &fields!({ "step": 10i64 }));
    expect_eq!(*order_log.lock().unwrap(), vec!["w1", "r2", "w3"]);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn fast_timeout_boundaries_enforce_2s_write_and_5s_read_across_1s_warning_ticks() {
    let mut db = TestEnv::new().build_db(); // Timeouts::FAST: write = 2s, read = 5s
    for id in ["target", "b1", "b2", "b3"] {
        db.put(&format!("t/{id}"), fields!({ "v": 0i64 }))
            .await
            .unwrap();
    }

    // --- Part A: 2s write-lock timeout boundary (crossing the 1s slow-lock warning timer) ---
    let mut holder = db.begin_rw_txn().await.unwrap();
    let _unused = holder.get("t/target").await.unwrap();

    let mut writer_db = db.clone();
    let mut write_op = ContentionProbe::spawn_op(async move {
        writer_db.put("t/target", fields!({ "v": 1i64 })).await
    });
    write_op.assert_pending().await;

    // Advance 1,999ms (crosses 1,000ms slow-lock warning branch): still pending!
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1_999),
        &mut write_op,
        None,
    )
    .await;
    write_op.assert_pending().await;

    // Advance +1ms (total 2,000ms) -> write lock aborts with Code::Aborted!
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1),
        &mut write_op,
        None,
    )
    .await;
    write_op.expect_aborted().await;
    holder.rollback().await.unwrap();

    // --- Part B: 5s read-lock timeout boundary ---
    // Hold read locks on `b1`, `b2`, `b3` in three transactions. A younger RW transaction writing
    // `[a_target, b1, b2, b3]` immediately acquires `WriteLease(a_target)` at t=0 and then awaits
    // each blocker in sequence (< 2s per-doc write timeout each), keeping `a_target` write-locked
    // past 5,000ms without wounding `hold_b*`.
    let mut hold_b1 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b1.get("t/b1").await.unwrap();
    let mut hold_b2 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b2.get("t/b2").await.unwrap();
    let mut hold_b3 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b3.get("t/b3").await.unwrap();

    let mut multi_writer = db.clone();
    let mut hold_target_write = ContentionProbe::spawn_op(async move {
        let mut tx = multi_writer.begin_rw_txn().await?;
        tx.put("t/a_target", fields!({ "v": 9i64 }))
            .put("t/b1", fields!({ "v": 9i64 }))
            .put("t/b2", fields!({ "v": 9i64 }))
            .put("t/b3", fields!({ "v": 9i64 }));
        tx.commit().await
    });
    hold_target_write.assert_pending().await;

    let mut reader_db = db.clone();
    let mut read_op = ContentionProbe::spawn_op(async move { reader_db.get("t/a_target").await });
    read_op.assert_pending().await;

    // t = 1,800ms: release b1 so multi_writer advances to b2 (fresh 2s timer)
    ContentionProbe::advance_and_poll(
        Duration::from_millis(1_800),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    hold_b1.rollback().await.unwrap();
    read_op.assert_pending().await;

    // t = 3,600ms (past 2s write timeout!): read_op is STILL pending, proving read timeout > 2s!
    ContentionProbe::advance_and_poll(
        Duration::from_millis(1_800),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    read_op.assert_pending().await;
    hold_b2.rollback().await.unwrap();

    // t = 4,999ms: still pending!
    ContentionProbe::advance_and_poll(
        Duration::from_millis(1_399),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    read_op.assert_pending().await;

    // t = 5,000ms (+1ms): read_op hits Timeouts::FAST.read (5s) -> Code::Aborted!
    ContentionProbe::advance_and_poll(
        Duration::from_millis(1),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    read_op.expect_aborted().await;

    hold_b3.rollback().await.unwrap();
    let _unused = hold_target_write.expect_ready().await.unwrap();
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn cloud_timeout_boundaries_enforce_15s_write_and_16s_read() {
    let mut db = TestEnv::new().cloud_timeouts().build_db(); // Timeouts::CLOUD: write = 15s, read = 16s
    for id in ["target", "b1", "b2"] {
        db.put(&format!("c/{id}"), fields!({ "v": 0i64 }))
            .await
            .unwrap();
    }

    // --- Part A: 15s write-lock timeout ---
    let mut holder = db.begin_rw_txn().await.unwrap();
    let _unused = holder.get("c/target").await.unwrap();

    let mut writer_db = db.clone();
    let mut write_op = ContentionProbe::spawn_op(async move {
        writer_db.put("c/target", fields!({ "v": 1i64 })).await
    });
    write_op.assert_pending().await;

    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(14_999),
        &mut write_op,
        None,
    )
    .await;
    write_op.assert_pending().await;

    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1),
        &mut write_op,
        None,
    )
    .await;
    write_op.expect_aborted().await;
    holder.rollback().await.unwrap();

    // --- Part B: 16s read-lock timeout ---
    let mut hold_b1 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b1.get("c/b1").await.unwrap();
    let mut hold_b2 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b2.get("c/b2").await.unwrap();

    let mut multi_writer = db.clone();
    let mut hold_target_write = ContentionProbe::spawn_op(async move {
        let mut tx = multi_writer.begin_rw_txn().await?;
        tx.put("c/a_target", fields!({ "v": 9i64 }))
            .put("c/b1", fields!({ "v": 9i64 }))
            .put("c/b2", fields!({ "v": 9i64 }));
        tx.commit().await
    });
    hold_target_write.assert_pending().await;

    let mut reader_db = db.clone();
    let mut read_op = ContentionProbe::spawn_op(async move { reader_db.get("c/a_target").await });
    read_op.assert_pending().await;

    // t = 14,000ms: release b1 so multi_writer holds `target` across t = 16,000ms
    ContentionProbe::advance_and_poll(
        Duration::from_secs(14),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    hold_b1.rollback().await.unwrap();

    // t = 15,999ms (past 15s write timeout!): read_op is still pending!
    ContentionProbe::advance_and_poll(
        Duration::from_millis(1_999),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    read_op.assert_pending().await;

    // t = 16,000ms (+1ms): read_op hits Timeouts::CLOUD.read (16s) -> Code::Aborted!
    ContentionProbe::advance_and_poll(
        Duration::from_millis(1),
        &mut hold_target_write,
        Some(&mut read_op),
    )
    .await;
    read_op.expect_aborted().await;

    hold_b2.rollback().await.unwrap();
    let _unused = hold_target_write.expect_ready().await.unwrap();
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn wound_wait_zero_ms_preemption_covers_read_on_younger_write_query_locks_and_parked_waiters()
{
    let mut db = TestEnv::new().build_db();
    for doc in ["ww/seed", "ww/a", "ww/b", "qww/seed", "qww/d1", "qww/d2"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    // (a) Older Read wounds younger Write (holding `ww/a` and parked behind `blocker` on `ww/b`)
    // at 0ms, cancelling younger's parked waiter immediately without releasing `blocker`.
    let mut blocker = db.begin_rw_txn().await.unwrap();
    let _unused = blocker.get("ww/b").await.unwrap();

    let mut older = db.begin_rw_txn().await.unwrap();
    let _unused = older.get("ww/seed").await.unwrap();

    let mut younger = db.begin_rw_txn().await.unwrap();
    younger
        .put("ww/a", fields!({ "v": 2i64 }))
        .put("ww/b", fields!({ "v": 2i64 }));
    let younger_id = younger.id().to_vec();
    let mut younger_commit = ContentionProbe::spawn_op(younger.commit());
    younger_commit.assert_pending().await;

    // Older reads `ww/a` while `younger` holds `WriteLease(ww/a)` -> wounds `younger` at 0ms!
    let read_a = older
        .get("ww/a")
        .await
        .expect("older read must wound younger write holder at 0ms");
    assert_doc_wire_eq(&read_a, "ww/a", &fields!({ "v": 0i64 }));
    younger_commit.expect_aborted().await;
    assert_status_code(db.get_in_txn("ww/a", &younger_id).await, Code::Aborted);

    blocker.rollback().await.unwrap();
    older.put("ww/a", fields!({ "v": 1i64 }));
    older.commit().await.unwrap();
    assert_doc_wire_eq(
        &db.get("ww/a").await.unwrap(),
        "ww/a",
        &fields!({ "v": 1i64 }),
    );

    // (b) Older writer wounds idle younger query-locked transaction at 0ms, freeing all of its
    // retained query read locks immediately.
    let mut older_q = db.begin_rw_txn().await.unwrap();
    let _unused = older_q.get("qww/seed").await.unwrap();

    let mut younger_q = db.begin_rw_txn().await.unwrap();
    let younger_q_id = younger_q.id().to_vec();
    let q_res = younger_q
        .query(QuerySpec::collection("qww"))
        .await
        .expect("query succeeds");
    expect_eq!(short_ids(&q_res.documents).len(), 3);

    older_q.put("qww/d1", fields!({ "v": 10i64 }));
    older_q
        .commit()
        .await
        .expect("older commit wounds idle younger query lock at 0ms");

    assert_status_code(db.get_in_txn("qww/d2", &younger_q_id).await, Code::Aborted);
    db.put("qww/d2", fields!({ "v": 20i64 }))
        .await
        .expect("retained query read lock on uncontested doc was released on wound");
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn read_time_read_only_transaction_and_listeners_are_immune_to_wounding() {
    let mut db = TestEnv::new().build_db();
    db.put("imm/seed", fields!({ "v": 0i64 })).await.unwrap();
    let created = db.put("imm/d1", fields!({ "v": 1i64 })).await.unwrap();
    let t0 = created.update_time.expect("update_time");

    let mut ro_latest = db.begin_ro_txn(None).await.unwrap();
    assert_doc_wire_eq(
        &ro_latest.get("imm/d1").await.unwrap(),
        "imm/d1",
        &fields!({ "v": 1i64 }),
    );
    let mut ro_at_t0 = db.begin_ro_txn(Some(t0)).await.unwrap();
    let (mut listen_handle, _initial) = db.listen_doc("imm/d1", None).await.unwrap();

    // Concurrent older RW transaction and external write mutate `imm/d1` while listeners and RO
    // transactions stay open.
    let mut older = db.begin_rw_txn().await.unwrap();
    let _unused = older.get("imm/seed").await.unwrap();
    db.put("imm/d1", fields!({ "v": 2i64 })).await.unwrap();
    let _batch = listen_handle.next_batch().await.unwrap();

    older.put("imm/d1", fields!({ "v": 3i64 }));
    older.commit().await.unwrap();
    let _batch2 = listen_handle.next_batch().await.unwrap();

    assert_doc_wire_eq(
        &db.get_at_time("imm/d1", t0).await.unwrap(),
        "imm/d1",
        &fields!({ "v": 1i64 }),
    );
    assert_doc_wire_eq(
        &ro_latest.get("imm/d1").await.unwrap(),
        "imm/d1",
        &fields!({ "v": 1i64 }),
    );
    assert_doc_wire_eq(
        &ro_at_t0.get("imm/d1").await.unwrap(),
        "imm/d1",
        &fields!({ "v": 1i64 }),
    );
    ro_latest.commit().await.expect("empty RO commit succeeds");
    ro_at_t0.rollback().await.expect("RO rollback succeeds");
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn commit_seal_provides_mvcc_atomicity_under_concurrent_read_only_snapshots() {
    let mut db = TestEnv::new().build_db();
    db.put("atom/a", fields!({ "v": 0i64 })).await.unwrap();
    db.put("atom/b", fields!({ "v": 0i64 })).await.unwrap();

    let ro_opts = || TransactionOptions {
        mode: Some(transaction_options::Mode::ReadOnly(ReadOnly {
            consistency_selector: None,
        })),
    };

    for step in 1..=25i64 {
        let mut w_db = db.clone();
        let mut r_batch_db = db.clone();
        let mut r_txn_db = db.clone();

        let writer = tokio::spawn(async move {
            w_db.commit_writes(
                WriteBatchSpec::new()
                    .put("atom/a", fields!({ "v": step }))
                    .put("atom/b", fields!({ "v": step })),
            )
            .await
            .unwrap();
        });

        let batch_reader = tokio::spawn(async move {
            let out = r_batch_db
                .batch_get_new_txn(["atom/a", "atom/b"], ro_opts())
                .await
                .unwrap();
            let docs = out.found_docs();
            assert_eq!(docs.len(), 2);
            let va = docs[0].fields["v"].as_integer().unwrap();
            let vb = docs[1].fields["v"].as_integer().unwrap();
            assert_eq!(
                va, vb,
                "MVCC ReadOnly batch_get observed torn commit at step {step}"
            );
        });

        let txn_reader = tokio::spawn(async move {
            let mut ro = r_txn_db.begin_ro_txn(None).await.unwrap();
            let a = ro.get("atom/a").await.unwrap().fields["v"]
                .as_integer()
                .unwrap();
            let b = ro.get("atom/b").await.unwrap().fields["v"]
                .as_integer()
                .unwrap();
            assert_eq!(
                a, b,
                "MVCC ReadOnly txn observed torn commit at step {step}"
            );
            ro.rollback().await.unwrap();
        });

        let (w, b, t) = tokio::join!(writer, batch_reader, txn_reader);
        w.unwrap();
        b.unwrap();
        t.unwrap();
    }

    assert_doc_wire_eq(
        &db.get("atom/a").await.unwrap(),
        "atom/a",
        &fields!({ "v": 25i64 }),
    );
    assert_doc_wire_eq(
        &db.get("atom/b").await.unwrap(),
        "atom/b",
        &fields!({ "v": 25i64 }),
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn non_transactional_single_doc_and_multi_doc_write_batch_wound_retry_timeout_and_clear() {
    let mut db = TestEnv::new().build_db();
    for doc in ["nt/s1", "nt/seed", "nt/m1", "nt/m2"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    // (a) Single-doc non-transactional commit (`can_wound = false`, `woundable = false`) waits
    // behind a younger RW transaction instead of wounding it.
    let mut rw = db.begin_rw_txn().await.unwrap();
    let _unused = rw.get("nt/s1").await.unwrap();
    let mut single_db = db.clone();
    let mut single_op =
        ContentionProbe::spawn_op(
            async move { single_db.put("nt/s1", fields!({ "v": 10i64 })).await },
        );
    single_op.assert_pending().await;
    rw.rollback().await.unwrap();
    let updated = single_op.expect_ready().await.unwrap();
    assert_doc_wire_eq(&updated, "nt/s1", &fields!({ "v": 10i64 }));

    // (b) Multi-doc non-tx WriteBatch acquires targets concurrently, gets wounded while blocked,
    // and retries server-side within `Timeouts.write` to succeed once holders release.
    let mut blocker = db.begin_rw_txn().await.unwrap();
    let _unused = blocker.get("nt/m1").await.unwrap();
    let mut older = db.begin_rw_txn().await.unwrap();
    let _unused = older.get("nt/seed").await.unwrap();

    let mut batch_db = db.clone();
    let mut batch_op = ContentionProbe::spawn_op(async move {
        batch_db
            .commit_writes(
                WriteBatchSpec::new()
                    .put("nt/m1", fields!({ "v": 1i64 }))
                    .put("nt/m2", fields!({ "v": 1i64 })),
            )
            .await
    });
    batch_op.assert_pending().await;

    // Older reads `nt/m2`, immediately wounding the multi-doc WriteBatch's held `nt/m2` lease.
    let _unused = older
        .get("nt/m2")
        .await
        .expect("older read wounds younger multi-doc WriteBatch on nt/m2");
    batch_op.assert_pending().await;

    older.rollback().await.unwrap();
    blocker.rollback().await.unwrap();
    batch_op
        .expect_ready()
        .await
        .expect("multi-doc WriteBatch retries server-side on wound and commits cleanly");
    assert_doc_wire_eq(
        &db.get("nt/m1").await.unwrap(),
        "nt/m1",
        &fields!({ "v": 1i64 }),
    );
    assert_doc_wire_eq(
        &db.get("nt/m2").await.unwrap(),
        "nt/m2",
        &fields!({ "v": 1i64 }),
    );

    // (c) Persistent contention past `Timeouts.write` fails WriteBatch immediately on
    // `WriteTimeout`.
    let mut hold_timeout = db.begin_rw_txn().await.unwrap();
    let _unused = hold_timeout.get("nt/m1").await.unwrap();
    let mut timeout_db = db.clone();
    let mut batch_timeout = ContentionProbe::spawn_op(async move {
        timeout_db
            .commit_writes(
                WriteBatchSpec::new()
                    .put("nt/m1", fields!({ "v": 2i64 }))
                    .put("nt/m2", fields!({ "v": 2i64 })),
            )
            .await
    });
    batch_timeout.assert_pending().await;
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1_999),
        &mut batch_timeout,
        None,
    )
    .await;
    batch_timeout.assert_pending().await;
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1),
        &mut batch_timeout,
        None,
    )
    .await;
    batch_timeout.expect_aborted().await;
    hold_timeout.rollback().await.unwrap();

    // (d) Clearing the database fails blocked WriteBatch immediately with Aborted (no retry).
    let mut hold_clear = db.begin_rw_txn().await.unwrap();
    let _unused = hold_clear.get("nt/m1").await.unwrap();
    let mut clear_db = db.clone();
    let mut batch_clear = ContentionProbe::spawn_op(async move {
        clear_db
            .commit_writes(
                WriteBatchSpec::new()
                    .put("nt/m1", fields!({ "v": 3i64 }))
                    .put("nt/m2", fields!({ "v": 3i64 })),
            )
            .await
    });
    batch_clear.assert_pending().await;
    db.clear_default_db().await;
    batch_clear.expect_aborted().await;
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn clear_database_aborts_idle_read_write_read_only_and_blocked_waiters_immediately() {
    use emulator_database::{GenericDatabaseError, read_consistency::ReadConsistency};
    use googleapis::google::firestore::v1::transaction_options::{Mode, ReadWrite};

    let mut db = TestEnv::new().build_db();
    db.put("clr/d", fields!({ "v": 1i64 })).await.unwrap();

    let cleared_db = db.project().database(&TestEnv::root_ref()).await;

    let mut rw_idle = db.begin_rw_txn().await.unwrap();
    let _unused = rw_idle.get("clr/d").await.unwrap();
    let rw_id_bytes = rw_idle.id().to_vec();
    let rw_txn_id = rw_id_bytes.clone().try_into().unwrap();

    let mut ro_idle = db.begin_ro_txn(None).await.unwrap();
    let _unused = ro_idle.get("clr/d").await.unwrap();
    let ro_txn_id = ro_idle.id().to_vec().try_into().unwrap();

    let mut writer_db = db.clone();
    let mut blocked_writer =
        ContentionProbe::spawn_op(
            async move { writer_db.put("clr/d", fields!({ "v": 2i64 })).await },
        );
    blocked_writer.assert_pending().await;

    db.clear_default_db().await;
    blocked_writer.expect_aborted().await;

    // On the cleared database instance, idle RW and RO transactions fail with DatabaseCleared.
    let doc_ref: emulator_database::reference::DocumentRef = "projects/test-project/databases/\
                                                              (default)/documents/clr/d"
        .parse()
        .unwrap();
    assert!(matches!(
        cleared_db
            .get_doc(&doc_ref, ReadConsistency::Transaction(rw_txn_id))
            .await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db.commit(vec![], Some(rw_txn_id)).await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db.rollback(rw_txn_id).await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db
            .new_txn(TransactionOptions {
                mode: Some(Mode::ReadWrite(ReadWrite {
                    retry_transaction: rw_id_bytes.clone(),
                    ..Default::default()
                })),
            })
            .await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db
            .get_doc(&doc_ref, ReadConsistency::Transaction(ro_txn_id))
            .await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db.commit(vec![], Some(ro_txn_id)).await,
        Err(GenericDatabaseError::Aborted(_))
    ));

    // Over gRPC, `clear_database` drops the old database so fresh requests hit a recreated empty
    // database with no leaked transaction slots.
    assert_status_code(rw_idle.get("clr/d").await, Code::InvalidArgument);
    assert_status_code(db.rollback_raw(&rw_id_bytes).await, Code::InvalidArgument);
    assert_status_code(ro_idle.get("clr/d").await, Code::InvalidArgument);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn lock_debug_logging_emits_emulator_database_lock_events_via_http() {
    let _tracing = emulator_tracing::DefaultTracing::start();
    let mut db = TestEnv::new().build_db();
    db.put("log/seed", fields!({ "v": 0i64 })).await.unwrap();
    db.put("log/d1", fields!({ "v": 0i64 })).await.unwrap();

    let log_handle = "/emulator/v1/logging/wound-wait-lock-check";
    let log_poll = "/emulator/v1/logging/wound-wait-lock-check:poll";
    let (st, _) = db
        .http_raw_body(
            Method::POST,
            log_handle,
            Some("text/plain"),
            "emulator_database::lock=debug",
        )
        .await;
    expect_eq!(st, StatusCode::OK);

    // Trigger both a wound event and a 1s slow-wait warning.
    let mut older = db.begin_rw_txn().await.unwrap();
    let _unused = older.get("log/seed").await.unwrap();
    let mut younger = db.begin_rw_txn().await.unwrap();
    let _unused = younger.get("log/d1").await.unwrap();
    older.put("log/d1", fields!({ "v": 1i64 }));
    older.commit().await.unwrap();
    let _unused = younger.rollback().await;

    let mut slow_holder = db.begin_rw_txn().await.unwrap();
    let _unused = slow_holder.get("log/d1").await.unwrap();
    let mut waiter_db = db.clone();
    let mut wait_op =
        ContentionProbe::spawn_op(
            async move { waiter_db.put("log/d1", fields!({ "v": 2i64 })).await },
        );
    wait_op.assert_pending().await;
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1_100),
        &mut wait_op,
        None,
    )
    .await;
    slow_holder.rollback().await.unwrap();
    wait_op.expect_ready().await.unwrap();

    let (poll_st, poll_bytes) = db.http_raw_body(Method::POST, log_poll, None, "").await;
    let (del_st, _) = db.http_oneshot(Method::DELETE, log_handle, None).await;
    expect_eq!(poll_st, StatusCode::OK);
    expect_eq!(del_st, StatusCode::OK);

    let captured = String::from_utf8_lossy(&poll_bytes);
    expect_that!(
        captured.as_ref(),
        contains_substring("wounded conflicting lock holder")
    );
    expect_that!(captured.as_ref(), contains_substring("wounder_seq"));
    expect_that!(
        captured.as_ref(),
        contains_substring("waiting more than 1 second")
    );
    expect_that!(captured.as_ref(), contains_substring("doc_name"));
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn clear_database_aborts_queue_free_queries_and_get_read_time() {
    use emulator_database::{
        GenericDatabaseError, query::QueryBuilder, read_consistency::ReadConsistency,
        reference::Ref,
    };

    let mut db = TestEnv::new().build_db();
    let seeded = db.put("clr_q/a", fields!({ "v": 1i64 })).await.unwrap();
    let t_before = seeded.update_time.expect("update_time");

    let cleared_db = db.project().database(&TestEnv::root_ref()).await;
    db.clear_default_db().await;

    // On `cleared_db`, queue-free `Query::once` (`Default`/`ReadTime`) and `get_read_time`
    // (`Default`/`ReadTime`) must return `Err(GenericDatabaseError::Aborted(_))`.
    let selector = CollectionSelector {
        collection_id:   "clr_q".to_string(),
        all_descendants: false,
    };
    let mut default_query =
        QueryBuilder::from(Ref::Root(TestEnv::root_ref()), vec![selector.clone()])
            .consistency(ReadConsistency::Default)
            .build()
            .unwrap();
    assert!(matches!(
        default_query.once(&cleared_db).await,
        Err(GenericDatabaseError::Aborted(_))
    ));

    let mut read_time_query = QueryBuilder::from(Ref::Root(TestEnv::root_ref()), vec![selector])
        .consistency(ReadConsistency::ReadTime(t_before))
        .build()
        .unwrap();
    assert!(matches!(
        read_time_query.once(&cleared_db).await,
        Err(GenericDatabaseError::Aborted(_))
    ));

    assert!(matches!(
        cleared_db.get_read_time(ReadConsistency::Default).await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db
            .get_read_time(ReadConsistency::ReadTime(t_before))
            .await,
        Err(GenericDatabaseError::Aborted(_))
    ));
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn clear_database_while_txn_committing_aborts_cleanly_and_releases_partial_leases() {
    use emulator_database::{
        GenericDatabaseError, read_consistency::ReadConsistency, reference::DocumentRef,
    };

    let mut db = TestEnv::new().build_db();
    db.put("clr_c/a", fields!({ "v": 0i64 })).await.unwrap();
    db.put("clr_c/b", fields!({ "v": 0i64 })).await.unwrap();

    // Put a ReadWrite transaction into `TxnSlot::Committing` (holding a partial `WriteLease` on
    // `clr_c/b` while blocked acquiring `clr_c/a` behind `blocker`).
    let mut blocker = db.begin_rw_txn().await.unwrap();
    let _unused = blocker.get("clr_c/a").await.unwrap();

    let committer = db.begin_rw_txn().await.unwrap();
    let committer_id = committer.id().to_vec();
    let committer_txn_id = committer_id.clone().try_into().unwrap();
    let mut commit_client = db.clone();
    let commit_id_clone = committer_id.clone();
    let mut commit_op = ContentionProbe::spawn_op(async move {
        commit_client
            .commit_raw(
                &commit_id_clone,
                WriteBatchSpec::new()
                    .put("clr_c/a", fields!({ "v": 2i64 }))
                    .put("clr_c/b", fields!({ "v": 2i64 }))
                    .into_writes(),
            )
            .await
    });
    commit_op.assert_pending().await;
    assert_status_code(
        db.begin_retry_rw_txn(&committer_id).await,
        Code::FailedPrecondition,
    );

    let cleared_db = db.project().database(&TestEnv::root_ref()).await;
    db.clear_default_db().await;

    // In-flight `Committing` future must abort immediately.
    commit_op.expect_aborted().await;

    let doc_a_ref: DocumentRef = "projects/test-project/databases/(default)/documents/clr_c/a"
        .parse()
        .unwrap();
    assert!(matches!(
        cleared_db
            .get_doc(&doc_a_ref, ReadConsistency::Transaction(committer_txn_id))
            .await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db.commit(vec![], Some(committer_txn_id)).await,
        Err(GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db.rollback(committer_txn_id).await,
        Err(GenericDatabaseError::Aborted(_))
    ));

    // Over gRPC, the recreated database starts clean with no leaked locks or slots.
    assert_status_code(
        db.get_in_txn("clr_c/a", &committer_id).await,
        Code::InvalidArgument,
    );
    assert_status_code(db.rollback_raw(&committer_id).await, Code::InvalidArgument);
    db.put("clr_c/a", fields!({ "v": 10i64 })).await.unwrap();
    db.put("clr_c/b", fields!({ "v": 10i64 })).await.unwrap();
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn foreign_db_get_doc_rejects_without_pinning_lazy_root_seq() {
    use emulator_database::{
        GenericDatabaseError, read_consistency::ReadConsistency, reference::DocumentRef,
    };

    let mut db = TestEnv::new().build_db();
    db.put("m3/target", fields!({ "v": 0i64 })).await.unwrap();

    let raw_db = db.project().database(&TestEnv::root_ref()).await;
    let foreign_doc: DocumentRef = "projects/test-project/databases/foreign-db/documents/m3/target"
        .parse()
        .unwrap();

    // Direct `get_doc` (`Default`) with foreign-DB `DocumentRef` rejects with `InvalidArgument`.
    assert!(matches!(
        raw_db.get_doc(&foreign_doc, ReadConsistency::Default).await,
        Err(GenericDatabaseError::InvalidArgument(_))
    ));

    // Start `txn_a` first (`Lazy`), then `txn_b` (`Lazy`). Calling `get_doc` /
    // `PessimisticState::read_doc` on `txn_a` with a foreign `DocumentRef` must fail with
    // `InvalidArgument` WITHOUT pinning `txn_a`'s `root_seq`.
    let mut txn_a = db.begin_rw_txn().await.unwrap();
    let txn_a_id = txn_a.id().to_vec().try_into().unwrap();
    let mut txn_b = db.begin_rw_txn().await.unwrap();

    assert!(matches!(
        raw_db
            .get_doc(&foreign_doc, ReadConsistency::Transaction(txn_a_id))
            .await,
        Err(GenericDatabaseError::InvalidArgument(_))
    ));

    // `txn_b` reads a real document first and therefore must receive an OLDER `root_seq` than
    // `txn_a`.
    let _unused = txn_b.get("m3/target").await.unwrap();

    // When `txn_a` now tries to write `m3/target`, it is younger than `txn_b` and must block
    // rather than wounding `txn_b`!
    txn_a.put("m3/target", fields!({ "v": 1i64 }));
    let mut commit_a = ContentionProbe::spawn_op(txn_a.commit());
    commit_a.assert_pending().await;

    assert_doc_wire_eq(
        &txn_b.get("m3/target").await.unwrap(),
        "m3/target",
        &fields!({ "v": 0i64 }),
    );
    txn_b.rollback().await.unwrap();

    commit_a.expect_ready().await.unwrap();
    assert_doc_wire_eq(
        &db.get("m3/target").await.unwrap(),
        "m3/target",
        &fields!({ "v": 1i64 }),
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn run_query_filter_out_preserves_prior_retained_read_hold() {
    let mut db = TestEnv::new().build_db();
    db.put("rq_keep/d1", fields!({ "tag": "keep" }))
        .await
        .unwrap();
    db.put("rq_keep/d4", fields!({ "tag": "drop" }))
        .await
        .unwrap();

    let mut tx = db.begin_rw_txn().await.unwrap();
    assert_doc_wire_eq(
        &tx.get("rq_keep/d4").await.unwrap(),
        "rq_keep/d4",
        &fields!({ "tag": "drop" }),
    );

    // Query scans `rq_keep` (acquiring and dropping an unmatched temporary `ReadLease` on `d4`).
    // Dropping that temporary lease must preserve `d4`'s prior `retained = true` read lock!
    let out = tx
        .query(QuerySpec::collection("rq_keep").where_filter(field_filter(
            "tag",
            Operator::Equal,
            "keep",
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&out.documents), vec!["d1"]);

    let mut writer_db = db.clone();
    let mut blocked_d4 = ContentionProbe::spawn_op(async move {
        writer_db
            .put("rq_keep/d4", fields!({ "tag": "mutated" }))
            .await
    });
    blocked_d4.assert_pending().await;

    tx.rollback().await.unwrap();
    let updated = blocked_d4.expect_ready().await.unwrap();
    assert_doc_wire_eq(&updated, "rq_keep/d4", &fields!({ "tag": "mutated" }));
}

#[gtest]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stress_multithreaded_concurrent_wounds_commits_rollbacks_reads_and_clear() {
    // Phase A: 8 worker threads hammering overlapping docs (wounds, multi-doc commits,
    // non-tx WriteBatch retries, and concurrent reads/commits/rollbacks on shared `txn_id`s)
    // without `clear_database` -> must NEVER return `Code::Internal` or `"database was cleared"`.
    {
        let mut db = TestEnv::new().build_db();
        let docs = ["stress/d0", "stress/d1", "stress/d2", "stress/d3"];
        for d in docs {
            db.put(d, fields!({ "v": 0i64 })).await.unwrap();
        }

        let start = Arc::new(Barrier::new(8));
        let mut handles = Vec::with_capacity(8);
        for worker in 0..8usize {
            let mut wdb = db.clone();
            let barrier = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for round in 0..25usize {
                    let a = docs[(worker + round) % docs.len()];
                    let b = docs[(worker + round + 1) % docs.len()];
                    let val = (worker * 100 + round) as i64;

                    let assert_clean = |ctx: &str, status: &tonic::Status| {
                        assert_ne!(
                            status.code(),
                            Code::Internal,
                            "worker {worker} round {round} {ctx} returned Code::Internal: \
                             {status:?}"
                        );
                        assert!(
                            !status.message().contains("database was cleared"),
                            "worker {worker} round {round} {ctx} returned database was cleared: \
                             {status:?}"
                        );
                    };

                    match (worker + round) % 4 {
                        0 => {
                            // RW txn read + multi-doc commit competing with other workers (wounds).
                            if let Ok(mut tx) = wdb.begin_rw_txn().await {
                                if let Err(e) = tx.get(a).await {
                                    assert_clean("tx.get", &e);
                                    let _unused = tx.rollback().await;
                                    continue;
                                }
                                tx.put(a, fields!({ "v": val }))
                                    .put(b, fields!({ "v": val }));
                                if let Err(e) = tx.commit().await {
                                    assert_clean("tx.commit", &e);
                                }
                            }
                        }
                        1 => {
                            // Race `get_in_txn` and `query_in_txn` against `commit_raw` on same id.
                            if let Ok(tx) = wdb.begin_rw_txn().await {
                                let id = tx.id().to_vec();
                                let mut gdb = wdb.clone();
                                let gid = id.clone();
                                let mut qdb = wdb.clone();
                                let qid = id.clone();
                                let mut cdb = wdb.clone();
                                let cid = id;
                                let (gr, qr, cr) = tokio::join!(
                                    gdb.get_in_txn(a, &gid),
                                    qdb.query_in_txn(QuerySpec::collection("stress"), &qid),
                                    cdb.commit_raw(
                                        &cid,
                                        WriteBatchSpec::new()
                                            .put(a, fields!({ "v": val }))
                                            .put(b, fields!({ "v": val }))
                                            .into_writes(),
                                    ),
                                );
                                if let Err(e) = gr {
                                    assert_clean("race get", &e);
                                }
                                if let Err(e) = qr {
                                    assert_clean("race query", &e);
                                }
                                if let Err(e) = cr {
                                    assert_clean("race commit", &e);
                                }
                            }
                        }
                        2 => {
                            // Race `get_in_txn` against `rollback_raw` on same id.
                            if let Ok(tx) = wdb.begin_rw_txn().await {
                                let id = tx.id().to_vec();
                                let mut gdb = wdb.clone();
                                let gid = id.clone();
                                let mut rdb = wdb.clone();
                                let rid = id;
                                let (gr, rr) =
                                    tokio::join!(gdb.get_in_txn(a, &gid), rdb.rollback_raw(&rid),);
                                if let Err(e) = gr {
                                    assert_clean("rb-race get", &e);
                                }
                                if let Err(e) = rr {
                                    assert_clean("rb-race rollback", &e);
                                }
                            }
                        }
                        _ => {
                            // Non-tx multi-doc WriteBatch colliding with RW txns.
                            if let Err(e) = wdb
                                .commit_writes(
                                    WriteBatchSpec::new()
                                        .put(a, fields!({ "v": val }))
                                        .put(b, fields!({ "v": val })),
                                )
                                .await
                            {
                                assert_clean("commit_writes", &e);
                            }
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    // Phase B: 8 worker threads hammering wounds, reads, commits, rollbacks, and implicit drops
    // while `clear_default_db()` runs repeatedly -> no deadlocks, panics, or `Code::Internal`.
    {
        let mut db = TestEnv::new().build_db();
        let done = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(9));

        let project = db.project();
        let clear_done = Arc::clone(&done);
        let clear_start = Arc::clone(&start);
        let clear_task = tokio::spawn(async move {
            clear_start.wait().await;
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(5)).await;
                project.clear_database(&TestEnv::root_ref()).await;
            }
            clear_done.store(true, Ordering::Release);
        });

        let mut handles = Vec::with_capacity(8);
        for worker in 0..8usize {
            let mut wdb = db.clone();
            let worker_done = Arc::clone(&done);
            let worker_start = Arc::clone(&start);
            handles.push(tokio::spawn(async move {
                worker_start.wait().await;
                let docs = ["sc/d0", "sc/d1", "sc/d2"];
                let mut iter = 0usize;
                while !worker_done.load(Ordering::Acquire) && iter < 80 {
                    iter += 1;
                    let a = docs[(worker + iter) % docs.len()];
                    let b = docs[(worker + iter + 1) % docs.len()];
                    let val = (worker * 1000 + iter) as i64;
                    if let Ok(mut tx) = wdb.begin_rw_txn().await {
                        if let Err(e) = tx.get(a).await {
                            assert_ne!(e.code(), Code::Internal, "Phase B get Internal: {e:?}");
                        }
                        if iter.is_multiple_of(5) {
                            // Drop active transaction holding read lock right across clear/wounds.
                            drop(tx);
                            continue;
                        }
                        tx.put(a, fields!({ "v": val }))
                            .put(b, fields!({ "v": val }));
                        if let Err(e) = tx.commit().await {
                            assert_ne!(e.code(), Code::Internal, "Phase B commit Internal: {e:?}");
                        }
                    }
                    if let Err(e) = wdb
                        .commit_writes(WriteBatchSpec::new().put(a, fields!({ "v": val })))
                        .await
                    {
                        assert_ne!(
                            e.code(),
                            Code::Internal,
                            "Phase B commit_writes Internal: {e:?}"
                        );
                    }
                }
            }));
        }

        clear_task.await.unwrap();
        for h in handles {
            h.await.unwrap();
        }

        // Database remains usable after all concurrent clear + wound stress finishes.
        db.put("sc/final", fields!({ "ok": true })).await.unwrap();
        assert_doc_wire_eq(
            &db.get("sc/final").await.unwrap(),
            "sc/final",
            &fields!({ "ok": true }),
        );
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn write_batch_repeated_wounds_back_off_and_expire_at_2s_write_timeout() {
    let mut db = TestEnv::new().build_db(); // Timeouts::FAST: write = 2s
    for doc in ["wb_wound/seed", "wb_wound/m1", "wb_wound/m2"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    // Acquire `root_seq`s for `blocker` (`m1`) and five older transactions (`older_1..older_5`)
    // before launching the multi-doc `WriteBatch`.
    let mut blocker = db.begin_rw_txn().await.unwrap();
    let _unused = blocker.get("wb_wound/m1").await.unwrap();

    let mut older = Vec::with_capacity(5);
    for _ in 0..5 {
        let mut tx = db.begin_rw_txn().await.unwrap();
        let _unused = tx.get("wb_wound/seed").await.unwrap();
        older.push(tx);
    }

    let mut batch_db = db.clone();
    let mut batch_op = ContentionProbe::spawn_op(async move {
        batch_db
            .commit_writes(
                WriteBatchSpec::new()
                    .put("wb_wound/m1", fields!({ "v": 1i64 }))
                    .put("wb_wound/m2", fields!({ "v": 1i64 })),
            )
            .await
    });
    batch_op.assert_pending().await;

    // Wound #1 (`wound_retries = 0 -> 1`): 0ms `yield_now()` retry still pending on `m1`.
    let mut o1 = older.remove(0);
    let _unused = o1
        .get("wb_wound/m2")
        .await
        .expect("older_1 wounds WriteBatch attempt 0");
    o1.rollback().await.unwrap();
    batch_op.assert_pending().await;

    // Wound #2 (`wound_retries = 1 -> 2`): 1ms backoff -> advance 5ms (`t = 5ms`).
    let mut o2 = older.remove(0);
    let _unused = o2
        .get("wb_wound/m2")
        .await
        .expect("older_2 wounds WriteBatch attempt 1");
    o2.rollback().await.unwrap();
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(5),
        &mut batch_op,
        None,
    )
    .await;
    batch_op.assert_pending().await;

    // Wound #3 (`wound_retries = 2 -> 3`): 2ms backoff -> advance 10ms (`t = 15ms`).
    let mut o3 = older.remove(0);
    let _unused = o3
        .get("wb_wound/m2")
        .await
        .expect("older_3 wounds WriteBatch attempt 2");
    o3.rollback().await.unwrap();
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(10),
        &mut batch_op,
        None,
    )
    .await;
    batch_op.assert_pending().await;

    // Wound #4 (`wound_retries = 3 -> 4`): 4ms backoff -> advance 1,980ms (`t = 1,995ms`).
    let mut o4 = older.remove(0);
    let _unused = o4
        .get("wb_wound/m2")
        .await
        .expect("older_4 wounds WriteBatch attempt 3");
    o4.rollback().await.unwrap();
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1_980),
        &mut batch_op,
        None,
    )
    .await;
    batch_op.assert_pending().await;

    // Wound #5 at `t = 1,995ms` (`wound_retries = 4 -> 5`, 8ms backoff clamped to 2,000ms
    // deadline): still pending at `t = 1,999ms`, then aborts cleanly at `t = 2,000ms`.
    let mut o5 = older.remove(0);
    let _unused = o5
        .get("wb_wound/m2")
        .await
        .expect("older_5 wounds WriteBatch attempt 4");
    o5.rollback().await.unwrap();
    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(4),
        &mut batch_op,
        None,
    )
    .await;
    batch_op.assert_pending().await;

    ContentionProbe::advance_and_poll::<_, std::future::Pending<()>>(
        Duration::from_millis(1),
        &mut batch_op,
        None,
    )
    .await;
    batch_op.expect_aborted().await;

    blocker.rollback().await.unwrap();
    assert_doc_wire_eq(
        &db.get("wb_wound/m1").await.unwrap(),
        "wb_wound/m1",
        &fields!({ "v": 0i64 }),
    );
    assert_doc_wire_eq(
        &db.get("wb_wound/m2").await.unwrap(),
        "wb_wound/m2",
        &fields!({ "v": 0i64 }),
    );
}
