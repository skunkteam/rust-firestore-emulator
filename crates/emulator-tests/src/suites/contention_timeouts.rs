use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use googleapis::{fields, val};
use googletest::prelude::*;

use crate::harness::{
    builders::{QuerySpec, WriteBatchSpec},
    contention::ContentionProbe,
    env::TestEnv,
    matchers::{assert_doc_wire_eq, short_ids},
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
    // Hold read locks on `b1`, `b2`, `b3` in three transactions. A multi-doc commit writing
    // `[target, b1, b2, b3]` immediately acquires `write_guard(target)` at t=0 and then awaits each
    // blocker in sequence (< 2s each), keeping `target` write-locked past 5,000ms!
    let mut hold_b1 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b1.get("t/b1").await.unwrap();
    let mut hold_b2 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b2.get("t/b2").await.unwrap();
    let mut hold_b3 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b3.get("t/b3").await.unwrap();

    let mut multi_writer = db.clone();
    let mut hold_target_write = ContentionProbe::spawn_op(async move {
        multi_writer
            .commit_writes(
                WriteBatchSpec::new()
                    .put("t/target", fields!({ "v": 9i64 }))
                    .put("t/b1", fields!({ "v": 9i64 }))
                    .put("t/b2", fields!({ "v": 9i64 }))
                    .put("t/b3", fields!({ "v": 9i64 })),
            )
            .await
    });
    hold_target_write.assert_pending().await;

    let mut reader_db = db.clone();
    let mut read_op = ContentionProbe::spawn_op(async move { reader_db.get("t/target").await });
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
        multi_writer
            .commit_writes(
                WriteBatchSpec::new()
                    .put("c/target", fields!({ "v": 9i64 }))
                    .put("c/b1", fields!({ "v": 9i64 }))
                    .put("c/b2", fields!({ "v": 9i64 })),
            )
            .await
    });
    hold_target_write.assert_pending().await;

    let mut reader_db = db.clone();
    let mut read_op = ContentionProbe::spawn_op(async move { reader_db.get("c/target").await });
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
