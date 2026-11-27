use std::{collections::HashMap, sync::Arc};

use googleapis::google::firestore::v1::{Document, Value, Write, write};
use tokio::sync::broadcast::error::RecvError;

use super::*;
use crate::{
    GenericDatabaseError,
    reference::{CollectionRef, DocumentRef},
};

#[tokio::test]
async fn clear_database_closes_events_and_suppresses_stale_inflight_emissions() {
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let mut proj_rx = project.subscribe_project_events();

    let db1 = project.database(&root).await;
    let mut db_rx = db1.subscribe();

    // Drain initial DatabaseCreated.
    assert!(matches!(
        proj_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseCreated { .. }
    ));

    // Clear while `db1` still has an extra `Arc` held (simulating an in-flight RPC).
    project.clear_database(&root).await;

    // 1. DatabaseEvent channel closes immediately without waiting for `db1` Arc drop.
    assert!(matches!(db_rx.recv().await, Err(RecvError::Closed)));

    // 2. DatabaseCleared is emitted before any subsequent DatabaseCreated.
    assert!(matches!(
        proj_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseCleared { .. }
    ));

    // 3. Stale writes/commits/config changes on cleared `db1` fail or emit no project events.
    let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/d"
        .parse()
        .unwrap();
    let write_err = db1
        .write(Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_ref.to_string(),
                fields: HashMap::from([("k".to_string(), Value::integer(1))]),
                ..Default::default()
            })),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(write_err, GenericDatabaseError::Aborted(_)));

    db1.set_enterprise_edition(true);
    db1.set_optimistic_concurrency(true);
    assert!(proj_rx.try_recv().is_err());

    // 4. Recreating the database emits DatabaseCreated strictly after DatabaseCleared.
    let db2 = project.database(&root).await;
    assert!(!Arc::ptr_eq(&db1, &db2));
    assert!(matches!(
        proj_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseCreated { .. }
    ));
}

#[tokio::test]
async fn database_for_listener_returns_err_default_edition_only_when_absent_and_mismatched() {
    for (default_enterprise, mismatched_req, expected_default) in [
        (
            false,
            DatabaseEdition::Enterprise,
            DatabaseEdition::Standard,
        ),
        (true, DatabaseEdition::Standard, DatabaseEdition::Enterprise),
    ] {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
                default_enterprise,
                ..Default::default()
            })));
        let absent: RootRef = "projects/p/databases/absent".parse().unwrap();
        let overridden: RootRef = "projects/p/databases/overridden".parse().unwrap();
        let mut proj_rx = project.subscribe_project_events();

        // Absent DB with mismatched required edition returns Err(default_edition) without creating.
        assert_eq!(
            project
                .database_for_listener(&absent, Some(mismatched_req))
                .await
                .unwrap_err(),
            expected_default
        );
        assert!(project.get_existing_database(&absent).await.is_none());
        assert!(proj_rx.try_recv().is_err());

        // Absent DB with matching edition or None creates the DB.
        let created = project
            .database_for_listener(&absent, Some(expected_default))
            .await
            .unwrap();
        assert_eq!(created.settings().edition, expected_default);
        assert!(matches!(
            proj_rx.try_recv().unwrap(),
            ProjectEvent::DatabaseCreated { ref database, .. } if database == &absent
        ));

        // Existing DB returns Ok(db) even when its edition differs from required_edition.
        let db_overridden = project.database(&overridden).await;
        let _ev = proj_rx.try_recv().unwrap();
        db_overridden.set_enterprise_edition(!default_enterprise);
        let got = project
            .database_for_listener(&overridden, Some(expected_default))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&got, &db_overridden));
    }
}

#[tokio::test]
async fn clear_database_and_database_creation_preserve_event_order() {
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let mut proj_rx = project.subscribe_project_events();

    drop(project.database(&root).await);
    assert!(matches!(
        proj_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseCreated { .. }
    ));

    let mut exists = true;
    for _ in 0..32 {
        let root_a = root.clone();
        let root_b = root.clone();
        tokio::join!(project.clear_database(&root_a), async move {
            let _db = project.database(&root_b).await;
        });
        while let Ok(ev) = proj_rx.try_recv() {
            match ev {
                ProjectEvent::DatabaseCleared { .. } => {
                    assert!(exists, "DatabaseCleared emitted when DB was absent");
                    exists = false;
                }
                ProjectEvent::DatabaseCreated { .. } => {
                    assert!(!exists, "DatabaseCreated emitted while DB already existed");
                    exists = true;
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn clear_database_emits_contention_database_cleared_and_suppresses_stale_emissions() {
    use std::task::Poll;

    use futures_util::poll;
    use tokio::sync::broadcast::error::TryRecvError;

    use crate::database::lock::{ContentionSseEvent, LockPriority, SessionKind};

    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let db1 = project.database(&root).await;

    let mut sub = project.subscribe_contention_events();
    assert_eq!(project.contention_subscriber_count(), 1);

    let d1: DocumentRef = "projects/p/databases/(default)/documents/c/d"
        .parse()
        .unwrap();
    let holder = db1.locks.new_session(
        None,
        SessionKind::SingleWrite,
        LockPriority::NextSeq,
        false,
        false,
    );
    let held = holder.acquire_write(&d1).await.unwrap();
    let waiter = db1.locks.new_session(
        None,
        SessionKind::PointRead,
        LockPriority::NextSeq,
        false,
        false,
    );
    let mut wait_fut = Box::pin(waiter.acquire_read(&d1));
    assert!(matches!(poll!(&mut wait_fut), Poll::Pending));

    assert_eq!(
        sub.rx.try_recv().unwrap(),
        ContentionSseEvent::ContentionChanged {
            database: root.clone(),
        }
    );

    project.clear_database(&root).await;
    assert_eq!(
        sub.rx.try_recv().unwrap(),
        ContentionSseEvent::DatabaseCleared {
            database: root.clone(),
        }
    );

    // Waiter unblocked by DatabaseCleared; releasing/closing on cleared db1 must not emit stale
    // SSE.
    assert!(wait_fut.await.is_err());
    drop(held);
    holder.close();
    waiter.close();
    assert!(matches!(sub.rx.try_recv(), Err(TryRecvError::Empty)));

    // Snapshot on absent cleared DB returns empty without creating a DB.
    assert!(
        project
            .contention_snapshot(Some(&root))
            .await
            .databases
            .is_empty()
    );
    assert!(matches!(sub.rx.try_recv(), Err(TryRecvError::Empty)));

    drop(sub);
    assert_eq!(project.contention_subscriber_count(), 0);
}

async fn collect_export_bytes(
    project: &FirestoreProject,
    targets: Option<&[RootRef]>,
) -> std::result::Result<Vec<u8>, GenericDatabaseError> {
    let plan = project.prepare_export(targets).await?;
    let mut stream = plan.into_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.expect("export stream should not fail"));
    }
    Ok(out)
}

fn bytes_stream(
    chunks: Vec<std::result::Result<Bytes, SnapshotError>>,
) -> impl Stream<Item = std::result::Result<Bytes, SnapshotError>> + Unpin {
    tokio_stream::iter(chunks)
}

#[tokio::test]
async fn concurrent_write_mvcc_point_in_time_export() {
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let unknown: RootRef = "projects/p/databases/missing".parse().unwrap();
    let col: CollectionRef = format!("{root}/c").parse().unwrap();

    let db = project.database(&root).await;
    db.create_doc(
        &col,
        "d1",
        HashMap::from([("v".to_string(), Value::integer(1))]),
        None,
    )
    .await
    .unwrap();
    db.create_doc(
        &col,
        "d2",
        HashMap::from([("v".to_string(), Value::integer(1))]),
        None,
    )
    .await
    .unwrap();

    // Explicit missing target fails with NotFound without lazy creation.
    let err = project
        .prepare_export(Some(std::slice::from_ref(&unknown)))
        .await
        .unwrap_err();
    assert!(matches!(err, GenericDatabaseError::NotFound(_)));
    assert_eq!(project.database_names().await, vec![root.clone()]);

    // Duplicate explicit targets deduplicate to single_target() == Some(&root).
    let plan = project
        .prepare_export(Some(&[root.clone(), root.clone()]))
        .await
        .unwrap();
    assert_eq!(plan.single_target(), Some(&root));

    // Mutate & delete after prepare_export while streaming; exported snapshot stays at snap
    // time.
    let mut stream = plan.into_stream();
    let d1: DocumentRef = format!("{root}/c/d1").parse().unwrap();
    let d2: DocumentRef = format!("{root}/c/d2").parse().unwrap();
    db.delete_doc(&d1, None).await.unwrap();
    db.update_doc(
        &d2,
        HashMap::from([("v".to_string(), Value::integer(99))]),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    db.create_doc(
        &col,
        "d3",
        HashMap::from([("v".to_string(), Value::integer(3))]),
        None,
    )
    .await
    .unwrap();

    let mut exported = Vec::new();
    while let Some(chunk) = stream.next().await {
        exported.extend_from_slice(&chunk.unwrap());
    }

    let project2: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let summary = project2
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(exported))]))
        .await
        .unwrap();
    assert_eq!(summary.total_databases(), 1);
    assert_eq!(summary.total_documents(), 2);

    let db2 = project2.database(&root).await;
    assert_eq!(
        db2.get_doc(&d1, crate::read_consistency::ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap()
            .fields["v"]
            .as_integer(),
        Some(1)
    );
    assert_eq!(
        db2.get_doc(&d2, crate::read_consistency::ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap()
            .fields["v"]
            .as_integer(),
        Some(1)
    );
    let d3: DocumentRef = format!("{root}/c/d3").parse().unwrap();
    assert!(
        db2.get_doc(&d3, crate::read_consistency::ReadConsistency::Default)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn reconnecting_documents_listener_with_resume_token_while_writes_race_import() {
    use googleapis::google::firestore::v1::{
        ListenRequest, Target, listen_request,
        listen_response::ResponseType,
        target::{self, DocumentsTarget, ResumeType},
        target_change::TargetChangeType,
    };
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let col: CollectionRef = format!("{root}/c").parse().unwrap();
    let doc_ref: DocumentRef = format!("{root}/c/doc1").parse().unwrap();

    let db1 = project.database(&root).await;
    db1.create_doc(
        &col,
        "doc1",
        HashMap::from([("v".to_string(), Value::integer(1))]),
        None,
    )
    .await
    .unwrap();
    let snapshot = collect_export_bytes(project, None).await.unwrap();

    // Capture pre-import resume_token from an initial DocumentsTarget listener.
    let (req_tx, req_rx) = mpsc::channel(4);
    let mut live_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(ListenRequest {
            database: root.to_string(),
            labels: Default::default(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Documents(DocumentsTarget {
                    documents: vec![doc_ref.to_string()],
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await
        .unwrap();

    let mut pre_import_token = Vec::new();
    while let Some(frame) = live_stream.next().await {
        if let Some(ResponseType::TargetChange(tc)) = frame.unwrap().response_type {
            if tc.target_change_type == TargetChangeType::Current as i32 {
                pre_import_token = tc.resume_token;
            } else if tc.target_change_type == TargetChangeType::NoChange as i32
                && tc.target_ids.is_empty()
            {
                break;
            }
        }
    }
    assert!(!pre_import_token.is_empty());

    // Start streaming import, then commit live writes on `db1` while decoder is mid-stream.
    let (chunk_tx, chunk_rx) = mpsc::channel::<std::result::Result<Bytes, SnapshotError>>(4);
    let split_at = snapshot.len() / 2;
    chunk_tx
        .send(Ok(Bytes::copy_from_slice(&snapshot[..split_at])))
        .await
        .unwrap();

    let import_handle =
        tokio::spawn(async move { project.import_snapshot(ReceiverStream::new(chunk_rx)).await });
    tokio::task::yield_now().await;

    // Live write commits on `db1` during decode with timestamp > `pre_import_token`.
    db1.update_doc(
        &doc_ref,
        HashMap::from([("v".to_string(), Value::integer(99))]),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    chunk_tx
        .send(Ok(Bytes::copy_from_slice(&snapshot[split_at..])))
        .await
        .unwrap();
    drop(chunk_tx);
    import_handle.await.unwrap().unwrap();

    // Active listener must eventually converge on restored value `1`.
    let mut saw_restored = false;
    while let Some(frame) = live_stream.next().await {
        match frame.unwrap().response_type {
            Some(ResponseType::DocumentChange(dc)) => {
                if dc.document.as_ref().and_then(|d| d.fields.get("v")) == Some(&Value::integer(1))
                {
                    saw_restored = true;
                }
            }
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty()
                    && saw_restored =>
            {
                break;
            }
            _ => {}
        }
    }
    assert!(saw_restored);
    drop(req_tx);

    // Reconnect a DocumentsTarget listener with the pre-import resume_token: because
    // `import_time` is stamped after `old_db.mark_cleared()`, it strictly exceeds the
    // pre-import token and delivers the restored `v = 1` document.
    let (req2_tx, req2_rx) = mpsc::channel(4);
    let mut reconnected = project.listen(ReceiverStream::new(req2_rx));
    req2_tx
        .send(ListenRequest {
            database: root.to_string(),
            labels: Default::default(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Documents(DocumentsTarget {
                    documents: vec![doc_ref.to_string()],
                })),
                resume_type: Some(ResumeType::ResumeToken(pre_import_token)),
            })),
            request_options: None,
        })
        .await
        .unwrap();

    let mut got_v1 = false;
    while let Some(frame) = reconnected.next().await {
        match frame.unwrap().response_type {
            Some(ResponseType::DocumentChange(dc)) => {
                assert_eq!(
                    dc.document.as_ref().and_then(|d| d.fields.get("v")),
                    Some(&Value::integer(1))
                );
                got_v1 = true;
            }
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty() =>
            {
                break;
            }
            _ => {}
        }
    }
    assert!(got_v1);
    drop(req2_tx);
}

#[tokio::test]
async fn selective_overwrite_empty_db_zero_db_and_project_events() {
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let app: RootRef = "projects/p/databases/app".parse().unwrap();
    let audit: RootRef = "projects/p/databases/audit".parse().unwrap();
    let scratch: RootRef = "projects/p/databases/scratch".parse().unwrap();

    let db_app = project.database(&app).await;
    db_app
        .create_doc(
            &format!("{app}/users").parse().unwrap(),
            "u1",
            HashMap::from([("role".to_string(), Value::string("admin"))]),
            None,
        )
        .await
        .unwrap();
    db_app
        .create_doc(
            &format!("{app}/rooms/r1/messages").parse().unwrap(),
            "m1",
            HashMap::new(),
            None,
        )
        .await
        .unwrap();

    let db_audit = project.database(&audit).await;
    db_audit.set_config(true, true);

    let db_scratch = project.database(&scratch).await;
    db_scratch
        .create_doc(
            &format!("{scratch}/notes").parse().unwrap(),
            "n1",
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
        )
        .await
        .unwrap();

    let snapshot = collect_export_bytes(project, Some(&[app.clone(), audit.clone()]))
        .await
        .unwrap();

    // Mutate all three databases after checkpoint.
    db_app
        .delete_doc(&format!("{app}/users/u1").parse().unwrap(), None)
        .await
        .unwrap();
    db_app.set_config(true, true);
    db_audit
        .create_doc(
            &format!("{audit}/leaked").parse().unwrap(),
            "bad",
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
        )
        .await
        .unwrap();
    db_scratch
        .create_doc(
            &format!("{scratch}/notes").parse().unwrap(),
            "n2",
            HashMap::from([("v".to_string(), Value::integer(2))]),
            None,
        )
        .await
        .unwrap();

    let mut proj_rx = project.subscribe_project_events();
    let summary = project
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(snapshot.clone()))]))
        .await
        .unwrap();

    assert_eq!(summary.total_databases(), 2);
    assert_eq!(summary.total_documents(), 2);
    assert_eq!(summary.import_id, Some(1));
    assert_eq!(
        summary.databases,
        vec![
            ImportedDatabase {
                name: app.clone(),
                settings: crate::DatabaseSettings::for_edition(DatabaseEdition::Standard),
                document_count: 2,
                replaced: true,
            },
            ImportedDatabase {
                name: audit.clone(),
                settings: crate::DatabaseSettings::for_edition(DatabaseEdition::Enterprise),
                document_count: 0,
                replaced: true,
            },
        ]
    );

    let ev1 = proj_rx.try_recv().unwrap();
    let ev2 = proj_rx.try_recv().unwrap();
    assert!(matches!(
        ev1,
        ProjectEvent::DatabaseReplaced {
            ref database,
            import_id: 1,
        } if database == &app
    ));
    assert!(matches!(
        ev2,
        ProjectEvent::DatabaseReplaced {
            ref database,
            import_id: 1,
        } if database == &audit
    ));
    assert!(proj_rx.try_recv().is_err());

    // `app` and `audit` restored; `scratch` untouched.
    let restored_app = project.database(&app).await;
    assert_eq!(
        restored_app.settings(),
        crate::DatabaseSettings::for_edition(DatabaseEdition::Standard)
    );
    assert!(
        restored_app
            .get_doc(
                &format!("{app}/users/u1").parse().unwrap(),
                crate::read_consistency::ReadConsistency::Default,
            )
            .await
            .unwrap()
            .is_some()
    );

    let restored_audit = project.database(&audit).await;
    assert_eq!(
        restored_audit.settings(),
        crate::DatabaseSettings::for_edition(DatabaseEdition::Enterprise)
    );
    assert!(
        restored_audit
            .list_collection_ids(&crate::reference::Ref::Root(audit.clone()), 0, "")
            .await
            .unwrap()
            .0
            .is_empty()
    );

    assert!(
        db_scratch
            .get_doc(
                &format!("{scratch}/notes/n2").parse().unwrap(),
                crate::read_consistency::ReadConsistency::Default,
            )
            .await
            .unwrap()
            .is_some()
    );

    // Fresh project receives `DatabaseCreated` and `replaced == false`.
    let project2: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let mut proj2_rx = project2.subscribe_project_events();
    let created_summary = project2
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(snapshot))]))
        .await
        .unwrap();
    assert_eq!(created_summary.import_id, Some(1));
    assert!(created_summary.databases.iter().all(|d| !d.replaced));
    assert!(matches!(
        proj2_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseCreated {
            ref database,
            import_id: Some(1),
        } if database == &app
    ));
    assert!(matches!(
        proj2_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseCreated {
            ref database,
            import_id: Some(1),
        } if database == &audit
    ));

    // Zero-database snapshot imports as a no-op without modifying live databases.
    let empty_project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let empty_snap = collect_export_bytes(empty_project, None).await.unwrap();
    let noop_summary = project
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(empty_snap))]))
        .await
        .unwrap();
    assert_eq!(noop_summary.total_databases(), 0);
    assert_eq!(noop_summary.total_documents(), 0);
    assert_eq!(noop_summary.import_id, None);
    assert!(proj_rx.try_recv().is_err());
    assert_eq!(project.database_names().await.len(), 3);

    // Next non-empty import increments import_id monotonically (skipping the 0-DB no-op).
    let summary_next = project
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(
            collect_export_bytes(project, Some(std::slice::from_ref(&app)))
                .await
                .unwrap(),
        ))]))
        .await
        .unwrap();
    assert_eq!(summary_next.import_id, Some(2));
    assert!(matches!(
        proj_rx.try_recv().unwrap(),
        ProjectEvent::DatabaseReplaced {
            ref database,
            import_id: 2,
        } if database == &app
    ));
}

#[tokio::test]
async fn import_lock_rejects_overlaps_and_releases_on_errors_or_cancellation() {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let col: CollectionRef = format!("{root}/c").parse().unwrap();
    let doc_ref: DocumentRef = format!("{root}/c/d1").parse().unwrap();

    let db = project.database(&root).await;
    db.create_doc(
        &col,
        "d1",
        HashMap::from([("v".to_string(), Value::integer(1))]),
        None,
    )
    .await
    .unwrap();
    let valid_snapshot = collect_export_bytes(project, None).await.unwrap();

    // Update live state so any failed/cancelled import must leave `v = 2` untouched.
    db.update_doc(
        &doc_ref,
        HashMap::from([("v".to_string(), Value::integer(2))]),
        None,
        None,
        None,
    )
    .await
    .unwrap();

    // 1. Overlapping import fails immediately with ImportInProgress until first completes.
    let (tx, rx) = mpsc::channel::<std::result::Result<Bytes, SnapshotError>>(4);
    let split_at = valid_snapshot.len() / 2;
    tx.send(Ok(Bytes::copy_from_slice(&valid_snapshot[..split_at])))
        .await
        .unwrap();

    let mut import_fut = Box::pin(project.import_snapshot(ReceiverStream::new(rx)));
    assert!(futures_util::poll!(&mut import_fut).is_pending());

    let overlap_err = project
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(valid_snapshot.clone()))]))
        .await
        .unwrap_err();
    assert!(matches!(overlap_err, SnapshotError::ImportInProgress));

    // 2. Cancelling (dropping) an in-progress import future releases `import_lock`.
    drop(import_fut);
    drop(tx);

    // 3. Stream error releases `import_lock` and leaves `v = 2` intact.
    let stream_err = project
        .import_snapshot(bytes_stream(vec![
            Ok(Bytes::copy_from_slice(&valid_snapshot[..split_at])),
            Err(SnapshotError::Stream("transport reset".to_string())),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(stream_err, SnapshotError::Stream(_)));
    assert_eq!(
        db.get_doc(&doc_ref, crate::read_consistency::ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap()
            .fields["v"]
            .as_integer(),
        Some(2)
    );

    // 4. Live task holding import_lock rejects overlapping import until completion, then releases
    //    cleanly so subsequent imports succeed.
    let (tx2, rx2) = mpsc::channel::<std::result::Result<Bytes, SnapshotError>>(4);
    tx2.send(Ok(Bytes::copy_from_slice(&valid_snapshot[..split_at])))
        .await
        .unwrap();
    let import_task =
        tokio::spawn(async move { project.import_snapshot(ReceiverStream::new(rx2)).await });
    tokio::task::yield_now().await;
    let overlap2 = project
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(valid_snapshot.clone()))]))
        .await
        .unwrap_err();
    assert!(matches!(overlap2, SnapshotError::ImportInProgress));
    tx2.send(Ok(Bytes::copy_from_slice(&valid_snapshot[split_at..])))
        .await
        .unwrap();
    drop(tx2);
    let summary = import_task.await.unwrap().unwrap();
    assert_eq!(summary.total_documents(), 1);
    let db_after = project.database(&root).await;
    assert_eq!(
        db_after
            .get_doc(&doc_ref, crate::read_consistency::ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap()
            .fields["v"]
            .as_integer(),
        Some(1)
    );

    // 5. Subsequent import succeeds cleanly after previous import completed.
    let summary2 = project
        .import_snapshot(bytes_stream(vec![Ok(Bytes::from(valid_snapshot))]))
        .await
        .unwrap();
    assert_eq!(summary2.total_documents(), 1);
}
