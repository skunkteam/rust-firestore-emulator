use std::{collections::HashMap, sync::Arc, time::Duration};

use googleapis::google::{
    firestore::v1::{
        Document, ListenRequest, ListenResponse, Precondition, StructuredQuery, Target, Value,
        Write, listen_request,
        listen_response::ResponseType,
        precondition,
        structured_query::CollectionSelector,
        target::{self, DocumentsTarget, QueryTarget as ProtoQueryTarget, query_target},
        target_change::TargetChangeType,
        write,
    },
    protobuf::Timestamp,
};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

use super::*;
use crate::{
    DatabaseEdition, DatabaseSettings, FirestoreConfig, FirestoreProject, GenericDatabaseError,
    document::{DocumentVersion, StoredDocumentVersion},
    event::{DatabaseEvent, ProjectEvent},
    query::QueryBuilder,
    reference::{CollectionRef, DocumentRef, RootRef},
    snapshot::{ImportSummary, codec::encode_single_db_bytes},
};

fn add_target_request(
    root: &RootRef,
    target_type: target::TargetType,
    resume_type: Option<target::ResumeType>,
) -> ListenRequest {
    ListenRequest {
        database: root.to_string(),
        labels: Default::default(),
        target_change: Some(listen_request::TargetChange::AddTarget(Target {
            target_id: 1,
            once: false,
            expected_count: None,
            target_type: Some(target_type),
            resume_type,
        })),
        request_options: None,
    }
}

fn add_document_target_request(
    root: &RootRef,
    doc_path: impl Into<String>,
    resume_type: Option<target::ResumeType>,
) -> ListenRequest {
    add_target_request(
        root,
        target::TargetType::Documents(DocumentsTarget {
            documents: vec![doc_path.into()],
        }),
        resume_type,
    )
}

fn add_collection_query_target_request(root: &RootRef, collection_id: &str) -> ListenRequest {
    add_target_request(
        root,
        target::TargetType::Query(ProtoQueryTarget {
            parent:     root.to_string(),
            query_type: Some(query_target::QueryType::StructuredQuery(StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id:   collection_id.to_string(),
                    all_descendants: false,
                }],
                ..Default::default()
            })),
        }),
        None,
    )
}

fn stored_doc_event(
    database: &Arc<FirestoreDatabase>,
    doc_ref: DocumentRef,
    update_time: Timestamp,
    fields: HashMap<String, Value>,
) -> DatabaseEvent {
    DatabaseEvent {
        database: Arc::downgrade(database),
        update_time,
        updates: [(
            doc_ref.clone(),
            DocumentVersion::Stored(Arc::new(StoredDocumentVersion {
                name: doc_ref,
                create_time: update_time,
                update_time,
                fields,
            })),
        )]
        .into(),
    }
}

fn count_target_changes(msgs: &[ResponseType], kind: TargetChangeType) -> usize {
    msgs.iter()
        .filter(|rt| {
            matches!(
                rt,
                ResponseType::TargetChange(tc) if tc.target_change_type == kind as i32
            )
        })
        .count()
}

async fn drain_until_no_change(
    stream: &mut (impl tokio_stream::Stream<Item = Result<ListenResponse>> + Unpin),
) -> Vec<ResponseType> {
    let mut out = Vec::new();
    while let Some(frame) = stream.next().await {
        let resp = frame.expect("listener stream should not fail");
        let rt = resp.response_type.expect("response_type");
        let is_no_change = matches!(
            &rt,
            ResponseType::TargetChange(tc)
                if tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty()
        );
        out.push(rt);
        if is_no_change {
            break;
        }
    }
    out
}

async fn replace_database_via_import(
    project: &'static FirestoreProject,
    root: &RootRef,
    settings: DatabaseSettings,
    docs: &[(&DocumentRef, &HashMap<String, Value>)],
) -> ImportSummary {
    let bytes = encode_single_db_bytes(root, settings, docs);
    project
        .import_snapshot(tokio_stream::iter([Ok(bytes::Bytes::from(bytes))]))
        .await
        .unwrap()
}

#[tokio::test]
async fn dropping_response_stream_terminates_idle_listener_task() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let (req_tx, req_rx) = mpsc::channel(4);

    let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(add_document_target_request(
            &root,
            format!("{root}/col/doc1"),
            None,
        ))
        .await
        .unwrap();

    // Wait until initial snapshot framing reaches TargetChange(NoChange).
    while let Some(frame) = resp_stream.next().await {
        let frame = frame.unwrap();
        if let Some(ResponseType::TargetChange(tc)) = frame.response_type
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    // Drop the response stream while keeping `req_tx` alive on an idle database.
    drop(resp_stream);

    tokio::time::timeout(Duration::from_secs(1), req_tx.closed())
        .await
        .expect("listener task must exit when response receiver is dropped");
}

#[tokio::test]
async fn query_and_document_target_watermarks_and_reset_consistency() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let db = project.database(&root).await;
    let doc_name = format!("{root}/col/doc1");

    // Pre-populate col/doc1.
    db.write(Write {
        operation: Some(write::Operation::Update(Document {
            name: doc_name.clone(),
            ..Default::default()
        })),
        current_document: Some(Precondition {
            condition_type: Some(precondition::ConditionType::Exists(false)),
        }),
        ..Default::default()
    })
    .await
    .unwrap();

    // 1. DocumentTarget::reset emits TargetChange(Reset) + DocumentChange and ignores stale
    //    DatabaseEvents at or before the pinned reset read_time.
    let doc_ref: DocumentRef = doc_name.parse().unwrap();
    let mut doc_target = DocumentTarget {
        name: doc_ref.clone(),
        last_read_time: Default::default(),
    };
    let (doc_reset_time, doc_msgs) = doc_target.reset(&db).await.unwrap();
    assert_eq!(doc_target.last_read_time, doc_reset_time);
    assert_eq!(doc_msgs.len(), 2);
    assert!(matches!(
        &doc_msgs[0],
        ResponseType::TargetChange(tc)
            if tc.target_change_type == TargetChangeType::Reset as i32
    ));
    assert!(matches!(&doc_msgs[1], ResponseType::DocumentChange(_)));

    let stale_doc_event = stored_doc_event(&db, doc_ref, doc_reset_time, Default::default());
    assert!(
        doc_target
            .process_event(&stale_doc_event)
            .unwrap()
            .is_empty()
    );

    // 2. QueryTarget::reset pins `last_reset_time` and ignores events at or before it.
    let mut target = QueryTarget::new(
        QueryBuilder::from_collection(format!("{root}/col").parse().unwrap())
            .build()
            .unwrap()
            .into(),
    );
    let (reset_time, init_msgs) = target.reset(&db).await.unwrap();
    assert_eq!(init_msgs.len(), 2); // Reset + DocumentChange(doc1)

    // An event at `reset_time` (or earlier) must be ignored completely even for an unseen doc.
    let stale_doc_ref: DocumentRef = format!("{root}/col/transient").parse().unwrap();
    let stale_event = stored_doc_event(&db, stale_doc_ref, reset_time, Default::default());
    let (_, skipped) = target.process_event(&db, &stale_event).await.unwrap();
    assert!(skipped.is_empty());
}

#[tokio::test]
async fn document_listener_rejects_invalid_resume_read_time() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let (req_tx, req_rx) = mpsc::channel(4);

    let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(add_document_target_request(
            &root,
            format!("{root}/col/doc1"),
            Some(target::ResumeType::ReadTime(Timestamp {
                seconds: -1,
                nanos:   0,
            })),
        ))
        .await
        .unwrap();

    let mut saw_invalid_arg = false;
    while let Some(frame) = resp_stream.next().await {
        match frame {
            Err(GenericDatabaseError::InvalidArgument(_)) => {
                saw_invalid_arg = true;
                break;
            }
            Ok(resp) => {
                if let Some(ResponseType::TargetChange(tc)) = resp.response_type
                    && tc.target_ids.is_empty()
                {
                    break;
                }
            }
            Err(other) => panic!("unexpected listener error: {other:?}"),
        }
    }
    assert!(
        saw_invalid_arg,
        "expected InvalidArgument for negative resume_type ReadTime"
    );
}

#[tokio::test]
async fn buffered_pre_clear_events_are_ignored_on_compatible_listeners() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let db1 = project.database(&root).await;
    assert!(!db1.enterprise_edition());
    let col: CollectionRef = format!("{root}/col").parse().unwrap();
    let doc_ref: DocumentRef = format!("{root}/col/doc1").parse().unwrap();

    // 1. Direct Listener::process_event ignores events whose database handle is cleared.
    let (sender, mut rx) = mpsc::channel(16);
    let mut listener = Listener {
        id: 999,
        project,
        database_events: Default::default(),
        sender,
        target: Some(ListenerTarget::DocumentTarget(DocumentTarget {
            name: doc_ref.clone(),
            last_read_time: Default::default(),
        })),
    };

    let ts_event = Timestamp::now();
    let buffered_event = stored_doc_event(
        &db1,
        doc_ref.clone(),
        ts_event,
        HashMap::from([("v".to_string(), Value::integer(1))]),
    );
    project.clear_database(&root).await;
    assert!(listener.process_event(&buffered_event).await.is_ok());
    assert!(
        rx.try_recv().is_err(),
        "cleared database event must emit no frames"
    );

    // 2. Live listen stream with buffered pre-clear event followed by replacement converges without
    //    aborting.
    let db_live = project.database(&root).await;
    db_live
        .create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(10))]),
            None,
        )
        .await
        .unwrap();

    let (req_tx, req_rx) = mpsc::channel(4);
    let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(add_collection_query_target_request(&root, "col"))
        .await
        .unwrap();

    let _initial = drain_until_no_change(&mut resp_stream).await;

    // Enqueue a pre-clear event into `db_live`'s broadcast channel, then immediately replace
    // `db_live` via `import_snapshot`.
    db_live
        .update_doc(
            &doc_ref,
            HashMap::from([("v".to_string(), Value::integer(20))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let replacement_fields = HashMap::from([("v".to_string(), Value::integer(30))]);
    replace_database_via_import(
        project,
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Standard),
        &[(&doc_ref, &replacement_fields)],
    )
    .await;

    let after_replace = drain_until_no_change(&mut resp_stream).await;
    assert_eq!(
        count_target_changes(&after_replace, TargetChangeType::Reset),
        1
    );
    assert!(after_replace.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                    == Some(&Value::integer(30))
        )
    }));

    drop(req_tx);
}

#[tokio::test]
async fn lagged_resubscribes_before_reset_and_converges_in_single_reset() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let db1 = project.database(&root).await;
    assert!(!db1.enterprise_edition());
    let col: CollectionRef = format!("{root}/col").parse().unwrap();
    let doc_ref: DocumentRef = format!("{root}/col/doc1").parse().unwrap();

    db1.create_doc(
        &col,
        "doc1",
        HashMap::from([("v".to_string(), Value::integer(1))]),
        None,
    )
    .await
    .unwrap();

    let (req_tx, req_rx) = mpsc::channel(4);
    let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(add_document_target_request(
            &root,
            doc_ref.to_string(),
            None,
        ))
        .await
        .unwrap();

    let _initial = drain_until_no_change(&mut resp_stream).await;

    // Overflow `db1`'s broadcast buffer (> MAX_EVENT_BACKLOG) AND replace `db1` before
    // `Listener::go` polls `database_events` again. Re-subscribing inside the `Lagged` arm
    // ensures `old_db`'s closed channel never emits a second `(db_name, None)` reset.
    for _ in 0..(crate::database::MAX_EVENT_BACKLOG + 32) {
        db1.send_event(DatabaseEvent {
            database:    Arc::downgrade(&db1),
            update_time: Timestamp::now(),
            updates:     HashMap::new(),
        });
    }
    let replacement_fields = HashMap::from([("v".to_string(), Value::integer(2))]);
    replace_database_via_import(
        project,
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Standard),
        &[(&doc_ref, &replacement_fields)],
    )
    .await;
    let db2 = project.database(&root).await;

    let recovery = drain_until_no_change(&mut resp_stream).await;
    assert_eq!(
        count_target_changes(&recovery, TargetChangeType::Reset),
        1,
        "Lagged recovery after replacement must emit exactly one Reset"
    );
    assert!(recovery.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v")) == Some(&Value::integer(2))
        )
    }));

    // Verify the listener's subscription now tracks `db2` (not the dropped `db1`).
    db2.update_doc(
        &doc_ref,
        HashMap::from([("v".to_string(), Value::integer(3))]),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let follow_up = drain_until_no_change(&mut resp_stream).await;
    assert!(follow_up.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v")) == Some(&Value::integer(3))
        )
    }));

    drop(req_tx);
}

#[tokio::test]
async fn toctou_clear_retries_during_set_document_set_query_and_reset() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let col: CollectionRef = format!("{root}/col").parse().unwrap();
    let doc_ref: DocumentRef = format!("{root}/col/doc1").parse().unwrap();

    // 1. set_document retries onto replacement database without sending duplicate Add.
    let db1 = project.database(&root).await;
    project.clear_database(&root).await;
    let db2 = project.database(&root).await;
    db2.create_doc(
        &col,
        "doc1",
        HashMap::from([("v".to_string(), Value::integer(42))]),
        None,
    )
    .await
    .unwrap();

    let (sender, rx) = mpsc::channel(16);
    let mut resp_stream = ReceiverStream::new(rx);
    let mut listener = Listener {
        id: 1001,
        project,
        database_events: Default::default(),
        sender,
        target: None,
    };
    listener
        .set_document(Arc::clone(&db1), doc_ref.clone(), None)
        .await
        .unwrap();
    let set_doc_msgs = drain_until_no_change(&mut resp_stream).await;
    assert_eq!(
        count_target_changes(&set_doc_msgs, TargetChangeType::Add),
        1
    );
    assert!(set_doc_msgs.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                    == Some(&Value::integer(42))
        )
    }));
    assert!(listener.database_events.contains_key(&root));

    // 2. reset on DocumentTarget retries onto replacement database.
    project.clear_database(&root).await;
    let db3 = project.database(&root).await;
    db3.create_doc(
        &col,
        "doc1",
        HashMap::from([("v".to_string(), Value::integer(77))]),
        None,
    )
    .await
    .unwrap();
    listener.reset(Arc::clone(&db2)).await.unwrap();
    let reset_doc_msgs = drain_until_no_change(&mut resp_stream).await;
    assert_eq!(
        count_target_changes(&reset_doc_msgs, TargetChangeType::Reset),
        1
    );
    assert!(reset_doc_msgs.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                    == Some(&Value::integer(77))
        )
    }));

    // 3. set_query and reset on QueryTarget retry onto replacement database.
    let query = QueryBuilder::from_collection(col.clone())
        .build()
        .unwrap()
        .into();
    project.clear_database(&root).await;
    let db4 = project.database(&root).await;
    db4.create_doc(
        &col,
        "doc1",
        HashMap::from([("v".to_string(), Value::integer(99))]),
        None,
    )
    .await
    .unwrap();
    let (q_sender, q_rx) = mpsc::channel(16);
    let mut q_stream = ReceiverStream::new(q_rx);
    let mut q_listener = Listener {
        id: 1002,
        project,
        database_events: Default::default(),
        sender: q_sender,
        target: None,
    };
    q_listener.set_query(Arc::clone(&db3), query).await.unwrap();
    let set_query_msgs = drain_until_no_change(&mut q_stream).await;
    assert_eq!(
        count_target_changes(&set_query_msgs, TargetChangeType::Add),
        1
    );
    assert_eq!(
        count_target_changes(&set_query_msgs, TargetChangeType::Reset),
        1
    );
    assert!(set_query_msgs.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                    == Some(&Value::integer(99))
        )
    }));

    // Reset QueryTarget across clear.
    project.clear_database(&root).await;
    let _db5 = project.database(&root).await;
    q_listener.reset(Arc::clone(&db4)).await.unwrap();
    let reset_query_msgs = drain_until_no_change(&mut q_stream).await;
    assert_eq!(
        count_target_changes(&reset_query_msgs, TargetChangeType::Reset),
        1
    );

    // Only `Aborted` on a cleared DB is retryable; non-Aborted errors (including
    // `FailedPrecondition`, `InvalidArgument`, and `Internal`) and `Aborted` on an active DB
    // must not be treated as cleared-retryable.
    db4.set_enterprise_edition(true);
    let err = q_listener.reset(Arc::clone(&db4)).await.unwrap_err();
    assert!(matches!(err, GenericDatabaseError::FailedPrecondition(_)));
    assert_eq!(MAX_RESET_RETRIES, 3);
    assert!(!is_cleared_retryable(&db4, &err));
    assert!(!is_cleared_retryable(
        &db4,
        &GenericDatabaseError::invalid_argument("bad cursor"),
    ));
    assert!(!is_cleared_retryable(
        &db4,
        &GenericDatabaseError::internal("unexpected"),
    ));
    assert!(is_cleared_retryable(
        &db4,
        &GenericDatabaseError::aborted("cleared"),
    ));
    let active_db = project.database(&root).await;
    assert!(!is_cleared_retryable(
        &active_db,
        &GenericDatabaseError::aborted("contended"),
    ));
}

#[tokio::test]
async fn single_reset_convergence_for_compatible_document_listener() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let db1 = project.database(&root).await;
    assert!(!db1.enterprise_edition());
    let col: CollectionRef = format!("{root}/col").parse().unwrap();
    let doc_ref: DocumentRef = format!("{root}/col/doc1").parse().unwrap();

    db1.create_doc(
        &col,
        "doc1",
        HashMap::from([("v".to_string(), Value::integer(1))]),
        None,
    )
    .await
    .unwrap();

    let (req_tx, req_rx) = mpsc::channel(4);
    let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(add_document_target_request(
            &root,
            doc_ref.to_string(),
            None,
        ))
        .await
        .unwrap();

    let _initial = drain_until_no_change(&mut resp_stream).await;

    let replacement_fields = HashMap::from([("v".to_string(), Value::integer(2))]);
    replace_database_via_import(
        project,
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Standard),
        &[(&doc_ref, &replacement_fields)],
    )
    .await;

    let msgs = drain_until_no_change(&mut resp_stream).await;
    assert_eq!(count_target_changes(&msgs, TargetChangeType::Reset), 1);
    assert!(
        !msgs
            .iter()
            .any(|rt| matches!(rt, ResponseType::DocumentDelete(_))),
        "single-reset convergence must not observe an intermediate empty database"
    );
    assert!(msgs.iter().any(|rt| {
        matches!(
            rt,
            ResponseType::DocumentChange(dc)
                if dc.document.as_ref().and_then(|d| d.fields.get("v")) == Some(&Value::integer(2))
        )
    }));

    drop(req_tx);
}

#[tokio::test]
async fn query_listener_terminates_with_failed_precondition_on_edition_change() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
    let db1 = project.database(&root).await;
    assert!(!db1.enterprise_edition());
    let col: CollectionRef = format!("{root}/col").parse().unwrap();

    let (req_tx, req_rx) = mpsc::channel(4);
    let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
    req_tx
        .send(add_collection_query_target_request(&root, "col"))
        .await
        .unwrap();

    let _initial = drain_until_no_change(&mut resp_stream).await;

    // Replace `db1` (STANDARD) with `db2` configured as ENTERPRISE via `import_snapshot`.
    replace_database_via_import(
        project,
        &root,
        DatabaseSettings::for_edition(DatabaseEdition::Enterprise),
        &[],
    )
    .await;
    let db2 = project.database(&root).await;

    let err = resp_stream
        .next()
        .await
        .expect("stream must emit FailedPrecondition")
        .unwrap_err();
    assert_eq!(
        err,
        GenericDatabaseError::failed_precondition(format!(
            "Database {root} edition changed from STANDARD to ENTERPRISE; re-create query listener"
        ))
    );
    assert!(
        resp_stream.next().await.is_none(),
        "listener stream must terminate immediately after FailedPrecondition"
    );

    // Also verify `QueryTarget::process_event` checks edition before the stale-timestamp
    // early return, including ENTERPRISE -> STANDARD direction.
    let mut ent_target = QueryTarget::new(
        QueryBuilder::from_collection(col)
            .enterprise_edition(true)
            .build()
            .unwrap()
            .into(),
    );
    let (reset_time, _) = ent_target.reset(&db2).await.unwrap();
    db2.set_enterprise_edition(false);
    let stale_event = DatabaseEvent {
        database:    Arc::downgrade(&db2),
        update_time: reset_time,
        updates:     Default::default(),
    };
    let err = ent_target
        .process_event(&db2, &stale_event)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        GenericDatabaseError::failed_precondition(format!(
            "Database {root} edition changed from ENTERPRISE to STANDARD; re-create query listener"
        ))
    );

    drop(req_tx);
}

#[tokio::test]
async fn clearing_edition_overridden_database_terminates_query_listener_without_ghost_db() {
    for (default_enterprise, overridden_enterprise, expected_msg) in [
        (
            false,
            true,
            "edition changed from ENTERPRISE to STANDARD; re-create query listener",
        ),
        (
            true,
            false,
            "edition changed from STANDARD to ENTERPRISE; re-create query listener",
        ),
    ] {
        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
            default_enterprise,
            ..Default::default()
        })));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        db.set_enterprise_edition(overridden_enterprise);

        let (req_tx, req_rx) = mpsc::channel(4);
        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(add_collection_query_target_request(&root, "col"))
            .await
            .unwrap();

        let _initial = drain_until_no_change(&mut resp_stream).await;

        // Subscribe and drain any setup project events right before clearing.
        let mut proj_rx = project.subscribe_project_events();
        project.clear_database(&root).await;

        let err = resp_stream
            .next()
            .await
            .expect("stream must emit FailedPrecondition")
            .unwrap_err();
        assert_eq!(
            err,
            GenericDatabaseError::failed_precondition(format!("Database {root} {expected_msg}"))
        );
        assert!(
            resp_stream.next().await.is_none(),
            "listener stream must terminate immediately after FailedPrecondition"
        );

        assert!(
            project.get_existing_database(&root).await.is_none(),
            "clearing edition-overridden database must not lazily re-create a ghost database"
        );

        assert!(matches!(
            proj_rx.try_recv().unwrap(),
            ProjectEvent::DatabaseCleared { ref database } if database == &root
        ));
        assert!(
            proj_rx.try_recv().is_err(),
            "no DatabaseCreated or other project events may follow DatabaseCleared"
        );

        drop(req_tx);
    }
}
