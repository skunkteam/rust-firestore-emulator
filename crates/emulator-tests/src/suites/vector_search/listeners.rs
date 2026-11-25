use std::{collections::HashMap, time::Duration};

use googleapis::{
    fields,
    google::firestore::v1::{
        ListenRequest, Target, listen_request,
        structured_query::{field_filter::Operator, find_nearest},
        target::{self, query_target},
    },
};
use googletest::prelude::*;
use tonic::Code;

use super::make_find_nearest;
use crate::harness::{
    builders::{QuerySpec, doc_path, field_filter, vec_val},
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
    matchers::short_ids,
};

#[gtest]
#[tokio::test]
async fn listen_find_nearest_updates_threshold_suppression_and_dynamic_nan_termination() {
    // (a) Initial catch-up evaluates `findNearest` (threshold/limit/distance injection), while
    //     subsequent writes stream incrementally as base-query updates matching Cloud Firestore.
    let mut db = TestEnv::new().build_db();
    db.put(
        "live/d1",
        fields!({ "emb": vec_val([1.0, 0.5]), "label": "one" }),
    )
    .await
    .unwrap();
    db.put(
        "live/d_init_far",
        fields!({ "emb": vec_val([0.0, 1.0]), "label": "init_far" }),
    )
    .await
    .unwrap();

    let (mut handle, init) = db
        .listen_query(
            QuerySpec::collection("live").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Cosine,
                2,
                Some("dist"),
                Some(0.2),
            )),
        )
        .await
        .unwrap();
    expect_eq!(init.was_reset, true);
    expect_eq!(short_ids(&init.changed_docs), vec!["d1"]);
    assert!(init.changed_docs[0].fields["dist"].as_double().is_some());

    for (id, vec, label) in [
        ("d_far", [0.0, 1.0], "far"),
        ("d2", [1.0, 0.0], "two"),
        ("d_zero", [0.0, 0.0], "zero"),
    ] {
        db.put(
            &format!("live/{id}"),
            fields!({ "emb": vec_val(vec), "label": label }),
        )
        .await
        .unwrap();
        let batch = handle.next_batch().await.unwrap();
        expect_eq!(batch.was_reset, false);
        expect_eq!(short_ids(&batch.changed_docs), vec![id]);
        assert!(!batch.changed_docs[0].fields.contains_key("dist"));
    }

    // Updating an already-tracked document emits an incremental delta without `dist`.
    db.put(
        "live/d1",
        fields!({ "emb": vec_val([1.0, 0.5]), "label": "one-updated" }),
    )
    .await
    .unwrap();
    let delta = handle.next_batch().await.unwrap();
    expect_eq!(delta.was_reset, false);
    expect_eq!(short_ids(&delta.changed_docs), vec!["d1"]);
    assert!(!delta.changed_docs[0].fields.contains_key("dist"));

    // Deleting a tracked document emits an incremental DocumentDelete delta.
    db.delete("live/d2", None).await.unwrap();
    let del_batch = handle.next_batch().await.unwrap();
    expect_eq!(del_batch.was_reset, false);
    expect_eq!(del_batch.deleted_docs, vec![doc_path("live/d2")]);
    handle.close().await;

    // Prefilter exit on a findNearest listener emits DocumentRemove without a collection Reset.
    let (mut pf_handle, pf_init) = db
        .listen_query(
            QuerySpec::collection("live_pf")
                .where_filter(field_filter("cat", Operator::Equal, "keep"))
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    2,
                    Some("dist"),
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(pf_init.was_reset, true);
    assert!(pf_init.changed_docs.is_empty());

    db.put(
        "live_pf/p1",
        fields!({ "cat": "keep", "emb": vec_val([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    let pf_add = pf_handle.next_batch().await.unwrap();
    expect_eq!(pf_add.was_reset, false);
    expect_eq!(short_ids(&pf_add.changed_docs), vec!["p1"]);

    db.put(
        "live_pf/p1",
        fields!({ "cat": "skip", "emb": vec_val([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    let pf_rm = pf_handle.next_batch().await.unwrap();
    expect_eq!(pf_rm.was_reset, false);
    expect_eq!(pf_rm.removed_docs, vec![doc_path("live_pf/p1")]);
    pf_handle.close().await;

    // BroadcastStreamRecvError::Lagged resets the findNearest target with k-NN filtering + dist
    // projection intact rather than falling back to an unfiltered collection scan.
    db.put("lag/d1", fields!({ "emb": vec_val([1.0, 0.5]) }))
        .await
        .unwrap();
    db.put("lag/d_init_far", fields!({ "emb": vec_val([0.0, 1.0]) }))
        .await
        .unwrap();
    let (_, lag_sq) = QuerySpec::collection("lag")
        .find_nearest(make_find_nearest(
            "emb",
            vec_val([1.0, 0.0]),
            find_nearest::DistanceMeasure::Cosine,
            2,
            Some("dist"),
            Some(0.2),
        ))
        .into_parts();
    let (lag_tx, lag_rx) = tokio::sync::mpsc::channel(16);
    let mut raw_lag_stream = db
        .raw_grpc()
        .listen(tokio_stream::wrappers::ReceiverStream::new(lag_rx))
        .await
        .unwrap()
        .into_inner();
    lag_tx
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent:     DOCUMENTS_ROOT.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(lag_sq)),
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await
        .unwrap();
    while let Some(frame) = raw_lag_stream.message().await.unwrap() {
        if let Some(googleapis::google::firestore::v1::listen_response::ResponseType::TargetChange(
            tc,
        )) = frame.response_type
            && tc.target_change_type
                == googleapis::google::firestore::v1::target_change::TargetChangeType::NoChange
                    as i32
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    let pad = "x".repeat(2048);
    for i in 0..1150 {
        let fields = if i < 100 {
            fields!({ "emb": vec_val([0.0, 1.0]), "pad": pad.as_str() })
        } else {
            fields!({ "emb": vec_val([0.0, 1.0]) })
        };
        db.put(&format!("lag/far_{i:04}"), fields).await.unwrap();
    }
    db.put("lag/d_close", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();

    let mut saw_reset = false;
    let mut reset_docs = Vec::new();
    while let Some(frame) = raw_lag_stream.message().await.unwrap() {
        match frame.response_type {
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::TargetChange(tc),
            ) => {
                let kind =
                    googleapis::google::firestore::v1::target_change::TargetChangeType::try_from(
                        tc.target_change_type,
                    );
                if kind
                    == Ok(
                        googleapis::google::firestore::v1::target_change::TargetChangeType::Reset,
                    )
                {
                    saw_reset = true;
                    reset_docs.clear();
                } else if saw_reset
                    && kind
                        == Ok(
                            googleapis::google::firestore::v1::target_change::TargetChangeType::NoChange,
                        )
                    && tc.target_ids.is_empty()
                {
                    break;
                }
            }
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::DocumentChange(
                    dc,
                ),
            ) if saw_reset => {
                if let Some(doc) = dc.document {
                    reset_docs.push(doc);
                }
            }
            _ => {}
        }
    }
    assert!(
        saw_reset,
        "expected Lagged listener to emit TargetChange::Reset"
    );
    expect_eq!(short_ids(&reset_docs), vec!["d_close", "d1"]);
    for doc in &reset_docs {
        assert!(doc.fields.contains_key("dist"));
    }

    // Confirm no stale pre-reset `far_*` DocumentChange frames leak after post-lag Reset, and a
    // follow-up write delivers only `lag/after_reset` without any spurious `far_*` events.
    db.put("lag/after_reset", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    let mut followup_docs = Vec::new();
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(2), raw_lag_stream.message())
        .await
        .expect("timed out waiting for post-lag follow-up listen batch")
        .unwrap()
    {
        match frame.response_type {
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::TargetChange(tc),
            ) => {
                let kind =
                    googleapis::google::firestore::v1::target_change::TargetChangeType::try_from(
                        tc.target_change_type,
                    );
                if kind == Ok(
                    googleapis::google::firestore::v1::target_change::TargetChangeType::NoChange,
                ) && tc.target_ids.is_empty()
                {
                    break;
                }
            }
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::DocumentChange(
                    dc,
                ),
            ) => {
                if let Some(doc) = dc.document {
                    assert!(
                        !doc.name.contains("/lag/far_"),
                        "stale far_* document leaked after lag recovery: {}",
                        doc.name
                    );
                    followup_docs.push(doc);
                }
            }
            _ => {}
        }
    }
    expect_eq!(short_ids(&followup_docs), vec!["after_reset"]);
    drop(lag_tx);

    // (b) Opening a Listen query on a collection that already fails COSINE `[0, 0]` delivers the
    //     edition-specific error via `send_err` and terminates the stream cleanly.
    db.put("err_listen/zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();
    let (_, sq) = QuerySpec::collection("err_listen")
        .find_nearest(make_find_nearest(
            "emb",
            vec_val([1.0, 0.0]),
            find_nearest::DistanceMeasure::Cosine,
            2,
            None,
            None,
        ))
        .into_parts();
    let mut err_handle = db.open_raw_listen().await.unwrap();
    err_handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent:     DOCUMENTS_ROOT.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(sq.clone())),
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await;
    let err = err_handle
        .next_batch()
        .await
        .expect_err("initial COSINE [0, 0] must surface on Standard Listen stream");
    expect_eq!(err.code(), Code::FailedPrecondition);

    db.put("err_listen/after", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    expect_that!(err_handle.next_raw().await, none());
    err_handle.close().await;

    // Enterprise Edition Listen delivers InvalidArgument on initial COSINE `[0, 0]`.
    let mut ent_db = TestEnv::enterprise().build_db();
    ent_db
        .put("err_listen/zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();
    let mut ent_err_handle = ent_db.open_raw_listen().await.unwrap();
    ent_err_handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent:     DOCUMENTS_ROOT.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(sq)),
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await;
    let ent_err = ent_err_handle
        .next_batch()
        .await
        .expect_err("initial COSINE [0, 0] must surface on Enterprise Listen stream");
    expect_eq!(ent_err.code(), Code::InvalidArgument);
    ent_err_handle.close().await;
}
