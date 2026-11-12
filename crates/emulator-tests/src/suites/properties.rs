use std::{cell::RefCell, collections::BTreeMap};

use googleapis::google::{firestore::v1::Document, protobuf::Timestamp};
use http::{Method, StatusCode};
use proptest::prelude::*;

use crate::harness::{
    builders::{PipelineSpec, QuerySpec, field_ref_val, fn_val},
    env::DATABASE_ROOT,
    matchers::doc_names,
    proptest_support::{
        AsyncProptestEnv, GeneratedQueryKind, arb_doc_fields, arb_doc_id, arb_query_kind,
        proptest_config,
    },
};

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    fn proptest_pagination_split_equivalence(
        entries in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=16),
        page_size in 1i32..=6i32,
        descending in any::<bool>(),
    ) {
        thread_local! {
            static ENV: RefCell<AsyncProptestEnv> = RefCell::new(AsyncProptestEnv::new(false));
        }
        ENV.with(|cell| {
            cell.borrow_mut().run_case(|mut db| async move {
                for (id, fields) in entries {
                    db.put(&format!("p/{id}"), fields).await.unwrap();
                }

                let order_by = if descending { "num desc" } else { "num asc" };
                let full = db
                    .list_docs("p", 0, "", order_by, None, false, None)
                    .await
                    .unwrap();

                let mut paged_docs = Vec::new();
                let mut token = String::new();
                loop {
                    let page = db
                        .list_docs("p", page_size, &token, order_by, None, false, None)
                        .await
                        .unwrap();
                    paged_docs.extend(page.documents);
                    if page.next_page_token.is_empty() {
                        break;
                    }
                    token = page.next_page_token;
                }

                prop_assert_eq!(doc_names(&paged_docs), doc_names(&full.documents));
                Ok(())
            })
        })?;
    }

    #[test]
    fn proptest_mvcc_read_time_snapshot_immutability(
        phase1 in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 1..=12),
        phase2_puts in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=10),
        phase2_deletes in prop::collection::vec(arb_doc_id(), 0..=8),
    ) {
        thread_local! {
            static ENV: RefCell<AsyncProptestEnv> = RefCell::new(AsyncProptestEnv::new(false));
        }
        ENV.with(|cell| {
            cell.borrow_mut().run_case(|mut db| async move {
                let mut snap_time = Timestamp::now();
                for (id, fields) in &phase1 {
                    let doc = db.put(&format!("m/{id}"), fields.clone()).await.unwrap();
                    snap_time = doc.update_time.unwrap();
                }

                let baseline = db.query_at_time(QuerySpec::collection("m"), snap_time).await.unwrap();

                for (id, fields) in phase2_puts {
                    db.put(&format!("m/{id}"), fields).await.unwrap();
                }
                for id in phase2_deletes {
                    db.delete(&format!("m/{id}"), None).await.unwrap();
                }

                let after_mutations = db.query_at_time(QuerySpec::collection("m"), snap_time).await.unwrap();
                prop_assert_eq!(after_mutations.documents, baseline.documents);
                Ok(())
            })
        })?;
    }

    #[test]
    fn proptest_grpc_write_http_rest_read_parity(
        id in arb_doc_id(),
        fields in arb_doc_fields(),
    ) {
        thread_local! {
            static ENV: RefCell<AsyncProptestEnv> = RefCell::new(AsyncProptestEnv::new(false));
        }
        ENV.with(|cell| {
            cell.borrow_mut().run_case(|mut db| async move {
                let written = db.put(&format!("parity/{id}"), fields).await.unwrap();
                let uri = format!("/v1/{DATABASE_ROOT}/documents/parity/{id}");
                let (status, bytes) = db.http_oneshot(Method::GET, &uri, None).await;
                prop_assert_eq!(status, StatusCode::OK);

                let http_doc: Document = serde_json::from_slice(&bytes)
                    .expect("HTTP REST JSON deserializes as google.firestore.v1.Document");
                prop_assert_eq!(&http_doc.name, &written.name);
                prop_assert_eq!(&http_doc.create_time, &written.create_time);
                prop_assert_eq!(&http_doc.update_time, &written.update_time);
                prop_assert_eq!(&http_doc.fields, &written.fields);
                Ok(())
            })
        })?;
    }

    #[test]
    fn proptest_small_collection_scan_query_and_pipeline_equivalence(
        entries in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=16),
        kind in arb_query_kind(),
    ) {
        thread_local! {
            static ENV: RefCell<AsyncProptestEnv> = RefCell::new(AsyncProptestEnv::new(true));
        }
        ENV.with(|cell| {
            cell.borrow_mut().run_case(|mut db| async move {
                let stored: BTreeMap<&str, _> = entries;
                for (id, fields) in &stored {
                    db.put(&format!("eq/{id}"), fields.clone()).await.unwrap();
                }

                // 1. StructuredQuery execution (with `__name__` tie-breaker for total order)
                let query_spec = match kind {
                    GeneratedQueryKind::NumGreaterOrEqual(_)
                    | GeneratedQueryKind::TagEquals(_)
                    | GeneratedQueryKind::FlagEquals(_) => {
                        kind.to_query_spec("eq").order_asc("__name__")
                    }
                    GeneratedQueryKind::NumLessThan(_) => {
                        kind.to_query_spec("eq").order_desc("__name__")
                    }
                };
                let query_docs = db.query(query_spec).await.unwrap().documents;

                // 2. Equivalent Enterprise Pipeline execution (`collection` -> `where` -> `sort`)
                let pipe_spec = match kind {
                    GeneratedQueryKind::NumGreaterOrEqual(t) => PipelineSpec::collection("eq")
                        .where_expr(fn_val(
                            "greater_than_or_equal",
                            vec![field_ref_val("num"), t.into()],
                        ))
                        .sort_asc("__name__")
                        .sort_asc("num"), // Note: pipeline `sort` takes multiple criteria in a single stage or chained stable sorts
                    GeneratedQueryKind::NumLessThan(t) => PipelineSpec::collection("eq")
                        .where_expr(fn_val(
                            "less_than",
                            vec![field_ref_val("num"), t.into()],
                        ))
                        .sort_desc("__name__")
                        .sort_desc("num"),
                    GeneratedQueryKind::TagEquals(tag) => PipelineSpec::collection("eq")
                        .where_expr(fn_val("equal", vec![field_ref_val("tag"), tag.into()]))
                        .sort_asc("__name__")
                        .sort_asc("num"),
                    GeneratedQueryKind::FlagEquals(flag) => PipelineSpec::collection("eq")
                        .where_expr(fn_val("equal", vec![field_ref_val("flag"), flag.into()]))
                        .sort_asc("__name__")
                        .sort_asc("num"),
                };
                let pipe_docs = db.pipeline(pipe_spec).await.unwrap().results;

                // 3. Independent full-scan reference evaluation in Rust
                let scan_all = db.list_docs("eq", 0, "", "", None, false, None).await.unwrap();
                let mut expected: Vec<Document> = scan_all
                    .documents
                    .into_iter()
                    .filter(|d| match kind {
                        GeneratedQueryKind::NumGreaterOrEqual(t) => {
                            d.fields["num"].as_integer().unwrap() >= t
                        }
                        GeneratedQueryKind::NumLessThan(t) => {
                            d.fields["num"].as_integer().unwrap() < t
                        }
                        GeneratedQueryKind::TagEquals(tag) => d.fields["tag"].as_string() == Some(tag),
                        GeneratedQueryKind::FlagEquals(flag) => {
                            d.fields["flag"].as_boolean() == Some(flag)
                        }
                    })
                    .collect();

                expected.sort_by(|a, b| {
                    let na = a.fields["num"].as_integer().unwrap();
                    let nb = b.fields["num"].as_integer().unwrap();
                    match kind {
                        GeneratedQueryKind::NumLessThan(_) => {
                            nb.cmp(&na).then_with(|| b.name.cmp(&a.name))
                        }
                        _ => na.cmp(&nb).then_with(|| a.name.cmp(&b.name)),
                    }
                });

                prop_assert_eq!(doc_names(&query_docs), doc_names(&expected));
                prop_assert_eq!(doc_names(&pipe_docs), doc_names(&expected));
                Ok(())
            })
        })?;
    }
}
