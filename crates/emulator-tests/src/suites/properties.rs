use std::{cmp::Ordering, collections::BTreeMap};

use googleapis::{
    cmp_reference_paths,
    google::{
        firestore::v1::{Document, Value},
        protobuf::Timestamp,
    },
};
use http::{Method, StatusCode};
use proptest::prelude::*;

use crate::harness::{
    builders::{QuerySpec, doc_path},
    env::DATABASE_ROOT,
    matchers::doc_names,
    proptest_support::{
        GeneratedQueryKind, arb_doc_fields, arb_doc_id, arb_query_kind, proptest_config,
        run_enterprise_proptest, run_standard_proptest,
    },
};

fn cursor_on_left(doc: &Document, cursor_vals: &[Value], before: bool, descending: bool) -> bool {
    let Some(num_val) = doc.fields.get("num") else {
        return false;
    };
    let doc_key = [num_val.clone(), Value::reference(doc.name.clone())];
    for (dv, cv) in doc_key.iter().zip(cursor_vals) {
        let ord = if descending {
            dv.cmp(cv).reverse()
        } else {
            dv.cmp(cv)
        };
        if ord != Ordering::Equal {
            return ord == Ordering::Less;
        }
    }
    !before
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    fn proptest_pagination_split_equivalence(
        entries in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=16),
        page_size in 1i32..=6i32,
        descending in any::<bool>(),
    ) {
        run_standard_proptest(|mut db| async move {
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
        })?;
    }

    #[test]
    fn proptest_mvcc_read_time_snapshot_immutability(
        phase1 in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 1..=12),
        phase2_puts in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=10),
        phase2_deletes in prop::collection::vec(arb_doc_id(), 0..=8),
    ) {
        run_standard_proptest(|mut db| async move {
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
            prop_assert_eq!(&after_mutations.documents, &baseline.documents);
            for (after_doc, base_doc) in after_mutations.documents.iter().zip(&baseline.documents) {
                prop_assert!(
                    Value::fields_storage_eq(&after_doc.fields, &base_doc.fields),
                    "MVCC storage mismatch on {}:\n  after: {:?}\n  base:  {:?}",
                    after_doc.name,
                    after_doc.fields,
                    base_doc.fields
                );
            }
            Ok(())
        })?;
    }

    #[test]
    fn proptest_grpc_write_http_rest_read_parity(
        id in arb_doc_id(),
        fields in arb_doc_fields(),
    ) {
        run_standard_proptest(|mut db| async move {
            let written = db.put(&format!("parity/{id}"), fields).await.unwrap();
            let uri = format!("/v1/{DATABASE_ROOT}/documents/parity/{id}");
            let (status, bytes) = db.http_oneshot(Method::GET, &uri, None).await;
            prop_assert_eq!(status, StatusCode::OK);

            let http_doc: Document = serde_json::from_slice(&bytes)
                .expect("HTTP REST JSON deserializes as google.firestore.v1.Document");
            prop_assert_eq!(&http_doc.name, &written.name);
            prop_assert_eq!(&http_doc.create_time, &written.create_time);
            prop_assert_eq!(&http_doc.update_time, &written.update_time);
            prop_assert!(
                Value::fields_storage_eq(&http_doc.fields, &written.fields),
                "wire-exact field mismatch between gRPC write and HTTP REST read:\n  http: {:?}\n  grpc: {:?}",
                http_doc.fields,
                written.fields
            );
            Ok(())
        })?;
    }

    #[test]
    fn proptest_small_collection_scan_query_and_pipeline_equivalence(
        entries in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=16),
        kind in arb_query_kind(),
        limit in prop::option::of(0usize..=10),
        offset in 0usize..=6,
    ) {
        let ent_query_docs = run_enterprise_proptest(|mut db| {
            let stored: BTreeMap<&str, _> = entries.clone();
            async move {
                for (id, fields) in &stored {
                    db.put(&format!("eq/{id}"), fields.clone()).await.unwrap();
                }

                // 1. StructuredQuery execution (Enterprise)
                let query_docs = db
                    .query(kind.to_query_spec("eq", limit, offset))
                    .await
                    .unwrap()
                    .documents;

                // 2. Equivalent Enterprise Pipeline execution (`collection` -> `where` -> `sort` -> `offset` -> `limit`)
                let pipe_docs = db
                    .pipeline(kind.to_pipeline_spec("eq", limit, offset))
                    .await
                    .unwrap()
                    .results;

                // 3. Independent full-scan reference evaluation in Rust
                let scan_all = db.list_docs("eq", 0, "", "", None, false, None).await.unwrap();
                let mut expected: Vec<Document> = scan_all
                    .documents
                    .into_iter()
                    .filter(|d| match kind {
                        GeneratedQueryKind::NumGreaterOrEqual(t) => d
                            .fields
                            .get("num")
                            .and_then(Value::as_integer)
                            .is_some_and(|n| n >= t),
                        GeneratedQueryKind::NumLessThan(t) => d
                            .fields
                            .get("num")
                            .and_then(Value::as_integer)
                            .is_some_and(|n| n < t),
                        GeneratedQueryKind::TagEquals(tag) => {
                            d.fields.get("num").and_then(Value::as_integer).is_some()
                                && d.fields.get("tag").and_then(Value::as_string) == Some(tag)
                        }
                        GeneratedQueryKind::TagNotEquals(tag) => d
                            .fields
                            .get("tag")
                            .and_then(Value::as_string)
                            .is_some_and(|t| t != tag),
                        GeneratedQueryKind::TagIn(first, second) => d
                            .fields
                            .get("tag")
                            .and_then(Value::as_string)
                            .is_some_and(|t| t == first || second == Some(t)),
                        GeneratedQueryKind::FlagEquals(flag) => {
                            d.fields.get("num").and_then(Value::as_integer).is_some()
                                && d.fields.get("flag").and_then(Value::as_boolean) == Some(flag)
                        }
                        GeneratedQueryKind::AlphaIsNull => {
                            d.fields.get("alpha").is_some_and(Value::is_null)
                        }
                        GeneratedQueryKind::AlphaIsNan => {
                            d.fields.get("alpha").is_some_and(Value::is_nan)
                        }
                    })
                    .collect();

                expected.sort_by(|a, b| match kind {
                    GeneratedQueryKind::NumGreaterOrEqual(_)
                    | GeneratedQueryKind::TagEquals(_)
                    | GeneratedQueryKind::FlagEquals(_) => {
                        let na = a.fields.get("num").and_then(Value::as_integer);
                        let nb = b.fields.get("num").and_then(Value::as_integer);
                        na.cmp(&nb).then_with(|| cmp_reference_paths(&a.name, &b.name))
                    }
                    GeneratedQueryKind::NumLessThan(_) => {
                        let na = a.fields.get("num").and_then(Value::as_integer);
                        let nb = b.fields.get("num").and_then(Value::as_integer);
                        nb.cmp(&na).then_with(|| cmp_reference_paths(&b.name, &a.name))
                    }
                    GeneratedQueryKind::TagNotEquals(_) => {
                        let ta = a.fields.get("tag").and_then(Value::as_string);
                        let tb = b.fields.get("tag").and_then(Value::as_string);
                        ta.cmp(&tb).then_with(|| cmp_reference_paths(&a.name, &b.name))
                    }
                    GeneratedQueryKind::TagIn(..)
                    | GeneratedQueryKind::AlphaIsNull
                    | GeneratedQueryKind::AlphaIsNan => cmp_reference_paths(&a.name, &b.name),
                });

                let expected: Vec<Document> = expected
                    .into_iter()
                    .skip(offset)
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();

                prop_assert_eq!(doc_names(&query_docs), doc_names(&expected));
                prop_assert_eq!(doc_names(&pipe_docs), doc_names(&expected));
                Ok(query_docs)
            }
        })?;

        // 4. Standard Edition StructuredQuery must match Enterprise StructuredQuery & Reference.
        run_standard_proptest(|mut db| async move {
            for (id, fields) in entries {
                db.put(&format!("eq/{id}"), fields).await.unwrap();
            }
            let std_query_docs = db
                .query(kind.to_query_spec("eq", limit, offset))
                .await
                .unwrap()
                .documents;
            prop_assert_eq!(doc_names(&std_query_docs), doc_names(&ent_query_docs));
            for (std_doc, ent_doc) in std_query_docs.iter().zip(&ent_query_docs) {
                prop_assert!(Value::fields_storage_eq(&std_doc.fields, &ent_doc.fields));
            }
            Ok(())
        })?;
    }

    #[test]
    fn proptest_cursor_slicing_equivalence(
        entries in prop::collection::btree_map(arb_doc_id(), arb_doc_fields(), 0..=16),
        descending in any::<bool>(),
        start_bound in prop::option::of((-55i64..=55i64, prop::option::of(arb_doc_id()), any::<bool>())),
        end_bound in prop::option::of((-55i64..=55i64, prop::option::of(arb_doc_id()), any::<bool>())),
    ) {
        let to_cursor = |(n, id, before): (i64, Option<&'static str>, bool)| {
            let vals = match id {
                Some(id) => vec![Value::from(n), Value::reference(doc_path(&format!("cur/{id}")))],
                None => vec![Value::from(n)],
            };
            (vals, before)
        };
        let start = start_bound.map(to_cursor);
        let end = end_bound.map(to_cursor);
        run_standard_proptest(|mut db| async move {
            for (id, fields) in entries {
                db.put(&format!("cur/{id}"), fields).await.unwrap();
            }
            let ordered = if descending {
                QuerySpec::collection("cur").order_desc("num").order_desc("__name__")
            } else {
                QuerySpec::collection("cur").order_asc("num").order_asc("__name__")
            };
            let full = db.query(ordered.clone()).await.unwrap().documents;
            let mut sliced_spec = ordered;
            if let Some((ref vals, before)) = start {
                sliced_spec = sliced_spec.start_at(vals.clone(), before);
            }
            if let Some((ref vals, before)) = end {
                sliced_spec = sliced_spec.end_at(vals.clone(), before);
            }
            let sliced = db.query(sliced_spec).await.unwrap().documents;
            let expected: Vec<String> = full
                .iter()
                .filter(|d| {
                    start.as_ref().is_none_or(|(v, b)| !cursor_on_left(d, v, *b, descending))
                        && end.as_ref().is_none_or(|(v, b)| cursor_on_left(d, v, *b, descending))
                })
                .map(|d| d.name.clone())
                .collect();
            prop_assert_eq!(doc_names(&sliced), expected);
            Ok(())
        })?;
    }
}

#[test]
fn storage_eq_rejects_semantic_only_numeric_representation_drift() {
    use googleapis::fields;

    let int_fields = fields!({ "n": 1i64, "z": 0.0f64 });
    let float_fields = fields!({ "n": 1.0f64, "z": -0.0f64 });

    assert_eq!(int_fields, float_fields);
    assert!(!Value::fields_storage_eq(&int_fields, &float_fields));
}
