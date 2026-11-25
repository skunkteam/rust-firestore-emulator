use googleapis::{
    fields,
    google::{
        firestore::v1::{
            ListCollectionIdsRequest, ListDocumentsRequest, ListDocumentsResponse, Value,
            list_documents_request::ConsistencySelector,
        },
        protobuf::Timestamp,
        r#type::LatLng,
    },
};
use googletest::prelude::*;
use http::{Method, StatusCode};
use tonic::Code;

use crate::harness::{
    builders::{doc_mask, doc_path},
    contention::ContentionProbe,
    env::{DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_status_code, doc_names, short_ids},
};

#[gtest]
#[tokio::test]
async fn list_documents_pagination_order_by_mask_and_phantom_show_missing() {
    let mut db = TestEnv::new().build_db();

    db.put("users/charlie", fields!({ "rank": 2i64, "secret": "c" }))
        .await
        .unwrap();
    db.put("users/alice", fields!({ "rank": 3i64, "secret": "a" }))
        .await
        .unwrap();
    let bob = db
        .put("users/bob", fields!({ "rank": 1i64, "secret": "b" }))
        .await
        .unwrap();
    let t_before_phantom = bob.update_time.unwrap();

    // Create subcollection doc under phantom parent `users/diana` (which has no stored doc itself).
    db.put("users/diana/notes/n1", fields!({ "text": "phantom child" }))
        .await
        .unwrap();

    // 1. Default listing omits phantom `diana` and orders by `__name__ ASC`.
    let default_list = db
        .list_docs("users", 0, "", "", None, false, None)
        .await
        .expect("ListDocuments default");
    expect_eq!(
        short_ids(&default_list.documents),
        vec!["alice", "bob", "charlie"]
    );
    expect_eq!(default_list.next_page_token, "");

    // 2. Paginate with `page_size = 2` and `mask = ["rank"]`.
    let page1 = db
        .list_docs(
            "users",
            2,
            "",
            "rank desc",
            Some(doc_mask(["rank"])),
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&page1.documents), vec!["alice", "charlie"]);
    assert_doc_wire_eq(
        &page1.documents[0],
        "users/alice",
        &fields!({ "rank": 3i64 }),
    );
    assert!(!page1.next_page_token.is_empty());

    let page2 = db
        .list_docs(
            "users",
            2,
            &page1.next_page_token,
            "rank desc",
            Some(doc_mask(["rank"])),
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&page2.documents), vec!["bob"]);
    expect_eq!(page2.next_page_token, "");

    // 3. `show_missing = true` surfaces phantom `users/diana` with empty fields & None timestamps.
    let with_missing = db
        .list_docs("users", 0, "", "", None, true, None)
        .await
        .expect("ListDocuments show_missing = true");
    expect_eq!(
        short_ids(&with_missing.documents),
        vec!["alice", "bob", "charlie", "diana"]
    );
    let phantom = &with_missing.documents[3];
    expect_eq!(phantom.name, doc_path("users/diana"));
    expect_that!(phantom.fields.is_empty(), eq(true));
    expect_that!(phantom.create_time, none());
    expect_that!(phantom.update_time, none());

    // 4. Historical ReadTime consistency works with ListDocuments.
    db.put("users/zoe", fields!({ "rank": 0i64 }))
        .await
        .unwrap();
    let historical = db
        .list_docs(
            "users",
            0,
            "",
            "",
            None,
            false,
            Some(ConsistencySelector::ReadTime(t_before_phantom)),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&historical.documents),
        vec!["alice", "bob", "charlie"]
    );

    // 5. Validation errors: negative page_size, show_missing + order_by, empty order_by clause.
    assert_status_code(
        db.list_docs("users", -1, "", "", None, false, None).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs("users", 0, "", "rank asc", None, true, None)
            .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs("users", 0, "", "rank asc, ", None, false, None)
            .await,
        Code::InvalidArgument,
    );
}

#[gtest]
#[tokio::test]
async fn list_collection_ids_lists_root_and_subcollections_and_rejects_unsupported_options() {
    let mut db = TestEnv::new().build_db();

    db.put("alpha/a1", fields!({ "x": 1i64 })).await.unwrap();
    let beta_doc = db.put("beta/b1", fields!({ "x": 2i64 })).await.unwrap();
    let snap_t = beta_doc.update_time.unwrap();
    db.put("gamma/g1", fields!({ "x": 3i64 })).await.unwrap();

    // Subcollections under phantom parent `orgs/acme`
    db.put("orgs/acme/teams/t1", fields!({ "n": "core" }))
        .await
        .unwrap();
    db.put("orgs/acme/audit/e1", fields!({ "n": "init" }))
        .await
        .unwrap();
    db.put("orgs/acme/billing/i1", fields!({ "n": "inv" }))
        .await
        .unwrap();

    // Root collections (`alpha`, `beta`, `gamma`, `orgs`) with `page_size = 0` and `page_size >=
    // len`
    let root_all = db.list_collection_ids("", 0, "", None).await.unwrap();
    expect_eq!(
        root_all.collection_ids,
        vec!["alpha", "beta", "gamma", "orgs"]
    );
    expect_eq!(root_all.next_page_token, "");

    let root_ten = db.list_collection_ids("", 10, "", None).await.unwrap();
    expect_eq!(
        root_ten.collection_ids,
        vec!["alpha", "beta", "gamma", "orgs"]
    );

    // Subcollections under phantom parent `orgs/acme`
    let subs = db
        .list_collection_ids("orgs/acme", 10, "", None)
        .await
        .unwrap();
    expect_eq!(subs.collection_ids, vec!["audit", "billing", "teams"]);

    // Deleting the sole document in `gamma` removes `gamma` from active collection IDs
    db.delete("gamma/g1", None).await.unwrap();
    let after_del = db.list_collection_ids("", 0, "", None).await.unwrap();
    expect_eq!(after_del.collection_ids, vec!["alpha", "beta", "orgs"]);

    // Paginate root collections (`alpha`, `beta`, `orgs`) with `page_size = 2` and exact fit `3`
    let root_p1 = db.list_collection_ids("", 2, "", None).await.unwrap();
    expect_eq!(root_p1.collection_ids, vec!["alpha", "beta"]);
    assert!(!root_p1.next_page_token.is_empty());

    let root_p2 = db
        .list_collection_ids("", 2, &root_p1.next_page_token, None)
        .await
        .unwrap();
    expect_eq!(root_p2.collection_ids, vec!["orgs"]);
    expect_eq!(root_p2.next_page_token, "");

    let root_exact = db.list_collection_ids("", 3, "", None).await.unwrap();
    expect_eq!(root_exact.collection_ids, vec!["alpha", "beta", "orgs"]);
    expect_eq!(root_exact.next_page_token, "");

    // Paginate subcollections under phantom parent `orgs/acme` (`audit`, `billing`, `teams`)
    let sub_p1 = db
        .list_collection_ids("orgs/acme", 2, "", None)
        .await
        .unwrap();
    expect_eq!(sub_p1.collection_ids, vec!["audit", "billing"]);
    assert!(!sub_p1.next_page_token.is_empty());

    let sub_p2 = db
        .list_collection_ids("orgs/acme", 2, &sub_p1.next_page_token, None)
        .await
        .unwrap();
    expect_eq!(sub_p2.collection_ids, vec!["teams"]);
    expect_eq!(sub_p2.next_page_token, "");

    // Nonexistent doc parent returns empty list and empty next_page_token
    let missing_parent = db
        .list_collection_ids("orgs/missing", 2, "", None)
        .await
        .unwrap();
    expect_eq!(missing_parent.collection_ids, Vec::<String>::new());
    expect_eq!(missing_parent.next_page_token, "");

    // Arbitrary / raw-string / cross-parent `page_token` -> `Code::InvalidArgument`
    for bad_tok in ["tok", "alpha", "not-a-valid-token"] {
        assert_status_code(
            db.list_collection_ids("", 2, bad_tok, None).await,
            Code::InvalidArgument,
        );
    }
    assert_status_code(
        db.list_collection_ids("orgs/acme", 2, &root_p1.next_page_token, None)
            .await,
        Code::InvalidArgument,
    );

    // `ReadTime` remains `Code::Unimplemented`
    assert_status_code(
        db.list_collection_ids("", 0, "", Some(snap_t)).await,
        Code::Unimplemented,
    );

    // Collection-path `parent` and malformed `parent` resource name -> `Code::InvalidArgument`
    for bad_parent in [
        format!("{DOCUMENTS_ROOT}/alpha"),
        format!("{DOCUMENTS_ROOT}/orgs/acme/teams"),
        format!("{DOCUMENTS_ROOT}/alpha/__reserved__/doc"),
    ] {
        assert_status_code(
            db.raw_grpc()
                .list_collection_ids(ListCollectionIdsRequest {
                    parent: bad_parent,
                    page_size: 0,
                    page_token: String::new(),
                    consistency_selector: None,
                    request_options: None,
                })
                .await,
            Code::InvalidArgument,
        );
    }
}

#[gtest]
#[tokio::test]
async fn list_documents_boundary_mutations_mismatched_tokens_quoted_orders_and_show_missing() {
    let mut db = TestEnv::new().build_db();

    // 1. Malformed/garbage page_token -> InvalidArgument even on an empty collection.
    for bad_token in [
        "garbage",
        "projects/test-project/databases/(default)/documents/paged/a",
    ] {
        assert_status_code(
            db.list_docs("paged", 2, bad_token, "", None, false, None)
                .await,
            Code::InvalidArgument,
        );
    }

    for (id, rank, other, comma_val, label) in [
        ("a", 1i64, 40i64, 10i64, "la"),
        ("b", 2i64, 30i64, 20i64, "lb"),
        ("c", 3i64, 20i64, 30i64, "lc"),
        ("d", 4i64, 10i64, 40i64, "ld"),
    ] {
        db.put(
            &format!("paged/{id}"),
            fields!({ "rank": rank, "other": other, "a,b": comma_val, "label": label }),
        )
        .await
        .unwrap();
    }

    // Malformed page_token on non-empty collection (default and with order_by) -> InvalidArgument.
    for order in ["", "rank asc"] {
        assert_status_code(
            db.list_docs("paged", 2, "not-a-valid-token", order, None, false, None)
                .await,
            Code::InvalidArgument,
        );
    }

    // 2. Deleted & mutated boundary doc under order_by ASC.
    let asc_p1 = db
        .list_docs("paged", 2, "", "rank asc", None, false, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&asc_p1.documents), vec!["a", "b"]);
    assert!(!asc_p1.next_page_token.is_empty());

    db.delete("paged/b", None).await.unwrap();
    let asc_p2_del = db
        .list_docs(
            "paged",
            2,
            &asc_p1.next_page_token,
            "rank asc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&asc_p2_del.documents), vec!["c", "d"]);
    expect_eq!(asc_p2_del.next_page_token, "");

    // Mutate boundary doc earlier (rank 2 -> 0) and later (rank 2 -> 5).
    db.put(
        "paged/b",
        fields!({ "rank": 2i64, "other": 30i64, "a,b": 20i64, "label": "lb" }),
    )
    .await
    .unwrap();
    let asc_p1_reset = db
        .list_docs("paged", 2, "", "rank asc", None, false, None)
        .await
        .unwrap();
    db.put(
        "paged/b",
        fields!({ "rank": 0i64, "other": 30i64, "a,b": 20i64, "label": "lb" }),
    )
    .await
    .unwrap();
    let asc_p2_earlier = db
        .list_docs(
            "paged",
            2,
            &asc_p1_reset.next_page_token,
            "rank asc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&asc_p2_earlier.documents), vec!["c", "d"]);

    db.put(
        "paged/b",
        fields!({ "rank": 5i64, "other": 30i64, "a,b": 20i64, "label": "lb" }),
    )
    .await
    .unwrap();
    let asc_p2_later = db
        .list_docs(
            "paged",
            3,
            &asc_p1_reset.next_page_token,
            "rank asc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&asc_p2_later.documents), vec!["c", "d", "b"]);
    db.put(
        "paged/b",
        fields!({ "rank": 2i64, "other": 30i64, "a,b": 20i64, "label": "lb" }),
    )
    .await
    .unwrap();

    // 3. Deleted & mutated boundary doc under order_by DESC.
    let desc_p1 = db
        .list_docs("paged", 2, "", "rank desc", None, false, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_p1.documents), vec!["d", "c"]);
    db.delete("paged/c", None).await.unwrap();
    let desc_p2_del = db
        .list_docs(
            "paged",
            2,
            &desc_p1.next_page_token,
            "rank desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_p2_del.documents), vec!["b", "a"]);

    db.put(
        "paged/c",
        fields!({ "rank": 3i64, "other": 20i64, "a,b": 30i64, "label": "lc" }),
    )
    .await
    .unwrap();
    let desc_p1_reset = db
        .list_docs("paged", 2, "", "rank desc", None, false, None)
        .await
        .unwrap();
    db.put(
        "paged/c",
        fields!({ "rank": 5i64, "other": 20i64, "a,b": 30i64, "label": "lc" }),
    )
    .await
    .unwrap();
    let desc_p2_earlier = db
        .list_docs(
            "paged",
            2,
            &desc_p1_reset.next_page_token,
            "rank desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_p2_earlier.documents), vec!["b", "a"]);

    db.put(
        "paged/c",
        fields!({ "rank": 0i64, "other": 20i64, "a,b": 30i64, "label": "lc" }),
    )
    .await
    .unwrap();
    let desc_p2_later = db
        .list_docs(
            "paged",
            3,
            &desc_p1_reset.next_page_token,
            "rank desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_p2_later.documents), vec!["b", "a", "c"]);
    db.put(
        "paged/c",
        fields!({ "rank": 3i64, "other": 20i64, "a,b": 30i64, "label": "lc" }),
    )
    .await
    .unwrap();

    // 4. Mask omitting order_by field (and empty mask).
    let masked_p1 = db
        .list_docs(
            "paged",
            2,
            "",
            "rank desc",
            Some(doc_mask(["label"])),
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&masked_p1.documents), vec!["d", "c"]);
    assert_doc_wire_eq(
        &masked_p1.documents[0],
        "paged/d",
        &fields!({ "label": "ld" }),
    );
    let masked_p2 = db
        .list_docs(
            "paged",
            2,
            &masked_p1.next_page_token,
            "rank desc",
            Some(doc_mask(["label"])),
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&masked_p2.documents), vec!["b", "a"]);

    let empty_mask_p1 = db
        .list_docs(
            "paged",
            2,
            "",
            "rank asc",
            Some(doc_mask(Vec::<&str>::new())),
            false,
            None,
        )
        .await
        .unwrap();
    assert!(empty_mask_p1.documents[0].fields.is_empty());
    let empty_mask_p2 = db
        .list_docs(
            "paged",
            2,
            &empty_mask_p1.next_page_token,
            "rank asc",
            Some(doc_mask(Vec::<&str>::new())),
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&empty_mask_p2.documents), vec!["c", "d"]);

    // 5. Tied sort keys (ASC & DESC) with deleted boundary doc.
    for id in ["t1", "t2", "t3", "t4"] {
        db.put(&format!("tied/{id}"), fields!({ "rank": 10i64 }))
            .await
            .unwrap();
    }
    let tie_asc_p1 = db
        .list_docs("tied", 2, "", "rank asc", None, false, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&tie_asc_p1.documents), vec!["t1", "t2"]);
    db.delete("tied/t2", None).await.unwrap();
    let tie_asc_p2 = db
        .list_docs(
            "tied",
            2,
            &tie_asc_p1.next_page_token,
            "rank asc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&tie_asc_p2.documents), vec!["t3", "t4"]);

    db.put("tied/t2", fields!({ "rank": 10i64 })).await.unwrap();
    let tie_desc_p1 = db
        .list_docs("tied", 2, "", "rank desc", None, false, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&tie_desc_p1.documents), vec!["t4", "t3"]);
    db.delete("tied/t3", None).await.unwrap();
    let tie_desc_p2 = db
        .list_docs(
            "tied",
            2,
            &tie_desc_p1.next_page_token,
            "rank desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&tie_desc_p2.documents), vec!["t2", "t1"]);

    // 5b. Multi-clause `order_by = "rank asc, other desc"` with tied `rank` and tied `other`
    //     values (proves 3-element cursor roundtrips and implicit `__name__` inheriting `DESC`).
    for (id, rank, other) in [
        ("m1", 1i64, 20i64),
        ("m2", 1, 20),
        ("m3", 1, 20),
        ("m4", 1, 10),
        ("m7", 1, 10),
        ("m5", 2, 30),
        ("m6", 2, 30),
    ] {
        db.put(
            &format!("multi_tied/{id}"),
            fields!({ "rank": rank, "other": other }),
        )
        .await
        .unwrap();
    }
    let multi_p1 = db
        .list_docs(
            "multi_tied",
            2,
            "",
            "rank asc, other desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&multi_p1.documents), vec!["m3", "m2"]);
    assert!(!multi_p1.next_page_token.is_empty());

    db.delete("multi_tied/m2", None).await.unwrap();
    let multi_p2 = db
        .list_docs(
            "multi_tied",
            2,
            &multi_p1.next_page_token,
            "rank asc, other desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&multi_p2.documents), vec!["m1", "m7"]);
    assert!(!multi_p2.next_page_token.is_empty());

    let multi_p3 = db
        .list_docs(
            "multi_tied",
            2,
            &multi_p2.next_page_token,
            "rank asc, other desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&multi_p3.documents), vec!["m4", "m6"]);
    assert!(!multi_p3.next_page_token.is_empty());

    let multi_p4 = db
        .list_docs(
            "multi_tied",
            2,
            &multi_p3.next_page_token,
            "rank asc, other desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&multi_p4.documents), vec!["m5"]);
    expect_eq!(multi_p4.next_page_token, "");

    // 6. Backtick-quoted comma order_by and invalid direction rejection.
    let comma_p1 = db
        .list_docs("paged", 2, "", "`a,b` desc", None, false, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&comma_p1.documents), vec!["d", "c"]);
    let comma_p2 = db
        .list_docs(
            "paged",
            2,
            &comma_p1.next_page_token,
            "`a,b` desc",
            None,
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&comma_p2.documents), vec!["b", "a"]);

    assert_status_code(
        db.list_docs("paged", 2, "", "rank sideways", None, false, None)
            .await,
        Code::InvalidArgument,
    );

    // 7. Reusing page_token with altered order_by / direction / syntactic order_by form / mask /
    //    collection_id / parent / show_missing fails with InvalidArgument; altered page_size and
    //    ReadTime with identical order_by and mask succeed.
    let tok = &asc_p1_reset.next_page_token;
    for bad_order in [
        "rank desc",
        "other asc",
        "",
        "rank",
        "rank asc, __name__ asc",
    ] {
        assert_status_code(
            db.list_docs("paged", 2, tok, bad_order, None, false, None)
                .await,
            Code::InvalidArgument,
        );
    }
    assert_status_code(
        db.list_docs(
            "paged",
            2,
            tok,
            "rank asc",
            Some(doc_mask(["other"])),
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs(
            "paged",
            2,
            tok,
            "rank asc",
            Some(doc_mask(Vec::<&str>::new())),
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs(
            "paged",
            2,
            &masked_p1.next_page_token,
            "rank desc",
            None,
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs(
            "paged",
            2,
            &masked_p1.next_page_token,
            "rank desc",
            Some(doc_mask(["other"])),
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs(
            "paged",
            2,
            &masked_p1.next_page_token,
            "rank desc",
            Some(doc_mask(["label", "rank"])),
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs(
            "paged",
            2,
            &empty_mask_p1.next_page_token,
            "rank asc",
            None,
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs("tied", 2, tok, "rank asc", None, false, None)
            .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.raw_grpc()
            .list_documents(ListDocumentsRequest {
                parent: format!("{DOCUMENTS_ROOT}/otherParent/doc"),
                collection_id: "paged".to_string(),
                page_size: 2,
                page_token: tok.clone(),
                order_by: "rank asc".to_string(),
                mask: None,
                show_missing: false,
                consistency_selector: None,
                request_options: None,
            })
            .await,
        Code::InvalidArgument,
    );

    // Altered page_size + ReadTime with identical order_by and mask succeed.
    let same_altered_size = db
        .list_docs(
            "paged",
            1,
            tok,
            "rank asc",
            None,
            false,
            Some(ConsistencySelector::ReadTime(Timestamp::now())),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&same_altered_size.documents), vec!["c"]);

    // 8. show_missing = true pagination across phantom docs (deleted & materialized boundary) and
    //    show_missing false <-> true mismatch.
    db.put("miss/d1", fields!({ "v": 1i64 })).await.unwrap();
    db.put("miss/d2/sub/s1", fields!({ "v": 2i64 }))
        .await
        .unwrap();
    db.put("miss/d3/sub/s1", fields!({ "v": 3i64 }))
        .await
        .unwrap();
    db.put("miss/d4", fields!({ "v": 4i64 })).await.unwrap();

    let default_p1 = db
        .list_docs("miss", 1, "", "", None, false, None)
        .await
        .unwrap();
    assert_status_code(
        db.list_docs("miss", 2, &default_p1.next_page_token, "", None, true, None)
            .await,
        Code::InvalidArgument,
    );

    let miss_p1 = db
        .list_docs("miss", 2, "", "", None, true, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&miss_p1.documents), vec!["d1", "d2"]);
    assert!(miss_p1.documents[0].create_time.is_some());
    assert!(miss_p1.documents[1].create_time.is_none());

    assert_status_code(
        db.list_docs("miss", 2, &miss_p1.next_page_token, "", None, false, None)
            .await,
        Code::InvalidArgument,
    );

    // Delete phantom boundary doc d2 by deleting its subdoc.
    db.delete("miss/d2/sub/s1", None).await.unwrap();
    let miss_p2 = db
        .list_docs("miss", 2, &miss_p1.next_page_token, "", None, true, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&miss_p2.documents), vec!["d3", "d4"]);
    expect_eq!(miss_p2.next_page_token, "");

    // Recreate phantom d2, paginate page 1 again, then materialize d2 before page 2.
    db.put("miss/d2/sub/s1", fields!({ "v": 2i64 }))
        .await
        .unwrap();
    let miss_p1_b = db
        .list_docs("miss", 2, "", "", None, true, None)
        .await
        .unwrap();
    db.put("miss/d2", fields!({ "materialized": true }))
        .await
        .unwrap();
    let miss_p2_b = db
        .list_docs("miss", 2, &miss_p1_b.next_page_token, "", None, true, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&miss_p2_b.documents), vec!["d3", "d4"]);

    // 9. HTTP REST ListDocuments pagination roundtrip (URL-safe token through percent_decode).
    let (status1, body1) = db
        .http_oneshot(
            Method::GET,
            "/v1/projects/test-project/databases/(default)/documents/paged?pageSize=2&\
             orderBy=rank%20asc",
            None,
        )
        .await;
    expect_eq!(status1, StatusCode::OK);
    let resp1: ListDocumentsResponse = serde_json::from_slice(&body1).unwrap();
    expect_eq!(short_ids(&resp1.documents), vec!["a", "b"]);
    assert!(!resp1.next_page_token.is_empty());

    let (status2, body2) = db
        .http_oneshot(
            Method::GET,
            &format!(
                "/v1/projects/test-project/databases/(default)/documents/paged?pageSize=2&\
                 orderBy=rank%20asc&pageToken={}",
                resp1.next_page_token
            ),
            None,
        )
        .await;
    expect_eq!(status2, StatusCode::OK);
    let resp2: ListDocumentsResponse = serde_json::from_slice(&body2).unwrap();
    expect_eq!(short_ids(&resp2.documents), vec!["c", "d"]);
    expect_eq!(resp2.next_page_token, "");
}

#[gtest]
#[tokio::test]
async fn list_documents_roundtrip_pagination_across_all_value_types() {
    let mut db = TestEnv::new().build_db();
    // Seed documents exercising every Firestore value type and tie-breaking edge case on field
    // `v`.
    let cases: [(&str, Value); 25] = [
        ("v_null_a", Value::null()),
        ("v_null_b", Value::null()),
        ("v_bool_f", Value::boolean(false)),
        ("v_nan_a", Value::double(f64::NAN)),
        ("v_nan_b", Value::double(f64::NAN)),
        ("v_zero_int", Value::integer(0)),
        ("v_zero_neg", Value::double(-0.0)),
        ("v_zero_pos", Value::double(0.0)),
        ("v_one_int", Value::integer(1)),
        ("v_one_dbl", Value::double(1.0)),
        ("v_max_int", Value::integer(i64::MAX)),
        ("v_2p63_dbl", Value::double(9_223_372_036_854_775_808.0)),
        (
            "v_ts_neg",
            Value::timestamp(Timestamp {
                seconds: -100,
                nanos:   500,
            }),
        ),
        (
            "v_ts_pos",
            Value::timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos:   1,
            }),
        ),
        ("v_str", Value::string("hello")),
        ("v_bytes", Value::bytes(vec![0u8, 1, 2])),
        (
            "v_ref_ds",
            Value::reference(doc_path("other/__id-9223372036854775808__")),
        ),
        ("v_ref_z", Value::reference(doc_path("other/z"))),
        (
            "v_geo",
            Value::from(LatLng {
                latitude:  52.0907,
                longitude: 5.1214,
            }),
        ),
        ("v_arr_empty", Value::array(vec![])),
        ("v_arr_nums", Value::array(vec![Value::integer(1)])),
        ("v_vec_1", Value::vector([1.0, 2.0])),
        ("v_vec_2", Value::vector([1.0, 3.0])),
        ("v_map_empty", Value::map(std::collections::HashMap::new())),
        (
            "v_map_nested",
            Value::map(std::collections::HashMap::from([(
                "k".to_string(),
                Value::integer(7),
            )])),
        ),
    ];

    for (id, val) in cases {
        let mut fields = std::collections::HashMap::new();
        fields.insert("v".to_string(), val);
        db.put(&format!("types/{id}"), fields).await.unwrap();
    }

    for order in ["v asc", "v desc"] {
        let full = db
            .list_docs("types", 0, "", order, None, false, None)
            .await
            .unwrap();
        expect_eq!(full.documents.len(), 25);

        for page_size in [1, 2, 5] {
            let mut paged = Vec::new();
            let mut token = String::new();
            loop {
                let page = db
                    .list_docs("types", page_size, &token, order, None, false, None)
                    .await
                    .unwrap();
                paged.extend(page.documents);
                if page.next_page_token.is_empty() {
                    break;
                }
                token = page.next_page_token;
            }
            expect_eq!(
                doc_names(&paged),
                doc_names(&full.documents),
                "pagination mismatch for order={order:?}, page_size={page_size}"
            );
        }
    }
}

#[gtest]
#[tokio::test]
async fn list_documents_enterprise_edition_behavior() {
    let mut db = TestEnv::enterprise().build_db();
    db.put("ent/a", fields!({ "rank": 1i64, "label": "la" }))
        .await
        .unwrap();
    db.put("ent/b/sub/s1", fields!({ "rank": 2i64, "label": "lb" }))
        .await
        .unwrap();
    db.put("ent/c", fields!({ "rank": 3i64, "label": "lc" }))
        .await
        .unwrap();

    for order in ["__name__", "rank asc", "`a,b` desc", "rank sideways"] {
        let err = db
            .list_docs("ent", 2, "", order, None, false, None)
            .await
            .unwrap_err();
        expect_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("order by is not supported"),
            "unexpected Enterprise error for order={order:?}: {}",
            err.message()
        );
    }

    let show_missing_err = db
        .list_docs("ent", 2, "", "__name__", None, true, None)
        .await
        .unwrap_err();
    expect_eq!(show_missing_err.code(), Code::InvalidArgument);
    assert!(
        show_missing_err
            .message()
            .contains("cannot specify an order when show_missing is true"),
        "unexpected Enterprise show_missing+order error: {}",
        show_missing_err.message()
    );

    let p1 = db
        .list_docs("ent", 1, "", "", Some(doc_mask(["label"])), false, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&p1.documents), vec!["a"]);
    assert_status_code(
        db.list_docs("ent", 1, &p1.next_page_token, "", None, false, None)
            .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.list_docs(
            "ent",
            1,
            &p1.next_page_token,
            "",
            Some(doc_mask(["rank"])),
            false,
            None,
        )
        .await,
        Code::InvalidArgument,
    );
    let p2 = db
        .list_docs(
            "ent",
            2,
            &p1.next_page_token,
            "",
            Some(doc_mask(["label"])),
            false,
            None,
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&p2.documents), vec!["c"]);
    expect_eq!(p2.next_page_token, "");

    // Paginated show_missing = true on Enterprise returns both real and phantom docs across pages,
    // including when the phantom boundary document disappears before page 2.
    db.put("ent/d/sub/s1", fields!({ "rank": 4i64 }))
        .await
        .unwrap();
    let miss1 = db
        .list_docs("ent", 2, "", "", None, true, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&miss1.documents), vec!["a", "b"]);
    assert!(miss1.documents[0].create_time.is_some());
    assert!(miss1.documents[1].create_time.is_none());
    assert!(!miss1.next_page_token.is_empty());

    db.delete("ent/b/sub/s1", None).await.unwrap();
    let miss2 = db
        .list_docs("ent", 2, &miss1.next_page_token, "", None, true, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&miss2.documents), vec!["c", "d"]);
    assert!(miss2.documents[0].create_time.is_some());
    assert!(miss2.documents[1].create_time.is_none());
    expect_eq!(miss2.next_page_token, "");
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn list_documents_show_missing_default_does_not_block_on_pessimistic_write_lock() {
    let mut db = TestEnv::new().build_db();

    db.put("users/a_phantom/sub/c1", fields!({ "v": 1i64 }))
        .await
        .unwrap();
    db.put("users/doc_z", fields!({ "v": 2i64 })).await.unwrap();

    // Tx1 acquires a retained read lock on `users/doc_z` (older lock priority).
    let mut tx1 = db.begin_rw_txn().await.unwrap();
    let _unused = tx1.get("users/doc_z").await.unwrap();

    // Tx2 commits `[users/a_phantom, users/doc_z]`. Because `commit_read_write` acquires locks
    // in sorted `BTreeSet<DocumentRef>` order (`"users/a_phantom" < "users/doc_z"`), Tx2 holds
    // the exclusive write lock on `users/a_phantom` while parking on `users/doc_z`.
    let mut tx2 = db.begin_rw_txn().await.unwrap();
    tx2.put("users/a_phantom", fields!({ "materialized": true }))
        .put("users/doc_z", fields!({ "v": 3i64 }));
    let mut tx2_commit = ContentionProbe::spawn_op(tx2.commit());
    tx2_commit.assert_pending().await;

    // A normal `ReadConsistency::Default` read on `users/a_phantom` blocks on Tx2's write lock.
    let mut blocked_get_client = db.clone();
    let mut blocked_get =
        ContentionProbe::spawn_op(async move { blocked_get_client.get("users/a_phantom").await });
    blocked_get.assert_pending().await;

    // `list_docs(..., show_missing: true)` under `ReadConsistency::Default` evaluates MVCC
    // snapshots lock-free and completes immediately over both gRPC and HTTP REST.
    let mut list_client = db.clone();
    let list_op = ContentionProbe::spawn_op(async move {
        list_client
            .list_docs("users", 0, "", "", None, true, None)
            .await
    });
    let listed = list_op
        .expect_ready()
        .await
        .expect("gRPC list_docs(show_missing=true) must not block on held write lock");
    expect_eq!(short_ids(&listed.documents), vec!["a_phantom", "doc_z"]);
    expect_that!(listed.documents[0].create_time, none());
    expect_that!(listed.documents[0].update_time, none());
    expect_eq!(listed.documents[1].fields["v"].as_integer(), Some(2));

    let http_op = ContentionProbe::spawn_op(db.http_oneshot(
        Method::GET,
        "/v1/projects/test-project/databases/(default)/documents/users?showMissing=true",
        None,
    ));
    let (http_status, http_body) = http_op.expect_ready().await;
    expect_eq!(http_status, StatusCode::OK);
    let http_resp: ListDocumentsResponse = serde_json::from_slice(&http_body).unwrap();
    expect_eq!(short_ids(&http_resp.documents), vec!["a_phantom", "doc_z"]);
    expect_that!(http_resp.documents[0].create_time, none());

    tx2_commit.assert_pending().await;

    // Releasing Tx1 unblocks Tx2's commit and the parked point read.
    tx1.rollback().await.unwrap();
    tx2_commit
        .expect_ready()
        .await
        .expect("Tx2 commit completes once Tx1 releases doc_z");
    let materialized = blocked_get
        .expect_ready()
        .await
        .expect("blocked point read completes after Tx2 commits");
    expect_eq!(materialized.fields["materialized"].as_boolean(), Some(true));

    let after = db
        .list_docs("users", 0, "", "", None, true, None)
        .await
        .unwrap();
    expect_eq!(short_ids(&after.documents), vec!["a_phantom", "doc_z"]);
    expect_that!(after.documents[0].create_time, some(anything()));
    expect_eq!(after.documents[1].fields["v"].as_integer(), Some(3));
}

#[gtest]
#[tokio::test]
async fn list_documents_show_missing_historical_read_time_excludes_future_phantoms() {
    let mut db = TestEnv::new().build_db();

    db.put("users/alice", fields!({ "v": 1i64 })).await.unwrap();
    db.put("users/phantom_before/sub/c1", fields!({ "v": 1i64 }))
        .await
        .unwrap();
    let deleted_child = db
        .put("users/phantom_deleted/sub/c2", fields!({ "v": 2i64 }))
        .await
        .unwrap();
    let snap_t = deleted_child.update_time.unwrap();

    // Mutations after `snap_t`:
    // - `phantom_after` has its sole child created after `snap_t` (must be excluded at `snap_t`).
    // - `zoe` is a normal document created after `snap_t` (must be excluded at `snap_t`).
    // - `phantom_before` is materialized after `snap_t` (must still appear phantom at `snap_t`).
    // - `phantom_deleted`'s sole child is deleted after `snap_t` (must still appear phantom at
    //   `snap_t`).
    db.put("users/phantom_after/sub/c3", fields!({ "v": 3i64 }))
        .await
        .unwrap();
    db.put("users/zoe", fields!({ "v": 4i64 })).await.unwrap();
    db.put("users/phantom_before", fields!({ "materialized": true }))
        .await
        .unwrap();
    db.delete("users/phantom_deleted/sub/c2", None)
        .await
        .unwrap();

    let historical = db
        .list_docs(
            "users",
            0,
            "",
            "",
            None,
            true,
            Some(ConsistencySelector::ReadTime(snap_t)),
        )
        .await
        .expect("historical list_docs(show_missing=true) at ReadTime");
    expect_eq!(
        short_ids(&historical.documents),
        vec!["alice", "phantom_before", "phantom_deleted"]
    );
    expect_that!(historical.documents[0].create_time, some(anything()));
    expect_that!(historical.documents[1].create_time, none());
    expect_that!(historical.documents[1].update_time, none());
    expect_that!(historical.documents[2].create_time, none());
    expect_that!(historical.documents[2].update_time, none());

    // Pagination at historical `ReadTime(snap_t)` with `show_missing = true` preserves snapshot
    // filtering across pages.
    let hist_p1 = db
        .list_docs(
            "users",
            2,
            "",
            "",
            None,
            true,
            Some(ConsistencySelector::ReadTime(snap_t)),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&hist_p1.documents),
        vec!["alice", "phantom_before"]
    );
    assert!(!hist_p1.next_page_token.is_empty());

    let hist_p2 = db
        .list_docs(
            "users",
            2,
            &hist_p1.next_page_token,
            "",
            None,
            true,
            Some(ConsistencySelector::ReadTime(snap_t)),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&hist_p2.documents), vec!["phantom_deleted"]);
    expect_eq!(hist_p2.next_page_token, "");

    // Latest `Default` listing reflects the post-`snap_t` mutations.
    let latest = db
        .list_docs("users", 0, "", "", None, true, None)
        .await
        .unwrap();
    expect_eq!(
        short_ids(&latest.documents),
        vec!["alice", "phantom_after", "phantom_before", "zoe"]
    );
    expect_that!(latest.documents[1].create_time, none());
    expect_that!(latest.documents[2].create_time, some(anything()));
}
