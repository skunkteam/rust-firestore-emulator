use googleapis::{
    fields,
    google::firestore::v1::{
        ListCollectionIdsRequest, list_documents_request::ConsistencySelector,
    },
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{doc_mask, doc_path},
    env::{DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
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

    // Pagination (`0 < page_size < len`), non-empty `page_token`, and `ReadTime` return
    // `Code::Unimplemented`
    assert_status_code(
        db.list_collection_ids("", 2, "", None).await,
        Code::Unimplemented,
    );
    assert_status_code(
        db.list_collection_ids("", 0, "tok", None).await,
        Code::Unimplemented,
    );
    assert_status_code(
        db.list_collection_ids("", 0, "", Some(snap_t)).await,
        Code::Unimplemented,
    );

    // Malformed `parent` resource name -> `Code::InvalidArgument`
    assert_status_code(
        db.raw_grpc()
            .list_collection_ids(ListCollectionIdsRequest {
                parent: format!("{DOCUMENTS_ROOT}/alpha/__reserved__/doc"),
                page_size: 0,
                page_token: String::new(),
                consistency_selector: None,
            })
            .await,
        Code::InvalidArgument,
    );
}
