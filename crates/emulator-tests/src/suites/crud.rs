use googleapis::{
    doc, fields,
    google::firestore::v1::{
        BatchGetDocumentsRequest, CreateDocumentRequest, GetDocumentRequest, UpdateDocumentRequest,
        Value, Write, write,
    },
    val,
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{WriteBatchSpec, doc_mask, doc_path, pre_exists},
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_monotonic_timestamps, assert_status_code},
};

#[gtest]
#[tokio::test]
async fn create_get_update_delete_and_wire_types_roundtrip() {
    let mut db = TestEnv::new().build_db();

    // 1. CreateDocument on top-level and nested subcollection, preserving wire-exact 1 vs 1.0 &
    //    -0.0
    let initial = fields!({
        "int_one": 1i64,
        "float_one": 1.0f64,
        "neg_zero": -0.0f64,
        "nan_val": f64::NAN,
        "raw": b"wire-bytes",
        "nested": {
            "keep": "yes",
            "drop_me": 99i64,
        },
        "tags": [1i64, 1.0f64],
    });
    let created = db
        .create("users/alice", initial.clone())
        .await
        .expect("CreateDocument should succeed");
    assert_doc_wire_eq(&created, "users/alice", &initial);

    // Duplicate CreateDocument -> AlreadyExists
    assert_status_code(
        db.create("users/alice", fields!({ "int_one": 2i64 })).await,
        Code::AlreadyExists,
    );

    // GetDocument full and projected via DocumentMask
    let fetched = db.get("users/alice").await.expect("GetDocument");
    assert_doc_wire_eq(&fetched, "users/alice", &initial);

    let projected = db
        .get_masked("users/alice", ["int_one", "nested.keep", "nonexistent"])
        .await
        .expect("masked GetDocument");
    assert_doc_wire_eq(
        &projected,
        "users/alice",
        &fields!({
            "int_one": 1i64,
            "nested": { "keep": "yes" },
        }),
    );

    // UpdateDocument with nested update_mask (modifies nested.keep, deletes nested.drop_me, adds
    // new leaf)
    let updated = db
        .update(
            "users/alice",
            fields!({
                "nested": {
                    "keep": "updated",
                    "added": true,
                }
            }),
            Some(doc_mask(["nested.keep", "nested.drop_me", "nested.added"])),
            None,
            Some(doc_mask(["int_one", "nested"])),
        )
        .await
        .expect("masked UpdateDocument");
    assert_doc_wire_eq(
        &updated,
        "users/alice",
        &fields!({
            "int_one": 1i64,
            "nested": {
                "keep": "updated",
                "added": true,
            }
        }),
    );
    assert_monotonic_timestamps(
        &[created.update_time.unwrap(), updated.update_time.unwrap()],
        true,
    );

    // Subcollection child under a phantom parent (users/ghost never written)
    let sub_doc = db
        .create("users/ghost/posts/post_1", fields!({ "title": "hello" }))
        .await
        .expect("subcollection CreateDocument under phantom parent");
    assert_doc_wire_eq(
        &sub_doc,
        "users/ghost/posts/post_1",
        &fields!({ "title": "hello" }),
    );
    // Phantom parent itself is NotFound on GetDocument
    assert_status_code(db.get("users/ghost").await, Code::NotFound);

    // DeleteDocument on existing document + idempotent delete on missing document
    db.delete("users/alice", None)
        .await
        .expect("DeleteDocument existing");
    assert_status_code(db.get("users/alice").await, Code::NotFound);
    db.delete("users/alice", None)
        .await
        .expect("DeleteDocument missing is idempotent when no precondition is set");
}

#[gtest]
#[tokio::test]
async fn crud_rejects_malformed_paths_and_non_storable_values() {
    let mut db = TestEnv::new().build_db();

    // CreateDocument: non-empty document.name -> InvalidArgument
    let res = db
        .raw_grpc()
        .create_document(CreateDocumentRequest {
            parent: DOCUMENTS_ROOT.to_string(),
            collection_id: "users".to_string(),
            document_id: "bob".to_string(),
            document: Some(doc!("must-be-empty", { "a": 1i64 })),
            mask: None,
        })
        .await;
    assert_status_code(res, Code::InvalidArgument);

    // CreateDocument: collection path passed as `parent` -> InvalidArgument
    let res = db
        .raw_grpc()
        .create_document(CreateDocumentRequest {
            parent: format!("{DOCUMENTS_ROOT}/users"),
            collection_id: "posts".to_string(),
            document_id: "p1".to_string(),
            document: Some(doc! { "a": 1i64 }),
            mask: None,
        })
        .await;
    assert_status_code(res, Code::InvalidArgument);

    // CreateDocument: empty or slash-containing collection_id -> InvalidArgument
    let res = db
        .raw_grpc()
        .create_document(CreateDocumentRequest {
            parent: DOCUMENTS_ROOT.to_string(),
            collection_id: "users/nested".to_string(),
            document_id: "p1".to_string(),
            document: Some(doc! { "a": 1i64 }),
            mask: None,
        })
        .await;
    assert_status_code(res, Code::InvalidArgument);

    // GetDocument: collection path instead of document path -> InvalidArgument
    let res = db
        .raw_grpc()
        .get_document(GetDocumentRequest {
            name: format!("{DOCUMENTS_ROOT}/users"),
            mask: None,
            consistency_selector: None,
        })
        .await;
    assert_status_code(res, Code::InvalidArgument);

    // UpdateDocument: reserved `__id__` document ID and nested array value -> InvalidArgument
    assert_status_code(
        db.put("users/__reserved__", fields!({ "a": 1i64 })).await,
        Code::InvalidArgument,
    );
    let nested_arr = fields!({
        "matrix": [val!([1i64, 2i64])],
    });
    assert_status_code(
        db.put("users/matrix_doc", nested_arr).await,
        Code::InvalidArgument,
    );

    // UpdateDocument: missing `document` field -> InvalidArgument
    let res = db
        .raw_grpc()
        .update_document(UpdateDocumentRequest {
            document: None,
            update_mask: None,
            mask: None,
            current_document: None,
        })
        .await;
    assert_status_code(res, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn batch_get_and_batch_write_handle_missing_stream_errors_and_partial_failures() {
    let mut db = TestEnv::new().build_db();

    db.put("items/a", fields!({ "n": 10i64 })).await.unwrap();
    db.put("items/c", fields!({ "n": 30i64 })).await.unwrap();

    // BatchGetDocuments preserves Found and Missing entries
    let batch = db
        .batch_get(["items/a", "items/b", "items/c"])
        .await
        .expect("BatchGetDocuments");
    expect_eq!(batch.items.len(), 3);
    expect_eq!(batch.found_docs().len(), 2);
    expect_eq!(batch.missing_names(), vec![doc_path("items/b").as_str()]);
    expect_that!(batch.read_time, some(anything()));

    // BatchGetDocuments with invalid document reference surfaces `error_in_stream` InvalidArgument
    let bad_stream = db
        .raw_grpc()
        .batch_get_documents(BatchGetDocumentsRequest {
            database: DATABASE_ROOT.to_string(),
            documents: vec![format!("{DOCUMENTS_ROOT}/items")], // odd segments (collection)
            mask: None,
            consistency_selector: None,
        })
        .await
        .expect("RPC envelope succeeds, error is delivered on first stream frame");
    let err = crate::harness::stream::collect_batch_get(bad_stream.into_inner()).await;
    assert_status_code(err, Code::InvalidArgument);

    // BatchWrite reports per-write google::rpc::Status without aborting sibling writes
    let bw = db
        .batch_write(
            WriteBatchSpec::new()
                // 0: valid create via Exists(false)
                .update_with(
                    "items/d",
                    fields!({ "n": 40i64 }),
                    None,
                    vec![],
                    Some(pre_exists(false)),
                )
                // 1: duplicate Exists(false) on existing `items/a` -> AlreadyExists
                .update_with(
                    "items/a",
                    fields!({ "n": 99i64 }),
                    None,
                    vec![],
                    Some(pre_exists(false)),
                )
                // 2: valid delete of `items/c`
                .delete("items/c", Some(pre_exists(true)))
                // 3: invalid write missing operation -> Unimplemented / non-zero status
                .raw(Write {
                    update_mask: None,
                    update_transforms: vec![],
                    current_document: None,
                    operation: Some(write::Operation::Update(doc!(
                        "projects/test-project/databases/(default)/documents/items/__bad__",
                        { "n": 1i64 }
                    ))),
                }),
        )
        .await
        .expect("BatchWrite returns per-entry statuses");

    expect_eq!(bw.status.len(), 4);
    expect_eq!(bw.write_results.len(), 4);
    expect_eq!(bw.status[0].code, Code::Ok as i32);
    expect_eq!(bw.status[1].code, Code::AlreadyExists as i32);
    expect_eq!(bw.status[2].code, Code::Ok as i32);
    expect_eq!(bw.status[3].code, Code::InvalidArgument as i32);

    // Sibling writes 0 and 2 took effect while failed write 1 left `items/a` untouched
    assert_doc_wire_eq(
        &db.get("items/a").await.unwrap(),
        "items/a",
        &fields!({ "n": 10i64 }),
    );
    assert_doc_wire_eq(
        &db.get("items/d").await.unwrap(),
        "items/d",
        &fields!({ "n": 40i64 }),
    );
    assert_status_code(db.get("items/c").await, Code::NotFound);
    let _unused = Value::null();
}
