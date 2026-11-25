use std::collections::HashMap;

use googleapis::{
    fields,
    google::firestore::v1::{
        Document, ExecutePipelineRequest, TransactionOptions, Value, execute_pipeline_request,
        transaction_options::{self, ReadWrite},
    },
    val,
};
use googletest::prelude::*;
use prost::Message;
use tonic::Code;

use crate::harness::{
    builders::{PipelineSpec, WriteBatchSpec, doc_path, field_ref_val, fn_val, vec_val},
    env::{DATABASE_ROOT, TestEnv},
    matchers::{assert_status_code, short_ids},
};

#[gtest]
#[tokio::test]
async fn enterprise_pipeline_vector_functions_find_nearest_and_chunked_streaming() {
    let mut db = TestEnv::enterprise().build_db();

    db.put(
        "pvec/v1",
        fields!({ "emb": vec_val([1.0, 0.0]), "alt": vec_val([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    db.put(
        "pvec/v2",
        fields!({ "emb": vec_val([0.0, 1.0]), "alt": vec_val([1.0, 0.0, 0.0]) }),
    )
    .await
    .unwrap();
    db.put("pvec/null_doc", fields!({ "emb": null, "alt": null }))
        .await
        .unwrap();
    db.put("pvec/bad_type", fields!({ "emb": "not_a_vec" }))
        .await
        .unwrap();
    db.put("pvec/zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();

    // 1a. Direct vector expressions + null propagation (excluding `bad_type` and `zero`, which
    //     raise InvalidArgument unless wrapped in `is_error`).
    let expr_out = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .where_expr(fn_val(
                    "and",
                    vec![
                        fn_val(
                            "not_equal",
                            vec![
                                field_ref_val("__name__"),
                                Value::reference(doc_path("pvec/bad_type")),
                            ],
                        ),
                        fn_val(
                            "not_equal",
                            vec![
                                field_ref_val("__name__"),
                                Value::reference(doc_path("pvec/zero")),
                            ],
                        ),
                    ],
                ))
                .add_fields(HashMap::from([
                    (
                        "cos".to_string(),
                        fn_val(
                            "cosine_distance",
                            vec![field_ref_val("emb"), vec_val([1.0, 0.0])],
                        ),
                    ),
                    (
                        "euc".to_string(),
                        fn_val(
                            "euclidean_distance",
                            vec![field_ref_val("emb"), vec_val([1.0, 0.0])],
                        ),
                    ),
                    (
                        "dot".to_string(),
                        fn_val(
                            "dot_product",
                            vec![field_ref_val("emb"), vec_val([2.0, 3.0])],
                        ),
                    ),
                    (
                        "vlen".to_string(),
                        fn_val("vector_length", vec![field_ref_val("emb")]),
                    ),
                ]))
                .sort_asc("__name__"),
        )
        .await
        .unwrap();

    let by_id: HashMap<&str, &Document> = expr_out
        .results
        .iter()
        .map(|d| (d.name.rsplit('/').next().unwrap(), d))
        .collect();

    // `v1`: matching 2D vectors -> cos=0, euc=0, dot=2, vlen=2
    expect_eq!(by_id["v1"].fields["cos"].as_double(), Some(0.0));
    expect_eq!(by_id["v1"].fields["euc"].as_double(), Some(0.0));
    expect_eq!(by_id["v1"].fields["dot"].as_double(), Some(2.0));
    expect_eq!(by_id["v1"].fields["vlen"].as_integer(), Some(2));

    // `v2`: [0.0, 1.0] vs [1.0, 0.0] -> cos=1.0, vlen=2
    expect_eq!(by_id["v2"].fields["cos"].as_double(), Some(1.0));
    expect_eq!(by_id["v2"].fields["vlen"].as_integer(), Some(2));

    // `null_doc`: missing/null -> NullValue without error
    expect_eq!(by_id["null_doc"].fields["cos"], Value::null());
    expect_eq!(by_id["null_doc"].fields["euc"], Value::null());
    expect_eq!(by_id["null_doc"].fields["dot"], Value::null());
    expect_eq!(by_id["null_doc"].fields["vlen"], Value::null());

    // 1b. `is_error(...)` catching InvalidArgument (dimension mismatch, non-vector type, [0, 0]
    //     cosine NaN) while propagating `null` safely as `false`.
    let err_out = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .add_fields(HashMap::from([
                    (
                        "alt_cos_err".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val(
                                "cosine_distance",
                                vec![field_ref_val("emb"), field_ref_val("alt")],
                            )],
                        ),
                    ),
                    (
                        "self_cos_err".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val(
                                "cosine_distance",
                                vec![field_ref_val("emb"), vec_val([1.0, 0.0])],
                            )],
                        ),
                    ),
                    (
                        "vlen_err".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val("vector_length", vec![field_ref_val("emb")])],
                        ),
                    ),
                ]))
                .sort_asc("__name__"),
        )
        .await
        .unwrap();

    let err_by_id: HashMap<&str, &Document> = err_out
        .results
        .iter()
        .map(|d| (d.name.rsplit('/').next().unwrap(), d))
        .collect();

    expect_eq!(
        err_by_id["v1"].fields["alt_cos_err"].as_boolean(),
        Some(false)
    );
    expect_eq!(
        err_by_id["v1"].fields["self_cos_err"].as_boolean(),
        Some(false)
    );
    expect_eq!(err_by_id["v1"].fields["vlen_err"].as_boolean(), Some(false));
    // `v2`: `alt` is 3D -> dimension mismatch caught by `is_error`
    expect_eq!(
        err_by_id["v2"].fields["alt_cos_err"].as_boolean(),
        Some(true)
    );
    expect_eq!(err_by_id["v2"].fields["vlen_err"].as_boolean(), Some(false));
    // `null_doc`: `null` returns NullValue, not error
    expect_eq!(
        err_by_id["null_doc"].fields["self_cos_err"].as_boolean(),
        Some(false)
    );
    expect_eq!(
        err_by_id["null_doc"].fields["vlen_err"].as_boolean(),
        Some(false)
    );
    // `bad_type`: non-vector `emb` caught by `is_error`
    expect_eq!(
        err_by_id["bad_type"].fields["self_cos_err"].as_boolean(),
        Some(true)
    );
    expect_eq!(
        err_by_id["bad_type"].fields["vlen_err"].as_boolean(),
        Some(true)
    );
    // `zero`: `[0, 0]` cosine_distance -> `NaN` InvalidArgument caught by `is_error`
    expect_eq!(
        err_by_id["zero"].fields["self_cos_err"].as_boolean(),
        Some(true)
    );

    // Unknown function inside `is_error(...)` must STILL return Code::Unimplemented!
    assert_status_code(
        db.pipeline(PipelineSpec::collection("pvec").add_fields(HashMap::from([(
            "x".to_string(),
            fn_val(
                "is_error",
                vec![fn_val("totally_unknown_fn", vec![val!(1i64)])],
            ),
        )])))
        .await,
        Code::Unimplemented,
    );

    // 2. Pipeline `find_nearest` stage: MapValue and ArrayValue query vectors, all 3 measures,
    //    limit & distance_field, and invalid options/limits.
    let stage_euc = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .where_expr(fn_val(
                    "not_equal",
                    vec![
                        field_ref_val("__name__"),
                        Value::reference(doc_path("pvec/zero")),
                    ],
                ))
                .find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    "euclidean",
                    Some(2),
                    Some("dist"),
                ),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&stage_euc.results), vec!["v1", "v2"]);
    expect_eq!(stage_euc.results[0].fields["dist"].as_double(), Some(0.0));
    expect_eq!(
        stage_euc.results[1].fields["dist"].as_double(),
        Some(2.0_f64.sqrt())
    );

    // Valid VectorValue query vectors with dot_product and cosine
    let stage_dot = db
        .pipeline(PipelineSpec::collection("pvec").find_nearest(
            "emb",
            vec_val([3.0, 1.0]),
            "dot_product",
            Some(2),
            Some("score"),
        ))
        .await
        .unwrap();
    expect_eq!(short_ids(&stage_dot.results), vec!["v1", "v2"]);
    expect_eq!(stage_dot.results[0].fields["score"].as_double(), Some(3.0));
    expect_eq!(stage_dot.results[1].fields["score"].as_double(), Some(1.0));

    let stage_cos = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .where_expr(fn_val(
                    "not_equal",
                    vec![
                        field_ref_val("__name__"),
                        Value::reference(doc_path("pvec/zero")),
                    ],
                ))
                .find_nearest("emb", vec_val([1.0, 0.0]), "cosine", Some(1), Some("cdist")),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&stage_cos.results), vec!["v1"]);
    expect_eq!(stage_cos.results[0].fields["cdist"].as_double(), Some(0.0));

    // Raw ArrayValue query vectors, StringValue field, and StringValue distance_field ->
    // InvalidArgument.
    for bad_vec in [val!([1.0f64, 0.0f64]), val!([1i64, 0i64])] {
        assert_status_code(
            db.pipeline(PipelineSpec::collection("pvec").find_nearest(
                "emb",
                bad_vec,
                "euclidean",
                Some(2),
                Some("dist"),
            ))
            .await,
            Code::InvalidArgument,
        );
    }
    assert_status_code(
        db.pipeline(PipelineSpec::collection("pvec").raw_stage(
            "find_nearest",
            vec![val!("emb"), vec_val([1.0, 0.0]), val!("euclidean")],
        ))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.pipeline(PipelineSpec::collection("pvec").raw_stage_with_options(
            "find_nearest",
            vec![field_ref_val("emb"), vec_val([1.0, 0.0]), val!("euclidean")],
            HashMap::from([("distance_field".to_string(), val!("dist"))]),
        ))
        .await,
        Code::InvalidArgument,
    );

    // Unknown options (`threshold` / `distance_threshold`) and invalid limit -> InvalidArgument.
    for opt_name in ["threshold", "distance_threshold"] {
        assert_status_code(
            db.pipeline(PipelineSpec::collection("pvec").raw_stage_with_options(
                "find_nearest",
                vec![field_ref_val("emb"), vec_val([1.0, 0.0]), val!("euclidean")],
                HashMap::from([(opt_name.to_string(), val!(0.5f64))]),
            ))
            .await,
            Code::InvalidArgument,
        );
    }
    for bad_limit in [0i64, 1001i64] {
        assert_status_code(
            db.pipeline(PipelineSpec::collection("pvec").find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                "euclidean",
                Some(bad_limit),
                None,
            ))
            .await,
            Code::InvalidArgument,
        );
    }

    // 3. Raw Tonic `execute_pipeline` chunking: (a) 200 x 2048D docs with NewTransaction -> every
    //    frame <= 1 MiB, transaction on frame 0 only, multi-frame stream, total 200 docs.
    let big_vec = vec_val((0..2048).map(|i| i as f64));
    for chunk_start in (0..200).step_by(50) {
        let mut batch = WriteBatchSpec::new();
        for idx in chunk_start..(chunk_start + 50) {
            batch = batch.put(
                &format!("chunk200/d{idx:03}"),
                HashMap::from([("emb".to_string(), big_vec.clone())]),
            );
        }
        db.batch_write(batch).await.unwrap();
    }

    let mut raw_stream = db
        .raw_grpc()
        .execute_pipeline(ExecutePipelineRequest {
            database: DATABASE_ROOT.to_string(),
            pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                PipelineSpec::collection("chunk200").into_structured(),
            )),
            consistency_selector: Some(
                execute_pipeline_request::ConsistencySelector::NewTransaction(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(ReadWrite::default())),
                }),
            ),
            auto_commit_transaction: false,
            request_options: None,
        })
        .await
        .unwrap()
        .into_inner();

    let mut frame_idx = 0usize;
    let mut total_docs = 0usize;
    let mut first_txn = Vec::new();
    while let Some(frame) = raw_stream.message().await.unwrap() {
        let encoded_len = frame.encoded_len();
        assert!(
            encoded_len <= 1_048_576,
            "frame {frame_idx} exceeded 1 MiB budget: {encoded_len} bytes"
        );
        if frame_idx == 0 {
            assert!(
                !frame.transaction.is_empty(),
                "frame 0 must carry NewTransaction ID"
            );
            first_txn = frame.transaction;
        } else {
            assert!(
                frame.transaction.is_empty(),
                "frame {frame_idx} must NOT repeat transaction ID"
            );
        }
        total_docs += frame.results.len();
        frame_idx += 1;
    }
    assert!(
        frame_idx > 1,
        "200 x 2048D documents must span multiple <= 1 MiB frames, got {frame_idx}"
    );
    expect_eq!(total_docs, 200);
    db.rollback_raw(&first_txn).await.unwrap();

    // (b) Single oversized document (> 1 MiB) still makes progress in 1 frame.
    let mut oversized_fields = HashMap::new();
    for k in 0..60 {
        oversized_fields.insert(format!("v{k:02}"), big_vec.clone());
    }
    db.put("oversize/one", oversized_fields).await.unwrap();
    let mut over_stream = db
        .raw_grpc()
        .execute_pipeline(ExecutePipelineRequest {
            database: DATABASE_ROOT.to_string(),
            pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                PipelineSpec::collection("oversize").into_structured(),
            )),
            consistency_selector: None,
            auto_commit_transaction: false,
            request_options: None,
        })
        .await
        .unwrap()
        .into_inner();
    let mut over_frames = 0usize;
    let mut over_docs = 0usize;
    while let Some(frame) = over_stream.message().await.unwrap() {
        over_frames += 1;
        over_docs += frame.results.len();
    }
    expect_eq!(over_frames, 1);
    expect_eq!(over_docs, 1);

    // (c) Empty pipeline result emits exactly 1 empty frame.
    let mut empty_stream = db
        .raw_grpc()
        .execute_pipeline(ExecutePipelineRequest {
            database: DATABASE_ROOT.to_string(),
            pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                PipelineSpec::collection("empty_col").into_structured(),
            )),
            consistency_selector: None,
            auto_commit_transaction: false,
            request_options: None,
        })
        .await
        .unwrap()
        .into_inner();
    let mut empty_frames = 0usize;
    while let Some(frame) = empty_stream.message().await.unwrap() {
        empty_frames += 1;
        expect_that!(frame.results.is_empty(), eq(true));
    }
    expect_eq!(empty_frames, 1);
}
