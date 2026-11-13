use std::{collections::HashMap, convert::Infallible};

use axum::{
    Router,
    body::{Body, BodyDataStream, to_bytes},
};
use bytes::Bytes;
use emulator_database::FirestoreProject;
use emulator_http::RouterBuilder;
use emulator_tracing::DefaultTracing;
use googleapis::google::{
    firestore::v1::{
        BatchGetDocumentsRequest, BatchWriteRequest, BatchWriteResponse, BeginTransactionRequest,
        CommitRequest, CreateDocumentRequest, DeleteDocumentRequest, Document, DocumentMask,
        ExecutePipelineRequest, GetDocumentRequest, ListCollectionIdsRequest,
        ListCollectionIdsResponse, ListDocumentsRequest, ListDocumentsResponse, ListenRequest,
        Precondition, RollbackRequest, RunAggregationQueryRequest, RunQueryRequest,
        StructuredAggregationQuery, Target, TransactionOptions, UpdateDocumentRequest, Value,
        Write, WriteResult, batch_get_documents_request, execute_pipeline_request,
        firestore_client::FirestoreClient,
        get_document_request, list_documents_request, listen_request,
        run_aggregation_query_request, run_query_request,
        target::{self, DocumentsTarget, QueryTarget, query_target},
        transaction_options::{self, ReadOnly, ReadWrite, read_only},
    },
    protobuf::Timestamp,
};
use http::{Method, Request, StatusCode, Uri};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, codec::CompressionEncoding};
use tower::{ServiceExt, util::BoxCloneService};

use super::{
    builders::{
        AggregationSpec, PipelineSpec, QuerySpec, TxnClient, WriteBatchSpec, doc_mask, doc_path,
        split_doc_parent_collection_id, split_parent_and_collection,
    },
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
    stream::{
        AggregationOutput, BatchGetOutput, ListenBatch, ListenHandle, PipelineOutput,
        QueryStreamOutput, collect_aggregation, collect_batch_get, collect_pipeline,
        collect_run_query,
    },
};

pub(crate) type GrpcTransport =
    BoxCloneService<Request<tonic::body::Body>, http::Response<tonic::body::Body>, Infallible>;

const MAX_DECODING_SIZE: usize = 50 * 1024 * 1024;

/// Dual-protocol in-memory client driving `emulator-grpc` and `emulator-http` against an isolated
/// [`FirestoreProject`] without binding OS sockets.
#[derive(Clone, Debug)]
pub(crate) struct TestDb {
    project: &'static FirestoreProject,
    grpc:    FirestoreClient<GrpcTransport>,
    http:    Router,
}

impl TestDb {
    pub(crate) fn from_project(project: &'static FirestoreProject) -> Self {
        let svc = BoxCloneService::new(emulator_grpc::service(project));
        let grpc = FirestoreClient::with_origin(svc, Uri::from_static("http://localhost"))
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(MAX_DECODING_SIZE);
        let http = RouterBuilder::new(project)
            .add_dynamic_tracing(DefaultTracing::start())
            .build();
        Self {
            project,
            grpc,
            http,
        }
    }

    pub(crate) fn project(&self) -> &'static FirestoreProject {
        self.project
    }

    /// Escape hatch returning the underlying Tonic [`FirestoreClient`] for malformed-path,
    /// raw-request, and protocol edge-case tests.
    pub(crate) fn raw_grpc(&mut self) -> &mut FirestoreClient<GrpcTransport> {
        &mut self.grpc
    }

    pub(crate) async fn clear_default_db(&self) {
        self.project.clear_database(&TestEnv::root_ref()).await;
    }

    // -------------------------------------------------------------------------
    // In-memory HTTP / Admin / SSE helpers
    // -------------------------------------------------------------------------

    /// Sends a unary HTTP request through `emulator_http::RouterBuilder` in-memory via `oneshot`,
    /// returning `(StatusCode, Bytes)`.
    pub(crate) async fn http_oneshot(
        &self,
        method: Method,
        uri: &str,
        json_body: Option<&serde_json::Value>,
    ) -> (StatusCode, Bytes) {
        let mut builder = Request::builder().method(method).uri(uri);
        let body = if let Some(val) = json_body {
            builder = builder.header(http::header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(val).expect("valid json payload"))
        } else {
            Body::empty()
        };
        let req = builder.body(body).expect("valid http request");
        let resp = self
            .http
            .clone()
            .oneshot(req)
            .await
            .expect("infallible router oneshot");
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("in-memory response body read");
        (status, bytes)
    }

    pub(crate) async fn http_raw_body(
        &self,
        method: Method,
        uri: &str,
        content_type: Option<&str>,
        raw_body: impl Into<Bytes>,
    ) -> (StatusCode, Bytes) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(ct) = content_type {
            builder = builder.header(http::header::CONTENT_TYPE, ct);
        }
        let req = builder
            .body(Body::from(raw_body.into()))
            .expect("valid http request");
        let resp = self
            .http
            .clone()
            .oneshot(req)
            .await
            .expect("infallible router oneshot");
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("in-memory response body read");
        (status, bytes)
    }

    /// Opens an in-memory SSE or streaming HTTP response via `oneshot`, returning status and
    /// [`BodyDataStream`].
    pub(crate) async fn http_stream(&self, uri: &str) -> (StatusCode, BodyDataStream) {
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .expect("valid stream GET request");
        let resp = self
            .http
            .clone()
            .oneshot(req)
            .await
            .expect("infallible router oneshot");
        (resp.status(), resp.into_body().into_data_stream())
    }

    /// Updates the `(default)` database edition and/or concurrency mode via the HTTP Admin `PATCH`
    /// endpoint (`STANDARD`/`ENTERPRISE`, `PESSIMISTIC`/`OPTIMISTIC`).
    pub(crate) async fn set_admin_mode(
        &self,
        edition: Option<&str>,
        concurrency_mode: Option<&str>,
    ) -> serde_json::Value {
        let mut patch = serde_json::Map::new();
        if let Some(ed) = edition {
            patch.insert("databaseEdition".to_string(), json!(ed));
        }
        if let Some(cm) = concurrency_mode {
            patch.insert("concurrencyMode".to_string(), json!(cm));
        }
        let uri = format!("/v1/{DATABASE_ROOT}");
        let (status, bytes) = self
            .http_oneshot(Method::PATCH, &uri, Some(&serde_json::Value::Object(patch)))
            .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "admin PATCH failed: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("admin PATCH JSON response")
    }

    // -------------------------------------------------------------------------
    // CRUD & Batch scenario methods
    // -------------------------------------------------------------------------

    /// Unconditional upsert (`UpdateDocument` with `update_mask: None`).
    pub(crate) async fn put(
        &mut self,
        rel_doc: &str,
        fields: HashMap<String, Value>,
    ) -> Result<Document, Status> {
        let resp = self
            .grpc
            .update_document(UpdateDocumentRequest {
                document: Some(Document {
                    name: doc_path(rel_doc),
                    fields,
                    create_time: None,
                    update_time: None,
                }),
                update_mask: None,
                mask: None,
                current_document: None,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn create(
        &mut self,
        rel_doc: &str,
        fields: HashMap<String, Value>,
    ) -> Result<Document, Status> {
        let (parent, collection_id, document_id) = split_doc_parent_collection_id(rel_doc);
        let resp = self
            .grpc
            .create_document(CreateDocumentRequest {
                parent,
                collection_id,
                document_id,
                document: Some(Document {
                    name: String::new(),
                    fields,
                    create_time: None,
                    update_time: None,
                }),
                mask: None,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn get(&mut self, rel_doc: &str) -> Result<Document, Status> {
        let resp = self
            .grpc
            .get_document(GetDocumentRequest {
                name: doc_path(rel_doc),
                mask: None,
                consistency_selector: None,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn get_masked<I, S>(
        &mut self,
        rel_doc: &str,
        mask_fields: I,
    ) -> Result<Document, Status>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let resp = self
            .grpc
            .get_document(GetDocumentRequest {
                name: doc_path(rel_doc),
                mask: Some(doc_mask(mask_fields)),
                consistency_selector: None,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn get_at_time(
        &mut self,
        rel_doc: &str,
        read_time: Timestamp,
    ) -> Result<Document, Status> {
        let resp = self
            .grpc
            .get_document(GetDocumentRequest {
                name: doc_path(rel_doc),
                mask: None,
                consistency_selector: Some(get_document_request::ConsistencySelector::ReadTime(
                    read_time,
                )),
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn get_in_txn(
        &mut self,
        rel_doc: &str,
        txn_id: &[u8],
    ) -> Result<Document, Status> {
        let resp = self
            .grpc
            .get_document(GetDocumentRequest {
                name: doc_path(rel_doc),
                mask: None,
                consistency_selector: Some(get_document_request::ConsistencySelector::Transaction(
                    txn_id.to_vec(),
                )),
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn update(
        &mut self,
        rel_doc: &str,
        fields: HashMap<String, Value>,
        update_mask: Option<DocumentMask>,
        current_document: Option<Precondition>,
        return_mask: Option<DocumentMask>,
    ) -> Result<Document, Status> {
        let resp = self
            .grpc
            .update_document(UpdateDocumentRequest {
                document: Some(Document {
                    name: doc_path(rel_doc),
                    fields,
                    create_time: None,
                    update_time: None,
                }),
                update_mask,
                mask: return_mask,
                current_document,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn delete(
        &mut self,
        rel_doc: &str,
        current_document: Option<Precondition>,
    ) -> Result<(), Status> {
        self.grpc
            .delete_document(DeleteDocumentRequest {
                name: doc_path(rel_doc),
                current_document,
            })
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_docs(
        &mut self,
        rel_col: &str,
        page_size: i32,
        page_token: &str,
        order_by: &str,
        mask: Option<DocumentMask>,
        show_missing: bool,
        consistency: Option<list_documents_request::ConsistencySelector>,
    ) -> Result<ListDocumentsResponse, Status> {
        let (parent, collection_id) = split_parent_and_collection(rel_col);
        let resp = self
            .grpc
            .list_documents(ListDocumentsRequest {
                parent,
                collection_id,
                page_size,
                page_token: page_token.to_string(),
                order_by: order_by.to_string(),
                mask,
                show_missing,
                consistency_selector: consistency,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn list_collection_ids(
        &mut self,
        rel_parent_doc: &str,
        page_size: i32,
        page_token: &str,
        read_time: Option<Timestamp>,
    ) -> Result<ListCollectionIdsResponse, Status> {
        let consistency_selector = read_time
            .map(googleapis::google::firestore::v1::list_collection_ids_request::ConsistencySelector::ReadTime);
        let resp = self
            .grpc
            .list_collection_ids(ListCollectionIdsRequest {
                parent: doc_path(rel_parent_doc),
                page_size,
                page_token: page_token.to_string(),
                consistency_selector,
            })
            .await?;
        Ok(resp.into_inner())
    }

    pub(crate) async fn batch_get<I, S>(&mut self, rel_docs: I) -> Result<BatchGetOutput, Status>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let documents = rel_docs.into_iter().map(|p| doc_path(p.as_ref())).collect();
        let resp = self
            .grpc
            .batch_get_documents(BatchGetDocumentsRequest {
                database: DATABASE_ROOT.to_string(),
                documents,
                mask: None,
                consistency_selector: None,
            })
            .await?;
        collect_batch_get(resp.into_inner()).await
    }

    pub(crate) async fn batch_get_in_txn<I, S>(
        &mut self,
        rel_docs: I,
        txn_id: &[u8],
    ) -> Result<BatchGetOutput, Status>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let documents = rel_docs.into_iter().map(|p| doc_path(p.as_ref())).collect();
        let resp = self
            .grpc
            .batch_get_documents(BatchGetDocumentsRequest {
                database: DATABASE_ROOT.to_string(),
                documents,
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::Transaction(txn_id.to_vec()),
                ),
            })
            .await?;
        collect_batch_get(resp.into_inner()).await
    }

    pub(crate) async fn batch_get_new_txn<I, S>(
        &mut self,
        rel_docs: I,
        options: TransactionOptions,
    ) -> Result<BatchGetOutput, Status>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let documents = rel_docs.into_iter().map(|p| doc_path(p.as_ref())).collect();
        let resp = self
            .grpc
            .batch_get_documents(BatchGetDocumentsRequest {
                database: DATABASE_ROOT.to_string(),
                documents,
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::NewTransaction(options),
                ),
            })
            .await?;
        collect_batch_get(resp.into_inner()).await
    }

    pub(crate) async fn batch_write(
        &mut self,
        spec: WriteBatchSpec,
    ) -> Result<BatchWriteResponse, Status> {
        let resp = self
            .grpc
            .batch_write(BatchWriteRequest {
                database: DATABASE_ROOT.to_string(),
                writes:   spec.into_writes(),
                labels:   HashMap::new(),
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Commits a non-transactional `CommitRequest` (`transaction = empty`), which executes
    /// transforms and preconditions atomically per write and returns `(commit_time,
    /// write_results)`.
    pub(crate) async fn commit_writes(
        &mut self,
        spec: WriteBatchSpec,
    ) -> Result<(Option<Timestamp>, Vec<WriteResult>), Status> {
        self.commit_raw(&[], spec.into_writes()).await
    }

    // -------------------------------------------------------------------------
    // Queries, Aggregations, and Pipelines
    // -------------------------------------------------------------------------

    pub(crate) async fn query(&mut self, spec: QuerySpec) -> Result<QueryStreamOutput, Status> {
        self.query_with_consistency(spec, None).await
    }

    pub(crate) async fn query_at_time(
        &mut self,
        spec: QuerySpec,
        read_time: Timestamp,
    ) -> Result<QueryStreamOutput, Status> {
        self.query_with_consistency(
            spec,
            Some(run_query_request::ConsistencySelector::ReadTime(read_time)),
        )
        .await
    }

    pub(crate) async fn query_in_txn(
        &mut self,
        spec: QuerySpec,
        txn_id: &[u8],
    ) -> Result<QueryStreamOutput, Status> {
        self.query_with_consistency(
            spec,
            Some(run_query_request::ConsistencySelector::Transaction(
                txn_id.to_vec(),
            )),
        )
        .await
    }

    pub(crate) async fn query_new_txn(
        &mut self,
        spec: QuerySpec,
        options: TransactionOptions,
    ) -> Result<QueryStreamOutput, Status> {
        self.query_with_consistency(
            spec,
            Some(run_query_request::ConsistencySelector::NewTransaction(
                options,
            )),
        )
        .await
    }

    async fn query_with_consistency(
        &mut self,
        spec: QuerySpec,
        consistency_selector: Option<run_query_request::ConsistencySelector>,
    ) -> Result<QueryStreamOutput, Status> {
        let (parent, structured_query) = spec.into_parts();
        let resp = self
            .grpc
            .run_query(RunQueryRequest {
                parent,
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    structured_query,
                )),
                consistency_selector,
                explain_options: None,
            })
            .await?;
        collect_run_query(resp.into_inner()).await
    }

    pub(crate) async fn aggregate(
        &mut self,
        spec: QuerySpec,
        aggs: AggregationSpec,
    ) -> Result<AggregationOutput, Status> {
        self.aggregate_with_consistency(spec, aggs, None).await
    }

    pub(crate) async fn aggregate_at_time(
        &mut self,
        spec: QuerySpec,
        aggs: AggregationSpec,
        read_time: Timestamp,
    ) -> Result<AggregationOutput, Status> {
        self.aggregate_with_consistency(
            spec,
            aggs,
            Some(run_aggregation_query_request::ConsistencySelector::ReadTime(read_time)),
        )
        .await
    }

    pub(crate) async fn aggregate_in_txn(
        &mut self,
        spec: QuerySpec,
        aggs: AggregationSpec,
        txn_id: &[u8],
    ) -> Result<AggregationOutput, Status> {
        self.aggregate_with_consistency(
            spec,
            aggs,
            Some(run_aggregation_query_request::ConsistencySelector::Transaction(txn_id.to_vec())),
        )
        .await
    }

    async fn aggregate_with_consistency(
        &mut self,
        spec: QuerySpec,
        aggs: AggregationSpec,
        consistency_selector: Option<run_aggregation_query_request::ConsistencySelector>,
    ) -> Result<AggregationOutput, Status> {
        let (parent, structured_query) = spec.into_parts();
        let resp = self
            .grpc
            .run_aggregation_query(RunAggregationQueryRequest {
                parent,
                query_type: Some(
                    run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                        StructuredAggregationQuery {
                            aggregations: aggs.into_vec(),
                            query_type:   Some(
                                googleapis::google::firestore::v1::structured_aggregation_query::QueryType::StructuredQuery(
                                    structured_query,
                                ),
                            ),
                        },
                    ),
                ),
                consistency_selector,
                explain_options: None,
            })
            .await?;
        collect_aggregation(resp.into_inner()).await
    }

    pub(crate) async fn pipeline(&mut self, spec: PipelineSpec) -> Result<PipelineOutput, Status> {
        self.pipeline_with_consistency(spec, None).await
    }

    pub(crate) async fn pipeline_in_txn(
        &mut self,
        spec: PipelineSpec,
        txn_id: &[u8],
    ) -> Result<PipelineOutput, Status> {
        self.pipeline_with_consistency(
            spec,
            Some(execute_pipeline_request::ConsistencySelector::Transaction(
                txn_id.to_vec(),
            )),
        )
        .await
    }

    pub(crate) async fn pipeline_new_txn(
        &mut self,
        spec: PipelineSpec,
        options: TransactionOptions,
    ) -> Result<PipelineOutput, Status> {
        self.pipeline_with_consistency(
            spec,
            Some(execute_pipeline_request::ConsistencySelector::NewTransaction(options)),
        )
        .await
    }

    async fn pipeline_with_consistency(
        &mut self,
        spec: PipelineSpec,
        consistency_selector: Option<execute_pipeline_request::ConsistencySelector>,
    ) -> Result<PipelineOutput, Status> {
        let resp = self
            .grpc
            .execute_pipeline(ExecutePipelineRequest {
                database: DATABASE_ROOT.to_string(),
                pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                    spec.into_structured(),
                )),
                consistency_selector,
            })
            .await?;
        collect_pipeline(resp.into_inner()).await
    }

    // -------------------------------------------------------------------------
    // Transactions
    // -------------------------------------------------------------------------

    pub(crate) async fn begin_rw_txn(&mut self) -> Result<TxnClient, Status> {
        let resp = self
            .grpc
            .begin_transaction(BeginTransactionRequest {
                database: DATABASE_ROOT.to_string(),
                options:  Some(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                        retry_transaction: Vec::new(),
                    })),
                }),
            })
            .await?;
        Ok(TxnClient::new(self.clone(), resp.into_inner().transaction))
    }

    pub(crate) async fn begin_retry_rw_txn(
        &mut self,
        retry_txn: &[u8],
    ) -> Result<TxnClient, Status> {
        let resp = self
            .grpc
            .begin_transaction(BeginTransactionRequest {
                database: DATABASE_ROOT.to_string(),
                options:  Some(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                        retry_transaction: retry_txn.to_vec(),
                    })),
                }),
            })
            .await?;
        Ok(TxnClient::new(self.clone(), resp.into_inner().transaction))
    }

    pub(crate) async fn begin_ro_txn(
        &mut self,
        read_time: Option<Timestamp>,
    ) -> Result<TxnClient, Status> {
        let consistency_selector = read_time.map(read_only::ConsistencySelector::ReadTime);
        let resp = self
            .grpc
            .begin_transaction(BeginTransactionRequest {
                database: DATABASE_ROOT.to_string(),
                options:  Some(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadOnly(ReadOnly {
                        consistency_selector,
                    })),
                }),
            })
            .await?;
        Ok(TxnClient::new(self.clone(), resp.into_inner().transaction))
    }

    pub(crate) async fn commit_raw(
        &mut self,
        txn_id: &[u8],
        writes: Vec<Write>,
    ) -> Result<(Option<Timestamp>, Vec<WriteResult>), Status> {
        let resp = self
            .grpc
            .commit(CommitRequest {
                database: DATABASE_ROOT.to_string(),
                writes,
                transaction: txn_id.to_vec(),
            })
            .await?
            .into_inner();
        Ok((resp.commit_time, resp.write_results))
    }

    pub(crate) async fn rollback_raw(&mut self, txn_id: &[u8]) -> Result<(), Status> {
        self.grpc
            .rollback(RollbackRequest {
                database:    DATABASE_ROOT.to_string(),
                transaction: txn_id.to_vec(),
            })
            .await?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Real-time Listeners
    // -------------------------------------------------------------------------

    pub(crate) async fn open_raw_listen(&mut self) -> Result<ListenHandle, Status> {
        let (tx, rx) = mpsc::channel(16);
        let resp = self.grpc.listen(ReceiverStream::new(rx)).await?;
        Ok(ListenHandle::spawn(tx, resp.into_inner()))
    }

    pub(crate) async fn listen_doc(
        &mut self,
        rel_doc: &str,
        resume_type: Option<target::ResumeType>,
    ) -> Result<(ListenHandle, ListenBatch), Status> {
        let mut handle = self.open_raw_listen().await?;
        handle
            .send(ListenRequest {
                database: DATABASE_ROOT.to_string(),
                labels: HashMap::new(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Documents(DocumentsTarget {
                        documents: vec![doc_path(rel_doc)],
                    })),
                    resume_type,
                })),
            })
            .await;
        let initial = handle.next_batch().await?;
        Ok((handle, initial))
    }

    pub(crate) async fn listen_query(
        &mut self,
        spec: QuerySpec,
    ) -> Result<(ListenHandle, ListenBatch), Status> {
        let (parent, structured_query) = spec.into_parts();
        let mut handle = self.open_raw_listen().await?;
        handle
            .send(ListenRequest {
                database: DATABASE_ROOT.to_string(),
                labels: HashMap::new(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Query(QueryTarget {
                        parent:     if parent.is_empty() {
                            DOCUMENTS_ROOT.to_string()
                        } else {
                            parent
                        },
                        query_type: Some(query_target::QueryType::StructuredQuery(
                            structured_query,
                        )),
                    })),
                    resume_type: None,
                })),
            })
            .await;
        let initial = handle.next_batch().await?;
        Ok((handle, initial))
    }
}
