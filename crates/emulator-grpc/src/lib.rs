//! This crate provides the service that implements the GRPC API to the emulator database. See
//! [`service`] for usage info.

use std::{
    mem,
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use emulator_database::{
    FirestoreDatabase, FirestoreProject, GenericDatabaseError, pipeline,
    projection::{Project, Projection},
    query::Query,
    read_consistency::ReadConsistency,
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
};
use futures_util::{TryStreamExt, future::join_all, stream::BoxStream};
use googleapis::google::{
    firestore::v1::{
        firestore_server::{Firestore, FirestoreServer},
        *,
    },
    protobuf::{Empty, Timestamp},
};
use itertools::Itertools;
use prost::{Message, encoding::encoded_len_varint};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Code, Request, Response, Result, Status, async_trait, codec::CompressionEncoding};
use tracing::{Instrument, Level, Span, debug, debug_span, field::display, instrument};
use utils::error_in_stream;

#[macro_use]
mod utils;

const MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;
const MAX_PIPELINE_RESPONSE_BYTES: usize = 1024 * 1024;

fn rollback_new_txn_sync(database: &FirestoreDatabase, new_txn_bytes: &[u8]) {
    if !new_txn_bytes.is_empty()
        && let Ok(id) = new_txn_bytes.to_vec().try_into()
    {
        drop(database.rollback_sync(id));
    }
}

struct NewTxnGuard {
    database:  Arc<FirestoreDatabase>,
    txn_bytes: Vec<u8>,
}

impl NewTxnGuard {
    fn new(database: Arc<FirestoreDatabase>, txn_bytes: Vec<u8>) -> Self {
        Self {
            database,
            txn_bytes,
        }
    }

    fn take_bytes(&mut self) -> Vec<u8> {
        mem::take(&mut self.txn_bytes)
    }

    fn rollback(&mut self) {
        let txn_bytes = mem::take(&mut self.txn_bytes);
        rollback_new_txn_sync(&self.database, &txn_bytes);
    }
}

impl Drop for NewTxnGuard {
    fn drop(&mut self) {
        self.rollback();
    }
}

fn take_guard(guard: &Mutex<Option<NewTxnGuard>>) -> Option<NewTxnGuard> {
    guard.lock().unwrap_or_else(PoisonError::into_inner).take()
}

struct BatchGetStream {
    inner:     ReceiverStream<Result<BatchGetDocumentsResponse>>,
    guard:     Arc<Mutex<Option<NewTxnGuard>>>,
    completed: Arc<AtomicBool>,
}

impl tokio_stream::Stream for BatchGetStream {
    type Item = Result<BatchGetDocumentsResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.inner).poll_next(cx);
        if matches!(poll, Poll::Ready(None))
            && self.completed.load(Ordering::Acquire)
            && let Some(mut guard) = take_guard(&self.guard)
        {
            drop(guard.take_bytes());
        }
        poll
    }
}

impl Drop for BatchGetStream {
    fn drop(&mut self) {
        drop(take_guard(&self.guard));
    }
}

fn check_doc_belongs_to_db(doc_ref: &DocumentRef, expected_db: &RootRef) -> Result<()> {
    if &doc_ref.collection_ref.root_ref != expected_db {
        return Err(Status::invalid_argument(format!(
            "document {doc_ref} does not belong to database {expected_db}"
        )));
    }
    Ok(())
}

fn check_write_database(write: &Write, expected_db: &RootRef) -> Result<()> {
    let name = match &write.operation {
        Some(write::Operation::Update(doc)) => &doc.name,
        Some(write::Operation::Delete(name)) => name,
        Some(write::Operation::Transform(trans)) => &trans.document,
        None => return Ok(()),
    };
    let doc_ref: DocumentRef = name.parse()?;
    check_doc_belongs_to_db(&doc_ref, expected_db)
}

fn forward_listen_requests<S>(
    mut req_stream: S,
) -> (
    impl tokio_stream::Stream<Item = ListenRequest> + Send + Unpin + 'static,
    tokio::sync::oneshot::Receiver<Status>,
)
where
    S: tokio_stream::Stream<Item = Result<ListenRequest>> + Send + Unpin + 'static,
{
    let (err_tx, err_rx) = tokio::sync::oneshot::channel::<Status>();
    let mut err_tx = Some(err_tx);
    let clean_requests = futures_util::stream::poll_fn(move |cx| {
        if err_tx.is_none() {
            return Poll::Ready(None);
        }
        match Pin::new(&mut req_stream).poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => Poll::Ready(Some(msg)),
            Poll::Ready(Some(Err(status))) => {
                if let Some(tx) = err_tx.take() {
                    drop(tx.send(status));
                }
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    });
    (Box::pin(clean_requests), err_rx)
}

fn chunk_pipeline_responses(
    mut new_txn_guard: NewTxnGuard,
    results: Vec<Document>,
    read_time: Timestamp,
) -> impl Iterator<Item = Result<ExecutePipelineResponse>> {
    let mut iter = results.into_iter().peekable();
    let mut emitted_empty = false;

    std::iter::from_fn(move || {
        if iter.peek().is_none() {
            if emitted_empty {
                return None;
            }
            emitted_empty = true;
            return Some(Ok(ExecutePipelineResponse {
                transaction: new_txn_guard.take_bytes(),
                results: Vec::new(),
                execution_time: Some(read_time),
                explain_stats: None,
            }));
        }

        emitted_empty = true;
        let transaction = new_txn_guard.take_bytes();
        let mut frame_bytes = ExecutePipelineResponse {
            transaction: transaction.clone(),
            results: Vec::new(),
            execution_time: Some(read_time),
            explain_stats: None,
        }
        .encoded_len();

        let mut chunk = Vec::new();
        while let Some(doc) = iter.peek() {
            let doc_len = doc.encoded_len();
            let entry_bytes = 1 + encoded_len_varint(doc_len as u64) + doc_len;
            if !chunk.is_empty() && frame_bytes + entry_bytes > MAX_PIPELINE_RESPONSE_BYTES {
                break;
            }
            frame_bytes += entry_bytes;
            if let Some(doc) = iter.next() {
                chunk.push(doc);
            }
        }

        Some(Ok(ExecutePipelineResponse {
            transaction,
            results: chunk,
            execution_time: Some(read_time),
            explain_stats: None,
        }))
    })
}

/// Instantiate a tonic service that provides the GRPC access to the given emulated Firestore
/// database.
///
/// Note that the service expects a reference to the Firestore database with a static
/// lifetime as we assume that there will only ever be one instance.
///
/// # Usage
/// ```
/// use emulator_database::{FirestoreConfig, FirestoreProject};
/// use tonic::transport::Server;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = FirestoreConfig::default();
/// let project = FirestoreProject::new(config);
/// // `service` needs a static reference to the `FirestoreProject`
/// let project = Box::leak(Box::new(project));
///
/// let service = emulator_grpc::service(project);
///
/// let server = Server::builder()
///     .add_service(service)
///     .serve("[::1]:0".parse()?);
///
/// // Spawn task or await using: `server.await?;`
/// tokio::spawn(server);
/// # Ok(())
/// # }
/// ```
pub fn service(project: &'static FirestoreProject) -> FirestoreServer<impl Firestore> {
    FirestoreServer::new(FirestoreEmulator { project })
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
}

struct FirestoreEmulator {
    project: &'static FirestoreProject,
}

#[async_trait]
impl Firestore for FirestoreEmulator {
    /// Gets a single document.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn get_document(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<Document>> {
        let GetDocumentRequest {
            name,
            mask,
            consistency_selector,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        let name: DocumentRef = name.parse()?;

        let projection = mask.map(Projection::try_from).transpose()?;

        let doc = self
            .project
            .database(&name.collection_ref.root_ref)
            .await
            .get_doc(&name, consistency_selector.try_into()?)
            .await?
            .ok_or_else(|| Status::not_found(Code::NotFound.description()))?;
        Ok(Response::new(projection.project(&doc)))
    }

    /// Server streaming response type for the BatchGetDocuments method.
    type BatchGetDocumentsStream = BoxStream<'static, Result<BatchGetDocumentsResponse>>;

    /// Gets multiple documents.
    ///
    /// Documents returned by this method are not guaranteed to be returned in the
    /// same order that they were requested.
    #[instrument(level = Level::DEBUG, skip_all, fields(
        count = request.get_ref().documents.len(),
        txn_id,
    ), err)]
    async fn batch_get_documents(
        &self,
        request: Request<BatchGetDocumentsRequest>,
    ) -> Result<Response<Self::BatchGetDocumentsStream>> {
        let BatchGetDocumentsRequest {
            database,
            documents,
            mask,
            consistency_selector,
            request_options,
        } = request.into_inner();

        error_in_stream(async {
            unimplemented_option!(request_options);
            let database = self.project.database(&database.parse()?).await;
            let documents: Vec<DocumentRef> = documents
                .into_iter()
                .map(|name| {
                    let doc_ref: DocumentRef = name.parse()?;
                    check_doc_belongs_to_db(&doc_ref, &database.name)?;
                    Ok::<DocumentRef, Status>(doc_ref)
                })
                .try_collect()?;
            let projection = mask.map(Projection::try_from).transpose()?;

            let (
                // Only used for new transactions.
                new_txn_bytes,
                read_consistency,
            ) = match consistency_selector {
                Some(batch_get_documents_request::ConsistencySelector::NewTransaction(
                    txn_opts,
                )) => {
                    let id = database.new_txn(txn_opts).await?;
                    debug!("started new transaction");
                    (Vec::from(id), ReadConsistency::Transaction(id))
                }
                s => (vec![], s.try_into()?),
            };
            let shared_guard = Arc::new(Mutex::new(Some(NewTxnGuard::new(
                Arc::clone(&database),
                new_txn_bytes.clone(),
            ))));
            debug!(?read_consistency);
            if let ReadConsistency::Transaction(id) = read_consistency {
                Span::current().record("txn_id", display(id));
            }

            let read_time = match database.get_read_time(read_consistency).await {
                Ok(read_time) => read_time,
                Err(err) => {
                    if let Some(mut guard) = take_guard(&shared_guard) {
                        guard.rollback();
                    }
                    return Err(err.into());
                }
            };

            let worker_guard = Arc::clone(&shared_guard);
            let completed = Arc::new(AtomicBool::new(false));
            let worker_completed = Arc::clone(&completed);
            let (tx, rx) = mpsc::channel(16);
            tokio::spawn(
                async move {
                    let mut new_transaction = new_txn_bytes;
                    if documents.is_empty() {
                        if !new_transaction.is_empty() {
                            let msg = Ok(BatchGetDocumentsResponse {
                                result:      None,
                                read_time:   Some(read_time),
                                transaction: mem::take(&mut new_transaction),
                            });
                            if tx.send(msg).await.is_err() {
                                if let Some(mut guard) = take_guard(&worker_guard) {
                                    guard.rollback();
                                }
                                return;
                            }
                        }
                        worker_completed.store(true, Ordering::Release);
                        return;
                    }
                    for name in documents {
                        use batch_get_documents_response::Result::*;
                        let doc_result = tokio::select! {
                            biased;
                            _ = tx.closed() => {
                                if let Some(mut guard) = take_guard(&worker_guard) {
                                    guard.rollback();
                                }
                                return;
                            }
                            res = database.get_doc(&name, read_consistency) => res,
                        };
                        let msg = match doc_result {
                            Ok(doc) => Ok(BatchGetDocumentsResponse {
                                result:      Some(match doc {
                                    None => Missing(name.to_string()),
                                    Some(doc) => Found(projection.project(&doc)),
                                }),
                                read_time:   Some(read_time),
                                transaction: mem::take(&mut new_transaction),
                            }),
                            Err(err) => {
                                if let Some(mut guard) = take_guard(&worker_guard) {
                                    guard.rollback();
                                }
                                drop(tx.send(Err(Status::from(err))).await);
                                return;
                            }
                        };
                        if tx.send(msg).await.is_err() {
                            if let Some(mut guard) = take_guard(&worker_guard) {
                                guard.rollback();
                            }
                            return;
                        }
                    }
                    worker_completed.store(true, Ordering::Release);
                }
                .instrument(debug_span!("worker")),
            );
            Ok(BatchGetStream {
                inner: ReceiverStream::new(rx),
                guard: shared_guard,
                completed,
            })
        })
        .await
    }

    /// Commits a transaction, while optionally updating documents.
    #[instrument(level = Level::DEBUG, skip_all, fields(
        count = request.get_ref().writes.len(),
        txn_id,
    ), err)]
    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>> {
        let CommitRequest {
            database,
            writes,
            transaction,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        let database = self.project.database(&database.parse()?).await;
        for write in &writes {
            check_write_database(write, &database.name)?;
        }

        let txn_id = if transaction.is_empty() {
            None
        } else {
            let txn_id = transaction.try_into()?;
            Span::current().record("txn_id", display(txn_id));
            debug!(?txn_id);
            Some(txn_id)
        };
        let (commit_time, write_results) = database.commit(writes, txn_id).await?;

        Ok(Response::new(CommitResponse {
            write_results,
            commit_time: Some(commit_time),
        }))
    }

    /// Creates a new document.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn create_document(
        &self,
        request: Request<CreateDocumentRequest>,
    ) -> Result<Response<Document>> {
        let CreateDocumentRequest {
            parent,
            collection_id,
            document_id,
            document,
            mask,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        if collection_id.is_empty() || collection_id.contains('/') {
            return Err(Status::invalid_argument(
                "collection_id must be a single non-empty segment",
            ));
        }
        if document_id.is_empty() || document_id.contains('/') {
            return Err(Status::invalid_argument(
                "document_id must be a single non-empty segment",
            ));
        }
        let doc = mandatory!(document);
        if !doc.name.is_empty() {
            return Err(Status::invalid_argument(
                "document.name must not be set on CreateDocument",
            ));
        }
        let parent_ref: Ref = parent.parse()?;
        if matches!(parent_ref, Ref::Collection(_)) {
            return Err(Status::invalid_argument(
                "parent must be a database root or document path",
            ));
        }
        let col_ref: CollectionRef = format!("{parent_ref}/{collection_id}").parse()?;
        let database = self.project.database(&col_ref.root_ref).await;
        let created = database
            .create_doc(&col_ref, &document_id, doc.fields, mask)
            .await?;
        Ok(Response::new(created))
    }

    /// Lists documents.
    #[instrument(level = Level::DEBUG, skip_all, fields(request = ?request.get_ref()), err)]
    async fn list_documents(
        &self,
        request: Request<ListDocumentsRequest>,
    ) -> Result<Response<ListDocumentsResponse>> {
        let ListDocumentsRequest {
            parent,
            collection_id,
            page_size,
            page_token,
            order_by,
            mask,
            show_missing,
            consistency_selector,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        let parent: Ref = parent.parse()?;
        let database = self.project.database(parent.root()).await;
        let (documents, next_page_token) = database
            .list_docs(
                parent,
                collection_id,
                page_size,
                page_token,
                order_by,
                mask,
                show_missing,
                consistency_selector.try_into()?,
            )
            .await?;

        Ok(Response::new(ListDocumentsResponse {
            documents,
            next_page_token,
        }))
    }

    /// Updates or inserts a document.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn update_document(
        &self,
        request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>> {
        let UpdateDocumentRequest {
            document,
            update_mask,
            mask,
            current_document,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        let doc = mandatory!(document);
        let doc_ref: DocumentRef = doc.name.parse()?;
        let database = self
            .project
            .database(&doc_ref.collection_ref.root_ref)
            .await;
        let updated = database
            .update_doc(&doc_ref, doc.fields, update_mask, current_document, mask)
            .await?;
        Ok(Response::new(updated))
    }

    /// Deletes a document.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn delete_document(
        &self,
        request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<Empty>> {
        let DeleteDocumentRequest {
            name,
            current_document,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        let doc_ref: DocumentRef = name.parse()?;
        let database = self
            .project
            .database(&doc_ref.collection_ref.root_ref)
            .await;
        database.delete_doc(&doc_ref, current_document).await?;
        Ok(Response::new(Empty {}))
    }

    /// Starts a new transaction.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>> {
        let BeginTransactionRequest {
            database,
            options,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);

        let txn_id = self
            .project
            .database(&database.parse()?)
            .await
            .new_txn(options.unwrap_or(TransactionOptions::READ_WRITE))
            .await?;

        debug!(?txn_id);
        Ok(Response::new(BeginTransactionResponse {
            transaction: txn_id.into(),
        }))
    }

    /// Rolls back a transaction.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<Empty>> {
        let RollbackRequest {
            database,
            transaction,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);
        let database = self.project.database(&database.parse()?).await;
        database.rollback(transaction.try_into()?).await?;
        Ok(Response::new(Empty {}))
    }

    /// Server streaming response type for the RunQuery method.
    type RunQueryStream = BoxStream<'static, Result<RunQueryResponse>>;

    /// Runs a query.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<Self::RunQueryStream>> {
        let RunQueryRequest {
            parent,
            explain_options,
            query_type,
            consistency_selector,
            request_options,
        } = request.into_inner();

        error_in_stream(async {
            unimplemented_option!(explain_options);
            unimplemented_option!(request_options);
            let run_query_request::QueryType::StructuredQuery(query) = mandatory!(query_type);

            let parent: Ref = parent.parse()?;

            let database = self.project.database(parent.root()).await;

            let (
                // Only used for new transactions.
                new_transaction,
                read_consistency,
            ) = match consistency_selector {
                Some(run_query_request::ConsistencySelector::NewTransaction(txn_opts)) => {
                    let id = database.new_txn(txn_opts).await?;
                    debug!("started new transaction");
                    (id.into(), ReadConsistency::Transaction(id))
                }
                s => (vec![], s.try_into()?),
            };
            let mut new_txn_guard = NewTxnGuard::new(Arc::clone(&database), new_transaction);
            let (read_time, docs) = match async {
                let mut query = Query::from_structured(
                    parent,
                    query,
                    read_consistency,
                    database.enterprise_edition(),
                )?;
                database.run_query(&mut query).await
            }
            .await
            {
                Ok(res) => res,
                Err(err) => {
                    new_txn_guard.rollback();
                    return Err(err.into());
                }
            };
            // The docs of RunQueryResponse::read_time say:
            // If the query returns no results, a response with `read_time` and
            // no `document` will be sent, and this represents the time at which
            // the query was run. So if we find no documents, we will send one
            // `RunQueryResponse` with `document: None` anyway.
            let docs = docs
                .into_iter()
                .map(Some)
                .pad_using(1, |_| None)
                .map(move |document| {
                    Ok(RunQueryResponse {
                        // only send the transaction ID in the first element (if any):
                        transaction: new_txn_guard.take_bytes(),
                        document,
                        read_time: Some(read_time),
                        skipped_results: 0,
                        explain_metrics: None,
                        continuation_selector: None,
                    })
                });

            Ok(tokio_stream::iter(docs))
        })
        .await
    }

    /// Server streaming response type for the ExecutePipeline method.
    type ExecutePipelineStream = BoxStream<'static, Result<ExecutePipelineResponse>>;

    /// Executes a pipeline query.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn execute_pipeline(
        &self,
        request: Request<ExecutePipelineRequest>,
    ) -> Result<Response<Self::ExecutePipelineStream>, Status> {
        let ExecutePipelineRequest {
            database,
            pipeline_type,
            consistency_selector,
            auto_commit_transaction,
            request_options,
        } = request.into_inner();

        error_in_stream(async {
            unimplemented_bool!(auto_commit_transaction);
            unimplemented_option!(request_options);
            let execute_pipeline_request::PipelineType::StructuredPipeline(pipeline) =
                mandatory!(pipeline_type);

            let root_ref: RootRef = database.parse()?;
            let database = self.project.database(&root_ref).await;

            // TODO: Deze code komt een paar keer in vergelijkbare vorm terug. Kijken of we dit
            // herbruikbaar kunnen maken.
            let (new_transaction, read_consistency) = match consistency_selector {
                Some(execute_pipeline_request::ConsistencySelector::NewTransaction(txn_opts)) => {
                    let id = database.new_txn(txn_opts).await?;
                    debug!("started new transaction");
                    (id.into(), ReadConsistency::Transaction(id))
                }
                s => (vec![], s.try_into()?),
            };
            let mut new_txn_guard = NewTxnGuard::new(Arc::clone(&database), new_transaction);

            let (results, read_time) = match async {
                let results =
                    pipeline::execute(&database, &root_ref, pipeline, read_consistency).await?;
                let read_time = database.get_read_time(read_consistency).await?;
                Ok::<_, GenericDatabaseError>((results, read_time))
            }
            .await
            {
                Ok(res) => res,
                Err(err) => {
                    new_txn_guard.rollback();
                    return Err(err.into());
                }
            };

            Ok(tokio_stream::iter(chunk_pipeline_responses(
                new_txn_guard,
                results,
                read_time,
            )))
        })
        .await
    }

    /// Server streaming response type for the RunAggregationQuery method.
    type RunAggregationQueryStream = BoxStream<'static, Result<RunAggregationQueryResponse>>;

    /// Runs an aggregation query.
    ///
    /// Rather than producing [Document][google.firestore.v1.Document] results like
    /// [Firestore.RunQuery][google.firestore.v1.Firestore.RunQuery], this API
    /// allows running an aggregation to produce a series of
    /// [AggregationResult][google.firestore.v1.AggregationResult] server-side.
    ///
    /// High-Level Example:
    ///
    /// ```sql
    /// -- Return the number of documents in table given a filter.
    /// SELECT COUNT(*) FROM ( SELECT * FROM k where a = true );
    /// ```
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn run_aggregation_query(
        &self,
        request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<Self::RunAggregationQueryStream>> {
        let RunAggregationQueryRequest {
            parent,
            explain_options,
            query_type,
            consistency_selector,
            request_options,
        } = request.into_inner();

        error_in_stream(async {
            unimplemented_option!(explain_options);
            unimplemented_option!(request_options);

            let run_aggregation_query_request::QueryType::StructuredAggregationQuery(agg_query) =
                mandatory!(query_type);

            let structured_aggregation_query::QueryType::StructuredQuery(query) =
                mandatory!(agg_query.query_type);

            let parent: Ref = parent.parse()?;

            let database = self.project.database(parent.root()).await;

            let (
                // Only used for new transactions.
                new_transaction,
                read_consistency,
            ) = match consistency_selector {
                Some(run_aggregation_query_request::ConsistencySelector::NewTransaction(
                    txn_opts,
                )) => {
                    let id = database.new_txn(txn_opts).await?;
                    debug!("started new transaction");
                    (id.into(), ReadConsistency::Transaction(id))
                }
                s => (vec![], s.try_into()?),
            };
            let mut new_txn_guard = NewTxnGuard::new(Arc::clone(&database), new_transaction);

            debug!(?read_consistency);
            let (read_time, aggregate_fields) = match database
                .run_aggregation_query(parent, query, agg_query.aggregations, read_consistency)
                .await
            {
                Ok(res) => res,
                Err(err) => {
                    new_txn_guard.rollback();
                    return Err(err.into());
                }
            };

            Ok(futures_util::stream::once(async move {
                let transaction = new_txn_guard.take_bytes();
                Ok(RunAggregationQueryResponse {
                    result: Some(AggregationResult { aggregate_fields }),
                    explain_metrics: None,
                    transaction,
                    read_time: Some(read_time),
                })
            }))
        })
        .await
    }

    /// Partitions a query by returning partition cursors that can be used to run
    /// the query in parallel. The returned partition cursors are split points that
    /// can be used by RunQuery as starting/end points for the query results.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn partition_query(
        &self,
        _request: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionQueryResponse>> {
        unimplemented!("partition_query");
    }

    /// Server streaming response type for the Write method.
    type WriteStream = tonic::Streaming<WriteResponse>;

    /// Streams batches of document updates and deletes, in order. This method is
    /// only available via gRPC or WebChannel (not REST).
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn write(
        &self,
        _request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStream>> {
        unimplemented!("write");
    }

    /// Server streaming response type for the Listen method.
    type ListenStream = BoxStream<'static, Result<ListenResponse>>;

    /// Listens to changes. This method is only available via gRPC or WebChannel
    /// (not REST).
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn listen(
        &self,
        request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>> {
        let (clean_requests, err_rx) = forward_listen_requests(request.into_inner());
        let err_stream = futures_util::stream::once(err_rx).filter_map(|res| res.ok().map(Err));
        let stream = self
            .project
            .listen(clean_requests)
            .map_err(Status::from)
            .chain(err_stream);
        Ok(Response::new(Box::pin(stream)))
    }

    /// Lists all the collection IDs underneath a document.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn list_collection_ids(
        &self,
        request: Request<ListCollectionIdsRequest>,
    ) -> Result<Response<ListCollectionIdsResponse>> {
        let ListCollectionIdsRequest {
            parent,
            page_size,
            page_token,
            consistency_selector,
            request_options,
        } = request.into_inner();
        unimplemented_option!(request_options);
        if page_size < 0 {
            return Err(Status::invalid_argument("page_size must be non-negative"));
        }
        let parent: Ref = parent.parse()?;
        let collection_ids = self
            .project
            .database(parent.root())
            .await
            .get_collection_ids(&(parent))
            .await?
            .into_iter()
            .collect_vec();
        unimplemented_bool!(page_size != 0 && (page_size as usize) < collection_ids.len());
        unimplemented_collection!(page_token);
        unimplemented_option!(consistency_selector);

        Ok(Response::new(ListCollectionIdsResponse {
            collection_ids,
            next_page_token: "".to_string(),
        }))
    }

    /// Applies a batch of write operations.
    ///
    /// The BatchWrite method does not apply the write operations atomically
    /// and can apply them out of order. Method does not allow more than one write
    /// per document. Each write succeeds or fails independently. See the
    /// [BatchWriteResponse][google.firestore.v1.BatchWriteResponse] for the
    /// success status of each write.
    ///
    /// If you require an atomically applied set of writes, use
    /// [Commit][google.firestore.v1.Firestore.Commit] instead.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>> {
        let BatchWriteRequest {
            database,
            writes,
            labels,
            request_options,
        } = request.into_inner();
        unimplemented_collection!(labels);
        unimplemented_option!(request_options);

        let database = self.project.database(&database.parse()?).await;

        let (status, write_results): (Vec<_>, Vec<_>) =
            join_all(writes.into_iter().map(async |write| {
                if let Err(err) = check_write_database(&write, &database.name) {
                    return (
                        googleapis::google::rpc::Status {
                            code:    err.code() as _,
                            message: err.message().to_string(),
                            details: vec![],
                        },
                        Default::default(),
                    );
                }
                let result = database
                    .write(write)
                    .await
                    .map_err(googleapis::google::rpc::Status::from);
                match result {
                    Ok(write_result) => (Default::default(), write_result),
                    Err(status) => (status, Default::default()),
                }
            }))
            .await
            .into_iter()
            .unzip();

        Ok(Response::new(BatchWriteResponse {
            write_results,
            status,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use emulator_database::{FirestoreConfig, FirestoreDatabase, FirestoreProject};
    use googleapis::{
        fields,
        google::firestore::v1::{
            BatchGetDocumentsRequest, BatchWriteRequest, CommitRequest, CreateDocumentRequest,
            Document, ExecutePipelineRequest, ListCollectionIdsRequest, ListenRequest, Pipeline,
            RunAggregationQueryRequest, RunQueryRequest, StructuredAggregationQuery,
            StructuredPipeline, StructuredQuery, TransactionOptions, Value, Write,
            batch_get_documents_request, execute_pipeline_request,
            firestore_server::Firestore,
            pipeline::Stage,
            run_aggregation_query_request, run_query_request,
            structured_aggregation_query::{self, Aggregation, aggregation},
            structured_query::CollectionSelector,
            transaction_options::{self, ReadWrite},
            write,
        },
    };
    use tokio::time::timeout;
    use tokio_stream::StreamExt;
    use tonic::{Code, Request, Status};

    use super::{FirestoreEmulator, forward_listen_requests};

    const DB_ROOT: &str = "projects/p/databases/(default)";
    const DOCS_ROOT: &str = "projects/p/databases/(default)/documents";
    const OTHER_DOCS_ROOT: &str = "projects/p/databases/other-db/documents";

    fn leaked_project(default_enterprise: bool) -> &'static FirestoreProject {
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
            long_contention_timeout: false,
            default_enterprise,
        })))
    }

    fn rw_opts() -> TransactionOptions {
        TransactionOptions::READ_WRITE
    }

    fn update_write(name: String, v: i64) -> Write {
        Write {
            update_mask: None,
            update_transforms: Vec::new(),
            current_document: None,
            operation: Some(write::Operation::Update(Document {
                name,
                fields: fields!({ "v": v }),
                create_time: None,
                update_time: None,
            })),
        }
    }

    #[tokio::test]
    async fn cross_database_document_paths_return_invalid_argument_without_panicking() {
        let project = leaked_project(false);
        let emu = FirestoreEmulator { project };

        // 1. BatchGetDocuments with doc from `other-db`
        let mut bg_stream = emu
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: DB_ROOT.to_string(),
                documents: vec![format!("{OTHER_DOCS_ROOT}/c/d1")],
                mask: None,
                consistency_selector: None,
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        let bg_err = bg_stream.next().await.unwrap().unwrap_err();
        assert_eq!(bg_err.code(), Code::InvalidArgument);

        // 2. Commit with write targeting `other-db`
        let commit_err = emu
            .commit(Request::new(CommitRequest {
                database: DB_ROOT.to_string(),
                writes: vec![update_write(format!("{OTHER_DOCS_ROOT}/c/d1"), 1)],
                transaction: Vec::new(),
                request_options: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(commit_err.code(), Code::InvalidArgument);

        // 3. BatchWrite with one valid write and one targeting `other-db`
        let bw_resp = emu
            .batch_write(Request::new(BatchWriteRequest {
                database: DB_ROOT.to_string(),
                writes: vec![
                    update_write(format!("{DOCS_ROOT}/c/ok"), 1),
                    update_write(format!("{OTHER_DOCS_ROOT}/c/bad"), 2),
                ],
                labels: HashMap::new(),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(bw_resp.status.len(), 2);
        assert_eq!(bw_resp.status[0].code, Code::Ok as i32);
        assert_eq!(bw_resp.status[1].code, Code::InvalidArgument as i32);
    }

    #[tokio::test]
    async fn batch_get_documents_empty_docs_rejects_invalid_txn_and_cleans_up_new_txn() {
        let project = leaked_project(false);
        let emu = FirestoreEmulator { project };

        // Invalid 8-byte transaction ID must surface InvalidArgument even when `documents` is
        // empty.
        let mut bad_stream = emu
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: DB_ROOT.to_string(),
                documents: Vec::new(),
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::Transaction(vec![
                        99, 0, 0, 0, 0, 0, 0, 0,
                    ]),
                ),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        let err = bad_stream.next().await.unwrap().unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // Unpolled NewTransaction stream with empty documents cleans up the transaction on drop.
        let db = project.database(&DB_ROOT.parse().unwrap()).await;
        let t1 = db.new_txn(rw_opts()).await.unwrap();
        let t1_bytes = Vec::from(t1);
        db.rollback(t1).await.unwrap();

        let new_txn_stream = emu
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: DB_ROOT.to_string(),
                documents: Vec::new(),
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::NewTransaction(
                        TransactionOptions {
                            mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                                retry_transaction: t1_bytes.clone(),
                                ..Default::default()
                            })),
                        },
                    ),
                ),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        drop(new_txn_stream);

        let retry = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                    retry_transaction: t1_bytes,
                    ..Default::default()
                })),
            })
            .await;
        assert!(
            retry.is_ok(),
            "unpolled empty-docs NewTransaction must roll back on drop"
        );
    }

    #[tokio::test]
    async fn create_document_rejects_slash_or_empty_document_id() {
        let project = leaked_project(false);
        let emu = FirestoreEmulator { project };
        let db = project.database(&DB_ROOT.parse().unwrap()).await;

        for bad_id in ["", "a/b", "a/b/c"] {
            let err = emu
                .create_document(Request::new(CreateDocumentRequest {
                    parent: DOCS_ROOT.to_string(),
                    collection_id: "users".to_string(),
                    document_id: bad_id.to_string(),
                    document: Some(Document {
                        name: String::new(),
                        fields: fields!({ "x": 1i64 }),
                        create_time: None,
                        update_time: None,
                    }),
                    mask: None,
                    request_options: None,
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::InvalidArgument, "document_id={bad_id:?}");
        }

        let nested_ref = format!("{DOCS_ROOT}/users/a/b/c").parse().unwrap();
        assert!(
            db.get_doc(
                &nested_ref,
                emulator_database::read_consistency::ReadConsistency::Default
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn list_collection_ids_rejects_negative_page_size_with_invalid_argument() {
        let project = leaked_project(false);
        let emu = FirestoreEmulator { project };

        let err = emu
            .list_collection_ids(Request::new(ListCollectionIdsRequest {
                parent: DOCS_ROOT.to_string(),
                page_size: -1,
                page_token: String::new(),
                consistency_selector: None,
                request_options: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        let db = project.database(&DB_ROOT.parse().unwrap()).await;
        let doc_ref = format!("{DOCS_ROOT}/col/d1").parse().unwrap();
        db.update_doc(&doc_ref, fields!({ "x": 1i64 }), None, None, None)
            .await
            .unwrap();

        let err = emu
            .list_collection_ids(Request::new(ListCollectionIdsRequest {
                parent: DOCS_ROOT.to_string(),
                page_size: -5,
                page_token: String::new(),
                consistency_selector: None,
                request_options: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn forward_listen_requests_stops_and_surfaces_stream_errors() {
        let input = futures_util::stream::iter([
            Ok(ListenRequest {
                database: DB_ROOT.to_string(),
                labels: HashMap::new(),
                target_change: None,
                request_options: None,
            }),
            Err(Status::out_of_range("frame exceeds limit")),
            Ok(ListenRequest {
                database: DB_ROOT.to_string(),
                labels: HashMap::new(),
                target_change: None,
                request_options: None,
            }),
        ]);
        let (clean, err_rx) = forward_listen_requests(input);
        let items: Vec<_> = clean.collect().await;
        assert_eq!(items.len(), 1);
        let err = err_rx.await.expect("error status sent on stream failure");
        assert_eq!(err.code(), Code::OutOfRange);
    }

    async fn seed_blocked_locks(
        db: &Arc<FirestoreDatabase>,
    ) -> (
        emulator_database::reference::DocumentRef,
        impl Future<Output = Result<(), emulator_database::GenericDatabaseError>> + '_,
        Vec<u8>,
    ) {
        let d1 = format!("{DOCS_ROOT}/locks/a_d1").parse().unwrap();
        let d2 = format!("{DOCS_ROOT}/locks/b_d2").parse().unwrap();
        let blocker_doc = format!("{DOCS_ROOT}/locks/z_blocker").parse().unwrap();
        for doc in [&d1, &d2, &blocker_doc] {
            db.update_doc(doc, fields!({ "v": 1i64 }), None, None, None)
                .await
                .unwrap();
        }

        let hold_id = db.new_txn(rw_opts()).await.unwrap();
        let _held = db
            .get_doc(
                &blocker_doc,
                emulator_database::read_consistency::ReadConsistency::Transaction(hold_id),
            )
            .await
            .unwrap();

        let blocker_txn = db.new_txn(rw_opts()).await.unwrap();
        let mut blocker_fut = Box::pin(async move {
            db.commit(
                vec![
                    update_write(d2.to_string(), 2),
                    update_write(blocker_doc.to_string(), 2),
                ],
                Some(blocker_txn),
            )
            .await
            .map(|_| ())
        });
        assert!(futures_util::poll!(&mut blocker_fut).is_pending());
        (d1, blocker_fut, Vec::from(hold_id))
    }

    fn collection_query(col: &str) -> StructuredQuery {
        StructuredQuery {
            from: vec![CollectionSelector {
                collection_id:   col.to_string(),
                all_descendants: false,
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn cancelled_new_transaction_rpcs_release_locks_and_clean_up() {
        // 1. Cancelled in-flight `run_query`, `run_aggregation_query`, `execute_pipeline`, and
        //    `commit` futures + pre-poll stream drops release `locks/a_d1` immediately.
        for kind in ["run_query", "run_agg", "execute_pipeline", "commit"] {
            let enterprise = kind == "execute_pipeline";
            let project = leaked_project(enterprise);
            let db = project.database(&DB_ROOT.parse().unwrap()).await;
            if enterprise {
                db.set_optimistic_concurrency(false);
            }
            let emu = FirestoreEmulator { project };
            let (d1, blocker_fut, hold_bytes) = seed_blocked_locks(&db).await;

            match kind {
                "run_query" => {
                    let mut fut = Box::pin(emu.run_query(Request::new(RunQueryRequest {
                        parent: DOCS_ROOT.to_string(),
                        explain_options: None,
                        query_type: Some(run_query_request::QueryType::StructuredQuery(
                            collection_query("locks"),
                        )),
                        consistency_selector: Some(
                            run_query_request::ConsistencySelector::NewTransaction(rw_opts()),
                        ),
                        request_options: None,
                    })));
                    assert!(futures_util::poll!(&mut fut).is_pending());
                    drop(fut);
                }
                "run_agg" => {
                    let mut fut = Box::pin(emu.run_aggregation_query(Request::new(
                        RunAggregationQueryRequest {
                            parent: DOCS_ROOT.to_string(),
                            explain_options: None,
                            query_type: Some(
                                run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                                    StructuredAggregationQuery {
                                        aggregations: vec![Aggregation {
                                            alias:    "n".to_string(),
                                            operator: Some(aggregation::Operator::Count(
                                                aggregation::Count { up_to: None },
                                            )),
                                        }],
                                        query_type: Some(
                                            structured_aggregation_query::QueryType::StructuredQuery(
                                                collection_query("locks"),
                                            ),
                                        ),
                                    },
                                ),
                            ),
                            consistency_selector: Some(
                                run_aggregation_query_request::ConsistencySelector::NewTransaction(
                                    rw_opts(),
                                ),
                            ),
                            request_options: None,
                        },
                    )));
                    assert!(futures_util::poll!(&mut fut).is_pending());
                    drop(fut);
                }
                "execute_pipeline" => {
                    let mut fut =
                        Box::pin(emu.execute_pipeline(Request::new(ExecutePipelineRequest {
                            database: DB_ROOT.to_string(),
                            pipeline_type: Some(
                                execute_pipeline_request::PipelineType::StructuredPipeline(
                                    StructuredPipeline {
                                        pipeline: Some(Pipeline {
                                            stages: vec![Stage {
                                                name:    "collection".to_string(),
                                                args:    vec![Value::reference(
                                                    "/locks".to_string(),
                                                )],
                                                options: HashMap::new(),
                                            }],
                                        }),
                                        options:  HashMap::new(),
                                    },
                                ),
                            ),
                            consistency_selector: Some(
                                execute_pipeline_request::ConsistencySelector::NewTransaction(
                                    rw_opts(),
                                ),
                            ),
                            auto_commit_transaction: false,
                            request_options: None,
                        })));
                    assert!(futures_util::poll!(&mut fut).is_pending());
                    drop(fut);
                }
                "commit" => {
                    let mut fut = Box::pin(emu.commit(Request::new(CommitRequest {
                        database: DB_ROOT.to_string(),
                        writes: vec![
                            update_write(d1.to_string(), 9),
                            update_write(format!("{DOCS_ROOT}/locks/b_d2"), 9),
                        ],
                        transaction: Vec::new(),
                        request_options: None,
                    })));
                    assert!(futures_util::poll!(&mut fut).is_pending());
                    drop(fut);
                }
                _ => unreachable!(),
            }

            db.rollback(hold_bytes.try_into().unwrap()).await.unwrap();
            blocker_fut.await.unwrap();
            tokio::task::yield_now().await;

            timeout(
                Duration::from_millis(250),
                db.update_doc(&d1, fields!({ "v": 42i64 }), None, None, None),
            )
            .await
            .expect("cancelled RPC must not leak lock")
            .unwrap();
        }

        // 2. Dropping `batch_get_documents` stream while worker is blocked on `b_d2` cancels wait
        //    via `tx.closed()` and rolls back `a_d1` WITHOUT releasing `b_d2` first.
        {
            let project = leaked_project(false);
            let db = project.database(&DB_ROOT.parse().unwrap()).await;
            let emu = FirestoreEmulator { project };
            let (d1, blocker_fut, hold_bytes) = seed_blocked_locks(&db).await;

            let stream = emu
                .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                    database: DB_ROOT.to_string(),
                    documents: vec![d1.to_string(), format!("{DOCS_ROOT}/locks/b_d2")],
                    mask: None,
                    consistency_selector: Some(
                        batch_get_documents_request::ConsistencySelector::NewTransaction(rw_opts()),
                    ),
                    request_options: None,
                }))
                .await
                .unwrap()
                .into_inner();
            tokio::task::yield_now().await;
            drop(stream);
            tokio::task::yield_now().await;

            // `b_d2` is STILL blocked by `blocker_fut`, yet `d1` must unlock immediately.
            timeout(
                Duration::from_millis(250),
                db.update_doc(&d1, fields!({ "v": 77i64 }), None, None, None),
            )
            .await
            .expect("dropped batch_get stream must wake worker and rollback d1 immediately")
            .unwrap();

            db.rollback(hold_bytes.try_into().unwrap()).await.unwrap();
            blocker_fut.await.unwrap();
        }

        // 3. Dropping returned streaming response before polling item 0 rolls back NewTransaction.
        {
            let project = leaked_project(true);
            let db = project.database(&DB_ROOT.parse().unwrap()).await;
            db.set_optimistic_concurrency(false);
            let emu = FirestoreEmulator { project };
            let d1: emulator_database::reference::DocumentRef =
                format!("{DOCS_ROOT}/prepoll/d1").parse().unwrap();
            db.update_doc(&d1, fields!({ "v": 1i64 }), None, None, None)
                .await
                .unwrap();

            let rq_stream = emu
                .run_query(Request::new(RunQueryRequest {
                    parent: DOCS_ROOT.to_string(),
                    explain_options: None,
                    query_type: Some(run_query_request::QueryType::StructuredQuery(
                        collection_query("prepoll"),
                    )),
                    consistency_selector: Some(
                        run_query_request::ConsistencySelector::NewTransaction(rw_opts()),
                    ),
                    request_options: None,
                }))
                .await
                .unwrap()
                .into_inner();
            drop(rq_stream);

            let ep_stream = emu
                .execute_pipeline(Request::new(ExecutePipelineRequest {
                    database: DB_ROOT.to_string(),
                    pipeline_type: Some(
                        execute_pipeline_request::PipelineType::StructuredPipeline(
                            StructuredPipeline {
                                pipeline: Some(Pipeline {
                                    stages: vec![Stage {
                                        name:    "collection".to_string(),
                                        args:    vec![Value::reference("/prepoll".to_string())],
                                        options: HashMap::new(),
                                    }],
                                }),
                                options:  HashMap::new(),
                            },
                        ),
                    ),
                    consistency_selector: Some(
                        execute_pipeline_request::ConsistencySelector::NewTransaction(rw_opts()),
                    ),
                    auto_commit_transaction: false,
                    request_options: None,
                }))
                .await
                .unwrap()
                .into_inner();
            drop(ep_stream);

            let agg_stream = emu
                .run_aggregation_query(Request::new(RunAggregationQueryRequest {
                    parent: DOCS_ROOT.to_string(),
                    explain_options: None,
                    query_type: Some(
                        run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                            StructuredAggregationQuery {
                                aggregations: vec![Aggregation {
                                    alias:    "n".to_string(),
                                    operator: Some(aggregation::Operator::Count(
                                        aggregation::Count { up_to: None },
                                    )),
                                }],
                                query_type:   Some(
                                    structured_aggregation_query::QueryType::StructuredQuery(
                                        collection_query("prepoll"),
                                    ),
                                ),
                            },
                        ),
                    ),
                    consistency_selector: Some(
                        run_aggregation_query_request::ConsistencySelector::NewTransaction(
                            rw_opts(),
                        ),
                    ),
                    request_options: None,
                }))
                .await
                .unwrap()
                .into_inner();
            drop(agg_stream);
            tokio::task::yield_now().await;

            timeout(
                Duration::from_millis(250),
                db.update_doc(&d1, fields!({ "v": 2i64 }), None, None, None),
            )
            .await
            .expect("unpolled dropped streams must rollback NewTransaction locks")
            .unwrap();
        }
    }

    #[tokio::test]
    async fn batch_get_documents_mid_stream_drop_after_first_item_rolls_back_new_transaction() {
        let project = leaked_project(false);
        let db = project.database(&DB_ROOT.parse().unwrap()).await;
        let emu = FirestoreEmulator { project };

        let mut doc_names = Vec::new();
        for i in 0..20 {
            let name = format!("{DOCS_ROOT}/c/d{i:02}");
            let doc_ref = name.parse().unwrap();
            db.update_doc(&doc_ref, fields!({ "v": i as i64 }), None, None, None)
                .await
                .unwrap();
            doc_names.push(name);
        }

        let mut stream = emu
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: DB_ROOT.to_string(),
                documents: doc_names,
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::NewTransaction(rw_opts()),
                ),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let first = stream
            .next()
            .await
            .expect("expected first BatchGetDocumentsResponse")
            .unwrap();
        let txn_bytes = first.transaction;
        assert!(!txn_bytes.is_empty());

        drop(stream);
        tokio::task::yield_now().await;

        let retry = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                    retry_transaction: txn_bytes,
                    ..Default::default()
                })),
            })
            .await;
        assert!(
            retry.is_ok(),
            "mid-stream cancelled BatchGetDocuments must roll back NewTransaction, got {retry:?}"
        );
    }

    #[tokio::test]
    async fn cancelled_new_transaction_allows_immediate_retry_without_yielding() {
        let project = leaked_project(false);
        let db = project.database(&DB_ROOT.parse().unwrap()).await;
        let emu = FirestoreEmulator { project };

        let d1 = format!("{DOCS_ROOT}/c/d1").parse().unwrap();
        db.update_doc(&d1, fields!({ "v": 1i64 }), None, None, None)
            .await
            .unwrap();

        let t1 = db.new_txn(rw_opts()).await.unwrap();
        let t1_bytes = Vec::from(t1);
        db.rollback(t1).await.unwrap();

        let rq_stream = emu
            .run_query(Request::new(RunQueryRequest {
                parent: DOCS_ROOT.to_string(),
                explain_options: None,
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    collection_query("c"),
                )),
                consistency_selector: Some(run_query_request::ConsistencySelector::NewTransaction(
                    TransactionOptions {
                        mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                            retry_transaction: t1_bytes.clone(),
                            ..Default::default()
                        })),
                    },
                )),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        drop(rq_stream);

        let retry = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                    retry_transaction: t1_bytes,
                    ..Default::default()
                })),
            })
            .await;
        assert!(
            retry.is_ok(),
            "dropping unpolled NewTransaction stream must synchronously free retry_transaction \
             ID, got {retry:?}"
        );
    }

    #[tokio::test]
    async fn batch_get_documents_double_rollback_does_not_destroy_subsequent_retry_transaction() {
        let project = leaked_project(false);
        let db = project.database(&DB_ROOT.parse().unwrap()).await;
        let emu = FirestoreEmulator { project };

        let d1 = format!("{DOCS_ROOT}/c/d1").parse().unwrap();
        db.update_doc(&d1, fields!({ "v": 1i64 }), None, None, None)
            .await
            .unwrap();

        let t1 = db.new_txn(rw_opts()).await.unwrap();
        let t1_bytes = Vec::from(t1);
        db.rollback(t1).await.unwrap();

        let stream = emu
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: DB_ROOT.to_string(),
                documents: vec![format!("{DOCS_ROOT}/c/d1")],
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::NewTransaction(
                        TransactionOptions {
                            mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                                retry_transaction: t1_bytes.clone(),
                                ..Default::default()
                            })),
                        },
                    ),
                ),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let retry_handle = tokio::spawn({
            let db = Arc::clone(&db);
            let retry_bytes = t1_bytes.clone();
            async move {
                db.new_txn(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                        retry_transaction: retry_bytes,
                        ..Default::default()
                    })),
                })
                .await
            }
        });

        drop(stream);

        let retried_id = retry_handle
            .await
            .unwrap()
            .expect("retry_transaction should succeed");
        tokio::task::yield_now().await;

        let commit_res = db.commit(Vec::new(), Some(retried_id)).await;
        assert!(
            commit_res.is_ok(),
            "duplicate rollback from cancelled attempt 1 must not destroy active retry \
             transaction, got {commit_res:?}"
        );
    }

    #[tokio::test]
    async fn batch_get_documents_empty_docs_with_new_transaction_emits_transaction_frame() {
        let project = leaked_project(false);
        let emu = FirestoreEmulator { project };

        let mut stream = emu
            .batch_get_documents(Request::new(BatchGetDocumentsRequest {
                database: DB_ROOT.to_string(),
                documents: Vec::new(),
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::NewTransaction(rw_opts()),
                ),
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let frame = stream
            .next()
            .await
            .expect("expected metadata frame carrying new_transaction")
            .unwrap();
        assert!(!frame.transaction.is_empty());
        assert!(frame.result.is_none());
        assert!(frame.read_time.is_some());
        assert!(stream.next().await.is_none());

        let commit_res = emu
            .commit(Request::new(CommitRequest {
                database: DB_ROOT.to_string(),
                writes: Vec::new(),
                transaction: frame.transaction,
                request_options: None,
            }))
            .await;
        assert!(
            commit_res.is_ok(),
            "empty BatchGetDocuments NewTransaction must remain usable for Commit, got \
             {commit_res:?}"
        );
    }

    #[tokio::test]
    async fn unimplemented_request_options_and_auto_commit_return_unimplemented() {
        use googleapis::google::firestore::v1::{GetDocumentRequest, RequestOptions};

        let project = leaked_project(true);
        let emu = FirestoreEmulator { project };

        let unary_err = emu
            .get_document(Request::new(GetDocumentRequest {
                name: format!("{DOCS_ROOT}/c/d1"),
                mask: None,
                consistency_selector: None,
                request_options: Some(RequestOptions::default()),
            }))
            .await
            .unwrap_err();
        assert_eq!(unary_err.code(), Code::Unimplemented);

        let mut ep_auto_commit = emu
            .execute_pipeline(Request::new(ExecutePipelineRequest {
                database: DB_ROOT.to_string(),
                pipeline_type: None,
                consistency_selector: None,
                auto_commit_transaction: true,
                request_options: None,
            }))
            .await
            .unwrap()
            .into_inner();
        let ep_auto_err = ep_auto_commit.next().await.unwrap().unwrap_err();
        assert_eq!(ep_auto_err.code(), Code::Unimplemented);

        let mut ep_req_opts = emu
            .execute_pipeline(Request::new(ExecutePipelineRequest {
                database: DB_ROOT.to_string(),
                pipeline_type: None,
                consistency_selector: None,
                auto_commit_transaction: false,
                request_options: Some(RequestOptions::default()),
            }))
            .await
            .unwrap()
            .into_inner();
        let ep_opts_err = ep_req_opts.next().await.unwrap().unwrap_err();
        assert_eq!(ep_opts_err.code(), Code::Unimplemented);
    }
}
