use crate::googleapis::google::firestore::v1::{precondition::ConditionType, write::Operation, *};
use itertools::Itertools;
use std::{pin::Pin, sync::Arc, time::SystemTime};
use tokio_stream::{Stream, StreamExt};
use tonic::{async_trait, Request, Response, Result};

mod database;

#[derive(Default)]
pub struct FirestoreEmulator {
    database: Arc<database::Database>,
}

#[async_trait]
impl firestore_server::Firestore for FirestoreEmulator {
    /// Gets a single document.
    async fn get_document(
        &self,
        _request: Request<GetDocumentRequest>,
    ) -> Result<Response<Document>> {
        unimplemented!("GetDocument is not used by the admin SDK")
    }

    /// Server streaming response type for the BatchGetDocuments method.
    type BatchGetDocumentsStream =
        Pin<Box<dyn Stream<Item = Result<BatchGetDocumentsResponse>> + Send + 'static>>;

    /// Gets multiple documents.
    ///
    /// Documents returned by this method are not guaranteed to be returned in the
    /// same order that they were requested.
    async fn batch_get_documents(
        &self,
        request: Request<BatchGetDocumentsRequest>,
    ) -> Result<Response<Self::BatchGetDocumentsStream>> {
        let request = request.into_inner();
        let database = self.database.clone();
        let transaction = vec![];
        let stream = tokio_stream::iter(request.documents).map(move |name| {
            let doc = database.get_by_name(&name)?;
            Ok(BatchGetDocumentsResponse {
                result: Some(match doc {
                    None => batch_get_documents_response::Result::Missing(name),
                    Some(doc) => batch_get_documents_response::Result::Found(Document::clone(&doc)),
                }),
                read_time: Some(SystemTime::now().into()),
                transaction: transaction.clone(),
            })
        });

        Ok(Response::new(Box::pin(stream)))
    }

    /// Commits a transaction, while optionally updating documents.
    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>> {
        let request = request.into_inner();
        let commit_time = Some(SystemTime::now().into());
        let write_results = request
            .writes
            .into_iter()
            .map(|write| -> Result<WriteResult> {
                let (Some(operation), Some(condition)) = (
                    write.operation,
                    write.current_document.and_then(|c| c.condition_type),
                ) else {
                    todo!("Missing operation or condition");
                };

                match (operation, condition) {
                    (Operation::Update(doc), ConditionType::Exists(false)) => {
                        self.database.add(doc.name, doc.fields, commit_time.clone())
                    }
                    (Operation::Update(_), ConditionType::Exists(true)) => {
                        todo!("update")
                    }
                    (Operation::Update(_), _) => todo!("update with other precondition"),
                    (Operation::Delete(_), _) => todo!("delete"),
                    (Operation::Transform(_), _) => todo!("transform"),
                }
            })
            .try_collect()?;

        Ok(Response::new(CommitResponse {
            write_results,
            commit_time,
        }))
    }

    /// Creates a new document.
    async fn create_document(
        &self,
        _request: Request<CreateDocumentRequest>,
    ) -> Result<Response<Document>> {
        unimplemented!("CreateDocument is not used by the admin SDK")
    }

    /// Lists documents.
    async fn list_documents(
        &self,
        _request: Request<ListDocumentsRequest>,
    ) -> Result<Response<ListDocumentsResponse>> {
        todo!("list_documents")
    }

    /// Updates or inserts a document.
    async fn update_document(
        &self,
        _request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>> {
        todo!("update_document")
    }

    /// Deletes a document.
    async fn delete_document(
        &self,
        _request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>> {
        todo!("delete_document")
    }

    /// Starts a new transaction.
    async fn begin_transaction(
        &self,
        _request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>> {
        todo!("begin_transaction")
    }

    /// Rolls back a transaction.
    async fn rollback(&self, _request: Request<RollbackRequest>) -> Result<Response<()>> {
        todo!("rollback")
    }

    /// Server streaming response type for the RunQuery method.
    type RunQueryStream = tonic::Streaming<RunQueryResponse>;

    /// Runs a query.
    async fn run_query(
        &self,
        _request: Request<RunQueryRequest>,
    ) -> Result<Response<Self::RunQueryStream>> {
        todo!("run_query")
    }

    /// Server streaming response type for the RunAggregationQuery method.
    type RunAggregationQueryStream = tonic::Streaming<RunAggregationQueryResponse>;

    /// Runs an aggregation query.
    ///
    /// Rather than producing [Document][google.firestore.v1.Document] results like
    /// [Firestore.RunQuery][google.firestore.v1.Firestore.RunQuery], this API
    /// allows running an aggregation to produce a series of
    /// [AggregationResult][google.firestore.v1.AggregationResult] server-side.
    ///
    /// High-Level Example:
    ///
    /// ```
    /// -- Return the number of documents in table given a filter.
    /// SELECT COUNT(*) FROM ( SELECT * FROM k where a = true );
    /// ```
    async fn run_aggregation_query(
        &self,
        _request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<Self::RunAggregationQueryStream>> {
        todo!("run_aggregation_query")
    }

    /// Partitions a query by returning partition cursors that can be used to run
    /// the query in parallel. The returned partition cursors are split points that
    /// can be used by RunQuery as starting/end points for the query results.
    async fn partition_query(
        &self,
        _request: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionQueryResponse>> {
        todo!("partition_query")
    }

    /// Server streaming response type for the Write method.
    type WriteStream = tonic::Streaming<WriteResponse>;

    /// Streams batches of document updates and deletes, in order. This method is
    /// only available via gRPC or WebChannel (not REST).
    async fn write(
        &self,
        _request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStream>> {
        todo!("write")
    }

    /// Server streaming response type for the Listen method.
    type ListenStream = tonic::Streaming<ListenResponse>;

    /// Listens to changes. This method is only available via gRPC or WebChannel
    /// (not REST).
    async fn listen(
        &self,
        _request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>> {
        todo!("listen")
    }

    /// Lists all the collection IDs underneath a document.
    async fn list_collection_ids(
        &self,
        _request: Request<ListCollectionIdsRequest>,
    ) -> Result<Response<ListCollectionIdsResponse>> {
        todo!("list_collection_ids")
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
    async fn batch_write(
        &self,
        _request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>> {
        todo!("batch_write")
    }
}
