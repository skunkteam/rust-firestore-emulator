use crate::googleapis::google::firestore::v1::*;
use itertools::Itertools;
use std::{pin::Pin, sync::Arc, time::SystemTime};
use tokio_stream::{Stream, StreamExt};
use tonic::{async_trait, Request, Response, Result, Status};

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
        Err(Status::unimplemented(
            "GetDocument is not used by the admin SDK",
        ))
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
        let transaction = vec![];
        let database = self.database.clone();
        let documents = request.documents.into_iter().map(move |name| {
            let doc = database.get_by_name(&name)?;
            use batch_get_documents_response::Result;
            Ok(BatchGetDocumentsResponse {
                result: Some(match doc {
                    None => Result::Missing(name),
                    Some(doc) => Result::Found(Document::clone(&doc)),
                }),
                read_time: Some(SystemTime::now().into()),
                transaction: transaction.clone(),
            })
        });

        Ok(Response::new(Box::pin(tokio_stream::iter(documents))))
    }

    /// Commits a transaction, while optionally updating documents.
    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>> {
        let request = request.into_inner();
        let commit_time = Some(SystemTime::now().into());
        let write_results = request
            .writes
            .into_iter()
            .map(|write| self.database.perform_write(write, commit_time.clone()))
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
        Err(Status::unimplemented(
            "CreateDocument is not used by the admin SDK",
        ))
    }

    /// Lists documents.
    async fn list_documents(
        &self,
        _request: Request<ListDocumentsRequest>,
    ) -> Result<Response<ListDocumentsResponse>> {
        Err(Status::unimplemented("list_documents"))
    }

    /// Updates or inserts a document.
    async fn update_document(
        &self,
        _request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>> {
        Err(Status::unimplemented("update_document"))
    }

    /// Deletes a document.
    async fn delete_document(
        &self,
        _request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>> {
        Err(Status::unimplemented("delete_document"))
    }

    /// Starts a new transaction.
    async fn begin_transaction(
        &self,
        _request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>> {
        Err(Status::unimplemented("begin_transaction"))
    }

    /// Rolls back a transaction.
    async fn rollback(&self, _request: Request<RollbackRequest>) -> Result<Response<()>> {
        Err(Status::unimplemented("rollback"))
    }

    /// Server streaming response type for the RunQuery method.
    type RunQueryStream = Pin<Box<dyn Stream<Item = Result<RunQueryResponse>> + Send + 'static>>;

    /// Runs a query.
    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<Self::RunQueryStream>> {
        let stream = self.database.run_query(request.into_inner())?;
        Ok(Response::new(Box::pin(stream)))
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
        Err(Status::unimplemented("run_aggregation_query"))
    }

    /// Partitions a query by returning partition cursors that can be used to run
    /// the query in parallel. The returned partition cursors are split points that
    /// can be used by RunQuery as starting/end points for the query results.
    async fn partition_query(
        &self,
        _request: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionQueryResponse>> {
        Err(Status::unimplemented("partition_query"))
    }

    /// Server streaming response type for the Write method.
    type WriteStream = Pin<Box<dyn Stream<Item = Result<WriteResponse>> + Send + 'static>>;

    /// Streams batches of document updates and deletes, in order. This method is
    /// only available via gRPC or WebChannel (not REST).
    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStream>> {
        let database = self.database.clone();
        Ok(Response::new(Box::pin(request.into_inner().map(
            move |wr| -> Result<WriteResponse> {
                let WriteRequest {
                    stream_id,
                    writes,
                    stream_token,
                    ..
                } = wr?;
                let commit_time = Some(SystemTime::now().into());
                let write_results = writes
                    .into_iter()
                    .map(|write| database.perform_write(write, commit_time.clone()))
                    .try_collect()?;
                Ok(WriteResponse {
                    stream_id,
                    stream_token,
                    write_results,
                    commit_time,
                })
            },
        ))))
    }

    /// Server streaming response type for the Listen method.
    type ListenStream = tonic::Streaming<ListenResponse>;

    /// Listens to changes. This method is only available via gRPC or WebChannel
    /// (not REST).
    async fn listen(
        &self,
        _request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>> {
        Err(Status::unimplemented("listen"))
    }

    /// Lists all the collection IDs underneath a document.
    async fn list_collection_ids(
        &self,
        request: Request<ListCollectionIdsRequest>,
    ) -> Result<Response<ListCollectionIdsResponse>> {
        let request = request.into_inner();
        let collection_ids = self.database.get_collection_ids(&request.parent);
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
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>> {
        let request = request.into_inner();
        let commit_time = Some(SystemTime::now().into());
        let (write_results, status): (Vec<_>, Vec<_>) = request
            .writes
            .into_iter()
            .map(move |write| {
                use crate::googleapis::google::rpc::Status;
                let result = self.database.perform_write(write, commit_time.clone());
                match result {
                    Ok(write_result) => (write_result, Default::default()),
                    Err(status) => (
                        Default::default(),
                        Status {
                            code: status.code().into(),
                            details: vec![],
                            message: status.message().to_string(),
                        },
                    ),
                }
            })
            .unzip();
        Ok(Response::new(BatchWriteResponse {
            write_results,
            status,
        }))
    }
}
