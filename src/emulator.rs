use self::database::{ConsistencySelector, Database};
use crate::{
    googleapis::google::firestore::v1::{transaction_options::ReadWrite, *},
    unimplemented, unimplemented_bool, unimplemented_collection, unimplemented_option,
};
use prost_types::Timestamp;
use std::{pin::Pin, sync::Arc, time::SystemTime};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{async_trait, Code, Request, Response, Result, Status};

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
        request: Request<GetDocumentRequest>,
    ) -> Result<Response<Document>> {
        let GetDocumentRequest {
            name,
            mask,
            consistency_selector,
        } = request.into_inner();
        unimplemented_option!(mask);

        let doc = self
            .database
            .get_doc(&name, &consistency_selector.try_into()?)
            .await?
            .ok_or_else(|| Status::not_found(Code::NotFound.description()))?;
        Ok(Response::new(doc))
    }

    /// Server streaming response type for the BatchGetDocuments method.
    type BatchGetDocumentsStream = ReceiverStream<Result<BatchGetDocumentsResponse>>;

    /// Gets multiple documents.
    ///
    /// Documents returned by this method are not guaranteed to be returned in the
    /// same order that they were requested.
    async fn batch_get_documents(
        &self,
        request: Request<BatchGetDocumentsRequest>,
    ) -> Result<Response<Self::BatchGetDocumentsStream>> {
        let BatchGetDocumentsRequest {
            database: _,
            documents,
            mask,
            consistency_selector,
        } = request.into_inner();
        unimplemented_option!(mask);

        // Only used for new transactions.
        let (mut new_transaction, consistency) = match consistency_selector {
            Some(batch_get_documents_request::ConsistencySelector::NewTransaction(
                transaction_options,
            )) => {
                unimplemented_option!(transaction_options.mode);
                let id = self.database.new_txn()?;
                (Some(id.into()), ConsistencySelector::Transaction(id))
            }
            s => (None, s.try_into()?),
        };

        let (tx, rx) = mpsc::channel(16);
        let database = self.database.clone();
        tokio::spawn(async move {
            for name in documents {
                use batch_get_documents_response::Result::*;
                let msg = match database.get_doc(&name, &consistency).await {
                    Ok(doc) => Ok(BatchGetDocumentsResponse {
                        result: Some(match doc {
                            None => Missing(name),
                            Some(doc) => Found(Document::clone(&doc)),
                        }),
                        read_time: Some(SystemTime::now().into()),
                        transaction: new_transaction.take().unwrap_or_default(),
                    }),
                    Err(err) => Err(err),
                };
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Commits a transaction, while optionally updating documents.
    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>> {
        let CommitRequest {
            database: _,
            writes,
            transaction,
        } = request.into_inner();
        unimplemented_collection!(transaction);

        let (commit_time, write_results) = perform_writes(&self.database, writes).await?;

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
        unimplemented!("CreateDocument")
    }

    /// Lists documents.
    async fn list_documents(
        &self,
        _request: Request<ListDocumentsRequest>,
    ) -> Result<Response<ListDocumentsResponse>> {
        unimplemented!("list_documents")
    }

    /// Updates or inserts a document.
    async fn update_document(
        &self,
        _request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>> {
        unimplemented!("update_document")
    }

    /// Deletes a document.
    async fn delete_document(
        &self,
        _request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>> {
        unimplemented!("delete_document")
    }

    /// Starts a new transaction.
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>> {
        let BeginTransactionRequest {
            database: _,
            options,
        } = request.into_inner();
        use transaction_options::Mode;
        match options {
            None => (),
            Some(TransactionOptions {
                mode: None | Some(Mode::ReadOnly(_)),
            }) => {
                unimplemented!("read-only transactions")
            }
            Some(TransactionOptions {
                mode: Some(Mode::ReadWrite(ReadWrite { retry_transaction })),
            }) => unimplemented_collection!(retry_transaction),
        };

        Ok(Response::new(BeginTransactionResponse {
            transaction: self.database.new_txn()?.into(),
        }))
    }

    /// Rolls back a transaction.
    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<()>> {
        let RollbackRequest {
            database: _,
            transaction,
        } = request.into_inner();
        self.database
            .rollback(&transaction.try_into()?)
            .map(Response::new)
    }

    /// Server streaming response type for the RunQuery method.
    type RunQueryStream = Pin<Box<dyn Stream<Item = Result<RunQueryResponse>> + Send + 'static>>;

    /// Runs a query.
    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<Self::RunQueryStream>> {
        let RunQueryRequest {
            parent,
            query_type,
            consistency_selector,
        } = request.into_inner();
        let Some(run_query_request::QueryType::StructuredQuery(query)) = query_type else {
            unimplemented!("query without query")
        };

        let (tx, rx) = mpsc::channel(16);

        self.database
            .run_query(parent, query, &consistency_selector.try_into()?, tx)?;
        let stream = ReceiverStream::new(rx).map(|doc| {
            Ok(RunQueryResponse {
                transaction: vec![],
                document: Some(doc?),
                read_time: Some(SystemTime::now().into()),
                skipped_results: 0,
                continuation_selector: None,
            })
        });

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
        unimplemented!("run_aggregation_query")
    }

    /// Partitions a query by returning partition cursors that can be used to run
    /// the query in parallel. The returned partition cursors are split points that
    /// can be used by RunQuery as starting/end points for the query results.
    async fn partition_query(
        &self,
        _request: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionQueryResponse>> {
        unimplemented!("partition_query")
    }

    /// Server streaming response type for the Write method.
    type WriteStream = ReceiverStream<Result<WriteResponse>>;

    /// Streams batches of document updates and deletes, in order. This method is
    /// only available via gRPC or WebChannel (not REST).
    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStream>> {
        let (tx, rx) = mpsc::channel(16);

        let database = self.database.clone();
        tokio::spawn(async move {
            let mut write_requests = request.into_inner();
            while let Some(wr) = write_requests.message().await? {
                let WriteRequest {
                    stream_id,
                    writes,
                    stream_token,
                    database: _,
                    labels,
                } = wr;
                unimplemented_collection!(labels);

                let (commit_time, write_results) = perform_writes(&database, writes).await?;

                let msg = Ok(WriteResponse {
                    stream_id,
                    stream_token,
                    write_results,
                    commit_time,
                });
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
            Ok(())
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Server streaming response type for the Listen method.
    type ListenStream = tonic::Streaming<ListenResponse>;

    /// Listens to changes. This method is only available via gRPC or WebChannel
    /// (not REST).
    async fn listen(
        &self,
        _request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>> {
        unimplemented!("listen")
    }

    /// Lists all the collection IDs underneath a document.
    async fn list_collection_ids(
        &self,
        request: Request<ListCollectionIdsRequest>,
    ) -> Result<Response<ListCollectionIdsResponse>> {
        let ListCollectionIdsRequest {
            parent,
            page_size,
            page_token,
            consistency_selector,
        } = request.into_inner();
        let collection_ids = self.database.get_collection_ids(&parent);
        unimplemented_bool!(collection_ids.len() as i32 > page_size);
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
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>> {
        let BatchWriteRequest {
            database: _,
            writes,
            labels,
        } = request.into_inner();
        unimplemented_collection!(labels);

        let time: Timestamp = SystemTime::now().into();
        let mut write_results = Vec::with_capacity(writes.len());
        let mut status = Vec::with_capacity(writes.len());
        for write in writes {
            let result = perform_write(&self.database, write, time.clone()).await;
            use crate::googleapis::google::rpc::Status;
            match result {
                Ok(wr) => {
                    status.push(Default::default());
                    write_results.push(wr);
                }
                Err(err) => {
                    status.push(Status {
                        code: err.code().into(),
                        message: err.message().to_string(),
                        details: vec![],
                    });
                    write_results.push(Default::default());
                }
            }
        }
        Ok(Response::new(BatchWriteResponse {
            write_results,
            status,
        }))
    }
}

async fn perform_write(
    database: &Database,
    write: Write,
    commit_time: Timestamp,
) -> Result<WriteResult> {
    let Write {
        update_mask,
        update_transforms,
        current_document,
        operation,
    } = write;
    unimplemented_option!(update_mask);
    unimplemented_collection!(update_transforms);
    let operation = operation.ok_or(Status::unimplemented("missing operation in write"))?;
    let condition = current_document
        .and_then(|cd| cd.condition_type)
        .map(Into::into);

    use write::Operation::*;
    match operation {
        Update(doc) => {
            database
                .set(doc.name, doc.fields, commit_time.clone(), condition)
                .await?;
        }
        Delete(name) => {
            database
                .delete(name, commit_time.clone(), condition)
                .await?;
        }
        Transform(_) => unimplemented!("transform"),
    }
    Ok(WriteResult {
        update_time: Some(commit_time),
        transform_results: vec![],
    })
}

async fn perform_writes(
    database: &Database,
    writes: Vec<Write>,
) -> Result<(Option<Timestamp>, Vec<WriteResult>)> {
    let time: Timestamp = SystemTime::now().into();
    let mut write_results = Vec::with_capacity(writes.len());
    for write in writes {
        write_results.push(perform_write(database, write, time.clone()).await?);
    }
    Ok((Some(time), write_results))
}
