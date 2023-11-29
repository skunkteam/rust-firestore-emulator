use crate::{
    database::{get_doc_name_from_write, Database, ReadConsistency},
    googleapis::google::firestore::v1::{
        structured_query::{CollectionSelector, FieldReference},
        transaction_options::ReadWrite,
        *,
    },
    unimplemented, unimplemented_bool, unimplemented_collection, unimplemented_option,
};
use futures::future::try_join_all;
use prost_types::Timestamp;
use std::{pin::Pin, sync::Arc, time::SystemTime};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{async_trait, Code, Request, Response, Result, Status};
use tracing::{info, instrument};

#[derive(Default)]
pub struct FirestoreEmulator {
    pub database: Arc<Database>,
}

impl std::fmt::Debug for FirestoreEmulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreEmulator").finish_non_exhaustive()
    }
}

impl FirestoreEmulator {
    async fn eval_command(&self, writes: &[Write]) -> Result<Option<WriteResult>> {
        let [Write {
            operation: Some(write::Operation::Update(update)),
            ..
        }] = writes
        else {
            return Ok(None);
        };
        let Some(command_name) = command_name(&update.name) else {
            return Ok(None);
        };
        info!(command_name, "received command");
        match command_name {
            "CLEAR_EMULATOR" => {
                self.database.clear().await;
                Ok(Some(WriteResult {
                    update_time: Some(SystemTime::now().into()),
                    transform_results: vec![],
                }))
            }
            _ => Err(Status::invalid_argument(format!(
                "Unknown COMMAND: {command_name}"
            ))),
        }
    }
}

fn command_name(doc_name: &str) -> Option<&str> {
    let path = doc_name.strip_prefix("projects/")?;
    let (_project_id, path) = path.split_once("/databases/")?;
    let (_database_name, path) = path.split_once("/documents/")?;
    path.strip_prefix("__COMMANDS__/")
}

#[async_trait]
impl firestore_server::Firestore for FirestoreEmulator {
    /// Gets a single document.
    #[instrument(skip_all, err)]
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
    #[instrument(skip_all, fields(count = request.get_ref().documents.len()), err)]
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
                let id = self.database.new_txn().await?;
                (Some(id.into()), ReadConsistency::Transaction(id))
            }
            s => (None, s.try_into()?),
        };

        let (tx, rx) = mpsc::channel(16);
        let database = Arc::clone(&self.database);
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
    #[instrument(skip_all, fields(count = request.get_ref().writes.len()), err)]
    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>> {
        let CommitRequest {
            database: _,
            writes,
            transaction,
        } = request.into_inner();

        if let Some(write_result) = self.eval_command(&writes).await? {
            return Ok(Response::new(CommitResponse {
                write_results: vec![write_result],
                commit_time: Some(SystemTime::now().into()),
            }));
        }

        let (commit_time, write_results) = if transaction.is_empty() {
            perform_writes(&self.database, writes).await?
        } else {
            self.database
                .commit(writes, &transaction.try_into()?)
                .await?
        };

        Ok(Response::new(CommitResponse {
            write_results,
            commit_time: Some(commit_time),
        }))
    }

    /// Creates a new document.
    #[instrument(skip_all, err)]
    async fn create_document(
        &self,
        _request: Request<CreateDocumentRequest>,
    ) -> Result<Response<Document>> {
        unimplemented!("CreateDocument")
    }

    /// Lists documents.
    #[instrument(skip_all, fields(request = ?request.get_ref()), err)]
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
        } = request.into_inner();

        unimplemented_bool!(show_missing);
        unimplemented_collection!(order_by);
        unimplemented_collection!(page_token);
        unimplemented_option!(mask);
        if page_size > 0 {
            unimplemented!("page_size");
        }

        let documents = self
            .database
            .run_query(
                parent,
                StructuredQuery {
                    select: mask.map(|v| structured_query::Projection {
                        fields: v
                            .field_paths
                            .into_iter()
                            .map(|field_path| FieldReference { field_path })
                            .collect(),
                    }),
                    from: vec![CollectionSelector {
                        collection_id,
                        all_descendants: false,
                    }],
                    r#where: None,
                    order_by: vec![],
                    start_at: None,
                    end_at: None,
                    offset: 0,
                    limit: None,
                },
                &consistency_selector.try_into()?,
            )
            .await?;

        Ok(Response::new(ListDocumentsResponse {
            documents,
            next_page_token: Default::default(),
        }))
    }

    /// Updates or inserts a document.
    #[instrument(skip_all, err)]
    async fn update_document(
        &self,
        _request: Request<UpdateDocumentRequest>,
    ) -> Result<Response<Document>> {
        unimplemented!("update_document")
    }

    /// Deletes a document.
    #[instrument(skip_all, err)]
    async fn delete_document(
        &self,
        _request: Request<DeleteDocumentRequest>,
    ) -> Result<Response<()>> {
        unimplemented!("delete_document")
    }

    /// Starts a new transaction.
    #[instrument(skip_all, err)]
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
            transaction: self.database.new_txn().await?.into(),
        }))
    }

    /// Rolls back a transaction.
    #[instrument(skip_all, err)]
    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<()>> {
        let RollbackRequest {
            database: _,
            transaction,
        } = request.into_inner();
        self.database
            .rollback(&transaction.try_into()?)
            .await
            .map(Response::new)
    }

    /// Server streaming response type for the RunQuery method.
    type RunQueryStream = Pin<Box<dyn Stream<Item = Result<RunQueryResponse>> + Send + 'static>>;

    /// Runs a query.
    #[instrument(skip_all, err)]
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

        let docs = self
            .database
            .run_query(parent, query, &consistency_selector.try_into()?)
            .await?;

        let stream = tokio_stream::iter(docs).map(|doc| {
            Ok(RunQueryResponse {
                transaction: vec![],
                document: Some(doc),
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
    #[instrument(skip_all, err)]
    async fn run_aggregation_query(
        &self,
        _request: Request<RunAggregationQueryRequest>,
    ) -> Result<Response<Self::RunAggregationQueryStream>> {
        unimplemented!("run_aggregation_query")
    }

    /// Partitions a query by returning partition cursors that can be used to run
    /// the query in parallel. The returned partition cursors are split points that
    /// can be used by RunQuery as starting/end points for the query results.
    #[instrument(skip_all, err)]
    async fn partition_query(
        &self,
        _request: Request<PartitionQueryRequest>,
    ) -> Result<Response<PartitionQueryResponse>> {
        unimplemented!("partition_query")
    }

    /// Server streaming response type for the Write method.
    type WriteStream = tonic::Streaming<WriteResponse>;

    /// Streams batches of document updates and deletes, in order. This method is
    /// only available via gRPC or WebChannel (not REST).
    #[instrument(skip_all, err)]
    async fn write(
        &self,
        _request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStream>> {
        unimplemented!("write");
    }

    /// Server streaming response type for the Listen method.
    type ListenStream = tonic::Streaming<ListenResponse>;

    /// Listens to changes. This method is only available via gRPC or WebChannel
    /// (not REST).
    #[instrument(skip_all, err)]
    async fn listen(
        &self,
        _request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>> {
        unimplemented!("listen")
    }

    /// Lists all the collection IDs underneath a document.
    #[instrument(skip_all, err)]
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
        let collection_ids = self.database.get_collection_ids(&parent).await;
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
    #[instrument(skip_all, err)]
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

        let (status, write_results) = try_join_all(writes.into_iter().map(|write| async {
            let name = get_doc_name_from_write(&write)?;
            let guard = self.database.get_doc_meta_mut(name).await?;
            let result = self
                .database
                .perform_write(write, &guard, time.clone())
                .await;
            use crate::googleapis::google::rpc;
            Ok(match result {
                Ok(wr) => (Default::default(), wr),
                Err(err) => (
                    rpc::Status {
                        code: err.code().into(),
                        message: err.message().to_string(),
                        details: vec![],
                    },
                    Default::default(),
                ),
            }) as Result<_, Status>
        }))
        .await?
        .into_iter()
        .unzip();
        Ok(Response::new(BatchWriteResponse {
            write_results,
            status,
        }))
    }
}

async fn perform_writes(
    database: &Database,
    writes: Vec<Write>,
) -> Result<(Timestamp, Vec<WriteResult>)> {
    let time: Timestamp = SystemTime::now().into();
    let write_results = try_join_all(writes.into_iter().map(|write| async {
        let name = get_doc_name_from_write(&write)?;
        let guard = database.get_doc_meta_mut(name).await?;
        database.perform_write(write, &guard, time.clone()).await
    }))
    .await?;
    Ok((time, write_results))
}
