use std::sync::Arc;

use firestore_database::{
    event::DatabaseEvent,
    get_doc_name_from_write,
    reference::{DocumentRef, Ref},
    FirestoreDatabase, FirestoreProject, ReadConsistency,
};
use futures::{future::try_join_all, stream::BoxStream, TryStreamExt};
use googleapis::google::{
    firestore::v1::{
        firestore_server::FirestoreServer,
        structured_query::{CollectionSelector, FieldReference},
        transaction_options::ReadWrite,
        *,
    },
    protobuf::{Empty, Timestamp},
};
use itertools::Itertools;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{async_trait, codec::CompressionEncoding, Code, Request, Response, Result, Status};
use tracing::{info, info_span, instrument, Instrument};

#[macro_use]
mod utils;

const MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;

pub fn service() -> FirestoreServer<FirestoreEmulator> {
    FirestoreServer::new(FirestoreEmulator::default())
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
}

pub struct FirestoreEmulator {
    project: &'static FirestoreProject,
}

impl Default for FirestoreEmulator {
    fn default() -> Self {
        Self {
            project: FirestoreProject::get(),
        }
    }
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

        let name: DocumentRef = name.parse()?;

        let doc = self
            .project
            .database(&name.collection_ref.root_ref)
            .await
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
    #[instrument(skip_all, fields(
        count = request.get_ref().documents.len(),
        in_txn = display(is_txn(&request.get_ref().consistency_selector))
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
        } = request.into_inner();
        unimplemented_option!(mask);

        let database = self.project.database(&database.parse()?).await;
        let documents: Vec<_> = documents
            .into_iter()
            .map(|name| name.parse::<DocumentRef>())
            .try_collect()?;

        // Only used for new transactions.
        let (mut new_transaction, read_consistency) = match consistency_selector {
            Some(batch_get_documents_request::ConsistencySelector::NewTransaction(
                transaction_options,
            )) => {
                unimplemented_option!(transaction_options.mode);
                let id = database.new_txn().await?;
                info!("started new transaction");
                (Some(id.into()), ReadConsistency::Transaction(id))
            }
            s => (None, s.try_into()?),
        };
        info!(?read_consistency);

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(
            async move {
                for name in documents {
                    use batch_get_documents_response::Result::*;
                    let msg = match database.get_doc(&name, &read_consistency).await {
                        Ok(doc) => Ok(BatchGetDocumentsResponse {
                            result:      Some(match doc {
                                None => Missing(name.to_string()),
                                Some(doc) => Found(doc),
                            }),
                            read_time:   Some(Timestamp::now()),
                            transaction: new_transaction.take().unwrap_or_default(),
                        }),
                        Err(err) => Err(Status::from(err)),
                    };
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
            .instrument(info_span!("worker")),
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Commits a transaction, while optionally updating documents.
    #[instrument(skip_all, fields(
        count = request.get_ref().writes.len(),
        in_txn = !request.get_ref().transaction.is_empty(),
    ), err)]
    async fn commit(&self, request: Request<CommitRequest>) -> Result<Response<CommitResponse>> {
        let CommitRequest {
            database,
            writes,
            transaction,
        } = request.into_inner();

        let database = self.project.database(&database.parse()?).await;

        let (commit_time, write_results) = if transaction.is_empty() {
            perform_writes(&database, writes).await?
        } else {
            let txn_id = transaction.try_into()?;
            info!(?txn_id);
            database.commit(writes, &txn_id).await?
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

        let parent: Ref = parent.parse()?;
        let database = self.project.database(parent.root()).await;

        let documents = database
            .run_query(
                parent,
                StructuredQuery {
                    select:   mask.map(|v| structured_query::Projection {
                        fields: v
                            .field_paths
                            .into_iter()
                            .map(|field_path| FieldReference { field_path })
                            .collect(),
                    }),
                    from:     vec![CollectionSelector {
                        collection_id,
                        all_descendants: false,
                    }],
                    r#where:  None,
                    order_by: vec![],
                    start_at: None,
                    end_at:   None,
                    offset:   0,
                    limit:    None,
                },
                consistency_selector.try_into()?,
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
    ) -> Result<Response<Empty>> {
        unimplemented!("delete_document")
    }

    /// Starts a new transaction.
    #[instrument(skip_all, err)]
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>> {
        let BeginTransactionRequest { database, options } = request.into_inner();

        let database = self.project.database(&database.parse()?).await;

        use transaction_options::Mode;

        let txn_id = match options {
            None => database.new_txn().await?,
            Some(TransactionOptions {
                mode: None | Some(Mode::ReadOnly(_)),
            }) => {
                unimplemented!("read-only transactions")
            }
            Some(TransactionOptions {
                mode: Some(Mode::ReadWrite(ReadWrite { retry_transaction })),
            }) => {
                let id = retry_transaction.try_into()?;
                database.new_txn_with_id(id).await?;
                id
            }
        };

        info!(?txn_id);
        Ok(Response::new(BeginTransactionResponse {
            transaction: txn_id.into(),
        }))
    }

    /// Rolls back a transaction.
    #[instrument(skip_all, err)]
    async fn rollback(&self, request: Request<RollbackRequest>) -> Result<Response<Empty>> {
        let RollbackRequest {
            database,
            transaction,
        } = request.into_inner();
        let database = self.project.database(&database.parse()?).await;
        database.rollback(&transaction.try_into()?).await?;
        Ok(Response::new(Empty {}))
    }

    /// Server streaming response type for the RunQuery method.
    type RunQueryStream = BoxStream<'static, Result<RunQueryResponse>>;

    /// Runs a query.
    #[instrument(skip_all, err)]
    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> Result<Response<Self::RunQueryStream>> {
        let RunQueryRequest {
            parent,
            // mode,
            query_type,
            consistency_selector,
        } = request.into_inner();
        // if mode != 0 {
        //     unimplemented!("QueryMode")
        // }
        let Some(run_query_request::QueryType::StructuredQuery(query)) = query_type else {
            unimplemented!("query without query")
        };

        let parent: Ref = parent.parse()?;

        let docs = self
            .project
            .database(parent.root())
            .await
            .run_query(parent, query, consistency_selector.try_into()?)
            .await?;

        let stream = tokio_stream::iter(docs).map(|doc| {
            Ok(RunQueryResponse {
                transaction: vec![],
                document: Some(doc),
                read_time: Some(Timestamp::now()),
                skipped_results: 0,
                // stats: None,
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
    /// ```sql
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
    type ListenStream = BoxStream<'static, Result<ListenResponse>>;

    /// Listens to changes. This method is only available via gRPC or WebChannel
    /// (not REST).
    #[instrument(skip_all, err)]
    async fn listen(
        &self,
        request: Request<tonic::Streaming<ListenRequest>>,
    ) -> Result<Response<Self::ListenStream>> {
        let stream = self
            .project
            .listen(request.into_inner().filter_map(Result::ok))
            .map_err(Into::into);
        Ok(Response::new(Box::pin(stream)))
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
        let parent: Ref = parent.parse()?;
        let collection_ids = self
            .project
            .database(parent.root())
            .await
            .get_collection_ids(&(parent))
            .await?
            .into_iter()
            .collect_vec();
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
            database,
            writes,
            labels,
        } = request.into_inner();
        unimplemented_collection!(labels);

        let time: Timestamp = Timestamp::now();

        let database = self.project.database(&database.parse()?).await;

        let (status, write_results, updates): (Vec<_>, Vec<_>, Vec<_>) =
            try_join_all(writes.into_iter().map(|write| async {
                let name = get_doc_name_from_write(&write)?;
                let mut guard = database.get_doc_meta_mut_no_txn(&name).await?;
                let result = database
                    .perform_write(write, &mut guard, time.clone())
                    .await;
                use googleapis::google::rpc;
                Ok(match result {
                    Ok((wr, update)) => (Default::default(), wr, Some((name.clone(), update))),
                    Err(err) => (
                        rpc::Status {
                            code:    err.grpc_code() as _,
                            message: err.to_string(),
                            details: vec![],
                        },
                        Default::default(),
                        None,
                    ),
                }) as Result<_, Status>
            }))
            .await?
            .into_iter()
            .multiunzip();

        database.send_event(DatabaseEvent {
            database:    Arc::downgrade(&database),
            update_time: time.clone(),
            updates:     updates.into_iter().flatten().collect(),
        });

        Ok(Response::new(BatchWriteResponse {
            write_results,
            status,
        }))
    }
}

async fn perform_writes(
    database: &Arc<FirestoreDatabase>,
    writes: Vec<Write>,
) -> Result<(Timestamp, Vec<WriteResult>)> {
    let time: Timestamp = Timestamp::now();
    let (write_results, updates): (Vec<_>, Vec<_>) =
        try_join_all(writes.into_iter().map(|write| async {
            let name = get_doc_name_from_write(&write)?;
            let mut guard = database.get_doc_meta_mut_no_txn(&name).await?;
            database
                .perform_write(write, &mut guard, time.clone())
                .await
        }))
        .await?
        .into_iter()
        .unzip();
    database.send_event(DatabaseEvent {
        database:    Arc::downgrade(database),
        update_time: time.clone(),
        updates:     updates.into_iter().map(|u| (u.name().clone(), u)).collect(),
    });
    Ok((time, write_results))
}

fn is_txn(selector: &Option<batch_get_documents_request::ConsistencySelector>) -> &'static str {
    match selector {
        Some(batch_get_documents_request::ConsistencySelector::Transaction(_)) => "true",
        Some(batch_get_documents_request::ConsistencySelector::NewTransaction(_)) => "new",
        Some(batch_get_documents_request::ConsistencySelector::ReadTime(_)) | None => "false",
    }
}
