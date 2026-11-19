use std::collections::HashMap;

use googleapis::google::{
    firestore::v1::{
        BatchGetDocumentsResponse, Document, ExecutePipelineResponse, ListenRequest,
        ListenResponse, RunAggregationQueryResponse, RunQueryResponse, TargetChange, Value,
        batch_get_documents_response, listen_response::ResponseType,
        target_change::TargetChangeType,
    },
    protobuf::Timestamp,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tonic::{Status, Streaming};

#[derive(Clone, Debug)]
pub(crate) struct QueryStreamOutput {
    pub(crate) documents: Vec<Document>,
    pub(crate) raw_frame_count: usize,
    pub(crate) read_time: Option<Timestamp>,
    pub(crate) transaction: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BatchGetItem {
    Found(Document),
    Missing(String),
}

#[derive(Clone, Debug)]
pub(crate) struct BatchGetOutput {
    pub(crate) items: Vec<BatchGetItem>,
    pub(crate) transaction: Vec<u8>,
    pub(crate) read_time: Option<Timestamp>,
}

impl BatchGetOutput {
    pub(crate) fn found_docs(&self) -> Vec<&Document> {
        self.items
            .iter()
            .filter_map(|item| match item {
                BatchGetItem::Found(doc) => Some(doc),
                BatchGetItem::Missing(_) => None,
            })
            .collect()
    }

    pub(crate) fn missing_names(&self) -> Vec<&str> {
        self.items
            .iter()
            .filter_map(|item| match item {
                BatchGetItem::Found(_) => None,
                BatchGetItem::Missing(name) => Some(name.as_str()),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AggregationOutput {
    pub(crate) fields:      HashMap<String, Value>,
    pub(crate) read_time:   Option<Timestamp>,
    pub(crate) transaction: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct PipelineOutput {
    pub(crate) results: Vec<Document>,
    pub(crate) execution_time: Option<Timestamp>,
    pub(crate) transaction: Vec<u8>,
}

/// Unpacks `error_in_stream` frames and strips empty `RunQueryResponse` sentinel frames
/// (`document: None`) while preserving raw frame count, `read_time`, and `transaction`.
pub(crate) async fn collect_run_query(
    mut stream: Streaming<RunQueryResponse>,
) -> Result<QueryStreamOutput, Status> {
    let mut documents = Vec::new();
    let mut raw_frame_count = 0usize;
    let mut read_time = None;
    let mut transaction = Vec::new();

    while let Some(frame) = stream.message().await? {
        raw_frame_count += 1;
        if transaction.is_empty() && !frame.transaction.is_empty() {
            transaction = frame.transaction;
        }
        if frame.read_time.is_some() {
            read_time = frame.read_time;
        }
        if let Some(doc) = frame.document {
            documents.push(doc);
        }
    }

    Ok(QueryStreamOutput {
        documents,
        raw_frame_count,
        read_time,
        transaction,
    })
}

struct AbortOnDrop(Option<JoinHandle<()>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

/// Drains `BatchGetDocuments` on a background collector task so large batches never stall
/// server-side bounded channels, unpacking `error_in_stream` errors automatically.
pub(crate) async fn collect_batch_get(
    mut stream: Streaming<BatchGetDocumentsResponse>,
) -> Result<BatchGetOutput, Status> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut collector_guard = AbortOnDrop(Some(tokio::spawn(async move {
        while let Some(item) = stream.message().await.transpose() {
            if tx.send(item).is_err() {
                break;
            }
        }
    })));

    let mut items = Vec::new();
    let mut transaction = Vec::new();
    let mut read_time = None;

    while let Some(res) = rx.recv().await {
        let frame = res?;
        if transaction.is_empty() && !frame.transaction.is_empty() {
            transaction = frame.transaction;
        }
        if frame.read_time.is_some() {
            read_time = frame.read_time;
        }
        match frame.result {
            Some(batch_get_documents_response::Result::Found(doc)) => {
                items.push(BatchGetItem::Found(doc));
            }
            Some(batch_get_documents_response::Result::Missing(name)) => {
                items.push(BatchGetItem::Missing(name));
            }
            None => {}
        }
    }

    if let Some(collector) = collector_guard.0.take() {
        let _unused = collector.await;
    }
    Ok(BatchGetOutput {
        items,
        transaction,
        read_time,
    })
}

pub(crate) async fn collect_aggregation(
    mut stream: Streaming<RunAggregationQueryResponse>,
) -> Result<AggregationOutput, Status> {
    let mut fields = HashMap::new();
    let mut read_time = None;
    let mut transaction = Vec::new();

    while let Some(frame) = stream.message().await? {
        if transaction.is_empty() && !frame.transaction.is_empty() {
            transaction = frame.transaction;
        }
        if frame.read_time.is_some() {
            read_time = frame.read_time;
        }
        if let Some(res) = frame.result {
            fields.extend(res.aggregate_fields);
        }
    }

    Ok(AggregationOutput {
        fields,
        read_time,
        transaction,
    })
}

pub(crate) async fn collect_pipeline(
    mut stream: Streaming<ExecutePipelineResponse>,
) -> Result<PipelineOutput, Status> {
    let mut results = Vec::new();
    let mut execution_time = None;
    let mut transaction = Vec::new();

    while let Some(frame) = stream.message().await? {
        if transaction.is_empty() && !frame.transaction.is_empty() {
            transaction = frame.transaction;
        }
        if frame.execution_time.is_some() {
            execution_time = frame.execution_time;
        }
        results.extend(frame.results);
    }

    Ok(PipelineOutput {
        results,
        execution_time,
        transaction,
    })
}

/// Coalesced snapshot batch emitted by `Listener` up to its `Current` + `NoChange` completion pair.
#[derive(Clone, Debug, Default)]
pub(crate) struct ListenBatch {
    pub(crate) added_targets: Vec<i32>,
    pub(crate) was_reset:     bool,
    pub(crate) saw_current:   bool,
    pub(crate) changed_docs:  Vec<Document>,
    pub(crate) deleted_docs:  Vec<String>,
    pub(crate) removed_docs:  Vec<String>,
    pub(crate) read_time:     Option<Timestamp>,
    pub(crate) resume_token:  Vec<u8>,
}

/// Active bidirectional `Listen` stream handle backed by a background collector task so rapid
/// sequential writes never deadlock on the server's bounded `mpsc::channel(16)`.
#[derive(Debug)]
pub(crate) struct ListenHandle {
    req_tx: Option<mpsc::Sender<ListenRequest>>,
    event_rx: mpsc::UnboundedReceiver<Result<ListenResponse, Status>>,
    collector_task: Option<JoinHandle<()>>,
}

impl ListenHandle {
    pub(crate) fn spawn(
        req_tx: mpsc::Sender<ListenRequest>,
        mut stream: Streaming<ListenResponse>,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let collector_task = tokio::spawn(async move {
            while let Some(item) = stream.message().await.transpose() {
                if event_tx.send(item).is_err() {
                    break;
                }
            }
        });
        Self {
            req_tx: Some(req_tx),
            event_rx,
            collector_task: Some(collector_task),
        }
    }

    pub(crate) async fn send(&self, req: ListenRequest) {
        if let Some(tx) = &self.req_tx {
            let _unused = tx.send(req).await;
        }
    }

    pub(crate) async fn next_raw(&mut self) -> Option<Result<ListenResponse, Status>> {
        self.event_rx.recv().await
    }

    /// Collects frames until `send_complete` emits the trailing `TargetChange(NoChange)` frame
    /// with empty `target_ids` (after `TargetChange(Current)`).
    pub(crate) async fn next_batch(&mut self) -> Result<ListenBatch, Status> {
        let mut batch = ListenBatch::default();
        while let Some(frame_res) = self.event_rx.recv().await {
            let frame = frame_res?;
            match frame.response_type {
                Some(ResponseType::TargetChange(tc)) => {
                    if apply_target_change(&mut batch, tc)? {
                        return Ok(batch);
                    }
                }
                Some(ResponseType::DocumentChange(dc)) => {
                    if let Some(doc) = dc.document {
                        batch.changed_docs.push(doc);
                    }
                }
                Some(ResponseType::DocumentDelete(dd)) => {
                    batch.deleted_docs.push(dd.document);
                }
                Some(ResponseType::DocumentRemove(dr)) => {
                    batch.removed_docs.push(dr.document);
                }
                Some(ResponseType::Filter(_)) | None => {}
            }
        }
        Err(Status::cancelled(
            "listen stream closed before batch completed",
        ))
    }

    pub(crate) async fn close(mut self) {
        self.req_tx.take();
        if let Some(task) = self.collector_task.take() {
            let _unused = task.await;
        }
    }
}

impl Drop for ListenHandle {
    fn drop(&mut self) {
        self.req_tx.take();
        if let Some(task) = self.collector_task.take() {
            task.abort();
        }
    }
}

fn apply_target_change(batch: &mut ListenBatch, tc: TargetChange) -> Result<bool, Status> {
    if let Some(cause) = tc.cause {
        return Err(Status::new(
            tonic::Code::from_i32(cause.code),
            cause.message,
        ));
    }
    let change_type = TargetChangeType::try_from(tc.target_change_type).map_err(|_| {
        Status::internal(format!(
            "unknown TargetChangeType value: {}",
            tc.target_change_type
        ))
    })?;
    match change_type {
        TargetChangeType::Add => {
            batch.added_targets.extend(tc.target_ids);
        }
        TargetChangeType::Reset => {
            batch.was_reset = true;
        }
        TargetChangeType::Current => {
            batch.saw_current = true;
            if tc.read_time.is_some() {
                batch.read_time = tc.read_time;
            }
            if !tc.resume_token.is_empty() {
                batch.resume_token = tc.resume_token;
            }
        }
        TargetChangeType::NoChange if tc.target_ids.is_empty() => {
            if tc.read_time.is_some() {
                batch.read_time = tc.read_time;
            }
            if !tc.resume_token.is_empty() {
                batch.resume_token = tc.resume_token;
            }
            return Ok(true);
        }
        TargetChangeType::NoChange | TargetChangeType::Remove => {}
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use googleapis::google::rpc::Status as RpcStatus;
    use tonic::Code;

    use super::*;

    #[test]
    fn apply_target_change_surfaces_cause_on_no_change_with_empty_targets() {
        let mut batch = ListenBatch::default();
        let err = apply_target_change(
            &mut batch,
            TargetChange {
                target_change_type: TargetChangeType::NoChange as i32,
                target_ids: vec![],
                cause: Some(RpcStatus {
                    code:    Code::PermissionDenied as i32,
                    message: "denied".to_string(),
                    details: vec![],
                }),
                resume_token: vec![],
                read_time: None,
            },
        )
        .expect_err("TargetChange with cause must return Err even when target_ids is empty");
        assert_eq!(err.code(), Code::PermissionDenied);
        assert_eq!(err.message(), "denied");
    }

    #[test]
    fn apply_target_change_rejects_unknown_change_type() {
        let mut batch = ListenBatch::default();
        let err = apply_target_change(
            &mut batch,
            TargetChange {
                target_change_type: 999,
                target_ids: vec![],
                cause: None,
                resume_token: vec![],
                read_time: None,
            },
        )
        .expect_err("unknown TargetChangeType must not silently complete batch");
        assert_eq!(err.code(), Code::Internal);
    }

    #[test]
    fn apply_target_change_completes_batch_on_valid_current_and_no_change() {
        let mut batch = ListenBatch::default();
        let ts = Timestamp {
            seconds: 42,
            nanos:   7,
        };
        assert!(
            !apply_target_change(
                &mut batch,
                TargetChange {
                    target_change_type: TargetChangeType::Current as i32,
                    target_ids: vec![1],
                    cause: None,
                    resume_token: vec![1, 2],
                    read_time: Some(ts),
                },
            )
            .unwrap()
        );
        assert!(batch.saw_current);
        assert!(
            apply_target_change(
                &mut batch,
                TargetChange {
                    target_change_type: TargetChangeType::NoChange as i32,
                    target_ids: vec![],
                    cause: None,
                    resume_token: vec![1, 2],
                    read_time: Some(ts),
                },
            )
            .unwrap()
        );
        assert_eq!(batch.read_time, Some(ts));
        assert_eq!(batch.resume_token, vec![1, 2]);
    }

    #[tokio::test]
    async fn abort_on_drop_cancels_collector_task() {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        let handle = tokio::spawn(async move {
            std::future::pending::<()>().await;
            drop(tx);
        });
        let guard = AbortOnDrop(Some(handle));
        drop(guard);
        assert!(rx.recv().await.is_none());
    }
}
