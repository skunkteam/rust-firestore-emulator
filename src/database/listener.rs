use super::{
    collection::Collection, collection_name, document::DocumentVersion, query::Query, Database,
};
use crate::{
    database::ReadConsistency,
    googleapis::google::firestore::v1::{
        listen_request,
        listen_response::ResponseType,
        target::{self, query_target},
        target_change::TargetChangeType,
        DocumentChange, DocumentDelete, DocumentRemove, ListenRequest, ListenResponse, Target,
        TargetChange,
    },
    required_option, unimplemented, unimplemented_bool, unimplemented_collection,
    unimplemented_option,
    utils::{timestamp, timestamp_from_nanos, timestamp_nanos},
};
use prost_types::Timestamp;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
    vec,
};
use tokio::sync::mpsc;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, ReceiverStream},
    StreamExt, StreamMap,
};
use tonic::{Result, Status};
use tracing::{debug, error, instrument};

const TARGET_CHANGE_DEFAULT: TargetChange = TargetChange {
    target_change_type: TargetChangeType::NoChange as _,
    target_ids: Vec::new(),
    cause: None,
    resume_token: Vec::new(),
    read_time: None,
};

pub struct Listener {
    /// For debug purposes.
    id: usize,
    database: Arc<Database>,
    sender: mpsc::Sender<Result<ListenResponse>>,
    events_by_collection: StreamMap<String, BroadcastStream<DocumentVersion>>,
    targets_by_collection: HashMap<String, Vec<ListenerTarget>>,
    queries: Vec<QueryTarget>,
}

impl Listener {
    pub fn start(
        database: Arc<Database>,
        request_stream: tonic::Streaming<ListenRequest>,
    ) -> ReceiverStream<Result<ListenResponse>> {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

        let (sender, rx) = mpsc::channel(16);
        let listener = Self {
            id: NEXT_ID.fetch_add(1, atomic::Ordering::Relaxed),
            database: Arc::clone(&database),
            sender,
            events_by_collection: Default::default(),
            targets_by_collection: Default::default(),
            queries: Default::default(),
        };

        tokio::spawn(listener.go(request_stream));

        ReceiverStream::new(rx)
    }

    #[instrument(name = "listener", skip_all, fields(id = self.id), err)]
    async fn go(mut self, mut request_stream: tonic::Streaming<ListenRequest>) -> Result<()> {
        let mut new_collections = self.database.new_collections.subscribe();
        // Every Listener has a single event-loop, this is it:
        loop {
            tokio::select! {
                req = request_stream.next() => {
                    match req {
                        Some(Ok(msg)) => {
                            if let Err(err) = self.process_request(msg).await {
                                self.send_err(err).await?;
                            };
                        }
                        // For now, echo errors in our request stream.
                        Some(Err(err)) => self.send_err(err).await?,
                        // We're done, drop the listener.
                        None => return Ok(())
                    }
                }

                Some((_, event)) = self.events_by_collection.next() => {
                    match event {
                        Ok(event) => self.process_event(event).await?,
                        Err(BroadcastStreamRecvError::Lagged(count)) => {
                            error!(
                                id = self.id,
                                count = count,
                                "listener missed {} events, because of buffer overflow",
                                count
                            );
                        }
                    }
                }

                Ok(new_collection) = new_collections.recv() => {
                    self.process_new_collection(new_collection).await?;
                }
            }
        }
    }

    #[instrument(skip_all, err)]
    async fn process_request(&mut self, msg: ListenRequest) -> Result<()> {
        let ListenRequest {
            database: _,
            labels,
            target_change,
        } = msg;
        unimplemented_collection!(labels);
        required_option!(target_change);

        match target_change {
            listen_request::TargetChange::AddTarget(target) => {
                let Target {
                    target_id,
                    once,
                    expected_count,
                    target_type,
                    resume_type,
                } = target;
                unimplemented_bool!(once);
                unimplemented_option!(expected_count);
                required_option!(target_type);

                match target_type {
                    target::TargetType::Query(target::QueryTarget { parent, query_type }) => {
                        required_option!(query_type);
                        let query_target::QueryType::StructuredQuery(query) = query_type;
                        let query =
                            Query::from_structured(parent, query, ReadConsistency::Default)?;
                        query.check_live_query_compat()?;
                        self.add_query(target_id, query, resume_type).await?;
                    }
                    target::TargetType::Documents(target::DocumentsTarget { documents }) => {
                        self.add_documents(target_id, documents, resume_type)
                            .await?;
                    }
                }
            }
            listen_request::TargetChange::RemoveTarget(target_id) => {
                unimplemented!(format!("RemoveTarget: {target_id}"));
            }
        };
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn process_event(&mut self, event: DocumentVersion) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are processed. That's why we know for sure that
        // the output stream is not used for something else until we respond with our NO_CHANGE msg. That msg means that everything is up
        // to date until that point and this is (for now) the easiest way to make sure that is actually the case. This is probably okay,
        // but if it becomes a hotspot we might look into optimizing later.
        let doc_name = event.name();
        let update_time = event.update_time();
        let collection_name = collection_name(doc_name)?;
        let mut target_ids = HashSet::new();
        let mut removed_target_ids = HashSet::new();
        if let Some(targets) = self.targets_by_collection.get_mut(collection_name) {
            for i in (0..targets.len()).rev() {
                let target = &mut targets[i];
                if target.doc_name() != doc_name {
                    continue;
                }
                let last_read_time = target.last_read_time_mut();
                if timestamp_nanos(last_read_time) >= timestamp_nanos(update_time) {
                    continue;
                }
                *last_read_time = update_time.clone();
                if target.needs_removal(&event)? {
                    removed_target_ids.insert(target.target_id());
                    targets.swap_remove(i);
                } else {
                    target_ids.insert(target.target_id());
                }
            }
        }

        if !removed_target_ids.is_empty() {
            self.send(ResponseType::DocumentRemove(DocumentRemove {
                document: doc_name.to_string(),
                removed_target_ids: removed_target_ids.into_iter().collect(),
                read_time: Some(update_time.clone()),
            }))
            .await?;
        }

        if let Some(version) = event.stored_document() {
            for target in &self.queries {
                if target_ids.contains(&target.target_id) {
                    continue;
                }
                if !target.query.includes_collection(collection_name) {
                    continue;
                }
                if target.query.includes_document(version)? {
                    target_ids.insert(target.target_id);
                    self.targets_by_collection
                        .entry(collection_name.to_string())
                        .or_default()
                        .push(ListenerTarget::DocumentInQuery {
                            target_id: target.target_id,
                            name: doc_name.to_string(),
                            query: Arc::clone(&target.query),
                            last_read_time: update_time.clone(),
                        });
                    self.send(ResponseType::TargetChange(TargetChange {
                        target_change_type: TargetChangeType::Add as _,
                        target_ids: vec![target.target_id],
                        cause: None,
                        resume_token: vec![],
                        read_time: Some(update_time.clone()),
                    }))
                    .await?
                }
            }
        }

        if target_ids.is_empty() {
            return Ok(());
        }

        let target_ids = target_ids.into_iter().collect();

        let msg = match event.to_document() {
            Some(d) => ResponseType::DocumentChange(DocumentChange {
                document: Some(d),
                target_ids,
                removed_target_ids: vec![],
            }),
            None => ResponseType::DocumentDelete(DocumentDelete {
                document: doc_name.to_string(),
                removed_target_ids: target_ids,
                read_time: Some(update_time.clone()),
            }),
        };
        self.send(msg).await?;
        self.send(ResponseType::TargetChange(TargetChange {
            resume_token: timestamp_nanos(update_time).to_ne_bytes().to_vec(),
            read_time: Some(update_time.clone()),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await
    }

    #[instrument(skip_all, err)]
    async fn process_new_collection(&mut self, new_collection: Arc<Collection>) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are processed. That's why we know for sure that
        // the output stream is not used for something else until we respond with our NO_CHANGE msg. That msg means that everything is up
        // to date until that point and this is (for now) the easiest way to make sure that is actually the case. This is probably okay,
        // but if it becomes a hotspot we might look into optimizing later.
        if self
            .queries
            .iter()
            .any(|target| target.query.includes_collection(&new_collection.name))
        {
            self.ensure_listening_to_collection(&new_collection.name)
                .await;
        }
        for doc in new_collection.docs().await {
            let Some(latest_version) = doc.read().await?.current_version().map(Arc::clone) else {
                continue;
            };
            self.process_event(DocumentVersion::Stored(latest_version))
                .await?;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(target_id = target_id, count = documents.len()), err)]
    async fn add_documents(
        &mut self,
        target_id: i32,
        documents: Vec<String>,
        resume_type: Option<target::ResumeType>,
    ) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are processed. That's why we know for sure that
        // the output stream is not used for something else until we respond with our NO_CHANGE msg. That msg means that everything is up
        // to date until that point and this is (for now) the easiest way to make sure that is actually the case. This is probably okay,
        // but if it becomes a hotspot we might look into optimizing later.
        let send_if_newer_than = resume_type.map(Timestamp::try_from).transpose()?;

        // Response: I'm on it!
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Add as _,
            target_ids: vec![target_id],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let read_time = timestamp();
        for name in &documents {
            let collection_name = collection_name(name)?;
            debug!(name);
            // Make sure we get to receive all updates from now on..
            self.ensure_listening_to_collection(collection_name).await;
            self.add_listener_target(ListenerTarget::Document {
                target_id,
                name: name.to_string(),
                last_read_time: read_time.clone(),
            })?;

            // Now determine the latest version we can find...
            let doc = self
                .database
                .get_doc(name, &ReadConsistency::Default)
                .await?;

            // Only send if newer than the resume_token
            if let Some(previous_time) = &send_if_newer_than {
                if doc.as_ref().is_some_and(|v| {
                    timestamp_nanos(v.update_time.as_ref().unwrap())
                        <= timestamp_nanos(previous_time)
                }) {
                    continue;
                }
            }
            // Response: This is the current version, whether you like it or not.
            let msg = match doc {
                Some(d) => ResponseType::DocumentChange(DocumentChange {
                    document: Some(d),
                    target_ids: vec![target_id],
                    removed_target_ids: vec![],
                }),
                None => ResponseType::DocumentDelete(DocumentDelete {
                    document: name.to_string(),
                    removed_target_ids: vec![target_id],
                    read_time: Some(read_time.clone()),
                }),
            };
            self.send(msg).await?;
        }

        // Response: I've sent you the state of all documents now
        let resume_token = timestamp_nanos(&read_time).to_ne_bytes();
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Current as _,
            target_ids: vec![target_id],
            read_time: Some(read_time.clone()),
            resume_token: resume_token.to_vec(),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        // Response: Oh, by the way, everything is up to date. 🤷🏼‍♂️
        self.send(ResponseType::TargetChange(TargetChange {
            read_time: Some(read_time),
            resume_token: resume_token.to_vec(),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        Ok(())
    }

    async fn add_query(
        &mut self,
        target_id: i32,
        query: Query,
        resume_type: Option<target::ResumeType>,
    ) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are processed. That's why we know for sure that
        // the output stream is not used for something else until we respond with our NO_CHANGE msg. That msg means that everything is up
        // to date until that point and this is (for now) the easiest way to make sure that is actually the case. This is probably okay,
        // but if it becomes a hotspot we might look into optimizing later.
        let send_if_newer_than = resume_type.map(Timestamp::try_from).transpose()?;

        // Response: I'm on it!
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Add as _,
            target_ids: vec![target_id],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let read_time = timestamp();
        let query = Arc::new(query);
        self.queries.push(QueryTarget {
            target_id,
            query: Arc::clone(&query),
        });

        for doc in query.once(&self.database).await? {
            let name = &doc.name;
            let collection_name = collection_name(name)?;
            debug!(name);
            // Make sure we get to receive all updates from now on..
            self.ensure_listening_to_collection(collection_name).await;

            self.add_listener_target(ListenerTarget::DocumentInQuery {
                target_id,
                name: name.to_string(),
                query: Arc::clone(&query),
                last_read_time: read_time.clone(),
            })?;

            // Only send if newer than the resume_token
            if let Some(previous_time) = &send_if_newer_than {
                if doc
                    .update_time
                    .as_ref()
                    .is_some_and(|v| timestamp_nanos(v) <= timestamp_nanos(previous_time))
                {
                    continue;
                }
            }
            // Response: This is the current version, whether you like it or not.
            let msg = ResponseType::DocumentChange(DocumentChange {
                document: Some(doc),
                target_ids: vec![target_id],
                removed_target_ids: vec![],
            });
            self.send(msg).await?;
        }

        // Response: I've sent you the state of all documents now
        let resume_token = timestamp_nanos(&read_time).to_ne_bytes();
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Current as _,
            target_ids: vec![target_id],
            read_time: Some(read_time.clone()),
            resume_token: resume_token.to_vec(),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        // Response: Oh, by the way, everything is up to date. 🤷🏼‍♂️
        self.send(ResponseType::TargetChange(TargetChange {
            read_time: Some(read_time),
            resume_token: resume_token.to_vec(),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        Ok(())
    }

    #[instrument(skip_all, fields(message = display(show_response_type(&response_type))), err)]
    async fn send(&self, response_type: ResponseType) -> Result<()> {
        self.sender
            .send(Ok(ListenResponse {
                response_type: Some(response_type),
            }))
            .await
            .map_err(|_| Status::cancelled("stream closed"))
    }

    #[instrument(skip(self), err)]
    async fn send_err(&self, err: Status) -> Result<()> {
        self.sender
            .send(Err(err))
            .await
            .map_err(|_| Status::cancelled("stream closed"))
    }

    #[instrument(skip(self))]
    async fn ensure_listening_to_collection(&mut self, collection_name: &str) {
        if !self.events_by_collection.contains_key(collection_name) {
            let collection_events = self
                .database
                .get_collection(collection_name)
                .await
                .events
                .subscribe();
            self.events_by_collection.insert(
                collection_name.to_string(),
                BroadcastStream::new(collection_events),
            );
        }
    }

    fn add_listener_target(&mut self, target: ListenerTarget) -> Result<()> {
        self.targets_by_collection
            .entry(target.collection_name()?.to_string())
            .or_default()
            .push(target);
        Ok(())
    }
}

enum ListenerTarget {
    Document {
        target_id: i32,
        name: String,
        last_read_time: Timestamp,
    },
    DocumentInQuery {
        target_id: i32,
        name: String,
        query: Arc<Query>,
        last_read_time: Timestamp,
    },
}

impl ListenerTarget {
    fn doc_name(&self) -> &str {
        match self {
            ListenerTarget::Document { name, .. } => name,
            ListenerTarget::DocumentInQuery { name, .. } => name,
        }
    }

    fn collection_name(&self) -> Result<&str> {
        collection_name(self.doc_name())
    }

    fn last_read_time_mut(&mut self) -> &mut Timestamp {
        match self {
            ListenerTarget::Document { last_read_time, .. } => last_read_time,
            ListenerTarget::DocumentInQuery { last_read_time, .. } => last_read_time,
        }
    }

    fn target_id(&self) -> i32 {
        match self {
            ListenerTarget::Document { target_id, .. } => *target_id,
            ListenerTarget::DocumentInQuery { target_id, .. } => *target_id,
        }
    }

    fn needs_removal(&self, event: &DocumentVersion) -> Result<bool> {
        let Self::DocumentInQuery { query, .. } = self else {
            return Ok(false);
        };
        let Some(stored) = event.stored_document() else {
            return Ok(true);
        };
        Ok(!query.includes_document(stored)?)
    }
}

struct QueryTarget {
    target_id: i32,
    query: Arc<Query>,
}

fn show_response_type(rt: &ResponseType) -> &str {
    match rt {
        ResponseType::TargetChange(_) => "TargetChange",
        ResponseType::DocumentChange(_) => "DocumentChange",
        ResponseType::DocumentDelete(_) => "DocumentDelete",
        ResponseType::DocumentRemove(_) => "DocumentRemove",
        ResponseType::Filter(_) => "Filter",
    }
}

impl TryFrom<target::ResumeType> for Timestamp {
    type Error = Status;

    fn try_from(value: target::ResumeType) -> Result<Self, Self::Error> {
        match value {
            target::ResumeType::ResumeToken(token) => {
                let token = token
                    .try_into()
                    .map_err(|_| Status::invalid_argument("invalid resume token"))?;
                Ok(timestamp_from_nanos(i128::from_ne_bytes(token)))
            }
            target::ResumeType::ReadTime(time) => Ok(time),
        }
    }
}
