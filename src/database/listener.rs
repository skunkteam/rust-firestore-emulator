use super::{collection_name, document::DocumentUpdate, Database};
use crate::{
    database::ReadConsistency,
    googleapis::google::firestore::v1::{
        listen_request, listen_response::ResponseType, target, target_change::TargetChangeType,
        DocumentChange, DocumentDelete, ListenRequest, ListenResponse, Target, TargetChange,
    },
    required_option, unimplemented, unimplemented_bool, unimplemented_collection,
    unimplemented_option,
    utils::{timestamp, timestamp_from_nanos, timestamp_nanos},
};
use itertools::Itertools;
use prost_types::Timestamp;
use std::{
    collections::HashMap,
    iter,
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};
use tokio::sync::{mpsc, Mutex};
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
    target_admin: Mutex<ListenerTargetAdmin>,
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
            target_admin: Mutex::new(ListenerTargetAdmin::new(database)),
        };

        tokio::spawn(listener.go(request_stream));

        ReceiverStream::new(rx)
    }

    #[instrument(name = "listener", skip_all, fields(id = self.id))]
    async fn go(self, mut request_stream: tonic::Streaming<ListenRequest>) -> Result<()> {
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

                Some(event) = self.next_event() => {
                    self.process_event(event).await?;
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn next_event(&self) -> Option<DocumentUpdate> {
        loop {
            match self.target_admin.lock().await.events.next().await? {
                (_, Ok(event)) => return Some(event),
                (_, Err(BroadcastStreamRecvError::Lagged(nr))) => {
                    error!(
                        id = self.id,
                        "listener missed {} events, because of buffer overflow", nr
                    );
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn process_request(&self, msg: ListenRequest) -> Result<()> {
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
                    target::TargetType::Query(_) => unimplemented!("TargetType::Query"),
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

    #[instrument(skip_all)]
    async fn process_event(&self, event: DocumentUpdate) -> Result<()> {
        // Lock the target_admin until we're done to make sure the stream is not used for something else until we respond with our
        // NO_CHANGE msg. That msg means that everything is up to date until that point and this is (for now) the easiest way to make
        // sure that is actually the case. This is probably okay, but if it becomes a hotspot we might look into optimizing later.
        let mut admin = self.target_admin.lock().await;
        let doc_name = event.new_version.name();
        let update_time = event.new_version.update_time();
        let target_ids = admin
            .targets_for_doc_name_mut(doc_name)?
            .filter_map(|target| {
                let ListenerTarget::Document {
                    target_id,
                    name: _,
                    last_read_time,
                } = target;
                if timestamp_nanos(last_read_time) >= timestamp_nanos(update_time) {
                    return None;
                }
                *last_read_time = update_time.clone();
                Some(*target_id)
            })
            .collect_vec();
        if target_ids.is_empty() {
            return Ok(());
        }

        let msg = match event.new_version.to_document() {
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

    #[instrument(skip_all, fields(target_id = target_id, count = documents.len()))]
    async fn add_documents(
        &self,
        target_id: i32,
        documents: Vec<String>,
        resume_type: Option<target::ResumeType>,
    ) -> Result<()> {
        // Lock the target_admin until we're done to make sure the stream is not used for something else until we respond with our
        // NO_CHANGE msg. That msg means that everything is up to date until that point and this is (for now) the easiest way to make
        // sure that is actually the case. This is probably okay, but if it becomes a hotspot we might look into optimizing later.
        let mut admin = self.target_admin.lock().await;
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
            debug!(name);
            // Make sure we get to receive all updates from now on..
            admin.ensure_listening_to_doc(name).await?;
            admin.add_listener_target(ListenerTarget::Document {
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

    #[instrument(skip_all, fields(message = display(show_response_type(&response_type))))]
    async fn send(&self, response_type: ResponseType) -> Result<()> {
        self.sender
            .send(Ok(ListenResponse {
                response_type: Some(response_type),
            }))
            .await
            .map_err(|_| Status::cancelled("stream closed"))
    }

    #[instrument(skip(self))]
    async fn send_err(&self, err: Status) -> Result<()> {
        self.sender
            .send(Err(err))
            .await
            .map_err(|_| Status::cancelled("stream closed"))
    }
}

struct ListenerTargetAdmin {
    database: Arc<Database>,
    events: StreamMap<String, BroadcastStream<DocumentUpdate>>,
    targets_by_collection: HashMap<String, Vec<ListenerTarget>>,
}

impl ListenerTargetAdmin {
    fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            events: Default::default(),
            targets_by_collection: Default::default(),
        }
    }

    #[instrument(skip(self))]
    async fn ensure_listening_to_doc(&mut self, name: &str) -> Result<()> {
        let collection_name = collection_name(name)?;
        if !self.events.contains_key(collection_name) {
            let collection_events = self
                .database
                .get_collection(collection_name)
                .await
                .events
                .subscribe();
            self.events.insert(
                collection_name.to_string(),
                BroadcastStream::new(collection_events),
            );
        }
        Ok(())
    }

    fn add_listener_target(&mut self, target: ListenerTarget) -> Result<()> {
        self.targets_by_collection
            .entry(target.collection_name()?.to_string())
            .or_default()
            .push(target);
        Ok(())
    }

    fn targets_for_doc_name_mut<'a>(
        &'a mut self,
        doc_name: &'a str,
    ) -> Result<Box<dyn Iterator<Item = &'a mut ListenerTarget> + 'a>> {
        let collection = collection_name(doc_name)?;
        let Some(targets) = self.targets_by_collection.get_mut(collection) else {
            return Ok(Box::new(iter::empty()));
        };
        let iter = targets.iter_mut().filter(move |target| {
            let ListenerTarget::Document { name, .. } = target;
            name == doc_name
        });
        Ok(Box::new(iter))
    }
}

enum ListenerTarget {
    Document {
        target_id: i32,
        name: String,
        last_read_time: Timestamp,
    },
}

impl ListenerTarget {
    fn collection_name(&self) -> Result<&str> {
        match self {
            ListenerTarget::Document { name, .. } => collection_name(name),
        }
    }
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
