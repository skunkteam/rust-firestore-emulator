use super::{
    document::DocumentVersion, event::DatabaseEvent, query::Query, target_change, Database,
};
use crate::{
    database::ReadConsistency,
    googleapis::google::firestore::v1::{
        listen_request,
        listen_response::ResponseType,
        target::{self, query_target},
        target_change::TargetChangeType,
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
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};
use string_cache::DefaultAtom;
use tokio::sync::{broadcast::error::RecvError, mpsc};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Result, Status};
use tracing::{debug, error, instrument};

const TARGET_ID: i32 = 1;

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
    target: Option<ListenerTarget>,
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
            // No target yet, will be added by a message in the request stream.
            target: None,
        };

        tokio::spawn(listener.go(request_stream));

        ReceiverStream::new(rx)
    }

    #[instrument(name = "listener", skip_all, fields(id = self.id), err)]
    async fn go(mut self, mut request_stream: tonic::Streaming<ListenRequest>) -> Result<()> {
        let mut database_events = self.database.events.subscribe();
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

                database_event = database_events.recv() => {
                    match database_event {
                        Ok(event) => self.process_event(event).await?,
                        Err(RecvError::Lagged(count)) => {
                            error!(
                                id = self.id,
                                count = count,
                                "listener missed {} events, because of buffer overflow",
                                count
                            );
                        },
                        // Database is dropped?
                        Err(RecvError::Closed) => return Ok(())
                    }
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

                if self.target.is_some() || target_id != TARGET_ID {
                    unimplemented!("multiple targets inside a single listen stream")
                }

                match target_type {
                    target::TargetType::Query(target::QueryTarget { parent, query_type }) => {
                        required_option!(query_type);
                        let query_target::QueryType::StructuredQuery(query) = query_type;
                        let query =
                            Query::from_structured(parent, query, ReadConsistency::Default)?;
                        query.check_live_query_compat()?;
                        self.set_query(query, resume_type).await?;
                    }
                    target::TargetType::Documents(target::DocumentsTarget { mut documents }) => {
                        if documents.len() != 1 {
                            unimplemented!("multiple documents inside a single listen stream")
                        }
                        self.set_document(documents.pop().unwrap().into(), resume_type)
                            .await?;
                    }
                };
            }
            listen_request::TargetChange::RemoveTarget(target_id) => {
                unimplemented!(format!("RemoveTarget: {target_id}"));
            }
        };
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn process_event(&mut self, event: DatabaseEvent) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are processed. That's why we know for sure that
        // the output stream is not used for something else until we respond with our NO_CHANGE msg. That msg means that everything is up
        // to date until that point and this is (for now) the easiest way to make sure that is actually the case. This is probably okay,
        // but if it becomes a hotspot we might look into optimizing later.
        let Some(target) = &mut self.target else {
            return Ok(());
        };

        let update_time = event.update_time.clone();
        let msgs = target.process_event(&self.database, event).await?;

        if msgs.is_empty() {
            return Ok(());
        }

        for msg in msgs {
            self.send(msg).await?;
        }

        // Response: I've sent you the state of all documents now
        let resume_token = timestamp_nanos(&update_time).to_ne_bytes();
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Current as _,
            target_ids: vec![TARGET_ID],
            read_time: Some(update_time.clone()),
            resume_token: resume_token.to_vec(),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        // Response: Oh, by the way, everything is up to date. 🤷🏼‍♂️
        self.send(ResponseType::TargetChange(TargetChange {
            resume_token: timestamp_nanos(&update_time).to_ne_bytes().to_vec(),
            read_time: Some(update_time),
            ..TARGET_CHANGE_DEFAULT
        }))
        .await
    }

    #[instrument(skip_all, fields(document = &*name), err)]
    async fn set_document(
        &mut self,
        name: DefaultAtom,
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
            target_ids: vec![TARGET_ID],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let read_time = timestamp();
        debug!(name = &*name);
        self.target = Some(ListenerTarget::DocumentTarget(DocumentTarget {
            name: name.clone(),
            last_read_time: read_time.clone(),
        }));

        // Now determine the latest version we can find...
        let doc = self
            .database
            .get_doc(&name, &ReadConsistency::Default)
            .await?;

        // Only send if newer than the resume_token
        let send_initial = match &send_if_newer_than {
            Some(previous_time) => !doc.as_ref().is_some_and(|v| {
                timestamp_nanos(v.update_time.as_ref().unwrap()) <= timestamp_nanos(previous_time)
            }),
            _ => true,
        };

        if send_initial {
            // Response: This is the current version, whether you like it or not.
            let msg = match doc {
                Some(d) => ResponseType::DocumentChange(DocumentChange {
                    document: Some(d),
                    target_ids: vec![TARGET_ID],
                    removed_target_ids: vec![],
                }),
                None => ResponseType::DocumentDelete(DocumentDelete {
                    document: name.to_string(),
                    removed_target_ids: vec![TARGET_ID],
                    read_time: Some(read_time.clone()),
                }),
            };
            self.send(msg).await?;
        }

        // Response: I've sent you the state of all documents now
        let resume_token = timestamp_nanos(&read_time).to_ne_bytes();
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Current as _,
            target_ids: vec![TARGET_ID],
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

    async fn set_query(
        &mut self,
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
            target_ids: vec![TARGET_ID],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let read_time = timestamp();
        let query = Arc::new(query);
        self.target = Some(ListenerTarget::QueryTarget(QueryTarget {
            query: Arc::clone(&query),
            doctargets_by_name: Default::default(),
        }));

        // TODO: Verplaats naar QueryTarget
        for doc in query.once(&self.database).await? {
            debug!(name = &*doc.name);

            // Only send if newer than the resume_token
            let send_initial = match &send_if_newer_than {
                Some(previous_time) => !doc
                    .update_time
                    .as_ref()
                    .is_some_and(|v| timestamp_nanos(v) <= timestamp_nanos(previous_time)),
                _ => true,
            };

            if send_initial {
                // Response: This is the current version, whether you like it or not.
                let msg = ResponseType::DocumentChange(DocumentChange {
                    document: Some(doc),
                    target_ids: vec![TARGET_ID],
                    removed_target_ids: vec![],
                });
                self.send(msg).await?;
            }
        }

        // Response: I've sent you the state of all documents now
        let resume_token = timestamp_nanos(&read_time).to_ne_bytes();
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Current as _,
            target_ids: vec![TARGET_ID],
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
}

enum ListenerTarget {
    DocumentTarget(DocumentTarget),
    QueryTarget(QueryTarget),
}

impl ListenerTarget {
    async fn process_event(
        &mut self,
        database: &Arc<Database>,
        event: DatabaseEvent,
    ) -> Result<Vec<ResponseType>> {
        match self {
            ListenerTarget::DocumentTarget(target) => target.process_event(event),
            ListenerTarget::QueryTarget(target) => target.process_event(database, event).await,
        }
    }
}

struct DocumentTarget {
    name: DefaultAtom,
    last_read_time: Timestamp,
}

impl DocumentTarget {
    fn process_event(&mut self, event: DatabaseEvent) -> Result<Vec<ResponseType>> {
        let Ok(update) = event
            .updates
            .into_iter()
            .filter(|update| update.name() == &self.name)
            .at_most_one()
        else {
            unimplemented!("multiple updates to a single document in one txn")
        };
        let Some(update) = update else {
            return Ok(vec![]);
        };
        if timestamp_nanos(&self.last_read_time) >= timestamp_nanos(&event.update_time) {
            return Ok(vec![]);
        }
        self.last_read_time = event.update_time.clone();

        let msg = match update.to_document() {
            Some(d) => ResponseType::DocumentChange(DocumentChange {
                document: Some(d),
                target_ids: vec![TARGET_ID],
                removed_target_ids: vec![],
            }),
            None => ResponseType::DocumentDelete(DocumentDelete {
                document: update.name().to_string(),
                removed_target_ids: vec![TARGET_ID],
                read_time: Some(event.update_time.clone()),
            }),
        };
        Ok(vec![msg])
    }
}

struct QueryTarget {
    query: Arc<Query>,
    doctargets_by_name: HashMap<DefaultAtom, DocumentTarget>,
}
impl QueryTarget {
    async fn process_event(
        &mut self,
        database: &Arc<Database>,
        event: DatabaseEvent,
    ) -> Result<Vec<ResponseType>> {
        // TODO: do not reset if not needed (which is very often)
        let reset = 'reset: {
            for update in &event.updates {
                match update {
                    DocumentVersion::Deleted(deleted) => {
                        if self.doctargets_by_name.contains_key(&deleted.name) {
                            break 'reset true;
                        }
                    }
                    DocumentVersion::Stored(stored) => {
                        if self.query.includes_document(stored)? {
                            break 'reset true;
                        }
                    }
                }
            }
            false
        };

        if !reset {
            return Ok(vec![]);
        }

        self.reset(database, &event.update_time).await
    }

    async fn reset(
        &mut self,
        database: &Arc<Database>,
        time: &Timestamp,
    ) -> Result<Vec<ResponseType>> {
        let mut msgs = vec![ResponseType::TargetChange(TargetChange {
            target_change_type: target_change::TargetChangeType::Reset as _,
            target_ids: vec![TARGET_ID],
            cause: None,
            resume_token: vec![],
            read_time: None,
        })];

        self.doctargets_by_name.clear();
        for doc in self.query.once(database).await? {
            let name = DefaultAtom::from(&*doc.name);
            self.doctargets_by_name.insert(
                name.clone(),
                DocumentTarget {
                    name,
                    last_read_time: time.clone(),
                },
            );
            msgs.push(ResponseType::DocumentChange(DocumentChange {
                document: Some(doc),
                target_ids: vec![TARGET_ID],
                removed_target_ids: vec![],
            }))
        }

        Ok(msgs)
    }
}

fn show_response_type(rt: &ResponseType) -> &'static str {
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
