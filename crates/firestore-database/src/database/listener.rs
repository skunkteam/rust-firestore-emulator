use std::{
    collections::HashMap,
    marker::Unpin,
    sync::{
        atomic::{self, AtomicUsize},
        Weak,
    },
};

use googleapis::google::{
    firestore::v1::{
        listen_request,
        listen_response::ResponseType,
        target::{self, query_target},
        target_change::TargetChangeType,
        DocumentChange, DocumentDelete, ListenRequest, ListenResponse, Target, TargetChange,
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use tokio::sync::{broadcast::error::RecvError, mpsc};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{debug, error, instrument};

use super::{
    document::DocumentVersion, event::DatabaseEvent, query::Query, reference::DocumentRef,
    target_change, Database,
};
use crate::{
    database::{reference::Ref, ReadConsistency},
    error::Result,
    required_option, unimplemented, unimplemented_bool, unimplemented_collection,
    unimplemented_option, GenericDatabaseError,
};

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
    database: Weak<Database>,
    sender: mpsc::Sender<Result<ListenResponse>>,
    target: Option<ListenerTarget>,
}

impl Listener {
    pub fn start(
        database: Weak<Database>,
        request_stream: impl tokio_stream::Stream<Item = ListenRequest> + Send + Unpin,
    ) -> ReceiverStream<Result<ListenResponse>> {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

        let (sender, rx) = mpsc::channel(16);
        let listener = Self {
            id: NEXT_ID.fetch_add(1, atomic::Ordering::Relaxed),
            database,
            sender,
            // No target yet, will be added by a message in the request stream.
            target: None,
        };

        tokio::spawn(listener.go(request_stream));

        ReceiverStream::new(rx)
    }

    #[instrument(name = "listener", skip_all, fields(id = self.id), err)]
    async fn go(
        mut self,
        mut request_stream: impl tokio_stream::Stream<Item = ListenRequest> + Send + Unpin,
    ) -> Result<()> {
        let mut database_events = {
            let Some(database) = self.database.upgrade() else {
                return Ok(());
            };
            database.subscribe()
        };
        // Every Listener has a single event-loop, this is it:
        loop {
            tokio::select! {
                req = request_stream.next() => {
                    match req {
                        Some(msg) => {
                            if let Err(err) = self.process_request(msg).await {
                                self.send_err(err).await?;
                            };
                        }
                        // We're done, drop the listener.
                        None => return Ok(())
                    }
                }

                database_event = database_events.recv() => {
                    match database_event {
                        Ok(event) => self.process_event(&event).await?,
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

                if self.target.is_some() {
                    unimplemented!("target already set inside this listen stream")
                }
                if target_id != TARGET_ID {
                    unimplemented!("target_id should always be 1")
                }

                match target_type {
                    target::TargetType::Query(target::QueryTarget { parent, query_type }) => {
                        required_option!(query_type);
                        let query_target::QueryType::StructuredQuery(query) = query_type;
                        let parent: Ref = parent.parse()?;
                        let query =
                            Query::from_structured(parent, query, ReadConsistency::Default)?;
                        self.set_query(query).await?;
                    }
                    target::TargetType::Documents(target::DocumentsTarget { documents }) => {
                        let Ok(document) = documents.into_iter().exactly_one() else {
                            unimplemented!("multiple documents inside a single listen stream")
                        };
                        self.set_document(document.parse()?, resume_type).await?;
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
    async fn process_event(&mut self, event: &DatabaseEvent) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are
        // processed. That's why we know for sure that the output stream is not used for
        // something else until we respond with our NO_CHANGE msg. That msg means that everything is
        // up to date until that point and this is (for now) the easiest way to make sure
        // that is actually the case. This is probably okay, but if it becomes a hotspot we
        // might look into optimizing later.
        let Some(target) = &mut self.target else {
            return Ok(());
        };
        let Some(database) = self.database.upgrade() else {
            return Ok(());
        };

        let update_time = event.update_time.clone();
        let msgs = target.process_event(&database, event).await?;

        if msgs.is_empty() {
            return Ok(());
        }

        self.send_all(msgs).await?;
        self.send_complete(update_time).await
    }

    #[instrument(skip_all, fields(document = %name), err)]
    async fn set_document(
        &mut self,
        name: DocumentRef,
        resume_type: Option<target::ResumeType>,
    ) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are
        // processed. That's why we know for sure that the output stream is not used for
        // something else until we respond with our NO_CHANGE msg. That msg means that everything is
        // up to date until that point and this is (for now) the easiest way to make sure
        // that is actually the case. This is probably okay, but if it becomes a hotspot we
        // might look into optimizing later.
        let Some(database) = self.database.upgrade() else {
            return Ok(());
        };

        let send_if_newer_than = resume_type
            .map(|rt| match rt {
                target::ResumeType::ResumeToken(token) => {
                    Timestamp::from_token(token).map_err(GenericDatabaseError::invalid_argument)
                }
                target::ResumeType::ReadTime(time) => Ok(time),
            })
            .transpose()?;

        // Response: I'm on it!
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Add as _,
            target_ids: vec![TARGET_ID],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let read_time = Timestamp::now();
        debug!(name = %name);

        // Now determine the latest version we can find...
        let doc = database.get_doc(&name, &ReadConsistency::Default).await?;

        // Only send if newer than the resume_token
        let send_initial = match &send_if_newer_than {
            Some(previous_time) => !doc
                .as_ref()
                .is_some_and(|v| (v.update_time.as_ref().unwrap()) <= (previous_time)),
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

        self.send_complete(read_time.clone()).await?;

        self.target = Some(ListenerTarget::DocumentTarget(DocumentTarget {
            name,
            last_read_time: read_time,
        }));

        Ok(())
    }

    async fn set_query(&mut self, query: Query) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are
        // processed. That's why we know for sure that the output stream is not used for
        // something else until we respond with our NO_CHANGE msg. That msg means that everything is
        // up to date until that point and this is (for now) the easiest way to make sure
        // that is actually the case. This is probably okay, but if it becomes a hotspot we
        // might look into optimizing later.
        let Some(database) = self.database.upgrade() else {
            return Ok(());
        };

        // Response: I'm on it!
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Add as _,
            target_ids: vec![TARGET_ID],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let read_time = Timestamp::now();
        let mut target = QueryTarget::new(query);
        let msgs = target.reset(&database, &read_time).await?;
        self.send_all(msgs).await?;
        self.send_complete(read_time).await?;

        self.target = Some(ListenerTarget::QueryTarget(target));

        Ok(())
    }

    async fn send_complete(&self, read_time: Timestamp) -> Result<()> {
        let resume_token = (read_time)
            .get_token()
            .expect("Timestamp should not be outside of common era");
        self.send_all([
            // Response: I've sent you the state of all documents now
            ResponseType::TargetChange(TargetChange {
                target_change_type: TargetChangeType::Current as _,
                target_ids: vec![TARGET_ID],
                read_time: Some(read_time.clone()),
                resume_token: resume_token.clone(),
                ..TARGET_CHANGE_DEFAULT
            }),
            // Response: Oh, by the way, everything is up to date. 🤷🏼‍♂️
            ResponseType::TargetChange(TargetChange {
                read_time: Some(read_time),
                resume_token,
                ..TARGET_CHANGE_DEFAULT
            }),
        ])
        .await
    }

    async fn send_all(&self, msgs: impl IntoIterator<Item = ResponseType>) -> Result<()> {
        for msg in msgs {
            self.send(msg).await?;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(message = display(show_response_type(&response_type))), err)]
    async fn send(&self, response_type: ResponseType) -> Result<()> {
        self.sender
            .send(Ok(ListenResponse {
                response_type: Some(response_type),
            }))
            .await
            .map_err(|_| GenericDatabaseError::cancelled("stream closed"))
    }

    #[instrument(skip(self), err)]
    async fn send_err(&self, err: GenericDatabaseError) -> Result<()> {
        self.sender
            .send(Err(err))
            .await
            .map_err(|_| GenericDatabaseError::cancelled("stream closed"))
    }
}

enum ListenerTarget {
    DocumentTarget(DocumentTarget),
    QueryTarget(QueryTarget),
}

impl ListenerTarget {
    async fn process_event(
        &mut self,
        database: &Database,
        event: &DatabaseEvent,
    ) -> Result<Vec<ResponseType>> {
        match self {
            ListenerTarget::DocumentTarget(target) => target.process_event(event),
            ListenerTarget::QueryTarget(target) => target.process_event(database, event).await,
        }
    }
}

struct DocumentTarget {
    name: DocumentRef,
    last_read_time: Timestamp,
}

impl DocumentTarget {
    fn process_event(&mut self, event: &DatabaseEvent) -> Result<Vec<ResponseType>> {
        if let Some(update) = event.updates.get(&self.name) {
            self.process_update(update)
        } else {
            Ok(vec![])
        }
    }

    fn process_update(&mut self, update: &DocumentVersion) -> Result<Vec<ResponseType>> {
        debug_assert_eq!(update.name(), &self.name);
        let update_time = update.update_time().clone();
        if self.last_read_time >= update_time {
            return Ok(vec![]);
        }
        self.last_read_time = update_time.clone();

        let msg = match update.to_document() {
            Some(d) => ResponseType::DocumentChange(DocumentChange {
                document: Some(d),
                target_ids: vec![TARGET_ID],
                removed_target_ids: vec![],
            }),
            None => ResponseType::DocumentDelete(DocumentDelete {
                document: update.name().to_string(),
                removed_target_ids: vec![TARGET_ID],
                read_time: Some(update_time),
            }),
        };
        Ok(vec![msg])
    }
}

struct QueryTarget {
    query: Query,
    reset_on_update: bool,
    doctargets_by_name: HashMap<DocumentRef, DocumentTarget>,
}
impl QueryTarget {
    fn new(query: Query) -> Self {
        let reset_on_update = query.reset_on_update();
        Self {
            query,
            reset_on_update,
            doctargets_by_name: Default::default(),
        }
    }

    async fn process_event(
        &mut self,
        database: &Database,
        event: &DatabaseEvent,
    ) -> Result<Vec<ResponseType>> {
        let mut updates_to_apply = vec![];
        let needs_reset = 'reset: {
            for update in event.updates.values() {
                let name = update.name();
                let doc = update.stored_document();
                let target = self.doctargets_by_name.get_mut(name);
                let should_be_included =
                    matches!(doc, Some(doc) if self.query.includes_document(doc)?);
                if should_be_included != target.is_some() {
                    break 'reset true;
                }
                let Some(target) = target else {
                    continue;
                };
                if self.reset_on_update {
                    break 'reset true;
                }
                updates_to_apply.extend(target.process_update(update)?);
            }
            false
        };

        if needs_reset {
            self.reset(database, &event.update_time).await
        } else {
            Ok(updates_to_apply)
        }
    }

    async fn reset(&mut self, database: &Database, time: &Timestamp) -> Result<Vec<ResponseType>> {
        let mut msgs = vec![ResponseType::TargetChange(TargetChange {
            target_change_type: target_change::TargetChangeType::Reset as _,
            target_ids: vec![TARGET_ID],
            cause: None,
            resume_token: vec![],
            read_time: None,
        })];

        self.doctargets_by_name.clear();
        for (name, doc) in self.query.once(database).await? {
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
