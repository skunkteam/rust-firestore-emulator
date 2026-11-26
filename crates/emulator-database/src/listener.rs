use std::{
    collections::HashMap,
    marker::Unpin,
    sync::{
        Arc,
        atomic::{self, AtomicUsize},
    },
};

use googleapis::google::{
    firestore::v1::{
        Document, DocumentChange, DocumentDelete, DocumentRemove, ListenRequest, ListenResponse,
        Target, TargetChange, listen_request,
        listen_response::ResponseType,
        target::{self, query_target},
        target_change::TargetChangeType,
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use tokio::sync::mpsc;
use tokio_stream::{
    StreamExt, StreamMap, StreamNotifyClose,
    wrappers::{BroadcastStream, ReceiverStream, errors::BroadcastStreamRecvError},
};
use tracing::{Level, debug, error, instrument, warn};

use crate::{
    DatabaseEdition, FirestoreDatabase, FirestoreProject, GenericDatabaseError,
    database::{
        read_consistency::{ReadConsistency, validate_read_time},
        reference::Ref,
    },
    document::DocumentVersion,
    error::Result,
    event::DatabaseEvent,
    query::Query,
    reference::{DocumentRef, RootRef},
    required_option, unimplemented, unimplemented_bool, unimplemented_collection,
    unimplemented_option,
};

const TARGET_ID: i32 = 1;
const MAX_RESET_RETRIES: usize = 3;

const TARGET_CHANGE_DEFAULT: TargetChange = TargetChange {
    target_change_type: TargetChangeType::NoChange as _,
    target_ids: Vec::new(),
    cause: None,
    resume_token: Vec::new(),
    read_time: None,
};

fn target_reset() -> ResponseType {
    ResponseType::TargetChange(TargetChange {
        target_change_type: TargetChangeType::Reset as _,
        target_ids: vec![TARGET_ID],
        ..TARGET_CHANGE_DEFAULT
    })
}

fn document_change(document: Document) -> ResponseType {
    ResponseType::DocumentChange(DocumentChange {
        document: Some(document),
        target_ids: vec![TARGET_ID],
        removed_target_ids: vec![],
    })
}

fn document_delete(document: &DocumentRef, read_time: Timestamp) -> ResponseType {
    ResponseType::DocumentDelete(DocumentDelete {
        document: document.to_string(),
        removed_target_ids: vec![TARGET_ID],
        read_time: Some(read_time),
    })
}

fn document_remove(document: &DocumentRef, read_time: Timestamp) -> ResponseType {
    ResponseType::DocumentRemove(DocumentRemove {
        document: document.to_string(),
        removed_target_ids: vec![TARGET_ID],
        read_time: Some(read_time),
    })
}

/// Accumulates `(doc_ref, item)` pairs without allocating or sorting when at most one item is
/// collected; sorts deterministically by [`DocumentRef`] (`Ord`) when `len > 1`.
enum OrderedUpdates<'a, T> {
    Empty,
    One(&'a DocumentRef, T),
    Many(Vec<(&'a DocumentRef, T)>),
}

impl<'a, T> OrderedUpdates<'a, T> {
    fn push(&mut self, doc_ref: &'a DocumentRef, item: T) {
        match std::mem::replace(self, Self::Empty) {
            Self::Empty => *self = Self::One(doc_ref, item),
            Self::One(first_ref, first_item) => {
                *self = Self::Many(vec![(first_ref, first_item), (doc_ref, item)]);
            }
            Self::Many(mut items) => {
                items.push((doc_ref, item));
                *self = Self::Many(items);
            }
        }
    }

    fn into_sorted_vec<U>(self, mut map: impl FnMut(T) -> U) -> Vec<U> {
        match self {
            Self::Empty => Vec::new(),
            Self::One(_, item) => vec![map(item)],
            Self::Many(mut items) => {
                items.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
                items.into_iter().map(|(_, item)| map(item)).collect()
            }
        }
    }
}

pub(crate) struct Listener {
    /// For debug purposes.
    id: usize,
    project: &'static FirestoreProject,
    database_events: StreamMap<RootRef, StreamNotifyClose<BroadcastStream<Arc<DatabaseEvent>>>>,
    sender: mpsc::Sender<Result<ListenResponse>>,
    target: Option<ListenerTarget>,
}

impl Listener {
    pub(crate) fn start(
        project: &'static FirestoreProject,
        request_stream: impl tokio_stream::Stream<Item = ListenRequest> + Send + Unpin + 'static,
    ) -> ReceiverStream<Result<ListenResponse>> {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

        let (sender, rx) = mpsc::channel(16);
        let listener = Self {
            id: NEXT_ID.fetch_add(1, atomic::Ordering::Relaxed),
            project,
            database_events: Default::default(),
            sender,
            // No target yet, will be added by a message in the request stream.
            target: None,
        };

        tokio::spawn(listener.go(request_stream));

        ReceiverStream::new(rx)
    }

    #[instrument(level = Level::DEBUG, name = "listener", skip_all, fields(id = self.id), err(level = Level::DEBUG))]
    async fn go(
        mut self,
        mut request_stream: impl tokio_stream::Stream<Item = ListenRequest> + Send + Unpin,
    ) -> Result<()> {
        // Every Listener has a single event-loop, this is it:
        loop {
            tokio::select! {
                _ = self.sender.closed() => {
                    debug!("response stream closed");
                    return Ok(());
                }

                req = request_stream.next() => {
                    match req {
                        Some(msg) => {
                            if let Err(err) = self.process_request(msg).await {
                                self.send_err(err).await?;
                                return Ok(());
                            }
                        }
                        // We're done, drop the listener.
                        None => {
                            debug!("request stream finished");
                            return Ok(());
                        }
                    }
                }

                Some(database_event) = self.database_events.next() => {
                    match database_event {
                        // Database was dropped, reconnect to automatically create a new Database.
                        (db_name, None) => {
                            let database = reacquire_database(
                                self.project,
                                &mut self.database_events,
                                &db_name,
                            )
                            .await;
                            if let Err(err) = self.reset(database).await {
                                self.send_err(err).await?;
                                return Ok(());
                            }
                        }
                        // Received an event from the database.
                        (_, Some(Ok(event))) => {
                            if let Err(err) = self.process_event(&event).await {
                                self.send_err(err).await?;
                                return Ok(());
                            }
                        }
                        // Buffer overflow, we weren't able to keep up with the amount of events of one of the databases.
                        (db_name, Some(Err(BroadcastStreamRecvError::Lagged(count)))) => {
                            warn!(
                                id = self.id,
                                count = count,
                                "listener missed {} events, because of buffer overflow",
                                count
                            );
                            let database = reacquire_database(
                                self.project,
                                &mut self.database_events,
                                &db_name,
                            )
                            .await;
                            if let Err(err) = self.reset(database).await {
                                self.send_err(err).await?;
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }
    }

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
    async fn process_request(&mut self, msg: ListenRequest) -> Result<()> {
        let ListenRequest {
            database,
            labels,
            target_change,
            request_options,
        } = msg;
        unimplemented_collection!(labels);
        unimplemented_option!(request_options);
        required_option!(target_change);

        let database = self.project.database(&database.parse()?).await;

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
                debug!(target_id);
                if target_id != TARGET_ID {
                    unimplemented!("target_id should always be 1")
                }

                match target_type {
                    target::TargetType::Query(target::QueryTarget { parent, query_type }) => {
                        required_option!(query_type);
                        let query_target::QueryType::StructuredQuery(query) = query_type;
                        let parent: Ref = parent.parse()?;
                        if parent.root() != &database.name {
                            return Err(GenericDatabaseError::invalid_argument(format!(
                                "query parent {parent} does not belong to database {}",
                                database.name
                            )));
                        }
                        let query = Query::from_structured(
                            parent,
                            query,
                            ReadConsistency::Default,
                            database.enterprise_edition(),
                        )?;
                        self.set_query(database, query).await?;
                    }
                    target::TargetType::Documents(target::DocumentsTarget { documents }) => {
                        let Ok(document) = documents.into_iter().exactly_one() else {
                            unimplemented!("multiple documents inside a single listen stream")
                        };
                        let doc_ref: DocumentRef = document.parse()?;
                        if doc_ref.collection_ref.root_ref != database.name {
                            return Err(GenericDatabaseError::invalid_argument(format!(
                                "document {doc_ref} does not belong to database {}",
                                database.name
                            )));
                        }
                        self.set_document(database, doc_ref, resume_type).await?;
                    }
                };
            }
            listen_request::TargetChange::RemoveTarget(target_id) => {
                unimplemented!(format!("RemoveTarget: {target_id}"));
            }
        };
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
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
        let Some(database) = event.database.upgrade() else {
            return Ok(());
        };
        if database.is_cleared() {
            return Ok(());
        }

        let (complete_time, msgs) = match target.process_event(&database, event).await {
            Ok(res) => res,
            Err(err) if is_cleared_retryable(&database, &err) => return Ok(()),
            Err(err) => return Err(err),
        };

        if msgs.is_empty() {
            return Ok(());
        }

        self.send_all(msgs).await?;
        self.send_complete(complete_time).await
    }

    async fn reset(&mut self, database: Arc<FirestoreDatabase>) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are
        // processed. That's why we know for sure that the output stream is not used for
        // something else until we respond with our NO_CHANGE msg. That msg means that everything is
        // up to date until that point and this is (for now) the easiest way to make sure
        // that is actually the case. This is probably okay, but if it becomes a hotspot we
        // might look into optimizing later.
        let Some(target) = &mut self.target else {
            return Ok(());
        };

        let (time, msgs) =
            reset_target(self.project, &mut self.database_events, target, database).await?;
        self.send_all(msgs).await?;
        self.send_complete(time).await?;
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
    async fn set_document(
        &mut self,
        mut database: Arc<FirestoreDatabase>,
        name: DocumentRef,
        resume_type: Option<target::ResumeType>,
    ) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are
        // processed. That's why we know for sure that the output stream is not used for
        // something else until we respond with our NO_CHANGE msg. That msg means that everything is
        // up to date until that point and this is (for now) the easiest way to make sure
        // that is actually the case. This is probably okay, but if it becomes a hotspot we
        // might look into optimizing later.
        self.ensure_subscribed_to(&database);

        let send_if_newer_than = resume_type
            .map(|rt| match rt {
                target::ResumeType::ResumeToken(token) => {
                    Timestamp::from_token(token).map_err(GenericDatabaseError::invalid_argument)
                }
                target::ResumeType::ReadTime(time) => validate_read_time(time),
            })
            .transpose()?;

        // Response: I'm on it!
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Add as _,
            target_ids: vec![TARGET_ID],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let mut retries = 0;
        let (read_time, doc) = loop {
            let read_time = Timestamp::now();
            debug!(%name);

            // Now determine the latest version at the pinned snapshot timestamp so concurrent
            // writes after `read_time` are delivered via the already-subscribed broadcast channel
            // instead of duplicating the initial snapshot.
            match database
                .get_doc(&name, ReadConsistency::ReadTime(read_time))
                .await
            {
                Ok(doc) => break (read_time, doc),
                Err(err)
                    if is_cleared_retryable(&database, &err) && retries < MAX_RESET_RETRIES =>
                {
                    retries += 1;
                    database =
                        reacquire_database(self.project, &mut self.database_events, &database.name)
                            .await;
                }
                Err(err) => return Err(err),
            }
        };

        // Only send if newer than the resume_token
        let send_initial = match &send_if_newer_than {
            Some(previous_time) => doc.as_ref().is_none_or(|v| &v.update_time > previous_time),
            _ => true,
        };

        if send_initial {
            // Response: This is the current version, whether you like it or not.
            let msg = match doc {
                Some(d) => document_change(d.to_document()),
                None => document_delete(&name, read_time),
            };
            self.send(msg).await?;
        }

        self.send_complete(read_time).await?;

        self.target = Some(ListenerTarget::DocumentTarget(DocumentTarget {
            name,
            last_read_time: read_time,
        }));

        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
    async fn set_query(
        &mut self,
        database: Arc<FirestoreDatabase>,
        query: Box<Query>,
    ) -> Result<()> {
        // We rely on the fact that this function will complete before any other events are
        // processed. That's why we know for sure that the output stream is not used for
        // something else until we respond with our NO_CHANGE msg. That msg means that everything is
        // up to date until that point and this is (for now) the easiest way to make sure
        // that is actually the case. This is probably okay, but if it becomes a hotspot we
        // might look into optimizing later.
        self.ensure_subscribed_to(&database);

        debug!(?query);

        // Response: I'm on it!
        self.send(ResponseType::TargetChange(TargetChange {
            target_change_type: TargetChangeType::Add as _,
            target_ids: vec![TARGET_ID],
            ..TARGET_CHANGE_DEFAULT
        }))
        .await?;

        let mut target = ListenerTarget::QueryTarget(QueryTarget::new(query));
        let (read_time, msgs) = reset_target(
            self.project,
            &mut self.database_events,
            &mut target,
            database,
        )
        .await?;
        self.send_all(msgs).await?;
        self.send_complete(read_time).await?;

        self.target = Some(target);

        Ok(())
    }

    fn ensure_subscribed_to(&mut self, database: &FirestoreDatabase) {
        if !self.database_events.contains_key(&database.name) {
            self.database_events.insert(
                database.name.clone(),
                StreamNotifyClose::new(BroadcastStream::new(database.subscribe())),
            );
        }
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
                read_time: Some(read_time),
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

    #[instrument(level = Level::DEBUG, skip_all, fields(message = display(show_response_type(&response_type))), err(level = Level::DEBUG))]
    async fn send(&self, response_type: ResponseType) -> Result<()> {
        self.sender
            .send(Ok(ListenResponse {
                response_type: Some(response_type),
            }))
            .await
            .map_err(|_| GenericDatabaseError::cancelled("stream closed"))
    }

    #[instrument(level = Level::DEBUG, skip(self), err(level = Level::DEBUG))]
    async fn send_err(&self, err: GenericDatabaseError) -> Result<()> {
        self.sender
            .send(Err(err))
            .await
            .map_err(|_| GenericDatabaseError::cancelled("stream closed"))
    }
}

fn is_cleared_retryable(database: &FirestoreDatabase, err: &GenericDatabaseError) -> bool {
    database.is_cleared() && !matches!(err, GenericDatabaseError::FailedPrecondition(_))
}

async fn reacquire_database(
    project: &'static FirestoreProject,
    database_events: &mut StreamMap<
        RootRef,
        StreamNotifyClose<BroadcastStream<Arc<DatabaseEvent>>>,
    >,
    db_name: &RootRef,
) -> Arc<FirestoreDatabase> {
    let database = project.database(db_name).await;
    database_events.insert(
        database.name.clone(),
        StreamNotifyClose::new(BroadcastStream::new(database.subscribe())),
    );
    database
}

async fn reset_target(
    project: &'static FirestoreProject,
    database_events: &mut StreamMap<
        RootRef,
        StreamNotifyClose<BroadcastStream<Arc<DatabaseEvent>>>,
    >,
    target: &mut ListenerTarget,
    mut database: Arc<FirestoreDatabase>,
) -> Result<(Timestamp, Vec<ResponseType>)> {
    let mut retries = 0;
    loop {
        match target.reset(&database).await {
            Ok(res) => return Ok(res),
            Err(err) if is_cleared_retryable(&database, &err) && retries < MAX_RESET_RETRIES => {
                retries += 1;
                database = reacquire_database(project, database_events, &database.name).await;
            }
            Err(err) => return Err(err),
        }
    }
}

enum ListenerTarget {
    DocumentTarget(DocumentTarget),
    QueryTarget(QueryTarget),
}

impl ListenerTarget {
    async fn process_event(
        &mut self,
        database: &FirestoreDatabase,
        event: &DatabaseEvent,
    ) -> Result<(Timestamp, Vec<ResponseType>)> {
        match self {
            ListenerTarget::DocumentTarget(target) => {
                Ok((event.update_time, target.process_event(event)?))
            }
            ListenerTarget::QueryTarget(target) => target.process_event(database, event).await,
        }
    }

    async fn reset(
        &mut self,
        database: &FirestoreDatabase,
    ) -> Result<(Timestamp, Vec<ResponseType>)> {
        match self {
            ListenerTarget::DocumentTarget(target) => target.reset(database).await,
            ListenerTarget::QueryTarget(target) => target.reset(database).await,
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
        let update_time = update.update_time();
        if self.last_read_time >= update_time {
            return Ok(vec![]);
        }
        self.last_read_time = update_time;

        let msg = match update.to_document() {
            Some(d) => document_change(d),
            None => document_delete(update.name(), update_time),
        };
        Ok(vec![msg])
    }

    async fn reset(
        &mut self,
        database: &FirestoreDatabase,
    ) -> Result<(Timestamp, Vec<ResponseType>)> {
        let read_time = Timestamp::now();
        let doc = database
            .get_doc(&self.name, ReadConsistency::ReadTime(read_time))
            .await?;
        self.last_read_time = read_time;

        let mut msgs = vec![target_reset()];
        msgs.push(match doc {
            Some(d) => document_change(d.to_document()),
            None => document_delete(&self.name, read_time),
        });
        Ok((read_time, msgs))
    }
}

struct QueryTarget {
    query: Box<Query>,
    reset_on_update: bool,
    last_reset_time: Timestamp,
    doctargets_by_name: HashMap<DocumentRef, DocumentTarget>,
}
impl QueryTarget {
    fn new(query: Box<Query>) -> Self {
        let reset_on_update = query.reset_on_update();
        Self {
            query,
            reset_on_update,
            last_reset_time: Timestamp::default(),
            doctargets_by_name: Default::default(),
        }
    }

    fn ensure_matching_edition(&self, database: &FirestoreDatabase) -> Result<()> {
        let query_enterprise = self.query.enterprise_edition();
        let db_enterprise = database.enterprise_edition();
        if query_enterprise != db_enterprise {
            let query_edition = DatabaseEdition::from_enterprise(query_enterprise);
            let db_edition = DatabaseEdition::from_enterprise(db_enterprise);
            error!(
                database = %database.name,
                %query_edition,
                %db_edition,
                "Database {} edition changed from {} to {}; terminating query listener",
                database.name,
                query_edition,
                db_edition
            );
            return Err(GenericDatabaseError::failed_precondition(format!(
                "Database {} edition changed from {} to {}; re-create query listener",
                database.name, query_edition, db_edition
            )));
        }
        Ok(())
    }

    async fn process_event(
        &mut self,
        database: &FirestoreDatabase,
        event: &DatabaseEvent,
    ) -> Result<(Timestamp, Vec<ResponseType>)> {
        self.ensure_matching_edition(database)?;
        if event.update_time <= self.last_reset_time {
            return Ok((self.last_reset_time, vec![]));
        }

        if self.query.has_find_nearest() {
            Ok((event.update_time, self.process_find_nearest_event(event)))
        } else {
            self.process_standard_event(database, event).await
        }
    }

    fn process_find_nearest_event(&mut self, event: &DatabaseEvent) -> Vec<ResponseType> {
        let mut updates_to_apply = OrderedUpdates::Empty;
        for update in event.updates.values() {
            let name = update.name();
            let update_time = update.update_time();
            match update.stored_document() {
                Some(doc) => {
                    if self.query.matches_prefilters(doc) {
                        if let Some(target) = self.doctargets_by_name.get_mut(name) {
                            if target.last_read_time >= update_time {
                                continue;
                            }
                            target.last_read_time = update_time;
                        } else {
                            self.doctargets_by_name.insert(
                                name.clone(),
                                DocumentTarget {
                                    name: name.clone(),
                                    last_read_time: update_time,
                                },
                            );
                        }
                        updates_to_apply.push(name, document_change(self.query.project_base(doc)));
                    } else if let Some(target) = self.doctargets_by_name.get(name)
                        && target.last_read_time < update_time
                    {
                        self.doctargets_by_name.remove(name);
                        updates_to_apply.push(name, document_remove(name, update_time));
                    }
                }
                None => {
                    if let Some(target) = self.doctargets_by_name.get(name)
                        && target.last_read_time < update_time
                    {
                        self.doctargets_by_name.remove(name);
                        updates_to_apply.push(name, document_delete(name, update_time));
                    }
                }
            }
        }
        updates_to_apply.into_sorted_vec(std::convert::identity)
    }

    async fn process_standard_event(
        &mut self,
        database: &FirestoreDatabase,
        event: &DatabaseEvent,
    ) -> Result<(Timestamp, Vec<ResponseType>)> {
        let mut updates_to_apply = OrderedUpdates::Empty;
        let needs_reset = 'reset: {
            for update in event.updates.values() {
                let name = update.name();
                let doc = update.stored_document();
                let should_be_included =
                    matches!(doc, Some(doc) if self.query.includes_document(doc)?);
                let target = self.doctargets_by_name.get_mut(name);
                if should_be_included != target.is_some()
                    || self.query.affects_offset_window(&name.collection_ref)
                {
                    break 'reset true;
                }
                let Some(target) = target else {
                    continue;
                };
                if self.reset_on_update {
                    break 'reset true;
                }
                if target.last_read_time < update.update_time() {
                    target.last_read_time = update.update_time();
                    if let Some(doc) = doc {
                        updates_to_apply.push(name, doc);
                    }
                }
            }
            false
        };

        if needs_reset {
            self.reset(database).await
        } else {
            Ok((
                event.update_time,
                updates_to_apply
                    .into_sorted_vec(|doc| document_change(self.query.project_base(doc))),
            ))
        }
    }

    async fn reset(
        &mut self,
        database: &FirestoreDatabase,
    ) -> Result<(Timestamp, Vec<ResponseType>)> {
        self.ensure_matching_edition(database)?;
        let mut msgs = vec![target_reset()];

        self.doctargets_by_name.clear();
        let (read_time, matches) = self.query.once(database).await?;
        self.last_reset_time = read_time;
        for m in matches {
            self.doctargets_by_name.insert(
                m.version.name.clone(),
                DocumentTarget {
                    name: m.version.name.clone(),
                    last_read_time: read_time,
                },
            );
            msgs.push(document_change(self.query.project(&m)));
        }

        Ok((read_time, msgs))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use googleapis::google::firestore::v1::{
        Document, ListenRequest, ListenResponse, Target, listen_request,
        listen_response::ResponseType,
        target::{self, DocumentsTarget},
    };
    use tokio::sync::mpsc;
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};

    use crate::{FirestoreConfig, FirestoreProject, reference::RootRef};

    #[tokio::test]
    async fn dropping_response_stream_terminates_idle_listener_task() {
        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let (req_tx, req_rx) = mpsc::channel(4);

        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(ListenRequest {
                database: root.to_string(),
                labels: Default::default(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Documents(DocumentsTarget {
                        documents: vec![format!("{root}/col/doc1")],
                    })),
                    resume_type: None,
                })),
                request_options: None,
            })
            .await
            .unwrap();

        // Wait until initial snapshot framing reaches TargetChange(NoChange).
        while let Some(frame) = resp_stream.next().await {
            let frame = frame.unwrap();
            if let Some(ResponseType::TargetChange(tc)) = frame.response_type
                && tc.target_ids.is_empty()
            {
                break;
            }
        }

        // Drop the response stream while keeping `req_tx` alive on an idle database.
        drop(resp_stream);

        tokio::time::timeout(Duration::from_secs(1), req_tx.closed())
            .await
            .expect("listener task must exit when response receiver is dropped");
    }

    #[tokio::test]
    async fn query_and_document_target_watermarks_and_reset_consistency() {
        use googleapis::google::firestore::v1::{
            Precondition, Write, precondition, target_change::TargetChangeType, write,
        };

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        let doc_name = format!("{root}/col/doc1");

        // Pre-populate col/doc1.
        db.write(Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_name.clone(),
                ..Default::default()
            })),
            current_document: Some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(false)),
            }),
            ..Default::default()
        })
        .await
        .unwrap();

        // 1. DocumentTarget::reset emits TargetChange(Reset) + DocumentChange and ignores stale
        //    DatabaseEvents at or before the pinned reset read_time.
        let doc_ref: crate::reference::DocumentRef = doc_name.parse().unwrap();
        let mut doc_target = super::DocumentTarget {
            name: doc_ref.clone(),
            last_read_time: Default::default(),
        };
        let (doc_reset_time, doc_msgs) = doc_target.reset(&db).await.unwrap();
        assert_eq!(doc_target.last_read_time, doc_reset_time);
        assert_eq!(doc_msgs.len(), 2);
        assert!(matches!(
            &doc_msgs[0],
            ResponseType::TargetChange(tc)
                if tc.target_change_type == TargetChangeType::Reset as i32
        ));
        assert!(matches!(&doc_msgs[1], ResponseType::DocumentChange(_)));

        let stale_doc_event = crate::event::DatabaseEvent {
            database:    std::sync::Arc::downgrade(&db),
            update_time: doc_reset_time,
            updates:     [(
                doc_ref.clone(),
                crate::document::DocumentVersion::Stored(std::sync::Arc::new(
                    crate::document::StoredDocumentVersion {
                        name: doc_ref,
                        create_time: doc_reset_time,
                        update_time: doc_reset_time,
                        fields: Default::default(),
                    },
                )),
            )]
            .into(),
        };
        assert!(
            doc_target
                .process_event(&stale_doc_event)
                .unwrap()
                .is_empty()
        );

        // 2. QueryTarget::reset pins `last_reset_time` and ignores events at or before it.
        let mut target = super::QueryTarget::new(
            crate::query::QueryBuilder::from_collection(format!("{root}/col").parse().unwrap())
                .build()
                .unwrap()
                .into(),
        );
        let (reset_time, init_msgs) = target.reset(&db).await.unwrap();
        assert_eq!(init_msgs.len(), 2); // Reset + DocumentChange(doc1)

        // An event at `reset_time` (or earlier) must be ignored completely even for an unseen doc.
        let stale_doc_ref: crate::reference::DocumentRef =
            format!("{root}/col/transient").parse().unwrap();
        let stale_event = crate::event::DatabaseEvent {
            database:    std::sync::Arc::downgrade(&db),
            update_time: reset_time,
            updates:     [(
                stale_doc_ref.clone(),
                crate::document::DocumentVersion::Stored(std::sync::Arc::new(
                    crate::document::StoredDocumentVersion {
                        name: stale_doc_ref,
                        create_time: reset_time,
                        update_time: reset_time,
                        fields: Default::default(),
                    },
                )),
            )]
            .into(),
        };
        let (_, skipped) = target.process_event(&db, &stale_event).await.unwrap();
        assert!(skipped.is_empty());
    }

    #[tokio::test]
    async fn document_listener_rejects_invalid_resume_read_time() {
        use googleapis::google::protobuf::Timestamp;

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let (req_tx, req_rx) = mpsc::channel(4);

        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(ListenRequest {
                database: root.to_string(),
                labels: Default::default(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Documents(DocumentsTarget {
                        documents: vec![format!("{root}/col/doc1")],
                    })),
                    resume_type: Some(target::ResumeType::ReadTime(Timestamp {
                        seconds: -1,
                        nanos:   0,
                    })),
                })),
                request_options: None,
            })
            .await
            .unwrap();

        let mut saw_invalid_arg = false;
        while let Some(frame) = resp_stream.next().await {
            match frame {
                Err(crate::GenericDatabaseError::InvalidArgument(_)) => {
                    saw_invalid_arg = true;
                    break;
                }
                Ok(resp) => {
                    if let Some(ResponseType::TargetChange(tc)) = resp.response_type
                        && tc.target_ids.is_empty()
                    {
                        break;
                    }
                }
                Err(other) => panic!("unexpected listener error: {other:?}"),
            }
        }
        assert!(
            saw_invalid_arg,
            "expected InvalidArgument for negative resume_type ReadTime"
        );
    }

    async fn drain_until_no_change(
        stream: &mut (impl tokio_stream::Stream<Item = crate::error::Result<ListenResponse>> + Unpin),
    ) -> Vec<ResponseType> {
        let mut out = Vec::new();
        while let Some(frame) = stream.next().await {
            let resp = frame.expect("listener stream should not fail");
            let rt = resp.response_type.expect("response_type");
            let is_no_change = matches!(
                &rt,
                ResponseType::TargetChange(tc)
                    if tc.target_change_type == super::TargetChangeType::NoChange as i32
                        && tc.target_ids.is_empty()
            );
            out.push(rt);
            if is_no_change {
                break;
            }
        }
        out
    }

    #[tokio::test]
    async fn buffered_pre_clear_events_are_ignored_on_compatible_listeners() {
        use std::collections::HashMap;

        use googleapis::google::firestore::v1::{
            StructuredQuery, Value,
            structured_query::CollectionSelector,
            target::{self, QueryTarget as ProtoQueryTarget, query_target},
        };

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db1 = project.database(&root).await;
        assert!(!db1.enterprise_edition());
        let col: crate::reference::CollectionRef = format!("{root}/col").parse().unwrap();
        let doc_ref: crate::reference::DocumentRef = format!("{root}/col/doc1").parse().unwrap();

        // 1. Direct Listener::process_event ignores events whose database handle is cleared.
        let (sender, mut rx) = mpsc::channel(16);
        let mut listener = super::Listener {
            id: 999,
            project,
            database_events: Default::default(),
            sender,
            target: Some(super::ListenerTarget::DocumentTarget(
                super::DocumentTarget {
                    name: doc_ref.clone(),
                    last_read_time: Default::default(),
                },
            )),
        };

        let ts_event = googleapis::google::protobuf::Timestamp::now();
        let buffered_event = crate::event::DatabaseEvent {
            database:    std::sync::Arc::downgrade(&db1),
            update_time: ts_event,
            updates:     [(
                doc_ref.clone(),
                crate::document::DocumentVersion::Stored(std::sync::Arc::new(
                    crate::document::StoredDocumentVersion {
                        name: doc_ref.clone(),
                        create_time: ts_event,
                        update_time: ts_event,
                        fields: HashMap::from([("v".to_string(), Value::integer(1))]),
                    },
                )),
            )]
            .into(),
        };
        project.clear_database(&root).await;
        assert!(listener.process_event(&buffered_event).await.is_ok());
        assert!(
            rx.try_recv().is_err(),
            "cleared database event must emit no frames"
        );

        // 2. Live listen stream with buffered pre-clear event followed by replacement converges
        //    without aborting.
        let db_live = project.database(&root).await;
        db_live
            .create_doc(
                &col,
                "doc1",
                HashMap::from([("v".to_string(), Value::integer(10))]),
                None,
            )
            .await
            .unwrap();

        let (req_tx, req_rx) = mpsc::channel(4);
        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(ListenRequest {
                database: root.to_string(),
                labels: Default::default(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Query(ProtoQueryTarget {
                        parent:     root.to_string(),
                        query_type: Some(query_target::QueryType::StructuredQuery(
                            StructuredQuery {
                                from: vec![CollectionSelector {
                                    collection_id:   "col".to_string(),
                                    all_descendants: false,
                                }],
                                ..Default::default()
                            },
                        )),
                    })),
                    resume_type: None,
                })),
                request_options: None,
            })
            .await
            .unwrap();

        let _initial = drain_until_no_change(&mut resp_stream).await;

        // Enqueue a pre-clear event into `db_live`'s broadcast channel, then immediately clear and
        // replace `db_live`.
        db_live
            .update_doc(
                &doc_ref,
                HashMap::from([("v".to_string(), Value::integer(20))]),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        project.clear_database(&root).await;
        let db_next = project.database(&root).await;
        db_next
            .create_doc(
                &col,
                "doc1",
                HashMap::from([("v".to_string(), Value::integer(30))]),
                None,
            )
            .await
            .unwrap();

        let after_replace = drain_until_no_change(&mut resp_stream).await;
        let reset_count = after_replace
            .iter()
            .filter(|rt| {
                matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == super::TargetChangeType::Reset as i32
                )
            })
            .count();
        assert_eq!(reset_count, 1);
        assert!(after_replace.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(30))
            )
        }));

        drop(req_tx);
    }

    #[tokio::test]
    async fn lagged_resubscribes_before_reset_and_converges_in_single_reset() {
        use std::collections::HashMap;

        use googleapis::google::firestore::v1::Value;

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db1 = project.database(&root).await;
        assert!(!db1.enterprise_edition());
        let col: crate::reference::CollectionRef = format!("{root}/col").parse().unwrap();
        let doc_ref: crate::reference::DocumentRef = format!("{root}/col/doc1").parse().unwrap();

        db1.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
        )
        .await
        .unwrap();

        let (req_tx, req_rx) = mpsc::channel(4);
        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(ListenRequest {
                database: root.to_string(),
                labels: Default::default(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Documents(DocumentsTarget {
                        documents: vec![doc_ref.to_string()],
                    })),
                    resume_type: None,
                })),
                request_options: None,
            })
            .await
            .unwrap();

        let _initial = drain_until_no_change(&mut resp_stream).await;

        // Overflow `db1`'s broadcast buffer (> MAX_EVENT_BACKLOG) AND replace `db1` before
        // `Listener::go` polls `database_events` again. Re-subscribing inside the `Lagged` arm
        // ensures `old_db`'s closed channel never emits a second `(db_name, None)` reset.
        for _ in 0..(crate::database::MAX_EVENT_BACKLOG + 32) {
            db1.send_event(crate::event::DatabaseEvent {
                database:    std::sync::Arc::downgrade(&db1),
                update_time: googleapis::google::protobuf::Timestamp::now(),
                updates:     HashMap::new(),
            });
        }
        project.clear_database(&root).await;
        let db2 = project.database(&root).await;
        db2.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(2))]),
            None,
        )
        .await
        .unwrap();

        let recovery = drain_until_no_change(&mut resp_stream).await;
        let reset_count = recovery
            .iter()
            .filter(|rt| {
                matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == super::TargetChangeType::Reset as i32
                )
            })
            .count();
        assert_eq!(
            reset_count, 1,
            "Lagged recovery after replacement must emit exactly one Reset"
        );
        assert!(recovery.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(2))
            )
        }));

        // Verify the listener's subscription now tracks `db2` (not the dropped `db1`).
        db2.update_doc(
            &doc_ref,
            HashMap::from([("v".to_string(), Value::integer(3))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let follow_up = drain_until_no_change(&mut resp_stream).await;
        assert!(follow_up.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(3))
            )
        }));

        drop(req_tx);
    }

    #[tokio::test]
    async fn toctou_clear_retries_during_set_document_set_query_and_reset() {
        use std::collections::HashMap;

        use googleapis::google::firestore::v1::{Value, target_change::TargetChangeType};

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let col: crate::reference::CollectionRef = format!("{root}/col").parse().unwrap();
        let doc_ref: crate::reference::DocumentRef = format!("{root}/col/doc1").parse().unwrap();

        // 1. set_document retries onto replacement database without sending duplicate Add.
        let db1 = project.database(&root).await;
        project.clear_database(&root).await;
        let db2 = project.database(&root).await;
        db2.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(42))]),
            None,
        )
        .await
        .unwrap();

        let (sender, rx) = mpsc::channel(16);
        let mut resp_stream = ReceiverStream::new(rx);
        let mut listener = super::Listener {
            id: 1001,
            project,
            database_events: Default::default(),
            sender,
            target: None,
        };
        listener
            .set_document(std::sync::Arc::clone(&db1), doc_ref.clone(), None)
            .await
            .unwrap();
        let set_doc_msgs = drain_until_no_change(&mut resp_stream).await;
        let add_count = set_doc_msgs
            .iter()
            .filter(|rt| {
                matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == TargetChangeType::Add as i32
                )
            })
            .count();
        assert_eq!(add_count, 1);
        assert!(set_doc_msgs.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(42))
            )
        }));
        assert!(listener.database_events.contains_key(&root));

        // 2. reset on DocumentTarget retries onto replacement database.
        project.clear_database(&root).await;
        let db3 = project.database(&root).await;
        db3.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(77))]),
            None,
        )
        .await
        .unwrap();
        listener.reset(std::sync::Arc::clone(&db2)).await.unwrap();
        let reset_doc_msgs = drain_until_no_change(&mut resp_stream).await;
        let reset_count = reset_doc_msgs
            .iter()
            .filter(|rt| {
                matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == TargetChangeType::Reset as i32
                )
            })
            .count();
        assert_eq!(reset_count, 1);
        assert!(reset_doc_msgs.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(77))
            )
        }));

        // 3. set_query and reset on QueryTarget retry onto replacement database.
        let query = crate::query::QueryBuilder::from_collection(col.clone())
            .build()
            .unwrap()
            .into();
        project.clear_database(&root).await;
        let db4 = project.database(&root).await;
        db4.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(99))]),
            None,
        )
        .await
        .unwrap();
        let (q_sender, q_rx) = mpsc::channel(16);
        let mut q_stream = ReceiverStream::new(q_rx);
        let mut q_listener = super::Listener {
            id: 1002,
            project,
            database_events: Default::default(),
            sender: q_sender,
            target: None,
        };
        q_listener
            .set_query(std::sync::Arc::clone(&db3), query)
            .await
            .unwrap();
        let set_query_msgs = drain_until_no_change(&mut q_stream).await;
        assert_eq!(
            set_query_msgs
                .iter()
                .filter(|rt| matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == TargetChangeType::Add as i32
                ))
                .count(),
            1
        );
        assert_eq!(
            set_query_msgs
                .iter()
                .filter(|rt| matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == TargetChangeType::Reset as i32
                ))
                .count(),
            1
        );
        assert!(set_query_msgs.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(99))
            )
        }));

        // Reset QueryTarget across clear.
        project.clear_database(&root).await;
        let _db5 = project.database(&root).await;
        q_listener.reset(std::sync::Arc::clone(&db4)).await.unwrap();
        let reset_query_msgs = drain_until_no_change(&mut q_stream).await;
        assert_eq!(
            reset_query_msgs
                .iter()
                .filter(|rt| matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == TargetChangeType::Reset as i32
                ))
                .count(),
            1
        );

        // FailedPrecondition must not be swallowed or retried even if `database.is_cleared()`.
        db4.set_enterprise_edition(true);
        let err = q_listener
            .reset(std::sync::Arc::clone(&db4))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::GenericDatabaseError::FailedPrecondition(_)
        ));
        assert_eq!(super::MAX_RESET_RETRIES, 3);
        assert!(!super::is_cleared_retryable(&db4, &err));
        assert!(super::is_cleared_retryable(
            &db4,
            &crate::GenericDatabaseError::aborted("cleared"),
        ));
    }

    #[tokio::test]
    async fn single_reset_convergence_for_compatible_document_listener() {
        use std::collections::HashMap;

        use googleapis::google::firestore::v1::{Value, target_change::TargetChangeType};

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db1 = project.database(&root).await;
        assert!(!db1.enterprise_edition());
        let col: crate::reference::CollectionRef = format!("{root}/col").parse().unwrap();
        let doc_ref: crate::reference::DocumentRef = format!("{root}/col/doc1").parse().unwrap();

        db1.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
        )
        .await
        .unwrap();

        let (req_tx, req_rx) = mpsc::channel(4);
        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(ListenRequest {
                database: root.to_string(),
                labels: Default::default(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Documents(DocumentsTarget {
                        documents: vec![doc_ref.to_string()],
                    })),
                    resume_type: None,
                })),
                request_options: None,
            })
            .await
            .unwrap();

        let _initial = drain_until_no_change(&mut resp_stream).await;

        project.clear_database(&root).await;
        let db2 = project.database(&root).await;
        db2.create_doc(
            &col,
            "doc1",
            HashMap::from([("v".to_string(), Value::integer(2))]),
            None,
        )
        .await
        .unwrap();

        let msgs = drain_until_no_change(&mut resp_stream).await;
        let reset_count = msgs
            .iter()
            .filter(|rt| {
                matches!(
                    rt,
                    ResponseType::TargetChange(tc)
                        if tc.target_change_type == TargetChangeType::Reset as i32
                )
            })
            .count();
        assert_eq!(reset_count, 1);
        assert!(
            !msgs
                .iter()
                .any(|rt| matches!(rt, ResponseType::DocumentDelete(_))),
            "single-reset convergence must not observe an intermediate empty database"
        );
        assert!(msgs.iter().any(|rt| {
            matches!(
                rt,
                ResponseType::DocumentChange(dc)
                    if dc.document.as_ref().and_then(|d| d.fields.get("v"))
                        == Some(&Value::integer(2))
            )
        }));

        drop(req_tx);
    }

    #[tokio::test]
    async fn query_listener_terminates_with_failed_precondition_on_edition_change() {
        use googleapis::google::firestore::v1::{
            StructuredQuery,
            structured_query::CollectionSelector,
            target::{self, QueryTarget as ProtoQueryTarget, query_target},
        };

        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db1 = project.database(&root).await;
        assert!(!db1.enterprise_edition());
        let col: crate::reference::CollectionRef = format!("{root}/col").parse().unwrap();

        let (req_tx, req_rx) = mpsc::channel(4);
        let mut resp_stream = project.listen(ReceiverStream::new(req_rx));
        req_tx
            .send(ListenRequest {
                database: root.to_string(),
                labels: Default::default(),
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id: 1,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Query(ProtoQueryTarget {
                        parent:     root.to_string(),
                        query_type: Some(query_target::QueryType::StructuredQuery(
                            StructuredQuery {
                                from: vec![CollectionSelector {
                                    collection_id:   "col".to_string(),
                                    all_descendants: false,
                                }],
                                ..Default::default()
                            },
                        )),
                    })),
                    resume_type: None,
                })),
                request_options: None,
            })
            .await
            .unwrap();

        let _initial = drain_until_no_change(&mut resp_stream).await;

        // Replace `db1` (STANDARD) with `db2` configured as ENTERPRISE before yielding.
        project.clear_database(&root).await;
        let db2 = project.database(&root).await;
        db2.set_enterprise_edition(true);

        let err = resp_stream
            .next()
            .await
            .expect("stream must emit FailedPrecondition")
            .unwrap_err();
        assert_eq!(
            err,
            crate::GenericDatabaseError::failed_precondition(format!(
                "Database {root} edition changed from STANDARD to ENTERPRISE; re-create query \
                 listener"
            ))
        );
        assert!(
            resp_stream.next().await.is_none(),
            "listener stream must terminate immediately after FailedPrecondition"
        );

        // Also verify `QueryTarget::process_event` checks edition before the stale-timestamp
        // early return, including ENTERPRISE -> STANDARD direction.
        let mut ent_target = super::QueryTarget::new(
            crate::query::QueryBuilder::from_collection(col)
                .enterprise_edition(true)
                .build()
                .unwrap()
                .into(),
        );
        let (reset_time, _) = ent_target.reset(&db2).await.unwrap();
        db2.set_enterprise_edition(false);
        let stale_event = crate::event::DatabaseEvent {
            database:    std::sync::Arc::downgrade(&db2),
            update_time: reset_time,
            updates:     Default::default(),
        };
        let err = ent_target
            .process_event(&db2, &stale_event)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            crate::GenericDatabaseError::failed_precondition(format!(
                "Database {root} edition changed from ENTERPRISE to STANDARD; re-create query \
                 listener"
            ))
        );

        drop(req_tx);
    }
}
