use std::{
    collections::HashMap,
    future::Future,
    marker::Unpin,
    pin::Pin,
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
                            if let Err(err) = self.reacquire_and_reset(&db_name).await {
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
                            if let Err(err) = self.reacquire_and_reset(&db_name).await {
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

    async fn reacquire_and_reset(&mut self, db_name: &RootRef) -> Result<()> {
        let required_edition = self
            .target
            .as_ref()
            .and_then(ListenerTarget::required_edition);
        let database = reacquire_database(
            self.project,
            &mut self.database_events,
            db_name,
            required_edition,
        )
        .await?;
        self.reset(database).await
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
        database: Arc<FirestoreDatabase>,
        mut name: DocumentRef,
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

        let (read_time, doc) = retry_on_cleared(
            self.project,
            &mut self.database_events,
            database,
            None,
            &mut name,
            |doc_name, db| {
                Box::pin(async move {
                    let read_time = Timestamp::now();
                    debug!(%doc_name);

                    // Now determine the latest version at the pinned snapshot timestamp so
                    // concurrent writes after `read_time` are delivered via the already-subscribed
                    // broadcast channel instead of duplicating the initial snapshot.
                    let doc = db
                        .get_doc(doc_name, ReadConsistency::ReadTime(read_time))
                        .await?;
                    Ok((read_time, doc))
                })
            },
        )
        .await?;

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
    database.is_cleared() && matches!(err, GenericDatabaseError::Aborted(_))
}

fn edition_mismatch_error(
    db_name: &RootRef,
    query_edition: DatabaseEdition,
    db_edition: DatabaseEdition,
) -> GenericDatabaseError {
    error!(
        database = %db_name,
        %query_edition,
        %db_edition,
        "Database {} edition changed from {} to {}; terminating query listener",
        db_name,
        query_edition,
        db_edition
    );
    GenericDatabaseError::failed_precondition(format!(
        "Database {} edition changed from {} to {}; re-create query listener",
        db_name, query_edition, db_edition
    ))
}

async fn reacquire_database(
    project: &'static FirestoreProject,
    database_events: &mut StreamMap<
        RootRef,
        StreamNotifyClose<BroadcastStream<Arc<DatabaseEvent>>>,
    >,
    db_name: &RootRef,
    required_edition: Option<DatabaseEdition>,
) -> Result<Arc<FirestoreDatabase>> {
    let database = match project
        .database_for_listener(db_name, required_edition)
        .await
    {
        Ok(database) => database,
        Err(default_edition) => {
            let query_edition = required_edition
                .expect("database_for_listener only returns Err when required_edition is Some");
            return Err(edition_mismatch_error(
                db_name,
                query_edition,
                default_edition,
            ));
        }
    };
    database_events.insert(
        database.name.clone(),
        StreamNotifyClose::new(BroadcastStream::new(database.subscribe())),
    );
    Ok(database)
}

async fn retry_on_cleared<State: Send, T>(
    project: &'static FirestoreProject,
    database_events: &mut StreamMap<
        RootRef,
        StreamNotifyClose<BroadcastStream<Arc<DatabaseEvent>>>,
    >,
    mut database: Arc<FirestoreDatabase>,
    required_edition: Option<DatabaseEdition>,
    state: &mut State,
    mut op: impl for<'a> FnMut(
        &'a mut State,
        &'a FirestoreDatabase,
    ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
) -> Result<T> {
    let mut retries = 0;
    loop {
        match op(state, &database).await {
            Ok(res) => return Ok(res),
            Err(err) if is_cleared_retryable(&database, &err) && retries < MAX_RESET_RETRIES => {
                retries += 1;
                database =
                    reacquire_database(project, database_events, &database.name, required_edition)
                        .await?;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn reset_target(
    project: &'static FirestoreProject,
    database_events: &mut StreamMap<
        RootRef,
        StreamNotifyClose<BroadcastStream<Arc<DatabaseEvent>>>,
    >,
    target: &mut ListenerTarget,
    database: Arc<FirestoreDatabase>,
) -> Result<(Timestamp, Vec<ResponseType>)> {
    let required_edition = target.required_edition();
    retry_on_cleared(
        project,
        database_events,
        database,
        required_edition,
        target,
        |target, db| Box::pin(target.reset(db)),
    )
    .await
}

enum ListenerTarget {
    DocumentTarget(DocumentTarget),
    QueryTarget(QueryTarget),
}

impl ListenerTarget {
    fn required_edition(&self) -> Option<DatabaseEdition> {
        match self {
            ListenerTarget::DocumentTarget(_) => None,
            ListenerTarget::QueryTarget(target) => Some(DatabaseEdition::from_enterprise(
                target.query.enterprise_edition(),
            )),
        }
    }
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
        let query_edition = DatabaseEdition::from_enterprise(self.query.enterprise_edition());
        let db_edition = database.settings().edition;
        if query_edition != db_edition {
            return Err(edition_mismatch_error(
                &database.name,
                query_edition,
                db_edition,
            ));
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
mod tests;
