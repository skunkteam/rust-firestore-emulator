use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    routing::post,
};
use emulator_tracing::{DynamicSubscriber, Tracing};

use crate::error::{RestError, Result};

const DEFAULT_MAX_HANDLES: usize = 64;

struct SubscriberHandle {
    subscriber: Mutex<Option<Box<dyn DynamicSubscriber>>>,
}

impl SubscriberHandle {
    fn new(subscriber: Box<dyn DynamicSubscriber>) -> Self {
        Self {
            subscriber: Mutex::new(Some(subscriber)),
        }
    }

    fn drain(&self) -> Option<Bytes> {
        let mut guard = self
            .subscriber
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        guard.as_mut().map(|subscriber| subscriber.drain())
    }

    fn take(&self) -> Option<Box<dyn DynamicSubscriber>> {
        self.subscriber
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

struct SubscriberEntry {
    handle:     Arc<SubscriberHandle>,
    generation: u64,
}

struct SubscriberStore {
    entries: HashMap<String, SubscriberEntry>,
    recency: VecDeque<(String, u64)>,
    next_generation: u64,
    max_handles: usize,
}

impl SubscriberStore {
    fn new(max_handles: usize) -> Self {
        Self {
            entries: HashMap::new(),
            recency: VecDeque::new(),
            next_generation: 0,
            max_handles: max_handles.max(1),
        }
    }

    fn next_gen(&mut self) -> u64 {
        self.next_generation = self.next_generation.wrapping_add(1);
        self.next_generation
    }

    fn insert(
        &mut self,
        name: String,
        handle: Arc<SubscriberHandle>,
    ) -> (Option<Arc<SubscriberHandle>>, Vec<Arc<SubscriberHandle>>) {
        let generation = self.next_gen();
        self.recency.push_back((name.clone(), generation));
        let replaced = self
            .entries
            .insert(name, SubscriberEntry { handle, generation })
            .map(|entry| entry.handle);

        let mut evicted = Vec::new();
        while self.entries.len() > self.max_handles
            && let Some((oldest_name, oldest_gen)) = self.recency.pop_front()
        {
            if self
                .entries
                .get(&oldest_name)
                .is_some_and(|entry| entry.generation == oldest_gen)
                && let Some(entry) = self.entries.remove(&oldest_name)
            {
                evicted.push(entry.handle);
            }
        }
        self.compact_recency();
        (replaced, evicted)
    }

    fn get_and_touch(&mut self, name: &str) -> Option<Arc<SubscriberHandle>> {
        {
            let entry = self.entries.get(name)?;
            if self.recency.back().is_some_and(|(back_name, back_gen)| {
                back_name == name && *back_gen == entry.generation
            }) {
                return Some(Arc::clone(&entry.handle));
            }
        }
        let generation = self.next_gen();
        let entry = self.entries.get_mut(name)?;
        entry.generation = generation;
        let handle = Arc::clone(&entry.handle);
        self.recency.push_back((name.to_owned(), generation));
        self.compact_recency();
        Some(handle)
    }

    fn remove(&mut self, name: &str) -> Option<Arc<SubscriberHandle>> {
        let removed = self.entries.remove(name)?;
        self.compact_recency();
        Some(removed.handle)
    }

    fn compact_recency(&mut self) {
        if self.recency.len() > self.max_handles.saturating_mul(2) {
            self.recency.retain(|(name, generation)| {
                self.entries
                    .get(name)
                    .is_some_and(|entry| entry.generation == *generation)
            });
        }
    }
}

struct TracingState<S: Tracing + 'static> {
    tracing:     &'static S,
    subscribers: Arc<Mutex<SubscriberStore>>,
}

impl<S: Tracing + 'static> Clone for TracingState<S> {
    fn clone(&self) -> Self {
        Self {
            tracing:     self.tracing,
            subscribers: self.subscribers.clone(),
        }
    }
}

impl<S: Tracing + 'static> TracingState<S> {
    fn lock_store(&self) -> MutexGuard<'_, SubscriberStore> {
        self.subscribers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn insert_handle(
        &self,
        name: String,
        handle: Arc<SubscriberHandle>,
    ) -> (Option<Arc<SubscriberHandle>>, Vec<Arc<SubscriberHandle>>) {
        self.lock_store().insert(name, handle)
    }

    fn get_handle(&self, name: &str) -> Option<Arc<SubscriberHandle>> {
        self.lock_store().get_and_touch(name)
    }

    fn remove_handle(&self, name: &str) -> Option<Arc<SubscriberHandle>> {
        self.lock_store().remove(name)
    }
}

pub(crate) fn router(tracing: &'static impl Tracing) -> Router {
    router_with_max_handles(tracing, DEFAULT_MAX_HANDLES)
}

fn router_with_max_handles(tracing: &'static impl Tracing, max_handles: usize) -> Router {
    Router::new()
        .route(
            "/{name}",
            post(start_capture_or_poll_logs).delete(stop_capture),
        )
        .with_state(TracingState {
            tracing,
            subscribers: Arc::new(Mutex::new(SubscriberStore::new(max_handles))),
        })
}

async fn start_capture_or_poll_logs<S: Tracing + 'static>(
    State(tracing): State<TracingState<S>>,
    Path(name): Path<String>,
    body: String,
) -> Result<Bytes> {
    if let Some(name) = name.strip_suffix(":poll") {
        return poll_logs(tracing, name);
    }

    let tracing_backend = tracing.tracing;
    let subscriber = tokio::task::spawn_blocking(move || {
        tracing_backend
            .subscribe(&body)
            .map(|subscriber| Box::new(subscriber) as Box<dyn DynamicSubscriber>)
    })
    .await
    .map_err(|err| RestError::internal(format!("Failed to start log capture: {err}")))??;

    let (replaced, evicted) =
        tracing.insert_handle(name, Arc::new(SubscriberHandle::new(subscriber)));
    drain_and_drop_handles(replaced, evicted).await
}

async fn drain_and_drop_handles(
    replaced: Option<Arc<SubscriberHandle>>,
    evicted: Vec<Arc<SubscriberHandle>>,
) -> Result<Bytes> {
    if replaced.is_none() && evicted.is_empty() {
        return Ok(Bytes::new());
    }
    tokio::task::spawn_blocking(move || {
        for handle in evicted {
            drop(handle.take());
        }
        if let Some(handle) = replaced
            && let Some(mut subscriber) = handle.take()
        {
            let logs = subscriber.drain();
            drop(subscriber);
            return logs;
        }
        Bytes::new()
    })
    .await
    .map_err(|err| RestError::internal(format!("Failed to clean up log subscriber: {err}")))
}

fn poll_logs<S: Tracing + 'static>(tracing: TracingState<S>, name: &str) -> Result<Bytes> {
    let Some(handle) = tracing.get_handle(name) else {
        return Err(RestError::not_found());
    };
    poll_or_retry_replacement(&tracing, name, &handle)
}

fn poll_or_retry_replacement<S: Tracing + 'static>(
    tracing: &TracingState<S>,
    name: &str,
    handle: &Arc<SubscriberHandle>,
) -> Result<Bytes> {
    if let Some(logs) = handle.drain() {
        return Ok(logs);
    }
    if let Some(replacement) = tracing.get_handle(name)
        && !Arc::ptr_eq(handle, &replacement)
        && let Some(logs) = replacement.drain()
    {
        return Ok(logs);
    }
    Err(RestError::not_found())
}

async fn stop_capture<S: Tracing + 'static>(
    State(tracing): State<TracingState<S>>,
    Path(name): Path<String>,
) -> Result<Bytes> {
    let Some(handle) = tracing.remove_handle(&name) else {
        return Err(RestError::not_found());
    };
    tokio::task::spawn_blocking(move || {
        let Some(mut subscriber) = handle.take() else {
            return Err(RestError::not_found());
        };
        let logs = subscriber.drain();
        drop(subscriber);
        Ok(logs)
    })
    .await
    .map_err(|err| RestError::internal(format!("Failed to stop log capture: {err}")))?
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{
            Arc, Condvar, Mutex, PoisonError,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Router,
        body::{Body, Bytes, to_bytes},
        http::{Method, Request, StatusCode},
    };
    use emulator_database::{FirestoreConfig, FirestoreProject};
    use emulator_tracing::{DefaultTracing, DynamicSubscriber, SetLogLevelsError, Tracing};
    use googletest::prelude::*;
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use super::{
        SubscriberHandle, SubscriberStore, TracingState, poll_or_retry_replacement,
        router_with_max_handles,
    };
    use crate::RouterBuilder;

    async fn send_request(
        router: &Router,
        method: Method,
        uri: &str,
        body: &str,
    ) -> Result<(StatusCode, String)> {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::from(body.to_owned()))?,
            )
            .await?;
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await?;
        Ok((status, String::from_utf8(bytes.to_vec())?))
    }

    #[gtest]
    #[tokio::test]
    async fn test_dynamic_tracing() -> Result<()> {
        let project = FirestoreProject::new(FirestoreConfig::default());
        let project = Box::leak(Box::new(project));
        let tracing = DefaultTracing::start();
        let router = RouterBuilder::new(project)
            .add_dynamic_tracing(tracing)
            .build();

        let handle_uri = "/emulator/v1/logging/my-handle";
        let poll_uri = "/emulator/v1/logging/my-handle:poll";

        // Start capturing logging of all tower_http DEBUG messages
        let (status, logs) =
            send_request(&router, Method::POST, handle_uri, "tower_http=debug").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(logs, eq(""), "we start with no logging captured");

        let (status, logs) = send_request(&router, Method::POST, poll_uri, "").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(
            logs,
            all!(
                contains_substring("DEBUG"),
                contains_substring("tower_http::trace")
            ),
            "the first poll shows some DEBUG messages from the tower_http crate"
        );

        // Now we will adjust the logging directives of this capture handle...
        let (status, logs) = send_request(&router, Method::POST, handle_uri, "warn").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(
            logs,
            all!(
                contains_substring("DEBUG"),
                contains_substring("tower_http::trace")
            ),
            "we still receive the logs that were captured up until now"
        );

        let (status, logs) = send_request(&router, Method::POST, poll_uri, "").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(
            logs,
            eq(""),
            "but this time there is no logging, because no WARN-level logs have been captured"
        );

        // Stop the capture
        let (status, _) = send_request(&router, Method::DELETE, handle_uri, "").await?;
        expect_eq!(status, StatusCode::OK);

        let (unknown_poll_status, _) = send_request(&router, Method::POST, poll_uri, "").await?;
        expect_eq!(
            unknown_poll_status,
            StatusCode::NOT_FOUND,
            "polling on an unknown handle gives a 404"
        );
        Ok(())
    }

    #[derive(Default)]
    struct GateState {
        entered:   bool,
        released:  bool,
        timed_out: bool,
    }

    struct BlockingGate {
        state: Mutex<GateState>,
        cv: Condvar,
        entered_notify: Notify,
    }

    struct GateGuard {
        gate: Arc<BlockingGate>,
    }

    impl GateGuard {
        fn release(self) {
            self.gate.release();
        }
    }

    impl Drop for GateGuard {
        fn drop(&mut self) {
            self.gate.release();
        }
    }

    impl BlockingGate {
        fn armed() -> (Arc<Self>, GateGuard) {
            let gate = Arc::new(Self {
                state: Mutex::new(GateState::default()),
                cv: Condvar::new(),
                entered_notify: Notify::new(),
            });
            let guard = GateGuard {
                gate: Arc::clone(&gate),
            };
            (gate, guard)
        }

        fn wait(&self) {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            guard.entered = true;
            self.entered_notify.notify_waiters();
            let (mut guard, result) = self
                .cv
                .wait_timeout_while(guard, Duration::from_secs(5), |s| !s.released)
                .unwrap_or_else(PoisonError::into_inner);
            if result.timed_out() && !guard.released {
                guard.timed_out = true;
            }
        }

        async fn wait_entered(&self) {
            loop {
                let notified = self.entered_notify.notified();
                if self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entered
                {
                    return;
                }
                notified.await;
            }
        }

        fn release(&self) {
            let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            guard.released = true;
            self.cv.notify_all();
        }

        fn timed_out(&self) -> bool {
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .timed_out
        }
    }

    #[derive(Clone, Default)]
    struct MockBehavior {
        logs: VecDeque<Bytes>,
        subscribe_gate: Option<Arc<BlockingGate>>,
        drop_gate: Option<Arc<BlockingGate>>,
        panic_on_subscribe: bool,
        panic_on_drain: Option<Arc<AtomicBool>>,
        panic_on_drop: bool,
        invalid_directive: bool,
    }

    impl MockBehavior {
        fn with_logs(mut self, chunks: &[&str]) -> Self {
            self.logs = chunks
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect();
            self
        }

        fn with_subscribe_gate(mut self, gate: Arc<BlockingGate>) -> Self {
            self.subscribe_gate = Some(gate);
            self
        }

        fn with_drop_gate(mut self, gate: Arc<BlockingGate>) -> Self {
            self.drop_gate = Some(gate);
            self
        }

        fn panic_on_subscribe(mut self) -> Self {
            self.panic_on_subscribe = true;
            self
        }

        fn panic_first_drain(mut self) -> Self {
            self.panic_on_drain = Some(Arc::new(AtomicBool::new(true)));
            self
        }

        fn panic_on_drop(mut self) -> Self {
            self.panic_on_drop = true;
            self
        }

        fn invalid_directive(mut self) -> Self {
            self.invalid_directive = true;
            self
        }
    }

    struct MockTracing {
        live_count:   Arc<AtomicUsize>,
        dropped_ids:  Arc<Mutex<Vec<usize>>>,
        dropped_dirs: Arc<Mutex<Vec<String>>>,
        next_id:      AtomicUsize,
        behaviors:    Mutex<HashMap<String, MockBehavior>>,
    }

    impl MockTracing {
        fn new() -> Self {
            Self {
                live_count:   Arc::new(AtomicUsize::new(0)),
                dropped_ids:  Arc::new(Mutex::new(Vec::new())),
                dropped_dirs: Arc::new(Mutex::new(Vec::new())),
                next_id:      AtomicUsize::new(1),
                behaviors:    Mutex::new(HashMap::new()),
            }
        }

        fn configure(&self, directive: &str, behavior: MockBehavior) {
            self.behaviors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(directive.to_owned(), behavior);
        }

        fn live_count(&self) -> usize {
            self.live_count.load(Ordering::SeqCst)
        }

        fn dropped_dirs(&self) -> Vec<String> {
            self.dropped_dirs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        fn dropped_ids(&self) -> Vec<usize> {
            self.dropped_ids
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    struct MockSubscriber {
        id: usize,
        dir: String,
        logs: VecDeque<Bytes>,
        drop_gate: Option<Arc<BlockingGate>>,
        panic_drain: Option<Arc<AtomicBool>>,
        panic_on_drop: bool,
        live_count: Arc<AtomicUsize>,
        dropped_ids: Arc<Mutex<Vec<usize>>>,
        dropped_dirs: Arc<Mutex<Vec<String>>>,
    }

    impl DynamicSubscriber for MockSubscriber {
        fn drain(&mut self) -> Bytes {
            if let Some(flag) = &self.panic_drain
                && flag.swap(false, Ordering::SeqCst)
            {
                panic!("intentional mock drain panic");
            }
            self.logs.pop_front().unwrap_or_default()
        }
    }

    impl Drop for MockSubscriber {
        fn drop(&mut self) {
            if let Some(gate) = &self.drop_gate {
                gate.wait();
            }
            self.live_count.fetch_sub(1, Ordering::SeqCst);
            self.dropped_ids
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(self.id);
            self.dropped_dirs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(self.dir.clone());
            if self.panic_on_drop {
                panic!("intentional mock drop panic");
            }
        }
    }

    fn invalid_directives_error() -> SetLogLevelsError {
        match DefaultTracing::start().subscribe("=invalid=") {
            Err(err) => err,
            Ok(_) => unreachable!("invalid directive must fail to parse"),
        }
    }

    impl Tracing for MockTracing {
        fn subscribe(
            &self,
            dirs: &str,
        ) -> std::result::Result<impl DynamicSubscriber + '_, SetLogLevelsError> {
            let behavior = self
                .behaviors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(dirs)
                .cloned()
                .unwrap_or_default();

            if let Some(gate) = &behavior.subscribe_gate {
                gate.wait();
            }
            if behavior.panic_on_subscribe {
                panic!("intentional mock subscribe panic");
            }
            if behavior.invalid_directive {
                return Err(invalid_directives_error());
            }

            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            self.live_count.fetch_add(1, Ordering::SeqCst);
            Ok(MockSubscriber {
                id,
                dir: dirs.to_owned(),
                logs: behavior.logs,
                drop_gate: behavior.drop_gate,
                panic_drain: behavior.panic_on_drain,
                panic_on_drop: behavior.panic_on_drop,
                live_count: Arc::clone(&self.live_count),
                dropped_ids: Arc::clone(&self.dropped_ids),
                dropped_dirs: Arc::clone(&self.dropped_dirs),
            })
        }
    }

    #[gtest]
    #[tokio::test]
    async fn subscribe_runs_outside_map_lock_without_blocking_poll_or_delete() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        let (gate, guard) = BlockingGate::armed();
        mock.configure(
            "slow-sub",
            MockBehavior::default().with_subscribe_gate(Arc::clone(&gate)),
        );
        mock.configure("active", MockBehavior::default().with_logs(&["active-log"]));

        let router = router_with_max_handles(mock, 4);

        let (status, _) = send_request(&router, Method::POST, "/active", "active").await?;
        expect_eq!(status, StatusCode::OK);

        let router_clone = router.clone();
        let slow_task = tokio::spawn(async move {
            send_request(&router_clone, Method::POST, "/blocked", "slow-sub").await
        });

        tokio::time::timeout(Duration::from_secs(5), gate.wait_entered())
            .await
            .expect("subscribe entered blocking gate");

        let (poll_status, poll_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/active:poll", ""),
        )
        .await
        .expect("poll did not block behind subscribe")?;
        expect_eq!(poll_status, StatusCode::OK);
        expect_eq!(poll_body, "active-log");

        let (del_status, _) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::DELETE, "/active", ""),
        )
        .await
        .expect("delete did not block behind subscribe")?;
        expect_eq!(del_status, StatusCode::OK);

        let (other_status, _) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/other", "fast"),
        )
        .await
        .expect("concurrent subscribe did not block behind map lock")?;
        expect_eq!(other_status, StatusCode::OK);

        guard.release();
        let (slow_status, slow_body) = slow_task.await??;
        expect_eq!(slow_status, StatusCode::OK);
        expect_eq!(slow_body, "");
        expect_false!(gate.timed_out());

        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn replacement_drains_old_logs_and_drops_outside_locks() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        let (gate, guard) = BlockingGate::armed();
        mock.configure(
            "old",
            MockBehavior::default()
                .with_logs(&["old-logs"])
                .with_drop_gate(Arc::clone(&gate)),
        );
        mock.configure("new", MockBehavior::default().with_logs(&["new-logs"]));
        mock.configure("sibling", MockBehavior::default().with_logs(&["sib-logs"]));

        let router = router_with_max_handles(mock, 4);

        let (status, _) = send_request(&router, Method::POST, "/h1", "old").await?;
        expect_eq!(status, StatusCode::OK);
        let (status, _) = send_request(&router, Method::POST, "/h2", "sibling").await?;
        expect_eq!(status, StatusCode::OK);

        let router_clone = router.clone();
        let replace_task =
            tokio::spawn(
                async move { send_request(&router_clone, Method::POST, "/h1", "new").await },
            );

        tokio::time::timeout(Duration::from_secs(5), gate.wait_entered())
            .await
            .expect("replacement drop entered blocking gate");

        let (poll_new_status, poll_new_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/h1:poll", ""),
        )
        .await
        .expect("polling replacement handle did not block on old drop")?;
        expect_eq!(poll_new_status, StatusCode::OK);
        expect_eq!(poll_new_body, "new-logs");

        let (poll_sib_status, poll_sib_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/h2:poll", ""),
        )
        .await
        .expect("polling sibling handle did not block on old drop")?;
        expect_eq!(poll_sib_status, StatusCode::OK);
        expect_eq!(poll_sib_body, "sib-logs");

        guard.release();
        let (rep_status, rep_body) = replace_task.await??;
        expect_eq!(rep_status, StatusCode::OK);
        expect_eq!(rep_body, "old-logs");
        expect_false!(gate.timed_out());
        expect_eq!(mock.dropped_dirs(), vec!["old"]);

        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn stop_capture_drops_outside_lock_returns_logs_and_404_after() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        let (gate, guard) = BlockingGate::armed();
        mock.configure(
            "closing",
            MockBehavior::default()
                .with_logs(&["final-logs"])
                .with_drop_gate(Arc::clone(&gate)),
        );
        mock.configure("sibling", MockBehavior::default().with_logs(&["sib-logs"]));

        let router = router_with_max_handles(mock, 4);

        expect_eq!(
            send_request(&router, Method::POST, "/h1", "closing")
                .await?
                .0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/h2", "sibling")
                .await?
                .0,
            StatusCode::OK
        );

        let router_clone = router.clone();
        let stop_task =
            tokio::spawn(
                async move { send_request(&router_clone, Method::DELETE, "/h1", "").await },
            );

        tokio::time::timeout(Duration::from_secs(5), gate.wait_entered())
            .await
            .expect("stop_capture drop entered blocking gate");

        let (poll_removed_status, _) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/h1:poll", ""),
        )
        .await
        .expect("poll on removed handle did not block")?;
        expect_eq!(poll_removed_status, StatusCode::NOT_FOUND);

        let (del_again_status, _) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::DELETE, "/h1", ""),
        )
        .await
        .expect("duplicate delete did not block")?;
        expect_eq!(del_again_status, StatusCode::NOT_FOUND);

        let (sib_status, sib_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/h2:poll", ""),
        )
        .await
        .expect("sibling poll did not block during drop")?;
        expect_eq!(sib_status, StatusCode::OK);
        expect_eq!(sib_body, "sib-logs");

        guard.release();
        let (stop_status, stop_body) = stop_task.await??;
        expect_eq!(stop_status, StatusCode::OK);
        expect_eq!(stop_body, "final-logs");
        expect_false!(gate.timed_out());
        expect_eq!(mock.dropped_dirs(), vec!["closing"]);

        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn lru_eviction_drops_oldest_outside_lock_while_active_handles_work() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        let (gate, guard) = BlockingGate::armed();
        mock.configure(
            "oldest",
            MockBehavior::default()
                .with_logs(&["oldest-logs"])
                .with_drop_gate(Arc::clone(&gate)),
        );
        mock.configure("keep-b", MockBehavior::default().with_logs(&["b-logs"]));
        mock.configure("keep-c", MockBehavior::default().with_logs(&["c-logs"]));

        let router = router_with_max_handles(mock, 2);

        expect_eq!(
            send_request(&router, Method::POST, "/a", "oldest").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/b", "keep-b").await?.0,
            StatusCode::OK
        );

        let router_clone = router.clone();
        let evict_task =
            tokio::spawn(
                async move { send_request(&router_clone, Method::POST, "/c", "keep-c").await },
            );

        tokio::time::timeout(Duration::from_secs(5), gate.wait_entered())
            .await
            .expect("eviction drop entered blocking gate");

        let (evicted_poll, _) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/a:poll", ""),
        )
        .await
        .expect("evicted poll did not block")?;
        expect_eq!(evicted_poll, StatusCode::NOT_FOUND);

        let (b_poll, b_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/b:poll", ""),
        )
        .await
        .expect("b poll did not block")?;
        expect_eq!(b_poll, StatusCode::OK);
        expect_eq!(b_body, "b-logs");

        let (c_poll, c_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_request(&router, Method::POST, "/c:poll", ""),
        )
        .await
        .expect("c poll did not block")?;
        expect_eq!(c_poll, StatusCode::OK);
        expect_eq!(c_body, "c-logs");

        guard.release();
        let (c_status, c_post_body) = evict_task.await??;
        expect_eq!(c_status, StatusCode::OK);
        expect_eq!(c_post_body, "");
        expect_false!(gate.timed_out());
        expect_eq!(mock.dropped_dirs(), vec!["oldest"]);
        expect_eq!(mock.dropped_ids(), vec![1]);

        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn lru_recency_poll_replace_invalid_directive_and_delete() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        mock.configure(
            "d-a",
            MockBehavior::default().with_logs(&["a1", "a2", "a3"]),
        );
        mock.configure("d-b", MockBehavior::default().with_logs(&["b1"]));
        mock.configure("d-c", MockBehavior::default().with_logs(&["c1"]));
        mock.configure("d-a2", MockBehavior::default().with_logs(&["a-new"]));
        mock.configure("d-d", MockBehavior::default().with_logs(&["d1"]));
        mock.configure("d-e", MockBehavior::default().with_logs(&["e1"]));
        mock.configure("invalid", MockBehavior::default().invalid_directive());

        let router = router_with_max_handles(mock, 2);

        // Open `a`, `b`; poll `a` so `a` becomes MRU and `b` becomes LRU.
        expect_eq!(
            send_request(&router, Method::POST, "/a", "d-a").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/b", "d-b").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/a:poll", "").await?,
            (StatusCode::OK, "a1".to_owned())
        );

        // Opening `c` evicts `b` (LRU), keeping `a` and `c`.
        expect_eq!(
            send_request(&router, Method::POST, "/c", "d-c").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/b:poll", "").await?.0,
            StatusCode::NOT_FOUND
        );
        expect_eq!(
            send_request(&router, Method::POST, "/a:poll", "").await?,
            (StatusCode::OK, "a2".to_owned())
        );

        // At capacity (`[c, a]`, with `a` MRU, wait: polling `a` made `a` MRU and `c` LRU;
        // let's poll `c` first so `a` is LRU and `c` is MRU, then replace `a`!).
        expect_eq!(
            send_request(&router, Method::POST, "/c:poll", "").await?,
            (StatusCode::OK, "c1".to_owned())
        );
        // Replacing `a` at capacity does NOT evict `c`, returns `a`'s remaining logs, and moves
        // `a` to MRU (making `c` LRU).
        expect_eq!(
            send_request(&router, Method::POST, "/a", "d-a2").await?,
            (StatusCode::OK, "a3".to_owned())
        );
        expect_eq!(mock.live_count(), 2);

        // Invalid directive (`400`) at capacity neither evicts nor alters existing handles.
        expect_eq!(
            send_request(&router, Method::POST, "/bad", "invalid")
                .await?
                .0,
            StatusCode::BAD_REQUEST
        );
        expect_eq!(
            send_request(&router, Method::POST, "/a", "invalid")
                .await?
                .0,
            StatusCode::BAD_REQUEST
        );
        expect_eq!(mock.live_count(), 2);

        // Opening `d` now evicts `c` (since replacing `a` moved `a` to MRU), keeping `a` and `d`.
        expect_eq!(
            send_request(&router, Method::POST, "/d", "d-d").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/c:poll", "").await?.0,
            StatusCode::NOT_FOUND
        );
        expect_eq!(
            send_request(&router, Method::POST, "/a:poll", "").await?,
            (StatusCode::OK, "a-new".to_owned())
        );
        expect_eq!(
            send_request(&router, Method::POST, "/d:poll", "").await?,
            (StatusCode::OK, "d1".to_owned())
        );

        // `DELETE` frees capacity so opening `e` does not evict `d`.
        expect_eq!(
            send_request(&router, Method::DELETE, "/a", "").await?.0,
            StatusCode::OK
        );
        expect_eq!(mock.live_count(), 1);
        expect_eq!(
            send_request(&router, Method::POST, "/e", "d-e").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/d:poll", "").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/e:poll", "").await?.0,
            StatusCode::OK
        );

        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    fn subscriber_store_compacts_recency_clamps_zero_and_wraps_generation() {
        let mock = Box::leak(Box::new(MockTracing::new()));
        let make_handle = |dir: &str| {
            let sub = mock.subscribe(dir).expect("mock subscribe succeeds");
            Arc::new(SubscriberHandle::new(Box::new(sub)))
        };

        let mut store = SubscriberStore::new(0);
        assert_eq!(store.max_handles, 1);

        let (rep, ev) = store.insert("only".to_owned(), make_handle("d1"));
        assert!(rep.is_none());
        assert!(ev.is_empty());
        let (rep, ev) = store.insert("second".to_owned(), make_handle("d2"));
        assert!(rep.is_none());
        assert_eq!(ev.len(), 1);
        assert_eq!(store.entries.len(), 1);
        drop(ev);

        // Repeated alternating touches compact `recency` to `<= max_handles * 2`.
        let mut two_store = SubscriberStore::new(2);
        drop(two_store.insert("a".to_owned(), make_handle("a")));
        drop(two_store.insert("b".to_owned(), make_handle("b")));
        for _ in 0..20 {
            assert!(two_store.get_and_touch("a").is_some());
            assert!(two_store.get_and_touch("b").is_some());
            assert!(
                two_store.recency.len() <= two_store.max_handles * 2,
                "recency len {} exceeded bound {}",
                two_store.recency.len(),
                two_store.max_handles * 2
            );
        }

        // Repeatedly polling the current MRU handle avoids pushing duplicate recency entries.
        let before_len = two_store.recency.len();
        for _ in 0..10 {
            assert!(two_store.get_and_touch("b").is_some());
            assert_eq!(two_store.recency.len(), before_len);
        }

        // Wrapping generation on `u64::MAX`.
        two_store.next_generation = u64::MAX;
        assert_eq!(two_store.next_gen(), 0);
        assert_eq!(two_store.next_gen(), 1);

        drop(two_store);
        drop(store);
        expect_eq!(mock.live_count(), 0);
    }

    #[gtest]
    #[tokio::test]
    async fn poison_free_recovery_on_subscribe_drain_and_drop_panics() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        mock.configure("ok", MockBehavior::default().with_logs(&["ok-logs"]));
        mock.configure("panic-sub", MockBehavior::default().panic_on_subscribe());
        mock.configure(
            "panic-drain",
            MockBehavior::default()
                .panic_first_drain()
                .with_logs(&["recovered-logs"]),
        );
        mock.configure("panic-drop", MockBehavior::default().panic_on_drop());

        let router = router_with_max_handles(mock, 4);

        // 1. `subscribe` panic returns 500 and preserves existing handles + map health.
        expect_eq!(
            send_request(&router, Method::POST, "/h1", "ok").await?.0,
            StatusCode::OK
        );
        let (sub_panic_status, sub_panic_body) =
            send_request(&router, Method::POST, "/h1", "panic-sub").await?;
        expect_eq!(sub_panic_status, StatusCode::INTERNAL_SERVER_ERROR);
        expect_that!(sub_panic_body, contains_substring("INTERNAL"));
        expect_eq!(
            send_request(&router, Method::POST, "/h1:poll", "").await?,
            (StatusCode::OK, "ok-logs".to_owned())
        );
        expect_eq!(
            send_request(&router, Method::DELETE, "/h1", "").await?.0,
            StatusCode::OK
        );

        // 2. `drain` panic genuinely poisons `SubscriberHandle.subscriber`, yet subsequent `:poll`
        //    and `DELETE` recover via `PoisonError::into_inner`.
        expect_eq!(
            send_request(&router, Method::POST, "/poison-drain", "panic-drain")
                .await?
                .0,
            StatusCode::OK
        );
        let router_clone = router.clone();
        let panic_poll_join = tokio::spawn(async move {
            send_request(&router_clone, Method::POST, "/poison-drain:poll", "").await
        })
        .await;
        let join_err = panic_poll_join.expect_err("first drain must panic");
        assert!(join_err.is_panic());

        expect_eq!(
            send_request(&router, Method::POST, "/poison-drain:poll", "").await?,
            (StatusCode::OK, "recovered-logs".to_owned())
        );
        expect_eq!(
            send_request(&router, Method::DELETE, "/poison-drain", "")
                .await?
                .0,
            StatusCode::OK
        );

        // 3. `drop` panic during `stop_capture` returns 500, cleans up handle, and map works.
        expect_eq!(
            send_request(&router, Method::POST, "/panic-stop", "panic-drop")
                .await?
                .0,
            StatusCode::OK
        );
        let (stop_panic_status, _) =
            send_request(&router, Method::DELETE, "/panic-stop", "").await?;
        expect_eq!(stop_panic_status, StatusCode::INTERNAL_SERVER_ERROR);
        expect_eq!(
            send_request(&router, Method::POST, "/panic-stop:poll", "")
                .await?
                .0,
            StatusCode::NOT_FOUND
        );
        expect_eq!(
            send_request(&router, Method::POST, "/panic-stop", "ok")
                .await?
                .0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::DELETE, "/panic-stop", "")
                .await?
                .0,
            StatusCode::OK
        );

        // 4. `drop` panic during replacement returns 500, keeps replacement handle, and works.
        expect_eq!(
            send_request(&router, Method::POST, "/panic-rep", "panic-drop")
                .await?
                .0,
            StatusCode::OK
        );
        let (rep_panic_status, _) = send_request(&router, Method::POST, "/panic-rep", "ok").await?;
        expect_eq!(rep_panic_status, StatusCode::INTERNAL_SERVER_ERROR);
        expect_eq!(
            send_request(&router, Method::POST, "/panic-rep:poll", "").await?,
            (StatusCode::OK, "ok-logs".to_owned())
        );
        expect_eq!(
            send_request(&router, Method::DELETE, "/panic-rep", "")
                .await?
                .0,
            StatusCode::OK
        );

        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn poll_retries_once_on_concurrent_replacement() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        mock.configure("d1", MockBehavior::default());
        mock.configure(
            "d2",
            MockBehavior::default().with_logs(&["replacement-logs"]),
        );

        let state = TracingState {
            tracing:     mock,
            subscribers: Arc::new(Mutex::new(SubscriberStore::new(4))),
        };

        let old_sub = Box::new(mock.subscribe("d1")?);
        let old_handle = Arc::new(SubscriberHandle::new(old_sub));
        let (replaced, evicted) = state.insert_handle("h".to_owned(), Arc::clone(&old_handle));
        assert!(replaced.is_none());
        assert!(evicted.is_empty());

        // Simulate concurrent replacement where old handle's box was already taken before `poll`
        // called `drain()`, falling back to `state.get_handle("h")` for the replacement.
        let new_sub = Box::new(mock.subscribe("d2")?);
        let new_handle = Arc::new(SubscriberHandle::new(new_sub));
        let (replaced, _) = state.insert_handle("h".to_owned(), Arc::clone(&new_handle));
        drop(replaced.expect("old handle returned").take());

        let logs =
            poll_or_retry_replacement(&state, "h", &old_handle).expect("replacement poll succeeds");
        expect_eq!(logs, Bytes::from_static(b"replacement-logs"));

        // If `new_handle` itself is already taken while still in the map (same `Arc`),
        // `poll_or_retry_replacement` does not loop on itself and returns 404.
        drop(new_handle.take());
        assert!(poll_or_retry_replacement(&state, "h", &new_handle).is_err());

        // If the handle was also removed/evicted, `poll_or_retry_replacement` returns 404.
        assert!(state.remove_handle("h").is_some());
        assert!(poll_or_retry_replacement(&state, "h", &old_handle).is_err());

        drop(state);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn concurrent_stress_bounds_live_handles_and_drops_cleanly() -> Result<()> {
        let mock = Box::leak(Box::new(MockTracing::new()));
        mock.configure("ok", MockBehavior::default().with_logs(&["log-line"]));
        mock.configure("invalid", MockBehavior::default().invalid_directive());

        let max_handles = 4;
        let router = router_with_max_handles(mock, max_handles);

        let mut tasks = Vec::new();
        for worker in 0..12usize {
            let router = router.clone();
            tasks.push(tokio::spawn(async move {
                for step in 0..25usize {
                    let name = format!("stress-{}", (worker + step) % 10);
                    let action = (worker * 7 + step) % 4;
                    let (status, _) = match action {
                        0 => send_request(&router, Method::POST, &format!("/{name}"), "ok").await?,
                        1 => {
                            send_request(&router, Method::POST, &format!("/{name}:poll"), "")
                                .await?
                        }
                        2 => send_request(&router, Method::DELETE, &format!("/{name}"), "").await?,
                        _ => {
                            send_request(&router, Method::POST, &format!("/{name}"), "invalid")
                                .await?
                        }
                    };
                    assert!(
                        matches!(
                            status,
                            StatusCode::OK | StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND
                        ),
                        "unexpected status {status} in concurrent stress test"
                    );
                }
                Result::<()>::Ok(())
            }));
        }

        for task in tasks {
            task.await??;
        }

        assert!(
            mock.live_count() <= max_handles,
            "live_count {} exceeded max_handles {max_handles}",
            mock.live_count()
        );
        drop(router);
        expect_eq!(mock.live_count(), 0);
        Ok(())
    }

    #[gtest]
    #[tokio::test]
    async fn real_default_tracing_respects_max_handles_replacement_and_cleanup() -> Result<()> {
        let tracing = DefaultTracing::start();
        let router = router_with_max_handles(tracing, 2);

        let target = "logging_real_e2e_target";
        let directive = format!("{target}=debug");

        // Open `a` and `b`.
        expect_eq!(
            send_request(&router, Method::POST, "/a", &directive)
                .await?
                .0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/b", "off").await?.0,
            StatusCode::OK
        );

        tracing::debug!(target: "logging_real_e2e_target", "before-replace");

        // Poll `b` so `b` becomes MRU and `a` is LRU, then replace `a` (drains `"before-replace"`
        // and refreshes `a` to MRU).
        expect_eq!(
            send_request(&router, Method::POST, "/b:poll", "").await?,
            (StatusCode::OK, String::new())
        );
        let (rep_status, rep_logs) = send_request(&router, Method::POST, "/a", "off").await?;
        expect_eq!(rep_status, StatusCode::OK);
        expect_that!(rep_logs, contains_substring("before-replace"));

        // Open `c` -> evicts `b` (LRU), keeping `a` and `c`.
        expect_eq!(
            send_request(&router, Method::POST, "/c", &directive)
                .await?
                .0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/b:poll", "").await?.0,
            StatusCode::NOT_FOUND
        );

        tracing::debug!(target: "logging_real_e2e_target", "only-c-captures");
        expect_eq!(
            send_request(&router, Method::POST, "/a:poll", "").await?,
            (StatusCode::OK, String::new())
        );
        let (c_poll_status, c_logs) = send_request(&router, Method::POST, "/c:poll", "").await?;
        expect_eq!(c_poll_status, StatusCode::OK);
        expect_that!(c_logs, contains_substring("only-c-captures"));

        // Clean delete of active handles.
        expect_eq!(
            send_request(&router, Method::DELETE, "/a", "").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::DELETE, "/c", "").await?.0,
            StatusCode::OK
        );
        expect_eq!(
            send_request(&router, Method::POST, "/a:poll", "").await?.0,
            StatusCode::NOT_FOUND
        );
        expect_eq!(
            send_request(&router, Method::POST, "/c:poll", "").await?.0,
            StatusCode::NOT_FOUND
        );

        drop(router);
        Ok(())
    }
}
