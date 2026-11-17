use std::task::{Context, Poll};

use futures::future::Either;
use tower::{Layer, Service, ServiceExt, util::Oneshot};

/// Layer that routes to a different service whenever `when` returns `true`. Otherwise calls the
/// wrapped service without touching the request or response.
#[derive(Clone)]
pub(crate) struct MultiplexLayer<F, O> {
    pub(crate) when:    F,
    pub(crate) use_svc: O,
}

impl<F, S, O> Layer<S> for MultiplexLayer<F, O>
where
    F: Clone,
    O: Clone,
{
    type Service = MultiplexService<F, S, O>;

    fn layer(&self, inner: S) -> Self::Service {
        MultiplexService {
            when:  self.when.clone(),
            base:  inner,
            other: self.use_svc.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct MultiplexService<F, S, O> {
    when:  F,
    base:  S,
    other: O,
}

impl<When, BaseSvc, OtherSvc, Request, Response, Error> Service<Request>
    for MultiplexService<When, BaseSvc, OtherSvc>
where
    When: Fn(&Request) -> bool,
    BaseSvc: Service<Request, Response = Response, Error = Error> + Clone,
    OtherSvc: Service<Request, Response = Response, Error = Error> + Clone,
{
    type Response = Response;
    type Error = Error;
    type Future = Either<Oneshot<OtherSvc, Request>, Oneshot<BaseSvc, Request>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if (self.when)(&req) {
            Either::Left(self.other.clone().oneshot(req))
        } else {
            Either::Right(self.base.clone().oneshot(req))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use tokio::sync::Notify;
    use tower::{Service, ServiceExt};

    use super::MultiplexLayer;

    #[derive(Clone)]
    struct MockState {
        ready:      Arc<AtomicBool>,
        error:      Arc<AtomicBool>,
        notify:     Arc<Notify>,
        poll_count: Arc<AtomicUsize>,
        call_count: Arc<AtomicUsize>,
        label:      &'static str,
    }

    impl MockState {
        fn new(label: &'static str) -> Self {
            Self {
                ready: Arc::new(AtomicBool::new(true)),
                error: Arc::new(AtomicBool::new(false)),
                notify: Arc::new(Notify::new()),
                poll_count: Arc::new(AtomicUsize::new(0)),
                call_count: Arc::new(AtomicUsize::new(0)),
                label,
            }
        }

        fn set_ready(&self, ready: bool) {
            self.ready.store(ready, Ordering::SeqCst);
            if ready {
                self.notify.notify_waiters();
            }
        }
    }

    #[derive(Clone)]
    struct MockSvc(MockState);

    impl Service<bool> for MockSvc {
        type Response = &'static str;
        type Error = &'static str;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.0.poll_count.fetch_add(1, Ordering::SeqCst);
            if self.0.error.load(Ordering::SeqCst) {
                return Poll::Ready(Err("poll_ready failed"));
            }
            if self.0.ready.load(Ordering::SeqCst) {
                return Poll::Ready(Ok(()));
            }
            let notified = self.0.notify.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().poll(cx);
            if self.0.ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn call(&mut self, _req: bool) -> Self::Future {
            self.0.call_count.fetch_add(1, Ordering::SeqCst);
            let label = self.0.label;
            Box::pin(async move { Ok(label) })
        }
    }

    #[tokio::test]
    async fn pending_branch_does_not_panic_or_block_ready_branch() {
        use tower::Layer;

        let base_state = MockState::new("base");
        let other_state = MockState::new("other");
        base_state.set_ready(false);

        let layer = MultiplexLayer {
            when:    |use_other: &bool| *use_other,
            use_svc: MockSvc(other_state.clone()),
        };
        let mut svc = layer.layer(MockSvc(base_state.clone()));

        // Routing to `other` succeeds immediately without polling pending `base`.
        let res = svc.ready().await.unwrap().call(true).await;
        assert_eq!(res, Ok("other"));
        assert_eq!(base_state.poll_count.load(Ordering::SeqCst), 0);
        assert_eq!(other_state.poll_count.load(Ordering::SeqCst), 1);

        // Routing to pending `base` awaits readiness instead of panicking.
        let mut pending_fut = svc.ready().await.unwrap().call(false);
        assert!(futures::poll!(&mut pending_fut).is_pending());
        base_state.set_ready(true);
        assert_eq!(pending_fut.await, Ok("base"));
        assert_eq!(other_state.poll_count.load(Ordering::SeqCst), 1);
        assert_eq!(base_state.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn readiness_error_propagates_without_invoking_call() {
        use tower::Layer;

        let base_state = MockState::new("base");
        let other_state = MockState::new("other");
        other_state.error.store(true, Ordering::SeqCst);

        let layer = MultiplexLayer {
            when:    |use_other: &bool| *use_other,
            use_svc: MockSvc(other_state.clone()),
        };
        let mut svc = layer.layer(MockSvc(base_state.clone()));

        let err = svc.ready().await.unwrap().call(true).await;
        assert_eq!(err, Err("poll_ready failed"));
        assert_eq!(other_state.call_count.load(Ordering::SeqCst), 0);
        assert_eq!(base_state.poll_count.load(Ordering::SeqCst), 0);

        // Unrelated `base` branch remains unaffected.
        let ok = svc.ready().await.unwrap().call(false).await;
        assert_eq!(ok, Ok("base"));
    }
}
