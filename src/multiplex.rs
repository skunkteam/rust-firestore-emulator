use std::task::{Context, Poll};

use futures::future::Either;
use tower::{Layer, Service};

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
    BaseSvc: Service<Request, Response = Response, Error = Error>,
    OtherSvc: Service<Request, Response = Response, Error = Error>,
{
    type Response = Response;
    type Error = Error;
    type Future = Either<OtherSvc::Future, BaseSvc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        assert!(self.base.poll_ready(cx).is_ready());
        assert!(self.other.poll_ready(cx).is_ready());
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if (self.when)(&req) {
            Either::Left(self.other.call(req))
        } else {
            Either::Right(self.base.call(req))
        }
    }
}
