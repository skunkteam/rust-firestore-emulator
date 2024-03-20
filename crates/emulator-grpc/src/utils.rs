use futures::{stream::BoxStream, Future, Stream};
use tokio_stream::once;
use tonic::{Response, Result};

macro_rules! unimplemented {
    ($name:expr) => {{
        use tonic::Status;
        let msg = format!("{} is not supported yet ({}:{})", $name, file!(), line!());
        eprintln!("{msg}");
        return Err(Status::unimplemented(msg));
    }};
}

macro_rules! unimplemented_option {
    ($val:expr) => {
        if $val.is_some() {
            unimplemented!(stringify!($val));
        }
    };
}

macro_rules! unimplemented_collection {
    ($val:expr) => {
        if !$val.is_empty() {
            unimplemented!(stringify!($val));
        }
    };
}

macro_rules! unimplemented_bool {
    ($val:expr) => {
        if $val {
            unimplemented!(stringify!($val));
        }
    };
}

macro_rules! mandatory {
    ($val:expr) => {
        $val.ok_or_else(|| Status::invalid_argument(concat!("missing ", stringify!($val))))?
    };
}

/// Wrap the code that produces a stream such that any error while producing a stream, e.g. invalid
/// arguments, are returned as first token in an actual stream, instead of a failing request. This
/// is what cloud Firestore does and has major performance implications in case of the NodeJS SDK.
///
/// In the NodeJS SDK a request that expects to result in a stream is always retried, while an error
/// received inside a stream is not retried, this reduces the time of a single test in our
/// test-suite from 7s to 3ms.
///
/// The returned Result never fails.
pub(crate) async fn error_in_stream<T, F, S>(
    stream: F,
) -> Result<Response<BoxStream<'static, Result<T>>>>
where
    F: Future<Output = Result<S>>,
    S: Stream<Item = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let stream: BoxStream<_> = match stream.await {
        Ok(stream) => Box::pin(stream),
        Err(err) => Box::pin(once(Err(err))),
    };
    Ok(Response::new(stream))
}
