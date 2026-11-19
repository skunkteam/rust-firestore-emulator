#![allow(unused_crate_dependencies)]

use std::future::Future;

use emulator_database::FirestoreProject;
use emulator_tracing::Tracing;
use http::header::CONTENT_TYPE;
use tokio::net::TcpListener;
use tonic::transport::{Server, server::TcpIncoming};
use tower::ServiceExt;
use tracing::{Level, enabled, info};

mod multiplex;

pub async fn run(
    project: &'static FirestoreProject,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
    tracing: &'static impl Tracing,
) -> color_eyre::Result<()> {
    let rest_router = emulator_http::RouterBuilder::new(project)
        .add_dynamic_tracing(tracing)
        .build()
        // possible slight performance improvement by doing some prepare work early:
        .with_state(())
        // convert it into a service that is compatible with tonic's GRPC services:
        .into_service()
        .map_response(|r| r.map(tonic::body::Body::new))
        .map_err(Into::into);

    let server = Server::builder()
        .accept_http1(true)
        // simulator is only used on localhost, so TCP_NODELAY is a nobrainer
        .tcp_nodelay(true)
        // route to our Axum service if the request is not a GRPC request
        .layer(multiplex::MultiplexLayer {
            when:    is_not_a_grpc_request,
            use_svc: rest_router,
        })
        .add_service(emulator_grpc::service(project));

    if enabled!(Level::INFO) {
        info!("Firestore emulator listening on {}", listener.local_addr()?);
    } else {
        eprintln!("Firestore emulator listening on {}", listener.local_addr()?);
    }

    server
        .serve_with_incoming_shutdown(
            TcpIncoming::from(listener).with_nodelay(Some(true)),
            shutdown,
        )
        .await?;

    if enabled!(Level::INFO) {
        info!("Firestore emulator stopped");
    } else {
        eprintln!("Firestore emulator stopped");
    }

    Ok(())
}

fn is_not_a_grpc_request<B>(req: &http::Request<B>) -> bool {
    !is_grpc_request(req)
}

fn is_grpc_request<B>(req: &http::Request<B>) -> bool {
    let Some(content_type) = req.headers().get(CONTENT_TYPE) else {
        return false;
    };
    let bytes = content_type.as_bytes();
    let media_type = bytes
        .split(|&b| b == b';')
        .next()
        .unwrap_or(bytes)
        .trim_ascii();
    let prefix = b"application/grpc";
    if media_type.len() < prefix.len() || !media_type[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return false;
    }
    match media_type.get(prefix.len()) {
        None => true,
        Some(b'+') => media_type.len() > prefix.len() + 1,
        Some(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderValue, Request, header::CONTENT_TYPE};

    use super::is_not_a_grpc_request;

    fn req_with_content_type(ct: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(ct) = ct {
            req.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_str(ct).unwrap());
        }
        req
    }

    #[test]
    fn detects_grpc_content_type_case_insensitively_with_delimiters() {
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpc"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "Application/Grpc"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "APPLICATION/GRPC+proto"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            "Application/Grpc; charset=utf-8"
        ))));
        assert!(!is_not_a_grpc_request(&req_with_content_type(Some(
            " application/grpc+json ; charset=utf-8"
        ))));

        assert!(is_not_a_grpc_request(&req_with_content_type(None)));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(""))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpc+"
        ))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpc-web"
        ))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/grpcfoo"
        ))));
        assert!(is_not_a_grpc_request(&req_with_content_type(Some(
            "application/json"
        ))));
    }
}
