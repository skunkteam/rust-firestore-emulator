//! Implementation of a layer that routes non-grpc request to the same port to
//! an Axum Router, inspired by:
//! https://github.com/tokio-rs/axum/issues/2736#issuecomment-2256154646
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() {
//!     // This is the normal rest-router, to which all normal requests are routed
//!     let rest = axum::Router::new()
//!         .route("/", get(|| async move { "FIXED RESPONSE" }))
//!         .into_service()
//!         .map_response(|r| r.map(tonic::body::boxed));
//!
//!     // This is the GRPC router with the multiplex layer installed
//!     let grpc = tonic::transport::Server::builder()
//!         .accept_http1(true)
//!         .tcp_nodelay(true)
//!         .layer(GrpcMultiplexLayer::new(rest))
//!         .add_service(Test1Server::new(Test1Service));
//!
//!     // Serve at 127.0.0.1:8080
//!     grpc.serve(&"127.0.0.1:8080".parse().unwrap())
//!         .await
//!         .unwrap();
//! }
//! ```

mod multiplex;
pub use multiplex::GrpcMultiplexLayer;
