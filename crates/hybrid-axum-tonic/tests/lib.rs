mod common;

use std::{sync::Mutex, time::Duration};

use axum::routing::get;
use common::{
    proto::{test1_client::Test1Client, test1_server::Test1Server, Test1Request},
    server::Test1Service,
};
use hybrid_axum_tonic::GrpcMultiplexLayer;
use tokio::net::TcpListener;
use tonic::transport::{server::TcpIncoming, Channel, Uri};
use tower::ServiceExt;

#[tokio::test]
async fn test_hybrid() {
    let rest = axum::Router::new()
        .route("/", get(|| async move { "FIXED RESPONSE" }))
        .into_service()
        .map_response(|r| r.map(tonic::body::boxed));

    let grpc = tonic::transport::Server::builder()
        .accept_http1(true)
        .tcp_nodelay(true)
        .layer(GrpcMultiplexLayer::new(rest))
        .add_service(Test1Server::new(Test1Service {
            state: Mutex::new(10),
        }));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let uri: Uri = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();

    let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
    tokio::task::spawn(async move { grpc.serve_with_incoming(incoming).await });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let channel = Channel::builder(uri.clone()).connect().await.unwrap();

    let mut client1 = Test1Client::new(channel.clone());
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();

    let resp = reqwest::get(uri.to_string())
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(resp, "FIXED RESPONSE")
}
