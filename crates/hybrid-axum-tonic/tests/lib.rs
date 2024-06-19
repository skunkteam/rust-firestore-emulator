mod common;

use std::{net::TcpListener, sync::Mutex, time::Duration};

use axum::{
    middleware::{from_fn, Next},
    response::Response,
    routing::get,
    Router,
};
use common::{
    proto::{
        test1_client::Test1Client, test1_server::Test1Server, test2_client::Test2Client,
        test2_server::Test2Server, Test1Request, Test2Request,
    },
    server::{Test1Service, Test2Service},
};
use hybrid_axum_tonic::{GrpcStatus, NestTonic, RestGrpcService};
use hyper::{Request, Uri};
use tonic::transport::Channel;

async fn do_nothing<B>(req: Request<B>, next: Next<B>) -> Result<Response, GrpcStatus> {
    Ok(next.run(req).await)
}

async fn cancel_request<B>(_req: Request<B>, _next: Next<B>) -> Result<Response, GrpcStatus> {
    Err(tonic::Status::cancelled("Canceled").into())
}

#[tokio::test]
async fn test_hybrid() {
    let grpc_router1 = Router::new()
        .nest_tonic(Test1Server::new(Test1Service {
            state: Mutex::new(10),
        }))
        .layer(from_fn(do_nothing));

    let grpc_router2 = Router::new()
        .nest_tonic(Test2Server::new(Test2Service))
        .layer(from_fn(cancel_request));

    let grpc_router = grpc_router1.merge(grpc_router2);

    let rest_router = Router::new().nest("/", Router::new().route("/123", get(|| async move {})));

    let make_service = RestGrpcService::new(rest_router, grpc_router).into_make_service();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();

    let uri: Uri = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();

    tokio::task::spawn(async move {
        axum::Server::from_tcp(listener)
            .unwrap()
            .serve(make_service)
            .await
        // .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let channel = Channel::builder(uri.clone()).connect().await.unwrap();

    let mut client1 = Test1Client::new(channel.clone());
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();

    let channel = Channel::builder(uri).connect().await.unwrap();

    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();
    client1.test1(Test1Request {}).await.unwrap();

    let mut client2 = Test2Client::new(channel);
    assert_eq!(
        client2.test2(Test2Request {}).await.unwrap_err().code(),
        tonic::Code::Cancelled,
    );
}
