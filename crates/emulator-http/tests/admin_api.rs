#![allow(unused_crate_dependencies)]

use axum::http::StatusCode;
use emulator_database::{FirestoreProject, config::FirestoreConfig};
use emulator_http::RouterBuilder;
use reqwest::Client;
use serde_json::json;

#[tokio::test]
async fn test_database_admin_api() {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
        default_enterprise: false,
        ..Default::default()
    })));
    let router = RouterBuilder::new(project).build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!(
        "http://127.0.0.1:{}/v1/projects/my-project/databases/(default)",
        port
    );

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let client = Client::new();

    // 1. GET - Should default to STANDARD (because default_enterprise is false)
    let res = client.get(&url).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["databaseEdition"], "STANDARD");
    assert_eq!(body["concurrencyMode"], "PESSIMISTIC");

    // 2. PATCH - Change to ENTERPRISE
    let res = client
        .patch(&url)
        .json(&json!({
            "databaseEdition": "ENTERPRISE"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["databaseEdition"], "ENTERPRISE");
    assert_eq!(body["concurrencyMode"], "OPTIMISTIC");

    // 3. GET - Should now be ENTERPRISE and OPTIMISTIC
    let res = client.get(&url).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["databaseEdition"], "ENTERPRISE");
    assert_eq!(body["concurrencyMode"], "OPTIMISTIC");

    // 4. PATCH - Change to PESSIMISTIC
    let res = client
        .patch(&url)
        .json(&json!({
            "concurrencyMode": "PESSIMISTIC"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["databaseEdition"], "ENTERPRISE");
    assert_eq!(body["concurrencyMode"], "PESSIMISTIC");

    // 5. GET - Should now be ENTERPRISE and PESSIMISTIC
    let res = client.get(&url).send().await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["databaseEdition"], "ENTERPRISE");
    assert_eq!(body["concurrencyMode"], "PESSIMISTIC");
}
