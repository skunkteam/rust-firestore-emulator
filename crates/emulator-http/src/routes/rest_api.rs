use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use emulator_database::{reference::Ref, FirestoreProject};
use serde_json::json;

use crate::error::{RestError, Result};

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new().route("/*path", post(handler))
}

async fn handler(
    State(project): State<&'static FirestoreProject>,
    Path(path): Path<String>,
) -> Result<Response> {
    let (path, operation) = path.rsplit_once(':').ok_or_else(RestError::not_found)?;
    if operation != "listCollectionIds" {
        return Err(RestError::not_found());
    }
    let path: Ref = path.parse()?;
    let collections = project
        .database(path.root())
        .await
        .get_collection_ids(&path)
        .await?;
    Ok(Json(json!({ "collectionIds": collections })).into_response())
}

// #[cfg(test)]
// mod tests {
//     use emulator_database::{FirestoreConfig, FirestoreProject};
//     use googletest::prelude::*;
//     use reqwest::Client;
//     use tokio::net::TcpListener;

//     use crate::RouterBuilder;

//     #[gtest]
//     #[tokio::test]
//     async fn test_list_collection_ids() -> Result<()> {
//         let project = FirestoreProject::new(FirestoreConfig::default());
//         let project = Box::leak(Box::new(project));
//         let router = RouterBuilder::new(project).build();

//         let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
//         let host = tcp_listener.local_addr()?;
//         tokio::spawn(async {
//             axum::serve(tcp_listener, router).await.unwrap();
//         });

//         let database = project
//             .database(&"projects/demo-project/databases/(default)".parse()?)
//             .await;

//         let client = Client::new();
//         let root_url = format!("http://{host}/v1/projects/demo-project/databases/(default)/documents:listCollectionIds");

//         Ok(())
//     }
// }
