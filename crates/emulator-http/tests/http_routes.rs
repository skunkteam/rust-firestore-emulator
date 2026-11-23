#![allow(unused_crate_dependencies)]

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use emulator_database::{FirestoreProject, config::FirestoreConfig};
use emulator_http::RouterBuilder;
use googletest::prelude::*;
use tower::ServiceExt;

fn build_router() -> Router {
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    RouterBuilder::new(project).build()
}

async fn send(
    router: &Router,
    method: Method,
    uri: &str,
) -> Result<(StatusCode, HeaderMap, Bytes)> {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())?;
    let resp = router.clone().oneshot(req).await?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = to_bytes(resp.into_body(), usize::MAX).await?;
    Ok((status, headers, body))
}

#[gtest]
#[tokio::test]
async fn head_root_returns_ok_with_matching_headers_and_empty_body() -> Result<()> {
    let router = build_router();

    let (get_status, get_headers, get_body) = send(&router, Method::GET, "/").await?;
    let (head_status, head_headers, head_body) = send(&router, Method::HEAD, "/").await?;

    expect_eq!(get_status, StatusCode::OK);
    expect_eq!(head_status, StatusCode::OK);
    expect_that!(head_body.is_empty(), eq(true));
    expect_eq!(
        head_headers
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-cache")
    );
    expect_eq!(
        head_headers.get(header::CONTENT_TYPE),
        get_headers.get(header::CONTENT_TYPE)
    );

    #[cfg(feature = "ui")]
    {
        expect_eq!(
            head_headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
        expect_that!(get_body.is_empty(), eq(false));
    }

    #[cfg(not(feature = "ui"))]
    {
        expect_eq!(
            head_headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/plain; charset=utf-8")
        );
        expect_eq!(get_body.as_ref(), b"OK");
    }

    let (deep_status, deep_headers, deep_body) = send(
        &router,
        Method::HEAD,
        "/projects/demo/databases/(default)/documents/users/alice",
    )
    .await?;
    expect_eq!(deep_status, StatusCode::OK);
    expect_eq!(
        deep_headers.get(header::CONTENT_TYPE),
        head_headers.get(header::CONTENT_TYPE)
    );
    expect_eq!(
        deep_headers
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok()),
        Some("no-cache")
    );
    expect_that!(deep_body.is_empty(), eq(true));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn head_unknown_api_routes_and_non_get_root_return_not_found() -> Result<()> {
    let router = build_router();

    for path in ["/v1/unknown", "/emulator/unknown"] {
        let (status, headers, body) = send(&router, Method::HEAD, path).await?;
        expect_eq!(status, StatusCode::NOT_FOUND);
        expect_eq!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        expect_that!(body.is_empty(), eq(true));
    }

    let (post_status, _, post_body) = send(&router, Method::POST, "/").await?;
    expect_eq!(post_status, StatusCode::NOT_FOUND);
    expect_that!(post_body.is_empty(), eq(false));

    Ok(())
}

#[cfg(feature = "ui")]
#[gtest]
#[tokio::test]
async fn ui_asset_routes_reject_backslash_paths_and_support_head() -> Result<()> {
    let router = build_router();

    // Positive control: forward-slash UI asset resolves cleanly for both GET and HEAD.
    let (get_status, get_headers, get_body) =
        send(&router, Method::GET, "/ui/components/editor.js").await?;
    expect_eq!(get_status, StatusCode::OK);
    expect_eq!(
        get_headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/javascript; charset=utf-8")
    );
    expect_that!(get_body.is_empty(), eq(false));

    let (head_status, head_headers, head_body) =
        send(&router, Method::HEAD, "/ui/components/editor.js").await?;
    expect_eq!(head_status, StatusCode::OK);
    expect_eq!(
        head_headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/javascript; charset=utf-8")
    );
    expect_that!(head_body.is_empty(), eq(true));

    // Backslash paths (percent-encoded in URI, decoded by Axum Path extractor) must return 404.
    for path in [
        "/ui/components%5Ceditor.js",
        "/ui/%5Capp.js",
        "/ui/..%5CCargo.toml",
        "/ui/..%5C..%5CCargo.toml",
    ] {
        let (status, _, _) = send(&router, Method::GET, path).await?;
        expect_eq!(status, StatusCode::NOT_FOUND, "expected 404 for {path}");

        let (head_status, _, head_body) = send(&router, Method::HEAD, path).await?;
        expect_eq!(
            head_status,
            StatusCode::NOT_FOUND,
            "expected 404 for HEAD {path}"
        );
        expect_that!(head_body.is_empty(), eq(true));
    }

    for path in ["/ui", "/ui/"] {
        let (status, _, body) = send(&router, Method::HEAD, path).await?;
        expect_eq!(
            status,
            StatusCode::NOT_FOUND,
            "expected 404 for HEAD {path}"
        );
        expect_that!(body.is_empty(), eq(true));
    }

    Ok(())
}
