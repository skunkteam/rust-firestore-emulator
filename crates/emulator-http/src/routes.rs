#[cfg(all(feature = "ui", debug_assertions))]
use std::path::{Path, PathBuf};

#[cfg(feature = "ui")]
use axum::extract::Path as AxumPath;
#[cfg(feature = "ui")]
use axum::routing::get;
use axum::{
    Router,
    http::{HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use emulator_database::FirestoreProject;
use emulator_tracing::Tracing;
use tower_http::{set_header::SetResponseHeaderLayer, trace::TraceLayer};

use crate::error::RestError;

mod emulator;
mod logging;
mod projects;

#[cfg(feature = "ui")]
mod generated_ui {
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));
}

#[allow(clippy::declare_interior_mutable_const)]
const NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");

#[derive(Debug)]
pub struct RouterBuilder(Router);

impl RouterBuilder {
    pub fn new(project: &'static FirestoreProject) -> Self {
        Self(
            Router::new()
                .nest("/emulator/v1", emulator::router())
                .nest("/v1/projects", projects::router())
                .with_state(project),
        )
    }

    pub fn add_dynamic_tracing(self, tracing: &'static impl Tracing) -> Self {
        let Self(router) = self;
        let router = router.nest("/emulator/v1/logging", logging::router(tracing));
        Self(router)
    }

    pub fn build(self) -> Router {
        let Self(router) = self;

        #[cfg(feature = "ui")]
        let router = router
            .route(
                "/ui/{*path}",
                get(|AxumPath(path): AxumPath<String>| async move { serve_ui_asset(&path) }),
            )
            .route(
                "/lit-html.js",
                get(|| async { serve_ui_asset("lit-html.js") }),
            );

        router
            .fallback(root_fallback)
            .layer(SetResponseHeaderLayer::overriding(
                header::CACHE_CONTROL,
                NO_CACHE,
            ))
            .layer(TraceLayer::new_for_http())
    }
}

async fn root_fallback(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if path == "/v1"
        || path.starts_with("/v1/")
        || path == "/emulator"
        || path.starts_with("/emulator/")
    {
        return RestError::not_found_msg(format!("Unknown API endpoint: {method} {path}"))
            .into_response();
    }

    #[cfg(feature = "ui")]
    {
        if path == "/ui" || path.starts_with("/ui/") {
            return (StatusCode::NOT_FOUND, "UI asset not found").into_response();
        }
        if method == Method::GET {
            return serve_ui_asset("index.html");
        }
        RestError::not_found_msg(format!("Not found: {method} {path}")).into_response()
    }

    #[cfg(not(feature = "ui"))]
    {
        if method == Method::GET {
            return (StatusCode::OK, "OK").into_response();
        }
        RestError::not_found().into_response()
    }
}

#[cfg(feature = "ui")]
fn serve_ui_asset(rel_path: &str) -> Response {
    let trimmed = rel_path.trim_start_matches('/');
    if trimmed.is_empty()
        || trimmed
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
        || trimmed.ends_with(".d.ts")
        || trimmed.ends_with(".md")
    {
        return (StatusCode::NOT_FOUND, "UI asset not found").into_response();
    }

    #[cfg(debug_assertions)]
    {
        let ui_dir = std::env::var_os("FIRESTORE_EMULATOR_UI_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ui"));
        let disk_path = ui_dir.join(trimmed);
        if let Some(content_type) = mime_for_rel_path(trimmed)
            && let Ok(bytes) = std::fs::read(&disk_path)
        {
            return build_asset_response(trimmed, content_type, bytes);
        }
    }

    if let Some((_, content_type, bytes)) = generated_ui::UI_ASSETS
        .iter()
        .find(|(name, _, _)| *name == trimmed)
    {
        return build_asset_response(trimmed, content_type, bytes.to_vec());
    }

    (StatusCode::NOT_FOUND, "UI asset not found").into_response()
}

#[cfg(feature = "ui")]
fn build_asset_response(rel_path: &str, content_type: &'static str, bytes: Vec<u8>) -> Response {
    if rel_path == "index.html"
        && let Ok(html) = String::from_utf8(bytes.clone())
    {
        let version_script = format!(
            "<script>window.EMULATOR_VERSION={:?};</script>",
            env!("CARGO_PKG_VERSION")
        );
        let injected = if let Some((before, after)) = html.split_once("</head>") {
            format!("{before}{version_script}</head>{after}")
        } else {
            format!("{html}{version_script}")
        };
        return ([(header::CONTENT_TYPE, content_type)], injected).into_response();
    }
    ([(header::CONTENT_TYPE, content_type)], bytes).into_response()
}

#[cfg(all(feature = "ui", debug_assertions))]
fn mime_for_rel_path(rel_path: &str) -> Option<&'static str> {
    match Path::new(rel_path).extension()?.to_str()? {
        "html" => Some("text/html; charset=utf-8"),
        "js" => Some("text/javascript; charset=utf-8"),
        "css" => Some("text/css; charset=utf-8"),
        "svg" => Some("image/svg+xml"),
        _ => None,
    }
}
