use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::Result,
    routing::{get, put},
    Router,
};
#[cfg(feature = "ui")]
use axum::{http::header, response::Html};
use emulator_tracing::{SetLogLevelsError, Tracing};
use firestore_database::FirestoreProject;

mod emulator;

#[cfg(feature = "ui")]
const HTML: &str = concat!(
    include_str!("../../../ui/index.html"),
    "<script>version.innerHTML=\"",
    env!("CARGO_PKG_VERSION"),
    "\"</script>",
);

#[derive(Debug)]
pub struct RouterBuilder(Router);

impl RouterBuilder {
    pub fn new(project: &'static FirestoreProject) -> Self {
        Self(
            Router::new()
                .nest("/emulator/v1", emulator::router())
                .with_state(project),
        )
    }

    pub fn add_dynamic_tracing(self, tracing: impl Tracing + Send + Sync + 'static) -> Self {
        let Self(router) = self;
        let router = router.route(
            "/emulator/v1/loglevels",
            put(set_log_levels).with_state(Arc::new(tracing)),
        );
        Self(router)
    }

    pub fn build(self) -> Router {
        let Self(router) = self;

        #[cfg(feature = "ui")]
        let router = router
            .route(
                "/lit-html.js",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/javascript")],
                        include_str!("../../../ui/lit-html.js"),
                    )
                }),
            )
            .fallback(get(|| async { Html(HTML) }));

        #[cfg(not(feature = "ui"))]
        let router = router.fallback(get(|| async { "OK" }));

        router
    }
}

async fn set_log_levels(
    State(tracing): State<Arc<impl Tracing + Send + Sync>>,
    body: String,
) -> Result<()> {
    tracing.set_log_levels(&body).map_err(|err| match err {
        SetLogLevelsError::InvalidDirectives(_) => (StatusCode::BAD_REQUEST, err.to_string()),
        SetLogLevelsError::ReloadError(_) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    })?;
    Ok(())
}
