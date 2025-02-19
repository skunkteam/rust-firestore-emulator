#[cfg(feature = "ui")]
use axum::response::Html;
use axum::{
    http::{header, HeaderValue},
    routing::get,
    Router,
};
use emulator_database::FirestoreProject;
use emulator_tracing::Tracing;
use tower_http::{set_header::SetResponseHeaderLayer, trace::TraceLayer};

mod emulator;
mod logging;

#[cfg(feature = "ui")]
const HTML: &str = concat!(
    include_str!("../../../ui/index.html"),
    "<script>version.innerHTML=\"",
    env!("CARGO_PKG_VERSION"),
    "\"</script>",
);

#[allow(clippy::declare_interior_mutable_const)]
const NO_CACHE: HeaderValue = HeaderValue::from_static("no-cache");

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
            .layer(SetResponseHeaderLayer::overriding(
                header::CACHE_CONTROL,
                NO_CACHE,
            ))
            .layer(TraceLayer::new_for_http())
    }
}
