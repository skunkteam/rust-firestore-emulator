#[cfg(feature = "ui")]
use axum::{http::header, response::Html};
use axum::{routing::get, Router};
use emulator_tracing::Tracing;
use emulator_database::FirestoreProject;

mod emulator;
mod logging;

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

    pub fn add_dynamic_tracing(self, tracing: &'static impl Tracing) -> Self {
        let Self(router) = self;
        let router = router.nest(
            "/emulator/v1/logging",
            logging::router().with_state(tracing),
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
