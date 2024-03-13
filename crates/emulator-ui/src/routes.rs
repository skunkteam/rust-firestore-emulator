use axum::Router;
#[cfg(feature = "ui")]
use axum::{http::header, response::Html, routing::get};
use firestore_database::FirestoreProject;

mod emulator;

#[cfg(feature = "ui")]
const HTML: &str = concat!(
    include_str!("../../../ui/index.html"),
    "<script>version.innerHTML=\"",
    env!("CARGO_PKG_VERSION"),
    "\"</script>",
);

pub fn router() -> Router {
    let router = Router::new()
        .nest("/emulator/v1", emulator::router())
        .with_state(FirestoreProject::get());

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
        .fallback(fallback);

    #[cfg(not(feature = "ui"))]
    let router = router.fallback(|| async { "OK" });

    router
}

#[cfg(feature = "ui")]
async fn fallback() -> Html<&'static str> {
    Html(HTML)
}
