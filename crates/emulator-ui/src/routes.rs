use axum::{http::header, response::Html, routing::get, Router};
use firestore_database::FirestoreProject;

mod emulator;

const HTML: &str = concat!(
    include_str!("../../../ui/index.html"),
    "<script>version.innerHTML=\"",
    env!("CARGO_PKG_VERSION"),
    "\"</script>",
);

pub fn router() -> Router {
    Router::new()
        .nest("/emulator/v1", emulator::router())
        .route(
            "/lit-html.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript")],
                    include_str!("../../../ui/lit-html.js"),
                )
            }),
        )
        .fallback(fallback)
        .with_state(FirestoreProject::get())
}

async fn fallback() -> Html<&'static str> {
    Html(HTML)
}
