//! Screener dashboard page templates.

use axum::response::Html;

const SCREENER_HTML: &str = include_str!("templates/screener.html");
const FLEET_HTML: &str = include_str!("templates/fleet.html");
const TRIALS_HTML: &str = include_str!("templates/trials.html");

pub async fn screener_page() -> Html<&'static str> {
    Html(SCREENER_HTML)
}

pub async fn fleet_page() -> Html<&'static str> {
    Html(FLEET_HTML)
}

pub async fn trials_page() -> Html<&'static str> {
    Html(TRIALS_HTML)
}
