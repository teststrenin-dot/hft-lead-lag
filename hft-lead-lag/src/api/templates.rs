//! Screener dashboard page templates.

use axum::response::Html;

const SCREENER_HTML: &str = include_str!("templates/screener.html");
const FLEET_HTML: &str = include_str!("templates/fleet.html");

pub async fn screener_page() -> Html<&'static str> {
    Html(SCREENER_HTML)
}

pub async fn fleet_page() -> Html<&'static str> {
    Html(FLEET_HTML)
}
