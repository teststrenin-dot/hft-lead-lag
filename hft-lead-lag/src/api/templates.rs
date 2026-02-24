//! Screener dashboard page templates.

use axum::response::{Html, Redirect};

const SCREENER_HTML: &str = include_str!("templates/screener.html");
const TRIALS_HTML: &str = include_str!("templates/trials.html");

pub async fn screener_page() -> Html<&'static str> {
    Html(SCREENER_HTML)
}

pub async fn fleet_page() -> Redirect {
    Redirect::temporary("/trials?tab=forward")
}

pub async fn trials_page() -> Html<&'static str> {
    Html(TRIALS_HTML)
}
