//! Screener dashboard page templates.

use axum::response::Html;

const SCREENER_HTML: &str = include_str!("templates/screener.html");
const TRIALS_HTML: &str = include_str!("templates/trials.html");
const PORTFOLIO_HTML: &str = include_str!("templates/portfolio.html");

pub async fn screener_page() -> Html<&'static str> {
    Html(SCREENER_HTML)
}

fn trials_dashboard_page() -> Html<&'static str> {
    Html(TRIALS_HTML)
}

pub async fn fleet_page() -> Html<&'static str> {
    trials_dashboard_page()
}

pub async fn trials_page() -> Html<&'static str> {
    trials_dashboard_page()
}

pub async fn portfolio_page() -> Html<&'static str> {
    Html(PORTFOLIO_HTML)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn screener_page_contains_portfolio_navigation_link() {
        assert!(
            SCREENER_HTML.contains("/portfolio"),
            "screener page should link to portfolio race UI"
        );
    }

    #[test]
    fn trials_page_contains_portfolio_navigation_link() {
        assert!(
            TRIALS_HTML.contains("/portfolio"),
            "trials page should link to portfolio race UI"
        );
    }

    #[test]
    fn portfolio_template_exists_and_uses_portfolio_endpoints() {
        let html = std::fs::read_to_string("src/api/templates/portfolio.html")
            .expect("portfolio template should exist");
        for endpoint in [
            "/api/v1/portfolio/active",
            "/api/v1/portfolio/candidates",
            "/api/v1/portfolio/performance",
            "/api/v1/portfolio/guards",
        ] {
            assert!(
                html.contains(endpoint),
                "portfolio template should query endpoint {endpoint}"
            );
        }
    }
}
