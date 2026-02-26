//! HTTP server — routing and configuration.

use arc_swap::ArcSwap;
use axum::{
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::sync::Arc;

use crate::domain::screener::ScreenerStore;

use super::handlers::{self, HttpState};
use super::templates;

/// HTTP server configuration
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub bind_address: String,
    pub port: u16,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 5000,
        }
    }
}

/// Shared health state (lock-free via atomics)
#[derive(Debug)]
pub struct HealthState {
    pub binance_connected: AtomicBool,
    pub gate_connected: AtomicBool,
    pub binance_last_tick_ms: AtomicI64,
    pub gate_last_tick_ms: AtomicI64,
    pub trial_last_ack_ms: AtomicI64,
    pub trial_last_ack_error: AtomicBool,
    pub trial_queue_depth: AtomicU64,
    pub trial_queue_quarantined: AtomicU64,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            binance_connected: AtomicBool::new(false),
            gate_connected: AtomicBool::new(false),
            binance_last_tick_ms: AtomicI64::new(0),
            gate_last_tick_ms: AtomicI64::new(0),
            trial_last_ack_ms: AtomicI64::new(0),
            trial_last_ack_error: AtomicBool::new(false),
            trial_queue_depth: AtomicU64::new(0),
            trial_queue_quarantined: AtomicU64::new(0),
        }
    }
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP server for REST API
pub struct HttpServer {
    config: HttpServerConfig,
    min_volume_usd: f64,
    screener: ScreenerStore,
    health: Arc<HealthState>,
}

impl HttpServer {
    pub fn with_runtime(
        config: HttpServerConfig,
        min_volume_usd: f64,
        screener: ScreenerStore,
        health: Arc<HealthState>,
    ) -> Self {
        Self {
            config,
            min_volume_usd,
            screener,
            health,
        }
    }

    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.config.bind_address, self.config.port)
    }

    /// Start serving on a pre-bound listener (fail-fast: bind in main, serve in task)
    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db_path = PathBuf::from("data/optimizer.db");
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Run schema init/migrations once on server boot; handlers use read-only opens.
        let _ = crate::infrastructure::db::open_db(&db_path)?;

        let state = Arc::new(HttpState {
            min_volume_usd: self.min_volume_usd,
            screener: self.screener.clone(),
            natr_cache: Arc::new(DashMap::new()),
            fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
            fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
            fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
            health: self.health.clone(),
            trial_runner: super::runner::TrialRunnerManager::new(
                super::runner::resolve_runner_workdir(),
            ),
            db_path,
        });

        let app = Router::new()
            .route(endpoints::HEALTH, get(handlers::health))
            .route(endpoints::SYMBOLS, get(handlers::get_symbols))
            .route(endpoints::SCREENER_DATA, get(handlers::get_screener))
            .route(endpoints::SCREENER_PAGE, get(templates::screener_page))
            .route(
                "/api/v1/portfolio/active",
                get(handlers::get_portfolio_active),
            )
            .route(
                "/api/v1/portfolio/candidates",
                get(handlers::get_portfolio_candidates),
            )
            .route(
                "/api/v1/portfolio/guards",
                get(handlers::get_portfolio_guards),
            )
            .route("/api/v1/shadow/:symbol", get(handlers::get_shadow_debug))
            .route("/api/v1/chart/:symbol", get(handlers::get_chart_data))
            .route("/api/v1/fleet", get(handlers::get_fleet_ranking))
            .route("/api/v1/fleet/ranked", get(handlers::get_fleet_ranked))
            .route("/api/v1/fleet/symbols", get(handlers::get_fleet_by_symbol))
            .route(
                "/api/v1/fleet/policy",
                get(handlers::get_fleet_policy_overview),
            )
            .route(
                "/api/v1/fleet/policy/:symbol",
                get(handlers::get_fleet_policy_for_symbol),
            )
            .route("/api/v1/forward/runs", get(handlers::get_forward_runs))
            .route(
                "/api/v1/forward/symbols",
                get(handlers::get_forward_by_symbol),
            )
            .route("/fleet", get(templates::fleet_page))
            .route("/trials", get(templates::trials_page))
            .route("/api/v1/trials", get(handlers::get_trial_runs))
            .route("/api/v1/trials/axes", get(handlers::get_trial_axes))
            .route("/api/v1/trials/:run_id", get(handlers::get_trial_configs))
            .route(
                "/api/v1/trials/runner/config",
                get(handlers::get_trial_runner_config),
            )
            .route(
                "/api/v1/trials/runner/status",
                get(handlers::get_trial_runner_status),
            )
            .route(
                "/api/v1/trials/runner/start",
                post(handlers::start_trial_runner),
            )
            .route(
                "/api/v1/trials/runner/stop",
                post(handlers::stop_trial_runner),
            )
            .with_state(state);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

/// Active API endpoints
pub mod endpoints {
    pub const HEALTH: &str = "/health";
    pub const SYMBOLS: &str = "/api/v1/symbols";
    pub const SCREENER_DATA: &str = "/api/v1/screener";
    pub const SCREENER_PAGE: &str = "/screener";
}
