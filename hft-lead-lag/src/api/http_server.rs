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
    pub runtime_last_recv_ws_frame_ts_ns: AtomicI64,
    pub runtime_last_parsed_ts_ns: AtomicI64,
    pub runtime_last_state_updated_ts_ns: AtomicI64,
    pub runtime_last_signal_decided_ts_ns: AtomicI64,
    pub runtime_last_order_intent_enqueued_ts_ns: AtomicI64,
    pub runtime_last_order_intent_sent_ts_ns: AtomicI64,
    pub runtime_ingest_samples: AtomicU64,
    pub runtime_ingest_p50_us: AtomicU64,
    pub runtime_ingest_p95_us: AtomicU64,
    pub runtime_ingest_p99_us: AtomicU64,
    pub runtime_ingest_max_us: AtomicU64,
    pub runtime_decision_samples: AtomicU64,
    pub runtime_decision_p50_us: AtomicU64,
    pub runtime_decision_p95_us: AtomicU64,
    pub runtime_decision_p99_us: AtomicU64,
    pub runtime_decision_max_us: AtomicU64,
    pub runtime_end_to_end_samples: AtomicU64,
    pub runtime_end_to_end_p50_us: AtomicU64,
    pub runtime_end_to_end_p95_us: AtomicU64,
    pub runtime_end_to_end_p99_us: AtomicU64,
    pub runtime_end_to_end_max_us: AtomicU64,
    pub runtime_drift_samples: AtomicU64,
    pub runtime_drift_avg_ms: AtomicI64,
    pub runtime_drift_p50_ms: AtomicI64,
    pub runtime_drift_p95_ms: AtomicI64,
    pub runtime_drift_p99_ms: AtomicI64,
    pub runtime_drift_abs_p99_ms: AtomicU64,
    pub runtime_drift_abs_max_ms: AtomicU64,
    pub runtime_binance_msg_queue_depth: AtomicU64,
    pub runtime_gate_msg_queue_depth: AtomicU64,
    pub runtime_signal_backlog_depth: AtomicU64,
    pub runtime_control_queue_depth: AtomicU64,
    pub runtime_control_dropped_updates: AtomicU64,
    pub runtime_execution_queue_depth: AtomicU64,
    pub runtime_execution_sent_intents: AtomicU64,
    pub runtime_execution_dropped_intents: AtomicU64,
    pub runtime_execution_send_timeouts: AtomicU64,
    pub runtime_execution_kill_switch_active: AtomicBool,
    pub runtime_execution_intent_to_sent_samples: AtomicU64,
    pub runtime_execution_intent_to_sent_p50_us: AtomicU64,
    pub runtime_execution_intent_to_sent_p95_us: AtomicU64,
    pub runtime_execution_intent_to_sent_p99_us: AtomicU64,
    pub runtime_execution_intent_to_sent_max_us: AtomicU64,
    pub runtime_rm4_last_eval_ms: AtomicI64,
    pub runtime_rm4_last_window_breached: AtomicBool,
    pub runtime_rm4_breach_streak: AtomicU64,
    pub runtime_hft_mode_degraded: AtomicBool,
    pub runtime_hft_mode_ever_degraded: AtomicBool,
    pub runtime_last_eval_execution_dropped_intents: AtomicU64,
    pub runtime_last_eval_execution_send_timeouts: AtomicU64,
    pub runtime_last_eval_control_dropped_updates: AtomicU64,
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
            runtime_last_recv_ws_frame_ts_ns: AtomicI64::new(0),
            runtime_last_parsed_ts_ns: AtomicI64::new(0),
            runtime_last_state_updated_ts_ns: AtomicI64::new(0),
            runtime_last_signal_decided_ts_ns: AtomicI64::new(0),
            runtime_last_order_intent_enqueued_ts_ns: AtomicI64::new(0),
            runtime_last_order_intent_sent_ts_ns: AtomicI64::new(0),
            runtime_ingest_samples: AtomicU64::new(0),
            runtime_ingest_p50_us: AtomicU64::new(0),
            runtime_ingest_p95_us: AtomicU64::new(0),
            runtime_ingest_p99_us: AtomicU64::new(0),
            runtime_ingest_max_us: AtomicU64::new(0),
            runtime_decision_samples: AtomicU64::new(0),
            runtime_decision_p50_us: AtomicU64::new(0),
            runtime_decision_p95_us: AtomicU64::new(0),
            runtime_decision_p99_us: AtomicU64::new(0),
            runtime_decision_max_us: AtomicU64::new(0),
            runtime_end_to_end_samples: AtomicU64::new(0),
            runtime_end_to_end_p50_us: AtomicU64::new(0),
            runtime_end_to_end_p95_us: AtomicU64::new(0),
            runtime_end_to_end_p99_us: AtomicU64::new(0),
            runtime_end_to_end_max_us: AtomicU64::new(0),
            runtime_drift_samples: AtomicU64::new(0),
            runtime_drift_avg_ms: AtomicI64::new(0),
            runtime_drift_p50_ms: AtomicI64::new(0),
            runtime_drift_p95_ms: AtomicI64::new(0),
            runtime_drift_p99_ms: AtomicI64::new(0),
            runtime_drift_abs_p99_ms: AtomicU64::new(0),
            runtime_drift_abs_max_ms: AtomicU64::new(0),
            runtime_binance_msg_queue_depth: AtomicU64::new(0),
            runtime_gate_msg_queue_depth: AtomicU64::new(0),
            runtime_signal_backlog_depth: AtomicU64::new(0),
            runtime_control_queue_depth: AtomicU64::new(0),
            runtime_control_dropped_updates: AtomicU64::new(0),
            runtime_execution_queue_depth: AtomicU64::new(0),
            runtime_execution_sent_intents: AtomicU64::new(0),
            runtime_execution_dropped_intents: AtomicU64::new(0),
            runtime_execution_send_timeouts: AtomicU64::new(0),
            runtime_execution_kill_switch_active: AtomicBool::new(false),
            runtime_execution_intent_to_sent_samples: AtomicU64::new(0),
            runtime_execution_intent_to_sent_p50_us: AtomicU64::new(0),
            runtime_execution_intent_to_sent_p95_us: AtomicU64::new(0),
            runtime_execution_intent_to_sent_p99_us: AtomicU64::new(0),
            runtime_execution_intent_to_sent_max_us: AtomicU64::new(0),
            runtime_rm4_last_eval_ms: AtomicI64::new(0),
            runtime_rm4_last_window_breached: AtomicBool::new(false),
            runtime_rm4_breach_streak: AtomicU64::new(0),
            runtime_hft_mode_degraded: AtomicBool::new(false),
            runtime_hft_mode_ever_degraded: AtomicBool::new(false),
            runtime_last_eval_execution_dropped_intents: AtomicU64::new(0),
            runtime_last_eval_execution_send_timeouts: AtomicU64::new(0),
            runtime_last_eval_control_dropped_updates: AtomicU64::new(0),
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
    surface: HttpServerSurface,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpServerSurface {
    Full,
    HealthOnly,
}

impl HttpServer {
    pub fn with_runtime(
        config: HttpServerConfig,
        min_volume_usd: f64,
        screener: ScreenerStore,
        health: Arc<HealthState>,
        surface: HttpServerSurface,
    ) -> Self {
        Self {
            config,
            min_volume_usd,
            screener,
            health,
            surface,
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

        let app = match self.surface {
            HttpServerSurface::HealthOnly => Router::new()
                .route(endpoints::HEALTH, get(handlers::health))
                .with_state(state),
            HttpServerSurface::Full => Router::new()
                .route(endpoints::HEALTH, get(handlers::health))
                .route(endpoints::SYMBOLS, get(handlers::get_symbols))
                .route(endpoints::SCREENER_DATA, get(handlers::get_screener))
                .route(endpoints::SCREENER_PAGE, get(templates::screener_page))
                .route(endpoints::PORTFOLIO_PAGE, get(templates::portfolio_page))
                .route(
                    endpoints::PORTFOLIO_ACTIVE,
                    get(handlers::get_portfolio_active),
                )
                .route(
                    endpoints::PORTFOLIO_CANDIDATES,
                    get(handlers::get_portfolio_candidates),
                )
                .route(
                    endpoints::PORTFOLIO_PERFORMANCE,
                    get(handlers::get_portfolio_performance),
                )
                .route(
                    endpoints::PORTFOLIO_GUARDS,
                    get(handlers::get_portfolio_guards),
                )
                .route(endpoints::SHADOW_DEBUG, get(handlers::get_shadow_debug))
                .route(endpoints::CHART_DATA, get(handlers::get_chart_data))
                .route(endpoints::FLEET_RANKING, get(handlers::get_fleet_ranking))
                .route(endpoints::FLEET_RANKED, get(handlers::get_fleet_ranked))
                .route(endpoints::FLEET_SYMBOLS, get(handlers::get_fleet_by_symbol))
                .route(
                    endpoints::FLEET_POLICY_OVERVIEW,
                    get(handlers::get_fleet_policy_overview),
                )
                .route(
                    endpoints::FLEET_POLICY_FOR_SYMBOL,
                    get(handlers::get_fleet_policy_for_symbol),
                )
                .route(endpoints::FORWARD_RUNS, get(handlers::get_forward_runs))
                .route(
                    endpoints::FORWARD_SYMBOLS,
                    get(handlers::get_forward_by_symbol),
                )
                .route(
                    endpoints::FORWARD_FRESH_START,
                    post(handlers::post_forward_fresh_start),
                )
                .route(endpoints::FLEET_PAGE, get(templates::fleet_page))
                .route(endpoints::TRIALS_PAGE, get(templates::trials_page))
                .route(endpoints::TRIALS, get(handlers::get_trial_runs))
                .route(endpoints::TRIALS_AXES, get(handlers::get_trial_axes))
                .route(endpoints::TRIALS_RUN_ID, get(handlers::get_trial_configs))
                .route(
                    endpoints::TRIALS_RUNNER_CONFIG,
                    get(handlers::get_trial_runner_config),
                )
                .route(
                    endpoints::TRIALS_RUNNER_STATUS,
                    get(handlers::get_trial_runner_status),
                )
                .route(
                    endpoints::TRIALS_RUNNER_START,
                    post(handlers::start_trial_runner),
                )
                .route(
                    endpoints::TRIALS_RUNNER_STOP,
                    post(handlers::stop_trial_runner),
                )
                .with_state(state),
        };

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
    pub const PORTFOLIO_PAGE: &str = "/portfolio";
    pub const PORTFOLIO_ACTIVE: &str = "/api/v1/portfolio/active";
    pub const PORTFOLIO_CANDIDATES: &str = "/api/v1/portfolio/candidates";
    pub const PORTFOLIO_PERFORMANCE: &str = "/api/v1/portfolio/performance";
    pub const PORTFOLIO_GUARDS: &str = "/api/v1/portfolio/guards";
    pub const SHADOW_DEBUG: &str = "/api/v1/shadow/:symbol";
    pub const CHART_DATA: &str = "/api/v1/chart/:symbol";
    pub const FLEET_RANKING: &str = "/api/v1/fleet";
    pub const FLEET_RANKED: &str = "/api/v1/fleet/ranked";
    pub const FLEET_SYMBOLS: &str = "/api/v1/fleet/symbols";
    pub const FLEET_POLICY_OVERVIEW: &str = "/api/v1/fleet/policy";
    pub const FLEET_POLICY_FOR_SYMBOL: &str = "/api/v1/fleet/policy/:symbol";
    pub const FORWARD_RUNS: &str = "/api/v1/forward/runs";
    pub const FORWARD_SYMBOLS: &str = "/api/v1/forward/symbols";
    pub const FORWARD_FRESH_START: &str = "/api/v1/forward/fresh-start";
    pub const FLEET_PAGE: &str = "/fleet";
    pub const TRIALS_PAGE: &str = "/trials";
    pub const TRIALS: &str = "/api/v1/trials";
    pub const TRIALS_AXES: &str = "/api/v1/trials/axes";
    pub const TRIALS_RUN_ID: &str = "/api/v1/trials/:run_id";
    pub const TRIALS_RUNNER_CONFIG: &str = "/api/v1/trials/runner/config";
    pub const TRIALS_RUNNER_STATUS: &str = "/api/v1/trials/runner/status";
    pub const TRIALS_RUNNER_START: &str = "/api/v1/trials/runner/start";
    pub const TRIALS_RUNNER_STOP: &str = "/api/v1/trials/runner/stop";
}
