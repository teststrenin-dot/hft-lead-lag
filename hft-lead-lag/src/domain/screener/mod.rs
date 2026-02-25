//! Screener — domain layer for lead-lag metrics, cycle analysis, and shadow trading.
//!
//! # Module structure
//! - `state`          — per-symbol state (quotes, drift, lag)
//! - `cycle_tracker`  — divergence/convergence half-life measurement
//! - `shadow_trader`  — paper-trading spike-follow model with DTOs
//! - `utils`          — percentile math, timestamp normalisation

mod catalog_cache;
pub mod cycle_tracker;
pub mod fleet_patch;
mod fleet_reload;
mod policy_views;
pub mod price_samples;
mod quote_ingest;
pub mod shadow_fleet;
pub mod shadow_trader;
pub mod state;
pub mod trader_config;
pub mod utils;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::fmt;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde::Serialize;

use self::fleet_patch::{FleetPatchMode, FleetPatchPlan};
use self::shadow_fleet::generate_grid;
use self::shadow_trader::{ChartData, ShadowDebug};
use self::state::SymbolState;
use self::utils::now_ms;

use crate::infrastructure::db::DbWriter;

pub use self::shadow_fleet::PolicyConfigSnapshot;
pub use self::shadow_trader::{ChartTrade, ShadowStats};
pub use self::trader_config::{TraderConfig, CONFIG_ID_CONTRACT_VERSION};

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;
const LAG_WINDOW_MS: i64 = 5 * 60 * 1000;
const SYMBOL_STALE_TTL_MS: i64 = 30 * 60 * 1000;
const SYMBOL_CATALOG_MAX_SIZE: usize = 2_000;
const SYMBOL_CATALOG_PRUNE_INTERVAL_MS: i64 = 30_000;
const ROWS_CACHE_MIN_REBUILD_INTERVAL_MS: i64 = 250;

// ---------------------------------------------------------------------------
// ScreenerRow — read-model DTO for API / UI consumption
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ScreenerRow {
    pub symbol: String,
    pub leader_exchange: &'static str,
    pub data_source: &'static str,
    pub is_fallback: bool,
    pub last_update_ms: i64,
    pub lag_ms: f64,
    pub ws_drift_ms: f64,
    pub ws_drift_binance_ms: f64,
    pub ws_drift_gate_ms: f64,
    pub ws_drift_ingress_binance_ms: f64,
    pub ws_drift_ingress_gate_ms: f64,
    pub entry_half_life_ms: f64,
    pub avg_gt_p90_ms: f64,
    pub gate_natr_30m_pct: f64,
    pub volume_24h_usd: f64,
    pub shadow_session_pnl_pct: f64,
    pub shadow_session_trades: usize,
    pub shadow_avg_trade_pct: f64,
    pub shadow_win_rate_pct: f64,
    pub shadow_position: &'static str,
    pub shadow_spikes_detected: usize,
    pub shadow_avg_catchup_pct: f64,
    pub shadow_avg_lag_ms: f64,
}

// ---------------------------------------------------------------------------
// ScreenerStore — thread-safe facade over per-symbol state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ScreenerStore {
    symbols: Arc<DashMap<String, SymbolState>>,
    window_ms: i64,
    fleet_configs: Arc<ArcSwap<Vec<TraderConfig>>>,
    db_writer: Option<DbWriter>,
    current_run_id: Arc<ArcSwap<Option<String>>>,
    last_catalog_prune_ms: Arc<AtomicI64>,
    rows_cache: Arc<ArcSwap<Vec<ScreenerRow>>>,
    rows_cache_last_rebuild_ms: Arc<AtomicI64>,
    rows_cache_dirty: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy)]
pub struct FleetReloadReport {
    pub old_config_count: usize,
    pub new_config_count: usize,
    pub symbols_reset: usize,
    pub drained_trades: usize,
    pub changed_ids_requested: usize,
    pub matched_changed_ids_old: usize,
    pub matched_changed_ids_new: usize,
    pub unmatched_changed_ids: usize,
    pub scope_symbols_requested: usize,
    pub scope_symbols_matched: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum FleetPatchApplyError {
    IncrementalMissingChangedConfigIds,
    IncrementalNewConfigIdsRequireSymbolScope {
        changed_ids_requested: usize,
    },
    IncrementalNoMatchedChangedConfigIds {
        changed_ids_requested: usize,
        scope_symbols_requested: usize,
    },
}

impl fmt::Display for FleetPatchApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncrementalMissingChangedConfigIds => {
                write!(f, "incremental patch requires non-empty changed_config_ids")
            }
            Self::IncrementalNewConfigIdsRequireSymbolScope {
                changed_ids_requested,
            } => write!(
                f,
                "incremental patch with new-only changed ids requires symbol scope (requested_ids={changed_ids_requested})"
            ),
            Self::IncrementalNoMatchedChangedConfigIds {
                changed_ids_requested,
                scope_symbols_requested,
            } => write!(
                f,
                "incremental patch changed_config_ids matched nothing (requested_ids={changed_ids_requested} scope_symbols_requested={scope_symbols_requested})"
            ),
        }
    }
}

impl std::error::Error for FleetPatchApplyError {}

#[derive(Debug, Clone, Copy)]
struct FleetPatchMatchStats {
    changed_ids_requested: usize,
    matched_changed_ids_old: usize,
    matched_changed_ids_new: usize,
    matched_changed_ids_any: usize,
    unmatched_changed_ids: usize,
    has_new_only_changed_ids: bool,
    scope_symbols_requested: usize,
}

impl FleetPatchMatchStats {
    fn has_any_match(self) -> bool {
        self.matched_changed_ids_any > 0
    }
}

impl ScreenerStore {
    pub fn new(window_ms: i64) -> Self {
        Self {
            symbols: Arc::new(DashMap::new()),
            window_ms,
            fleet_configs: Arc::new(ArcSwap::from_pointee(generate_grid())),
            db_writer: None,
            current_run_id: Arc::new(ArcSwap::from_pointee(None)),
            last_catalog_prune_ms: Arc::new(AtomicI64::new(0)),
            rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
            rows_cache_last_rebuild_ms: Arc::new(AtomicI64::new(0)),
            rows_cache_dirty: Arc::new(AtomicBool::new(true)),
        }
    }

    fn mark_rows_cache_dirty(&self) {
        self.rows_cache_dirty.store(true, Ordering::Relaxed);
    }

    /// Attach a db writer for fleet trade persistence.
    pub fn set_db_writer(&mut self, writer: DbWriter) {
        self.db_writer = Some(writer);
    }

    pub fn fleet_configs(&self) -> Arc<Vec<TraderConfig>> {
        self.fleet_configs.load_full()
    }

    pub fn window_ms(&self) -> i64 {
        self.window_ms
    }

    pub fn set_run_id(&self, run_id: Option<String>) {
        self.current_run_id.store(Arc::new(run_id));
    }

    pub fn current_run_id(&self) -> Option<String> {
        (**self.current_run_id.load()).clone()
    }

    /// Set 24h volume for symbols (called once at startup from REST data).
    pub fn set_volumes(&self, volumes: &[(String, f64)]) {
        let mut changed = false;
        for (sym, vol) in volumes {
            let mut state = self.symbols.entry(sym.clone()).or_default();
            let state = state.value_mut();
            if state.volume_24h_usd != *vol {
                state.volume_24h_usd = *vol;
                changed = true;
            }
        }
        if changed {
            self.mark_rows_cache_dirty();
        }
    }

    /// Set Gate 30m NATR (%) snapshots for symbols.
    pub fn set_gate_natr_30m(&self, values: &[(String, f64)]) {
        let mut changed = false;
        for (sym, natr_pct) in values {
            let next = (*natr_pct).max(0.0);
            let mut state = self.symbols.entry(sym.clone()).or_default();
            let state = state.value_mut();
            if (state.gate_natr_30m_pct - next).abs() > f64::EPSILON {
                state.gate_natr_30m_pct = next;
                changed = true;
            }
        }
        if changed {
            self.mark_rows_cache_dirty();
        }
    }

    /// Replace fleet configs for all symbols.
    ///
    /// Existing fleet instances are reset so that new configs are picked up
    /// on the next tick. Pending completed trades are drained and forwarded
    /// to DB writer before reset.
    pub fn replace_fleet_configs(&self, new_configs: Vec<TraderConfig>) -> FleetReloadReport {
        self.try_apply_fleet_patch(
            new_configs,
            FleetPatchPlan::new(
                FleetPatchMode::FullReplace,
                Vec::<u64>::new(),
                None::<Vec<String>>,
            ),
        )
        .expect("full-replace patch must be valid")
    }

    /// Apply a patch plan to fleet configs, resetting only symbols selected
    /// by the planner in incremental mode.
    pub fn try_apply_fleet_patch(
        &self,
        new_configs: Vec<TraderConfig>,
        plan: FleetPatchPlan,
    ) -> Result<FleetReloadReport, FleetPatchApplyError> {
        fleet_reload::try_apply_fleet_patch(self, new_configs, plan)
    }

    /// Apply fleet patch or panic in internal/test call sites where error
    /// propagation is not expected.
    pub fn apply_fleet_patch(
        &self,
        new_configs: Vec<TraderConfig>,
        plan: FleetPatchPlan,
    ) -> FleetReloadReport {
        self.try_apply_fleet_patch(new_configs, plan)
            .expect("patch apply must be valid")
    }

    fn prune_symbol_catalog_if_needed(&self, now_ms: i64) {
        catalog_cache::prune_symbol_catalog_if_needed(self, now_ms);
    }

    #[cfg(test)]
    fn prune_symbol_catalog_with_limits(
        &self,
        now_ms: i64,
        stale_ttl_ms: i64,
        max_symbols: usize,
    ) -> usize {
        catalog_cache::prune_symbol_catalog_with_limits(self, now_ms, stale_ttl_ms, max_symbols)
    }

    /// Force flush pending DB writer buffers (best effort).
    pub async fn flush_db_writer(&self) {
        if let Some(writer) = self.db_writer.clone() {
            writer.flush_all().await;
        }
    }

    /// Ingest a new quote from an exchange.
    ///
    /// Only bid/ask prices are needed — quantities are irrelevant for
    /// spread, drift, and shadow-trading calculations.
    pub fn update(
        &self,
        symbol: &str,
        exchange: &'static str,
        bid: f64,
        ask: f64,
        timestamp_ns: i64,
        local_receive_ts_ns: i64,
    ) {
        quote_ingest::update(
            self,
            symbol,
            exchange,
            bid,
            ask,
            timestamp_ns,
            local_receive_ts_ns,
        );
    }

    pub fn rows_sorted(&self) -> Vec<ScreenerRow> {
        catalog_cache::rows_sorted(self)
    }

    pub fn shadow_debug(&self, symbol: &str) -> Option<ShadowDebug> {
        self.symbols
            .get(symbol)
            .map(|s| s.shadow.debug(&s.price_samples))
    }

    pub fn chart_data(&self, symbol: &str) -> Option<ChartData> {
        self.symbols
            .get(symbol)
            .map(|s| s.shadow.chart_data(symbol, &s.price_samples))
    }

    pub fn top_policy_configs(
        &self,
        symbol: &str,
        top_k: usize,
    ) -> Option<Vec<PolicyConfigSnapshot>> {
        policy_views::top_policy_configs(self, symbol, top_k)
    }

    pub fn fleet_policy_overview(
        &self,
        top_k: usize,
        max_symbols: usize,
    ) -> Vec<(String, Vec<PolicyConfigSnapshot>)> {
        policy_views::fleet_policy_overview(self, top_k, max_symbols)
    }
}

impl Default for ScreenerStore {
    fn default() -> Self {
        Self::new(TEN_MINUTES_MS)
    }
}


#[cfg(test)]
mod tests;
