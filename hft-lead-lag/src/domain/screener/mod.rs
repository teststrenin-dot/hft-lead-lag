//! Screener — domain layer for lead-lag metrics, cycle analysis, and shadow trading.
//!
//! # Module structure
//! - `state`          — per-symbol state (quotes, drift, lag)
//! - `cycle_tracker`  — divergence/convergence half-life measurement
//! - `shadow_trader`  — paper-trading spike-follow model with DTOs
//! - `utils`          — percentile math, timestamp normalisation

mod catalog_cache;
mod clock_offset;
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

use std::collections::BTreeMap;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde::Serialize;

use self::fleet_patch::{FleetPatchMode, FleetPatchPlan};
use self::shadow_fleet::{generate_grid, FleetTrade};
use self::shadow_trader::{ChartData, ShadowDebug};
use self::state::SymbolState;
use self::utils::now_ms;

use crate::application::services::{
    PortfolioEngineV1, PortfolioStateV1, SymbolGuardStateV1, SymbolStatsV1,
};
use crate::infrastructure::db::{
    DbWriter, PortfolioCandidateHistoryRecordV1, PortfolioGuardRecordV1, PortfolioStateRecordV1,
};

pub use self::shadow_fleet::PolicyConfigSnapshot;
pub use self::shadow_trader::{ChartTrade, ShadowStats};
pub use self::trader_config::{TraderConfig, CONFIG_ID_CONTRACT_VERSION};

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;
const LAG_WINDOW_MS: i64 = 5 * 60 * 1000;
const SYMBOL_STALE_TTL_MS: i64 = 30 * 60 * 1000;
const SYMBOL_CATALOG_MAX_SIZE: usize = 2_000;
const SYMBOL_CATALOG_PRUNE_INTERVAL_MS: i64 = 30_000;
const ROWS_CACHE_MIN_REBUILD_INTERVAL_MS: i64 = 250;
const PORTFOLIO_REBALANCE_INTERVAL_MS: i64 = 2 * 60 * 1000;

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

#[derive(Debug, Clone, Copy, Default)]
struct TradeAccumulator {
    closed_trades: u32,
    profitable_trades: u32,
    losing_trades: u32,
    pnl_sum_pct: f64,
    first_observed_ts_ms: Option<i64>,
}

impl TradeAccumulator {
    fn observe(&mut self, pnl_pct: f64, ts_ms: i64) {
        self.closed_trades = self.closed_trades.saturating_add(1);
        if pnl_pct > 0.0 {
            self.profitable_trades = self.profitable_trades.saturating_add(1);
        } else if pnl_pct < 0.0 {
            self.losing_trades = self.losing_trades.saturating_add(1);
        }
        self.pnl_sum_pct += pnl_pct;
        self.first_observed_ts_ms = Some(
            self.first_observed_ts_ms
                .map(|existing| existing.min(ts_ms))
                .unwrap_or(ts_ms),
        );
    }

    fn avg_pnl_pct(self) -> f64 {
        if self.closed_trades == 0 {
            0.0
        } else {
            self.pnl_sum_pct / self.closed_trades as f64
        }
    }
}

#[derive(Debug, Default)]
struct PortfolioRuntimeState {
    engine: PortfolioEngineV1,
    last_rebalance_ms: Option<i64>,
    latest_assignment: BTreeMap<String, PortfolioStateV1>,
}

// ---------------------------------------------------------------------------
// ScreenerStore — thread-safe facade over per-symbol state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ScreenerStore {
    symbols: Arc<DashMap<String, SymbolState>>,
    trade_accumulators: Arc<DashMap<String, TradeAccumulator>>,
    portfolio_runtime: Arc<Mutex<PortfolioRuntimeState>>,
    window_ms: i64,
    fleet_configs: Arc<ArcSwap<Vec<TraderConfig>>>,
    db_writer: Option<DbWriter>,
    current_run_id: Arc<ArcSwap<Option<String>>>,
    clock_offsets: Arc<Mutex<clock_offset::ExchangeClockOffsets>>,
    last_catalog_prune_ms: Arc<AtomicI64>,
    rows_cache: Arc<ArcSwap<Vec<ScreenerRow>>>,
    rows_cache_last_rebuild_ms: Arc<AtomicI64>,
    rows_cache_dirty: Arc<AtomicBool>,
    #[cfg(test)]
    candidate_stats_build_count: Arc<AtomicU64>,
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
            trade_accumulators: Arc::new(DashMap::new()),
            portfolio_runtime: Arc::new(Mutex::new(PortfolioRuntimeState::default())),
            window_ms,
            fleet_configs: Arc::new(ArcSwap::from_pointee(generate_grid())),
            db_writer: None,
            current_run_id: Arc::new(ArcSwap::from_pointee(None)),
            clock_offsets: Arc::new(Mutex::new(clock_offset::ExchangeClockOffsets::default())),
            last_catalog_prune_ms: Arc::new(AtomicI64::new(0)),
            rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
            rows_cache_last_rebuild_ms: Arc::new(AtomicI64::new(0)),
            rows_cache_dirty: Arc::new(AtomicBool::new(true)),
            #[cfg(test)]
            candidate_stats_build_count: Arc::new(AtomicU64::new(0)),
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

    pub(super) fn corrected_exchange_ts_ms(
        &self,
        exchange: &str,
        exchange_ts_ms: i64,
        ingress_ts_ms: i64,
    ) -> i64 {
        self.clock_offsets
            .lock()
            .expect("clock offset mutex poisoned")
            .corrected_exchange_ms(exchange, exchange_ts_ms, ingress_ts_ms)
    }

    /// Latest portfolio assignment snapshot (v1 runtime).
    pub fn portfolio_assignment_v1(&self) -> BTreeMap<String, PortfolioStateV1> {
        self.portfolio_runtime
            .lock()
            .expect("portfolio runtime mutex poisoned")
            .latest_assignment
            .clone()
    }

    /// Current configured portfolio ids.
    pub fn portfolio_ids_v1(&self) -> Vec<String> {
        self.portfolio_runtime
            .lock()
            .expect("portfolio runtime mutex poisoned")
            .engine
            .portfolio_ids()
            .to_vec()
    }

    /// Replace runtime portfolio id set (v1).
    pub fn set_portfolio_ids_v1(&self, portfolio_ids: Vec<String>) {
        let mut runtime = self
            .portfolio_runtime
            .lock()
            .expect("portfolio runtime mutex poisoned");
        runtime.engine.set_portfolio_ids(portfolio_ids);
        runtime.latest_assignment = runtime
            .engine
            .portfolio_ids()
            .iter()
            .map(|id| (id.clone(), PortfolioStateV1::default()))
            .collect();
        runtime.last_rebalance_ms = None;
    }

    /// Build global candidate stats from cumulative per-symbol history.
    pub fn portfolio_candidate_stats_v1(&self, now_ms: i64) -> Vec<SymbolStatsV1> {
        self.global_candidate_stats(now_ms)
    }

    /// Restore in-memory portfolio runtime from persisted DB snapshots.
    pub fn restore_portfolio_runtime_v1_from_db_rows(
        &self,
        states: &[PortfolioStateRecordV1],
        guards: &[PortfolioGuardRecordV1],
    ) {
        let mut runtime = self
            .portfolio_runtime
            .lock()
            .expect("portfolio runtime mutex poisoned");

        let state_by_id: std::collections::HashMap<&str, &PortfolioStateRecordV1> = states
            .iter()
            .map(|row| (row.portfolio_id.as_str(), row))
            .collect();
        runtime.latest_assignment = runtime
            .engine
            .portfolio_ids()
            .iter()
            .map(|portfolio_id| {
                let state = state_by_id
                    .get(portfolio_id.as_str())
                    .map(|row| PortfolioStateV1 {
                        shortlist: row.shortlist.clone(),
                        active_symbols: row.active_symbols.clone(),
                    })
                    .unwrap_or_default();
                (portfolio_id.clone(), state)
            })
            .collect();

        runtime.engine.replace_guard_states(
            guards
                .iter()
                .map(|row| {
                    (
                        row.symbol.clone(),
                        SymbolGuardStateV1 {
                            streak_count: row.streak_count,
                            first_streak_ts_ms: row.first_streak_ts_ms,
                            cooldown_until_ms: row.cooldown_until_ms,
                        },
                    )
                })
                .collect(),
        );

        runtime.last_rebalance_ms = states
            .iter()
            .map(|row| row.updated_at_ms)
            .chain(guards.iter().map(|row| row.updated_at_ms))
            .max();
    }

    /// Restore per-symbol cumulative trade history used for candidate stats.
    pub fn restore_portfolio_candidate_history_v1_from_db_rows(
        &self,
        rows: &[PortfolioCandidateHistoryRecordV1],
    ) {
        self.trade_accumulators.clear();
        for row in rows {
            if row.closed_trades == 0 {
                continue;
            }
            self.trade_accumulators.insert(
                row.symbol.clone(),
                TradeAccumulator {
                    closed_trades: row.closed_trades,
                    profitable_trades: row.profitable_trades.min(row.closed_trades),
                    losing_trades: row.losing_trades.min(row.closed_trades),
                    pnl_sum_pct: row.pnl_sum_pct,
                    first_observed_ts_ms: row.first_trade_ts_ms,
                },
            );
        }
    }

    /// Snapshot guard/cooldown state per symbol used by portfolio runtime.
    pub fn portfolio_guard_states_v1(&self) -> Vec<(String, SymbolGuardStateV1)> {
        self.portfolio_runtime
            .lock()
            .expect("portfolio runtime mutex poisoned")
            .engine
            .guard_states()
    }

    #[cfg(test)]
    fn portfolio_last_rebalance_ms(&self) -> Option<i64> {
        self.portfolio_runtime
            .lock()
            .expect("portfolio runtime mutex poisoned")
            .last_rebalance_ms
    }

    pub(super) fn observe_closed_trade_for_portfolio(
        &self,
        symbol: &str,
        pnl_pct: f64,
        is_stop_loss: bool,
        ts_ms: i64,
    ) {
        self.trade_accumulators
            .entry(symbol.to_string())
            .or_default()
            .value_mut()
            .observe(pnl_pct, ts_ms);

        let maybe_snapshot = {
            let mut runtime = self
                .portfolio_runtime
                .lock()
                .expect("portfolio runtime mutex poisoned");
            let before = runtime.engine.guard_state(symbol);
            runtime
                .engine
                .record_closed_trade(symbol, pnl_pct, is_stop_loss, ts_ms);
            let after = runtime.engine.guard_state(symbol);
            if before != after {
                Some(Self::portfolio_snapshot_records(&runtime, ts_ms))
            } else {
                None
            }
        };

        if let (Some((states, guards)), Some(writer)) = (maybe_snapshot, self.db_writer.clone()) {
            writer.send_portfolio_snapshot_v1(states, guards);
        }
    }

    pub(super) fn handle_drained_fleet_trades(&self, drained_trades: Vec<FleetTrade>) -> usize {
        if drained_trades.is_empty() {
            return 0;
        }
        for ft in &drained_trades {
            self.observe_closed_trade_for_portfolio(
                &ft.symbol,
                ft.trade.pnl_pct,
                ft.trade.exit_reason == "stop_loss",
                ft.trade.ts_ms,
            );
        }

        let drained_count = drained_trades.len();
        if let Some(writer) = self.db_writer.clone() {
            writer.send(drained_trades);
        }
        drained_count
    }

    #[cfg(test)]
    pub fn portfolio_observe_closed_trade_v1(
        &self,
        symbol: &str,
        pnl_pct: f64,
        is_stop_loss: bool,
        ts_ms: i64,
    ) {
        self.observe_closed_trade_for_portfolio(symbol, pnl_pct, is_stop_loss, ts_ms);
    }

    pub(super) fn maybe_rebalance_portfolios(&self, now_ms: i64) {
        {
            let runtime = self
                .portfolio_runtime
                .lock()
                .expect("portfolio runtime mutex poisoned");
            if let Some(last_ms) = runtime.last_rebalance_ms {
                if now_ms.saturating_sub(last_ms) < PORTFOLIO_REBALANCE_INTERVAL_MS {
                    return;
                }
            }
        }

        let candidates = self.global_candidate_stats(now_ms);
        let maybe_snapshot = {
            let mut runtime = self
                .portfolio_runtime
                .lock()
                .expect("portfolio runtime mutex poisoned");

            if let Some(last_ms) = runtime.last_rebalance_ms {
                if now_ms.saturating_sub(last_ms) < PORTFOLIO_REBALANCE_INTERVAL_MS {
                    return;
                }
            }

            runtime.latest_assignment = runtime.engine.assign_without_overlap(&candidates, now_ms);
            runtime.last_rebalance_ms = Some(now_ms);
            Some(Self::portfolio_snapshot_records(&runtime, now_ms))
        };

        if let (Some((states, guards)), Some(writer)) = (maybe_snapshot, self.db_writer.clone()) {
            writer.send_portfolio_snapshot_v1(states, guards);
        }
    }

    /// Trigger portfolio rebalance scheduler tick (v1).
    /// Cadence gate is enforced internally (`2m`) and does not depend on tick flow.
    pub fn portfolio_scheduler_tick_v1(&self, now_ms: i64) {
        self.maybe_rebalance_portfolios(now_ms);
    }

    #[cfg(test)]
    pub fn portfolio_maybe_rebalance_v1(&self, now_ms: i64) {
        self.maybe_rebalance_portfolios(now_ms);
    }

    #[cfg(test)]
    pub fn portfolio_candidate_build_count_v1(&self) -> u64 {
        self.candidate_stats_build_count.load(Ordering::Relaxed)
    }

    fn global_candidate_stats(&self, now_ms: i64) -> Vec<SymbolStatsV1> {
        #[cfg(test)]
        self.candidate_stats_build_count
            .fetch_add(1, Ordering::Relaxed);

        let mut out = Vec::new();
        for entry in self.trade_accumulators.iter() {
            let symbol = entry.key();
            let acc = *entry.value();
            let first_tick_ms = self
                .symbols
                .get(symbol.as_str())
                .and_then(|state| state.first_tick_ms)
                .or(acc.first_observed_ts_ms)
                .unwrap_or(now_ms);
            let age_minutes_from_first_tick = now_ms
                .saturating_sub(first_tick_ms)
                .max(0)
                .div_euclid(60_000) as u64;
            out.push(SymbolStatsV1 {
                symbol: symbol.clone(),
                age_minutes_from_first_tick,
                closed_trades: acc.closed_trades,
                profitable_trades: acc.profitable_trades,
                losing_trades: acc.losing_trades,
                avg_pnl_pct: acc.avg_pnl_pct(),
            });
        }
        out
    }

    fn portfolio_snapshot_records(
        runtime: &PortfolioRuntimeState,
        updated_at_ms: i64,
    ) -> (Vec<PortfolioStateRecordV1>, Vec<PortfolioGuardRecordV1>) {
        let states = runtime
            .engine
            .portfolio_ids()
            .iter()
            .map(|portfolio_id| {
                let entry = runtime
                    .latest_assignment
                    .get(portfolio_id)
                    .cloned()
                    .unwrap_or_default();
                PortfolioStateRecordV1 {
                    portfolio_id: portfolio_id.clone(),
                    shortlist: entry.shortlist,
                    active_symbols: entry.active_symbols,
                    updated_at_ms,
                }
            })
            .collect();
        let guards = runtime
            .engine
            .guard_states()
            .into_iter()
            .map(|(symbol, guard)| PortfolioGuardRecordV1 {
                symbol,
                streak_count: guard.streak_count,
                first_streak_ts_ms: guard.first_streak_ts_ms,
                cooldown_until_ms: guard.cooldown_until_ms,
                updated_at_ms,
            })
            .collect();
        (states, guards)
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
