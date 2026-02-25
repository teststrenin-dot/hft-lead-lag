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
use self::shadow_fleet::{generate_grid, FleetTickMeta, ShadowFleet};
use self::shadow_trader::{ChartData, ShadowDebug};
use self::state::{Quote, SymbolState};
use self::utils::{now_ms, TimeDomainSample};

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
        if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
            return;
        }

        let clocks = TimeDomainSample::from_raw(timestamp_ns, local_receive_ts_ns, now_ms());
        self.prune_symbol_catalog_if_needed(clocks.decision_ts_ms);

        let mut state = self.symbols.entry(symbol.to_string()).or_default();

        let state = state.value_mut();
        let ws_drift = clocks.decision_ws_drift_ms();
        let ingress_ws_drift = clocks.ingress_ws_drift_ms();
        let quote = Quote {
            bid,
            ask,
            ts_ms: clocks.exchange_event_ts_ms,
        };

        if !state.ingest_quote(exchange, quote, ws_drift, ingress_ws_drift) {
            return;
        }

        if state.binance.is_none() || state.gate.is_none() {
            state.updated_at_ms = clocks.exchange_event_ts_ms;
            state.leader_exchange = exchange;
            state.lag_ms = 0.0;
            self.mark_rows_cache_dirty();
            return;
        }

        state.updated_at_ms = clocks.exchange_event_ts_ms;
        state.update_lag(clocks.exchange_event_ts_ms, LAG_WINDOW_MS);
        state.update_cycles(clocks.exchange_event_ts_ms, self.window_ms);
        state.tick_shadow(clocks.exchange_event_ts_ms, self.window_ms);

        // Fleet: lazy-init on first tick, then tick all + drain trades to db.
        let (binance_ref, gate_ref) = match (state.binance.as_ref(), state.gate.as_ref()) {
            (Some(b), Some(g)) => (b, g),
            _ => return,
        };
        let fleet_configs = self.fleet_configs.load_full();
        let fleet = state
            .fleet
            .get_or_insert_with(|| ShadowFleet::new(fleet_configs.as_ref()));
        let run_id_arc = self.current_run_id.load();
        let run_id_ref = run_id_arc.as_deref();
        fleet.tick_all(
            clocks.exchange_event_ts_ms,
            binance_ref,
            gate_ref,
            &state.price_samples,
            self.window_ms,
            FleetTickMeta {
                symbol,
                gate_natr_30m_pct_at_entry: state.gate_natr_30m_pct,
                run_id: run_id_ref,
            },
        );
        let trades = fleet.drain_trades();
        if !trades.is_empty() {
            if let Some(ref writer) = self.db_writer {
                writer.send(trades);
            }
            // Without a writer attached, drop drained trades to keep fleet queue bounded.
        }
        self.mark_rows_cache_dirty();
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
mod tests {
    use super::{
        shadow_fleet::FleetTrade, FleetPatchApplyError, FleetPatchMode, FleetPatchPlan,
        ScreenerStore, ShadowFleet, SymbolState, TraderConfig,
    };
    use crate::domain::screener::shadow_trader::{ClosedTrade, Direction};

    fn config_with_gap(spike_threshold_bps: f64) -> TraderConfig {
        TraderConfig {
            spike_threshold_bps,
            ..TraderConfig::default()
        }
    }

    fn with_symbol_fleet(store: &ScreenerStore, symbol: &str, configs: &[TraderConfig]) {
        let state = SymbolState {
            fleet: Some(ShadowFleet::new(configs)),
            ..SymbolState::default()
        };
        store.symbols.insert(symbol.to_string(), state);
    }

    fn sample_closed_trade(ts_ms: i64) -> ClosedTrade {
        ClosedTrade {
            direction: Direction::Long,
            entry_ts_ms: ts_ms - 500,
            ts_ms,
            entry_price: 100.0,
            exit_price: 100.2,
            spike_bps: 50.0,
            pnl_pct: 0.2,
            exit_reason: "trailing_take",
            catchup_pct: 0.2,
            catchup_ms: 500,
            gate_spread_at_entry_bps: 1.0,
            gate_natr_30m_pct_at_entry: 0.0,
            hold_ms: 500,
            early_stop_churn: false,
        }
    }

    #[test]
    fn top_policy_configs_returns_none_for_unknown_symbol() {
        let store = ScreenerStore::default();
        assert!(store.top_policy_configs("BTCUSDT", 5).is_none());
    }

    #[test]
    fn top_policy_configs_returns_some_for_known_symbol_fleet() {
        let store = ScreenerStore::default();
        with_symbol_fleet(&store, "BTCUSDT", &[config_with_gap(50.0)]);
        let rows = store
            .top_policy_configs("btcusdt", 5)
            .expect("policy rows for known symbol");
        assert!(rows.is_empty());
    }

    #[test]
    fn fleet_policy_overview_sorts_symbols_and_applies_limit() {
        let store = ScreenerStore::default();
        with_symbol_fleet(&store, "BTCUSDT", &[config_with_gap(50.0)]);
        with_symbol_fleet(&store, "ADAUSDT", &[config_with_gap(60.0)]);

        let overview = store.fleet_policy_overview(5, 1);
        assert_eq!(overview.len(), 1);
        assert_eq!(overview[0].0, "ADAUSDT");
    }

    #[test]
    fn prune_symbol_catalog_with_limits_drops_stale_symbols() {
        let store = ScreenerStore::default();
        let stale = SymbolState {
            updated_at_ms: 1_000,
            ..SymbolState::default()
        };
        let fresh = SymbolState {
            updated_at_ms: 9_950,
            ..SymbolState::default()
        };
        store.symbols.insert("STALE".to_string(), stale);
        store.symbols.insert("FRESH".to_string(), fresh);

        let removed = store.prune_symbol_catalog_with_limits(10_000, 500, 10);

        assert_eq!(removed, 1);
        assert!(store.symbols.get("STALE").is_none());
        assert!(store.symbols.get("FRESH").is_some());
    }

    #[test]
    fn prune_symbol_catalog_with_limits_enforces_cardinality_cap() {
        let store = ScreenerStore::default();
        for idx in 0..5 {
            let state = SymbolState {
                updated_at_ms: 1_000 + idx,
                ..SymbolState::default()
            };
            store.symbols.insert(format!("SYM{idx}"), state);
        }

        let removed = store.prune_symbol_catalog_with_limits(2_000, 10_000, 3);

        assert_eq!(removed, 2);
        assert_eq!(store.symbols.len(), 3);
        assert!(store.symbols.get("SYM0").is_none());
        assert!(store.symbols.get("SYM1").is_none());
        assert!(store.symbols.get("SYM2").is_some());
        assert!(store.symbols.get("SYM3").is_some());
        assert!(store.symbols.get("SYM4").is_some());
    }

    #[test]
    fn update_drains_pending_fleet_trades_even_without_db_writer() {
        let store = ScreenerStore::default();
        let cfg = config_with_gap(55.0);
        with_symbol_fleet(&store, "BTCUSDT", &[cfg]);
        {
            let mut state = store.symbols.get_mut("BTCUSDT").expect("BTCUSDT state");
            let fleet = state.fleet.as_mut().expect("fleet");
            fleet.push_pending_trade_for_test(FleetTrade {
                config_id: cfg.config_id(),
                symbol: "BTCUSDT".to_string(),
                run_id: None,
                trade: sample_closed_trade(2_000),
            });
            assert_eq!(fleet.pending_trades_len(), 1);
        }

        let ts_ns = 1_700_000_000_000_000_000_i64;
        store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
        store.update(
            "BTCUSDT",
            "gate",
            100.0,
            100.1,
            ts_ns + 1_000_000,
            ts_ns + 1_000_000,
        );

        let state = store.symbols.get("BTCUSDT").expect("BTCUSDT state");
        let fleet = state.fleet.as_ref().expect("fleet");
        assert_eq!(fleet.pending_trades_len(), 0);
    }

    #[test]
    fn rows_sorted_marks_live_ws_source_and_update_time() {
        let store = ScreenerStore::default();
        let ts_ns = 1_700_000_000_000_000_000_i64;
        store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
        store.update(
            "BTCUSDT",
            "gate",
            100.0,
            100.1,
            ts_ns + 1_000_000,
            ts_ns + 1_000_000,
        );

        let rows = store.rows_sorted();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.symbol, "BTCUSDT");
        assert_eq!(row.data_source, "ws_live");
        assert!(!row.is_fallback);
        assert!(row.last_update_ms > 0);
    }

    #[test]
    fn rows_sorted_uses_snapshot_within_rebuild_interval() {
        let store = ScreenerStore::default();
        let ts_ns = 1_700_000_000_000_000_000_i64;
        store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
        store.update(
            "BTCUSDT",
            "gate",
            100.0,
            100.1,
            ts_ns + 1_000_000,
            ts_ns + 1_000_000,
        );

        let first = store.rows_sorted();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].volume_24h_usd, 0.0);

        store.set_volumes(&[("BTCUSDT".to_string(), 42.0)]);
        let second = store.rows_sorted();
        assert_eq!(second.len(), 1);
        assert_eq!(
            second[0].volume_24h_usd, 0.0,
            "within cache interval rows must come from previous snapshot"
        );

        std::thread::sleep(std::time::Duration::from_millis(350));
        let third = store.rows_sorted();
        assert_eq!(third.len(), 1);
        assert_eq!(third[0].volume_24h_usd, 42.0);
    }

    #[test]
    fn full_replace_resets_all_symbol_fleets() {
        let store = ScreenerStore::default();
        let old_a = config_with_gap(31.0);
        let old_b = config_with_gap(32.0);
        with_symbol_fleet(&store, "BTCUSDT", &[old_a]);
        with_symbol_fleet(&store, "ETHUSDT", &[old_b]);

        let report = store.apply_fleet_patch(
            vec![config_with_gap(40.0)],
            FleetPatchPlan::new(
                FleetPatchMode::FullReplace,
                Vec::<u64>::new(),
                None::<Vec<String>>,
            ),
        );

        assert_eq!(report.symbols_reset, 2);
        assert_eq!(report.drained_trades, 0);
        assert_eq!(report.changed_ids_requested, 0);
        assert!(store
            .symbols
            .get("BTCUSDT")
            .expect("BTCUSDT state")
            .fleet
            .is_none());
        assert!(store
            .symbols
            .get("ETHUSDT")
            .expect("ETHUSDT state")
            .fleet
            .is_none());
    }

    #[test]
    fn incremental_resets_only_symbols_with_touched_configs() {
        let store = ScreenerStore::default();
        let touched_cfg = config_with_gap(51.0);
        let untouched_cfg = config_with_gap(61.0);
        with_symbol_fleet(&store, "BTCUSDT", &[touched_cfg]);
        with_symbol_fleet(&store, "ETHUSDT", &[untouched_cfg]);

        let report = store.apply_fleet_patch(
            vec![touched_cfg, untouched_cfg],
            FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                [touched_cfg.config_id()],
                None::<Vec<String>>,
            ),
        );

        assert_eq!(report.symbols_reset, 1);
        assert_eq!(report.matched_changed_ids_old, 1);
        assert_eq!(report.matched_changed_ids_new, 1);
        assert_eq!(report.unmatched_changed_ids, 0);
        assert!(store
            .symbols
            .get("BTCUSDT")
            .expect("BTCUSDT state")
            .fleet
            .is_none());
        assert!(store
            .symbols
            .get("ETHUSDT")
            .expect("ETHUSDT state")
            .fleet
            .is_some());
    }

    #[test]
    fn incremental_preserves_unaffected_symbol_state_and_does_not_drain() {
        let store = ScreenerStore::default();
        let touched_cfg = config_with_gap(71.0);
        let untouched_cfg = config_with_gap(81.0);
        with_symbol_fleet(&store, "BTCUSDT", &[touched_cfg]);
        with_symbol_fleet(&store, "ETHUSDT", &[untouched_cfg]);

        let report = store.apply_fleet_patch(
            vec![touched_cfg, untouched_cfg],
            FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                [touched_cfg.config_id()],
                Some(vec!["BTCUSDT".to_string()]),
            ),
        );

        assert_eq!(report.symbols_reset, 1);
        assert_eq!(report.drained_trades, 0);
        assert_eq!(report.scope_symbols_requested, 1);
        assert_eq!(report.scope_symbols_matched, 1);
        assert_eq!(report.unmatched_changed_ids, 0);
        let eth = store.symbols.get("ETHUSDT").expect("ETHUSDT state");
        assert!(eth.fleet.is_some());
        assert_eq!(eth.fleet.as_ref().expect("ETH fleet").len(), 1);
    }

    #[test]
    fn incremental_matches_changed_ids_from_old_or_new_configs() {
        let store = ScreenerStore::default();
        let old_cfg = config_with_gap(91.0);
        with_symbol_fleet(&store, "BTCUSDT", &[old_cfg]);
        with_symbol_fleet(&store, "ETHUSDT", &[old_cfg]);

        let new_cfg = TraderConfig {
            spike_threshold_bps: 92.0,
            ..old_cfg
        };
        let report = store
            .try_apply_fleet_patch(
                vec![new_cfg],
                FleetPatchPlan::new(
                    FleetPatchMode::Incremental,
                    [new_cfg.config_id()],
                    Some(vec!["BTCUSDT".to_string()]),
                ),
            )
            .expect("incremental patch should apply when ids match new configs");

        assert_eq!(report.matched_changed_ids_old, 0);
        assert_eq!(report.matched_changed_ids_new, 1);
        assert_eq!(report.unmatched_changed_ids, 0);
        assert_eq!(report.symbols_reset, 1);
    }

    #[test]
    fn incremental_rejects_when_changed_ids_match_nothing() {
        let store = ScreenerStore::default();
        let old_cfg = config_with_gap(101.0);
        with_symbol_fleet(&store, "BTCUSDT", &[old_cfg]);

        let err = store
            .try_apply_fleet_patch(
                vec![old_cfg],
                FleetPatchPlan::new(FleetPatchMode::Incremental, [u64::MAX], None::<Vec<String>>),
            )
            .expect_err("incremental patch should reject unmatched changed ids");

        assert!(matches!(
            err,
            FleetPatchApplyError::IncrementalNoMatchedChangedConfigIds {
                changed_ids_requested: 1,
                ..
            }
        ));
        assert!(store
            .symbols
            .get("BTCUSDT")
            .expect("BTCUSDT state")
            .fleet
            .is_some());
    }

    #[test]
    fn incremental_rejects_new_only_ids_without_symbol_scope() {
        let store = ScreenerStore::default();
        let old_cfg = config_with_gap(131.0);
        with_symbol_fleet(&store, "BTCUSDT", &[old_cfg]);
        with_symbol_fleet(&store, "ETHUSDT", &[old_cfg]);
        let new_cfg = TraderConfig {
            spike_threshold_bps: 132.0,
            ..old_cfg
        };

        let err = store
            .try_apply_fleet_patch(
                vec![new_cfg],
                FleetPatchPlan::new(
                    FleetPatchMode::Incremental,
                    [new_cfg.config_id()],
                    None::<Vec<String>>,
                ),
            )
            .expect_err("new-only ids without symbol scope must fail");

        assert!(matches!(
            err,
            FleetPatchApplyError::IncrementalNewConfigIdsRequireSymbolScope { .. }
        ));
    }

    #[test]
    fn incremental_with_mixed_old_and_new_ids_resets_symbol_for_new_only_id() {
        let store = ScreenerStore::default();
        let old_a = config_with_gap(111.0);
        let old_b = config_with_gap(121.0);
        with_symbol_fleet(&store, "BTCUSDT", &[old_a]);
        with_symbol_fleet(&store, "ETHUSDT", &[old_b]);

        let new_b = TraderConfig {
            spike_threshold_bps: 122.0,
            ..old_b
        };
        let report = store
            .try_apply_fleet_patch(
                vec![old_a, new_b],
                FleetPatchPlan::new(
                    FleetPatchMode::Incremental,
                    [old_a.config_id(), new_b.config_id()],
                    Some(vec!["ETHUSDT".to_string()]),
                ),
            )
            .expect("mixed old/new incremental patch should apply");

        assert_eq!(report.matched_changed_ids_old, 1);
        assert_eq!(report.matched_changed_ids_new, 2);
        assert_eq!(report.unmatched_changed_ids, 0);
        assert_eq!(report.scope_symbols_requested, 1);
        assert_eq!(report.scope_symbols_matched, 1);
        assert_eq!(report.symbols_reset, 1);
        assert!(
            store
                .symbols
                .get("ETHUSDT")
                .expect("ETHUSDT state")
                .fleet
                .is_none(),
            "new-only changed id must reset in-scope symbol fleet"
        );
    }
}
