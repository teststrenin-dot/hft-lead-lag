//! Screener — domain layer for lead-lag metrics, cycle analysis, and shadow trading.
//!
//! # Module structure
//! - `state`          — per-symbol state (quotes, drift, lag)
//! - `cycle_tracker`  — divergence/convergence half-life measurement
//! - `shadow_trader`  — paper-trading spike-follow model with DTOs
//! - `utils`          — percentile math, timestamp normalisation

pub mod cycle_tracker;
pub mod price_samples;
pub mod shadow_fleet;
pub mod shadow_trader;
pub mod state;
pub mod trader_config;
pub mod utils;

use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;

use self::shadow_fleet::{generate_grid, ShadowFleet};
use self::shadow_trader::{ChartData, ShadowDebug};
use self::state::{Quote, SymbolState};
use self::utils::{now_ms, TimeDomainSample};

use crate::infrastructure::db::DbWriter;

pub use self::shadow_trader::{ChartTrade, ShadowStats};
pub use self::trader_config::TraderConfig;

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;
const LAG_WINDOW_MS: i64 = 5 * 60 * 1000;

// ---------------------------------------------------------------------------
// ScreenerRow — read-model DTO for API / UI consumption
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ScreenerRow {
    pub symbol: String,
    pub leader_exchange: &'static str,
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
    fleet_configs: Arc<Vec<TraderConfig>>,
    db_writer: Option<DbWriter>,
}

impl ScreenerStore {
    pub fn new(window_ms: i64) -> Self {
        Self {
            symbols: Arc::new(DashMap::new()),
            window_ms,
            fleet_configs: Arc::new(generate_grid()),
            db_writer: None,
        }
    }

    /// Attach a db writer for fleet trade persistence.
    pub fn set_db_writer(&mut self, writer: DbWriter) {
        self.db_writer = Some(writer);
    }

    pub fn fleet_configs(&self) -> &[TraderConfig] {
        &self.fleet_configs
    }

    pub fn window_ms(&self) -> i64 {
        self.window_ms
    }

    /// Set 24h volume for symbols (called once at startup from REST data).
    pub fn set_volumes(&self, volumes: &[(String, f64)]) {
        for (sym, vol) in volumes {
            self.symbols.entry(sym.clone()).or_default().volume_24h_usd = *vol;
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
        let fleet = state
            .fleet
            .get_or_insert_with(|| ShadowFleet::new(&self.fleet_configs));
        fleet.tick_all(
            clocks.exchange_event_ts_ms,
            binance_ref,
            gate_ref,
            &state.price_samples,
            self.window_ms,
            symbol,
        );
        if let Some(ref writer) = self.db_writer {
            let trades = fleet.drain_trades();
            if !trades.is_empty() {
                writer.send(trades);
            }
        }
    }

    pub fn rows_sorted(&self) -> Vec<ScreenerRow> {
        let mut rows: Vec<ScreenerRow> = self
            .symbols
            .iter()
            .filter(|item| !item.value().leader_exchange.is_empty())
            .map(|item| {
                let shadow = &item.value().shadow;
                let stats = shadow.stats();
                ScreenerRow {
                    symbol: item.key().clone(),
                    leader_exchange: item.value().leader_exchange,
                    lag_ms: item.value().lag_ms,
                    ws_drift_ms: item.value().drifts.combined,
                    ws_drift_binance_ms: item.value().drifts.binance.unwrap_or(0.0),
                    ws_drift_gate_ms: item.value().drifts.gate.unwrap_or(0.0),
                    ws_drift_ingress_binance_ms: item.value().drifts.binance_ingress.unwrap_or(0.0),
                    ws_drift_ingress_gate_ms: item.value().drifts.gate_ingress.unwrap_or(0.0),
                    entry_half_life_ms: item.value().entry_half_life_ms,
                    avg_gt_p90_ms: item.value().avg_gt_p90_ms,
                    gate_natr_30m_pct: 0.0,
                    volume_24h_usd: item.value().volume_24h_usd,
                    shadow_session_pnl_pct: stats.session_pnl_pct,
                    shadow_session_trades: stats.session_trades,
                    shadow_avg_trade_pct: stats.avg_trade_pct,
                    shadow_win_rate_pct: stats.win_rate_pct,
                    shadow_position: stats.position,
                    shadow_spikes_detected: stats.spikes_detected,
                    shadow_avg_catchup_pct: stats.avg_catchup_pct,
                    shadow_avg_lag_ms: stats.avg_catchup_lag_ms,
                }
            })
            .collect();

        rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        rows
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
}

impl Default for ScreenerStore {
    fn default() -> Self {
        Self::new(TEN_MINUTES_MS)
    }
}
