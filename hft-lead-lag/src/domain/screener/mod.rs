//! Screener — domain layer for lead-lag metrics, cycle analysis, and shadow trading.
//!
//! # Module structure
//! - `state`          — per-symbol state (quotes, drift, lag)
//! - `cycle_tracker`  — divergence/convergence half-life measurement
//! - `shadow_trader`  — paper-trading spike-follow model with DTOs
//! - `utils`          — percentile math, timestamp normalisation

pub mod cycle_tracker;
pub mod shadow_trader;
pub mod state;
pub mod utils;

use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;

use self::state::{Quote, SymbolState, refresh_ws_drift};
use self::shadow_trader::{ChartData, ShadowDebug};
use self::utils::{now_ms, calculate_ws_drift_ms, normalize_exchange_ts_ms, percentile};

pub use self::shadow_trader::{ShadowStats, ChartTrade};

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
    pub shadow_pnl_per_hour_pct: f64,
    pub shadow_trades: usize,
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
}

impl ScreenerStore {
    pub fn new(window_ms: i64) -> Self {
        Self {
            symbols: Arc::new(DashMap::new()),
            window_ms,
        }
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

        let local_ts_ms = now_ms();
        let exchange_ts_ms = normalize_exchange_ts_ms(timestamp_ns).unwrap_or(local_ts_ms);
        let ingress_local_ts_ms = normalize_exchange_ts_ms(local_receive_ts_ns).unwrap_or(local_ts_ms);

        let mut state = self
            .symbols
            .entry(symbol.to_string())
            .or_insert_with(SymbolState::default);

        let state = state.value_mut();
        let ws_drift = calculate_ws_drift_ms(local_ts_ms, timestamp_ns);
        let ingress_ws_drift = calculate_ws_drift_ms(ingress_local_ts_ms, timestamp_ns);
        let quote = Quote { bid, ask, ts_ms: exchange_ts_ms };

        match exchange {
            "binance" => {
                state.binance = Some(quote);
                if let Some(v) = ws_drift { state.binance_ws_drift_ms = Some(v); }
                if let Some(v) = ingress_ws_drift { state.binance_ingress_ws_drift_ms = Some(v); }
            }
            "gate" => {
                state.gate = Some(quote);
                if let Some(v) = ws_drift { state.gate_ws_drift_ms = Some(v); }
                if let Some(v) = ingress_ws_drift { state.gate_ingress_ws_drift_ms = Some(v); }
            }
            _ => return,
        }
        refresh_ws_drift(state);

        let (Some(binance), Some(gate)) = (state.binance.as_ref(), state.gate.as_ref()) else {
            state.updated_at_ms = exchange_ts_ms;
            state.leader_exchange = exchange;
            state.lag_ms = 0.0;
            return;
        };

        state.updated_at_ms = exchange_ts_ms;
        let instant_lag = (binance.ts_ms - gate.ts_ms).unsigned_abs() as f64;
        state.lag_samples.push_back((exchange_ts_ms, instant_lag));
        while state.lag_samples.front().map_or(false, |(t, _)| exchange_ts_ms - *t > LAG_WINDOW_MS) {
            state.lag_samples.pop_front();
        }
        state.lag_ms = percentile(state.lag_samples.iter().map(|(_, v)| *v), 50.0).unwrap_or(instant_lag);
        state.leader_exchange = if binance.ts_ms >= gate.ts_ms { "binance" } else { "gate" };

        let leader_mid = if binance.ts_ms >= gate.ts_ms {
            (binance.bid + binance.ask) / 2.0
        } else {
            (gate.bid + gate.ask) / 2.0
        }
        .max(1e-12);

        let binance_div_bps = ((binance.bid - gate.ask) / leader_mid) * 10_000.0;
        let binance_conv_bps = ((binance.ask - gate.bid) / leader_mid) * 10_000.0;
        let gate_div_bps = ((gate.bid - binance.ask) / leader_mid) * 10_000.0;
        let gate_conv_bps = ((gate.ask - binance.bid) / leader_mid) * 10_000.0;

        state.binance_leads.update(exchange_ts_ms, binance_div_bps, binance_conv_bps, self.window_ms);
        state.gate_leads.update(exchange_ts_ms, gate_div_bps, gate_conv_bps, self.window_ms);

        let mut means = Vec::with_capacity(2);
        if let Some(v) = state.binance_leads.average_half_life_ms() { means.push(v); }
        if let Some(v) = state.gate_leads.average_half_life_ms() { means.push(v); }
        state.entry_half_life_ms = if means.is_empty() { 0.0 } else { means.iter().sum::<f64>() / means.len() as f64 };

        let mut gt_p90_means = Vec::with_capacity(2);
        if let Some(v) = state.binance_leads.average_above_p90_ms() { gt_p90_means.push(v); }
        if let Some(v) = state.gate_leads.average_above_p90_ms() { gt_p90_means.push(v); }
        state.avg_gt_p90_ms = if gt_p90_means.is_empty() { 0.0 } else { gt_p90_means.iter().sum::<f64>() / gt_p90_means.len() as f64 };

        state.shadow.tick(exchange_ts_ms, binance, gate, self.window_ms);
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
                    ws_drift_ms: item.value().ws_drift_ms,
                    ws_drift_binance_ms: item.value().binance_ws_drift_ms.unwrap_or(0.0),
                    ws_drift_gate_ms: item.value().gate_ws_drift_ms.unwrap_or(0.0),
                    ws_drift_ingress_binance_ms: item.value().binance_ingress_ws_drift_ms.unwrap_or(0.0),
                    ws_drift_ingress_gate_ms: item.value().gate_ingress_ws_drift_ms.unwrap_or(0.0),
                    entry_half_life_ms: item.value().entry_half_life_ms,
                    avg_gt_p90_ms: item.value().avg_gt_p90_ms,
                    gate_natr_30m_pct: 0.0,
                    volume_24h_usd: item.value().volume_24h_usd,
                    shadow_pnl_per_hour_pct: stats.pnl_per_hour_pct,
                    shadow_trades: stats.trades_in_window,
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
        self.symbols.get(symbol).map(|s| s.shadow.debug())
    }

    pub fn chart_data(&self, symbol: &str) -> Option<ChartData> {
        self.symbols.get(symbol).map(|s| s.shadow.chart_data(symbol))
    }

    pub fn symbol_list(&self) -> Vec<String> {
        let mut syms: Vec<String> = self.symbols.iter().map(|r| r.key().clone()).collect();
        syms.sort();
        syms
    }
}

impl Default for ScreenerStore {
    fn default() -> Self {
        Self::new(TEN_MINUTES_MS)
    }
}
