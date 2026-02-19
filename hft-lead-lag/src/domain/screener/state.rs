//! Per-symbol state: quotes, drift, lag samples, cycle trackers, shadow trader.

use std::collections::VecDeque;
use super::cycle_tracker::CycleTracker;
use super::shadow_trader::ShadowTrader;

/// Snapshot of one side of the order book for a single exchange.
/// Only bid/ask prices are stored — quantities are not used in screener logic.
#[derive(Debug, Clone)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Default)]
pub struct SymbolState {
    pub binance: Option<Quote>,
    pub gate: Option<Quote>,
    pub leader_exchange: &'static str,
    pub lag_ms: f64,
    pub lag_samples: VecDeque<(i64, f64)>,
    pub ws_drift_ms: f64,
    pub binance_ws_drift_ms: Option<f64>,
    pub gate_ws_drift_ms: Option<f64>,
    pub binance_ingress_ws_drift_ms: Option<f64>,
    pub gate_ingress_ws_drift_ms: Option<f64>,
    pub entry_half_life_ms: f64,
    pub avg_gt_p90_ms: f64,
    pub updated_at_ms: i64,
    pub volume_24h_usd: f64,
    pub binance_leads: CycleTracker,
    pub gate_leads: CycleTracker,
    pub shadow: ShadowTrader,
}

/// Recompute combined ws_drift from per-exchange values (pick lowest absolute).
pub fn refresh_ws_drift(state: &mut SymbolState) {
    let mut best: Option<f64> = None;
    if let Some(v) = state.binance_ws_drift_ms {
        best = Some(v);
    }
    if let Some(v) = state.gate_ws_drift_ms {
        best = match best {
            Some(current) if current.abs() <= v.abs() => Some(current),
            _ => Some(v),
        };
    }
    state.ws_drift_ms = best.unwrap_or(0.0);
}
