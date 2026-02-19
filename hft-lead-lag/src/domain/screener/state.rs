//! Per-symbol state: quotes, drift, lag samples, cycle trackers, shadow trader.

use std::collections::VecDeque;
use super::cycle_tracker::CycleTracker;
use super::price_samples::PriceSamples;
use super::shadow_fleet::ShadowFleet;
use super::shadow_trader::ShadowTrader;

/// Snapshot of one side of the order book for a single exchange.
/// Only bid/ask prices are stored — quantities are not used in screener logic.
#[derive(Debug, Clone)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub ts_ms: i64,
}

/// Per-exchange WebSocket drift measurements (ms).
#[derive(Debug, Clone, Copy, Default)]
pub struct ExchangeDrifts {
    /// Best (lowest absolute) drift across exchanges.
    pub combined: f64,
    pub binance: Option<f64>,
    pub gate: Option<f64>,
    pub binance_ingress: Option<f64>,
    pub gate_ingress: Option<f64>,
}

impl ExchangeDrifts {
    /// Recompute combined drift from per-exchange values (pick lowest absolute).
    pub fn refresh(&mut self) {
        let mut best: Option<f64> = None;
        if let Some(v) = self.binance {
            best = Some(v);
        }
        if let Some(v) = self.gate {
            best = match best {
                Some(current) if current.abs() <= v.abs() => Some(current),
                _ => Some(v),
            };
        }
        self.combined = best.unwrap_or(0.0);
    }
}

#[derive(Debug, Default)]
pub struct SymbolState {
    pub(crate) binance: Option<Quote>,
    pub(crate) gate: Option<Quote>,
    pub(crate) leader_exchange: &'static str,
    pub(crate) lag_ms: f64,
    pub(crate) lag_samples: VecDeque<(i64, f64)>,
    pub(crate) drifts: ExchangeDrifts,
    pub(crate) entry_half_life_ms: f64,
    pub(crate) avg_gt_p90_ms: f64,
    pub(crate) updated_at_ms: i64,
    pub(crate) volume_24h_usd: f64,
    pub(crate) binance_leads: CycleTracker,
    pub(crate) gate_leads: CycleTracker,
    pub(crate) price_samples: PriceSamples,
    pub(crate) shadow: ShadowTrader,
    pub(crate) fleet: Option<ShadowFleet>,
}

impl SymbolState {
    /// Ingest a quote + drift measurement for one exchange.
    /// Returns `false` if the exchange is unknown (caller should skip).
    pub(crate) fn ingest_quote(
        &mut self,
        exchange: &str,
        quote: Quote,
        ws_drift: Option<f64>,
        ingress_ws_drift: Option<f64>,
    ) -> bool {
        match exchange {
            "binance" => {
                self.binance = Some(quote);
                if let Some(v) = ws_drift { self.drifts.binance = Some(v); }
                if let Some(v) = ingress_ws_drift { self.drifts.binance_ingress = Some(v); }
            }
            "gate" => {
                self.gate = Some(quote);
                if let Some(v) = ws_drift { self.drifts.gate = Some(v); }
                if let Some(v) = ingress_ws_drift { self.drifts.gate_ingress = Some(v); }
            }
            _ => return false,
        }
        self.drifts.refresh();
        true
    }

    /// Compute lag metrics from both quotes. Requires both binance + gate present.
    pub(crate) fn update_lag(
        &mut self,
        exchange_ts_ms: i64,
        lag_window_ms: i64,
    ) {
        let (Some(binance), Some(gate)) = (self.binance.as_ref(), self.gate.as_ref()) else {
            return;
        };
        let instant_lag = (binance.ts_ms - gate.ts_ms).unsigned_abs() as f64;
        self.lag_samples.push_back((exchange_ts_ms, instant_lag));
        while self.lag_samples.front().map_or(false, |(t, _)| exchange_ts_ms - *t > lag_window_ms) {
            self.lag_samples.pop_front();
        }
        self.lag_ms = super::utils::percentile(self.lag_samples.iter().map(|(_, v)| *v), 50.0)
            .unwrap_or(instant_lag);
        self.leader_exchange = if binance.ts_ms >= gate.ts_ms { "binance" } else { "gate" };
    }

    /// Compute cycle-tracker half-life and P90 averages from both quotes.
    pub(crate) fn update_cycles(
        &mut self,
        exchange_ts_ms: i64,
        window_ms: i64,
    ) {
        let (Some(binance), Some(gate)) = (self.binance.as_ref(), self.gate.as_ref()) else {
            return;
        };
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

        self.binance_leads.update(exchange_ts_ms, binance_div_bps, binance_conv_bps, window_ms);
        self.gate_leads.update(exchange_ts_ms, gate_div_bps, gate_conv_bps, window_ms);

        let mut means = Vec::with_capacity(2);
        if let Some(v) = self.binance_leads.average_half_life_ms() { means.push(v); }
        if let Some(v) = self.gate_leads.average_half_life_ms() { means.push(v); }
        self.entry_half_life_ms = if means.is_empty() { 0.0 } else { means.iter().sum::<f64>() / means.len() as f64 };

        let mut gt_p90_means = Vec::with_capacity(2);
        if let Some(v) = self.binance_leads.average_above_p90_ms() { gt_p90_means.push(v); }
        if let Some(v) = self.gate_leads.average_above_p90_ms() { gt_p90_means.push(v); }
        self.avg_gt_p90_ms = if gt_p90_means.is_empty() { 0.0 } else { gt_p90_means.iter().sum::<f64>() / gt_p90_means.len() as f64 };
    }

    /// Push price sample and tick shadow trader.
    pub(crate) fn tick_shadow(
        &mut self,
        exchange_ts_ms: i64,
        window_ms: i64,
    ) {
        let (Some(binance), Some(gate)) = (self.binance.as_ref(), self.gate.as_ref()) else {
            return;
        };
        self.price_samples.push(super::price_samples::PriceSample {
            ts_ms: exchange_ts_ms,
            gate_bid: gate.bid,
            gate_ask: gate.ask,
            binance_bid: binance.bid,
            binance_ask: binance.ask,
        });
        self.price_samples.cleanup(exchange_ts_ms);
        self.shadow.tick(exchange_ts_ms, binance, gate, &self.price_samples, window_ms);
    }
}
