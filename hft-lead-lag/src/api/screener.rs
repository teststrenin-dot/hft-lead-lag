//! Screener state and calculations for lead-lag metrics.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::Serialize;

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;

#[derive(Debug, Clone, Serialize)]
pub struct ScreenerRow {
    pub symbol: String,
    pub leader_exchange: String,
    pub lag_ms: f64,
    pub ws_drift_ms: f64,
    pub entry_half_life_ms: f64,
    pub avg_gt_p90_ms: f64,
    pub gate_natr_30m_pct: f64,
}

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

    pub fn update(
        &self,
        symbol: &str,
        exchange: &'static str,
        bid: f64,
        ask: f64,
        timestamp_ns: i64,
    ) {
        if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
            return;
        }

        let ts_ms = now_ms();

        let mut state = self
            .symbols
            .entry(symbol.to_string())
            .or_insert_with(SymbolState::default);

        let state = state.value_mut();
        let ws_drift = calculate_ws_drift_ms(ts_ms, timestamp_ns);
        let quote = Quote { bid, ask, ts_ms };
        match exchange {
            "binance" => {
                state.binance = Some(quote);
                if let Some(v) = ws_drift {
                    state.binance_ws_drift_ms = Some(v);
                }
            }
            "gate" => {
                state.gate = Some(quote);
                if let Some(v) = ws_drift {
                    state.gate_ws_drift_ms = Some(v);
                }
            }
            _ => return,
        }
        refresh_ws_drift(state);

        let (Some(binance), Some(gate)) = (state.binance.clone(), state.gate.clone()) else {
            state.updated_at_ms = ts_ms;
            state.leader_exchange = exchange.to_string();
            state.lag_ms = 0.0;
            return;
        };

        state.updated_at_ms = ts_ms;
        state.lag_ms = (binance.ts_ms - gate.ts_ms).unsigned_abs() as f64;
        state.leader_exchange = if binance.ts_ms >= gate.ts_ms {
            "binance".to_string()
        } else {
            "gate".to_string()
        };

        let reference_mid = ((binance.bid + binance.ask + gate.bid + gate.ask) / 4.0).max(1e-12);

        let binance_div_bps = ((binance.bid - gate.ask) / reference_mid) * 10_000.0;
        let binance_conv_bps = ((binance.ask - gate.bid) / reference_mid) * 10_000.0;

        let gate_div_bps = ((gate.bid - binance.ask) / reference_mid) * 10_000.0;
        let gate_conv_bps = ((gate.ask - binance.bid) / reference_mid) * 10_000.0;

        state
            .binance_leads
            .update(ts_ms, binance_div_bps, binance_conv_bps, self.window_ms);
        state
            .gate_leads
            .update(ts_ms, gate_div_bps, gate_conv_bps, self.window_ms);

        let mut means = Vec::with_capacity(2);
        if let Some(v) = state.binance_leads.average_half_life_ms() {
            means.push(v);
        }
        if let Some(v) = state.gate_leads.average_half_life_ms() {
            means.push(v);
        }

        state.entry_half_life_ms = if means.is_empty() {
            0.0
        } else {
            means.iter().sum::<f64>() / means.len() as f64
        };

        let mut gt_p90_means = Vec::with_capacity(2);
        if let Some(v) = state.binance_leads.average_above_p90_ms() {
            gt_p90_means.push(v);
        }
        if let Some(v) = state.gate_leads.average_above_p90_ms() {
            gt_p90_means.push(v);
        }
        state.avg_gt_p90_ms = if gt_p90_means.is_empty() {
            0.0
        } else {
            gt_p90_means.iter().sum::<f64>() / gt_p90_means.len() as f64
        };
    }

    pub fn rows_sorted(&self) -> Vec<ScreenerRow> {
        let mut rows: Vec<ScreenerRow> = self
            .symbols
            .iter()
            .filter(|item| !item.value().leader_exchange.is_empty())
            .map(|item| ScreenerRow {
                symbol: item.key().clone(),
                leader_exchange: item.value().leader_exchange.clone(),
                lag_ms: item.value().lag_ms,
                ws_drift_ms: item.value().ws_drift_ms,
                entry_half_life_ms: item.value().entry_half_life_ms,
                avg_gt_p90_ms: item.value().avg_gt_p90_ms,
                gate_natr_30m_pct: 0.0,
            })
            .collect();

        rows.sort_by(|a, b| {
            b.lag_ms
                .partial_cmp(&a.lag_ms)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.symbol.cmp(&b.symbol))
        });
        rows
    }
}

impl Default for ScreenerStore {
    fn default() -> Self {
        Self::new(TEN_MINUTES_MS)
    }
}

#[derive(Debug, Clone)]
struct Quote {
    bid: f64,
    ask: f64,
    ts_ms: i64,
}

#[derive(Debug, Default)]
struct SymbolState {
    binance: Option<Quote>,
    gate: Option<Quote>,
    leader_exchange: String,
    lag_ms: f64,
    ws_drift_ms: f64,
    binance_ws_drift_ms: Option<f64>,
    gate_ws_drift_ms: Option<f64>,
    entry_half_life_ms: f64,
    avg_gt_p90_ms: f64,
    updated_at_ms: i64,
    binance_leads: CycleTracker,
    gate_leads: CycleTracker,
}

#[derive(Debug, Default)]
struct CycleTracker {
    divergence_bps: VecDeque<(i64, f64)>,
    convergence_bps: VecDeque<(i64, f64)>,
    half_life_samples_ms: VecDeque<(i64, f64)>,
    above_p90_samples_ms: VecDeque<(i64, f64)>,
    open_entry_ts_ms: Option<i64>,
    open_above_p90_ts_ms: Option<i64>,
}

impl CycleTracker {
    fn update(&mut self, ts_ms: i64, divergence_bps: f64, convergence_bps: f64, window_ms: i64) {
        self.divergence_bps.push_back((ts_ms, divergence_bps));
        self.convergence_bps.push_back((ts_ms, convergence_bps));
        self.cleanup(ts_ms, window_ms);

        let Some(p90_divergence) = percentile(self.divergence_bps.iter().map(|(_, v)| *v), 90.0) else {
            return;
        };
        let Some(p50_convergence) = percentile(self.convergence_bps.iter().map(|(_, v)| *v), 50.0) else {
            return;
        };

        if divergence_bps >= p90_divergence {
            if self.open_above_p90_ts_ms.is_none() {
                self.open_above_p90_ts_ms = Some(ts_ms);
            }
        } else if let Some(zone_entry_ts) = self.open_above_p90_ts_ms.take() {
            if ts_ms >= zone_entry_ts {
                let zone_duration_ms = (ts_ms - zone_entry_ts).max(0) as f64;
                self.above_p90_samples_ms
                    .push_back((ts_ms, zone_duration_ms));
            }
        }

        if self.open_entry_ts_ms.is_none() && divergence_bps >= p90_divergence {
            self.open_entry_ts_ms = Some(ts_ms);
        }

        if let Some(entry_ts) = self.open_entry_ts_ms {
            if ts_ms >= entry_ts && convergence_bps <= p50_convergence {
                let half_life_ms = (ts_ms - entry_ts).max(0) as f64;
                self.half_life_samples_ms.push_back((ts_ms, half_life_ms));
                self.open_entry_ts_ms = None;
                self.cleanup(ts_ms, window_ms);
            }
        }
    }

    fn average_half_life_ms(&self) -> Option<f64> {
        if self.half_life_samples_ms.is_empty() {
            return None;
        }
        let sum = self
            .half_life_samples_ms
            .iter()
            .map(|(_, v)| *v)
            .sum::<f64>();
        Some(sum / self.half_life_samples_ms.len() as f64)
    }

    fn average_above_p90_ms(&self) -> Option<f64> {
        if self.above_p90_samples_ms.is_empty() {
            return None;
        }
        let sum = self
            .above_p90_samples_ms
            .iter()
            .map(|(_, v)| *v)
            .sum::<f64>();
        Some(sum / self.above_p90_samples_ms.len() as f64)
    }

    fn cleanup(&mut self, ts_ms: i64, window_ms: i64) {
        let cutoff = ts_ms - window_ms;
        while let Some((ts, _)) = self.divergence_bps.front() {
            if *ts >= cutoff {
                break;
            }
            let _ = self.divergence_bps.pop_front();
        }
        while let Some((ts, _)) = self.convergence_bps.front() {
            if *ts >= cutoff {
                break;
            }
            let _ = self.convergence_bps.pop_front();
        }
        while let Some((ts, _)) = self.half_life_samples_ms.front() {
            if *ts >= cutoff {
                break;
            }
            let _ = self.half_life_samples_ms.pop_front();
        }
        while let Some((ts, _)) = self.above_p90_samples_ms.front() {
            if *ts >= cutoff {
                break;
            }
            let _ = self.above_p90_samples_ms.pop_front();
        }
    }
}

fn percentile(values: impl Iterator<Item = f64>, percentile: f64) -> Option<f64> {
    let mut values: Vec<f64> = values.filter(|v| v.is_finite()).collect();
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let rank = (percentile.clamp(0.0, 100.0) / 100.0) * (values.len() - 1) as f64;
    let index = rank.round() as usize;
    values.get(index).copied()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn refresh_ws_drift(state: &mut SymbolState) {
    let mut sum = 0.0;
    let mut count = 0usize;
    if let Some(v) = state.binance_ws_drift_ms {
        sum += v;
        count += 1;
    }
    if let Some(v) = state.gate_ws_drift_ms {
        sum += v;
        count += 1;
    }
    state.ws_drift_ms = if count == 0 { 0.0 } else { sum / count as f64 };
}

fn calculate_ws_drift_ms(local_ts_ms: i64, raw_exchange_ts_ns: i64) -> Option<f64> {
    let exchange_ts_ms = normalize_exchange_ts_ms(raw_exchange_ts_ns)?;
    let drift_ms = local_ts_ms.saturating_sub(exchange_ts_ms) as f64;
    if drift_ms.abs() <= 24.0 * 60.0 * 60.0 * 1000.0 {
        Some(drift_ms)
    } else {
        None
    }
}

fn normalize_exchange_ts_ms(raw_ts_ns: i64) -> Option<i64> {
    if raw_ts_ns <= 0 {
        return None;
    }

    if raw_ts_ns >= 1_000_000_000_000_000_000 {
        return Some(raw_ts_ns / 1_000_000);
    }
    if raw_ts_ns >= 1_000_000_000_000_000 {
        return Some(raw_ts_ns / 1_000);
    }
    if raw_ts_ns >= 1_000_000_000_000 {
        return Some(raw_ts_ns);
    }
    if raw_ts_ns >= 1_000_000_000 {
        return raw_ts_ns.checked_mul(1_000);
    }

    None
}
