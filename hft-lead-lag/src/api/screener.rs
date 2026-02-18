//! Screener state and calculations for lead-lag metrics.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::Serialize;

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;
const LAG_WINDOW_MS: i64 = 5 * 60 * 1000;

/// Assumed notional per leg in USD for market impact estimation.
const ASSUMED_NOTIONAL_USD: f64 = 1_000.0;
/// Gate taker fee (fraction, not percent).
const GATE_TAKER_FEE: f64 = 0.000_5; // 0.05 %
/// Simulated order-to-fill latency in milliseconds.
const EXECUTION_DELAY_MS: i64 = 10;
/// Minimum expected edge (bps) to enter — must exceed round-trip fees.
const MIN_EDGE_BPS: f64 = 10.0; // 2 × 0.05% = 10 bps

#[derive(Debug, Clone, Serialize)]
pub struct ScreenerRow {
    pub symbol: String,
    pub leader_exchange: String,
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
    // Shadow trader fields
    pub shadow_pnl_per_hour_pct: f64,
    pub shadow_trades: usize,
    pub shadow_avg_trade_pct: f64,
    pub shadow_win_rate_pct: f64,
    pub shadow_position: String,
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

    /// Set 24h volume for symbols (called once at startup from REST data).
    pub fn set_volumes(&self, volumes: &[(String, f64)]) {
        for (sym, vol) in volumes {
            self.symbols.entry(sym.clone()).or_default().volume_24h_usd = *vol;
        }
    }

    pub fn update(
        &self,
        symbol: &str,
        exchange: &'static str,
        bid: f64,
        ask: f64,
        bid_qty: f64,
        ask_qty: f64,
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
        let quote = Quote {
            bid,
            ask,
            bid_qty,
            ask_qty,
            ts_ms: exchange_ts_ms,
        };
        match exchange {
            "binance" => {
                state.binance = Some(quote);
                if let Some(v) = ws_drift {
                    state.binance_ws_drift_ms = Some(v);
                }
                if let Some(v) = ingress_ws_drift {
                    state.binance_ingress_ws_drift_ms = Some(v);
                }
            }
            "gate" => {
                state.gate = Some(quote);
                if let Some(v) = ws_drift {
                    state.gate_ws_drift_ms = Some(v);
                }
                if let Some(v) = ingress_ws_drift {
                    state.gate_ingress_ws_drift_ms = Some(v);
                }
            }
            _ => return,
        }
        refresh_ws_drift(state);

        let (Some(binance), Some(gate)) = (state.binance.clone(), state.gate.clone()) else {
            state.updated_at_ms = exchange_ts_ms;
            state.leader_exchange = exchange.to_string();
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
        state.leader_exchange = if binance.ts_ms >= gate.ts_ms {
            "binance".to_string()
        } else {
            "gate".to_string()
        };

        // Use the leading exchange's mid as per-coin normalizer (no cross-exchange blending).
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

        state
            .binance_leads
            .update(exchange_ts_ms, binance_div_bps, binance_conv_bps, self.window_ms);
        state
            .gate_leads
            .update(exchange_ts_ms, gate_div_bps, gate_conv_bps, self.window_ms);

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

        // Shadow trader: feed both quotes
        state.shadow.tick(exchange_ts_ms, &binance, &gate, self.window_ms);
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
                    leader_exchange: item.value().leader_exchange.clone(),
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
                    shadow_position: stats.position.clone(),
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

#[derive(Debug, Clone)]
struct Quote {
    bid: f64,
    ask: f64,
    bid_qty: f64,
    ask_qty: f64,
    ts_ms: i64,
}

#[derive(Debug, Default)]
struct SymbolState {
    binance: Option<Quote>,
    gate: Option<Quote>,
    leader_exchange: String,
    lag_ms: f64,
    lag_samples: VecDeque<(i64, f64)>,
    ws_drift_ms: f64,
    binance_ws_drift_ms: Option<f64>,
    gate_ws_drift_ms: Option<f64>,
    binance_ingress_ws_drift_ms: Option<f64>,
    gate_ingress_ws_drift_ms: Option<f64>,
    entry_half_life_ms: f64,
    avg_gt_p90_ms: f64,
    updated_at_ms: i64,
    volume_24h_usd: f64,
    binance_leads: CycleTracker,
    gate_leads: CycleTracker,
    shadow: ShadowTrader,
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

/// Post-trade cooldown to prevent overtrading (ms).
const COOLDOWN_MS: i64 = 5_000;
/// Warmup duration: shadow trader ignores data until enough history (ms).
const WARMUP_MS: i64 = 120_000; // 2 minutes
/// Maximum age of a quote to be considered "fresh" (ms).
const QUOTE_FRESHNESS_MS: i64 = 1_000;

// ---------------------------------------------------------------------------
// Shadow Trader — paper-trades ONLY on Gate, uses Binance as signal source.
//
// Model: Binance leads, Gate lags. When Gate premium vs Binance reaches P90
// (extreme divergence), open a position on Gate. Exit when premium reverts
// to P50. Execution uses Gate bid/ask + market impact from Gate L1 depth.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum ShadowDirection {
    /// Gate overpriced vs Binance → sell Gate
    Short,
    /// Gate underpriced vs Binance → buy Gate
    Long,
}

#[derive(Debug, Clone)]
struct ShadowPosition {
    direction: ShadowDirection,
    gate_entry_price: f64,
    entry_ts_ms: i64,
    entry_premium_bps: f64,
}

#[derive(Debug, Clone)]
struct ShadowSignal {
    direction: ShadowDirection,
    fire_ts_ms: i64,
}

#[derive(Debug, Clone)]
struct ShadowTrade {
    pnl_pct: f64,
    ts_ms: i64,
    direction: ShadowDirection,
    entry_ts_ms: i64,
    entry_premium_bps: f64,
    exit_premium_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChartTrade {
    pub entry_ts_ms: i64,
    pub exit_ts_ms: i64,
    pub direction: String,
    pub pnl_pct: f64,
    pub entry_premium_bps: f64,
    pub exit_premium_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChartData {
    pub symbol: String,
    pub ts: Vec<f64>,
    pub premium_bps: Vec<f64>,
    pub p90: Option<f64>,
    pub p10: Option<f64>,
    pub p50: Option<f64>,
    pub trades: Vec<ChartTrade>,
    pub position: String,
    pub entry_price: Option<f64>,
    pub entry_ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowStats {
    pub pnl_per_hour_pct: f64,
    pub trades_in_window: usize,
    pub avg_trade_pct: f64,
    pub win_rate_pct: f64,
    pub position: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowDebug {
    pub premium_samples: usize,
    pub last_premium_bps: f64,
    pub cached_p90: Option<f64>,
    pub cached_p10: Option<f64>,
    pub cached_p50: Option<f64>,
    pub completed_trades_in_window: usize,
    pub cooldown_remaining_ms: i64,
    pub warmup_remaining_ms: i64,
    pub position: String,
    pub entry_price: Option<f64>,
    pub last_5_trades_pnl_pct: Vec<f64>,
    pub short_edge_bps: f64,
    pub long_edge_bps: f64,
    pub min_edge_required_bps: f64,
}

/// Interval for recalculating P90/P10/P50 thresholds (ms).
const THRESHOLD_INTERVAL_MS: i64 = 60_000; // 1 minute

#[derive(Debug, Default)]
struct ShadowTrader {
    /// Rolling gate premium: (gate.mid − binance.mid) / binance.mid × 10000 bps
    premium_bps: VecDeque<(i64, f64)>,
    position: Option<ShadowPosition>,
    pending_signal: Option<ShadowSignal>,
    completed_trades: VecDeque<ShadowTrade>,
    start_ts_ms: Option<i64>,
    latest_ts_ms: i64,
    cooldown_until_ms: i64,
    prev_premium_bps: f64,
    // Frozen thresholds — recalculated once per THRESHOLD_INTERVAL_MS
    cached_p90: Option<f64>,
    cached_p10: Option<f64>,
    cached_p50: Option<f64>,
    thresholds_updated_at_ms: i64,
}

impl ShadowTrader {
    fn tick(&mut self, ts_ms: i64, binance: &Quote, gate: &Quote, window_ms: i64) {
        if self.start_ts_ms.is_none() {
            self.start_ts_ms = Some(ts_ms);
        }
        self.latest_ts_ms = ts_ms;

        // Both quotes must be fresh
        if (ts_ms - binance.ts_ms).unsigned_abs() > QUOTE_FRESHNESS_MS as u64
            || (ts_ms - gate.ts_ms).unsigned_abs() > QUOTE_FRESHNESS_MS as u64
        {
            return;
        }

        let bn_mid = ((binance.bid + binance.ask) / 2.0).max(1e-12);
        let gt_mid = (gate.bid + gate.ask) / 2.0;
        let premium_bps = ((gt_mid - bn_mid) / bn_mid) * 10_000.0;

        self.cleanup(ts_ms, window_ms);

        // 2-minute warmup
        let elapsed = ts_ms.saturating_sub(self.start_ts_ms.unwrap_or(ts_ms));
        if elapsed < WARMUP_MS {
            self.premium_bps.push_back((ts_ms, premium_bps));
            self.prev_premium_bps = premium_bps;
            return;
        }

        // Recalculate frozen thresholds once per minute (before adding current sample)
        if ts_ms >= self.thresholds_updated_at_ms + THRESHOLD_INTERVAL_MS {
            self.cached_p90 = percentile(self.premium_bps.iter().map(|(_, v)| *v), 90.0);
            self.cached_p10 = percentile(self.premium_bps.iter().map(|(_, v)| *v), 10.0);
            self.cached_p50 = percentile(self.premium_bps.iter().map(|(_, v)| *v), 50.0);
            self.thresholds_updated_at_ms = ts_ms;
        }

        self.premium_bps.push_back((ts_ms, premium_bps));

        // Execute pending signal after delay — revalidate edge before filling
        let mut just_entered = false;
        if let Some(ref sig) = self.pending_signal {
            if ts_ms >= sig.fire_ts_ms + EXECUTION_DELAY_MS {
                let dir = sig.direction;
                let still_valid = match (dir, self.cached_p90, self.cached_p10, self.cached_p50) {
                    (ShadowDirection::Short, Some(p90), _, Some(p50)) => {
                        premium_bps >= p90 && (p90 - p50).abs() >= MIN_EDGE_BPS
                    }
                    (ShadowDirection::Long, _, Some(p10), Some(p50)) => {
                        premium_bps <= p10 && (p50 - p10).abs() >= MIN_EDGE_BPS
                    }
                    _ => false,
                };
                self.pending_signal = None;
                if still_valid {
                    self.execute_entry(ts_ms, dir, gate, premium_bps);
                    just_entered = true;
                }
            }
        }

        let (p90, p10, p50) = match (self.cached_p90, self.cached_p10, self.cached_p50) {
            (Some(a), Some(b), Some(c)) => (a, b, c),
            _ => {
                self.prev_premium_bps = premium_bps;
                return;
            }
        };

        // Exit: premium reverted to frozen P50 (skip on the tick we just entered)
        if !just_entered {
            if let Some(ref pos) = self.position {
                let should_exit = match pos.direction {
                    ShadowDirection::Short => premium_bps <= p50,
                    ShadowDirection::Long => premium_bps >= p50,
                };
                if should_exit {
                    self.execute_exit(ts_ms, gate, window_ms, premium_bps);
                }
            }
        }

        // Entry: flat, no pending, cooldown elapsed, frozen P90/P10 crossing
        // Only enter if expected edge (|threshold - P50|) exceeds round-trip fees
        let short_edge = (p90 - p50).abs();
        let long_edge = (p50 - p10).abs();

        if self.position.is_none()
            && self.pending_signal.is_none()
            && ts_ms >= self.cooldown_until_ms
        {
            let short_cross = short_edge >= MIN_EDGE_BPS
                && premium_bps >= p90
                && self.prev_premium_bps < p90;
            let long_cross = long_edge >= MIN_EDGE_BPS
                && premium_bps <= p10
                && self.prev_premium_bps > p10;

            let direction = match (short_cross, long_cross) {
                (true, true) => {
                    if (premium_bps - p90).abs() >= (premium_bps - p10).abs() {
                        Some(ShadowDirection::Short)
                    } else {
                        Some(ShadowDirection::Long)
                    }
                }
                (true, false) => Some(ShadowDirection::Short),
                (false, true) => Some(ShadowDirection::Long),
                _ => None,
            };

            if let Some(dir) = direction {
                self.pending_signal = Some(ShadowSignal {
                    direction: dir,
                    fire_ts_ms: ts_ms,
                });
            }
        }

        self.prev_premium_bps = premium_bps;
    }

    fn execute_entry(&mut self, ts_ms: i64, direction: ShadowDirection, gate: &Quote, premium_bps: f64) {
        let gate_price = match direction {
            ShadowDirection::Short => apply_impact(gate.bid, gate.bid_qty, gate.bid, true),
            ShadowDirection::Long => apply_impact(gate.ask, gate.ask_qty, gate.ask, false),
        };
        self.position = Some(ShadowPosition {
            direction,
            gate_entry_price: gate_price,
            entry_ts_ms: ts_ms,
            entry_premium_bps: premium_bps,
        });
    }

    fn execute_exit(&mut self, ts_ms: i64, gate: &Quote, window_ms: i64, premium_bps: f64) {
        let pos = match self.position.take() {
            Some(p) => p,
            None => return,
        };

        let fees = GATE_TAKER_FEE * 2.0; // entry + exit
        let pnl_pct = match pos.direction {
            ShadowDirection::Short => {
                // Sold at entry, buy back now
                let exit_price = apply_impact(gate.ask, gate.ask_qty, gate.ask, false);
                ((pos.gate_entry_price - exit_price) / pos.gate_entry_price - fees) * 100.0
            }
            ShadowDirection::Long => {
                // Bought at entry, sell now
                let exit_price = apply_impact(gate.bid, gate.bid_qty, gate.bid, true);
                ((exit_price - pos.gate_entry_price) / pos.gate_entry_price - fees) * 100.0
            }
        };

        self.completed_trades.push_back(ShadowTrade {
            pnl_pct,
            ts_ms,
            direction: pos.direction,
            entry_ts_ms: pos.entry_ts_ms,
            entry_premium_bps: pos.entry_premium_bps,
            exit_premium_bps: premium_bps,
        });
        self.cooldown_until_ms = ts_ms + COOLDOWN_MS;
        let cutoff = ts_ms - window_ms;
        while let Some(t) = self.completed_trades.front() {
            if t.ts_ms >= cutoff { break; }
            self.completed_trades.pop_front();
        }
    }

    fn stats(&self) -> ShadowStats {
        let n = self.completed_trades.len();

        if n == 0 {
            return ShadowStats {
                pnl_per_hour_pct: 0.0,
                trades_in_window: 0,
                avg_trade_pct: 0.0,
                win_rate_pct: 0.0,
                position: self.position_label(),
            };
        }

        // Observation period = time since warmup ended, capped to window_ms.
        let obs_ms = self.start_ts_ms
            .map(|s| {
                let post_warmup = s + WARMUP_MS;
                (self.latest_ts_ms - post_warmup).max(1).min(TEN_MINUTES_MS) as f64
            })
            .unwrap_or(COOLDOWN_MS as f64);
        let window_hours = obs_ms / 3_600_000.0;

        let total_pnl: f64 = self.completed_trades.iter().map(|t| t.pnl_pct).sum();
        let wins = self.completed_trades.iter().filter(|t| t.pnl_pct > 0.0).count();

        ShadowStats {
            pnl_per_hour_pct: total_pnl / window_hours,
            trades_in_window: n,
            avg_trade_pct: total_pnl / n as f64,
            win_rate_pct: (wins as f64 / n as f64) * 100.0,
            position: self.position_label(),
        }
    }

    fn position_label(&self) -> String {
        match &self.position {
            None if self.pending_signal.is_some() => "PENDING".to_string(),
            None => "FLAT".to_string(),
            Some(p) => match p.direction {
                ShadowDirection::Short => "SHORT_GT".to_string(),
                ShadowDirection::Long => "LONG_GT".to_string(),
            },
        }
    }

    fn cleanup(&mut self, ts_ms: i64, window_ms: i64) {
        let cutoff = ts_ms - window_ms;
        while let Some((ts, _)) = self.premium_bps.front() {
            if *ts >= cutoff { break; }
            self.premium_bps.pop_front();
        }
    }

    fn debug(&self) -> ShadowDebug {
        let elapsed = self.start_ts_ms
            .map(|s| self.latest_ts_ms.saturating_sub(s))
            .unwrap_or(0);
        let warmup_remaining = (WARMUP_MS - elapsed).max(0);
        let cooldown_remaining = (self.cooldown_until_ms - self.latest_ts_ms).max(0);
        let last_5: Vec<f64> = self.completed_trades.iter()
            .rev().take(5).map(|t| t.pnl_pct).collect();
        let (short_edge, long_edge) = match (self.cached_p90, self.cached_p10, self.cached_p50) {
            (Some(p90), Some(p10), Some(p50)) => ((p90 - p50).abs(), (p50 - p10).abs()),
            _ => (0.0, 0.0),
        };
        ShadowDebug {
            premium_samples: self.premium_bps.len(),
            last_premium_bps: self.prev_premium_bps,
            cached_p90: self.cached_p90,
            cached_p10: self.cached_p10,
            cached_p50: self.cached_p50,
            completed_trades_in_window: self.completed_trades.len(),
            cooldown_remaining_ms: cooldown_remaining,
            warmup_remaining_ms: warmup_remaining,
            position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            last_5_trades_pnl_pct: last_5,
            short_edge_bps: short_edge,
            long_edge_bps: long_edge,
            min_edge_required_bps: MIN_EDGE_BPS,
        }
    }

    fn chart_data(&self, symbol: &str) -> ChartData {
        // Downsample premium_bps to max ~600 points (1 per second for 10 min)
        let len = self.premium_bps.len();
        let step = (len / 600).max(1);
        let mut ts = Vec::with_capacity(len / step + 1);
        let mut vals = Vec::with_capacity(len / step + 1);
        for (i, (t, v)) in self.premium_bps.iter().enumerate() {
            if i % step == 0 {
                ts.push(*t as f64 / 1000.0); // seconds for uPlot
                vals.push(*v);
            }
        }
        let trades: Vec<ChartTrade> = self.completed_trades.iter().map(|t| ChartTrade {
            entry_ts_ms: t.entry_ts_ms,
            exit_ts_ms: t.ts_ms,
            direction: match t.direction {
                ShadowDirection::Short => "SHORT".to_string(),
                ShadowDirection::Long => "LONG".to_string(),
            },
            pnl_pct: t.pnl_pct,
            entry_premium_bps: t.entry_premium_bps,
            exit_premium_bps: t.exit_premium_bps,
        }).collect();

        ChartData {
            symbol: symbol.to_string(),
            ts,
            premium_bps: vals,
            p90: self.cached_p90,
            p10: self.cached_p10,
            p50: self.cached_p50,
            trades,
            position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            entry_ts_ms: self.position.as_ref().map(|p| p.entry_ts_ms),
        }
    }
}

/// Market impact from L1 depth overflow (Gate only).
/// Impact is capped: overflow ratio beyond MAX_OVERFLOW is not executed.
const MAX_OVERFLOW_RATIO: f64 = 5.0;

fn apply_impact(price: f64, qty: f64, ref_price: f64, is_sell: bool) -> f64 {
    if qty <= 0.0 || ref_price <= 0.0 {
        return price;
    }
    let order_qty = ASSUMED_NOTIONAL_USD / ref_price;
    if order_qty <= qty {
        return price;
    }
    let overflow_ratio = (order_qty / qty).min(MAX_OVERFLOW_RATIO);
    let impact = price * (overflow_ratio - 1.0) * 0.001;
    if is_sell { price - impact } else { price + impact }
}

fn percentile(values: impl Iterator<Item = f64>, pct: f64) -> Option<f64> {
    let mut values: Vec<f64> = values.filter(|v| v.is_finite()).collect();
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let rank = (pct.clamp(0.0, 100.0) / 100.0) * (values.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        values.get(lo).copied()
    } else {
        let frac = rank - lo as f64;
        Some(values[lo] * (1.0 - frac) + values[hi] * frac)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn refresh_ws_drift(state: &mut SymbolState) {
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

fn calculate_ws_drift_ms(local_ts_ms: i64, raw_exchange_ts_ns: i64) -> Option<f64> {
    let exchange_ts_ms = normalize_exchange_ts_ms(raw_exchange_ts_ns)?;
    let drift_ms = local_ts_ms.saturating_sub(exchange_ts_ms) as f64;
    if drift_ms.abs() <= 30_000.0 {
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
