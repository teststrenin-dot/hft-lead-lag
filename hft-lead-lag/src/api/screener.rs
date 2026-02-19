//! Screener state and calculations for lead-lag metrics.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::Serialize;

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;
const LAG_WINDOW_MS: i64 = 5 * 60 * 1000;

/// Gate taker fee (fraction, not percent).
const GATE_TAKER_FEE: f64 = 0.000_5; // 0.05 %
/// Simulated order-to-fill latency in milliseconds.
const FILL_DELAY_MS: i64 = 7;

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
    pub shadow_spikes_detected: usize,
    pub shadow_avg_catchup_pct: f64,
    pub shadow_avg_lag_ms: f64,
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
const COOLDOWN_MS: i64 = 3_000;
/// Warmup duration: shadow trader ignores data until enough history (ms).
const WARMUP_MS: i64 = 30_000; // 30 seconds — just need a few seconds of baseline
/// Maximum age of a quote to be considered "fresh" (ms).
const QUOTE_FRESHNESS_MS: i64 = 1_000;

// ---------------------------------------------------------------------------
// Shadow Trader — spike-follow model.
//
// Strategy: Binance leads, Gate lags. When Binance mid spikes ≥ SPIKE_THRESHOLD
// in a short window, enter on Gate in the same direction. Exit when Gate catches
// up (target profit reached), or on timeout / stop-loss.
// All execution is paper-traded on Gate bid/ask with 7ms fill delay.
// ---------------------------------------------------------------------------

/// Minimum Binance mid move (bps) to trigger entry.
const SPIKE_THRESHOLD_BPS: f64 = 30.0; // 0.30%
/// Time window to measure Binance spike (ms).
const SPIKE_WINDOW_MS: i64 = 500;
/// Maximum time to hold a position waiting for Gate catchup (ms).
const MAX_HOLD_MS: i64 = 30_000; // 30 seconds
/// Stop-loss threshold (bps) — cut if position goes against us.
const STOP_LOSS_BPS: f64 = 20.0; // 0.20%

#[derive(Debug, Clone, Copy, PartialEq)]
enum ShadowDirection {
    Short,
    Long,
}

#[derive(Debug, Clone)]
struct ShadowPosition {
    direction: ShadowDirection,
    gate_entry_price: f64,
    entry_ts_ms: i64,
    /// Binance mid at entry — used to measure catchup
    binance_mid_at_entry: f64,
    spike_bps: f64,
}

#[derive(Debug, Clone)]
struct ShadowTrade {
    pnl_pct: f64,
    ts_ms: i64,
    direction: ShadowDirection,
    entry_ts_ms: i64,
    entry_price: f64,
    exit_price: f64,
    exit_reason: &'static str,
    spike_bps: f64,
    catchup_pct: f64,
    catchup_ms: i64,
}

#[derive(Debug, Clone)]
struct SpikeEvent {
    ts_ms: i64,
    direction: ShadowDirection,
    spike_bps: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChartTrade {
    pub entry_ts_ms: i64,
    pub exit_ts_ms: i64,
    pub direction: String,
    pub pnl_pct: f64,
    pub exit_reason: String,
    pub spike_bps: f64,
    pub catchup_pct: f64,
    pub entry_price: f64,
    pub exit_price: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChartData {
    pub symbol: String,
    pub ts: Vec<f64>,
    pub gate_bid: Vec<f64>,
    pub gate_ask: Vec<f64>,
    pub binance_bid: Vec<f64>,
    pub binance_ask: Vec<f64>,
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
    pub spikes_detected: usize,
    pub avg_catchup_pct: f64,
    pub avg_catchup_lag_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowDebug {
    pub samples: usize,
    pub last_binance_bid: f64,
    pub last_binance_ask: f64,
    pub last_gate_bid: f64,
    pub last_gate_ask: f64,
    pub completed_trades_in_window: usize,
    pub cooldown_remaining_ms: i64,
    pub warmup_remaining_ms: i64,
    pub position: String,
    pub entry_price: Option<f64>,
    pub last_5_trades_pnl_pct: Vec<f64>,
    pub spike_threshold_bps: f64,
    pub spikes_in_window: usize,
    pub max_hold_ms: i64,
    pub stop_loss_bps: f64,
}

#[derive(Debug, Clone)]
struct MidSample {
    ts_ms: i64,
    gate_bid: f64,
    gate_ask: f64,
    binance_bid: f64,
    binance_ask: f64,
}

impl MidSample {
    fn binance_mid(&self) -> f64 { (self.binance_bid + self.binance_ask) / 2.0 }
    fn gate_mid(&self) -> f64 { (self.gate_bid + self.gate_ask) / 2.0 }
}

#[derive(Debug, Clone)]
struct PendingOrder {
    direction: ShadowDirection,
    fire_ts_ms: i64,
    is_exit: bool,
    /// Only for exit: snapshot of position at exit request time
    exit_pos: Option<ShadowPosition>,
    spike_bps: f64,
    exit_reason: &'static str,
}

#[derive(Debug, Default)]
struct ShadowTrader {
    /// Rolling mid-price samples for spike detection and charting
    mid_samples: VecDeque<MidSample>,
    position: Option<ShadowPosition>,
    pending: Option<PendingOrder>,
    completed_trades: VecDeque<ShadowTrade>,
    spike_history: VecDeque<SpikeEvent>,
    start_ts_ms: Option<i64>,
    latest_ts_ms: i64,
    cooldown_until_ms: i64,
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

        self.mid_samples.push_back(MidSample {
            ts_ms,
            gate_bid: gate.bid, gate_ask: gate.ask,
            binance_bid: binance.bid, binance_ask: binance.ask,
        });
        self.cleanup(ts_ms, window_ms);

        // Warmup
        let elapsed = ts_ms.saturating_sub(self.start_ts_ms.unwrap_or(ts_ms));
        if elapsed < WARMUP_MS {
            return;
        }

        // --- Execute pending orders after FILL_DELAY_MS ---
        if let Some(pending) = self.pending.take() {
            if ts_ms >= pending.fire_ts_ms + FILL_DELAY_MS {
                if pending.is_exit {
                    // Fill exit at current Gate bid/ask
                    if let Some(pos) = pending.exit_pos {
                        self.fill_exit(ts_ms, gate, window_ms, pos, pending.exit_reason);
                    }
                } else {
                    // Fill entry at current Gate bid/ask
                    let gate_price = match pending.direction {
                        ShadowDirection::Long => gate.ask,
                        ShadowDirection::Short => gate.bid,
                    };
                    self.position = Some(ShadowPosition {
                        direction: pending.direction,
                        gate_entry_price: gate_price,
                        entry_ts_ms: ts_ms,
                        binance_mid_at_entry: bn_mid,
                        spike_bps: pending.spike_bps,
                    });
                }
            } else {
                self.pending = Some(pending); // put it back, not yet time
            }
            return; // while pending, don't do anything else
        }

        // --- Exit logic (check before entry) ---
        if let Some(pos) = self.position.as_ref() {
            let hold_ms = ts_ms - pos.entry_ts_ms;

            let unrealized_bps = match pos.direction {
                ShadowDirection::Long => {
                    ((gate.bid - pos.gate_entry_price) / pos.gate_entry_price) * 10_000.0
                }
                ShadowDirection::Short => {
                    ((pos.gate_entry_price - gate.ask) / pos.gate_entry_price) * 10_000.0
                }
            };

            // Gate caught up: profitable exit
            let gate_moved_enough = unrealized_bps >= SPIKE_THRESHOLD_BPS;
            // Timeout
            let timed_out = hold_ms >= MAX_HOLD_MS;
            // Stop-loss
            let stopped_out = unrealized_bps <= -STOP_LOSS_BPS;

            if gate_moved_enough || timed_out || stopped_out {
                let reason = if gate_moved_enough { "target" }
                    else if stopped_out { "stop_loss" }
                    else { "timeout" };
                // Queue exit with 7ms delay
                let pos = self.position.take().unwrap();
                self.pending = Some(PendingOrder {
                    direction: pos.direction,
                    fire_ts_ms: ts_ms,
                    is_exit: true,
                    exit_pos: Some(pos),
                    spike_bps: 0.0,
                    exit_reason: reason,
                });
            }
        }

        // --- Spike detection & entry ---
        if self.position.is_none() && self.pending.is_none() && ts_ms >= self.cooldown_until_ms {
            if let Some((direction, spike_bps)) = self.detect_spike(ts_ms, bn_mid) {
                // Record spike
                self.spike_history.push_back(SpikeEvent {
                    ts_ms,
                    direction,
                    spike_bps,
                });

                // Queue entry with 7ms delay
                self.pending = Some(PendingOrder {
                    direction,
                    fire_ts_ms: ts_ms,
                    is_exit: false,
                    exit_pos: None,
                    spike_bps,
                    exit_reason: "",
                });
            }
        }
    }

    /// Detect if Binance mid spiked ≥ threshold within SPIKE_WINDOW_MS.
    fn detect_spike(&self, now_ms: i64, current_mid: f64) -> Option<(ShadowDirection, f64)> {
        let cutoff = now_ms - SPIKE_WINDOW_MS;
        // Find the oldest sample within spike window
        let baseline = self.mid_samples.iter()
            .find(|s| s.ts_ms >= cutoff)?;

        let baseline_mid = baseline.binance_mid();
        if baseline_mid <= 0.0 {
            return None;
        }

        let move_bps = ((current_mid - baseline_mid) / baseline_mid) * 10_000.0;

        if move_bps >= SPIKE_THRESHOLD_BPS {
            Some((ShadowDirection::Long, move_bps))
        } else if move_bps <= -SPIKE_THRESHOLD_BPS {
            Some((ShadowDirection::Short, move_bps.abs()))
        } else {
            None
        }
    }

    fn fill_exit(
        &mut self,
        ts_ms: i64,
        gate: &Quote,
        window_ms: i64,
        pos: ShadowPosition,
        exit_reason: &'static str,
    ) {
        let fees = GATE_TAKER_FEE * 2.0; // entry + exit

        let (pnl_pct, catchup_pct, exit_price) = match pos.direction {
            ShadowDirection::Long => {
                let exit_price = gate.bid;
                let raw_pnl = (exit_price - pos.gate_entry_price) / pos.gate_entry_price;
                let catchup = raw_pnl * 100.0;
                ((raw_pnl - fees) * 100.0, catchup, exit_price)
            }
            ShadowDirection::Short => {
                let exit_price = gate.ask;
                let raw_pnl = (pos.gate_entry_price - exit_price) / pos.gate_entry_price;
                let catchup = raw_pnl * 100.0;
                ((raw_pnl - fees) * 100.0, catchup, exit_price)
            }
        };

        let catchup_ms = ts_ms - pos.entry_ts_ms;

        self.completed_trades.push_back(ShadowTrade {
            pnl_pct,
            ts_ms,
            direction: pos.direction,
            entry_ts_ms: pos.entry_ts_ms,
            entry_price: pos.gate_entry_price,
            exit_price,
            exit_reason,
            spike_bps: pos.spike_bps,
            catchup_pct,
            catchup_ms,
        });
        self.cooldown_until_ms = ts_ms + COOLDOWN_MS;

        // Trim old trades
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
                spikes_detected: self.spike_history.len(),
                avg_catchup_pct: 0.0,
                avg_catchup_lag_ms: 0.0,
            };
        }

        let obs_ms = self.start_ts_ms
            .map(|s| {
                let post_warmup = s + WARMUP_MS;
                (self.latest_ts_ms - post_warmup).clamp(1, TEN_MINUTES_MS) as f64
            })
            .unwrap_or(1.0);
        let window_hours = obs_ms / 3_600_000.0;

        let total_pnl: f64 = self.completed_trades.iter().map(|t| t.pnl_pct).sum();
        let wins = self.completed_trades.iter().filter(|t| t.pnl_pct > 0.0).count();
        let avg_catchup: f64 = self.completed_trades.iter().map(|t| t.catchup_pct).sum::<f64>() / n as f64;
        let avg_lag: f64 = self.completed_trades.iter().map(|t| t.catchup_ms as f64).sum::<f64>() / n as f64;

        ShadowStats {
            pnl_per_hour_pct: total_pnl / window_hours,
            trades_in_window: n,
            avg_trade_pct: total_pnl / n as f64,
            win_rate_pct: (wins as f64 / n as f64) * 100.0,
            position: self.position_label(),
            spikes_detected: self.spike_history.len(),
            avg_catchup_pct: avg_catchup,
            avg_catchup_lag_ms: avg_lag,
        }
    }

    fn position_label(&self) -> String {
        if self.pending.is_some() {
            return "PENDING".to_string();
        }
        match &self.position {
            None => "FLAT".to_string(),
            Some(p) => match p.direction {
                ShadowDirection::Short => "SHORT_GT".to_string(),
                ShadowDirection::Long => "LONG_GT".to_string(),
            },
        }
    }

    fn cleanup(&mut self, ts_ms: i64, window_ms: i64) {
        let cutoff = ts_ms - window_ms;
        while let Some(s) = self.mid_samples.front() {
            if s.ts_ms >= cutoff { break; }
            self.mid_samples.pop_front();
        }
        while let Some(s) = self.spike_history.front() {
            if s.ts_ms >= cutoff { break; }
            self.spike_history.pop_front();
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
        let last_bn = self.mid_samples.back();
        let last_gt = self.mid_samples.back();

        ShadowDebug {
            samples: self.mid_samples.len(),
            last_binance_bid: last_bn.map(|s| s.binance_bid).unwrap_or(0.0),
            last_binance_ask: last_bn.map(|s| s.binance_ask).unwrap_or(0.0),
            last_gate_bid: last_gt.map(|s| s.gate_bid).unwrap_or(0.0),
            last_gate_ask: last_gt.map(|s| s.gate_ask).unwrap_or(0.0),
            completed_trades_in_window: self.completed_trades.len(),
            cooldown_remaining_ms: cooldown_remaining,
            warmup_remaining_ms: warmup_remaining,
            position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            last_5_trades_pnl_pct: last_5,
            spike_threshold_bps: SPIKE_THRESHOLD_BPS,
            spikes_in_window: self.spike_history.len(),
            max_hold_ms: MAX_HOLD_MS,
            stop_loss_bps: STOP_LOSS_BPS,
        }
    }

    fn chart_data(&self, symbol: &str) -> ChartData {
        // Downsample to max ~600 points
        let len = self.mid_samples.len();
        let step = (len / 600).max(1);
        let cap = len / step + 1;
        let mut ts = Vec::with_capacity(cap);
        let mut gt_bid = Vec::with_capacity(cap);
        let mut gt_ask = Vec::with_capacity(cap);
        let mut bn_bid = Vec::with_capacity(cap);
        let mut bn_ask = Vec::with_capacity(cap);
        for (i, s) in self.mid_samples.iter().enumerate() {
            if i % step == 0 {
                ts.push(s.ts_ms as f64 / 1000.0);
                gt_bid.push(s.gate_bid);
                gt_ask.push(s.gate_ask);
                bn_bid.push(s.binance_bid);
                bn_ask.push(s.binance_ask);
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
            exit_reason: t.exit_reason.to_string(),
            spike_bps: t.spike_bps,
            catchup_pct: t.catchup_pct,
            entry_price: t.entry_price,
            exit_price: t.exit_price,
        }).collect();

        ChartData {
            symbol: symbol.to_string(),
            ts,
            gate_bid: gt_bid,
            gate_ask: gt_ask,
            binance_bid: bn_bid,
            binance_ask: bn_ask,
            trades,
            position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            entry_ts_ms: self.position.as_ref().map(|p| p.entry_ts_ms),
        }
    }
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
