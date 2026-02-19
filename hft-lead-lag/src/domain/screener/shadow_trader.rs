//! ShadowTrader — paper-trading spike-follow model.
//!
//! Strategy: Binance leads, Gate lags. When Binance ask spikes up (for longs)
//! or Binance bid drops (for shorts) ≥ threshold in a short window, enter on
//! Gate in the same direction. Exit when Gate catches up (target), on timeout,
//! or stop-loss.
//! All execution is paper-traded on Gate bid/ask with simulated fill delay.

use std::collections::VecDeque;
use serde::Serialize;
use super::state::Quote;

const TEN_MINUTES_MS: i64 = 10 * 60 * 1000;

/// Gate taker fee (fraction).
const GATE_TAKER_FEE: f64 = 0.000_5;
/// Simulated order-to-fill latency (ms).
const FILL_DELAY_MS: i64 = 6;
/// Post-trade cooldown (ms).
const COOLDOWN_MS: i64 = 3_000;
/// Mid-price sample retention for chart + spike detection (ms).
const CHART_RETENTION_MS: i64 = 2 * 60 * 1000;
/// Warmup before trading starts (ms).
const WARMUP_MS: i64 = 30_000;
/// Max quote staleness (ms).
const QUOTE_FRESHNESS_MS: i64 = 1_000;
/// Min Binance move to trigger entry (bps). Ask for longs, bid for shorts.
const SPIKE_THRESHOLD_BPS: f64 = 30.0;
/// Window to measure Binance spike (ms).
const SPIKE_WINDOW_MS: i64 = 500;
/// Max hold time (ms).
const MAX_HOLD_MS: i64 = 30_000;
/// Stop-loss (bps).
const STOP_LOSS_BPS: f64 = 10.0;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    Short,
    Long,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    direction: Direction,
    gate_entry_price: f64,
    entry_ts_ms: i64,
    spike_bps: f64,
}

#[derive(Debug, Clone)]
struct ClosedTrade {
    pnl_pct: f64,
    ts_ms: i64,
    direction: Direction,
    entry_ts_ms: i64,
    entry_price: f64,
    exit_price: f64,
    exit_reason: &'static str,
    spike_bps: f64,
    catchup_pct: f64,
    catchup_ms: i64,
}

/// Only timestamp is needed — we count spikes in window via `.len()`.
#[derive(Debug, Clone)]
struct MidSample {
    ts_ms: i64,
    gate_bid: f64,
    gate_ask: f64,
    binance_bid: f64,
    binance_ask: f64,
}

#[derive(Debug, Clone)]
struct PendingOrder {
    direction: Direction,
    fire_ts_ms: i64,
    is_exit: bool,
    exit_pos: Option<OpenPosition>,
    spike_bps: f64,
    exit_reason: &'static str,
}

// ---------------------------------------------------------------------------
// Public DTOs (read models)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct ChartTrade {
    pub entry_ts_ms: i64,
    pub exit_ts_ms: i64,
    pub direction: &'static str,
    pub pnl_pct: f64,
    pub exit_reason: &'static str,
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
    pub position: &'static str,
    pub entry_price: Option<f64>,
    pub entry_ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShadowStats {
    pub pnl_per_hour_pct: f64,
    pub trades_in_window: usize,
    pub avg_trade_pct: f64,
    pub win_rate_pct: f64,
    pub position: &'static str,
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
    pub position: &'static str,
    pub entry_price: Option<f64>,
    pub last_5_trades_pnl_pct: Vec<f64>,
    pub spike_threshold_bps: f64,
    pub spikes_in_window: usize,
    pub max_hold_ms: i64,
    pub stop_loss_bps: f64,
}

// ---------------------------------------------------------------------------
// ShadowTrader
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ShadowTrader {
    mid_samples: VecDeque<MidSample>,
    position: Option<OpenPosition>,
    pending: Option<PendingOrder>,
    completed_trades: VecDeque<ClosedTrade>,
    /// Only timestamps — direction/magnitude aren't queried from history.
    spike_timestamps: VecDeque<i64>,
    start_ts_ms: Option<i64>,
    latest_ts_ms: i64,
    cooldown_until_ms: i64,
}

impl ShadowTrader {
    pub fn tick(&mut self, ts_ms: i64, binance: &Quote, gate: &Quote, window_ms: i64) {
        if self.start_ts_ms.is_none() {
            self.start_ts_ms = Some(ts_ms);
        }
        self.latest_ts_ms = ts_ms;

        if (ts_ms - binance.ts_ms).unsigned_abs() > QUOTE_FRESHNESS_MS as u64
            || (ts_ms - gate.ts_ms).unsigned_abs() > QUOTE_FRESHNESS_MS as u64
        {
            return;
        }

        self.mid_samples.push_back(MidSample {
            ts_ms,
            gate_bid: gate.bid, gate_ask: gate.ask,
            binance_bid: binance.bid, binance_ask: binance.ask,
        });
        self.cleanup(ts_ms, window_ms);

        let elapsed = ts_ms.saturating_sub(self.start_ts_ms.unwrap_or(ts_ms));
        if elapsed < WARMUP_MS {
            return;
        }

        // Execute pending orders after FILL_DELAY_MS
        if let Some(pending) = self.pending.take() {
            if ts_ms >= pending.fire_ts_ms + FILL_DELAY_MS {
                if pending.is_exit {
                    if let Some(pos) = pending.exit_pos {
                        self.fill_exit(ts_ms, gate, window_ms, pos, pending.exit_reason);
                    }
                } else {
                    let gate_price = match pending.direction {
                        Direction::Long => gate.ask,
                        Direction::Short => gate.bid,
                    };
                    self.position = Some(OpenPosition {
                        direction: pending.direction,
                        gate_entry_price: gate_price,
                        entry_ts_ms: ts_ms,
                        spike_bps: pending.spike_bps,
                    });
                }
            } else {
                self.pending = Some(pending);
            }
            return;
        }

        // Exit logic
        if let Some(pos) = self.position.as_ref() {
            let hold_ms = ts_ms - pos.entry_ts_ms;
            let unrealized_bps = match pos.direction {
                Direction::Long => {
                    ((gate.bid - pos.gate_entry_price) / pos.gate_entry_price) * 10_000.0
                }
                Direction::Short => {
                    ((pos.gate_entry_price - gate.ask) / pos.gate_entry_price) * 10_000.0
                }
            };

            let gate_moved_enough = unrealized_bps >= SPIKE_THRESHOLD_BPS;
            let timed_out = hold_ms >= MAX_HOLD_MS;
            let stopped_out = unrealized_bps <= -STOP_LOSS_BPS;

            if gate_moved_enough || timed_out || stopped_out {
                let reason = if gate_moved_enough { "target" }
                    else if stopped_out { "stop_loss" }
                    else { "timeout" };
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

        // Spike detection & entry
        if self.position.is_none() && self.pending.is_none() && ts_ms >= self.cooldown_until_ms {
            if let Some((direction, spike_bps)) = self.detect_spike(ts_ms, binance) {
                self.spike_timestamps.push_back(ts_ms);
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

    fn detect_spike(&self, now_ms: i64, binance: &Quote) -> Option<(Direction, f64)> {
        let cutoff = now_ms - SPIKE_WINDOW_MS;
        let baseline = self.mid_samples.iter().find(|s| s.ts_ms >= cutoff)?;

        // LONG: Binance ask spiked up → we buy Gate ask
        if baseline.binance_ask > 0.0 {
            let ask_move_bps = ((binance.ask - baseline.binance_ask) / baseline.binance_ask) * 10_000.0;
            if ask_move_bps >= SPIKE_THRESHOLD_BPS {
                return Some((Direction::Long, ask_move_bps));
            }
        }

        // SHORT: Binance bid dropped → we sell Gate bid
        if baseline.binance_bid > 0.0 {
            let bid_move_bps = ((baseline.binance_bid - binance.bid) / baseline.binance_bid) * 10_000.0;
            if bid_move_bps >= SPIKE_THRESHOLD_BPS {
                return Some((Direction::Short, bid_move_bps));
            }
        }

        None
    }

    fn fill_exit(
        &mut self,
        ts_ms: i64,
        gate: &Quote,
        window_ms: i64,
        pos: OpenPosition,
        exit_reason: &'static str,
    ) {
        let fees = GATE_TAKER_FEE * 2.0;
        let (pnl_pct, catchup_pct, exit_price) = match pos.direction {
            Direction::Long => {
                let exit_price = gate.bid;
                let raw_pnl = (exit_price - pos.gate_entry_price) / pos.gate_entry_price;
                ((raw_pnl - fees) * 100.0, raw_pnl * 100.0, exit_price)
            }
            Direction::Short => {
                let exit_price = gate.ask;
                let raw_pnl = (pos.gate_entry_price - exit_price) / pos.gate_entry_price;
                ((raw_pnl - fees) * 100.0, raw_pnl * 100.0, exit_price)
            }
        };

        self.completed_trades.push_back(ClosedTrade {
            pnl_pct,
            ts_ms,
            direction: pos.direction,
            entry_ts_ms: pos.entry_ts_ms,
            entry_price: pos.gate_entry_price,
            exit_price,
            exit_reason,
            spike_bps: pos.spike_bps,
            catchup_pct,
            catchup_ms: ts_ms - pos.entry_ts_ms,
        });
        self.cooldown_until_ms = ts_ms + COOLDOWN_MS;

        let cutoff = ts_ms - window_ms;
        while let Some(t) = self.completed_trades.front() {
            if t.ts_ms >= cutoff { break; }
            self.completed_trades.pop_front();
        }
    }

    pub fn stats(&self) -> ShadowStats {
        let n = self.completed_trades.len();
        if n == 0 {
            return ShadowStats {
                pnl_per_hour_pct: 0.0,
                trades_in_window: 0,
                avg_trade_pct: 0.0,
                win_rate_pct: 0.0,
                position: self.position_label(),
                spikes_detected: self.spike_timestamps.len(),
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
        let avg_catchup = self.completed_trades.iter().map(|t| t.catchup_pct).sum::<f64>() / n as f64;
        let avg_lag = self.completed_trades.iter().map(|t| t.catchup_ms as f64).sum::<f64>() / n as f64;

        ShadowStats {
            pnl_per_hour_pct: total_pnl / window_hours,
            trades_in_window: n,
            avg_trade_pct: total_pnl / n as f64,
            win_rate_pct: (wins as f64 / n as f64) * 100.0,
            position: self.position_label(),
            spikes_detected: self.spike_timestamps.len(),
            avg_catchup_pct: avg_catchup,
            avg_catchup_lag_ms: avg_lag,
        }
    }

    pub fn position_label(&self) -> &'static str {
        if self.pending.is_some() {
            return "PENDING";
        }
        match &self.position {
            None => "FLAT",
            Some(p) => match p.direction {
                Direction::Short => "SHORT_GT",
                Direction::Long => "LONG_GT",
            },
        }
    }

    fn cleanup(&mut self, ts_ms: i64, _window_ms: i64) {
        let sample_cutoff = ts_ms - CHART_RETENTION_MS;
        while let Some(s) = self.mid_samples.front() {
            if s.ts_ms >= sample_cutoff { break; }
            self.mid_samples.pop_front();
        }
        while let Some(&spike_ts) = self.spike_timestamps.front() {
            if spike_ts >= sample_cutoff { break; }
            self.spike_timestamps.pop_front();
        }
    }

    pub fn debug(&self) -> ShadowDebug {
        let elapsed = self.start_ts_ms
            .map(|s| self.latest_ts_ms.saturating_sub(s))
            .unwrap_or(0);
        let warmup_remaining = (WARMUP_MS - elapsed).max(0);
        let cooldown_remaining = (self.cooldown_until_ms - self.latest_ts_ms).max(0);
        let last_5: Vec<f64> = self.completed_trades.iter()
            .rev().take(5).map(|t| t.pnl_pct).collect();
        let last = self.mid_samples.back();

        ShadowDebug {
            samples: self.mid_samples.len(),
            last_binance_bid: last.map(|s| s.binance_bid).unwrap_or(0.0),
            last_binance_ask: last.map(|s| s.binance_ask).unwrap_or(0.0),
            last_gate_bid: last.map(|s| s.gate_bid).unwrap_or(0.0),
            last_gate_ask: last.map(|s| s.gate_ask).unwrap_or(0.0),
            completed_trades_in_window: self.completed_trades.len(),
            cooldown_remaining_ms: cooldown_remaining,
            warmup_remaining_ms: warmup_remaining,
            position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            last_5_trades_pnl_pct: last_5,
            spike_threshold_bps: SPIKE_THRESHOLD_BPS,
            spikes_in_window: self.spike_timestamps.len(),
            max_hold_ms: MAX_HOLD_MS,
            stop_loss_bps: STOP_LOSS_BPS,
        }
    }

    pub fn chart_data(&self, symbol: &str) -> ChartData {
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
                Direction::Short => "SHORT",
                Direction::Long => "LONG",
            },
            pnl_pct: t.pnl_pct,
            exit_reason: t.exit_reason,
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
