//! ShadowTrader — paper-trading spike-follow model.
//!
//! Strategy: Binance leads, Gate lags. When Binance ask spikes up (for longs)
//! or Binance bid drops (for shorts) ≥ threshold in a short window, enter on
//! Gate in the same direction. Exit when Gate catches up (target), on timeout,
//! or stop-loss.

use std::collections::VecDeque;
use serde::Serialize;

use super::price_samples::PriceSamples;
use super::state::Quote;
use super::trader_config::TraderConfig;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction { Short, Long }

#[derive(Debug, Clone)]
struct OpenPosition {
    direction: Direction,
    gate_entry_price: f64,
    entry_ts_ms: i64,
    spike_bps: f64,
    gate_spread_at_entry_bps: f64,
    /// Highest unrealized profit seen (bps) — for trailing stop.
    peak_unrealized_bps: f64,
}

/// Completed trade record — public for fleet/db persistence.
#[derive(Debug, Clone)]
pub struct ClosedTrade {
    pub pnl_pct: f64,
    pub ts_ms: i64,
    pub direction: Direction,
    pub entry_ts_ms: i64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub exit_reason: &'static str,
    pub spike_bps: f64,
    pub catchup_pct: f64,
    pub catchup_ms: i64,
    pub gate_spread_at_entry_bps: f64,
}

impl ClosedTrade {
    pub fn direction_str(&self) -> &'static str {
        match self.direction {
            Direction::Long => "LONG",
            Direction::Short => "SHORT",
        }
    }
}

#[derive(Debug, Clone)]
struct PendingOrder {
    direction: Direction,
    fire_ts_ms: i64,
    is_exit: bool,
    exit_pos: Option<OpenPosition>,
    spike_bps: f64,
    exit_reason: &'static str,
    gate_spread_at_entry_bps: f64,
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
    pub session_pnl_pct: f64,
    pub session_trades: usize,
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

/// Shadow trader instance. Holds only trading state (no price samples).
/// Receives `&PriceSamples` on each tick — samples are shared per symbol.
#[derive(Debug)]
pub struct ShadowTrader {
    config: TraderConfig,
    position: Option<OpenPosition>,
    pending: Option<PendingOrder>,
    completed_trades: VecDeque<ClosedTrade>,
    session_total_pnl_pct: f64,
    session_trades: usize,
    session_wins: usize,
    spike_timestamps: VecDeque<i64>,
    start_ts_ms: Option<i64>,
    latest_ts_ms: i64,
    cooldown_until_ms: i64,
}

impl Default for ShadowTrader {
    fn default() -> Self { Self::new(TraderConfig::default()) }
}

impl ShadowTrader {
    pub fn new(config: TraderConfig) -> Self {
        Self {
            config,
            position: None,
            pending: None,
            completed_trades: VecDeque::new(),
            session_total_pnl_pct: 0.0,
            session_trades: 0,
            session_wins: 0,
            spike_timestamps: VecDeque::new(),
            start_ts_ms: None,
            latest_ts_ms: 0,
            cooldown_until_ms: 0,
        }
    }

    pub fn config(&self) -> &TraderConfig { &self.config }

    pub fn completed_trades(&self) -> &VecDeque<ClosedTrade> { &self.completed_trades }

    pub fn session_trades(&self) -> usize { self.session_trades }

    pub fn session_pnl_pct(&self) -> f64 { self.session_total_pnl_pct }

    // -- Core tick -----------------------------------------------------------

    pub fn tick(
        &mut self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
        window_ms: i64,
    ) {
        if self.start_ts_ms.is_none() {
            self.start_ts_ms = Some(ts_ms);
        }
        self.latest_ts_ms = ts_ms;
        self.cleanup_spikes(ts_ms);

        let cfg = &self.config;

        if (ts_ms - binance.ts_ms).unsigned_abs() > cfg.quote_freshness_ms as u64
            || (ts_ms - gate.ts_ms).unsigned_abs() > cfg.quote_freshness_ms as u64
        {
            return;
        }

        let elapsed = ts_ms.saturating_sub(self.start_ts_ms.unwrap_or(ts_ms));
        if elapsed < cfg.warmup_ms {
            return;
        }

        self.try_fill(ts_ms, gate, window_ms);
        if self.pending.is_some() { return; }
        self.try_exit(ts_ms, gate);
        self.try_entry(ts_ms, binance, gate, samples);
    }

    // -- Fill pending orders -------------------------------------------------

    fn try_fill(&mut self, ts_ms: i64, gate: &Quote, window_ms: i64) {
        let Some(pending) = self.pending.take() else { return };
        if ts_ms < pending.fire_ts_ms + self.config.fill_delay_ms {
            self.pending = Some(pending);
            return;
        }
        if pending.is_exit {
            if let Some(pos) = pending.exit_pos {
                self.fill_exit(ts_ms, gate, window_ms, pos, pending.exit_reason);
            } else {
                tracing::warn!("exit order with exit_pos=None — dropping");
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
                gate_spread_at_entry_bps: pending.gate_spread_at_entry_bps,
                peak_unrealized_bps: 0.0,
            });
        }
    }

    // -- Exit logic ----------------------------------------------------------

    fn try_exit(&mut self, ts_ms: i64, gate: &Quote) {
        let Some(pos) = self.position.as_mut() else { return };
        let cfg = &self.config;
        let hold_ms = ts_ms - pos.entry_ts_ms;

        let unrealized_bps = match pos.direction {
            Direction::Long =>
                ((gate.bid - pos.gate_entry_price) / pos.gate_entry_price) * 10_000.0,
            Direction::Short =>
                ((pos.gate_entry_price - gate.ask) / pos.gate_entry_price) * 10_000.0,
        };
        if unrealized_bps > pos.peak_unrealized_bps {
            pos.peak_unrealized_bps = unrealized_bps;
        }

        let target_bps = pos.spike_bps * cfg.target_ratio;
        let gate_moved_enough = unrealized_bps >= target_bps;
        let timed_out = hold_ms >= cfg.max_hold_ms;
        let stopped_out = unrealized_bps <= -cfg.stop_loss_bps;
        let trailing_stopped = cfg.trailing_stop_bps > 0.0
            && pos.peak_unrealized_bps >= cfg.trailing_stop_bps
            && unrealized_bps <= pos.peak_unrealized_bps * cfg.trailing_decay_ratio;

        if gate_moved_enough || timed_out || stopped_out || trailing_stopped {
            let reason = if gate_moved_enough { "target" }
                else if stopped_out { "stop_loss" }
                else if trailing_stopped { "trailing_stop" }
                else { "timeout" };
            let pos = self.position.take().unwrap();
            self.pending = Some(PendingOrder {
                direction: pos.direction,
                fire_ts_ms: ts_ms,
                is_exit: true,
                exit_pos: Some(pos),
                spike_bps: 0.0,
                exit_reason: reason,
                gate_spread_at_entry_bps: 0.0,
            });
        }
    }

    // -- Entry logic ---------------------------------------------------------

    fn try_entry(&mut self, ts_ms: i64, binance: &Quote, gate: &Quote, samples: &PriceSamples) {
        if self.position.is_some() || self.pending.is_some() || ts_ms < self.cooldown_until_ms {
            return;
        }
        let cfg = &self.config;

        // Spread filter
        if cfg.max_spread_bps > 0.0 {
            let gate_mid = (gate.bid + gate.ask) * 0.5;
            if gate_mid > 0.0 {
                let spread_bps = ((gate.ask - gate.bid) / gate_mid) * 10_000.0;
                if spread_bps > cfg.max_spread_bps { return; }
            }
        }

        if let Some((direction, gap_bps)) = self.detect_gap(binance, gate, samples) {
            self.spike_timestamps.push_back(ts_ms);
            let gate_mid = (gate.bid + gate.ask) * 0.5;
            let spread_bps = if gate_mid > 0.0 {
                ((gate.ask - gate.bid) / gate_mid) * 10_000.0
            } else { 0.0 };
            self.pending = Some(PendingOrder {
                direction,
                fire_ts_ms: ts_ms,
                is_exit: false,
                exit_pos: None,
                spike_bps: gap_bps,
                exit_reason: "",
                gate_spread_at_entry_bps: spread_bps,
            });
        }
    }

    // -- Gap detection (lead-lag) --------------------------------------------

    /// Lead-lag with baseline: enter when current gap EXCEEDS the average gap.
    /// Baseline = mean(binance - gate) over PriceSamples history (~2 min).
    /// Signal = current_gap - baseline_gap. Enter when signal > threshold.
    fn detect_gap(
        &self, binance: &Quote, gate: &Quote, samples: &PriceSamples,
    ) -> Option<(Direction, f64)> {
        if samples.len() < 20 { return None; } // need enough history for baseline

        // Compute baseline gap (average ask-gap and bid-gap over history)
        let (mut ask_gap_sum, mut bid_gap_sum, mut count) = (0.0_f64, 0.0_f64, 0_u32);
        for s in samples.iter() {
            if s.gate_ask > 0.0 && s.binance_ask > 0.0 {
                ask_gap_sum += ((s.binance_ask - s.gate_ask) / s.gate_ask) * 10_000.0;
            }
            if s.gate_bid > 0.0 && s.binance_bid > 0.0 {
                bid_gap_sum += ((s.gate_bid - s.binance_bid) / s.gate_bid) * 10_000.0;
            }
            count += 1;
        }
        if count == 0 { return None; }
        let baseline_ask_gap = ask_gap_sum / count as f64;
        let baseline_bid_gap = bid_gap_sum / count as f64;

        let threshold = self.config.spike_threshold_bps;

        // Long: current ask-gap exceeds baseline by threshold
        if gate.ask > 0.0 {
            let current_gap = ((binance.ask - gate.ask) / gate.ask) * 10_000.0;
            let signal = current_gap - baseline_ask_gap;
            if signal >= threshold {
                return Some((Direction::Long, signal));
            }
        }
        // Short: current bid-gap exceeds baseline by threshold
        if gate.bid > 0.0 {
            let current_gap = ((gate.bid - binance.bid) / gate.bid) * 10_000.0;
            let signal = current_gap - baseline_bid_gap;
            if signal >= threshold {
                return Some((Direction::Short, signal));
            }
        }
        None
    }

    // -- Fill exit & bookkeeping ---------------------------------------------

    fn fill_exit(
        &mut self, ts_ms: i64, gate: &Quote, window_ms: i64,
        pos: OpenPosition, exit_reason: &'static str,
    ) {
        let fees = self.config.taker_fee * 2.0;
        let (pnl_pct, catchup_pct, exit_price) = match pos.direction {
            Direction::Long => {
                let ep = gate.bid;
                let raw = (ep - pos.gate_entry_price) / pos.gate_entry_price;
                ((raw - fees) * 100.0, raw * 100.0, ep)
            }
            Direction::Short => {
                let ep = gate.ask;
                let raw = (pos.gate_entry_price - ep) / pos.gate_entry_price;
                ((raw - fees) * 100.0, raw * 100.0, ep)
            }
        };

        self.completed_trades.push_back(ClosedTrade {
            pnl_pct, ts_ms, direction: pos.direction,
            entry_ts_ms: pos.entry_ts_ms, entry_price: pos.gate_entry_price,
            exit_price, exit_reason, spike_bps: pos.spike_bps,
            catchup_pct, catchup_ms: ts_ms - pos.entry_ts_ms,
            gate_spread_at_entry_bps: pos.gate_spread_at_entry_bps,
        });
        self.session_total_pnl_pct += pnl_pct;
        self.session_trades += 1;
        if pnl_pct > 0.0 {
            self.session_wins += 1;
        }
        self.cooldown_until_ms = ts_ms + self.config.cooldown_ms;

        let cutoff = ts_ms - window_ms;
        while let Some(t) = self.completed_trades.front() {
            if t.ts_ms >= cutoff { break; }
            self.completed_trades.pop_front();
        }
    }

    // -- Read models ---------------------------------------------------------

    pub fn stats(&self) -> ShadowStats {
        let window_n = self.completed_trades.len();
        if self.session_trades == 0 {
            return ShadowStats {
                session_pnl_pct: 0.0, session_trades: 0,
                avg_trade_pct: 0.0, win_rate_pct: 0.0,
                position: self.position_label(),
                spikes_detected: self.spike_timestamps.len(),
                avg_catchup_pct: 0.0, avg_catchup_lag_ms: 0.0,
            };
        }
        let avg_catchup = if window_n > 0 {
            self.completed_trades.iter().map(|t| t.catchup_pct).sum::<f64>() / window_n as f64
        } else {
            0.0
        };
        let avg_lag = if window_n > 0 {
            self.completed_trades.iter().map(|t| t.catchup_ms as f64).sum::<f64>() / window_n as f64
        } else {
            0.0
        };

        ShadowStats {
            session_pnl_pct: self.session_total_pnl_pct,
            session_trades: self.session_trades,
            avg_trade_pct: self.session_total_pnl_pct / self.session_trades as f64,
            win_rate_pct: (self.session_wins as f64 / self.session_trades as f64) * 100.0,
            position: self.position_label(),
            spikes_detected: self.spike_timestamps.len(),
            avg_catchup_pct: avg_catchup,
            avg_catchup_lag_ms: avg_lag,
        }
    }

    pub fn position_label(&self) -> &'static str {
        if self.pending.is_some() { return "PENDING"; }
        match &self.position {
            None => "FLAT",
            Some(p) => match p.direction {
                Direction::Short => "SHORT_GT",
                Direction::Long => "LONG_GT",
            },
        }
    }

    fn cleanup_spikes(&mut self, ts_ms: i64) {
        let cutoff = ts_ms - 2 * 60 * 1000;
        while let Some(&spike_ts) = self.spike_timestamps.front() {
            if spike_ts >= cutoff { break; }
            self.spike_timestamps.pop_front();
        }
    }

    pub fn debug(&self, samples: &PriceSamples) -> ShadowDebug {
        let elapsed = self.start_ts_ms
            .map(|s| self.latest_ts_ms.saturating_sub(s)).unwrap_or(0);
        let last_5: Vec<f64> = self.completed_trades.iter()
            .rev().take(5).map(|t| t.pnl_pct).collect();
        let last = samples.back();
        ShadowDebug {
            samples: samples.len(),
            last_binance_bid: last.map(|s| s.binance_bid).unwrap_or(0.0),
            last_binance_ask: last.map(|s| s.binance_ask).unwrap_or(0.0),
            last_gate_bid: last.map(|s| s.gate_bid).unwrap_or(0.0),
            last_gate_ask: last.map(|s| s.gate_ask).unwrap_or(0.0),
            completed_trades_in_window: self.completed_trades.len(),
            cooldown_remaining_ms: (self.cooldown_until_ms - self.latest_ts_ms).max(0),
            warmup_remaining_ms: (self.config.warmup_ms - elapsed).max(0),
            position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            last_5_trades_pnl_pct: last_5,
            spike_threshold_bps: self.config.spike_threshold_bps,
            spikes_in_window: self.spike_timestamps.len(),
            max_hold_ms: self.config.max_hold_ms,
            stop_loss_bps: self.config.stop_loss_bps,
        }
    }

    pub fn chart_data(&self, symbol: &str, samples: &PriceSamples) -> ChartData {
        let len = samples.len();
        let step = (len / 600).max(1);
        let cap = len / step + 1;
        let mut ts = Vec::with_capacity(cap);
        let mut gt_bid = Vec::with_capacity(cap);
        let mut gt_ask = Vec::with_capacity(cap);
        let mut bn_bid = Vec::with_capacity(cap);
        let mut bn_ask = Vec::with_capacity(cap);
        for (i, s) in samples.iter().enumerate() {
            if i % step == 0 {
                ts.push(s.ts_ms as f64 / 1000.0);
                gt_bid.push(s.gate_bid);
                gt_ask.push(s.gate_ask);
                bn_bid.push(s.binance_bid);
                bn_ask.push(s.binance_ask);
            }
        }
        let trades: Vec<ChartTrade> = self.completed_trades.iter().map(|t| ChartTrade {
            entry_ts_ms: t.entry_ts_ms, exit_ts_ms: t.ts_ms,
            direction: t.direction_str(),
            pnl_pct: t.pnl_pct, exit_reason: t.exit_reason,
            spike_bps: t.spike_bps, catchup_pct: t.catchup_pct,
            entry_price: t.entry_price, exit_price: t.exit_price,
        }).collect();
        ChartData {
            symbol: symbol.to_string(), ts,
            gate_bid: gt_bid, gate_ask: gt_ask,
            binance_bid: bn_bid, binance_ask: bn_ask,
            trades, position: self.position_label(),
            entry_price: self.position.as_ref().map(|p| p.gate_entry_price),
            entry_ts_ms: self.position.as_ref().map(|p| p.entry_ts_ms),
        }
    }
}
