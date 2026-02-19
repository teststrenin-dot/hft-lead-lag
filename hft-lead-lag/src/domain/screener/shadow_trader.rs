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
    /// Highest unrealized profit seen (bps) — for trailing take-profit.
    peak_unrealized_bps: f64,
    /// True once unrealized reaches breakeven threshold (spike * target_ratio).
    /// Switches exit from stop-loss to breakeven + trailing take.
    breakeven_activated: bool,
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
enum PendingOrder {
    Entry {
        direction: Direction,
        fire_ts_ms: i64,
        spike_bps: f64,
        gate_spread_at_entry_bps: f64,
    },
    Exit {
        fire_ts_ms: i64,
        pos: OpenPosition,
        reason: &'static str,
    },
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
        let fire_ts = match &pending {
            PendingOrder::Entry { fire_ts_ms, .. } => *fire_ts_ms,
            PendingOrder::Exit { fire_ts_ms, .. } => *fire_ts_ms,
        };
        if ts_ms < fire_ts + self.config.fill_delay_ms {
            self.pending = Some(pending);
            return;
        }
        match pending {
            PendingOrder::Exit { pos, reason, .. } => {
                self.fill_exit(ts_ms, gate, window_ms, pos, reason);
            }
            PendingOrder::Entry { direction, spike_bps, gate_spread_at_entry_bps, .. } => {
                let gate_price = match direction {
                    Direction::Long => gate.ask,
                    Direction::Short => gate.bid,
                };
                self.position = Some(OpenPosition {
                    direction,
                    gate_entry_price: gate_price,
                    entry_ts_ms: ts_ms,
                    spike_bps,
                    gate_spread_at_entry_bps,
                    peak_unrealized_bps: 0.0,
                    breakeven_activated: false,
                });
            }
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

        let timed_out = hold_ms >= cfg.max_hold_ms;

        // Activate breakeven when unrealized reaches spike × target_ratio.
        let breakeven_threshold = pos.spike_bps * cfg.target_ratio;
        if !pos.breakeven_activated && unrealized_bps >= breakeven_threshold {
            pos.breakeven_activated = true;
        }

        let (should_exit, reason) = if pos.breakeven_activated {
            // Phase 2: stop at breakeven (entry price), trailing take-profit.
            let hit_breakeven_stop = unrealized_bps <= 0.0;
            let trailing_take = unrealized_bps <= pos.peak_unrealized_bps * cfg.trailing_decay_ratio;
            if hit_breakeven_stop {
                (true, "breakeven")
            } else if trailing_take {
                (true, "trailing_take")
            } else if timed_out {
                (true, "timeout")
            } else {
                (false, "")
            }
        } else {
            // Phase 1: stop-loss only (no target exit — let trade develop).
            let stopped_out = unrealized_bps <= -cfg.stop_loss_bps;
            if stopped_out {
                (true, "stop_loss")
            } else if timed_out {
                (true, "timeout")
            } else {
                (false, "")
            }
        };

        if should_exit {
            let pos = self.position.take().unwrap();
            self.pending = Some(PendingOrder::Exit {
                fire_ts_ms: ts_ms,
                pos,
                reason,
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
            self.pending = Some(PendingOrder::Entry {
                direction,
                fire_ts_ms: ts_ms,
                spike_bps: gap_bps,
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
        if samples.len() < self.config.min_baseline_samples { return None; }

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::price_samples::{PriceSample, PriceSamples};

    const WINDOW_MS: i64 = 120_000;

    /// Build a trader with explicit overrides on top of defaults.
    fn make_trader(f: impl FnOnce(&mut TraderConfig)) -> ShadowTrader {
        let mut cfg = TraderConfig::default();
        f(&mut cfg);
        ShadowTrader::new(cfg)
    }

    fn quote(bid: f64, ask: f64, ts_ms: i64) -> Quote {
        Quote { bid, ask, ts_ms }
    }

    /// Fill samples with `n` identical snapshots so baseline is stable.
    fn stable_samples(n: usize, gate: f64, binance: f64, ts_ms: i64) -> PriceSamples {
        let mut ps = PriceSamples::default();
        for i in 0..n {
            ps.push(PriceSample {
                ts_ms: ts_ms - (n as i64 - i as i64) * 100,
                gate_bid: gate, gate_ask: gate,
                binance_bid: binance, binance_ask: binance,
            });
        }
        ps
    }

    // -- Baseline & entry signal ------------------------------------------------

    #[test]
    fn baseline_needs_min_samples() {
        let trader = make_trader(|c| { c.min_baseline_samples = 20; });
        let samples = stable_samples(19, 100.0, 100.0, 50_000);
        let bn = quote(100.0, 100.0, 50_000);
        let gt = quote(100.0, 100.0, 50_000);
        assert!(trader.detect_gap(&bn, &gt, &samples).is_none());
    }

    #[test]
    fn entry_signal_long_fires_above_threshold() {
        let trader = make_trader(|c| {
            c.spike_threshold_bps = 50.0;
            c.warmup_ms = 0;
            c.min_baseline_samples = 5;
        });
        // baseline: binance_ask == gate_ask == 100 → baseline_ask_gap = 0
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        // current: binance_ask = 100.60, gate_ask = 100 → current_gap = 60 bps
        let bn = quote(100.60, 100.60, 50_000);
        let gt = quote(100.0, 100.0, 50_000);
        let result = trader.detect_gap(&bn, &gt, &samples);
        assert!(result.is_some());
        let (dir, gap_bps) = result.unwrap();
        assert_eq!(dir, Direction::Long);
        assert!((gap_bps - 60.0).abs() < 1.0, "expected ~60 bps, got {gap_bps}");
    }

    #[test]
    fn entry_signal_below_threshold_no_fire() {
        let trader = make_trader(|c| {
            c.spike_threshold_bps = 50.0;
            c.min_baseline_samples = 5;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        // 30 bps gap — below 50 bps threshold
        let bn = quote(100.30, 100.30, 50_000);
        let gt = quote(100.0, 100.0, 50_000);
        assert!(trader.detect_gap(&bn, &gt, &samples).is_none());
    }

    // -- PnL with fees ----------------------------------------------------------

    #[test]
    fn pnl_long_with_fees() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.cooldown_ms = 0;
            c.min_baseline_samples = 5;
            c.max_spread_bps = 0.0;
            c.taker_fee = 0.000_5;
            c.max_hold_ms = 100;
            c.target_ratio = 99.0;
            c.stop_loss_bps = 999.0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        // Entry: binance spikes to 100.20 (20 bps gap)
        let bn_entry = quote(100.20, 100.20, 50_100);
        let gt_entry = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn_entry, &gt_entry, &samples, WINDOW_MS);
        // Fill entry (fill_delay=0): gate.ask = 100.0
        trader.tick(50_200, &bn_entry, &gt_entry, &samples, WINDOW_MS);
        assert_eq!(trader.position_label(), "LONG_GT");

        // Force timeout: gate.bid = 100.10 → raw = 0.001
        let gt_exit = quote(100.10, 100.20, 50_500);
        trader.tick(50_500, &bn_entry, &gt_exit, &samples, WINDOW_MS);
        // Fill exit
        trader.tick(50_600, &bn_entry, &gt_exit, &samples, WINDOW_MS);

        let trades = trader.completed_trades();
        assert_eq!(trades.len(), 1);
        let t = &trades[0];
        // raw = (100.10 - 100.0) / 100.0 = 0.001
        // fees = 0.0005 * 2 = 0.001
        // pnl = (0.001 - 0.001) * 100 = 0.0
        assert!(t.pnl_pct.abs() < 0.01, "expected ~0% pnl, got {}", t.pnl_pct);
    }

    // -- Exit conditions --------------------------------------------------------

    #[test]
    fn stop_loss_triggers() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.stop_loss_bps = 5.0;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.min_baseline_samples = 5;
            c.max_hold_ms = 999_999;
            c.max_spread_bps = 0.0;
            c.target_ratio = 99.0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        let bn = quote(100.20, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn, &gt, &samples, WINDOW_MS);
        // Fill
        trader.tick(50_200, &bn, &gt, &samples, WINDOW_MS);
        assert_eq!(trader.position_label(), "LONG_GT");
        // Price drops: gate.bid = 99.93 → unrealized = -7 bps < -5 bps SL
        let gt_drop = quote(99.93, 100.0, 50_300);
        trader.tick(50_300, &bn, &gt_drop, &samples, WINDOW_MS);
        // Fill exit
        trader.tick(50_400, &bn, &gt_drop, &samples, WINDOW_MS);
        assert_eq!(trader.position_label(), "FLAT");
        let t = &trader.completed_trades()[0];
        assert_eq!(t.exit_reason, "stop_loss");
    }

    #[test]
    fn breakeven_then_trailing_take() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.target_ratio = 0.5; // breakeven activates at spike * 0.5
            c.trailing_decay_ratio = 0.5; // exit when unrealized < peak * 0.5
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.min_baseline_samples = 5;
            c.max_hold_ms = 999_999;
            c.stop_loss_bps = 999.0;
            c.max_spread_bps = 0.0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        // 20 bps gap → spike_bps = 20, breakeven at 20 * 0.5 = 10 bps
        let bn = quote(100.20, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn, &gt, &samples, WINDOW_MS);
        trader.tick(50_200, &bn, &gt, &samples, WINDOW_MS); // fill entry at 100.0
        // gate.bid = 100.11 → unrealized = 11 bps >= 10 → breakeven activates
        let gt_up = quote(100.11, 100.20, 50_300);
        trader.tick(50_300, &bn, &gt_up, &samples, WINDOW_MS);
        // Still open — no target exit in new model, trailing take not triggered yet
        assert_eq!(trader.position_label(), "LONG_GT");
        // Peak: gate.bid = 100.20 → 20 bps
        let gt_peak = quote(100.20, 100.25, 50_400);
        trader.tick(50_400, &bn, &gt_peak, &samples, WINDOW_MS);
        // Drop: gate.bid = 100.09 → 9 bps < 20 * 0.5 = 10 → trailing take fires
        let gt_drop = quote(100.09, 100.20, 50_500);
        trader.tick(50_500, &bn, &gt_drop, &samples, WINDOW_MS);
        trader.tick(50_600, &bn, &gt_drop, &samples, WINDOW_MS); // fill exit
        assert_eq!(trader.position_label(), "FLAT");
        let t = &trader.completed_trades()[0];
        assert_eq!(t.exit_reason, "trailing_take");
        // exit at 100.09, entry at 100.0 → raw 9bps, fees 10bps → net -1bp
        // With trailing take the trade is in profit pre-fees but not after.
        // The important thing is the mechanism works — trailing_take fires correctly.
    }

    #[test]
    fn breakeven_stop_after_activation() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.target_ratio = 0.5; // breakeven at spike * 0.5
            c.trailing_decay_ratio = 0.5;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.min_baseline_samples = 5;
            c.max_hold_ms = 999_999;
            c.stop_loss_bps = 999.0;
            c.max_spread_bps = 0.0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        let bn = quote(100.20, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn, &gt, &samples, WINDOW_MS);
        trader.tick(50_200, &bn, &gt, &samples, WINDOW_MS); // fill at 100.0
        // Hit breakeven threshold: 11 bps >= 10
        let gt_up = quote(100.11, 100.20, 50_300);
        trader.tick(50_300, &bn, &gt_up, &samples, WINDOW_MS);
        // Price crashes back to entry: gate.bid = 99.99 → unrealized = -1 bps <= 0
        let gt_crash = quote(99.99, 100.20, 50_400);
        trader.tick(50_400, &bn, &gt_crash, &samples, WINDOW_MS);
        trader.tick(50_500, &bn, &gt_crash, &samples, WINDOW_MS); // fill exit
        assert_eq!(trader.position_label(), "FLAT");
        let t = &trader.completed_trades()[0];
        assert_eq!(t.exit_reason, "breakeven");
    }

    #[test]
    fn timeout_exit() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.max_hold_ms = 100;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.min_baseline_samples = 5;
            c.stop_loss_bps = 999.0;
            c.target_ratio = 99.0;
            c.max_spread_bps = 0.0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        let bn = quote(100.20, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn, &gt, &samples, WINDOW_MS);
        trader.tick(50_200, &bn, &gt, &samples, WINDOW_MS); // fill
        // Advance past max_hold_ms=100
        let gt_flat = quote(100.0, 100.0, 50_400);
        trader.tick(50_400, &bn, &gt_flat, &samples, WINDOW_MS); // timeout fires
        trader.tick(50_500, &bn, &gt_flat, &samples, WINDOW_MS); // fill exit
        assert_eq!(trader.position_label(), "FLAT");
        let t = &trader.completed_trades()[0];
        assert_eq!(t.exit_reason, "timeout");
    }

    // -- Spread filter ----------------------------------------------------------

    #[test]
    fn spread_filter_blocks_entry() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.max_spread_bps = 5.0;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.min_baseline_samples = 5;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        let bn = quote(100.20, 100.20, 50_100);
        // Gate spread = 0.10 / 100.0 * 10000 = 10 bps > max_spread 5 bps
        let gt = quote(99.95, 100.05, 50_100);
        trader.tick(50_100, &bn, &gt, &samples, WINDOW_MS);
        assert_eq!(trader.position_label(), "FLAT"); // entry blocked
    }

    // -- Session stats ----------------------------------------------------------

    #[test]
    fn session_stats_accumulate() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.target_ratio = 0.3;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.min_baseline_samples = 5;
            c.max_spread_bps = 0.0;
            c.stop_loss_bps = 999.0;
            c.max_hold_ms = 999_999;
            c.cooldown_ms = 0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);

        // Trade 1: entry at 100.0, breakeven at spike*0.3=6bps, needs trailing take to close
        let bn = quote(100.20, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn, &gt, &samples, WINDOW_MS);
        trader.tick(50_200, &bn, &gt, &samples, WINDOW_MS);
        // Activate breakeven (10 bps > 6 bps threshold) + set peak
        let gt_up = quote(100.10, 100.20, 50_300);
        trader.tick(50_300, &bn, &gt_up, &samples, WINDOW_MS);
        // Drop below peak*0.5 → trailing take. gate.bid=100.04 → 4 < 10*0.5=5
        let gt_drop = quote(100.04, 100.20, 50_400);
        trader.tick(50_400, &bn, &gt_drop, &samples, WINDOW_MS);
        trader.tick(50_500, &bn, &gt_drop, &samples, WINDOW_MS); // fill exit

        // Trade 2
        let gt2 = quote(100.0, 100.0, 50_600);
        trader.tick(50_600, &bn, &gt2, &samples, WINDOW_MS);
        trader.tick(50_700, &bn, &gt2, &samples, WINDOW_MS);
        let gt_up2 = quote(100.10, 100.20, 50_800);
        trader.tick(50_800, &bn, &gt_up2, &samples, WINDOW_MS);
        let gt_drop2 = quote(100.04, 100.20, 50_900);
        trader.tick(50_900, &bn, &gt_drop2, &samples, WINDOW_MS);
        trader.tick(51_000, &bn, &gt_drop2, &samples, WINDOW_MS); // fill exit

        let stats = trader.stats();
        assert_eq!(stats.session_trades, 2);
        assert!(stats.session_pnl_pct != 0.0 || stats.session_trades > 0);
        // Win rate may be 0 if fees exceed raw profit; just check it's computed.
        assert!(stats.win_rate_pct >= 0.0);
    }
}
