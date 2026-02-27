//! ShadowTrader — paper-trading spike-follow model.
//!
//! Strategy: Binance leads, Gate lags. When Binance ask spikes up (for longs)
//! or Binance bid drops (for shorts) ≥ threshold in a short window, enter on
//! Gate in the same direction. Exit when Gate catches up (target), on timeout,
//! or stop-loss.

use serde::Serialize;
use std::collections::VecDeque;

use super::price_samples::PriceSamples;
use super::state::Quote;
use super::trader_config::TraderConfig;

/// stop_loss exits closed within this time budget are treated as early-stop churn.
pub const EARLY_STOP_CHURN_HOLD_MS: i64 = 500;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Direction {
    Short,
    Long,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    direction: Direction,
    gate_entry_price: f64,
    entry_ts_ms: i64,
    spike_bps: f64,
    gate_spread_at_entry_bps: f64,
    gate_natr_30m_pct_at_entry: f64,
    run_id: Option<String>,
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
    pub gate_natr_30m_pct_at_entry: f64,
    pub hold_ms: i64,
    pub early_stop_churn: bool,
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
        gate_natr_30m_pct_at_entry: f64,
        run_id: Option<String>,
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
    completed_trade_run_ids: VecDeque<Option<String>>,
    session_total_pnl_pct: f64,
    session_trades: usize,
    session_wins: usize,
    spike_timestamps: VecDeque<i64>,
    start_ts_ms: Option<i64>,
    latest_ts_ms: i64,
    cooldown_until_ms: i64,
}

impl Default for ShadowTrader {
    fn default() -> Self {
        Self::new(TraderConfig::default())
    }
}

impl ShadowTrader {
    pub fn new(config: TraderConfig) -> Self {
        Self {
            config,
            position: None,
            pending: None,
            completed_trades: VecDeque::new(),
            completed_trade_run_ids: VecDeque::new(),
            session_total_pnl_pct: 0.0,
            session_trades: 0,
            session_wins: 0,
            spike_timestamps: VecDeque::new(),
            start_ts_ms: None,
            latest_ts_ms: 0,
            cooldown_until_ms: 0,
        }
    }

    pub fn config(&self) -> &TraderConfig {
        &self.config
    }

    pub fn completed_trades(&self) -> &VecDeque<ClosedTrade> {
        &self.completed_trades
    }

    pub fn completed_trade_run_ids(&self) -> &VecDeque<Option<String>> {
        &self.completed_trade_run_ids
    }

    pub fn session_trades(&self) -> usize {
        self.session_trades
    }

    pub fn session_pnl_pct(&self) -> f64 {
        self.session_total_pnl_pct
    }

    // -- Core tick -----------------------------------------------------------

    pub fn tick(
        &mut self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
        window_ms: i64,
    ) {
        self.tick_with_context(ts_ms, binance, gate, samples, window_ms, 0.0, None);
    }

    pub fn tick_with_context(
        &mut self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
        window_ms: i64,
        gate_natr_30m_pct_at_entry: f64,
        run_id: Option<&str>,
    ) {
        if self.start_ts_ms.is_none() {
            self.start_ts_ms = Some(ts_ms);
        }
        self.latest_ts_ms = ts_ms;
        self.cleanup_spikes(ts_ms);

        let quote_freshness_ms = self.config.quote_freshness_ms as u64;
        let warmup_ms = self.config.warmup_ms;
        let binance_fresh = (ts_ms - binance.ts_ms).unsigned_abs() <= quote_freshness_ms;
        let gate_fresh = (ts_ms - gate.ts_ms).unsigned_abs() <= quote_freshness_ms;

        // Exit lifecycle is gate-side only; stale opposite side must not freeze fills/timeouts.
        if gate_fresh {
            self.try_fill(ts_ms, gate, window_ms);
            if self.pending.is_some() {
                return;
            }
            self.try_exit(ts_ms, gate);
            if self.pending.is_some() {
                return;
            }
        }

        if !binance_fresh || !gate_fresh {
            return;
        }
        let elapsed = ts_ms.saturating_sub(self.start_ts_ms.unwrap_or(ts_ms));
        if elapsed < warmup_ms {
            return;
        }
        self.try_entry(
            ts_ms,
            binance,
            gate,
            samples,
            gate_natr_30m_pct_at_entry,
            run_id,
        );
    }

    // -- Fill pending orders -------------------------------------------------

    fn try_fill(&mut self, ts_ms: i64, gate: &Quote, window_ms: i64) {
        let Some(pending) = self.pending.take() else {
            return;
        };
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
            PendingOrder::Entry {
                direction,
                spike_bps,
                gate_spread_at_entry_bps,
                gate_natr_30m_pct_at_entry,
                run_id,
                ..
            } => {
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
                    gate_natr_30m_pct_at_entry,
                    run_id,
                    peak_unrealized_bps: 0.0,
                    breakeven_activated: false,
                });
            }
        }
    }

    // -- Exit logic ----------------------------------------------------------

    fn unrealized_bps(pos: &OpenPosition, gate: &Quote) -> f64 {
        match pos.direction {
            Direction::Long => {
                ((gate.bid - pos.gate_entry_price) / pos.gate_entry_price) * 10_000.0
            }
            Direction::Short => {
                ((pos.gate_entry_price - gate.ask) / pos.gate_entry_price) * 10_000.0
            }
        }
    }

    fn determine_exit_reason(
        cfg: &TraderConfig,
        pos: &OpenPosition,
        unrealized_bps: f64,
        hold_ms: i64,
    ) -> Option<&'static str> {
        let timed_out = hold_ms >= cfg.max_hold_ms;
        if pos.breakeven_activated {
            if unrealized_bps <= 0.0 {
                Some("breakeven")
            } else if unrealized_bps <= pos.peak_unrealized_bps * cfg.trailing_decay_ratio {
                Some("trailing_take")
            } else if timed_out {
                Some("timeout")
            } else {
                None
            }
        } else if unrealized_bps <= -cfg.stop_loss_bps {
            Some("stop_loss")
        } else if timed_out {
            Some("timeout")
        } else {
            None
        }
    }

    fn try_exit(&mut self, ts_ms: i64, gate: &Quote) {
        let Some(pos) = self.position.as_mut() else {
            return;
        };
        let hold_ms = ts_ms - pos.entry_ts_ms;

        let unrealized_bps = Self::unrealized_bps(pos, gate);
        if unrealized_bps > pos.peak_unrealized_bps {
            pos.peak_unrealized_bps = unrealized_bps;
        }

        let breakeven_threshold = pos.spike_bps * self.config.target_ratio;
        if !pos.breakeven_activated && unrealized_bps >= breakeven_threshold {
            pos.breakeven_activated = true;
        }

        if let Some(reason) =
            Self::determine_exit_reason(&self.config, pos, unrealized_bps, hold_ms)
        {
            let pos = self.position.take().unwrap();
            self.pending = Some(PendingOrder::Exit {
                fire_ts_ms: ts_ms,
                pos,
                reason,
            });
        }
    }

    // -- Entry logic ---------------------------------------------------------

    fn try_entry(
        &mut self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
        gate_natr_30m_pct_at_entry: f64,
        run_id: Option<&str>,
    ) {
        if self.position.is_some() || self.pending.is_some() || ts_ms < self.cooldown_until_ms {
            return;
        }
        let cfg = &self.config;

        // Spread filter
        if cfg.max_spread_bps > 0.0 {
            let gate_mid = (gate.bid + gate.ask) * 0.5;
            if gate_mid > 0.0 {
                let spread_bps = ((gate.ask - gate.bid) / gate_mid) * 10_000.0;
                if spread_bps > cfg.max_spread_bps {
                    return;
                }
            }
        }

        if let Some((direction, gap_bps)) = self.detect_gap(ts_ms, binance, gate, samples) {
            self.spike_timestamps.push_back(ts_ms);
            let gate_mid = (gate.bid + gate.ask) * 0.5;
            let spread_bps = if gate_mid > 0.0 {
                ((gate.ask - gate.bid) / gate_mid) * 10_000.0
            } else {
                0.0
            };
            self.pending = Some(PendingOrder::Entry {
                direction,
                fire_ts_ms: ts_ms,
                spike_bps: gap_bps,
                gate_spread_at_entry_bps: spread_bps,
                gate_natr_30m_pct_at_entry,
                run_id: run_id.map(|value| value.to_string()),
            });
        }
    }

    // -- Gap detection (lead-lag) --------------------------------------------

    /// Lead-lag with baseline: enter when current gap EXCEEDS the average gap.
    /// Baseline = mean(binance - gate) over last `baseline_window_ms`.
    /// Signal = current_gap - baseline_gap. Enter when signal > threshold.
    fn detect_gap(
        &self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
    ) -> Option<(Direction, f64)> {
        let cutoff = ts_ms - self.config.baseline_window_ms;

        // Compute baseline gap over window (not all samples)
        let (mut ask_gap_sum, mut bid_gap_sum) = (0.0_f64, 0.0_f64);
        let (mut ask_count, mut bid_count) = (0_u32, 0_u32);
        for s in samples.iter() {
            if s.ts_ms < cutoff || s.ts_ms >= ts_ms {
                continue;
            }
            if s.gate_ask > 0.0 && s.binance_ask > 0.0 {
                ask_gap_sum += ((s.binance_ask - s.gate_ask) / s.gate_ask) * 10_000.0;
                ask_count += 1;
            }
            if s.gate_bid > 0.0 && s.binance_bid > 0.0 {
                bid_gap_sum += ((s.gate_bid - s.binance_bid) / s.gate_bid) * 10_000.0;
                bid_count += 1;
            }
        }
        if ask_count == 0 && bid_count == 0 {
            return None;
        }
        let min_baseline_samples = self.config.min_baseline_samples as u32;
        let ask_ready = ask_count >= min_baseline_samples;
        let bid_ready = bid_count >= min_baseline_samples;
        if !ask_ready && !bid_ready {
            return None;
        }
        let baseline_ask_gap = if ask_ready {
            ask_gap_sum / ask_count as f64
        } else {
            0.0
        };
        let baseline_bid_gap = if bid_ready {
            bid_gap_sum / bid_count as f64
        } else {
            0.0
        };

        let threshold = self.config.spike_threshold_bps;

        let mut long_signal = None;
        if ask_ready && gate.ask > 0.0 {
            let current_gap = ((binance.ask - gate.ask) / gate.ask) * 10_000.0;
            let signal = current_gap - baseline_ask_gap;
            if signal >= threshold {
                long_signal = Some(signal);
            }
        }

        let mut short_signal = None;
        if bid_ready && gate.bid > 0.0 {
            let current_gap = ((gate.bid - binance.bid) / gate.bid) * 10_000.0;
            let signal = current_gap - baseline_bid_gap;
            if signal >= threshold {
                short_signal = Some(signal);
            }
        }

        match (long_signal, short_signal) {
            (Some(long_bps), Some(short_bps)) => {
                if short_bps > long_bps {
                    Some((Direction::Short, short_bps))
                } else {
                    Some((Direction::Long, long_bps))
                }
            }
            (Some(long_bps), None) => Some((Direction::Long, long_bps)),
            (None, Some(short_bps)) => Some((Direction::Short, short_bps)),
            (None, None) => None,
        }
    }

    // -- Fill exit & bookkeeping ---------------------------------------------

    fn fill_exit(
        &mut self,
        ts_ms: i64,
        gate: &Quote,
        window_ms: i64,
        pos: OpenPosition,
        exit_reason: &'static str,
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
        let hold_ms = ts_ms.saturating_sub(pos.entry_ts_ms);
        let early_stop_churn = exit_reason == "stop_loss" && hold_ms <= EARLY_STOP_CHURN_HOLD_MS;

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
            gate_spread_at_entry_bps: pos.gate_spread_at_entry_bps,
            gate_natr_30m_pct_at_entry: pos.gate_natr_30m_pct_at_entry,
            hold_ms,
            early_stop_churn,
        });
        self.completed_trade_run_ids.push_back(pos.run_id.clone());
        self.session_total_pnl_pct += pnl_pct;
        self.session_trades += 1;
        if pnl_pct > 0.0 {
            self.session_wins += 1;
        }
        self.cooldown_until_ms = ts_ms + self.config.cooldown_ms;

        let cutoff = ts_ms - window_ms;
        while let Some(t) = self.completed_trades.front() {
            if t.ts_ms >= cutoff {
                break;
            }
            self.completed_trades.pop_front();
            self.completed_trade_run_ids.pop_front();
        }
    }

    // -- Read models ---------------------------------------------------------

    pub fn stats(&self) -> ShadowStats {
        let window_n = self.completed_trades.len();
        if self.session_trades == 0 {
            return ShadowStats {
                session_pnl_pct: 0.0,
                session_trades: 0,
                avg_trade_pct: 0.0,
                win_rate_pct: 0.0,
                position: self.position_label(),
                spikes_detected: self.spike_timestamps.len(),
                avg_catchup_pct: 0.0,
                avg_catchup_lag_ms: 0.0,
            };
        }
        let avg_catchup = if window_n > 0 {
            self.completed_trades
                .iter()
                .map(|t| t.catchup_pct)
                .sum::<f64>()
                / window_n as f64
        } else {
            0.0
        };
        let avg_lag = if window_n > 0 {
            self.completed_trades
                .iter()
                .map(|t| t.catchup_ms as f64)
                .sum::<f64>()
                / window_n as f64
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

    fn cleanup_spikes(&mut self, ts_ms: i64) {
        let cutoff = ts_ms - 2 * 60 * 1000;
        while let Some(&spike_ts) = self.spike_timestamps.front() {
            if spike_ts >= cutoff {
                break;
            }
            self.spike_timestamps.pop_front();
        }
    }

    pub fn debug(&self, samples: &PriceSamples) -> ShadowDebug {
        let elapsed = self
            .start_ts_ms
            .map(|s| self.latest_ts_ms.saturating_sub(s))
            .unwrap_or(0);
        let last_5: Vec<f64> = self
            .completed_trades
            .iter()
            .rev()
            .take(5)
            .map(|t| t.pnl_pct)
            .collect();
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
        let trades: Vec<ChartTrade> = self
            .completed_trades
            .iter()
            .map(|t| ChartTrade {
                entry_ts_ms: t.entry_ts_ms,
                exit_ts_ms: t.ts_ms,
                direction: t.direction_str(),
                pnl_pct: t.pnl_pct,
                exit_reason: t.exit_reason,
                spike_bps: t.spike_bps,
                catchup_pct: t.catchup_pct,
                entry_price: t.entry_price,
                exit_price: t.exit_price,
            })
            .collect();
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::price_samples::{PriceSample, PriceSamples};
    use super::*;

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
                gate_bid: gate,
                gate_ask: gate,
                binance_bid: binance,
                binance_ask: binance,
            });
        }
        ps
    }

    // -- Baseline & entry signal ------------------------------------------------

    #[test]
    fn baseline_needs_min_samples() {
        let trader = make_trader(|c| {
            c.min_baseline_samples = 20;
        });
        let samples = stable_samples(19, 100.0, 100.0, 50_000);
        let bn = quote(100.0, 100.0, 50_000);
        let gt = quote(100.0, 100.0, 50_000);
        assert!(trader.detect_gap(50_100, &bn, &gt, &samples).is_none());
    }

    #[test]
    fn baseline_needs_min_samples_inside_window() {
        let trader = make_trader(|c| {
            c.spike_threshold_bps = 50.0;
            c.min_baseline_samples = 5;
            c.baseline_window_ms = 500;
            c.warmup_ms = 0;
        });
        let mut samples = PriceSamples::default();
        // Old baseline points (outside active window) should not satisfy min sample gate.
        for i in 0..8 {
            samples.push(PriceSample {
                ts_ms: 49_000 + i * 10,
                gate_bid: 100.0,
                gate_ask: 100.0,
                binance_bid: 100.0,
                binance_ask: 100.0,
            });
        }
        // Only 2 points in active window.
        for i in 0..2 {
            samples.push(PriceSample {
                ts_ms: 49_900 + i * 100,
                gate_bid: 100.0,
                gate_ask: 100.0,
                binance_bid: 100.0,
                binance_ask: 100.0,
            });
        }
        let bn = quote(100.60, 100.60, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        assert!(trader.detect_gap(50_100, &bn, &gt, &samples).is_none());
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
        let result = trader.detect_gap(50_100, &bn, &gt, &samples);
        assert!(result.is_some());
        let (dir, gap_bps) = result.unwrap();
        assert_eq!(dir, Direction::Long);
        assert!(
            (gap_bps - 60.0).abs() < 1.0,
            "expected ~60 bps, got {gap_bps}"
        );
    }

    #[test]
    fn entry_signal_prefers_stronger_direction_when_both_sides_trigger() {
        let trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.warmup_ms = 0;
            c.min_baseline_samples = 5;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);
        // Long signal: +20 bps (ask branch), short signal: +40 bps (bid branch).
        let bn = quote(99.60, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);

        let result = trader.detect_gap(50_100, &bn, &gt, &samples);
        assert!(result.is_some());
        let (dir, gap_bps) = result.unwrap();
        assert_eq!(dir, Direction::Short);
        assert!(
            (gap_bps - 40.0).abs() < 1.0,
            "expected ~40 bps short signal, got {gap_bps}"
        );
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
        assert!(trader.detect_gap(50_100, &bn, &gt, &samples).is_none());
    }

    #[test]
    fn baseline_ask_uses_only_valid_ask_samples() {
        let trader = make_trader(|c| {
            c.spike_threshold_bps = 9.0;
            c.min_baseline_samples = 2;
            c.baseline_window_ms = 10_000;
        });

        let mut samples = PriceSamples::default();
        // Valid ask gap sample: 20 bps.
        samples.push(PriceSample {
            ts_ms: 50_000,
            gate_bid: 0.0,
            gate_ask: 100.0,
            binance_bid: 0.0,
            binance_ask: 100.20,
        });
        // Invalid ask sample (gate ask missing): should not affect ask baseline denominator.
        samples.push(PriceSample {
            ts_ms: 50_050,
            gate_bid: 100.0,
            gate_ask: 0.0,
            binance_bid: 100.0,
            binance_ask: 100.20,
        });

        let bn = quote(100.20, 100.20, 50_100);
        let gt = quote(100.0, 100.0, 50_100);

        // If denominator incorrectly uses all samples (2), signal=10 bps and test fails.
        // Correct denominator for ask baseline is 1 valid ask sample => signal=0 bps.
        assert!(trader.detect_gap(50_100, &bn, &gt, &samples).is_none());
    }

    #[test]
    fn baseline_excludes_current_tick_sample() {
        let trader = make_trader(|c| {
            c.spike_threshold_bps = 80.0;
            c.min_baseline_samples = 1;
            c.baseline_window_ms = 10_000;
            c.warmup_ms = 0;
        });

        let mut samples = PriceSamples::default();
        // Historical baseline point: 0 bps.
        samples.push(PriceSample {
            ts_ms: 50_000,
            gate_bid: 100.0,
            gate_ask: 100.0,
            binance_bid: 100.0,
            binance_ask: 100.0,
        });
        // Current tick sample (same ts as decision tick): 100 bps.
        samples.push(PriceSample {
            ts_ms: 50_100,
            gate_bid: 100.0,
            gate_ask: 100.0,
            binance_bid: 101.0,
            binance_ask: 101.0,
        });

        let bn = quote(101.0, 101.0, 50_100);
        let gt = quote(100.0, 100.0, 50_100);
        let result = trader.detect_gap(50_100, &bn, &gt, &samples);
        assert!(
            result.is_some(),
            "current-tick sample must not dilute baseline"
        );
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
        assert!(
            t.pnl_pct.abs() < 0.01,
            "expected ~0% pnl, got {}",
            t.pnl_pct
        );
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
    fn stale_binance_does_not_block_gate_based_timeout_exit() {
        let mut trader = make_trader(|c| {
            c.spike_threshold_bps = 10.0;
            c.warmup_ms = 0;
            c.fill_delay_ms = 0;
            c.cooldown_ms = 0;
            c.min_baseline_samples = 5;
            c.max_spread_bps = 0.0;
            c.quote_freshness_ms = 100;
            c.max_hold_ms = 50;
            c.stop_loss_bps = 999.0;
            c.target_ratio = 99.0;
        });
        let samples = stable_samples(10, 100.0, 100.0, 50_000);

        let bn_fresh = quote(100.20, 100.20, 50_100);
        let gt_entry = quote(100.0, 100.0, 50_100);
        trader.tick(50_100, &bn_fresh, &gt_entry, &samples, WINDOW_MS);
        trader.tick(50_120, &bn_fresh, &gt_entry, &samples, WINDOW_MS); // fill entry
        assert_eq!(trader.position_label(), "LONG_GT");

        // Binance is stale relative to decision ts, but Gate is fresh and should allow timeout exit.
        let bn_stale = quote(100.20, 100.20, 50_100);
        let gt_fresh = quote(100.0, 100.0, 50_260);
        trader.tick(50_260, &bn_stale, &gt_fresh, &samples, WINDOW_MS);
        trader.tick(50_270, &bn_stale, &gt_fresh, &samples, WINDOW_MS); // fill exit

        assert_eq!(trader.position_label(), "FLAT");
        let trades = trader.completed_trades();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].exit_reason, "timeout");
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

    // -- determine_exit_reason unit tests --------------------------------------

    fn make_config(f: impl FnOnce(&mut TraderConfig)) -> TraderConfig {
        let mut cfg = TraderConfig::default();
        f(&mut cfg);
        cfg
    }

    fn make_pos(breakeven: bool, peak_bps: f64, spike_bps: f64) -> OpenPosition {
        OpenPosition {
            direction: Direction::Long,
            spike_bps,
            gate_entry_price: 100.0,
            entry_ts_ms: 0,
            breakeven_activated: breakeven,
            peak_unrealized_bps: peak_bps,
            gate_spread_at_entry_bps: 0.0,
            gate_natr_30m_pct_at_entry: 0.0,
            run_id: None,
        }
    }

    #[test]
    fn exit_reason_stop_loss() {
        let cfg = make_config(|c| {
            c.stop_loss_bps = 5.0;
            c.max_hold_ms = 999_999;
        });
        let pos = make_pos(false, 0.0, 20.0);
        assert_eq!(
            ShadowTrader::determine_exit_reason(&cfg, &pos, -6.0, 100),
            Some("stop_loss")
        );
    }

    #[test]
    fn exit_reason_breakeven_stop() {
        let cfg = make_config(|c| {
            c.stop_loss_bps = 999.0;
            c.max_hold_ms = 999_999;
            c.trailing_decay_ratio = 0.5;
        });
        let pos = make_pos(true, 10.0, 20.0);
        assert_eq!(
            ShadowTrader::determine_exit_reason(&cfg, &pos, -0.5, 100),
            Some("breakeven")
        );
    }

    #[test]
    fn exit_reason_trailing_take() {
        let cfg = make_config(|c| {
            c.stop_loss_bps = 999.0;
            c.max_hold_ms = 999_999;
            c.trailing_decay_ratio = 0.5;
        });
        let pos = make_pos(true, 20.0, 30.0);
        // unrealized 8 bps < peak 20 * 0.5 = 10 → trailing_take
        assert_eq!(
            ShadowTrader::determine_exit_reason(&cfg, &pos, 8.0, 100),
            Some("trailing_take")
        );
    }

    #[test]
    fn exit_reason_timeout_pre_breakeven() {
        let cfg = make_config(|c| {
            c.stop_loss_bps = 999.0;
            c.max_hold_ms = 100;
        });
        let pos = make_pos(false, 5.0, 20.0);
        assert_eq!(
            ShadowTrader::determine_exit_reason(&cfg, &pos, 3.0, 100),
            Some("timeout")
        );
    }

    #[test]
    fn exit_reason_none_when_healthy() {
        let cfg = make_config(|c| {
            c.stop_loss_bps = 50.0;
            c.max_hold_ms = 999_999;
            c.trailing_decay_ratio = 0.5;
        });
        let pos = make_pos(false, 10.0, 20.0);
        assert_eq!(
            ShadowTrader::determine_exit_reason(&cfg, &pos, 5.0, 100),
            None
        );
    }

    #[test]
    fn unrealized_bps_long() {
        let pos = make_pos(false, 0.0, 20.0);
        let gt = quote(100.10, 100.20, 0);
        let bps = ShadowTrader::unrealized_bps(&pos, &gt);
        assert!((bps - 10.0).abs() < 0.01);
    }
}
