//! ShadowFleet — runs N shadow traders with different configs on shared price samples.
//!
//! Each trader gets the same `&PriceSamples` (zero-copy), only trading state differs.
//! Completed trades are collected and can be drained for persistence.
//! Auto-prunes configs with negative expectancy after enough trades.

use serde::Serialize;
use std::collections::VecDeque;

use super::price_samples::PriceSamples;
use super::shadow_trader::{ClosedTrade, ShadowTrader};
use super::state::Quote;
use super::trader_config::TraderConfig;

/// Parameter grid values for fleet generation.
/// Exit model: breakeven at spike × target_ratio, then trailing take-profit.
/// Pre-breakeven: stop-loss only. Post-breakeven: stop at entry + trail peak.
const GAP_THRESHOLDS: &[f64] = &[30.0, 50.0, 60.0, 80.0];
const TARGET_RATIOS: &[f64] = &[0.3, 0.5, 0.7];
const STOP_LOSSES: &[f64] = &[8.0, 15.0, 25.0, 40.0];
const MAX_HOLDS: &[i64] = &[5_000, 10_000, 30_000];
const MAX_SPREADS: &[f64] = &[3.0, 5.0];
const TRAILING_TAKES: &[f64] = &[0.3, 0.7];
const BASELINE_WINDOWS: &[i64] = &[10_000, 20_000, 30_000, 60_000];

/// Min trades before a config can be pruned for poor performance.
const PRUNE_MIN_TRADES: usize = 30;
/// Expectancy threshold (avg PnL %) below which config is disabled.
const PRUNE_EXPECTANCY_THRESHOLD: f64 = -0.05;
/// Time (ms) after which zero-trade configs are pruned as inactive.
const PRUNE_INACTIVE_MS: i64 = 10 * 60 * 1000; // 10 minutes

/// Policy scoring windows.
const POLICY_WINDOW_1H_MS: i64 = 60 * 60 * 1000;
const POLICY_WINDOW_6H_MS: i64 = 6 * 60 * 60 * 1000;
const POLICY_WINDOW_24H_MS: i64 = 24 * 60 * 60 * 1000;

/// Phase-0 frozen scoring weights (Sprint 005).
/// score = w1*avg_pnl_6h + w2*win_rate_6h - w3*stop_loss_share_6h
const SCORE_W_AVG_PNL_6H: f64 = 1.0;
const SCORE_W_WIN_RATE_6H: f64 = 0.20;
const SCORE_W_STOP_LOSS_SHARE_6H: f64 = 0.50;

/// Initial symbol-level gate thresholds (shadow-decision mode).
const POLICY_MIN_TRADES_6H: f64 = 5.0;
const POLICY_MIN_EXPECTANCY_6H: f64 = 0.0;
const POLICY_MAX_STOP_LOSS_SHARE_6H_PCT: f64 = 55.0;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct PolicyWindowMetrics {
    pub trades: f64,
    pub wins: f64,
    pub stop_loss_trades: f64,
    pub avg_pnl_pct: f64,
    pub win_rate_pct: f64,
    pub stop_loss_share_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PolicyConfigSnapshot {
    pub config_id: u64,
    pub score: f64,
    pub gate_enabled: bool,
    pub gate_reason: &'static str,
    pub metrics_1h: PolicyWindowMetrics,
    pub metrics_6h: PolicyWindowMetrics,
    pub metrics_24h: PolicyWindowMetrics,
}

#[derive(Debug, Clone)]
struct DecayedWindow {
    horizon_ms: i64,
    last_ts_ms: Option<i64>,
    trades: f64,
    wins: f64,
    stop_loss_trades: f64,
    pnl_sum_pct: f64,
}

impl DecayedWindow {
    fn new(horizon_ms: i64) -> Self {
        Self {
            horizon_ms,
            last_ts_ms: None,
            trades: 0.0,
            wins: 0.0,
            stop_loss_trades: 0.0,
            pnl_sum_pct: 0.0,
        }
    }

    fn decay_to(&mut self, ts_ms: i64) {
        let Some(last_ts_ms) = self.last_ts_ms else {
            self.last_ts_ms = Some(ts_ms);
            return;
        };
        if ts_ms <= last_ts_ms {
            return;
        }
        let dt_ms = (ts_ms - last_ts_ms) as f64;
        let decay = (-(dt_ms / self.horizon_ms as f64)).exp();
        self.trades *= decay;
        self.wins *= decay;
        self.stop_loss_trades *= decay;
        self.pnl_sum_pct *= decay;
        self.last_ts_ms = Some(ts_ms);
    }

    fn observe_trade(&mut self, ts_ms: i64, pnl_pct: f64, is_stop_loss: bool) {
        self.decay_to(ts_ms);
        self.trades += 1.0;
        if pnl_pct > 0.0 {
            self.wins += 1.0;
        }
        if is_stop_loss {
            self.stop_loss_trades += 1.0;
        }
        self.pnl_sum_pct += pnl_pct;
    }

    fn metrics(&self) -> PolicyWindowMetrics {
        if self.trades <= 1e-9 {
            return PolicyWindowMetrics {
                trades: 0.0,
                wins: 0.0,
                stop_loss_trades: 0.0,
                avg_pnl_pct: 0.0,
                win_rate_pct: 0.0,
                stop_loss_share_pct: 0.0,
            };
        }
        let win_rate_pct = (self.wins / self.trades) * 100.0;
        let stop_loss_share_pct = (self.stop_loss_trades / self.trades) * 100.0;
        PolicyWindowMetrics {
            trades: self.trades,
            wins: self.wins,
            stop_loss_trades: self.stop_loss_trades,
            avg_pnl_pct: self.pnl_sum_pct / self.trades,
            win_rate_pct,
            stop_loss_share_pct,
        }
    }
}

#[derive(Debug, Clone)]
struct ConfigPolicyState {
    window_1h: DecayedWindow,
    window_6h: DecayedWindow,
    window_24h: DecayedWindow,
}

impl ConfigPolicyState {
    fn new() -> Self {
        Self {
            window_1h: DecayedWindow::new(POLICY_WINDOW_1H_MS),
            window_6h: DecayedWindow::new(POLICY_WINDOW_6H_MS),
            window_24h: DecayedWindow::new(POLICY_WINDOW_24H_MS),
        }
    }

    fn observe_trade(&mut self, trade: &ClosedTrade) {
        let is_stop_loss = trade.exit_reason == "stop_loss";
        self.window_1h
            .observe_trade(trade.ts_ms, trade.pnl_pct, is_stop_loss);
        self.window_6h
            .observe_trade(trade.ts_ms, trade.pnl_pct, is_stop_loss);
        self.window_24h
            .observe_trade(trade.ts_ms, trade.pnl_pct, is_stop_loss);
    }

    fn score_and_gate(
        &self,
    ) -> (
        f64,
        bool,
        &'static str,
        PolicyWindowMetrics,
        PolicyWindowMetrics,
        PolicyWindowMetrics,
    ) {
        let metrics_1h = self.window_1h.metrics();
        let metrics_6h = self.window_6h.metrics();
        let metrics_24h = self.window_24h.metrics();

        let score = SCORE_W_AVG_PNL_6H * metrics_6h.avg_pnl_pct
            + SCORE_W_WIN_RATE_6H * (metrics_6h.win_rate_pct / 100.0)
            - SCORE_W_STOP_LOSS_SHARE_6H * (metrics_6h.stop_loss_share_pct / 100.0);

        if metrics_6h.trades < POLICY_MIN_TRADES_6H {
            return (
                score,
                false,
                "min_trades",
                metrics_1h,
                metrics_6h,
                metrics_24h,
            );
        }
        if metrics_6h.avg_pnl_pct <= POLICY_MIN_EXPECTANCY_6H {
            return (
                score,
                false,
                "expectancy",
                metrics_1h,
                metrics_6h,
                metrics_24h,
            );
        }
        if metrics_6h.stop_loss_share_pct > POLICY_MAX_STOP_LOSS_SHARE_6H_PCT {
            return (
                score,
                false,
                "stop_loss_share",
                metrics_1h,
                metrics_6h,
                metrics_24h,
            );
        }
        (score, true, "ok", metrics_1h, metrics_6h, metrics_24h)
    }

    fn snapshot(&self, config_id: u64) -> PolicyConfigSnapshot {
        let (score, gate_enabled, gate_reason, metrics_1h, metrics_6h, metrics_24h) =
            self.score_and_gate();
        PolicyConfigSnapshot {
            config_id,
            score,
            gate_enabled,
            gate_reason,
            metrics_1h,
            metrics_6h,
            metrics_24h,
        }
    }
}

/// Generate all parameter combinations from the grid.
pub fn generate_grid() -> Vec<TraderConfig> {
    let cap = GAP_THRESHOLDS.len()
        * TARGET_RATIOS.len()
        * STOP_LOSSES.len()
        * MAX_HOLDS.len()
        * MAX_SPREADS.len()
        * TRAILING_TAKES.len()
        * BASELINE_WINDOWS.len();
    let mut configs = Vec::with_capacity(cap);
    let base = TraderConfig::default();

    for &gap in GAP_THRESHOLDS {
        for &target in TARGET_RATIOS {
            for &stop in STOP_LOSSES {
                for &hold in MAX_HOLDS {
                    for &spread in MAX_SPREADS {
                        for &trailing in TRAILING_TAKES {
                            for &bw in BASELINE_WINDOWS {
                                configs.push(TraderConfig {
                                    spike_threshold_bps: gap,
                                    target_ratio: target,
                                    stop_loss_bps: stop,
                                    max_hold_ms: hold,
                                    max_spread_bps: spread,
                                    trailing_decay_ratio: trailing,
                                    baseline_window_ms: bw,
                                    ..base
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    configs
}

/// A completed trade tagged with its config ID and symbol for persistence.
#[derive(Debug, Clone)]
pub struct FleetTrade {
    pub config_id: u64,
    pub symbol: String,
    pub trade: ClosedTrade,
}

/// Fleet of shadow traders for one symbol.
/// Holds N traders with different configs, ticks all on shared samples.
/// Auto-prunes configs with negative expectancy or zero trades after warmup.
#[derive(Debug)]
pub struct ShadowFleet {
    traders: Vec<(u64, ShadowTrader)>,
    /// Rolling policy state per config (same index as `traders`).
    policy: Vec<ConfigPolicyState>,
    /// Trades pending drain to persistence layer.
    pending_trades: VecDeque<FleetTrade>,
    /// Monotonic session trade count per trader — never decreases.
    last_session_trades: Vec<usize>,
    /// Disabled flags — pruned configs skip ticking.
    disabled: Vec<bool>,
    /// Number of active (non-pruned) traders.
    active_count: usize,
    /// Timestamp of first tick (for inactive pruning).
    first_tick_ms: Option<i64>,
}

impl ShadowFleet {
    pub fn new(configs: &[TraderConfig]) -> Self {
        let n = configs.len();
        let traders: Vec<(u64, ShadowTrader)> = configs
            .iter()
            .map(|c| (c.config_id(), ShadowTrader::new(*c)))
            .collect();
        let last_session_trades = vec![0; n];
        let policy = vec![ConfigPolicyState::new(); n];
        Self {
            traders,
            policy,
            pending_trades: VecDeque::new(),
            last_session_trades,
            disabled: vec![false; n],
            active_count: n,
            first_tick_ms: None,
        }
    }

    /// Total number of traders in the fleet (including pruned).
    #[inline]
    pub fn len(&self) -> usize {
        self.traders.len()
    }

    /// Returns true if fleet has no traders.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.traders.is_empty()
    }

    /// Number of active (non-pruned) traders.
    #[inline]
    pub fn active(&self) -> usize {
        self.active_count
    }

    /// Tick all active traders with shared price data.
    pub fn tick_all(
        &mut self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
        window_ms: i64,
        symbol: &str,
    ) {
        let first = *self.first_tick_ms.get_or_insert(ts_ms);
        let elapsed = ts_ms - first;

        for (idx, (config_id, trader)) in self.traders.iter_mut().enumerate() {
            if self.disabled[idx] {
                continue;
            }

            trader.tick(ts_ms, binance, gate, samples, window_ms);

            let session_n = trader.session_trades();
            let prev_n = self.last_session_trades[idx];

            if session_n > prev_n {
                let new_count = session_n - prev_n;
                let deque = trader.completed_trades();
                let start = deque.len().saturating_sub(new_count);
                let sym = symbol.to_string();
                for trade in deque.iter().skip(start) {
                    self.policy[idx].observe_trade(trade);
                    self.pending_trades.push_back(FleetTrade {
                        config_id: *config_id,
                        symbol: sym.clone(),
                        trade: trade.clone(),
                    });
                }
                self.last_session_trades[idx] = session_n;

                // Prune configs with negative expectancy after enough data.
                if session_n >= PRUNE_MIN_TRADES {
                    let avg_pnl = trader.session_pnl_pct() / session_n as f64;
                    if avg_pnl < PRUNE_EXPECTANCY_THRESHOLD {
                        self.disabled[idx] = true;
                        self.active_count -= 1;
                        tracing::debug!(
                            config_id = *config_id,
                            symbol,
                            trades = session_n,
                            avg_pnl_pct = format!("{avg_pnl:.4}"),
                            "fleet: pruned (negative expectancy)"
                        );
                    }
                }
            } else if session_n == 0 && elapsed >= PRUNE_INACTIVE_MS {
                // Prune configs that never traded after warmup period.
                self.disabled[idx] = true;
                self.active_count -= 1;
                tracing::debug!(
                    config_id = *config_id,
                    symbol,
                    elapsed_min = elapsed / 60_000,
                    "fleet: pruned (zero trades)"
                );
            }
        }
    }

    /// Drain pending trades for persistence (returns and clears buffer).
    pub fn drain_trades(&mut self) -> Vec<FleetTrade> {
        self.pending_trades.drain(..).collect()
    }

    /// Policy diagnostics snapshot per config (shadow decision mode).
    pub fn policy_snapshots(&self) -> Vec<PolicyConfigSnapshot> {
        self.traders
            .iter()
            .enumerate()
            .map(|(idx, (config_id, _))| self.policy[idx].snapshot(*config_id))
            .collect()
    }

    /// Return top-K configs by policy score among gate-enabled configs.
    pub fn top_policy_configs(&self, k: usize) -> Vec<PolicyConfigSnapshot> {
        let mut rows = self.policy_snapshots();
        rows.retain(|r| r.gate_enabled);
        rows.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        rows.truncate(k);
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::screener::shadow_trader::Direction;

    fn approx_eq(left: f64, right: f64, eps: f64) {
        assert!(
            (left - right).abs() <= eps,
            "expected {left} ~= {right} (eps={eps})"
        );
    }

    #[test]
    fn grid_size() {
        let grid = generate_grid();
        // 4 gaps × 3 targets × 4 SLs × 3 holds × 2 spreads × 2 trailing × 4 baseline = 2304
        assert_eq!(grid.len(), 2304);
    }

    #[test]
    fn config_ids_unique() {
        let grid = generate_grid();
        let mut ids: Vec<u64> = grid.iter().map(|c| c.config_id()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), grid.len(), "all config_ids must be unique");
    }

    #[test]
    fn fleet_creation() {
        let configs = generate_grid();
        let fleet = ShadowFleet::new(&configs);
        assert_eq!(fleet.len(), 2304);
        assert_eq!(fleet.active(), 2304);
    }

    #[test]
    fn policy_state_gate_needs_minimum_trades() {
        let mut state = ConfigPolicyState::new();
        for _ in 0..4 {
            state.observe_trade(&ClosedTrade {
                pnl_pct: 0.3,
                ts_ms: 1_000_000,
                direction: Direction::Long,
                entry_ts_ms: 999_500,
                entry_price: 100.0,
                exit_price: 100.3,
                exit_reason: "trailing_take",
                spike_bps: 50.0,
                catchup_pct: 0.3,
                catchup_ms: 500,
                gate_spread_at_entry_bps: 1.0,
            });
        }
        let snapshot = state.snapshot(42);
        assert!(!snapshot.gate_enabled);
        assert_eq!(snapshot.gate_reason, "min_trades");
        approx_eq(snapshot.metrics_6h.trades, 4.0, 1e-9);
    }

    #[test]
    fn policy_state_scores_and_enables_on_positive_profile() {
        let mut state = ConfigPolicyState::new();
        for idx in 0..10 {
            let pnl = if idx < 7 { 0.20 } else { -0.05 };
            let reason = if idx < 2 {
                "stop_loss"
            } else {
                "trailing_take"
            };
            state.observe_trade(&ClosedTrade {
                pnl_pct: pnl,
                ts_ms: 2_000_000,
                direction: Direction::Long,
                entry_ts_ms: 1_999_700,
                entry_price: 100.0,
                exit_price: 100.1,
                exit_reason: reason,
                spike_bps: 50.0,
                catchup_pct: 0.2,
                catchup_ms: 300,
                gate_spread_at_entry_bps: 1.0,
            });
        }
        let snapshot = state.snapshot(7);
        assert!(snapshot.gate_enabled);
        assert_eq!(snapshot.gate_reason, "ok");
        approx_eq(snapshot.metrics_6h.trades, 10.0, 1e-9);
        approx_eq(snapshot.metrics_6h.win_rate_pct, 70.0, 1e-9);
        approx_eq(snapshot.metrics_6h.stop_loss_share_pct, 20.0, 1e-9);
        assert!(snapshot.score > 0.0);
    }

    #[test]
    fn decayed_window_fades_old_observations() {
        let mut window = DecayedWindow::new(POLICY_WINDOW_6H_MS);
        window.observe_trade(10_000, 1.0, false);
        window.decay_to(10_000 + POLICY_WINDOW_6H_MS);
        approx_eq(window.trades, std::f64::consts::E.recip(), 1e-6);
        approx_eq(window.pnl_sum_pct, std::f64::consts::E.recip(), 1e-6);
    }

    #[test]
    fn top_policy_configs_returns_gate_enabled_sorted() {
        let cfg_a = TraderConfig {
            spike_threshold_bps: 40.0,
            ..TraderConfig::default()
        };
        let cfg_b = TraderConfig {
            spike_threshold_bps: 80.0,
            ..TraderConfig::default()
        };
        let mut fleet = ShadowFleet::new(&[cfg_a, cfg_b]);

        // Seed policy directly (unit-level, deterministic).
        for _ in 0..10 {
            fleet.policy[0].observe_trade(&ClosedTrade {
                pnl_pct: 0.20,
                ts_ms: 5_000_000,
                direction: Direction::Long,
                entry_ts_ms: 4_999_700,
                entry_price: 100.0,
                exit_price: 100.2,
                exit_reason: "trailing_take",
                spike_bps: 50.0,
                catchup_pct: 0.2,
                catchup_ms: 300,
                gate_spread_at_entry_bps: 1.0,
            });
            fleet.policy[1].observe_trade(&ClosedTrade {
                pnl_pct: -0.20,
                ts_ms: 5_000_000,
                direction: Direction::Long,
                entry_ts_ms: 4_999_700,
                entry_price: 100.0,
                exit_price: 99.8,
                exit_reason: "stop_loss",
                spike_bps: 50.0,
                catchup_pct: -0.2,
                catchup_ms: 300,
                gate_spread_at_entry_bps: 1.0,
            });
        }

        let top = fleet.top_policy_configs(5);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].config_id, cfg_a.config_id());
    }
}
