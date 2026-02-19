//! ShadowFleet — runs N shadow traders with different configs on shared price samples.
//!
//! Each trader gets the same `&PriceSamples` (zero-copy), only trading state differs.
//! Completed trades are collected and can be drained for persistence.
//! Auto-prunes configs with negative expectancy after enough trades.

use std::collections::VecDeque;

use super::price_samples::PriceSamples;
use super::shadow_trader::{ClosedTrade, ShadowTrader};
use super::state::Quote;
use super::trader_config::TraderConfig;

/// Parameter grid values for fleet generation.
/// Exit model: breakeven at spike × target_ratio, then trailing take-profit.
/// Pre-breakeven: stop-loss only. Post-breakeven: stop at entry + trail peak.
const GAP_THRESHOLDS: &[f64] = &[30.0, 40.0, 50.0, 60.0, 80.0];
const TARGET_RATIOS: &[f64] = &[0.3, 0.5, 0.7];
const STOP_LOSSES: &[f64] = &[8.0, 15.0, 25.0, 40.0];
const MAX_HOLDS: &[i64] = &[5_000, 10_000, 20_000, 30_000];
const MAX_SPREADS: &[f64] = &[3.0, 5.0];
const TRAILING_TAKES: &[f64] = &[0.3, 0.5, 0.7];

/// Min trades before a config can be pruned for poor performance.
const PRUNE_MIN_TRADES: usize = 30;
/// Expectancy threshold (avg PnL %) below which config is disabled.
const PRUNE_EXPECTANCY_THRESHOLD: f64 = -0.05;
/// Time (ms) after which zero-trade configs are pruned as inactive.
const PRUNE_INACTIVE_MS: i64 = 10 * 60 * 1000; // 10 minutes

/// Generate all parameter combinations from the grid.
pub fn generate_grid() -> Vec<TraderConfig> {
    let cap = GAP_THRESHOLDS.len()
        * TARGET_RATIOS.len()
        * STOP_LOSSES.len()
        * MAX_HOLDS.len()
        * MAX_SPREADS.len()
        * TRAILING_TAKES.len();
    let mut configs = Vec::with_capacity(cap);
    let base = TraderConfig::default();

    for &gap in GAP_THRESHOLDS {
        for &target in TARGET_RATIOS {
            for &stop in STOP_LOSSES {
                for &hold in MAX_HOLDS {
                    for &spread in MAX_SPREADS {
                        for &trailing in TRAILING_TAKES {
                            configs.push(TraderConfig {
                                spike_threshold_bps: gap,
                                target_ratio: target,
                                stop_loss_bps: stop,
                                max_hold_ms: hold,
                                max_spread_bps: spread,
                                trailing_decay_ratio: trailing,
                                ..base
                            });
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
        Self {
            traders,
            pending_trades: VecDeque::new(),
            last_session_trades,
            disabled: vec![false; n],
            active_count: n,
            first_tick_ms: None,
        }
    }

    /// Total number of traders in the fleet (including pruned).
    #[inline]
    pub fn len(&self) -> usize { self.traders.len() }

    /// Number of active (non-pruned) traders.
    #[inline]
    pub fn active(&self) -> usize { self.active_count }

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
            if self.disabled[idx] { continue; }

            trader.tick(ts_ms, binance, gate, samples, window_ms);

            let session_n = trader.session_trades();
            let prev_n = self.last_session_trades[idx];

            if session_n > prev_n {
                let new_count = session_n - prev_n;
                let deque = trader.completed_trades();
                let start = deque.len().saturating_sub(new_count);
                let sym = symbol.to_string();
                for trade in deque.iter().skip(start) {
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
                            config_id = *config_id, symbol,
                            trades = session_n, avg_pnl_pct = format!("{avg_pnl:.4}"),
                            "fleet: pruned (negative expectancy)"
                        );
                    }
                }
            } else if session_n == 0 && elapsed >= PRUNE_INACTIVE_MS {
                // Prune configs that never traded after warmup period.
                self.disabled[idx] = true;
                self.active_count -= 1;
                tracing::debug!(
                    config_id = *config_id, symbol,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_size() {
        let grid = generate_grid();
        // 5 gaps × 3 targets × 4 SLs × 4 holds × 2 spreads × 3 trailing = 1440
        assert_eq!(grid.len(), 1440);
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
        assert_eq!(fleet.len(), 1440);
        assert_eq!(fleet.active(), 1440);
    }
}
