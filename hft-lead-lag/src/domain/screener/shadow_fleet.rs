//! ShadowFleet — runs N shadow traders with different configs on shared price samples.
//!
//! Each trader gets the same `&PriceSamples` (zero-copy), only trading state differs.
//! Completed trades are collected and can be drained for persistence.

use std::collections::VecDeque;

use super::price_samples::PriceSamples;
use super::shadow_trader::{ClosedTrade, ShadowTrader};
use super::state::Quote;
use super::trader_config::TraderConfig;

/// Parameter grid values for fleet generation.
const SPIKE_THRESHOLDS: &[f64] = &[30.0, 40.0, 50.0, 60.0, 80.0];
const SPIKE_WINDOWS: &[i64] = &[500]; // not used for entry (gap-based), kept for stats
const TARGET_RATIOS: &[f64] = &[0.3, 0.5, 0.7, 1.0];
const STOP_LOSSES: &[f64] = &[8.0, 10.0, 15.0, 20.0];
const MAX_HOLDS: &[i64] = &[10_000, 30_000];
const MAX_SPREADS: &[f64] = &[5.0, 10.0, 15.0];

/// Generate all parameter combinations from the grid.
pub fn generate_grid() -> Vec<TraderConfig> {
    let cap = SPIKE_THRESHOLDS.len()
        * SPIKE_WINDOWS.len()
        * TARGET_RATIOS.len()
        * STOP_LOSSES.len()
        * MAX_HOLDS.len()
        * MAX_SPREADS.len();
    let mut configs = Vec::with_capacity(cap);
    let base = TraderConfig::default();

    for &spike in SPIKE_THRESHOLDS {
        for &window in SPIKE_WINDOWS {
            for &target in TARGET_RATIOS {
                for &stop in STOP_LOSSES {
                    for &hold in MAX_HOLDS {
                        for &spread in MAX_SPREADS {
                            configs.push(TraderConfig {
                                spike_threshold_bps: spike,
                                spike_window_ms: window,
                                target_ratio: target,
                                stop_loss_bps: stop,
                                max_hold_ms: hold,
                                max_spread_bps: spread,
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
#[derive(Debug)]
pub struct ShadowFleet {
    traders: Vec<(u64, ShadowTrader)>,
    /// Trades pending drain to persistence layer.
    pending_trades: VecDeque<FleetTrade>,
    /// Monotonic session trade count per trader — never decreases.
    last_session_trades: Vec<usize>,
}

impl ShadowFleet {
    pub fn new(configs: &[TraderConfig]) -> Self {
        let traders: Vec<(u64, ShadowTrader)> = configs
            .iter()
            .map(|c| (c.config_id(), ShadowTrader::new(*c)))
            .collect();
        let last_session_trades = vec![0; traders.len()];
        Self {
            traders,
            pending_trades: VecDeque::new(),
            last_session_trades,
        }
    }

    /// Number of traders in the fleet.
    #[inline]
    pub fn len(&self) -> usize { self.traders.len() }

    /// Tick all traders with shared price data.
    pub fn tick_all(
        &mut self,
        ts_ms: i64,
        binance: &Quote,
        gate: &Quote,
        samples: &PriceSamples,
        window_ms: i64,
        symbol: &str,
    ) {
        for (idx, (config_id, trader)) in self.traders.iter_mut().enumerate() {
            trader.tick(ts_ms, binance, gate, samples, window_ms);

            // Detect new completed trades via monotonic session counter.
            let session_n = trader.session_trades();
            let prev_n = self.last_session_trades[idx];
            if session_n > prev_n {
                // New trades are always at the tail of the deque.
                let new_count = session_n - prev_n;
                let deque = trader.completed_trades();
                let start = deque.len().saturating_sub(new_count);
                for trade in deque.iter().skip(start) {
                    self.pending_trades.push_back(FleetTrade {
                        config_id: *config_id,
                        symbol: symbol.to_string(),
                        trade: trade.clone(),
                    });
                }
                self.last_session_trades[idx] = session_n;
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
        assert_eq!(grid.len(), 5 * 1 * 4 * 4 * 2 * 3); // 480
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
        assert_eq!(fleet.len(), 480);
    }
}
