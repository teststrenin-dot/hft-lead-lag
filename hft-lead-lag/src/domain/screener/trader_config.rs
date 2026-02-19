//! TraderConfig — all tunable parameters for a shadow trader instance.

use serde::Serialize;

/// Configuration for a single shadow trader instance.
/// `Copy` + `Clone` so it can be shared across fleet without allocation.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TraderConfig {
    /// Min Binance move to trigger entry (bps). Ask for longs, bid for shorts.
    pub spike_threshold_bps: f64,
    /// Window to measure Binance spike (ms).
    pub spike_window_ms: i64,
    /// Target as fraction of detected spike (1.0 = full catchup).
    pub target_ratio: f64,
    /// Stop-loss (bps).
    pub stop_loss_bps: f64,
    /// Max hold time (ms).
    pub max_hold_ms: i64,
    /// Max Gate spread to allow entry (bps). 0 = disabled.
    pub max_spread_bps: f64,
    /// Trailing stop activation threshold (bps of unrealized profit). 0 = disabled.
    pub trailing_stop_bps: f64,
    /// Trailing stop decay ratio — exit when unrealized drops below peak * ratio.
    pub trailing_decay_ratio: f64,
    /// Simulated order-to-fill latency (ms).
    pub fill_delay_ms: i64,
    /// Post-trade cooldown (ms).
    pub cooldown_ms: i64,
    /// Warmup before trading starts (ms).
    pub warmup_ms: i64,
    /// Max quote staleness (ms).
    pub quote_freshness_ms: i64,
    /// Gate taker fee (fraction, e.g. 0.0005 = 0.05%).
    pub taker_fee: f64,
}

impl Default for TraderConfig {
    fn default() -> Self {
        Self {
            spike_threshold_bps: 30.0,
            spike_window_ms: 500,
            target_ratio: 1.0,
            stop_loss_bps: 10.0,
            max_hold_ms: 30_000,
            max_spread_bps: 0.0,
            trailing_stop_bps: 0.0,
            trailing_decay_ratio: 0.5,
            fill_delay_ms: 6,
            cooldown_ms: 3_000,
            warmup_ms: 30_000,
            quote_freshness_ms: 1_000,
            taker_fee: 0.000_5,
        }
    }
}

impl TraderConfig {
    /// Unique ID for this parameter set (deterministic hash).
    pub fn config_id(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        self.spike_threshold_bps.to_bits().hash(&mut h);
        self.spike_window_ms.hash(&mut h);
        self.target_ratio.to_bits().hash(&mut h);
        self.stop_loss_bps.to_bits().hash(&mut h);
        self.max_hold_ms.hash(&mut h);
        self.max_spread_bps.to_bits().hash(&mut h);
        self.trailing_stop_bps.to_bits().hash(&mut h);
        self.trailing_decay_ratio.to_bits().hash(&mut h);
        h.finish()
    }
}
