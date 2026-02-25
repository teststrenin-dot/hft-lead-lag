//! TraderConfig — all tunable parameters for a shadow trader instance.

use serde::{Deserialize, Serialize};

/// Version of the config-id hashing contract shared across runtime and drivers.
pub const CONFIG_ID_CONTRACT_VERSION: u16 = 1;

/// Configuration for a single shadow trader instance.
/// `Copy` + `Clone` so it can be shared across fleet without allocation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct TraderConfig {
    /// Min Binance move to trigger entry (bps). Ask for longs, bid for shorts.
    pub spike_threshold_bps: f64,
    /// Fraction of detected spike at which breakeven activates and trailing begins.
    pub target_ratio: f64,
    /// Stop-loss (bps) — active only before breakeven.
    pub stop_loss_bps: f64,
    /// Max hold time (ms).
    pub max_hold_ms: i64,
    /// Max Gate spread to allow entry (bps). 0 = disabled.
    pub max_spread_bps: f64,
    /// Trailing take-profit ratio — exit when unrealized drops to peak × ratio (post-breakeven).
    pub trailing_decay_ratio: f64,
    /// Baseline window for gap detection (ms). Shorter = more signals, noisier.
    pub baseline_window_ms: i64,
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
    /// Min price samples required before baseline calculation.
    pub min_baseline_samples: usize,
}

impl Default for TraderConfig {
    /// Defaults aligned with grid medians so bare `Default::default()`
    /// produces a representative mid-grid configuration.
    fn default() -> Self {
        Self {
            spike_threshold_bps: 50.0,
            target_ratio: 0.5,
            stop_loss_bps: 15.0,
            max_hold_ms: 10_000,
            max_spread_bps: 5.0,
            trailing_decay_ratio: 0.5,
            baseline_window_ms: 20_000,
            fill_delay_ms: 6,
            cooldown_ms: 3_000,
            warmup_ms: 30_000,
            quote_freshness_ms: 1_000,
            taker_fee: 0.000_5,
            min_baseline_samples: 20,
        }
    }
}

impl TraderConfig {
    /// Unique ID for this parameter set (deterministic hash).
    pub fn config_id(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        CONFIG_ID_CONTRACT_VERSION.hash(&mut h);
        self.spike_threshold_bps.to_bits().hash(&mut h);
        self.target_ratio.to_bits().hash(&mut h);
        self.stop_loss_bps.to_bits().hash(&mut h);
        self.max_hold_ms.hash(&mut h);
        self.max_spread_bps.to_bits().hash(&mut h);
        self.trailing_decay_ratio.to_bits().hash(&mut h);
        self.baseline_window_ms.hash(&mut h);
        self.fill_delay_ms.hash(&mut h);
        self.cooldown_ms.hash(&mut h);
        self.warmup_ms.hash(&mut h);
        self.quote_freshness_ms.hash(&mut h);
        self.taker_fee.to_bits().hash(&mut h);
        self.min_baseline_samples.hash(&mut h);
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::TraderConfig;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn legacy_config_id(cfg: &TraderConfig) -> u64 {
        let mut h = DefaultHasher::new();
        cfg.spike_threshold_bps.to_bits().hash(&mut h);
        cfg.target_ratio.to_bits().hash(&mut h);
        cfg.stop_loss_bps.to_bits().hash(&mut h);
        cfg.max_hold_ms.hash(&mut h);
        cfg.max_spread_bps.to_bits().hash(&mut h);
        cfg.trailing_decay_ratio.to_bits().hash(&mut h);
        cfg.baseline_window_ms.hash(&mut h);
        cfg.fill_delay_ms.hash(&mut h);
        cfg.cooldown_ms.hash(&mut h);
        cfg.warmup_ms.hash(&mut h);
        cfg.quote_freshness_ms.hash(&mut h);
        cfg.taker_fee.to_bits().hash(&mut h);
        cfg.min_baseline_samples.hash(&mut h);
        h.finish()
    }

    #[test]
    fn config_id_differs_from_legacy_unversioned_hash() {
        let cfg = TraderConfig::default();
        assert_ne!(cfg.config_id(), legacy_config_id(&cfg));
    }
}
