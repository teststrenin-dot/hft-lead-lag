use std::collections::VecDeque;

const OFFSET_WINDOW_SAMPLES: usize = 512;
const OFFSET_RECOMPUTE_INTERVAL: u32 = 64;
const MAX_OFFSET_SAMPLE_ABS_MS: i64 = 6 * 60 * 60 * 1000; // 6h safety bound

#[derive(Debug, Clone)]
pub struct ClockOffsetEstimator {
    samples: VecDeque<i64>,
    cached_median_ms: i64,
    pending_updates: u32,
}

impl Default for ClockOffsetEstimator {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(OFFSET_WINDOW_SAMPLES),
            cached_median_ms: 0,
            pending_updates: 0,
        }
    }
}

impl ClockOffsetEstimator {
    pub fn observe(&mut self, ingress_ts_ms: i64, exchange_ts_ms: i64) {
        if ingress_ts_ms <= 0 || exchange_ts_ms <= 0 {
            return;
        }
        let sample = ingress_ts_ms.saturating_sub(exchange_ts_ms);
        if sample.abs() > MAX_OFFSET_SAMPLE_ABS_MS {
            return;
        }

        self.samples.push_back(sample);
        while self.samples.len() > OFFSET_WINDOW_SAMPLES {
            self.samples.pop_front();
        }

        self.pending_updates = self.pending_updates.saturating_add(1);
        if self.samples.len() == 1 || self.pending_updates >= OFFSET_RECOMPUTE_INTERVAL {
            self.recompute_median();
            self.pending_updates = 0;
        }
    }

    pub fn corrected_exchange_ms(&self, exchange_ts_ms: i64) -> i64 {
        exchange_ts_ms.saturating_add(self.cached_median_ms)
    }

    #[cfg(test)]
    pub fn offset_ms(&self) -> i64 {
        self.cached_median_ms
    }

    fn recompute_median(&mut self) {
        if self.samples.is_empty() {
            self.cached_median_ms = 0;
            return;
        }
        let mut sorted: Vec<i64> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        self.cached_median_ms = sorted[sorted.len() / 2];
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExchangeClockOffsets {
    binance: ClockOffsetEstimator,
    gate: ClockOffsetEstimator,
}

impl ExchangeClockOffsets {
    pub fn corrected_exchange_ms(
        &mut self,
        exchange: &str,
        exchange_ts_ms: i64,
        ingress_ts_ms: i64,
    ) -> i64 {
        match exchange {
            "binance" => {
                self.binance.observe(ingress_ts_ms, exchange_ts_ms);
                self.binance.corrected_exchange_ms(exchange_ts_ms)
            }
            "gate" => {
                self.gate.observe(ingress_ts_ms, exchange_ts_ms);
                self.gate.corrected_exchange_ms(exchange_ts_ms)
            }
            _ => exchange_ts_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimator_tracks_large_constant_offset() {
        let mut est = ClockOffsetEstimator::default();
        for i in 0..80 {
            let ingress = 1_700_000_000_000 + i;
            let exchange = ingress + 3_600_000;
            est.observe(ingress, exchange);
        }
        assert!(est.offset_ms() <= -3_599_900 && est.offset_ms() >= -3_600_100);
        let corrected = est.corrected_exchange_ms(1_700_003_600_000);
        assert!((corrected - 1_700_000_000_000).abs() < 150);
    }
}
