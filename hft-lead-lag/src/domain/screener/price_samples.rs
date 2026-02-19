//! PriceSamples — shared price history for one symbol, used by all fleet traders.

use std::collections::VecDeque;

const CHART_RETENTION_MS: i64 = 2 * 60 * 1000;

/// Single price snapshot from both exchanges at one point in time.
#[derive(Debug, Clone)]
pub struct PriceSample {
    pub ts_ms: i64,
    pub gate_bid: f64,
    pub gate_ask: f64,
    pub binance_bid: f64,
    pub binance_ask: f64,
}

/// Shared price history for one symbol. Owned by SymbolState, passed by ref.
#[derive(Debug, Default)]
pub struct PriceSamples {
    samples: VecDeque<PriceSample>,
}

impl PriceSamples {
    pub fn push(&mut self, sample: PriceSample) {
        self.samples.push_back(sample);
    }

    pub fn cleanup(&mut self, ts_ms: i64) {
        let cutoff = ts_ms - CHART_RETENTION_MS;
        while let Some(s) = self.samples.front() {
            if s.ts_ms >= cutoff {
                break;
            }
            self.samples.pop_front();
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn back(&self) -> Option<&PriceSample> {
        self.samples.back()
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, PriceSample> {
        self.samples.iter()
    }
}
