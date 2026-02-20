//! Utility functions for timestamp normalisation, drift calculation, and percentile math.

use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_DRIFT_ABS_MS: f64 = 30_000.0;

/// Interpolated percentile over an iterator of f64 values.
pub fn percentile(values: impl Iterator<Item = f64>, pct: f64) -> Option<f64> {
    let mut values: Vec<f64> = values.filter(|v| v.is_finite()).collect();
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let rank = (pct.clamp(0.0, 100.0) / 100.0) * (values.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        values.get(lo).copied()
    } else {
        let frac = rank - lo as f64;
        Some(values[lo] * (1.0 - frac) + values[hi] * frac)
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[inline]
fn compute_drift_ms(local_ts_ms: i64, exchange_ts_ms: i64) -> Option<f64> {
    let drift_ms = local_ts_ms.saturating_sub(exchange_ts_ms) as f64;
    if drift_ms.abs() <= MAX_DRIFT_ABS_MS {
        Some(drift_ms)
    } else {
        None
    }
}

/// Explicit time-domain representation used by the screener.
///
/// - `exchange_event_ts_ms`: event timestamp from exchange payload.
/// - `ingress_ts_ms`: local WS ingress timestamp captured on frame receive.
/// - `decision_ts_ms`: local timestamp when screener update logic executes.
#[derive(Debug, Clone, Copy)]
pub struct TimeDomainSample {
    pub exchange_event_ts_ms: i64,
    pub ingress_ts_ms: i64,
    pub decision_ts_ms: i64,
}

impl TimeDomainSample {
    pub fn from_raw(
        raw_exchange_ts_ns: i64,
        raw_local_receive_ts_ns: i64,
        decision_ts_ms: i64,
    ) -> Self {
        let exchange_event_ts_ms =
            normalize_exchange_ts_ms(raw_exchange_ts_ns).unwrap_or(decision_ts_ms);
        let ingress_ts_ms =
            normalize_exchange_ts_ms(raw_local_receive_ts_ns).unwrap_or(decision_ts_ms);
        Self {
            exchange_event_ts_ms,
            ingress_ts_ms,
            decision_ts_ms,
        }
    }

    pub fn decision_ws_drift_ms(&self) -> Option<f64> {
        compute_drift_ms(self.decision_ts_ms, self.exchange_event_ts_ms)
    }

    pub fn ingress_ws_drift_ms(&self) -> Option<f64> {
        compute_drift_ms(self.ingress_ts_ms, self.exchange_event_ts_ms)
    }
}

pub fn calculate_ws_drift_ms(local_ts_ms: i64, raw_exchange_ts_ns: i64) -> Option<f64> {
    let exchange_ts_ms = normalize_exchange_ts_ms(raw_exchange_ts_ns)?;
    compute_drift_ms(local_ts_ms, exchange_ts_ms)
}

pub fn normalize_exchange_ts_ms(raw_ts_ns: i64) -> Option<i64> {
    if raw_ts_ns <= 0 {
        return None;
    }
    if raw_ts_ns >= 1_000_000_000_000_000_000 {
        return Some(raw_ts_ns / 1_000_000);
    }
    if raw_ts_ns >= 1_000_000_000_000_000 {
        return Some(raw_ts_ns / 1_000);
    }
    if raw_ts_ns >= 1_000_000_000_000 {
        return Some(raw_ts_ns);
    }
    if raw_ts_ns >= 1_000_000_000 {
        return raw_ts_ns.checked_mul(1_000);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_exchange_ts_ms_handles_sec_ms_us_ns() {
        assert_eq!(
            normalize_exchange_ts_ms(1_700_000_000),
            Some(1_700_000_000_000)
        ); // sec -> ms
        assert_eq!(
            normalize_exchange_ts_ms(1_700_000_000_000),
            Some(1_700_000_000_000)
        ); // ms
        assert_eq!(
            normalize_exchange_ts_ms(1_700_000_000_000_000),
            Some(1_700_000_000_000)
        ); // us -> ms
        assert_eq!(
            normalize_exchange_ts_ms(1_700_000_000_000_000_000),
            Some(1_700_000_000_000)
        ); // ns -> ms
    }

    #[test]
    fn time_domain_sample_computes_decision_and_ingress_drifts() {
        let sample = TimeDomainSample::from_raw(
            1_700_000_000_000,         // exchange ms
            1_700_000_000_080_000_000, // ingress ns
            1_700_000_000_100,         // decision ms
        );

        assert_eq!(sample.exchange_event_ts_ms, 1_700_000_000_000);
        assert_eq!(sample.ingress_ts_ms, 1_700_000_000_080);
        assert_eq!(sample.decision_ts_ms, 1_700_000_000_100);
        assert_eq!(sample.decision_ws_drift_ms(), Some(100.0));
        assert_eq!(sample.ingress_ws_drift_ms(), Some(80.0));
    }

    #[test]
    fn time_domain_sample_falls_back_to_decision_time_on_invalid_input() {
        let sample = TimeDomainSample::from_raw(0, -1, 42_000);
        assert_eq!(sample.exchange_event_ts_ms, 42_000);
        assert_eq!(sample.ingress_ts_ms, 42_000);
        assert_eq!(sample.decision_ws_drift_ms(), Some(0.0));
        assert_eq!(sample.ingress_ws_drift_ms(), Some(0.0));
    }

    #[test]
    fn ws_drift_filters_outliers() {
        let sample = TimeDomainSample::from_raw(
            1_700_000_000_000,         // exchange ms
            1_700_000_010_000_000_000, // ingress ns
            1_700_000_040_000,         // decision ms
        );
        assert_eq!(sample.decision_ws_drift_ms(), None); // 40_000ms
        assert_eq!(sample.ingress_ws_drift_ms(), Some(10_000.0));
    }
}
