//! Utility functions for timestamp normalisation, drift calculation, and percentile math.

use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

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

pub fn calculate_ws_drift_ms(local_ts_ms: i64, raw_exchange_ts_ns: i64) -> Option<f64> {
    let exchange_ts_ms = normalize_exchange_ts_ms(raw_exchange_ts_ns)?;
    let drift_ms = local_ts_ms.saturating_sub(exchange_ts_ms) as f64;
    if drift_ms.abs() <= 30_000.0 {
        Some(drift_ms)
    } else {
        None
    }
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
