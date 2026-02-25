//! CycleTracker — measures divergence → convergence half-life and P90 zone durations.

use super::utils::percentile;
use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct CycleTracker {
    divergence_bps: VecDeque<(i64, f64)>,
    convergence_bps: VecDeque<(i64, f64)>,
    half_life_samples_ms: VecDeque<(i64, f64)>,
    above_p90_samples_ms: VecDeque<(i64, f64)>,
    open_entry_ts_ms: Option<i64>,
    open_above_p90_ts_ms: Option<i64>,
}

impl CycleTracker {
    pub fn update(
        &mut self,
        ts_ms: i64,
        divergence_bps: f64,
        convergence_bps: f64,
        window_ms: i64,
    ) {
        self.divergence_bps.push_back((ts_ms, divergence_bps));
        self.convergence_bps.push_back((ts_ms, convergence_bps));
        self.cleanup(ts_ms, window_ms);

        let Some(p90_divergence) = percentile(self.divergence_bps.iter().map(|(_, v)| *v), 90.0)
        else {
            return;
        };
        let Some(p50_convergence) = percentile(self.convergence_bps.iter().map(|(_, v)| *v), 50.0)
        else {
            return;
        };

        if divergence_bps >= p90_divergence {
            if self.open_above_p90_ts_ms.is_none() {
                self.open_above_p90_ts_ms = Some(ts_ms);
            }
        } else if let Some(zone_entry_ts) = self.open_above_p90_ts_ms.take() {
            if ts_ms >= zone_entry_ts {
                let zone_duration_ms = (ts_ms - zone_entry_ts).max(0) as f64;
                self.above_p90_samples_ms
                    .push_back((ts_ms, zone_duration_ms));
            }
        }

        if self.open_entry_ts_ms.is_none() && divergence_bps >= p90_divergence {
            self.open_entry_ts_ms = Some(ts_ms);
        }

        if let Some(entry_ts) = self.open_entry_ts_ms {
            if ts_ms >= entry_ts && convergence_bps <= p50_convergence {
                let half_life_ms = (ts_ms - entry_ts).max(0) as f64;
                self.half_life_samples_ms.push_back((ts_ms, half_life_ms));
                self.open_entry_ts_ms = None;
                self.cleanup(ts_ms, window_ms);
            }
        }
    }

    pub fn average_half_life_ms(&self) -> Option<f64> {
        if self.half_life_samples_ms.is_empty() {
            return None;
        }
        let sum: f64 = self.half_life_samples_ms.iter().map(|(_, v)| *v).sum();
        Some(sum / self.half_life_samples_ms.len() as f64)
    }

    pub fn average_above_p90_ms(&self) -> Option<f64> {
        if self.above_p90_samples_ms.is_empty() {
            return None;
        }
        let sum: f64 = self.above_p90_samples_ms.iter().map(|(_, v)| *v).sum();
        Some(sum / self.above_p90_samples_ms.len() as f64)
    }

    fn cleanup(&mut self, ts_ms: i64, window_ms: i64) {
        let cutoff = ts_ms - window_ms;
        while let Some((ts, _)) = self.divergence_bps.front() {
            if *ts >= cutoff {
                break;
            }
            self.divergence_bps.pop_front();
        }
        while let Some((ts, _)) = self.convergence_bps.front() {
            if *ts >= cutoff {
                break;
            }
            self.convergence_bps.pop_front();
        }
        while let Some((ts, _)) = self.half_life_samples_ms.front() {
            if *ts >= cutoff {
                break;
            }
            self.half_life_samples_ms.pop_front();
        }
        while let Some((ts, _)) = self.above_p90_samples_ms.front() {
            if *ts >= cutoff {
                break;
            }
            self.above_p90_samples_ms.pop_front();
        }
    }
}
