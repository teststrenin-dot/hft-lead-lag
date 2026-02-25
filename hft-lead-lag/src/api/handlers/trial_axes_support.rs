use std::collections::BTreeMap;

use super::{AxisValueStats, TrialAxesBreakdown};

struct ConfigRow {
    vals: [f64; 7],
    trades: i64,
    avg_pnl: f64,
}

// Bucket step for each axis (0 = no bucketing).
const BUCKET: [f64; 7] = [
    0.0,     // spike_threshold_bps — low cardinality
    0.0,     // target_ratio
    2.0,     // stop_loss_bps — bucket by 2
    5000.0,  // max_hold_ms — bucket by 5s
    0.0,     // max_spread_bps
    0.0,     // trailing_decay_ratio
    10000.0, // baseline_window_ms — bucket by 10s
];

fn bucket_val(v: f64, step: f64) -> f64 {
    if step <= 0.0 {
        v
    } else {
        (v / step).round() * step
    }
}

fn aggregate_axis(rows: &[ConfigRow], idx: usize, step: f64) -> Vec<AxisValueStats> {
    let mut map: BTreeMap<i64, (i64, i64, i64, f64, f64)> = BTreeMap::new();
    for row in rows {
        let bucketed_value = bucket_val(row.vals[idx], step);
        let key = (bucketed_value * 1_000_000.0) as i64; // fixed-point key for BTreeMap ordering
        let entry = map.entry(key).or_insert((0, 0, 0, 0.0, bucketed_value));
        entry.0 += 1; // configs_total
        if row.trades > 0 {
            entry.1 += 1;
        } // configs_with_trades
        entry.2 += row.trades; // total_trades
        entry.3 += row.avg_pnl * row.trades as f64; // weighted pnl sum
    }
    map.values()
        .map(|&(configs_total, configs_with_trades, total_trades, pnl_sum, value)| {
            AxisValueStats {
                value,
                configs_total,
                configs_with_trades,
                total_trades,
                avg_pnl_pct: if total_trades > 0 {
                    pnl_sum / total_trades as f64
                } else {
                    0.0
                },
            }
        })
        .collect()
}

pub(super) fn build_trial_axes_breakdown(
    conn: &rusqlite::Connection,
    run_id: Option<String>,
) -> Result<TrialAxesBreakdown, (axum::http::StatusCode, String)> {
    // Single query: per-config row with pre-aggregated trade stats.
    let base_sql = if run_id.is_some() {
        "SELECT c.spike_threshold_bps, c.target_ratio, c.stop_loss_bps,
                c.max_hold_ms, c.max_spread_bps, c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(t.id) AS trades, COALESCE(AVG(t.pnl_pct),0) AS avg_pnl
         FROM configs c LEFT JOIN trades t ON t.config_id = c.id AND t.run_id = ?1
         GROUP BY c.id"
    } else {
        "SELECT c.spike_threshold_bps, c.target_ratio, c.stop_loss_bps,
                c.max_hold_ms, c.max_spread_bps, c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(t.id) AS trades, COALESCE(AVG(t.pnl_pct),0) AS avg_pnl
         FROM configs c LEFT JOIN trades t ON t.config_id = c.id
         GROUP BY c.id"
    };

    let mut stmt = conn.prepare(base_sql).map_err(|error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("sql: {error}"),
        )
    })?;

    let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<ConfigRow> {
        Ok(ConfigRow {
            vals: [
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ],
            trades: row.get(7)?,
            avg_pnl: row.get(8)?,
        })
    };

    let rows: Vec<ConfigRow> = if let Some(ref rid) = run_id {
        stmt.query_map(rusqlite::params![rid], map_row)
            .map_err(|error| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query: {error}"),
                )
            })?
            .filter_map(|row| row.ok())
            .collect()
    } else {
        stmt.query_map([], map_row)
            .map_err(|error| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query: {error}"),
                )
            })?
            .filter_map(|row| row.ok())
            .collect()
    };

    Ok(TrialAxesBreakdown {
        run_id,
        spike_threshold_bps: aggregate_axis(&rows, 0, BUCKET[0]),
        target_ratio: aggregate_axis(&rows, 1, BUCKET[1]),
        stop_loss_bps: aggregate_axis(&rows, 2, BUCKET[2]),
        max_hold_ms: aggregate_axis(&rows, 3, BUCKET[3]),
        max_spread_bps: aggregate_axis(&rows, 4, BUCKET[4]),
        trailing_decay_ratio: aggregate_axis(&rows, 5, BUCKET[5]),
        baseline_window_ms: aggregate_axis(&rows, 6, BUCKET[6]),
    })
}
