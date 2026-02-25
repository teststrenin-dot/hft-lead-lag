use super::{HttpState, SymbolSnapshot};
use crate::infrastructure::rest::Ticker24h;

#[derive(Debug, Clone, Copy)]
pub(super) struct FleetStats {
    pub(super) win_rate_pct: f64,
    pub(super) avg_pnl_pct: f64,
}

pub(super) fn compute_fleet_stats(total: i64, wins: i64, total_pnl: f64) -> FleetStats {
    if total > 0 {
        FleetStats {
            win_rate_pct: (wins as f64 / total as f64) * 100.0,
            avg_pnl_pct: total_pnl / total as f64,
        }
    } else {
        FleetStats {
            win_rate_pct: 0.0,
            avg_pnl_pct: 0.0,
        }
    }
}

pub(super) fn resolve_forward_run_id(
    conn: &rusqlite::Connection,
    requested: Option<&str>,
) -> Result<String, (axum::http::StatusCode, String)> {
    let requested = requested.map(str::trim).filter(|v| !v.is_empty());
    if let Some(rid) = requested {
        if !rid.starts_with("forward-") {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "run_id must start with forward-".to_string(),
            ));
        }
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM trades WHERE run_id = ?1",
                rusqlite::params![rid],
                |row| row.get(0),
            )
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query: {e}"),
                )
            })?;
        if exists == 0 {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("forward run not found: {rid}"),
            ));
        }
        return Ok(rid.to_string());
    }

    let latest = conn
        .query_row(
            "SELECT run_id
             FROM trades
             WHERE run_id LIKE 'forward-%'
             GROUP BY run_id
             ORDER BY MAX(exit_ts_ms) DESC
             LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => (
                axum::http::StatusCode::NOT_FOUND,
                "no forward runs found".to_string(),
            ),
            _ => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            ),
        })?;

    Ok(latest)
}

pub(super) fn to_snapshots(exchange: &'static str, tickers: Vec<Ticker24h>) -> Vec<SymbolSnapshot> {
    tickers
        .into_iter()
        .map(|ticker| SymbolSnapshot {
            exchange,
            symbol: ticker.symbol,
            quote_volume: ticker.quote_volume,
            last_price: ticker.last_price,
            price_change_24h_pct: ticker.price_change_24h_pct,
        })
        .collect()
}

pub(super) fn internal_error(error: crate::domain::ExchangeError) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::BAD_GATEWAY,
        format!("exchange error: {}", error),
    )
}

pub(super) fn open_readonly_conn(
    state: &HttpState,
) -> Result<rusqlite::Connection, (axum::http::StatusCode, String)> {
    crate::infrastructure::db::open_db_readonly(&state.db_path).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("db: {e}"),
        )
    })
}
