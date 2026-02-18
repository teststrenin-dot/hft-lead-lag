//! HTTP server for monitoring and control

use axum::{Json, Router, extract::State, response::Html, routing::get};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::api::screener::{ScreenerRow, ScreenerStore};
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};

/// HTTP server configuration
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub bind_address: String,
    pub port: u16,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 5000,
        }
    }
}

/// HTTP server for REST API
pub struct HttpServer {
    config: HttpServerConfig,
    min_volume_usd: f64,
    screener: ScreenerStore,
}

impl HttpServer {
    pub fn new(config: HttpServerConfig) -> Self {
        Self::with_min_volume(config, 1_000_000.0)
    }

    pub fn with_min_volume(config: HttpServerConfig, min_volume_usd: f64) -> Self {
        Self::with_runtime(config, min_volume_usd, ScreenerStore::default())
    }

    pub fn with_runtime(
        config: HttpServerConfig,
        min_volume_usd: f64,
        screener: ScreenerStore,
    ) -> Self {
        Self {
            config,
            min_volume_usd,
            screener,
        }
    }

    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.config.bind_address, self.config.port)
    }

    /// Start the HTTP API server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let state = Arc::new(HttpState {
            min_volume_usd: self.min_volume_usd,
            screener: self.screener.clone(),
        });

        let app = Router::new()
            .route(endpoints::HEALTH, get(health))
            .route(endpoints::SYMBOLS, get(get_symbols))
            .route(endpoints::SCREENER_DATA, get(get_screener))
            .route(endpoints::SCREENER_PAGE, get(screener_page))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(self.bind_address()).await?;
        info!("HTTP server listening on {}", self.bind_address());
        axum::serve(listener, app).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct HttpState {
    min_volume_usd: f64,
    screener: ScreenerStore,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct SymbolSnapshot {
    exchange: &'static str,
    symbol: String,
    quote_volume: f64,
    last_price: Option<f64>,
    price_change_24h_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
struct SymbolsResponse {
    min_volume_usd: f64,
    total_symbols: usize,
    common_symbols: Vec<String>,
    symbols: Vec<SymbolSnapshot>,
}

#[derive(Debug, Serialize)]
struct ScreenerResponse {
    generated_at_ms: i64,
    period_minutes: u64,
    total_symbols: usize,
    rows: Vec<ScreenerRow>,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn get_symbols(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<SymbolsResponse>, (axum::http::StatusCode, String)> {
    let binance = BinanceRestClient::new();
    let gate = GateRestClient::new();

    let (binance_tickers, gate_tickers) = tokio::join!(
        binance.get_tickers_with_volume(state.min_volume_usd),
        gate.get_tickers_with_volume(state.min_volume_usd)
    );

    let binance_tickers = binance_tickers.map_err(internal_error)?;
    let gate_tickers = gate_tickers.map_err(internal_error)?;

    let binance_symbols: HashSet<String> = binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_symbols: HashSet<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    let mut common_symbols: Vec<String> = binance_symbols.intersection(&gate_symbols).cloned().collect();
    common_symbols.sort_unstable();

    let mut symbols = Vec::with_capacity(binance_tickers.len() + gate_tickers.len());
    symbols.extend(to_snapshots("binance", binance_tickers));
    symbols.extend(to_snapshots("gate", gate_tickers));

    Ok(Json(SymbolsResponse {
        min_volume_usd: state.min_volume_usd,
        total_symbols: symbols.len(),
        common_symbols,
        symbols,
    }))
}

async fn get_screener(State(state): State<Arc<HttpState>>) -> Json<ScreenerResponse> {
    let live_rows = state.screener.rows_sorted();
    let mut by_symbol: HashMap<String, ScreenerRow> = fallback_screener_rows(state.min_volume_usd)
        .await
        .into_iter()
        .map(|row| (row.symbol.clone(), row))
        .collect();

    for row in live_rows {
        by_symbol.insert(row.symbol.clone(), row);
    }

    let mut rows: Vec<ScreenerRow> = by_symbol.into_values().collect();
    rows.sort_by(|a, b| {
        b.lag_ms
            .partial_cmp(&a.lag_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.symbol.cmp(&b.symbol))
    });

    Json(ScreenerResponse {
        generated_at_ms: now_ms(),
        period_minutes: (state.screener.window_ms() / 60_000) as u64,
        total_symbols: rows.len(),
        rows,
    })
}

async fn fallback_screener_rows(min_volume_usd: f64) -> Vec<ScreenerRow> {
    let binance = BinanceRestClient::new();
    let gate = GateRestClient::new();
    let (binance_tickers, gate_tickers) = tokio::join!(
        binance.get_tickers_with_volume(min_volume_usd),
        gate.get_tickers_with_volume(min_volume_usd)
    );

    let mut symbols: HashMap<String, (f64, f64)> = HashMap::new();
    if let Ok(tickers) = binance_tickers {
        for t in tickers {
            symbols
                .entry(t.symbol)
                .and_modify(|entry| entry.0 = t.quote_volume)
                .or_insert((t.quote_volume, 0.0));
        }
    }
    if let Ok(tickers) = gate_tickers {
        for t in tickers {
            symbols
                .entry(t.symbol)
                .and_modify(|entry| entry.1 = t.quote_volume)
                .or_insert((0.0, t.quote_volume));
        }
    }

    symbols
        .into_iter()
        .map(|(symbol, (binance_volume, gate_volume))| ScreenerRow {
            symbol,
            leader_exchange: if binance_volume >= gate_volume {
                "binance".to_string()
            } else {
                "gate".to_string()
            },
            lag_ms: 0.0,
            entry_half_life_ms: 0.0,
        })
        .collect()
}

async fn screener_page() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>HFT Lead-Lag Screener</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; background:#0b1020; color:#e5e7eb; }
    h1 { margin: 0 0 8px; }
    .meta { margin-bottom: 12px; color: #9ca3af; }
    table { width: 100%; border-collapse: collapse; background:#111827; }
    th, td { padding: 8px; border-bottom: 1px solid #1f2937; text-align: left; font-size: 14px; }
    th { position: sticky; top: 0; background:#111827; color:#93c5fd; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
  </style>
</head>
<body>
  <h1>Lead-Lag Screener</h1>
  <div class="meta" id="meta">Loading...</div>
  <table>
    <thead>
      <tr>
        <th>Coin</th>
        <th>Leader</th>
        <th class="num">Lag (ms)</th>
        <th class="num">Entry half-life (ms)</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
  <script>
    async function render() {
      try {
        const res = await fetch('/api/v1/screener', { cache: 'no-store' });
        const data = await res.json();
        const rows = data.rows || [];
        document.getElementById('meta').textContent =
          `symbols=${data.total_symbols} period=${data.period_minutes}m updated=${new Date(data.generated_at_ms).toLocaleTimeString()}`;
        document.getElementById('rows').innerHTML = rows.map(r => `
          <tr>
            <td>${r.symbol}</td>
            <td>${r.leader_exchange}</td>
            <td class="num">${Number(r.lag_ms).toFixed(2)}</td>
            <td class="num">${Number(r.entry_half_life_ms).toFixed(2)}</td>
          </tr>
        `).join('');
      } catch (e) {
        document.getElementById('meta').textContent = 'failed to load screener';
      }
    }
    render();
    setInterval(render, 2000);
  </script>
</body>
</html>"#,
    )
}

fn to_snapshots(exchange: &'static str, tickers: Vec<Ticker24h>) -> Vec<SymbolSnapshot> {
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

fn internal_error(error: crate::domain::ExchangeError) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::BAD_GATEWAY,
        format!("exchange error: {}", error),
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// API endpoints
pub mod endpoints {
    /// Health check endpoint
    pub const HEALTH: &str = "/health";
    
    /// Metrics endpoint
    pub const METRICS: &str = "/metrics";
    
    /// Positions endpoint
    pub const POSITIONS: &str = "/api/v1/positions";
    
    /// Orders endpoint
    pub const ORDERS: &str = "/api/v1/orders";
    
    /// Config endpoint
    pub const CONFIG: &str = "/api/v1/config";

    /// Symbols with volume and 24h price dynamics
    pub const SYMBOLS: &str = "/api/v1/symbols";

    /// Screener JSON data
    pub const SCREENER_DATA: &str = "/api/v1/screener";

    /// Screener web page
    pub const SCREENER_PAGE: &str = "/screener";
    
    /// Start trading endpoint
    pub const START_TRADING: &str = "/api/v1/trading/start";
    
    /// Stop trading endpoint
    pub const STOP_TRADING: &str = "/api/v1/trading/stop";
}
