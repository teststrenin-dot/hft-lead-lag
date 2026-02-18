//! HTTP server for monitoring and control

use axum::{Json, Router, extract::State, response::Html, routing::get};
use dashmap::DashMap;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::api::screener::{ChartData, ScreenerRow, ScreenerStore, ShadowDebug};
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};

const NATR_PERIOD_30M: usize = 30;
const NATR_CACHE_TTL_MS: i64 = 15 * 60 * 1000;
const NATR_FETCH_LIMIT_PER_REQUEST: usize = 6;
const NATR_FETCH_TIMEOUT_MS: u64 = 500;

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
        Self::with_min_volume(config, 10_000_000.0)
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
            natr_cache: Arc::new(DashMap::new()),
        });

        let app = Router::new()
            .route(endpoints::HEALTH, get(health))
            .route(endpoints::SYMBOLS, get(get_symbols))
            .route(endpoints::SCREENER_DATA, get(get_screener))
            .route(endpoints::SCREENER_PAGE, get(screener_page))
            .route("/api/v1/shadow/:symbol", get(get_shadow_debug))
            .route("/api/v1/chart/:symbol", get(get_chart_data))
            .route("/api/v1/chart-symbols", get(get_symbol_list))
            .route("/chart", get(chart_page))
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
    natr_cache: Arc<DashMap<String, CachedNatr>>,
}

#[derive(Debug, Clone, Copy)]
struct CachedNatr {
    value_pct: Option<f64>,
    updated_at_ms: i64,
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
    // Fallback REST snapshot is only needed when live WS data is not available yet.
    // Calling it on every screener poll adds avoidable load and can delay WS readers.
    let mut rows: Vec<ScreenerRow> = if live_rows.is_empty() {
        fallback_screener_rows(state.min_volume_usd).await
    } else {
        live_rows
    };
    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    enrich_gate_natr_30m(&mut rows, &state.natr_cache).await;

    Json(ScreenerResponse {
        generated_at_ms: now_ms(),
        period_minutes: (state.screener.window_ms() / 60_000) as u64,
        total_symbols: rows.len(),
        rows,
    })
}

async fn get_shadow_debug(
    State(state): State<Arc<HttpState>>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Json<Option<ShadowDebug>> {
    Json(state.screener.shadow_debug(&symbol))
}

async fn get_chart_data(
    State(state): State<Arc<HttpState>>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Json<Option<ChartData>> {
    Json(state.screener.chart_data(&symbol))
}

async fn get_symbol_list(
    State(state): State<Arc<HttpState>>,
) -> Json<Vec<String>> {
    Json(state.screener.symbol_list())
}

async fn fallback_screener_rows(min_volume_usd: f64) -> Vec<ScreenerRow> {
    let binance = BinanceRestClient::new();
    let gate = GateRestClient::new();
    let (binance_tickers, gate_tickers) = tokio::join!(
        binance.get_tickers_with_volume(min_volume_usd),
        gate.get_tickers_with_volume(min_volume_usd)
    );

    let mut binance_volumes: HashMap<String, f64> = HashMap::new();
    let mut gate_volumes: HashMap<String, f64> = HashMap::new();

    if let Ok(tickers) = binance_tickers {
        for t in tickers {
            binance_volumes.insert(t.symbol, t.quote_volume);
        }
    }
    if let Ok(tickers) = gate_tickers {
        for t in tickers {
            gate_volumes.insert(t.symbol, t.quote_volume);
        }
    }

    let binance_symbols: HashSet<String> = binance_volumes.keys().cloned().collect();
    let gate_symbols: HashSet<String> = gate_volumes.keys().cloned().collect();

    binance_symbols
        .intersection(&gate_symbols)
        .cloned()
        .map(|symbol| {
            let binance_volume = binance_volumes.get(&symbol).copied().unwrap_or(0.0);
            let gate_volume = gate_volumes.get(&symbol).copied().unwrap_or(0.0);
            ScreenerRow {
            symbol,
            leader_exchange: if binance_volume >= gate_volume {
                "binance".to_string()
            } else {
                "gate".to_string()
            },
            lag_ms: 0.0,
            ws_drift_ms: 0.0,
            ws_drift_binance_ms: 0.0,
            ws_drift_gate_ms: 0.0,
            ws_drift_ingress_binance_ms: 0.0,
            ws_drift_ingress_gate_ms: 0.0,
            entry_half_life_ms: 0.0,
            avg_gt_p90_ms: 0.0,
            gate_natr_30m_pct: 0.0,
            shadow_pnl_per_hour_pct: 0.0,
            shadow_trades: 0,
            shadow_avg_trade_pct: 0.0,
            shadow_win_rate_pct: 0.0,
            shadow_position: "FLAT".to_string(),
            }
        })
        .collect()
}

async fn enrich_gate_natr_30m(
    rows: &mut [ScreenerRow],
    cache: &Arc<DashMap<String, CachedNatr>>,
) {
    let now = now_ms();
    let mut to_fetch: Vec<(usize, String)> = Vec::new();

    for (idx, row) in rows.iter_mut().enumerate() {
        if let Some(cached) = cache.get(&row.symbol) {
            if now.saturating_sub(cached.updated_at_ms) <= NATR_CACHE_TTL_MS {
                row.gate_natr_30m_pct = cached.value_pct.unwrap_or(0.0);
                continue;
            }
        }

        if to_fetch.len() < NATR_FETCH_LIMIT_PER_REQUEST {
            to_fetch.push((idx, row.symbol.clone()));
        }
    }

    for (idx, symbol) in to_fetch {
        let client = GateRestClient::new();
        let value = match tokio::time::timeout(
            Duration::from_millis(NATR_FETCH_TIMEOUT_MS),
            client.get_natr_30m(&symbol, NATR_PERIOD_30M),
        )
        .await
        {
            Ok(Ok(Some(v))) if v.is_finite() && v >= 0.0 => Some(v),
            Ok(Ok(Some(_))) => Some(0.0),
            Ok(Ok(None)) => None,
            Ok(Err(_)) => None,
            Err(_) => None,
        };

        cache.insert(
            symbol.clone(),
            CachedNatr {
                value_pct: value,
                updated_at_ms: now,
            },
        );
        rows[idx].gate_natr_30m_pct = value.unwrap_or(0.0);
    }
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
        <th class="num">WS drift ingress Binance (ms)</th>
        <th class="num">WS drift ingress Gate (ms)</th>
        <th class="num">Entry half-life (ms)</th>
        <th class="num">Avg >P90 time (ms)</th>
        <th class="num">Gate NATR 30m (%)</th>
        <th>Shadow Pos</th>
        <th class="num">Shadow PnL/hr (%)</th>
        <th class="num">Trades</th>
        <th class="num">Avg trade (%)</th>
        <th class="num">Win rate (%)</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
  <script>
    async function render() {
      try {
        const res = await fetch('/api/v1/screener', { cache: 'no-store' });
        const data = await res.json();
        const rows = (data.rows || []).slice().sort((a, b) =>
          String(a.symbol).localeCompare(String(b.symbol))
        );
        document.getElementById('meta').textContent =
          `symbols=${data.total_symbols} period=${data.period_minutes}m updated=${new Date(data.generated_at_ms).toLocaleTimeString()}`;
        document.getElementById('rows').innerHTML = rows.map(r => `
          <tr>
            <td>${r.symbol}</td>
            <td>${r.leader_exchange}</td>
            <td class="num">${Number(r.lag_ms).toFixed(2)}</td>
            <td class="num">${Number(r.ws_drift_ingress_binance_ms).toFixed(2)}</td>
            <td class="num">${Number(r.ws_drift_ingress_gate_ms).toFixed(2)}</td>
            <td class="num">${Number(r.entry_half_life_ms).toFixed(2)}</td>
            <td class="num">${Number(r.avg_gt_p90_ms).toFixed(2)}</td>
            <td class="num">${Number(r.gate_natr_30m_pct).toFixed(4)}</td>
            <td>${r.shadow_position}</td>
            <td class="num" style="color:${Number(r.shadow_pnl_per_hour_pct)>=0?'#4ade80':'#f87171'}">${Number(r.shadow_pnl_per_hour_pct).toFixed(4)}</td>
            <td class="num">${r.shadow_trades}</td>
            <td class="num" style="color:${Number(r.shadow_avg_trade_pct)>=0?'#4ade80':'#f87171'}">${Number(r.shadow_avg_trade_pct).toFixed(4)}</td>
            <td class="num">${Number(r.shadow_win_rate_pct).toFixed(1)}</td>
          </tr>
        `).join('');
      } catch (e) {
        document.getElementById('meta').textContent = 'failed to load screener';
      }
    }
    render();
    setInterval(render, 1000);
  </script>
</body>
</html>"#,
    )
}

async fn chart_page() -> Html<&'static str> {
    Html(r#"<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Shadow Trader Chart</title>
<link rel="stylesheet" href="https://unpkg.com/uplot@1.6.31/dist/uPlot.min.css"/>
<script src="https://unpkg.com/uplot@1.6.31/dist/uPlot.iife.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:system-ui,-apple-system,sans-serif}
.toolbar{display:flex;align-items:center;gap:12px;padding:8px 16px;background:#161b22;border-bottom:1px solid #30363d}
.toolbar h1{font-size:14px;font-weight:600;color:#58a6ff}
.toolbar select{background:#0d1117;color:#c9d1d9;border:1px solid #30363d;padding:4px 8px;border-radius:4px;font-size:13px}
.toolbar .info{font-size:12px;color:#8b949e;margin-left:auto}
.chart-wrap{padding:8px}
.trades-tbl{width:100%;border-collapse:collapse;margin-top:8px;font-size:12px}
.trades-tbl th{background:#161b22;padding:4px 8px;text-align:left;border-bottom:1px solid #30363d;color:#8b949e}
.trades-tbl td{padding:4px 8px;border-bottom:1px solid #21262d}
.win{color:#3fb950}.loss{color:#f85149}
.legend{display:flex;gap:16px;padding:4px 16px;font-size:11px;color:#8b949e}
.legend span{display:inline-flex;align-items:center;gap:4px}
.legend .dot{width:10px;height:3px;display:inline-block;border-radius:1px}
</style>
</head>
<body>
<div class="toolbar">
  <h1>Shadow Trader</h1>
  <select id="sym"></select>
  <span class="info" id="status">Loading…</span>
</div>
<div class="legend">
  <span><span class="dot" style="background:#58a6ff"></span>Premium (bps)</span>
  <span><span class="dot" style="background:#f8514980"></span>P90 (short zone)</span>
  <span><span class="dot" style="background:#3fb95080"></span>P10 (long zone)</span>
  <span><span class="dot" style="background:#8b949e"></span>P50 (exit)</span>
</div>
<div class="chart-wrap" id="chart"></div>
<table class="trades-tbl" id="trades">
  <thead><tr><th>Entry</th><th>Exit</th><th>Dir</th><th>Entry bps</th><th>Exit bps</th><th>PnL %</th></tr></thead>
  <tbody></tbody>
</table>
<script>
const $ = s => document.querySelector(s);
let uplot = null, sym = '', raf = 0;

async function loadSymbols() {
  const res = await fetch('/api/v1/chart-symbols');
  const syms = await res.json();
  const sel = $('#sym');
  syms.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; sel.appendChild(o); });
  sym = syms[0] || '';
  sel.onchange = () => { sym = sel.value; fetchAndRender(); };
}

function makeChart() {
  const w = window.innerWidth - 32;
  const h = Math.min(window.innerHeight * 0.55, 500);
  const opts = {
    width: w, height: h,
    cursor: { show: true, drag: { x: false, y: false } },
    scales: { x: { time: true }, y: {} },
    axes: [
      { stroke: '#8b949e', grid: { stroke: '#21262d' }, ticks: { stroke: '#30363d' }, font: '11px system-ui', size: 40 },
      { stroke: '#8b949e', grid: { stroke: '#21262d' }, ticks: { stroke: '#30363d' }, font: '11px system-ui', size: 50, label: 'bps' }
    ],
    series: [
      {},
      { label: 'Premium', stroke: '#58a6ff', width: 1.5, points: { show: false } },
      { label: 'P90', stroke: '#f85149', width: 1, dash: [4,4], points: { show: false } },
      { label: 'P10', stroke: '#3fb950', width: 1, dash: [4,4], points: { show: false } },
      { label: 'P50', stroke: '#8b949e', width: 1, dash: [6,3], points: { show: false } },
      { label: 'Entry', stroke: '#d29922', width: 0, points: { show: true, size: 8, fill: '#d29922', stroke: '#d29922' } },
      { label: 'Exit', stroke: '#bc8cff', width: 0, points: { show: true, size: 6, fill: '#bc8cff', stroke: '#bc8cff' } },
    ]
  };
  const el = $('#chart');
  el.innerHTML = '';
  uplot = new uPlot(opts, [[], [], [], [], [], [], []], el);
}

async function fetchAndRender() {
  try {
    const res = await fetch(`/api/v1/chart/${sym}`);
    const d = await res.json();
    if (!d) { $('#status').textContent = `${sym}: no data`; return; }

    const ts = new Float64Array(d.ts);
    const prem = new Float64Array(d.premium_bps);
    const len = ts.length;

    // Threshold lines as constant arrays
    const p90 = d.p90 != null ? new Float64Array(len).fill(d.p90) : new Float64Array(len);
    const p10 = d.p10 != null ? new Float64Array(len).fill(d.p10) : new Float64Array(len);
    const p50 = d.p50 != null ? new Float64Array(len).fill(d.p50) : new Float64Array(len);

    // Trade markers — sparse arrays (null where no marker)
    const entries = new Array(len).fill(null);
    const exits = new Array(len).fill(null);
    if (d.trades && d.trades.length > 0) {
      for (const t of d.trades) {
        const ets = t.entry_ts_ms / 1000;
        const xts = t.exit_ts_ms / 1000;
        // Find nearest index
        let ei = bsearch(ts, ets);
        let xi = bsearch(ts, xts);
        if (ei >= 0 && ei < len) entries[ei] = t.entry_premium_bps;
        if (xi >= 0 && xi < len) exits[xi] = t.exit_premium_bps;
      }
    }

    if (!uplot) makeChart();
    uplot.setData([ts, prem, p90, p10, p50, entries, exits]);

    const pos = d.position !== 'FLAT' ? ` | ${d.position}` : '';
    $('#status').textContent = `${sym} | ${len} pts | P90=${fmt(d.p90)} P50=${fmt(d.p50)} P10=${fmt(d.p10)}${pos}`;

    // Trades table
    const tbody = $('#trades tbody');
    tbody.innerHTML = '';
    if (d.trades) {
      for (const t of d.trades.slice(-20).reverse()) {
        const cls = t.pnl_pct > 0 ? 'win' : 'loss';
        tbody.innerHTML += `<tr>
          <td>${new Date(t.entry_ts_ms).toLocaleTimeString()}</td>
          <td>${new Date(t.exit_ts_ms).toLocaleTimeString()}</td>
          <td>${t.direction}</td>
          <td>${t.entry_premium_bps.toFixed(2)}</td>
          <td>${t.exit_premium_bps.toFixed(2)}</td>
          <td class="${cls}">${t.pnl_pct.toFixed(4)}%</td>
        </tr>`;
      }
    }
  } catch(e) { $('#status').textContent = `Error: ${e.message}`; }
}

function bsearch(arr, val) {
  let lo = 0, hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    if (arr[mid] < val) lo = mid + 1; else hi = mid - 1;
  }
  return lo;
}

function fmt(v) { return v != null ? v.toFixed(2) : '—'; }

let lastFetch = 0;
function loop() {
  const now = performance.now();
  if (now - lastFetch >= 1000) { lastFetch = now; fetchAndRender(); }
  raf = requestAnimationFrame(loop);
}

window.addEventListener('resize', () => { if (uplot) { uplot.setSize({ width: window.innerWidth - 32, height: Math.min(window.innerHeight * 0.55, 500) }); } });

loadSymbols().then(() => { makeChart(); loop(); });
</script>
</body>
</html>"#)
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
