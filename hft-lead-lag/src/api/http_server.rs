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
            volume_24h_usd: 0.0,
            shadow_pnl_per_hour_pct: 0.0,
            shadow_trades: 0,
            shadow_avg_trade_pct: 0.0,
            shadow_win_rate_pct: 0.0,
            shadow_position: "FLAT".to_string(),
            shadow_spikes_detected: 0,
            shadow_avg_catchup_pct: 0.0,
            shadow_avg_lag_ms: 0.0,
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

    let futs: Vec<_> = to_fetch
        .iter()
        .map(|(_, symbol)| {
            let sym = symbol.clone();
            let c = GateRestClient::new();
            async move {
                match tokio::time::timeout(
                    Duration::from_millis(NATR_FETCH_TIMEOUT_MS),
                    c.get_natr_30m(&sym, NATR_PERIOD_30M),
                )
                .await
                {
                    Ok(Ok(Some(v))) if v.is_finite() && v >= 0.0 => Some(v),
                    Ok(Ok(Some(_))) => Some(0.0),
                    Ok(Ok(None)) => None,
                    Ok(Err(_)) => None,
                    Err(_) => None,
                }
            }
        })
        .collect();

    let results = futures_util::future::join_all(futs).await;

    for ((idx, symbol), value) in to_fetch.into_iter().zip(results) {
        cache.insert(
            symbol,
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
  <link rel="stylesheet" href="https://unpkg.com/uplot@1.6.31/dist/uPlot.min.css"/>
  <script src="https://unpkg.com/uplot@1.6.31/dist/uPlot.iife.min.js"></script>
  <style>
    body { font-family: system-ui, -apple-system, sans-serif; margin: 0; background:#0b1020; color:#e5e7eb; }
    .top { padding: 8px 16px; }
    h1 { margin: 0 0 4px; font-size: 16px; }
    .meta { margin-bottom: 8px; color: #9ca3af; font-size: 12px; }
    table { width: 100%; border-collapse: collapse; background:#111827; font-size: 13px; }
    th, td { padding: 5px 6px; border-bottom: 1px solid #1f2937; text-align: left; }
    th { position: sticky; top: 0; background:#111827; color:#93c5fd; font-size: 12px; cursor: pointer; user-select: none; }
    th.sort-asc::after { content: ' ▲'; font-size: 9px; }
    th.sort-desc::after { content: ' ▼'; font-size: 9px; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    tr.active { background: #1e3a5f !important; }
    tr:hover { background: #162032; cursor: pointer; }
    .chart-section { padding: 8px 16px; border-top: 1px solid #1f2937; }
    .chart-header { display: flex; align-items: center; gap: 12px; margin-bottom: 4px; }
    .chart-header h2 { font-size: 14px; margin: 0; color: #93c5fd; }
    .chart-header .info { font-size: 11px; color: #9ca3af; }
    .legend { display: flex; gap: 14px; font-size: 11px; color: #8b949e; margin-bottom: 4px; }
    .legend span { display: inline-flex; align-items: center; gap: 3px; }
    .legend .dot { width: 12px; height: 2px; display: inline-block; }
    #chart-wrap { min-height: 300px; }
    .trades-list { font-size: 11px; color: #8b949e; margin-top: 4px; }
    .trades-list .win { color: #3fb950; } .trades-list .loss { color: #f85149; }
  </style>
</head>
<body>
  <div class="top">
    <h1>Lead-Lag Screener</h1>
    <div class="meta" id="meta">Loading...</div>
  </div>

  <div class="chart-section">
    <div class="chart-header">
      <h2 id="chart-title">Select a coin ↓</h2>
      <span class="info" id="chart-info"></span>
    </div>
    <div class="legend">
      <span><span class="dot" style="background:#3fb950"></span>Gate bid</span>
      <span><span class="dot" style="background:#f85149"></span>Gate ask</span>
      <span><span class="dot" style="background:#58a6ff"></span>BN bid</span>
      <span><span class="dot" style="background:#f0883e"></span>BN ask</span>
    </div>
    <div id="chart-wrap"></div>
    <div class="trades-list" id="trades-info"></div>
  </div>

  <table>
    <thead>
      <tr>
        <th data-key="symbol" onclick="sortBy('symbol')">Coin</th>
        <th data-key="leader_exchange" onclick="sortBy('leader_exchange')">Leader</th>
        <th class="num" data-key="lag_ms" onclick="sortBy('lag_ms')">Lag</th>
        <th class="num" data-key="ws_drift_ingress_binance_ms" onclick="sortBy('ws_drift_ingress_binance_ms')">Drift BN</th>
        <th class="num" data-key="ws_drift_ingress_gate_ms" onclick="sortBy('ws_drift_ingress_gate_ms')">Drift GT</th>
        <th class="num" data-key="volume_24h_usd" onclick="sortBy('volume_24h_usd')">Vol 24h</th>
        <th class="num" data-key="entry_half_life_ms" onclick="sortBy('entry_half_life_ms')">Half-life</th>
        <th class="num" data-key="avg_gt_p90_ms" onclick="sortBy('avg_gt_p90_ms')">>P90</th>
        <th class="num" data-key="gate_natr_30m_pct" onclick="sortBy('gate_natr_30m_pct')">NATR%</th>
        <th data-key="shadow_position" onclick="sortBy('shadow_position')">Pos</th>
        <th class="num" data-key="shadow_spikes_detected" onclick="sortBy('shadow_spikes_detected')">Spikes</th>
        <th class="num" data-key="shadow_pnl_per_hour_pct" onclick="sortBy('shadow_pnl_per_hour_pct')">PnL/hr%</th>
        <th class="num" data-key="shadow_trades" onclick="sortBy('shadow_trades')">Trd</th>
        <th class="num" data-key="shadow_avg_trade_pct" onclick="sortBy('shadow_avg_trade_pct')">Avg%</th>
        <th class="num" data-key="shadow_win_rate_pct" onclick="sortBy('shadow_win_rate_pct')">Win%</th>
        <th class="num" data-key="shadow_avg_catchup_pct" onclick="sortBy('shadow_avg_catchup_pct')">Catch%</th>
        <th class="num" data-key="shadow_avg_lag_ms" onclick="sortBy('shadow_avg_lag_ms')">Lag ms</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>

  <script>
  // --- Sort state ---
  let sortKey = 'symbol', sortAsc = true;
  function sortBy(key) {
    if (sortKey === key) { sortAsc = !sortAsc; } else { sortKey = key; sortAsc = true; }
    // Update header indicators
    document.querySelectorAll('th').forEach(th => {
      th.classList.remove('sort-asc','sort-desc');
      if (th.dataset.key === sortKey) th.classList.add(sortAsc ? 'sort-asc' : 'sort-desc');
    });
    renderTable();
  }
  function sortRows(rows) {
    return rows.sort((a, b) => {
      let va = a[sortKey], vb = b[sortKey];
      if (typeof va === 'string') { const c = va.localeCompare(vb); return sortAsc ? c : -c; }
      return sortAsc ? va - vb : vb - va;
    });
  }

  // --- Screener table ---
  let selectedSym = '';
  async function renderTable() {
    try {
      const res = await fetch('/api/v1/screener', { cache: 'no-store' });
      const data = await res.json();
      const rows = sortRows((data.rows || []).slice());
      document.getElementById('meta').textContent =
        `symbols=${data.total_symbols} period=${data.period_minutes}m updated=${new Date(data.generated_at_ms).toLocaleTimeString()}`;
      document.getElementById('rows').innerHTML = rows.map(r => `
        <tr class="${r.symbol===selectedSym?'active':''}" onclick="selectSym('${r.symbol}')">
          <td><b>${r.symbol}</b></td>
          <td>${r.leader_exchange}</td>
          <td class="num">${Number(r.lag_ms).toFixed(0)}</td>
          <td class="num">${Number(r.ws_drift_ingress_binance_ms).toFixed(0)}</td>
          <td class="num">${Number(r.ws_drift_ingress_gate_ms).toFixed(0)}</td>
          <td class="num">${(Number(r.volume_24h_usd)/1e6).toFixed(1)}M</td>
          <td class="num">${Number(r.entry_half_life_ms).toFixed(0)}</td>
          <td class="num">${Number(r.avg_gt_p90_ms).toFixed(0)}</td>
          <td class="num">${Number(r.gate_natr_30m_pct).toFixed(4)}</td>
          <td>${r.shadow_position}</td>
          <td class="num">${r.shadow_spikes_detected}</td>
          <td class="num" style="color:${Number(r.shadow_pnl_per_hour_pct)>=0?'#4ade80':'#f87171'}">${Number(r.shadow_pnl_per_hour_pct).toFixed(4)}</td>
          <td class="num">${r.shadow_trades}</td>
          <td class="num" style="color:${Number(r.shadow_avg_trade_pct)>=0?'#4ade80':'#f87171'}">${Number(r.shadow_avg_trade_pct).toFixed(4)}</td>
          <td class="num">${Number(r.shadow_win_rate_pct).toFixed(1)}</td>
          <td class="num">${Number(r.shadow_avg_catchup_pct).toFixed(3)}</td>
          <td class="num">${Number(r.shadow_avg_lag_ms).toFixed(0)}</td>
        </tr>
      `).join('');
    } catch (e) { document.getElementById('meta').textContent = 'error'; }
  }

  function selectSym(sym) {
    if (sym === selectedSym) return;
    selectedSym = sym;
    clearChart();
    document.getElementById('chart-title').textContent = sym;
    renderTable();
    // Load historical chart data so zones/dots are visible
    loadHistory(sym);
  }

  async function loadHistory(sym) {
    try {
      const res = await fetch(`/api/v1/chart/${sym}`);
      if (!res.ok) return;
      const c = await res.json();
      if (!c || !c.ts || c.ts.length === 0) return;
      // Pre-fill chart buffers with historical bid/ask
      tsBuf = c.ts.slice();
      gtBid = (c.gate_bid || []).slice();
      gtAsk = (c.gate_ask || []).slice();
      bnBid = (c.binance_bid || []).slice();
      bnAsk = (c.binance_ask || []).slice();
      if (gtBid.length === 0) return;
      if (bnBid.length === 0) return;
      // Set last known prices
      if (gtBid.length > 0) { lastGate.bid = gtBid[gtBid.length-1]; lastGate.ask = gtAsk[gtAsk.length-1]; }
      if (bnBid.length > 0) { lastBn.bid = bnBid[bnBid.length-1]; lastBn.ask = bnAsk[bnAsk.length-1]; }
      // Load zones
      shadowZones = (c.trades || []).map(t => ({
        entry_s: t.entry_ts_ms / 1000,
        exit_s: t.exit_ts_ms / 1000,
        dir: t.direction,
        pnl: t.pnl_pct,
        entry_price: t.entry_price,
        exit_price: t.exit_price,
        reason: t.exit_reason,
        open: false
      }));
      if (c.position !== 'FLAT' && c.position !== 'PENDING' && c.entry_ts_ms) {
        openZone = { entry_s: c.entry_ts_ms / 1000, dir: c.position.replace('LONG_GT','LONG').replace('SHORT_GT','SHORT'), pnl: 0, entry_price: c.entry_price, open: true };
      } else {
        openZone = null;
      }
      dirty = true;
    } catch(e) {}
  }

  // --- Chart: 4 raw bid/ask lines via WS ---
  const MAX_PTS = 7200;
  let tsBuf = [], gtBid = [], gtAsk = [], bnBid = [], bnAsk = [];
  let uplot = null, dirty = false, ws = null;
  let shadowZones = [];
  let openZone = null;

  function clearChart() {
    tsBuf = []; gtBid = []; gtAsk = []; bnBid = []; bnAsk = [];
    lastGate = { bid: NaN, ask: NaN };
    lastBn = { bid: NaN, ask: NaN };
    shadowZones = []; openZone = null;
    dirty = true;
  }

  let lastGate = { bid: NaN, ask: NaN };
  let lastBn = { bid: NaN, ask: NaN };
  let reconnectMs = 1000;

  function connectWS() {
    const url = `ws://${location.hostname}:8181/ws`;
    const sock = new WebSocket(url);
    ws = sock;
    sock.onopen = () => { reconnectMs = 1000; };
    sock.onmessage = (ev) => {
      let d; try { d = JSON.parse(ev.data); } catch { return; }
      if (!d.symbol || d.symbol !== selectedSym) return;
      const ts = d.timestamp_ns / 1e9;
      if (d.exchange === 'gate') {
        lastGate.bid = d.bid; lastGate.ask = d.ask;
      } else {
        lastBn.bid = d.bid; lastBn.ask = d.ask;
      }
      if (isNaN(lastGate.bid) || isNaN(lastBn.bid)) return;
      tsBuf.push(ts);
      gtBid.push(lastGate.bid);
      gtAsk.push(lastGate.ask);
      bnBid.push(lastBn.bid);
      bnAsk.push(lastBn.ask);
      if (tsBuf.length > MAX_PTS) {
        const trim = tsBuf.length - MAX_PTS;
        tsBuf.splice(0, trim); gtBid.splice(0, trim); gtAsk.splice(0, trim);
        bnBid.splice(0, trim); bnAsk.splice(0, trim);
      }
      dirty = true;
    };
    sock.onclose = () => { setTimeout(connectWS, reconnectMs); reconnectMs = Math.min(reconnectMs * 2, 30000); };
    sock.onerror = () => sock.close();
  }

  function makeChart() {
    if (uplot) { uplot.destroy(); uplot = null; }
    const w = window.innerWidth - 32;
    const h = Math.min(window.innerHeight * 0.38, 360);
    const drawZones = (u) => {
      const ctx = u.ctx;
      const xs = u.scales.x;
      if (xs.min == null) return;
      const top = u.bbox.top, bot = top + u.bbox.height;
      const all = shadowZones.slice();
      if (openZone) all.push({ ...openZone, exit_s: xs.max });
      for (const z of all) {
        const x0 = u.valToPos(Math.max(z.entry_s, xs.min), 'x', true);
        const x1 = u.valToPos(Math.min(z.exit_s, xs.max), 'x', true);
        if (x1 <= x0) continue;
        const isLong = z.dir === 'LONG';
        // Shaded zone
        ctx.fillStyle = isLong ? 'rgba(74,222,128,0.10)' : 'rgba(248,113,113,0.10)';
        ctx.fillRect(x0, top, x1 - x0, bot - top);
        // Entry dot on the price line
        if (z.entry_price) {
          const y = u.valToPos(z.entry_price, 'y', true);
          ctx.beginPath(); ctx.arc(x0, y, 3, 0, 2 * Math.PI);
          ctx.fillStyle = isLong ? '#4ade80' : '#f87171';
          ctx.fill();
        }
        // Exit dot on the price line
        if (!z.open && z.exit_price) {
          const y = u.valToPos(z.exit_price, 'y', true);
          ctx.beginPath(); ctx.arc(x1, y, 3, 0, 2 * Math.PI);
          ctx.fillStyle = isLong ? '#4ade80' : '#f87171';
          ctx.fill();
        }
      }
    };
    const opts = {
      width: w, height: h,
      cursor: { show: true, drag: { x: false, y: false } },
      scales: { x: { time: true }, y: { auto: true } },
      hooks: { draw: [drawZones] },
      axes: [
        { stroke: '#6b7280', grid: { stroke: '#1f2937' }, ticks: { stroke: '#374151' }, font: '10px system-ui', size: 36 },
        { stroke: '#6b7280', grid: { stroke: '#1f2937' }, ticks: { stroke: '#374151' }, font: '10px system-ui', size: 60 }
      ],
      series: [
        {},
        { label: 'Gate bid', stroke: '#3fb950', width: 1.5, points: { show: false } },
        { label: 'Gate ask', stroke: '#f85149', width: 1.5, points: { show: false } },
        { label: 'BN bid',   stroke: '#58a6ff', width: 1, dash: [3,2], points: { show: false } },
        { label: 'BN ask',   stroke: '#f0883e', width: 1, dash: [3,2], points: { show: false } },
      ]
    };
    const el = document.getElementById('chart-wrap');
    el.innerHTML = '';
    uplot = new uPlot(opts, [new Float64Array(0), [], [], [], []], el);
  }

  // 15fps chart render
  let lastRenderTs = 0;
  function renderLoop(ts) {
    if (dirty && uplot && tsBuf.length > 1 && ts - lastRenderTs > 66) {
      lastRenderTs = ts;
      uplot.setData([
        new Float64Array(tsBuf),
        new Float64Array(gtBid),
        new Float64Array(gtAsk),
        new Float64Array(bnBid),
        new Float64Array(bnAsk),
      ]);
      const n = tsBuf.length;
      document.getElementById('chart-info').textContent = `${n} pts | gate: ${lastGate.bid.toFixed(4)}/${lastGate.ask.toFixed(4)} | bn: ${lastBn.bid.toFixed(4)}/${lastBn.ask.toFixed(4)}`;
      dirty = false;
    }
    requestAnimationFrame(renderLoop);
  }

  // Poll shadow trades every 5s — zones + debug info
  async function pollTrades() {
    if (!selectedSym) return;
    try {
      const [chartRes, shadowRes] = await Promise.all([
        fetch(`/api/v1/chart/${selectedSym}`),
        fetch(`/api/v1/shadow/${selectedSym}`)
      ]);
      if (chartRes.ok) {
        const c = await chartRes.json();
        if (c) {
          shadowZones = (c.trades || []).map(t => ({
            entry_s: t.entry_ts_ms / 1000,
            exit_s: t.exit_ts_ms / 1000,
            dir: t.direction,
            pnl: t.pnl_pct,
            entry_price: t.entry_price,
            exit_price: t.exit_price,
            reason: t.exit_reason,
            open: false
          }));
          if (c.position !== 'FLAT' && c.position !== 'PENDING' && c.entry_ts_ms) {
            openZone = { entry_s: c.entry_ts_ms / 1000, dir: c.position.replace('LONG_GT','LONG').replace('SHORT_GT','SHORT'), pnl: 0, entry_price: c.entry_price, open: true };
          } else {
            openZone = null;
          }
          dirty = true;
        }
      }
      if (shadowRes.ok) {
        const d = await shadowRes.json();
        if (d) {
          const el = document.getElementById('trades-info');
          const parts = [];
          parts.push(`${d.position} | spikes: ${d.spikes_in_window} | threshold: ${d.spike_threshold_bps}bps | hold: ${d.max_hold_ms}ms | SL: ${d.stop_loss_bps}bps`);
          if (d.last_5_trades_pnl_pct.length > 0) {
            parts.push('last trades: ' + d.last_5_trades_pnl_pct.map(
              p => `<span class="${p>=0?'win':'loss'}">${p.toFixed(4)}%</span>`
            ).join(', '));
          }
          el.innerHTML = parts.join(' | ');
        }
      }
    } catch(e) {}
  }

  window.addEventListener('resize', () => {
    if (uplot) uplot.setSize({ width: window.innerWidth - 32, height: Math.min(window.innerHeight * 0.38, 360) });
  });

  // Boot — use setTimeout chaining for table to avoid overlapping fetches
  makeChart();
  connectWS();
  (async function tableLoop() { await renderTable(); setTimeout(tableLoop, 1000); })();
  setInterval(pollTrades, 5000);
  requestAnimationFrame(renderLoop);
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
