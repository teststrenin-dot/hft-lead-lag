//! Screener dashboard — single-page HTML/JS/CSS application.

use axum::response::Html;

pub async fn screener_page() -> Html<&'static str> {
    Html(SCREENER_HTML)
}

const SCREENER_HTML: &str = r#"<!doctype html>
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
  let sortKey = 'symbol', sortAsc = true;
  function sortBy(key) {
    if (sortKey === key) { sortAsc = !sortAsc; } else { sortKey = key; sortAsc = true; }
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
    loadHistory(sym);
  }

  async function loadHistory(sym) {
    try {
      const res = await fetch(`/api/v1/chart/${sym}`);
      if (!res.ok) return;
      const c = await res.json();
      if (!c || !c.ts || c.ts.length === 0) return;
      tsBuf = c.ts.slice();
      gtBid = (c.gate_bid || []).slice();
      gtAsk = (c.gate_ask || []).slice();
      bnBid = (c.binance_bid || []).slice();
      bnAsk = (c.binance_ask || []).slice();
      if (gtBid.length === 0 || bnBid.length === 0) return;
      if (gtBid.length > 0) { lastGate.bid = gtBid[gtBid.length-1]; lastGate.ask = gtAsk[gtAsk.length-1]; }
      if (bnBid.length > 0) { lastBn.bid = bnBid[bnBid.length-1]; lastBn.ask = bnAsk[bnAsk.length-1]; }
      shadowZones = (c.trades || []).map(t => ({
        entry_s: t.entry_ts_ms / 1000, exit_s: t.exit_ts_ms / 1000,
        dir: t.direction, pnl: t.pnl_pct, entry_price: t.entry_price,
        exit_price: t.exit_price, reason: t.exit_reason, open: false
      }));
      if (c.position !== 'FLAT' && c.position !== 'PENDING' && c.entry_ts_ms) {
        openZone = { entry_s: c.entry_ts_ms / 1000, dir: c.position.replace('LONG_GT','LONG').replace('SHORT_GT','SHORT'), pnl: 0, entry_price: c.entry_price, open: true };
      } else { openZone = null; }
      dirty = true;
    } catch(e) {}
  }

  const MAX_PTS = 7200;
  let tsBuf = [], gtBid = [], gtAsk = [], bnBid = [], bnAsk = [];
  let uplot = null, dirty = false, ws = null;
  let shadowZones = [], openZone = null;

  function clearChart() {
    tsBuf = []; gtBid = []; gtAsk = []; bnBid = []; bnAsk = [];
    lastGate = { bid: NaN, ask: NaN }; lastBn = { bid: NaN, ask: NaN };
    shadowZones = []; openZone = null; dirty = true;
  }

  let lastGate = { bid: NaN, ask: NaN }, lastBn = { bid: NaN, ask: NaN };
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
      if (d.exchange === 'gate') { lastGate.bid = d.bid; lastGate.ask = d.ask; }
      else { lastBn.bid = d.bid; lastBn.ask = d.ask; }
      if (isNaN(lastGate.bid) || isNaN(lastBn.bid)) return;
      tsBuf.push(ts); gtBid.push(lastGate.bid); gtAsk.push(lastGate.ask);
      bnBid.push(lastBn.bid); bnAsk.push(lastBn.ask);
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
    const w = window.innerWidth - 32, h = Math.min(window.innerHeight * 0.38, 360);
    const drawZones = (u) => {
      const ctx = u.ctx, xs = u.scales.x;
      if (xs.min == null) return;
      const top = u.bbox.top, bot = top + u.bbox.height;
      const all = shadowZones.slice();
      if (openZone) all.push({ ...openZone, exit_s: xs.max });
      for (const z of all) {
        const x0 = u.valToPos(Math.max(z.entry_s, xs.min), 'x', true);
        const x1 = u.valToPos(Math.min(z.exit_s, xs.max), 'x', true);
        if (x1 <= x0) continue;
        const isLong = z.dir === 'LONG';
        ctx.fillStyle = isLong ? 'rgba(74,222,128,0.10)' : 'rgba(248,113,113,0.10)';
        ctx.fillRect(x0, top, x1 - x0, bot - top);
        if (z.entry_price) {
          const y = u.valToPos(z.entry_price, 'y', true);
          ctx.beginPath(); ctx.arc(x0, y, 3, 0, 2 * Math.PI);
          ctx.fillStyle = isLong ? '#4ade80' : '#f87171'; ctx.fill();
        }
        if (!z.open && z.exit_price) {
          const y = u.valToPos(z.exit_price, 'y', true);
          ctx.beginPath(); ctx.arc(x1, y, 3, 0, 2 * Math.PI);
          ctx.fillStyle = isLong ? '#4ade80' : '#f87171'; ctx.fill();
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

  let lastRenderTs = 0;
  function renderLoop(ts) {
    if (dirty && uplot && tsBuf.length > 1 && ts - lastRenderTs > 66) {
      lastRenderTs = ts;
      uplot.setData([new Float64Array(tsBuf), new Float64Array(gtBid), new Float64Array(gtAsk), new Float64Array(bnBid), new Float64Array(bnAsk)]);
      const n = tsBuf.length;
      document.getElementById('chart-info').textContent = `${n} pts | gate: ${lastGate.bid.toFixed(4)}/${lastGate.ask.toFixed(4)} | bn: ${lastBn.bid.toFixed(4)}/${lastBn.ask.toFixed(4)}`;
      dirty = false;
    }
    requestAnimationFrame(renderLoop);
  }

  async function pollTrades() {
    if (!selectedSym) return;
    try {
      const [chartRes, shadowRes] = await Promise.all([
        fetch(`/api/v1/chart/${selectedSym}`), fetch(`/api/v1/shadow/${selectedSym}`)
      ]);
      if (chartRes.ok) {
        const c = await chartRes.json();
        if (c) {
          shadowZones = (c.trades || []).map(t => ({
            entry_s: t.entry_ts_ms / 1000, exit_s: t.exit_ts_ms / 1000,
            dir: t.direction, pnl: t.pnl_pct, entry_price: t.entry_price,
            exit_price: t.exit_price, reason: t.exit_reason, open: false
          }));
          if (c.position !== 'FLAT' && c.position !== 'PENDING' && c.entry_ts_ms) {
            openZone = { entry_s: c.entry_ts_ms / 1000, dir: c.position.replace('LONG_GT','LONG').replace('SHORT_GT','SHORT'), pnl: 0, entry_price: c.entry_price, open: true };
          } else { openZone = null; }
          dirty = true;
        }
      }
      if (shadowRes.ok) {
        const d = await shadowRes.json();
        if (d) {
          const el = document.getElementById('trades-info');
          const parts = [`${d.position} | spikes: ${d.spikes_in_window} | threshold: ${d.spike_threshold_bps}bps | hold: ${d.max_hold_ms}ms | SL: ${d.stop_loss_bps}bps`];
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

  makeChart();
  connectWS();
  (async function tableLoop() { await renderTable(); setTimeout(tableLoop, 1000); })();
  setInterval(pollTrades, 5000);
  requestAnimationFrame(renderLoop);
  </script>
</body>
</html>"#;
