use super::{
    ConfigManager, EventLoopState, HealthState, SUBSCRIBE_DELAY_MS,
};
use hft_lead_lag::api::{
    HttpServer, HttpServerConfig, MarketDataEvent, MarketDataServer, ScreenerStore, WsServerConfig,
};
use hft_lead_lag::infrastructure::rest::GateRestClient;
use hft_lead_lag::{BinanceMarketData, GateMarketData, MarketDataStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{error, info, warn};

const GATE_NATR_PERIOD_30M: usize = 30;
const GATE_NATR_REFRESH_INTERVAL_SECS: u64 = 60;
const GATE_NATR_BATCH_SIZE: usize = 12;
const GATE_NATR_REQUEST_TIMEOUT_MS: u64 = 500;

pub(super) async fn subscribe_gate_symbols(gate: &mut GateMarketData, symbols: &[String]) {
    let mut ok = 0usize;
    let mut errs = 0usize;
    let mut timeouts = 0usize;
    for symbol in symbols {
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            gate.subscribe_book_ticker(symbol),
        )
        .await
        {
            Ok(Ok(_)) => {
                ok += 1;
            }
            Ok(Err(e)) => {
                errs += 1;
                error!("Gate subscribe error {}: {}", symbol, e);
            }
            Err(_) => {
                timeouts += 1;
                warn!(
                    "Gate subscription timeout on {}; proceeding with available streams",
                    symbol
                );
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(SUBSCRIBE_DELAY_MS)).await;
    }
    info!(
        "Gate subscription summary: ok={} err={} timeout={}",
        ok, errs, timeouts
    );
}

pub(super) async fn drain_stale_ticks(binance: &mut BinanceMarketData, gate: &mut GateMarketData) {
    let stale_binance = binance.drain_book_tickers().len();
    let stale_gate = gate.drain_book_tickers().len();
    if stale_binance + stale_gate > 0 {
        info!(
            "Drained {} stale startup ticks (binance={} gate={})",
            stale_binance + stale_gate,
            stale_binance,
            stale_gate
        );
    }
}

async fn refresh_gate_natr_batch(
    screener: &ScreenerStore,
    symbols: &[String],
    start_idx: usize,
) -> usize {
    if symbols.is_empty() {
        return 0;
    }

    let batch_size = GATE_NATR_BATCH_SIZE.min(symbols.len());
    let rest = GateRestClient::new();
    let mut updates: Vec<(String, f64)> = Vec::with_capacity(batch_size);
    let mut fetched = 0usize;
    let mut missing = 0usize;

    for offset in 0..batch_size {
        let idx = (start_idx + offset) % symbols.len();
        let symbol = &symbols[idx];
        let natr = match tokio::time::timeout(
            tokio::time::Duration::from_millis(GATE_NATR_REQUEST_TIMEOUT_MS),
            rest.get_natr_30m(symbol, GATE_NATR_PERIOD_30M),
        )
        .await
        {
            Ok(Ok(Some(v))) if v.is_finite() && v >= 0.0 => Some(v),
            _ => None,
        };
        if let Some(v) = natr {
            updates.push((symbol.clone(), v));
            fetched += 1;
        } else {
            updates.push((symbol.clone(), 0.0));
            missing += 1;
        }
    }

    screener.set_gate_natr_30m(&updates);
    info!(
        "Gate NATR refresh: fetched={} missing={} batch={} symbols={}",
        fetched,
        missing,
        batch_size,
        symbols.len()
    );

    (start_idx + batch_size) % symbols.len()
}

pub(super) fn spawn_gate_natr_refresher(screener: ScreenerStore, symbols: Vec<String>) {
    if symbols.is_empty() {
        warn!("Gate NATR refresher skipped: no symbols");
        return;
    }

    tokio::spawn(async move {
        let mut idx = 0usize;
        loop {
            idx = refresh_gate_natr_batch(&screener, &symbols, idx).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(
                GATE_NATR_REFRESH_INTERVAL_SECS,
            ))
            .await;
        }
    });
}

pub(super) async fn configure_and_connect_exchanges(
    config_manager: &ConfigManager,
    binance: &mut BinanceMarketData,
    gate: &mut GateMarketData,
    health_state: &HealthState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(creds) = config_manager.binance_credentials() {
        binance.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("Binance credentials configured");
    }
    if let Some(creds) = config_manager.gate_credentials() {
        gate.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("Gate credentials configured");
    }

    info!("Connecting to Binance Futures...");
    if let Err(e) = binance.connect().await {
        error!("Failed to connect to Binance: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }
    health_state
        .binance_connected
        .store(true, Ordering::Relaxed);
    health_state
        .binance_last_tick_ms
        .store(EventLoopState::now_ms(), Ordering::Relaxed);

    info!("Connecting to Gate.io Futures...");
    if let Err(e) = gate.connect().await {
        error!("Failed to connect to Gate: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }
    health_state.gate_connected.store(true, Ordering::Relaxed);
    health_state
        .gate_last_tick_ms
        .store(EventLoopState::now_ms(), Ordering::Relaxed);
    Ok(())
}

pub(super) fn init_screener_persistence(
    screener: &mut ScreenerStore,
    db_path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)?;
    let fleet_configs = screener.fleet_configs();
    hft_lead_lag::infrastructure::db::upsert_configs(&conn, fleet_configs.as_ref())?;
    let persisted_portfolios = hft_lead_lag::infrastructure::db::load_portfolio_state_v1(&conn)?;
    let persisted_guards = hft_lead_lag::infrastructure::db::load_portfolio_guards_v1(&conn)?;
    if !persisted_portfolios.is_empty() || !persisted_guards.is_empty() {
        screener.restore_portfolio_runtime_v1_from_db_rows(
            &persisted_portfolios,
            &persisted_guards,
        );
        info!(
            "Restored portfolio runtime snapshot: states={} guards={}",
            persisted_portfolios.len(),
            persisted_guards.len()
        );
    }
    info!(
        "Seeded {} fleet configs into {}",
        fleet_configs.len(),
        db_path.display()
    );
    let db_writer = hft_lead_lag::infrastructure::db::spawn_writer(db_path);
    screener.set_db_writer(db_writer);
    Ok(())
}

pub(super) async fn start_api_servers(
    min_volume_usd: f64,
    screener: ScreenerStore,
    health_state: Arc<HealthState>,
) -> Result<tokio::sync::broadcast::Sender<MarketDataEvent>, Box<dyn std::error::Error + Send + Sync>>
{
    let http_server = HttpServer::with_runtime(
        HttpServerConfig::default(),
        min_volume_usd,
        screener,
        health_state,
    );
    let http_listener = tokio::net::TcpListener::bind(http_server.bind_address()).await?;
    info!("HTTP server bound on {}", http_server.bind_address());

    let ws_server = MarketDataServer::new(WsServerConfig::default());
    let ws_tx = ws_server.transmitter();
    let ws_listener = tokio::net::TcpListener::bind(ws_server.bind_address()).await?;
    info!("WS server bound on {}", ws_server.bind_address());

    tokio::spawn(async move {
        if let Err(e) = http_server.serve(http_listener).await {
            error!("HTTP server failed: {}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = ws_server.serve(ws_listener).await {
            error!("WS server failed: {}", e);
        }
    });

    Ok(ws_tx)
}
