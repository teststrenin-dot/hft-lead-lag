use super::{rebuild_latest_map, EventLoopMetrics, MarketDataEvent, ScreenerStore};

pub(super) fn strategy_ticks_in_order<'a>(
    strategy_symbols: &'a [&'a str],
    latest: &'a std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
) -> impl Iterator<Item = &'a hft_lead_lag::domain::BookTicker> + 'a {
    strategy_symbols
        .iter()
        .filter_map(|symbol| latest.get(*symbol))
}

pub(super) fn updated_symbols_from_batch(
    first: &hft_lead_lag::domain::BookTicker,
    drained: &[hft_lead_lag::domain::BookTicker],
) -> Vec<String> {
    let mut symbols = Vec::with_capacity(drained.len() + 1);
    symbols.push(String::from_utf8_lossy(&first.symbol).to_string());
    for ticker in drained {
        symbols.push(String::from_utf8_lossy(&ticker.symbol).to_string());
    }
    symbols.sort_unstable();
    symbols.dedup();
    symbols
}

pub(super) fn ingest_latest_batch<F: Fn() -> i64>(
    latest: &std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    for (symbol, ticker) in latest {
        *ctx.ticker_count += 1;
        ctx.metrics
            .record_tick_drift((ctx.now_ms)(), ticker.exchange_ts_ns);
        let bid = ticker.bid_price();
        let ask = ticker.ask_price();
        ctx.screener.update(
            symbol,
            ctx.exchange,
            bid,
            ask,
            ticker.exchange_ts_ns,
            ticker.local_ts_ns,
        );
        if let Some(ws_tx) = ctx.ws_tx {
            let _ = ws_tx.send(MarketDataEvent {
                symbol: symbol.clone(),
                exchange: ctx.exchange,
                bid,
                ask,
                timestamp_ns: ticker.exchange_ts_ns,
            });
        }
    }
}

pub(super) struct BatchIngestContext<'a, F: Fn() -> i64> {
    pub(super) exchange: &'static str,
    pub(super) ticker_count: &'a mut usize,
    pub(super) metrics: &'a mut EventLoopMetrics,
    pub(super) now_ms: &'a F,
    pub(super) screener: &'a ScreenerStore,
    pub(super) ws_tx: Option<&'a tokio::sync::broadcast::Sender<MarketDataEvent>>,
}

pub(super) fn process_exchange_batch<F: Fn() -> i64>(
    latest: &mut std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    let updated_batch = rebuild_latest_map(latest, first, drained);
    ingest_latest_batch(&updated_batch, ctx);
}
