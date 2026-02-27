use super::{rebuild_latest_map, EventLoopMetrics, MarketDataEvent, ScreenerStore};
use bytes::Bytes;
use std::collections::HashSet;

#[cfg(test)]
pub(super) fn strategy_ticks_in_order<'a>(
    strategy_symbols: &'a [&'a Bytes],
    latest: &'a std::collections::HashMap<Bytes, hft_lead_lag::domain::BookTicker>,
) -> impl Iterator<Item = &'a hft_lead_lag::domain::BookTicker> + 'a {
    strategy_symbols
        .iter()
        .filter_map(|symbol| latest.get(*symbol))
}

pub(super) fn updated_symbols_from_batch(
    first: &hft_lead_lag::domain::BookTicker,
    drained: &[hft_lead_lag::domain::BookTicker],
) -> Vec<Bytes> {
    let mut symbols = Vec::with_capacity(drained.len() + 1);
    let mut seen: HashSet<Bytes> = HashSet::with_capacity(drained.len() + 1);
    if seen.insert(first.symbol.clone()) {
        symbols.push(first.symbol.clone());
    }
    for ticker in drained {
        if seen.insert(ticker.symbol.clone()) {
            symbols.push(ticker.symbol.clone());
        }
    }
    symbols
}

pub(super) fn ingest_latest_batch<F: Fn() -> i64>(
    latest: &std::collections::HashMap<Bytes, hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    for (symbol, ticker) in latest {
        let Ok(symbol_str) = std::str::from_utf8(symbol) else {
            continue;
        };
        *ctx.ticker_count += 1;
        ctx.metrics
            .record_tick_drift((ctx.now_ms)(), ticker.exchange_ts_ns);
        let bid = ticker.bid_price();
        let ask = ticker.ask_price();
        ctx.screener.update(
            symbol_str,
            ctx.exchange,
            bid,
            ask,
            ticker.exchange_ts_ns,
            ticker.local_ts_ns,
        );
        if let Some(ws_tx) = ctx.ws_tx {
            let _ = ws_tx.send(MarketDataEvent {
                symbol: symbol_str.to_string(),
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
    latest: &mut std::collections::HashMap<Bytes, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    let updated_batch = rebuild_latest_map(latest, first, drained);
    ingest_latest_batch(&updated_batch, ctx);
}
