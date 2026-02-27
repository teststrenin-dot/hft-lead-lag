#[cfg(test)]
use super::rebuild_latest_map;
use super::{EventLoopMetrics, MarketDataEvent, ScreenerStore, StrategySymbolIndex, SymbolId};
#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn updated_strategy_symbol_ids_from_batch(
    first: &hft_lead_lag::domain::BookTicker,
    drained: &[hft_lead_lag::domain::BookTicker],
    strategy_symbol_index: &StrategySymbolIndex,
) -> Vec<SymbolId> {
    let mut ids = Vec::with_capacity(drained.len() + 1);
    let mut seen: HashSet<SymbolId> = HashSet::with_capacity(drained.len() + 1);
    if let Some(symbol_id) = strategy_symbol_index.symbol_id(first.symbol.as_ref()) {
        if seen.insert(symbol_id) {
            ids.push(symbol_id);
        }
    }
    for ticker in drained {
        if let Some(symbol_id) = strategy_symbol_index.symbol_id(ticker.symbol.as_ref()) {
            if seen.insert(symbol_id) {
                ids.push(symbol_id);
            }
        }
    }
    ids
}

pub(super) fn strategy_symbol_updates_from_batch(
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    strategy_symbol_index: &StrategySymbolIndex,
) -> (
    Vec<SymbolId>,
    Vec<(SymbolId, hft_lead_lag::domain::BookTicker)>,
) {
    let mut ids = Vec::with_capacity(drained.len() + 1);
    let mut updates = Vec::with_capacity(drained.len() + 1);
    let mut seen: HashSet<SymbolId> = HashSet::with_capacity(drained.len() + 1);

    let mut push = |ticker: hft_lead_lag::domain::BookTicker| {
        let Some(symbol_id) = ticker
            .strategy_symbol_id
            .or_else(|| strategy_symbol_index.symbol_id(ticker.symbol.as_ref()))
        else {
            return;
        };
        if seen.insert(symbol_id) {
            ids.push(symbol_id);
        }
        updates.push((symbol_id, ticker));
    };

    push(first);
    for ticker in drained {
        push(ticker);
    }

    (ids, updates)
}

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn process_exchange_batch<F: Fn() -> i64>(
    latest: &mut std::collections::HashMap<Bytes, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    let updated_batch = rebuild_latest_map(latest, first, drained);
    ingest_latest_batch(&updated_batch, ctx);
}

fn ingest_ticker<F: Fn() -> i64>(
    ticker: &hft_lead_lag::domain::BookTicker,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    let Ok(symbol_str) = std::str::from_utf8(ticker.symbol.as_ref()) else {
        return;
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

pub(super) fn ingest_exchange_batch<F: Fn() -> i64>(
    first: &hft_lead_lag::domain::BookTicker,
    drained: &[hft_lead_lag::domain::BookTicker],
    ctx: &mut BatchIngestContext<'_, F>,
) {
    ingest_ticker(first, ctx);
    for ticker in drained {
        ingest_ticker(ticker, ctx);
    }
}
