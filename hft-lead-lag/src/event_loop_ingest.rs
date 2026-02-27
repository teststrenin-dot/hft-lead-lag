#[cfg(test)]
use super::rebuild_latest_map;
use super::{EventLoopMetrics, MarketDataEvent, ScreenerStore, StrategySymbolIndex, SymbolId};
#[cfg(test)]
use bytes::Bytes;
use std::collections::HashMap;
#[cfg(test)]
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
    let mut positions: HashMap<SymbolId, usize> = HashMap::with_capacity(drained.len() + 1);

    let mut push = |ticker: hft_lead_lag::domain::BookTicker| {
        let Some(symbol_id) = resolve_strategy_symbol_id(&ticker, strategy_symbol_index) else {
            return;
        };
        if let Some(idx) = positions.get(&symbol_id).copied() {
            updates[idx] = (symbol_id, ticker);
            return;
        }
        positions.insert(symbol_id, updates.len());
        ids.push(symbol_id);
        updates.push((symbol_id, ticker));
    };

    push(first);
    for ticker in drained {
        push(ticker);
    }

    (ids, updates)
}

#[inline]
fn resolve_strategy_symbol_id(
    ticker: &hft_lead_lag::domain::BookTicker,
    strategy_symbol_index: &StrategySymbolIndex,
) -> Option<SymbolId> {
    #[cfg(test)]
    {
        ticker
            .strategy_symbol_id
            .or_else(|| strategy_symbol_index.symbol_id(ticker.symbol.as_ref()))
    }
    #[cfg(not(test))]
    {
        let _ = strategy_symbol_index;
        ticker.strategy_symbol_id
    }
}

pub(super) fn ingest_exchange_batch<F: Fn() -> i64>(
    first: &hft_lead_lag::domain::BookTicker,
    drained: &[hft_lead_lag::domain::BookTicker],
    ctx: &mut BatchIngestContext<'_, F>,
) {
    let mut positions: HashMap<bytes::Bytes, usize> = HashMap::with_capacity(drained.len() + 1);
    let mut latest: Vec<&hft_lead_lag::domain::BookTicker> = Vec::with_capacity(drained.len() + 1);

    for ticker in std::iter::once(first).chain(drained.iter()) {
        if let Some(idx) = positions.get(&ticker.symbol).copied() {
            latest[idx] = ticker;
            continue;
        }
        positions.insert(ticker.symbol.clone(), latest.len());
        latest.push(ticker);
    }

    for ticker in latest {
        ingest_ticker(ticker, ctx);
    }
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
