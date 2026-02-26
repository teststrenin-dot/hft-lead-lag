use super::{
    BinanceMarketData, EventLoopState, ExchangeSide, GateMarketData, HealthState, MarketDataEvent,
    RuntimeStrategy, ScreenerStore, StrategyExchangeRouting,
};
use hft_lead_lag::MarketDataStream;
use std::time::Duration;

const PORTFOLIO_REBALANCE_SCHEDULER_INTERVAL_MS: u64 = 2 * 60 * 1000;

async fn handle_exchange_tick(
    state: &mut EventLoopState,
    side: ExchangeSide,
    result: Result<hft_lead_lag::domain::BookTicker, hft_lead_lag::domain::ExchangeError>,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    context: &ExchangeTickContext<'_, '_>,
) {
    match state.process_exchange_result(side, result, drained, context.screener, context.ws_tx) {
        Ok(updated_symbols) => {
            side.mark_alive(context.health_state, EventLoopState::now_ms());
            state.mark_pending_signal_symbols(&updated_symbols, context.strategy_symbol_set);
            state
                .update_strategy_books(
                    side,
                    context.strategy,
                    &updated_symbols,
                    context.strategy_symbol_set,
                    context.strategy_exchange_routing,
                )
                .await;
        }
        Err(e) => {
            side.maybe_mark_disconnected(context.health_state, &e);
            side.log_data_error(&e);
        }
    }
}

struct ExchangeTickContext<'a, 's> {
    strategy: &'a dyn RuntimeStrategy,
    strategy_symbol_set: &'a std::collections::HashSet<&'s str>,
    strategy_exchange_routing: StrategyExchangeRouting,
    screener: &'a ScreenerStore,
    health_state: &'a HealthState,
    ws_tx: Option<&'a tokio::sync::broadcast::Sender<MarketDataEvent>>,
}

pub(super) struct EventLoopRuntimeContext<'a> {
    pub(super) strategy_exchange_routing: StrategyExchangeRouting,
    pub(super) screener: &'a ScreenerStore,
    pub(super) health_state: &'a HealthState,
    pub(super) ws_tx: Option<&'a tokio::sync::broadcast::Sender<MarketDataEvent>>,
}

pub(super) async fn run_event_loop(
    binance: &mut BinanceMarketData,
    gate: &mut GateMarketData,
    strategy: &dyn RuntimeStrategy,
    strategy_symbols: &[String],
    runtime_context: EventLoopRuntimeContext<'_>,
) -> ! {
    let mut state = EventLoopState::new();
    let mut portfolio_rebalance_interval = tokio::time::interval(Duration::from_millis(
        PORTFOLIO_REBALANCE_SCHEDULER_INTERVAL_MS,
    ));
    portfolio_rebalance_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let strategy_symbol_set: std::collections::HashSet<&str> =
        strategy_symbols.iter().map(String::as_str).collect();
    let tick_context = ExchangeTickContext {
        strategy,
        strategy_symbol_set: &strategy_symbol_set,
        strategy_exchange_routing: runtime_context.strategy_exchange_routing,
        screener: runtime_context.screener,
        health_state: runtime_context.health_state,
        ws_tx: runtime_context.ws_tx,
    };

    loop {
        tokio::select! {
            result = binance.recv_book_ticker() => {
                handle_exchange_tick(
                    &mut state,
                    ExchangeSide::Binance,
                    result,
                    binance.drain_book_tickers(),
                    &tick_context,
                ).await;
            }

            result = gate.recv_book_ticker() => {
                handle_exchange_tick(
                    &mut state,
                    ExchangeSide::Gate,
                    result,
                    gate.drain_book_tickers(),
                    &tick_context,
                ).await;
            }

            _ = state.signal_interval.tick() => {
                state.handle_signal_tick(strategy).await;
            }

            _ = portfolio_rebalance_interval.tick() => {
                runtime_context
                    .screener
                    .portfolio_scheduler_tick_v1(EventLoopState::now_ms());
            }
        }
    }
}
