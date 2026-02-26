use super::{
    process_exchange_batch, strategy_ticks_in_order, updated_symbols_from_batch,
    BatchIngestContext, ConfigManager, HealthState, MarketDataEvent, RuntimeStrategy,
    ScreenerStore, SIGNAL_CHECK_BUDGET_PER_TICK,
};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

#[derive(Debug)]
pub(super) struct EventLoopMetrics {
    drift_samples: Vec<i64>,
    last_status_ticker_count: usize,
}

impl EventLoopMetrics {
    pub(super) fn new() -> Self {
        Self {
            drift_samples: Vec::with_capacity(8192),
            last_status_ticker_count: 0,
        }
    }

    pub(super) fn record_tick_drift(&mut self, local_ms: i64, exchange_ts_ns: i64) {
        let exch_ms = exchange_ts_ns / 1_000_000;
        if exch_ms > 0 {
            self.drift_samples.push(local_ms - exch_ms);
        }
    }

    pub(super) fn drift_stats_string_and_reset(&mut self) -> String {
        if self.drift_samples.is_empty() {
            return "no_data".to_string();
        }

        self.drift_samples.sort_unstable();
        let n = self.drift_samples.len();
        let p50 = self.drift_samples[n / 2];
        let p95 = self.drift_samples[n * 95 / 100];
        let p99 = self.drift_samples[n * 99 / 100];
        let max = self.drift_samples[n - 1];
        let avg = self.drift_samples.iter().sum::<i64>() / n as i64;
        self.drift_samples.clear();
        format!(
            "n={} avg={}ms p50={}ms p95={}ms p99={}ms max={}ms",
            n, avg, p50, p95, p99, max
        )
    }

    pub(super) fn snapshot_and_roll_status(&mut self, ticker_count: usize) -> usize {
        let interval_tickers = ticker_count.saturating_sub(self.last_status_ticker_count);
        self.last_status_ticker_count = ticker_count;
        interval_tickers
    }
}

pub(super) struct EventLoopState {
    pub(super) ticker_count: usize,
    pub(super) signal_count: usize,
    last_status_at: Instant,
    pub(super) signal_interval: tokio::time::Interval,
    pub(super) latest_bn: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    pub(super) latest_gt: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    pub(super) pending_signal_symbols: std::collections::BTreeSet<String>,
    pub(super) metrics: EventLoopMetrics,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExchangeSide {
    Binance,
    Gate,
}

impl ExchangeSide {
    pub(super) fn exchange_name(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Gate => "gate",
        }
    }

    fn from_config_exchange(exchange: hft_lead_lag::config::ExchangeId) -> Self {
        match exchange {
            hft_lead_lag::config::ExchangeId::Binance => Self::Binance,
            hft_lead_lag::config::ExchangeId::Gate => Self::Gate,
        }
    }

    pub(super) fn log_data_error(self, error: &hft_lead_lag::domain::ExchangeError) {
        match self {
            Self::Binance => error!("Binance data error: {}", error),
            Self::Gate => warn!("Gate data error: {}", error),
        }
    }

    pub(super) fn mark_alive(self, health: &HealthState, now_ms: i64) {
        match self {
            Self::Binance => {
                health.binance_connected.store(true, Ordering::Relaxed);
                health.binance_last_tick_ms.store(now_ms, Ordering::Relaxed);
            }
            Self::Gate => {
                health.gate_connected.store(true, Ordering::Relaxed);
                health.gate_last_tick_ms.store(now_ms, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn maybe_mark_disconnected(
        self,
        health: &HealthState,
        error: &hft_lead_lag::domain::ExchangeError,
    ) {
        let is_connectivity_error = matches!(
            error,
            hft_lead_lag::domain::ExchangeError::WebSocketError(_)
                | hft_lead_lag::domain::ExchangeError::ConnectionClosed(_)
                | hft_lead_lag::domain::ExchangeError::Timeout(_)
        );
        if !is_connectivity_error {
            return;
        }
        match self {
            Self::Binance => {
                health.binance_connected.store(false, Ordering::Relaxed);
            }
            Self::Gate => {
                health.gate_connected.store(false, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StrategyBookRole {
    Primary,
    Hedge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StrategyExchangeRouting {
    pub(super) primary: ExchangeSide,
    pub(super) hedge: ExchangeSide,
}

impl Default for StrategyExchangeRouting {
    fn default() -> Self {
        Self {
            primary: ExchangeSide::Binance,
            hedge: ExchangeSide::Gate,
        }
    }
}

impl StrategyExchangeRouting {
    fn role_for_side(self, side: ExchangeSide) -> StrategyBookRole {
        if side == self.primary {
            StrategyBookRole::Primary
        } else {
            StrategyBookRole::Hedge
        }
    }
}

pub(super) fn resolve_strategy_exchange_routing(
    config_manager: &ConfigManager,
) -> StrategyExchangeRouting {
    let default = StrategyExchangeRouting::default();
    let Some(lead_lag_config) = config_manager.lead_lag_config() else {
        return default;
    };
    let routing = StrategyExchangeRouting {
        primary: ExchangeSide::from_config_exchange(lead_lag_config.primary_exchange),
        hedge: ExchangeSide::from_config_exchange(lead_lag_config.hedge_exchange),
    };
    if routing.primary == routing.hedge {
        warn!(
            "lead_lag config primary and hedge exchanges match ({}); falling back to default routing",
            routing.primary.exchange_name()
        );
        default
    } else {
        routing
    }
}

impl EventLoopState {
    pub(super) fn new() -> Self {
        let mut signal_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        signal_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            ticker_count: 0,
            signal_count: 0,
            last_status_at: Instant::now(),
            signal_interval,
            latest_bn: std::collections::HashMap::new(),
            latest_gt: std::collections::HashMap::new(),
            pending_signal_symbols: std::collections::BTreeSet::new(),
            metrics: EventLoopMetrics::new(),
        }
    }

    pub(super) fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    pub(super) fn process_exchange_result(
        &mut self,
        side: ExchangeSide,
        result: Result<hft_lead_lag::domain::BookTicker, hft_lead_lag::domain::ExchangeError>,
        drained: Vec<hft_lead_lag::domain::BookTicker>,
        screener: &ScreenerStore,
        ws_tx: Option<&tokio::sync::broadcast::Sender<MarketDataEvent>>,
    ) -> Result<Vec<String>, hft_lead_lag::domain::ExchangeError> {
        let ticker = result?;
        let updated_symbols = updated_symbols_from_batch(&ticker, &drained);
        let mut ctx = BatchIngestContext {
            exchange: side.exchange_name(),
            ticker_count: &mut self.ticker_count,
            metrics: &mut self.metrics,
            now_ms: &Self::now_ms,
            screener,
            ws_tx,
        };
        match side {
            ExchangeSide::Binance => {
                process_exchange_batch(&mut self.latest_bn, ticker, drained, &mut ctx)
            }
            ExchangeSide::Gate => {
                process_exchange_batch(&mut self.latest_gt, ticker, drained, &mut ctx)
            }
        }
        Ok(updated_symbols)
    }

    pub(super) async fn update_strategy_books(
        &self,
        side: ExchangeSide,
        strategy: &dyn RuntimeStrategy,
        updated_symbols: &[String],
        strategy_symbol_set: &std::collections::HashSet<&str>,
        strategy_exchange_routing: StrategyExchangeRouting,
    ) {
        let symbols_for_side: Vec<&str> = updated_symbols
            .iter()
            .map(String::as_str)
            .filter(|symbol| strategy_symbol_set.contains(*symbol))
            .collect();

        let ticks: Vec<_> = match side {
            ExchangeSide::Binance => strategy_ticks_in_order(&symbols_for_side, &self.latest_bn)
                .cloned()
                .collect(),
            ExchangeSide::Gate => strategy_ticks_in_order(&symbols_for_side, &self.latest_gt)
                .cloned()
                .collect(),
        };

        match strategy_exchange_routing.role_for_side(side) {
            StrategyBookRole::Primary => {
                for ticker in ticks {
                    strategy.on_primary_book(ticker).await;
                }
            }
            StrategyBookRole::Hedge => {
                for ticker in ticks {
                    strategy.on_hedge_book(ticker).await;
                }
            }
        }
    }

    pub(super) fn mark_pending_signal_symbols(
        &mut self,
        updated_symbols: &[String],
        strategy_symbol_set: &std::collections::HashSet<&str>,
    ) {
        for symbol in updated_symbols {
            let raw = symbol.as_str();
            if strategy_symbol_set.contains(raw) {
                self.pending_signal_symbols.insert(symbol.clone());
            }
        }
    }

    pub(super) async fn handle_signal_tick(&mut self, strategy: &dyn RuntimeStrategy) {
        if self.pending_signal_symbols.is_empty() {
            self.maybe_log_status();
            return;
        }
        for _ in 0..SIGNAL_CHECK_BUDGET_PER_TICK {
            let Some(symbol) = self.pending_signal_symbols.pop_first() else {
                break;
            };
            if let Some(signal) = strategy.check_signal(&symbol).await {
                self.signal_count += 1;
                info!(
                    "{} signal #{}: {} | spread={:.2}bps | dir={} | bid_ask={:.2}bps ask_bid={:.2}bps | {}",
                    signal.strategy,
                    self.signal_count,
                    signal.symbol,
                    signal.spread_bps,
                    signal.direction,
                    signal.bid_ask_bps,
                    signal.ask_bid_bps,
                    signal.context
                );
            }
        }
        self.maybe_log_status();
    }

    fn maybe_log_status(&mut self) {
        if self.last_status_at.elapsed() >= Duration::from_secs(5) {
            let interval_tickers = self.metrics.snapshot_and_roll_status(self.ticker_count);
            let drift_stats = self.metrics.drift_stats_string_and_reset();
            info!(
                "Status: tickers={} (+{}/5s) signals={} drift=[{}]",
                self.ticker_count, interval_tickers, self.signal_count, drift_stats
            );
            self.last_status_at = Instant::now();
        }
    }
}
