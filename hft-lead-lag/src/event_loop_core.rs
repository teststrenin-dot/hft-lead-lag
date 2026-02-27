use super::{
    process_exchange_batch, updated_symbols_from_batch, BatchIngestContext, ConfigManager,
    HealthState, MarketDataEvent, RuntimeStrategy, ScreenerStore, SIGNAL_CHECK_BUDGET_PER_TICK,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy, Default)]
struct LatencyStatsSnapshot {
    samples: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SymbolStageTimestamps {
    recv_ws_frame_ts_ns: i64,
    parsed_ts_ns: i64,
    state_updated_ts_ns: i64,
}

#[derive(Debug)]
pub(super) struct EventLoopMetrics {
    drift_samples: Vec<i64>,
    ingest_latency_us: Vec<u64>,
    decision_latency_us: Vec<u64>,
    end_to_end_latency_us: Vec<u64>,
    last_status_ticker_count: usize,
}

impl EventLoopMetrics {
    pub(super) fn new() -> Self {
        Self {
            drift_samples: Vec::with_capacity(8192),
            ingest_latency_us: Vec::with_capacity(8192),
            decision_latency_us: Vec::with_capacity(8192),
            end_to_end_latency_us: Vec::with_capacity(8192),
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

    pub(super) fn record_ingest_latency_ns(&mut self, recv_ws_frame_ts_ns: i64, parsed_ts_ns: i64) {
        if recv_ws_frame_ts_ns <= 0 || parsed_ts_ns <= recv_ws_frame_ts_ns {
            return;
        }
        self.ingest_latency_us
            .push((parsed_ts_ns.saturating_sub(recv_ws_frame_ts_ns) as u64) / 1_000);
    }

    pub(super) fn record_decision_latency_ns(
        &mut self,
        state_updated_ts_ns: i64,
        signal_decided_ts_ns: i64,
    ) {
        if state_updated_ts_ns <= 0 || signal_decided_ts_ns <= state_updated_ts_ns {
            return;
        }
        self.decision_latency_us
            .push((signal_decided_ts_ns.saturating_sub(state_updated_ts_ns) as u64) / 1_000);
    }

    pub(super) fn record_end_to_end_latency_ns(
        &mut self,
        recv_ws_frame_ts_ns: i64,
        signal_decided_ts_ns: i64,
    ) {
        if recv_ws_frame_ts_ns <= 0 || signal_decided_ts_ns <= recv_ws_frame_ts_ns {
            return;
        }
        self.end_to_end_latency_us
            .push((signal_decided_ts_ns.saturating_sub(recv_ws_frame_ts_ns) as u64) / 1_000);
    }

    fn latency_snapshot_and_reset(samples: &mut Vec<u64>) -> LatencyStatsSnapshot {
        if samples.is_empty() {
            return LatencyStatsSnapshot::default();
        }
        samples.sort_unstable();
        let n = samples.len();
        let p50 = samples[n / 2];
        let p95 = samples[n * 95 / 100];
        let p99 = samples[n * 99 / 100];
        let max = samples[n - 1];
        samples.clear();
        LatencyStatsSnapshot {
            samples: n as u64,
            p50_us: p50,
            p95_us: p95,
            p99_us: p99,
            max_us: max,
        }
    }

    fn latency_snapshots_and_reset(
        &mut self,
    ) -> (
        LatencyStatsSnapshot,
        LatencyStatsSnapshot,
        LatencyStatsSnapshot,
    ) {
        let ingest = Self::latency_snapshot_and_reset(&mut self.ingest_latency_us);
        let decision = Self::latency_snapshot_and_reset(&mut self.decision_latency_us);
        let end_to_end = Self::latency_snapshot_and_reset(&mut self.end_to_end_latency_us);
        (ingest, decision, end_to_end)
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
    pub(super) latest_bn: std::collections::HashMap<Bytes, hft_lead_lag::domain::BookTicker>,
    pub(super) latest_gt: std::collections::HashMap<Bytes, hft_lead_lag::domain::BookTicker>,
    pub(super) pending_signal_symbols: std::collections::BTreeSet<SymbolId>,
    symbol_stage_timestamps: std::collections::HashMap<SymbolId, SymbolStageTimestamps>,
    pub(super) metrics: EventLoopMetrics,
}

pub(super) type SymbolId = u16;

pub(super) struct StrategySymbolIndex {
    symbol_to_id: HashMap<Bytes, SymbolId>,
    id_to_symbol: Vec<String>,
}

impl StrategySymbolIndex {
    pub(super) fn new(strategy_symbols: &[String]) -> Self {
        let mut symbol_to_id = HashMap::with_capacity(strategy_symbols.len());
        let mut id_to_symbol = Vec::with_capacity(strategy_symbols.len());

        for symbol in strategy_symbols {
            let key = Bytes::copy_from_slice(symbol.as_bytes());
            if symbol_to_id.contains_key(&key) {
                continue;
            }
            if id_to_symbol.len() >= SymbolId::MAX as usize {
                break;
            }
            let symbol_id = id_to_symbol.len() as SymbolId;
            symbol_to_id.insert(key, symbol_id);
            id_to_symbol.push(symbol.clone());
        }

        Self {
            symbol_to_id,
            id_to_symbol,
        }
    }

    pub(super) fn symbol_id(&self, symbol: &[u8]) -> Option<SymbolId> {
        self.symbol_to_id.get(symbol).copied()
    }

    pub(super) fn symbol(&self, symbol_id: SymbolId) -> Option<&str> {
        self.id_to_symbol
            .get(symbol_id as usize)
            .map(String::as_str)
    }
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
            symbol_stage_timestamps: std::collections::HashMap::new(),
            metrics: EventLoopMetrics::new(),
        }
    }

    pub(super) fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    pub(super) fn now_ns() -> i64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }

    pub(super) fn process_exchange_result(
        &mut self,
        side: ExchangeSide,
        result: Result<hft_lead_lag::domain::BookTicker, hft_lead_lag::domain::ExchangeError>,
        drained: Vec<hft_lead_lag::domain::BookTicker>,
        strategy_symbol_index: &StrategySymbolIndex,
        screener: &ScreenerStore,
        ws_tx: Option<&tokio::sync::broadcast::Sender<MarketDataEvent>>,
    ) -> Result<Vec<Bytes>, hft_lead_lag::domain::ExchangeError> {
        let parsed_ts_ns = Self::now_ns();
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
        let state_updated_ts_ns = Self::now_ns();
        self.record_stage_timestamps_for_batch(
            side,
            &updated_symbols,
            strategy_symbol_index,
            parsed_ts_ns,
            state_updated_ts_ns,
        );
        Ok(updated_symbols)
    }

    fn record_stage_timestamps_for_batch(
        &mut self,
        side: ExchangeSide,
        updated_symbols: &[Bytes],
        strategy_symbol_index: &StrategySymbolIndex,
        parsed_ts_ns: i64,
        state_updated_ts_ns: i64,
    ) {
        let latest = match side {
            ExchangeSide::Binance => &self.latest_bn,
            ExchangeSide::Gate => &self.latest_gt,
        };

        for symbol in updated_symbols {
            let Some(symbol_id) = strategy_symbol_index.symbol_id(symbol) else {
                continue;
            };
            let Some(ticker) = latest.get(symbol) else {
                continue;
            };

            let recv_ws_frame_ts_ns = ticker.local_ts_ns;
            self.metrics
                .record_ingest_latency_ns(recv_ws_frame_ts_ns, parsed_ts_ns);

            self.symbol_stage_timestamps.insert(
                symbol_id,
                SymbolStageTimestamps {
                    recv_ws_frame_ts_ns,
                    parsed_ts_ns,
                    state_updated_ts_ns,
                },
            );
        }
    }

    pub(super) fn sync_stage_timestamps_to_health(
        &self,
        updated_symbols: &[Bytes],
        strategy_symbol_index: &StrategySymbolIndex,
        health: &HealthState,
    ) {
        for symbol in updated_symbols {
            let Some(symbol_id) = strategy_symbol_index.symbol_id(symbol) else {
                continue;
            };
            let Some(stages) = self.symbol_stage_timestamps.get(&symbol_id) else {
                continue;
            };
            health
                .runtime_last_recv_ws_frame_ts_ns
                .store(stages.recv_ws_frame_ts_ns, Ordering::Relaxed);
            health
                .runtime_last_parsed_ts_ns
                .store(stages.parsed_ts_ns, Ordering::Relaxed);
            health
                .runtime_last_state_updated_ts_ns
                .store(stages.state_updated_ts_ns, Ordering::Relaxed);
        }
    }

    pub(super) async fn update_strategy_books(
        &self,
        side: ExchangeSide,
        strategy: &dyn RuntimeStrategy,
        updated_symbols: &[Bytes],
        strategy_symbol_index: &StrategySymbolIndex,
        strategy_exchange_routing: StrategyExchangeRouting,
    ) {
        for symbol in updated_symbols {
            if strategy_symbol_index.symbol_id(symbol).is_none() {
                continue;
            }
            let ticker = match side {
                ExchangeSide::Binance => self.latest_bn.get(symbol),
                ExchangeSide::Gate => self.latest_gt.get(symbol),
            };
            let Some(ticker) = ticker else {
                continue;
            };

            match strategy_exchange_routing.role_for_side(side) {
                StrategyBookRole::Primary => strategy.on_primary_book(ticker.clone()).await,
                StrategyBookRole::Hedge => strategy.on_hedge_book(ticker.clone()).await,
            }
        }
    }

    pub(super) fn mark_pending_signal_symbols(
        &mut self,
        updated_symbols: &[Bytes],
        strategy_symbol_index: &StrategySymbolIndex,
    ) {
        for symbol in updated_symbols {
            if let Some(symbol_id) = strategy_symbol_index.symbol_id(symbol) {
                self.pending_signal_symbols.insert(symbol_id);
            }
        }
    }

    pub(super) fn signal_backlog_depth(&self) -> u64 {
        self.pending_signal_symbols.len() as u64
    }

    pub(super) async fn handle_signal_tick(
        &mut self,
        strategy: &dyn RuntimeStrategy,
        strategy_symbol_index: &StrategySymbolIndex,
        health: &HealthState,
    ) {
        if self.pending_signal_symbols.is_empty() {
            self.maybe_log_status(health);
            return;
        }
        for _ in 0..SIGNAL_CHECK_BUDGET_PER_TICK {
            let Some(symbol_id) = self.pending_signal_symbols.pop_first() else {
                break;
            };
            let Some(symbol) = strategy_symbol_index.symbol(symbol_id) else {
                continue;
            };
            let signal = strategy.check_signal(symbol).await;
            let signal_decided_ts_ns = Self::now_ns();
            health
                .runtime_last_signal_decided_ts_ns
                .store(signal_decided_ts_ns, Ordering::Relaxed);

            if let Some(stages) = self.symbol_stage_timestamps.remove(&symbol_id) {
                self.metrics
                    .record_decision_latency_ns(stages.state_updated_ts_ns, signal_decided_ts_ns);
                self.metrics
                    .record_end_to_end_latency_ns(stages.recv_ws_frame_ts_ns, signal_decided_ts_ns);
            }

            if let Some(signal) = signal {
                self.signal_count += 1;
                // Proxy for CP0 until execution queue is introduced in CP6.
                health
                    .runtime_last_order_intent_enqueued_ts_ns
                    .store(signal_decided_ts_ns, Ordering::Relaxed);
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
        health
            .runtime_signal_backlog_depth
            .store(self.signal_backlog_depth(), Ordering::Relaxed);
        self.maybe_log_status(health);
    }

    fn maybe_log_status(&mut self, health: &HealthState) {
        if self.last_status_at.elapsed() >= Duration::from_secs(5) {
            let interval_tickers = self.metrics.snapshot_and_roll_status(self.ticker_count);
            let drift_stats = self.metrics.drift_stats_string_and_reset();
            let (ingest, decision, end_to_end) = self.metrics.latency_snapshots_and_reset();

            health
                .runtime_ingest_samples
                .store(ingest.samples, Ordering::Relaxed);
            health
                .runtime_ingest_p50_us
                .store(ingest.p50_us, Ordering::Relaxed);
            health
                .runtime_ingest_p95_us
                .store(ingest.p95_us, Ordering::Relaxed);
            health
                .runtime_ingest_p99_us
                .store(ingest.p99_us, Ordering::Relaxed);
            health
                .runtime_ingest_max_us
                .store(ingest.max_us, Ordering::Relaxed);

            health
                .runtime_decision_samples
                .store(decision.samples, Ordering::Relaxed);
            health
                .runtime_decision_p50_us
                .store(decision.p50_us, Ordering::Relaxed);
            health
                .runtime_decision_p95_us
                .store(decision.p95_us, Ordering::Relaxed);
            health
                .runtime_decision_p99_us
                .store(decision.p99_us, Ordering::Relaxed);
            health
                .runtime_decision_max_us
                .store(decision.max_us, Ordering::Relaxed);

            health
                .runtime_end_to_end_samples
                .store(end_to_end.samples, Ordering::Relaxed);
            health
                .runtime_end_to_end_p50_us
                .store(end_to_end.p50_us, Ordering::Relaxed);
            health
                .runtime_end_to_end_p95_us
                .store(end_to_end.p95_us, Ordering::Relaxed);
            health
                .runtime_end_to_end_p99_us
                .store(end_to_end.p99_us, Ordering::Relaxed);
            health
                .runtime_end_to_end_max_us
                .store(end_to_end.max_us, Ordering::Relaxed);

            info!(
                "Status: tickers={} (+{}/5s) signals={} drift=[{}] lat_us[ingest:samples={} p99={} decision:samples={} p99={} e2e:samples={} p99={}]",
                self.ticker_count,
                interval_tickers,
                self.signal_count,
                drift_stats,
                ingest.samples,
                ingest.p99_us,
                decision.samples,
                decision.p99_us,
                end_to_end.samples,
                end_to_end.p99_us
            );
            self.last_status_at = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_symbol_index_assigns_stable_ids() {
        let index = StrategySymbolIndex::new(&[
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ]);

        assert_eq!(index.symbol_id("BTCUSDT".as_bytes()), Some(0));
        assert_eq!(index.symbol_id("ETHUSDT".as_bytes()), Some(1));
        assert_eq!(index.symbol_id("SOLUSDT".as_bytes()), Some(2));
        assert_eq!(index.symbol_id("DOGEUSDT".as_bytes()), None);
    }

    #[test]
    fn strategy_symbol_index_resolves_symbol_by_id() {
        let index = StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        assert_eq!(index.symbol(0), Some("BTCUSDT"));
        assert_eq!(index.symbol(1), Some("ETHUSDT"));
        assert_eq!(index.symbol(9), None);
    }
}
