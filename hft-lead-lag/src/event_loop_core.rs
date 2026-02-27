use super::{
    ingest_exchange_batch, strategy_symbol_updates_from_batch, BatchIngestContext, ConfigManager,
    HealthState, MarketDataEvent, RuntimeStrategy, ScreenerStore, SIGNAL_CHECK_BUDGET_PER_TICK,
};
#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;
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
    latest_bn_by_symbol_id: Vec<Option<hft_lead_lag::domain::BookTicker>>,
    latest_gt_by_symbol_id: Vec<Option<hft_lead_lag::domain::BookTicker>>,
    pub(super) pending_signal_symbols: std::collections::BTreeSet<SymbolId>,
    symbol_stage_timestamps: std::collections::HashMap<SymbolId, SymbolStageTimestamps>,
    pub(super) metrics: EventLoopMetrics,
}

pub(super) type SymbolId = hft_lead_lag::domain::SymbolId;

pub(super) struct StrategySymbolIndex {
    #[cfg(test)]
    symbol_to_id: HashMap<Bytes, SymbolId>,
    #[cfg(test)]
    id_to_symbol: Vec<String>,
}

impl StrategySymbolIndex {
    pub(super) fn new(strategy_symbols: &[String]) -> Self {
        #[cfg(not(test))]
        {
            let _ = strategy_symbols;
            Self {}
        }

        #[cfg(test)]
        {
            let symbol_to_id = hft_lead_lag::domain::build_strategy_symbol_id_map(strategy_symbols)
                .expect("strategy symbol id map");
            let mut id_to_symbol = vec![String::new(); symbol_to_id.len()];
            for symbol in strategy_symbols {
                let Some(symbol_id) = symbol_to_id.get(symbol.as_bytes()).copied() else {
                    continue;
                };
                let slot = &mut id_to_symbol[symbol_id as usize];
                if slot.is_empty() {
                    *slot = symbol.clone();
                }
            }

            Self {
                symbol_to_id,
                id_to_symbol,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn symbol_id(&self, symbol: &[u8]) -> Option<SymbolId> {
        self.symbol_to_id.get(symbol).copied()
    }

    #[cfg(test)]
    pub(super) fn symbol(&self, symbol_id: SymbolId) -> Option<&str> {
        self.id_to_symbol
            .get(symbol_id as usize)
            .map(String::as_str)
    }

    #[cfg(test)]
    pub(super) fn symbol_ids(&self, updated_symbols: &[Bytes]) -> Vec<SymbolId> {
        let mut ids = Vec::with_capacity(updated_symbols.len());
        let mut seen = HashSet::with_capacity(updated_symbols.len());
        for symbol in updated_symbols {
            let Some(symbol_id) = self.symbol_id(symbol) else {
                continue;
            };
            if seen.insert(symbol_id) {
                ids.push(symbol_id);
            }
        }
        ids
    }
}

pub(super) struct ProcessExchangeResult {
    pub(super) updated_strategy_symbol_ids: Vec<SymbolId>,
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
            latest_bn_by_symbol_id: Vec::new(),
            latest_gt_by_symbol_id: Vec::new(),
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
    ) -> Result<ProcessExchangeResult, hft_lead_lag::domain::ExchangeError> {
        let parsed_ts_ns = Self::now_ns();
        let ticker = result?;
        let mut ctx = BatchIngestContext {
            exchange: side.exchange_name(),
            ticker_count: &mut self.ticker_count,
            metrics: &mut self.metrics,
            now_ms: &Self::now_ms,
            screener,
            ws_tx,
        };
        ingest_exchange_batch(&ticker, &drained, &mut ctx);
        let (updated_strategy_symbol_ids, strategy_updates) =
            strategy_symbol_updates_from_batch(ticker, drained, strategy_symbol_index);
        self.upsert_latest_books_by_symbol_ids(side, strategy_updates);
        let state_updated_ts_ns = Self::now_ns();
        self.record_stage_timestamps_for_batch(
            side,
            &updated_strategy_symbol_ids,
            parsed_ts_ns,
            state_updated_ts_ns,
        );
        Ok(ProcessExchangeResult {
            updated_strategy_symbol_ids,
        })
    }

    fn record_stage_timestamps_for_batch(
        &mut self,
        side: ExchangeSide,
        updated_strategy_symbol_ids: &[SymbolId],
        parsed_ts_ns: i64,
        state_updated_ts_ns: i64,
    ) {
        for symbol_id in updated_strategy_symbol_ids {
            let Some(ticker) = self.latest_book_for_strategy_symbol(side, *symbol_id) else {
                continue;
            };

            let recv_ws_frame_ts_ns = ticker.local_ts_ns;
            self.metrics
                .record_ingest_latency_ns(recv_ws_frame_ts_ns, parsed_ts_ns);

            self.symbol_stage_timestamps.insert(
                *symbol_id,
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
        updated_strategy_symbol_ids: &[SymbolId],
        health: &HealthState,
    ) {
        for symbol_id in updated_strategy_symbol_ids {
            let Some(stages) = self.symbol_stage_timestamps.get(symbol_id) else {
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

    pub(super) fn update_strategy_books(
        &self,
        side: ExchangeSide,
        strategy: &mut dyn RuntimeStrategy,
        updated_strategy_symbol_ids: &[SymbolId],
        strategy_exchange_routing: StrategyExchangeRouting,
    ) {
        for symbol_id in updated_strategy_symbol_ids {
            let Some(ticker) = self.latest_book_for_strategy_symbol(side, *symbol_id) else {
                continue;
            };

            match strategy_exchange_routing.role_for_side(side) {
                StrategyBookRole::Primary => strategy.on_primary_book(ticker.clone()),
                StrategyBookRole::Hedge => strategy.on_hedge_book(ticker.clone()),
            }
        }
    }

    pub(super) fn mark_pending_signal_symbols(&mut self, updated_strategy_symbol_ids: &[SymbolId]) {
        for symbol_id in updated_strategy_symbol_ids {
            self.pending_signal_symbols.insert(*symbol_id);
        }
    }

    fn latest_books_by_symbol_id(
        &self,
        side: ExchangeSide,
    ) -> &Vec<Option<hft_lead_lag::domain::BookTicker>> {
        match side {
            ExchangeSide::Binance => &self.latest_bn_by_symbol_id,
            ExchangeSide::Gate => &self.latest_gt_by_symbol_id,
        }
    }

    fn latest_books_by_symbol_id_mut(
        &mut self,
        side: ExchangeSide,
    ) -> &mut Vec<Option<hft_lead_lag::domain::BookTicker>> {
        match side {
            ExchangeSide::Binance => &mut self.latest_bn_by_symbol_id,
            ExchangeSide::Gate => &mut self.latest_gt_by_symbol_id,
        }
    }

    fn upsert_latest_books_by_symbol_ids(
        &mut self,
        side: ExchangeSide,
        updates: Vec<(SymbolId, hft_lead_lag::domain::BookTicker)>,
    ) {
        let cache = self.latest_books_by_symbol_id_mut(side);
        for (symbol_id, ticker) in updates {
            let idx = symbol_id as usize;
            if cache.len() <= idx {
                cache.resize(idx + 1, None);
            }
            cache[idx] = Some(ticker);
        }
    }

    pub(super) fn latest_book_for_strategy_symbol(
        &self,
        side: ExchangeSide,
        symbol_id: SymbolId,
    ) -> Option<&hft_lead_lag::domain::BookTicker> {
        self.latest_books_by_symbol_id(side)
            .get(symbol_id as usize)
            .and_then(Option::as_ref)
    }

    pub(super) fn signal_backlog_depth(&self) -> u64 {
        self.pending_signal_symbols.len() as u64
    }

    pub(super) fn handle_signal_tick(
        &mut self,
        strategy: &mut dyn RuntimeStrategy,
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
            let signal_decided_ts_ns = Self::now_ns();
            let signal = strategy.check_signal(symbol_id, signal_decided_ts_ns);
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

    #[test]
    fn strategy_symbol_index_collects_deduped_ids_for_updated_symbols() {
        let index = StrategySymbolIndex::new(&[
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ]);
        let updated = vec![
            Bytes::from_static(b"ETHUSDT"),
            Bytes::from_static(b"DOGEUSDT"),
            Bytes::from_static(b"BTCUSDT"),
            Bytes::from_static(b"ETHUSDT"),
        ];

        let ids = index.symbol_ids(&updated);

        assert_eq!(ids, vec![1, 0]);
    }
}
