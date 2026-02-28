use super::{HealthState, MarketDataEvent, ScreenerStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

const DEFAULT_CONTROL_UPDATE_QUEUE_CAPACITY: usize = 8_192;
const CONTROL_UPDATE_QUEUE_CAPACITY_ENV: &str = "CONTROL_UPDATE_QUEUE_CAPACITY";
const DEFAULT_CONTROL_UPDATE_FLUSH_INTERVAL_MS: u64 = 50;
const CONTROL_UPDATE_FLUSH_INTERVAL_MS_ENV: &str = "CONTROL_UPDATE_FLUSH_INTERVAL_MS";
const DEFAULT_CONTROL_UPDATE_MAX_BATCH: usize = 1_024;
const CONTROL_UPDATE_MAX_BATCH_ENV: &str = "CONTROL_UPDATE_MAX_BATCH";
type OverflowKey = (String, &'static str);

#[derive(Debug, Clone)]
pub(super) struct ControlUpdate {
    pub(super) symbol: String,
    pub(super) exchange: &'static str,
    pub(super) bid: f64,
    pub(super) ask: f64,
    pub(super) exchange_ts_ns: i64,
    pub(super) local_ts_ns: i64,
}

#[derive(Debug, Clone, Copy)]
struct ControlWorkerConfig {
    queue_capacity: usize,
    flush_interval_ms: u64,
    max_batch: usize,
}

#[derive(Clone)]
pub(super) struct ControlPlaneTx {
    sender: mpsc::Sender<ControlUpdate>,
    queue_depth: Arc<AtomicU64>,
    overflow_latest_by_symbol: Arc<Mutex<HashMap<OverflowKey, ControlUpdate>>>,
    health: Arc<HealthState>,
}

impl ControlPlaneTx {
    pub(super) fn try_enqueue(&self, update: ControlUpdate) -> bool {
        // Reserve depth before try_send to avoid producer/consumer drift.
        let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
        self.health
            .runtime_control_queue_depth
            .store(depth, Ordering::Relaxed);

        match self.sender.try_send(update) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(update)) => {
                decrement_saturating(self.queue_depth.as_ref());
                self.health
                    .runtime_control_queue_depth
                    .store(self.queue_depth.load(Ordering::Acquire), Ordering::Relaxed);
                // Keep latest update per (symbol, exchange) in overflow lane.
                let replaced = if let Ok(mut overflow) = self.overflow_latest_by_symbol.lock() {
                    let key = (update.symbol.clone(), update.exchange);
                    overflow.insert(key, update).is_some()
                } else {
                    self.health
                        .runtime_control_dropped_updates
                        .fetch_add(1, Ordering::Relaxed);
                    return false;
                };
                if replaced {
                    self.health
                        .runtime_control_dropped_updates
                        .fetch_add(1, Ordering::Relaxed);
                }
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                decrement_saturating(self.queue_depth.as_ref());
                self.health
                    .runtime_control_queue_depth
                    .store(self.queue_depth.load(Ordering::Acquire), Ordering::Relaxed);
                self.health
                    .runtime_control_dropped_updates
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }
}

fn parse_positive_env_usize(name: &str) -> Option<usize> {
    let raw = std::env::var(name).ok()?;
    let value = raw.trim();
    match value.parse::<usize>() {
        Ok(v) if v > 0 => Some(v),
        Ok(_) => {
            warn!("{name} is set to '{value}' but must be > 0; using default");
            None
        }
        Err(_) => {
            warn!("{name} is set to '{value}' but is not a valid integer; using default");
            None
        }
    }
}

fn parse_positive_env_u64(name: &str) -> Option<u64> {
    let raw = std::env::var(name).ok()?;
    let value = raw.trim();
    match value.parse::<u64>() {
        Ok(v) if v > 0 => Some(v),
        Ok(_) => {
            warn!("{name} is set to '{value}' but must be > 0; using default");
            None
        }
        Err(_) => {
            warn!("{name} is set to '{value}' but is not a valid integer; using default");
            None
        }
    }
}

fn worker_config_from_env() -> ControlWorkerConfig {
    ControlWorkerConfig {
        queue_capacity: parse_positive_env_usize(CONTROL_UPDATE_QUEUE_CAPACITY_ENV)
            .unwrap_or(DEFAULT_CONTROL_UPDATE_QUEUE_CAPACITY)
            .max(1),
        flush_interval_ms: parse_positive_env_u64(CONTROL_UPDATE_FLUSH_INTERVAL_MS_ENV)
            .unwrap_or(DEFAULT_CONTROL_UPDATE_FLUSH_INTERVAL_MS)
            .max(1),
        max_batch: parse_positive_env_usize(CONTROL_UPDATE_MAX_BATCH_ENV)
            .unwrap_or(DEFAULT_CONTROL_UPDATE_MAX_BATCH)
            .max(1),
    }
}

fn decrement_saturating(counter: &AtomicU64) {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let next = current.saturating_sub(1);
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn drain_overflow_updates(
    overflow_latest_by_symbol: &Mutex<HashMap<OverflowKey, ControlUpdate>>,
    pending: &mut HashMap<OverflowKey, ControlUpdate>,
) -> u64 {
    let Ok(mut overflow) = overflow_latest_by_symbol.lock() else {
        return 0;
    };
    let mut replaced: u64 = 0;
    for (key, update) in overflow.drain() {
        if pending.insert(key, update).is_some() {
            replaced = replaced.saturating_add(1);
        }
    }
    replaced
}

fn queue_pending_update(
    pending: &mut HashMap<OverflowKey, ControlUpdate>,
    update: ControlUpdate,
) -> bool {
    let key = (update.symbol.clone(), update.exchange);
    pending.insert(key, update).is_some()
}

fn flush_pending_updates(
    screener: &ScreenerStore,
    ws_tx: Option<&broadcast::Sender<MarketDataEvent>>,
    pending: &mut HashMap<OverflowKey, ControlUpdate>,
) {
    for update in pending.drain().map(|(_, update)| update) {
        apply_control_update(screener, ws_tx, update);
    }
}

async fn run_control_plane_worker_loop(
    mut receiver: mpsc::Receiver<ControlUpdate>,
    queue_depth: Arc<AtomicU64>,
    overflow_latest_by_symbol: Arc<Mutex<HashMap<OverflowKey, ControlUpdate>>>,
    screener: ScreenerStore,
    ws_tx: Option<broadcast::Sender<MarketDataEvent>>,
    health: Arc<HealthState>,
    config: ControlWorkerConfig,
) {
    let mut pending: HashMap<OverflowKey, ControlUpdate> = HashMap::with_capacity(config.max_batch);
    let mut flush_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(config.flush_interval_ms));
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            update = receiver.recv() => {
                let Some(update) = update else {
                    let replaced = drain_overflow_updates(&overflow_latest_by_symbol, &mut pending);
                    if replaced > 0 {
                        health
                            .runtime_control_dropped_updates
                            .fetch_add(replaced, Ordering::Relaxed);
                    }
                    flush_pending_updates(&screener, ws_tx.as_ref(), &mut pending);
                    break;
                };
                decrement_saturating(queue_depth.as_ref());
                health
                    .runtime_control_queue_depth
                    .store(queue_depth.load(Ordering::Acquire), Ordering::Relaxed);
                if queue_pending_update(&mut pending, update) {
                    health
                        .runtime_control_dropped_updates
                        .fetch_add(1, Ordering::Relaxed);
                }
                let replaced = drain_overflow_updates(&overflow_latest_by_symbol, &mut pending);
                if replaced > 0 {
                    health
                        .runtime_control_dropped_updates
                        .fetch_add(replaced, Ordering::Relaxed);
                }
                if pending.len() >= config.max_batch {
                    flush_pending_updates(&screener, ws_tx.as_ref(), &mut pending);
                }
            }
            _ = flush_interval.tick() => {
                let replaced = drain_overflow_updates(&overflow_latest_by_symbol, &mut pending);
                if replaced > 0 {
                    health
                        .runtime_control_dropped_updates
                        .fetch_add(replaced, Ordering::Relaxed);
                }
                if !pending.is_empty() {
                    flush_pending_updates(&screener, ws_tx.as_ref(), &mut pending);
                }
            }
        }
    }

    health
        .runtime_control_queue_depth
        .store(0, Ordering::Relaxed);
    warn!("control-plane worker stopped: update channel closed");
}

pub(super) fn spawn_control_plane_worker(
    screener: ScreenerStore,
    ws_tx: Option<broadcast::Sender<MarketDataEvent>>,
    health: Arc<HealthState>,
) -> Arc<ControlPlaneTx> {
    let config = worker_config_from_env();
    spawn_control_plane_worker_with_config(screener, ws_tx, health, config)
}

fn spawn_control_plane_worker_with_config(
    screener: ScreenerStore,
    ws_tx: Option<broadcast::Sender<MarketDataEvent>>,
    health: Arc<HealthState>,
    config: ControlWorkerConfig,
) -> Arc<ControlPlaneTx> {
    let (sender, receiver) = mpsc::channel::<ControlUpdate>(config.queue_capacity);
    let queue_depth = Arc::new(AtomicU64::new(0));
    let overflow_latest_by_symbol = Arc::new(Mutex::new(HashMap::new()));
    let tx = Arc::new(ControlPlaneTx {
        sender,
        queue_depth: queue_depth.clone(),
        overflow_latest_by_symbol: overflow_latest_by_symbol.clone(),
        health: health.clone(),
    });

    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        let Ok(runtime) = runtime else {
            warn!("control-plane worker runtime init failed");
            return;
        };
        runtime.block_on(run_control_plane_worker_loop(
            receiver,
            queue_depth,
            overflow_latest_by_symbol,
            screener,
            ws_tx,
            health,
            config,
        ));
    });

    tx
}

fn apply_control_update(
    screener: &ScreenerStore,
    ws_tx: Option<&broadcast::Sender<MarketDataEvent>>,
    update: ControlUpdate,
) {
    screener.update(
        &update.symbol,
        update.exchange,
        update.bid,
        update.ask,
        update.exchange_ts_ns,
        update.local_ts_ns,
    );
    if let Some(ws_tx) = ws_tx {
        let _ = ws_tx.send(MarketDataEvent {
            symbol: update.symbol,
            exchange: update.exchange,
            bid: update.bid,
            ask: update.ask,
            timestamp_ns: update.exchange_ts_ns,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn env_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock")
    }

    #[tokio::test]
    async fn control_plane_worker_applies_update_and_emits_ws_event() {
        let screener = ScreenerStore::default();
        let health = Arc::new(HealthState::new());
        let (ws_tx, mut ws_rx) = broadcast::channel::<MarketDataEvent>(8);
        let control = spawn_control_plane_worker(screener, Some(ws_tx), health.clone());

        let accepted = control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 101.0,
            ask: 101.2,
            exchange_ts_ns: 100_000_000,
            local_ts_ns: 100_000_100,
        });
        assert!(accepted, "control-plane queue must accept update");

        let event = tokio::time::timeout(tokio::time::Duration::from_millis(100), ws_rx.recv())
            .await
            .expect("ws recv timeout")
            .expect("ws event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.exchange, "binance");
        assert_eq!(event.timestamp_ns, 100_000_000);
        assert_eq!(
            health
                .runtime_control_dropped_updates
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn control_plane_worker_coalesces_latest_update_within_flush_window() {
        let screener = ScreenerStore::default();
        let health = Arc::new(HealthState::new());
        let (ws_tx, mut ws_rx) = broadcast::channel::<MarketDataEvent>(8);
        let control = spawn_control_plane_worker_with_config(
            screener,
            Some(ws_tx),
            health,
            ControlWorkerConfig {
                queue_capacity: 32,
                flush_interval_ms: 150,
                max_batch: 256,
            },
        );

        assert!(control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 100.0,
            ask: 100.2,
            exchange_ts_ns: 1_000,
            local_ts_ns: 1_100,
        }));
        assert!(control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 101.0,
            ask: 101.2,
            exchange_ts_ns: 1_200,
            local_ts_ns: 1_300,
        }));

        let event = tokio::time::timeout(tokio::time::Duration::from_millis(500), ws_rx.recv())
            .await
            .expect("ws recv timeout")
            .expect("ws event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.exchange, "binance");
        assert_eq!(event.bid, 101.0, "must keep latest update in window");
        assert_eq!(event.ask, 101.2, "must keep latest update in window");
        assert_eq!(
            control
                .health
                .runtime_control_dropped_updates
                .load(Ordering::Relaxed),
            1,
            "coalescing overwrite must be observable in dropped counter"
        );

        let next =
            tokio::time::timeout(tokio::time::Duration::from_millis(120), ws_rx.recv()).await;
        assert!(
            next.is_err(),
            "worker must emit one coalesced update for same symbol/exchange per flush window"
        );
    }

    #[test]
    fn worker_config_from_env_rejects_zero_values_and_uses_defaults() {
        let _lock = env_test_lock();
        std::env::set_var(CONTROL_UPDATE_QUEUE_CAPACITY_ENV, "0");
        std::env::set_var(CONTROL_UPDATE_FLUSH_INTERVAL_MS_ENV, "0");
        std::env::set_var(CONTROL_UPDATE_MAX_BATCH_ENV, "0");

        let cfg = worker_config_from_env();
        assert_eq!(cfg.queue_capacity, DEFAULT_CONTROL_UPDATE_QUEUE_CAPACITY);
        assert_eq!(
            cfg.flush_interval_ms,
            DEFAULT_CONTROL_UPDATE_FLUSH_INTERVAL_MS
        );
        assert_eq!(cfg.max_batch, DEFAULT_CONTROL_UPDATE_MAX_BATCH);

        std::env::remove_var(CONTROL_UPDATE_QUEUE_CAPACITY_ENV);
        std::env::remove_var(CONTROL_UPDATE_FLUSH_INTERVAL_MS_ENV);
        std::env::remove_var(CONTROL_UPDATE_MAX_BATCH_ENV);
    }

    #[test]
    fn control_plane_try_enqueue_overflow_lane_keeps_latest_by_symbol_and_counts_replacements() {
        let health = Arc::new(HealthState::new());
        let (sender, _receiver) = mpsc::channel::<ControlUpdate>(1);
        let control = ControlPlaneTx {
            sender,
            queue_depth: Arc::new(AtomicU64::new(0)),
            overflow_latest_by_symbol: Arc::new(Mutex::new(HashMap::new())),
            health: health.clone(),
        };

        let accepted_1 = control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 100.0,
            ask: 100.1,
            exchange_ts_ns: 1,
            local_ts_ns: 2,
        });
        assert!(accepted_1);

        let accepted_2 = control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 101.0,
            ask: 101.1,
            exchange_ts_ns: 3,
            local_ts_ns: 4,
        });
        assert!(accepted_2);

        let accepted_3 = control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 102.0,
            ask: 102.1,
            exchange_ts_ns: 5,
            local_ts_ns: 6,
        });
        assert!(accepted_3);

        let overflow = control
            .overflow_latest_by_symbol
            .lock()
            .expect("overflow lock");
        assert_eq!(overflow.len(), 1);
        assert_eq!(
            overflow
                .get(&(String::from("BTCUSDT"), "binance"))
                .expect("overflow entry")
                .bid,
            102.0
        );
        drop(overflow);

        assert_eq!(
            health
                .runtime_control_dropped_updates
                .load(Ordering::Relaxed),
            1,
            "replacement in overflow lane must increment dropped counter once",
        );
    }

    #[test]
    fn control_plane_overflow_lane_keeps_latest_per_symbol_and_exchange() {
        let health = Arc::new(HealthState::new());
        let (sender, _receiver) = mpsc::channel::<ControlUpdate>(1);
        let control = ControlPlaneTx {
            sender,
            queue_depth: Arc::new(AtomicU64::new(0)),
            overflow_latest_by_symbol: Arc::new(Mutex::new(HashMap::new())),
            health,
        };

        assert!(control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 100.0,
            ask: 100.1,
            exchange_ts_ns: 1,
            local_ts_ns: 2,
        }));
        assert!(control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "gate",
            bid: 200.0,
            ask: 200.1,
            exchange_ts_ns: 3,
            local_ts_ns: 4,
        }));
        assert!(control.try_enqueue(ControlUpdate {
            symbol: "BTCUSDT".to_string(),
            exchange: "binance",
            bid: 101.0,
            ask: 101.1,
            exchange_ts_ns: 5,
            local_ts_ns: 6,
        }));

        let overflow = control
            .overflow_latest_by_symbol
            .lock()
            .expect("overflow lock");
        assert_eq!(
            overflow.len(),
            2,
            "overflow must keep one update per exchange"
        );
        assert!(
            overflow.contains_key(&(String::from("BTCUSDT"), "gate"))
                && overflow.contains_key(&(String::from("BTCUSDT"), "binance"))
        );
    }
}
