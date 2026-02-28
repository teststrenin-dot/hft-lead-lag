use super::{HealthState, MarketDataEvent, ScreenerStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};
use tracing::warn;

const DEFAULT_CONTROL_UPDATE_QUEUE_CAPACITY: usize = 8_192;
const CONTROL_UPDATE_QUEUE_CAPACITY_ENV: &str = "CONTROL_UPDATE_QUEUE_CAPACITY";

#[derive(Debug, Clone)]
pub(super) struct ControlUpdate {
    pub(super) symbol: String,
    pub(super) exchange: &'static str,
    pub(super) bid: f64,
    pub(super) ask: f64,
    pub(super) exchange_ts_ns: i64,
    pub(super) local_ts_ns: i64,
}

#[derive(Clone)]
pub(super) struct ControlPlaneTx {
    sender: mpsc::Sender<ControlUpdate>,
    queue_depth: Arc<AtomicU64>,
    overflow_latest_by_symbol: Arc<Mutex<HashMap<String, ControlUpdate>>>,
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
                // Keep latest update per symbol in overflow lane.
                let replaced = if let Ok(mut overflow) = self.overflow_latest_by_symbol.lock() {
                    overflow.insert(update.symbol.clone(), update).is_some()
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

fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok()
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

fn pop_overflow_update(
    overflow_latest_by_symbol: &Mutex<HashMap<String, ControlUpdate>>,
) -> Option<ControlUpdate> {
    let mut overflow = overflow_latest_by_symbol.lock().ok()?;
    let key = overflow.keys().next()?.to_owned();
    overflow.remove(&key)
}

pub(super) fn spawn_control_plane_worker(
    screener: ScreenerStore,
    ws_tx: Option<broadcast::Sender<MarketDataEvent>>,
    health: Arc<HealthState>,
) -> Arc<ControlPlaneTx> {
    let capacity = parse_env_usize(CONTROL_UPDATE_QUEUE_CAPACITY_ENV)
        .unwrap_or(DEFAULT_CONTROL_UPDATE_QUEUE_CAPACITY)
        .max(1);
    let (sender, mut receiver) = mpsc::channel::<ControlUpdate>(capacity);
    let queue_depth = Arc::new(AtomicU64::new(0));
    let overflow_latest_by_symbol = Arc::new(Mutex::new(HashMap::new()));
    let tx = Arc::new(ControlPlaneTx {
        sender,
        queue_depth: queue_depth.clone(),
        overflow_latest_by_symbol: overflow_latest_by_symbol.clone(),
        health: health.clone(),
    });

    tokio::spawn(async move {
        loop {
            if let Some(update) = pop_overflow_update(&overflow_latest_by_symbol) {
                apply_control_update(&screener, ws_tx.as_ref(), update);
                continue;
            }
            let Some(update) = receiver.recv().await else {
                break;
            };
            decrement_saturating(queue_depth.as_ref());
            health
                .runtime_control_queue_depth
                .store(queue_depth.load(Ordering::Acquire), Ordering::Relaxed);
            apply_control_update(&screener, ws_tx.as_ref(), update);
        }
        health
            .runtime_control_queue_depth
            .store(0, Ordering::Relaxed);
        warn!("control-plane worker stopped: update channel closed");
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
}
