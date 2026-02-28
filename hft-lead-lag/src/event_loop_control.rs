use super::{MarketDataEvent, ScreenerStore};
use std::sync::Arc;
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
}

impl ControlPlaneTx {
    pub(super) fn try_enqueue(&self, update: ControlUpdate) -> bool {
        match self.sender.try_send(update) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => false,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok()
}

pub(super) fn spawn_control_plane_worker(
    screener: ScreenerStore,
    ws_tx: Option<broadcast::Sender<MarketDataEvent>>,
) -> Arc<ControlPlaneTx> {
    let capacity = parse_env_usize(CONTROL_UPDATE_QUEUE_CAPACITY_ENV)
        .unwrap_or(DEFAULT_CONTROL_UPDATE_QUEUE_CAPACITY)
        .max(1);
    let (sender, mut receiver) = mpsc::channel::<ControlUpdate>(capacity);
    let tx = Arc::new(ControlPlaneTx { sender });

    tokio::spawn(async move {
        while let Some(update) = receiver.recv().await {
            screener.update(
                &update.symbol,
                update.exchange,
                update.bid,
                update.ask,
                update.exchange_ts_ns,
                update.local_ts_ns,
            );
            if let Some(ws_tx) = ws_tx.as_ref() {
                let _ = ws_tx.send(MarketDataEvent {
                    symbol: update.symbol,
                    exchange: update.exchange,
                    bid: update.bid,
                    ask: update.ask,
                    timestamp_ns: update.exchange_ts_ns,
                });
            }
        }
        warn!("control-plane worker stopped: update channel closed");
    });

    tx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn control_plane_worker_applies_update_and_emits_ws_event() {
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = broadcast::channel::<MarketDataEvent>(8);
        let control = spawn_control_plane_worker(screener, Some(ws_tx));

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
    }
}
