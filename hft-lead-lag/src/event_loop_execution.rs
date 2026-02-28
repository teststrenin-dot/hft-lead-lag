use hft_lead_lag::api::HealthState;
use hft_lead_lag::StrategySignal;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

const DEFAULT_EXECUTION_INTENT_QUEUE_CAPACITY: usize = 2_048;
const DEFAULT_EXECUTION_SEND_TIMEOUT_MS: u64 = 25;
const DEFAULT_EXECUTION_KILL_SWITCH_TIMEOUT_STREAK: u32 = 4;
const DEFAULT_EXECUTION_METRICS_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_EXECUTION_SIMULATED_SEND_DELAY_MS: u64 = 0;

const EXECUTION_INTENT_QUEUE_CAPACITY_ENV: &str = "EXECUTION_INTENT_QUEUE_CAPACITY";
const EXECUTION_SEND_TIMEOUT_MS_ENV: &str = "EXECUTION_SEND_TIMEOUT_MS";
const EXECUTION_KILL_SWITCH_TIMEOUT_STREAK_ENV: &str = "EXECUTION_KILL_SWITCH_TIMEOUT_STREAK";
const EXECUTION_METRICS_FLUSH_INTERVAL_MS_ENV: &str = "EXECUTION_METRICS_FLUSH_INTERVAL_MS";
const EXECUTION_SIMULATED_SEND_DELAY_MS_ENV: &str = "EXECUTION_SIMULATED_SEND_DELAY_MS";

#[derive(Debug, Clone)]
pub(super) struct OrderIntent {
    pub(super) signal: StrategySignal,
    pub(super) signal_decided_ts_ns: i64,
    pub(super) enqueued_ts_ns: i64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LatencyStatsSnapshot {
    samples: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Debug, Clone, Copy)]
struct ExecutionConfig {
    queue_capacity: usize,
    send_timeout_ms: u64,
    kill_switch_timeout_streak: u32,
    metrics_flush_interval_ms: u64,
    simulated_send_delay_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_EXECUTION_INTENT_QUEUE_CAPACITY,
            send_timeout_ms: DEFAULT_EXECUTION_SEND_TIMEOUT_MS,
            kill_switch_timeout_streak: DEFAULT_EXECUTION_KILL_SWITCH_TIMEOUT_STREAK,
            metrics_flush_interval_ms: DEFAULT_EXECUTION_METRICS_FLUSH_INTERVAL_MS,
            simulated_send_delay_ms: DEFAULT_EXECUTION_SIMULATED_SEND_DELAY_MS,
        }
    }
}

impl ExecutionConfig {
    fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(v) = parse_env_usize(EXECUTION_INTENT_QUEUE_CAPACITY_ENV) {
            config.queue_capacity = v.max(1);
        }
        if let Some(v) = parse_env_u64(EXECUTION_SEND_TIMEOUT_MS_ENV) {
            config.send_timeout_ms = v.max(1);
        }
        if let Some(v) = parse_env_u32(EXECUTION_KILL_SWITCH_TIMEOUT_STREAK_ENV) {
            config.kill_switch_timeout_streak = v.max(1);
        }
        if let Some(v) = parse_env_u64(EXECUTION_METRICS_FLUSH_INTERVAL_MS_ENV) {
            config.metrics_flush_interval_ms = v.max(100);
        }
        if let Some(v) = parse_env_u64(EXECUTION_SIMULATED_SEND_DELAY_MS_ENV) {
            config.simulated_send_delay_ms = v;
        }
        config
    }
}

fn parse_env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse::<u64>().ok()
}

fn parse_env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok()?.trim().parse::<u32>().ok()
}

fn parse_env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok()
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
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

#[derive(Clone)]
pub(super) struct ExecutionQueueTx {
    sender: mpsc::Sender<OrderIntent>,
    queue_depth: Arc<AtomicU64>,
    health: Arc<HealthState>,
}

impl ExecutionQueueTx {
    pub(super) fn try_enqueue(&self, intent: OrderIntent) -> bool {
        if self
            .health
            .runtime_execution_kill_switch_active
            .load(Ordering::Relaxed)
        {
            self.health
                .runtime_execution_dropped_intents
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        match self.sender.try_send(intent) {
            Ok(()) => {
                let depth = self.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
                self.health
                    .runtime_execution_queue_depth
                    .store(depth, Ordering::Relaxed);
                true
            }
            Err(_err) => {
                self.health
                    .runtime_execution_dropped_intents
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    #[cfg(test)]
    pub(super) fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }
}

pub(super) fn spawn_execution_runtime(health: Arc<HealthState>) -> ExecutionQueueTx {
    let config = ExecutionConfig::from_env();
    spawn_execution_runtime_with_config(health, config)
}

fn spawn_execution_runtime_with_config(
    health: Arc<HealthState>,
    config: ExecutionConfig,
) -> ExecutionQueueTx {
    let queue_depth = Arc::new(AtomicU64::new(0));
    let (sender, receiver) = mpsc::channel(config.queue_capacity);
    let tx = ExecutionQueueTx {
        sender,
        queue_depth: queue_depth.clone(),
        health: health.clone(),
    };
    tokio::spawn(run_execution_worker(receiver, queue_depth, health, config));
    tx
}

async fn run_execution_worker(
    mut receiver: mpsc::Receiver<OrderIntent>,
    queue_depth: Arc<AtomicU64>,
    health: Arc<HealthState>,
    config: ExecutionConfig,
) {
    let mut flush_interval =
        tokio::time::interval(Duration::from_millis(config.metrics_flush_interval_ms));
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut intent_to_sent_samples_us: Vec<u64> = Vec::with_capacity(8_192);
    let mut consecutive_timeouts: u32 = 0;

    loop {
        tokio::select! {
            maybe_intent = receiver.recv() => {
                let Some(intent) = maybe_intent else {
                    break;
                };
                decrement_saturating(queue_depth.as_ref());
                let depth = queue_depth.load(Ordering::Acquire);
                health.runtime_execution_queue_depth.store(depth, Ordering::Relaxed);

                if health.runtime_execution_kill_switch_active.load(Ordering::Relaxed) {
                    health.runtime_execution_dropped_intents.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let send_result = tokio::time::timeout(
                    Duration::from_millis(config.send_timeout_ms),
                    send_order_intent(&intent, config.simulated_send_delay_ms),
                ).await;

                match send_result {
                    Ok(()) => {
                        consecutive_timeouts = 0;
                        let sent_ts_ns = now_ns();
                        health
                            .runtime_last_order_intent_sent_ts_ns
                            .store(sent_ts_ns, Ordering::Relaxed);
                        health
                            .runtime_execution_sent_intents
                            .fetch_add(1, Ordering::Relaxed);
                        if intent.enqueued_ts_ns > 0 && sent_ts_ns > intent.enqueued_ts_ns {
                            let latency_us =
                                (sent_ts_ns.saturating_sub(intent.enqueued_ts_ns) as u64) / 1_000;
                            intent_to_sent_samples_us.push(latency_us);
                        }
                    }
                    Err(_) => {
                        consecutive_timeouts = consecutive_timeouts.saturating_add(1);
                        health
                            .runtime_execution_send_timeouts
                            .fetch_add(1, Ordering::Relaxed);
                        if consecutive_timeouts >= config.kill_switch_timeout_streak {
                            health
                                .runtime_execution_kill_switch_active
                                .store(true, Ordering::Relaxed);
                            warn!(
                                "Execution kill-switch activated: timeout streak={} threshold={}",
                                consecutive_timeouts,
                                config.kill_switch_timeout_streak
                            );
                        }
                    }
                }
            }
            _ = flush_interval.tick() => {
                flush_execution_latency_metrics(&health, &mut intent_to_sent_samples_us);
                health.runtime_execution_queue_depth.store(
                    queue_depth.load(Ordering::Acquire),
                    Ordering::Relaxed
                );
            }
        }
    }

    flush_execution_latency_metrics(&health, &mut intent_to_sent_samples_us);
    health
        .runtime_execution_queue_depth
        .store(0, Ordering::Relaxed);
}

fn flush_execution_latency_metrics(health: &HealthState, samples: &mut Vec<u64>) {
    let snap = latency_snapshot_and_reset(samples);
    health
        .runtime_execution_intent_to_sent_samples
        .store(snap.samples, Ordering::Relaxed);
    health
        .runtime_execution_intent_to_sent_p50_us
        .store(snap.p50_us, Ordering::Relaxed);
    health
        .runtime_execution_intent_to_sent_p95_us
        .store(snap.p95_us, Ordering::Relaxed);
    health
        .runtime_execution_intent_to_sent_p99_us
        .store(snap.p99_us, Ordering::Relaxed);
    health
        .runtime_execution_intent_to_sent_max_us
        .store(snap.max_us, Ordering::Relaxed);
}

async fn send_order_intent(intent: &OrderIntent, simulated_send_delay_ms: u64) {
    let _ = (
        intent.signal.strategy,
        intent.signal.symbol.as_str(),
        intent.signal_decided_ts_ns,
    );
    if simulated_send_delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(simulated_send_delay_ms)).await;
    } else {
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_signal() -> StrategySignal {
        StrategySignal {
            strategy: "lead_lag_classic",
            symbol: "BTCUSDT".to_string(),
            spread_bps: 35.0,
            direction: "LONG_LAGGER",
            bid_ask_bps: 36.0,
            ask_bid_bps: 12.0,
            context: "test".to_string(),
        }
    }

    #[tokio::test]
    async fn execution_queue_accepts_intents_and_tracks_queue_depth() {
        let health = Arc::new(HealthState::new());
        let execution = spawn_execution_runtime_with_config(
            health.clone(),
            ExecutionConfig {
                send_timeout_ms: 500,
                simulated_send_delay_ms: 100,
                ..ExecutionConfig::default()
            },
        );
        let intent = OrderIntent {
            signal: sample_signal(),
            signal_decided_ts_ns: 1_000,
            enqueued_ts_ns: 1_001,
        };

        let enqueued = execution.try_enqueue(intent);

        assert!(enqueued, "execution queue must accept fresh intent");
        assert!(
            execution.queue_depth() >= 1,
            "queue depth must increase when intent is accepted"
        );
    }

    #[tokio::test]
    async fn execution_worker_reports_sent_intents_and_latency() {
        let health = Arc::new(HealthState::new());
        let execution = spawn_execution_runtime_with_config(
            health.clone(),
            ExecutionConfig {
                send_timeout_ms: 200,
                simulated_send_delay_ms: 0,
                ..ExecutionConfig::default()
            },
        );
        let intent = OrderIntent {
            signal: sample_signal(),
            signal_decided_ts_ns: 10_000,
            enqueued_ts_ns: now_ns(),
        };
        assert!(execution.try_enqueue(intent));
        tokio::time::sleep(tokio::time::Duration::from_millis(40)).await;

        assert!(
            health
                .runtime_execution_sent_intents
                .load(Ordering::Relaxed)
                >= 1,
            "worker must report sent intents"
        );
        assert!(
            health
                .runtime_execution_intent_to_sent_samples
                .load(Ordering::Relaxed)
                >= 1,
            "intent->sent latency samples must be recorded"
        );
    }

    #[tokio::test]
    async fn execution_kill_switch_activates_after_timeout_streak() {
        let health = Arc::new(HealthState::new());
        let execution = spawn_execution_runtime_with_config(
            health.clone(),
            ExecutionConfig {
                send_timeout_ms: 5,
                simulated_send_delay_ms: 50,
                kill_switch_timeout_streak: 2,
                ..ExecutionConfig::default()
            },
        );
        for idx in 0..3 {
            let intent = OrderIntent {
                signal: sample_signal(),
                signal_decided_ts_ns: 50_000 + idx,
                enqueued_ts_ns: 50_010 + idx,
            };
            let _ = execution.try_enqueue(intent);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(220)).await;
        assert!(
            health
                .runtime_execution_kill_switch_active
                .load(Ordering::Relaxed),
            "kill-switch must activate on repeated send timeouts"
        );
        assert!(
            health
                .runtime_execution_send_timeouts
                .load(Ordering::Relaxed)
                >= 2
        );
    }
}
