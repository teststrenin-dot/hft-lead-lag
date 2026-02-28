use hft_lead_lag::api::HealthState;
use hft_lead_lag::StrategySignal;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::warn;

const DEFAULT_EXECUTION_INTENT_QUEUE_CAPACITY: usize = 2_048;
const DEFAULT_EXECUTION_SEND_TIMEOUT_MS: u64 = 25;
const DEFAULT_EXECUTION_KILL_SWITCH_TIMEOUT_STREAK: u32 = 4;
const DEFAULT_EXECUTION_KILL_SWITCH_COOLDOWN_MS: u64 = 5_000;
const DEFAULT_EXECUTION_METRICS_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_EXECUTION_SIMULATED_SEND_DELAY_MS: u64 = 0;
const DEFAULT_EXECUTION_MAX_INTENT_AGE_MS: u64 = 250;

const EXECUTION_INTENT_QUEUE_CAPACITY_ENV: &str = "EXECUTION_INTENT_QUEUE_CAPACITY";
const EXECUTION_SEND_TIMEOUT_MS_ENV: &str = "EXECUTION_SEND_TIMEOUT_MS";
const EXECUTION_KILL_SWITCH_TIMEOUT_STREAK_ENV: &str = "EXECUTION_KILL_SWITCH_TIMEOUT_STREAK";
const EXECUTION_KILL_SWITCH_COOLDOWN_MS_ENV: &str = "EXECUTION_KILL_SWITCH_COOLDOWN_MS";
const EXECUTION_METRICS_FLUSH_INTERVAL_MS_ENV: &str = "EXECUTION_METRICS_FLUSH_INTERVAL_MS";
const EXECUTION_SIMULATED_SEND_DELAY_MS_ENV: &str = "EXECUTION_SIMULATED_SEND_DELAY_MS";
const EXECUTION_MAX_INTENT_AGE_MS_ENV: &str = "EXECUTION_MAX_INTENT_AGE_MS";

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
    kill_switch_cooldown_ms: u64,
    metrics_flush_interval_ms: u64,
    simulated_send_delay_ms: u64,
    max_intent_age_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_EXECUTION_INTENT_QUEUE_CAPACITY,
            send_timeout_ms: DEFAULT_EXECUTION_SEND_TIMEOUT_MS,
            kill_switch_timeout_streak: DEFAULT_EXECUTION_KILL_SWITCH_TIMEOUT_STREAK,
            kill_switch_cooldown_ms: DEFAULT_EXECUTION_KILL_SWITCH_COOLDOWN_MS,
            metrics_flush_interval_ms: DEFAULT_EXECUTION_METRICS_FLUSH_INTERVAL_MS,
            simulated_send_delay_ms: DEFAULT_EXECUTION_SIMULATED_SEND_DELAY_MS,
            max_intent_age_ms: DEFAULT_EXECUTION_MAX_INTENT_AGE_MS,
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
        if let Some(v) = parse_env_u64(EXECUTION_KILL_SWITCH_COOLDOWN_MS_ENV) {
            config.kill_switch_cooldown_ms = v.max(100);
        }
        if let Some(v) = parse_env_u64(EXECUTION_METRICS_FLUSH_INTERVAL_MS_ENV) {
            config.metrics_flush_interval_ms = v.max(100);
        }
        if let Some(v) = parse_env_u64(EXECUTION_SIMULATED_SEND_DELAY_MS_ENV) {
            config.simulated_send_delay_ms = v;
        }
        if let Some(v) = parse_env_u64(EXECUTION_MAX_INTENT_AGE_MS_ENV) {
            config.max_intent_age_ms = v.max(1);
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
    overflow_latest_by_symbol: Arc<Mutex<HashMap<String, OrderIntent>>>,
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

        // Reserve depth before send to avoid producer/consumer drift races.
        let reserved_depth = self.queue_depth.fetch_add(1, Ordering::AcqRel) + 1;
        self.health
            .runtime_execution_queue_depth
            .store(reserved_depth, Ordering::Relaxed);

        match self.sender.try_send(intent) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(intent)) => {
                decrement_saturating(self.queue_depth.as_ref());
                self.health
                    .runtime_execution_queue_depth
                    .store(self.queue_depth.load(Ordering::Acquire), Ordering::Relaxed);
                // Keep latest intent per symbol when bounded channel is full.
                let replaced = if let Ok(mut overflow) = self.overflow_latest_by_symbol.lock() {
                    overflow
                        .insert(intent.signal.symbol.clone(), intent)
                        .is_some()
                } else {
                    self.health
                        .runtime_execution_dropped_intents
                        .fetch_add(1, Ordering::Relaxed);
                    return false;
                };
                if replaced {
                    self.health
                        .runtime_execution_dropped_intents
                        .fetch_add(1, Ordering::Relaxed);
                }
                true
            }
            Err(mpsc::error::TrySendError::Closed(_intent)) => {
                decrement_saturating(self.queue_depth.as_ref());
                self.health
                    .runtime_execution_queue_depth
                    .store(self.queue_depth.load(Ordering::Acquire), Ordering::Relaxed);
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
    let overflow_latest_by_symbol = Arc::new(Mutex::new(HashMap::new()));
    let (sender, receiver) = mpsc::channel(config.queue_capacity);
    let tx = ExecutionQueueTx {
        sender,
        queue_depth: queue_depth.clone(),
        overflow_latest_by_symbol: overflow_latest_by_symbol.clone(),
        health: health.clone(),
    };
    tokio::spawn(run_execution_worker(
        receiver,
        queue_depth,
        overflow_latest_by_symbol,
        health,
        config,
    ));
    tx
}

async fn run_execution_worker(
    mut receiver: mpsc::Receiver<OrderIntent>,
    queue_depth: Arc<AtomicU64>,
    overflow_latest_by_symbol: Arc<Mutex<HashMap<String, OrderIntent>>>,
    health: Arc<HealthState>,
    config: ExecutionConfig,
) {
    let mut flush_interval =
        tokio::time::interval(Duration::from_millis(config.metrics_flush_interval_ms));
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut intent_to_sent_samples_us: Vec<u64> = Vec::with_capacity(8_192);
    let mut consecutive_timeouts: u32 = 0;
    let mut kill_switch_recover_at_ns: Option<i64> = None;

    loop {
        if let Some(overflow_intent) = pop_overflow_intent(&overflow_latest_by_symbol) {
            process_intent(
                overflow_intent,
                &health,
                &config,
                &mut intent_to_sent_samples_us,
                &mut consecutive_timeouts,
                &mut kill_switch_recover_at_ns,
            )
            .await;
            continue;
        }
        tokio::select! {
            maybe_intent = receiver.recv() => {
                let Some(intent) = maybe_intent else {
                    break;
                };
                decrement_saturating(queue_depth.as_ref());
                let depth = queue_depth.load(Ordering::Acquire);
                health.runtime_execution_queue_depth.store(depth, Ordering::Relaxed);

                process_intent(
                    intent,
                    &health,
                    &config,
                    &mut intent_to_sent_samples_us,
                    &mut consecutive_timeouts,
                    &mut kill_switch_recover_at_ns,
                ).await;
            }
            _ = flush_interval.tick() => {
                flush_execution_latency_metrics(&health, &mut intent_to_sent_samples_us);
                maybe_recover_kill_switch(&health, &mut kill_switch_recover_at_ns);
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

fn pop_overflow_intent(
    overflow_latest_by_symbol: &Mutex<HashMap<String, OrderIntent>>,
) -> Option<OrderIntent> {
    let mut overflow = overflow_latest_by_symbol.lock().ok()?;
    let key = overflow.keys().next()?.to_owned();
    overflow.remove(&key)
}

fn is_stale_intent(intent: &OrderIntent, max_intent_age_ms: u64) -> bool {
    let now = now_ns();
    if intent.signal_decided_ts_ns <= 0 || now <= intent.signal_decided_ts_ns {
        return false;
    }
    let max_age_ns = max_intent_age_ms.saturating_mul(1_000_000);
    now.abs_diff(intent.signal_decided_ts_ns) > max_age_ns
}

fn maybe_recover_kill_switch(health: &HealthState, kill_switch_recover_at_ns: &mut Option<i64>) {
    let Some(recover_at_ns) = *kill_switch_recover_at_ns else {
        return;
    };
    if now_ns() < recover_at_ns {
        return;
    }
    health
        .runtime_execution_kill_switch_active
        .store(false, Ordering::Relaxed);
    *kill_switch_recover_at_ns = None;
    warn!("Execution kill-switch recovered after cooldown");
}

async fn process_intent(
    intent: OrderIntent,
    health: &HealthState,
    config: &ExecutionConfig,
    intent_to_sent_samples_us: &mut Vec<u64>,
    consecutive_timeouts: &mut u32,
    kill_switch_recover_at_ns: &mut Option<i64>,
) {
    maybe_recover_kill_switch(health, kill_switch_recover_at_ns);
    if health
        .runtime_execution_kill_switch_active
        .load(Ordering::Relaxed)
    {
        health
            .runtime_execution_dropped_intents
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    if is_stale_intent(&intent, config.max_intent_age_ms) {
        health
            .runtime_execution_dropped_intents
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    let send_result = tokio::time::timeout(
        Duration::from_millis(config.send_timeout_ms),
        send_order_intent(&intent, config.simulated_send_delay_ms),
    )
    .await;

    match send_result {
        Ok(()) => {
            *consecutive_timeouts = 0;
            let sent_ts_ns = now_ns();
            health
                .runtime_last_order_intent_sent_ts_ns
                .store(sent_ts_ns, Ordering::Relaxed);
            health
                .runtime_execution_sent_intents
                .fetch_add(1, Ordering::Relaxed);
            if intent.enqueued_ts_ns > 0 && sent_ts_ns > intent.enqueued_ts_ns {
                let latency_us = (sent_ts_ns.saturating_sub(intent.enqueued_ts_ns) as u64) / 1_000;
                intent_to_sent_samples_us.push(latency_us);
            }
        }
        Err(_) => {
            *consecutive_timeouts = consecutive_timeouts.saturating_add(1);
            health
                .runtime_execution_send_timeouts
                .fetch_add(1, Ordering::Relaxed);
            if *consecutive_timeouts >= config.kill_switch_timeout_streak {
                health
                    .runtime_execution_kill_switch_active
                    .store(true, Ordering::Relaxed);
                let cooldown_ns_u64 = config.kill_switch_cooldown_ms.saturating_mul(1_000_000);
                let cooldown_ns_i64 = cooldown_ns_u64.min(i64::MAX as u64) as i64;
                *kill_switch_recover_at_ns = Some(now_ns().saturating_add(cooldown_ns_i64));
                warn!(
                    "Execution kill-switch activated: timeout streak={} threshold={} cooldown_ms={}",
                    *consecutive_timeouts,
                    config.kill_switch_timeout_streak,
                    config.kill_switch_cooldown_ms
                );
            }
        }
    }
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
                max_intent_age_ms: 10_000,
                ..ExecutionConfig::default()
            },
        );
        let base_ns = now_ns();
        let intent = OrderIntent {
            signal: sample_signal(),
            signal_decided_ts_ns: base_ns,
            enqueued_ts_ns: base_ns,
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
                kill_switch_cooldown_ms: 10_000,
                max_intent_age_ms: 10_000,
                ..ExecutionConfig::default()
            },
        );
        let base_ns = now_ns();
        for idx in 0..3 {
            let intent = OrderIntent {
                signal: sample_signal(),
                signal_decided_ts_ns: base_ns + idx,
                enqueued_ts_ns: base_ns + idx,
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
        let blocked = execution.try_enqueue(OrderIntent {
            signal: sample_signal(),
            signal_decided_ts_ns: 99_000,
            enqueued_ts_ns: now_ns(),
        });
        assert!(!blocked, "kill-switch must block enqueue while active");
    }

    #[tokio::test]
    async fn execution_kill_switch_recovers_after_cooldown() {
        let health = Arc::new(HealthState::new());
        let execution = spawn_execution_runtime_with_config(
            health.clone(),
            ExecutionConfig {
                send_timeout_ms: 5,
                simulated_send_delay_ms: 50,
                kill_switch_timeout_streak: 1,
                kill_switch_cooldown_ms: 100,
                metrics_flush_interval_ms: 20,
                max_intent_age_ms: 10_000,
                ..ExecutionConfig::default()
            },
        );
        let base_ns = now_ns();
        assert!(execution.try_enqueue(OrderIntent {
            signal: sample_signal(),
            signal_decided_ts_ns: base_ns,
            enqueued_ts_ns: base_ns,
        }));
        tokio::time::sleep(tokio::time::Duration::from_millis(80)).await;
        assert!(
            health
                .runtime_execution_kill_switch_active
                .load(Ordering::Relaxed),
            "kill-switch should trip first"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
        assert!(
            !health
                .runtime_execution_kill_switch_active
                .load(Ordering::Relaxed),
            "kill-switch should auto-recover after cooldown"
        );
        let accepted = execution.try_enqueue(OrderIntent {
            signal: sample_signal(),
            signal_decided_ts_ns: now_ns(),
            enqueued_ts_ns: now_ns(),
        });
        assert!(accepted, "enqueue should work after recovery");
    }

    #[tokio::test]
    async fn execution_queue_full_keeps_latest_overflow_intent() {
        let health = Arc::new(HealthState::new());
        let execution = spawn_execution_runtime_with_config(
            health.clone(),
            ExecutionConfig {
                queue_capacity: 1,
                send_timeout_ms: 200,
                simulated_send_delay_ms: 120,
                max_intent_age_ms: 10_000,
                ..ExecutionConfig::default()
            },
        );

        let mut first_signal = sample_signal();
        first_signal.symbol = "BTCUSDT".to_string();
        let mut second_signal = sample_signal();
        second_signal.symbol = "BTCUSDT".to_string();
        let mut third_signal = sample_signal();
        third_signal.symbol = "ETHUSDT".to_string();

        assert!(execution.try_enqueue(OrderIntent {
            signal: first_signal,
            signal_decided_ts_ns: now_ns(),
            enqueued_ts_ns: now_ns(),
        }));
        // Queue is likely full while worker is still busy.
        assert!(execution.try_enqueue(OrderIntent {
            signal: second_signal,
            signal_decided_ts_ns: now_ns(),
            enqueued_ts_ns: now_ns(),
        }));
        assert!(execution.try_enqueue(OrderIntent {
            signal: third_signal,
            signal_decided_ts_ns: now_ns(),
            enqueued_ts_ns: now_ns(),
        }));

        tokio::time::sleep(tokio::time::Duration::from_millis(420)).await;
        assert!(
            health
                .runtime_execution_sent_intents
                .load(Ordering::Relaxed)
                >= 2,
            "worker should send regular queue + overflow latest intents"
        );
    }
}
