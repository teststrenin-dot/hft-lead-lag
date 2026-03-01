use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::infrastructure::db::DbWriter;
use crate::infrastructure::enrichment;
use crate::infrastructure::exchanges::{BinanceMarketData, GateMarketData};

use super::{
    DbSaturationHealth, HealthResponse, HttpState, RuntimeBacklogDepth, RuntimeDriftSnapshot,
    RuntimeLatencySnapshot, RuntimeLatencyStats, RuntimeStageTimestamps,
};

pub(super) const FALLBACK_ROWS_TTL_MS: i64 = 5_000;
const RM4_BREACH_WINDOW_THRESHOLD: u64 = 3;
const RM4_EVAL_INTERVAL_MS: i64 = 5_000;
const RM4_MAX_INGEST_P99_US: u64 = 1_500;
const RM4_MAX_DECISION_P99_US: u64 = 1_500;
const RM4_MAX_END_TO_END_P99_US: u64 = 2_000;
const RM4_MAX_BINANCE_BACKLOG: u64 = 64;
const RM4_MAX_GATE_BACKLOG: u64 = 64;
const RM4_MAX_SIGNAL_BACKLOG: u64 = 128;
const RM4_MAX_EXECUTION_BACKLOG: u64 = 128;
const RM4_MAX_CONTROL_BACKLOG: u64 = 256;
const DB_WRITER_STALL_THRESHOLD_MS: i64 = 15_000;
const DRIFT_ABS_P99_WARN_MS: u64 = 200;
const ENGINE_STALL_THRESHOLD_MS: i64 = 5_000;
const SIGNAL_LOOP_STALL_THRESHOLD_MS: i64 = 3_000;
const EXECUTION_LOOP_STALL_THRESHOLD_MS: i64 = 3_000;

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn stage_age_ms_from_ns(now_ns: i64, ts_ns: i64) -> Option<i64> {
    if ts_ns <= 0 || now_ns <= ts_ns {
        return None;
    }
    Some(now_ns.saturating_sub(ts_ns) / 1_000_000)
}

pub(super) fn evaluate_db_saturation_health(
    db_dropped_batches: u64,
    db_overflowed_batches: u64,
) -> DbSaturationHealth {
    DbSaturationHealth {
        drop_budget_exhausted: db_dropped_batches > DbWriter::dropped_batch_budget(),
        overflow_warn: db_overflowed_batches >= DbWriter::overflow_warn_threshold(),
    }
}

pub(super) fn should_refresh_fallback_rows_cache(
    now_ms: i64,
    last_refresh_ms: i64,
    cache_empty: bool,
) -> bool {
    if cache_empty || last_refresh_ms <= 0 {
        return true;
    }
    now_ms.saturating_sub(last_refresh_ms) >= FALLBACK_ROWS_TTL_MS
}

pub(super) fn maybe_spawn_fallback_rows_refresh(state: &Arc<HttpState>) {
    if state
        .fallback_rows_refresh_in_flight
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    let min_volume_usd = state.min_volume_usd;
    let cache = state.fallback_rows_cache.clone();
    let last_refresh_ms = state.fallback_rows_last_refresh_ms.clone();
    let refresh_in_flight = state.fallback_rows_refresh_in_flight.clone();
    tokio::spawn(async move {
        let rows = enrichment::fallback_screener_rows(min_volume_usd).await;
        cache.store(Arc::new(rows));
        last_refresh_ms.store(crate::domain::screener::utils::now_ms(), Ordering::Relaxed);
        refresh_in_flight.store(false, Ordering::Relaxed);
    });
}

fn counter_delta_and_update_last(current: u64, last: &std::sync::atomic::AtomicU64) -> u64 {
    let previous = last.swap(current, Ordering::AcqRel);
    current.saturating_sub(previous)
}

fn try_claim_rm4_window_eval(health: &crate::api::HealthState, now_ms: i64) -> bool {
    let mut observed = health.runtime_rm4_last_eval_ms.load(Ordering::Acquire);
    loop {
        if observed > 0 && now_ms.saturating_sub(observed) < RM4_EVAL_INTERVAL_MS {
            return false;
        }
        match health.runtime_rm4_last_eval_ms.compare_exchange_weak(
            observed,
            now_ms,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(current) => {
                observed = current;
            }
        }
    }
}

fn rm4_slo_breached(
    runtime_latency_us: &RuntimeLatencySnapshot,
    runtime_backlog_depth: &RuntimeBacklogDepth,
    execution_dropped_delta: u64,
    execution_timeout_delta: u64,
    control_dropped_delta: u64,
) -> bool {
    runtime_latency_us.ingest.p99_us > RM4_MAX_INGEST_P99_US
        || runtime_latency_us.decision.p99_us > RM4_MAX_DECISION_P99_US
        || runtime_latency_us.end_to_end.p99_us > RM4_MAX_END_TO_END_P99_US
        || runtime_backlog_depth.binance_msg_queue_depth > RM4_MAX_BINANCE_BACKLOG
        || runtime_backlog_depth.gate_msg_queue_depth > RM4_MAX_GATE_BACKLOG
        || runtime_backlog_depth.signal_backlog_depth > RM4_MAX_SIGNAL_BACKLOG
        || runtime_backlog_depth.execution_intent_queue_depth > RM4_MAX_EXECUTION_BACKLOG
        || runtime_backlog_depth.control_update_queue_depth > RM4_MAX_CONTROL_BACKLOG
        || execution_dropped_delta > 0
        || execution_timeout_delta > 0
        || control_dropped_delta > 0
}

pub(super) fn health_response(state: &HttpState) -> (axum::http::StatusCode, HealthResponse) {
    const STALE_TICK_THRESHOLD_MS: i64 = 5_000;
    const TRIAL_ACK_STALE_WARN_MS: i64 = 120_000;
    const TRIAL_QUEUE_DEPTH_WARN_THRESHOLD: u64 = 10;

    let now_ms = crate::domain::screener::utils::now_ms();
    let binance_last_tick_ms = state.health.binance_last_tick_ms.load(Ordering::Relaxed);
    let gate_last_tick_ms = state.health.gate_last_tick_ms.load(Ordering::Relaxed);
    let binance_last_tick_age_ms = if binance_last_tick_ms > 0 {
        now_ms.saturating_sub(binance_last_tick_ms)
    } else {
        i64::MAX
    };
    let gate_last_tick_age_ms = if gate_last_tick_ms > 0 {
        now_ms.saturating_sub(gate_last_tick_ms)
    } else {
        i64::MAX
    };

    let binance_connected = state.health.binance_connected.load(Ordering::Relaxed);
    let gate_connected = state.health.gate_connected.load(Ordering::Relaxed);
    let trial_queue_depth = state.health.trial_queue_depth.load(Ordering::Relaxed);
    let trial_queue_quarantined = state.health.trial_queue_quarantined.load(Ordering::Relaxed);
    let trial_last_ack_ms = state.health.trial_last_ack_ms.load(Ordering::Relaxed);
    let trial_last_ack_error = state.health.trial_last_ack_error.load(Ordering::Relaxed);
    let trial_last_ack_age_ms = if trial_last_ack_ms > 0 {
        Some(now_ms.saturating_sub(trial_last_ack_ms))
    } else {
        None
    };
    let trial_last_ack_status = if trial_last_ack_ms <= 0 {
        "unknown"
    } else if trial_last_ack_error {
        "error"
    } else {
        "ok"
    };
    let trial_active_run_id = state.screener.current_run_id();
    let binance = binance_connected && binance_last_tick_age_ms <= STALE_TICK_THRESHOLD_MS;
    let gate = gate_connected && gate_last_tick_age_ms <= STALE_TICK_THRESHOLD_MS;

    let binance_dropped_messages = BinanceMarketData::dropped_messages();
    let gate_dropped_messages = GateMarketData::dropped_messages();
    let control_dropped_updates = state
        .health
        .runtime_control_dropped_updates
        .load(Ordering::Relaxed);
    let db_dropped_batches = DbWriter::dropped_batches();
    let db_overflowed_batches = DbWriter::overflowed_batches();
    let db_writer_enqueued_seq = DbWriter::watchdog_enqueued_max_seq();
    let db_writer_observed_seq = DbWriter::watchdog_observed_max_seq();
    let db_writer_backlog_seq = db_writer_enqueued_seq.saturating_sub(db_writer_observed_seq);
    let db_writer_last_progress_ms = DbWriter::watchdog_last_progress_ms();
    let db_writer_last_progress_age_ms = if db_writer_last_progress_ms > 0 {
        Some(now_ms.saturating_sub(db_writer_last_progress_ms))
    } else {
        None
    };
    let execution_sent_intents = state
        .health
        .runtime_execution_sent_intents
        .load(Ordering::Relaxed);
    let execution_dropped_intents = state
        .health
        .runtime_execution_dropped_intents
        .load(Ordering::Relaxed);
    let execution_send_timeouts = state
        .health
        .runtime_execution_send_timeouts
        .load(Ordering::Relaxed);
    let execution_kill_switch_active = state
        .health
        .runtime_execution_kill_switch_active
        .load(Ordering::Relaxed);
    let db_saturation = evaluate_db_saturation_health(db_dropped_batches, db_overflowed_batches);
    let runtime_stage_timestamps = RuntimeStageTimestamps {
        recv_ws_frame_ts_ns: state
            .health
            .runtime_last_recv_ws_frame_ts_ns
            .load(Ordering::Relaxed),
        parsed_ts_ns: state
            .health
            .runtime_last_parsed_ts_ns
            .load(Ordering::Relaxed),
        state_updated_ts_ns: state
            .health
            .runtime_last_state_updated_ts_ns
            .load(Ordering::Relaxed),
        signal_decided_ts_ns: state
            .health
            .runtime_last_signal_decided_ts_ns
            .load(Ordering::Relaxed),
        order_intent_enqueued_ts_ns: state
            .health
            .runtime_last_order_intent_enqueued_ts_ns
            .load(Ordering::Relaxed),
        order_intent_sent_ts_ns: state
            .health
            .runtime_last_order_intent_sent_ts_ns
            .load(Ordering::Relaxed),
    };
    let runtime_latency_us = RuntimeLatencySnapshot {
        ingest: RuntimeLatencyStats {
            samples: state.health.runtime_ingest_samples.load(Ordering::Relaxed),
            p50_us: state.health.runtime_ingest_p50_us.load(Ordering::Relaxed),
            p95_us: state.health.runtime_ingest_p95_us.load(Ordering::Relaxed),
            p99_us: state.health.runtime_ingest_p99_us.load(Ordering::Relaxed),
            max_us: state.health.runtime_ingest_max_us.load(Ordering::Relaxed),
        },
        decision: RuntimeLatencyStats {
            samples: state
                .health
                .runtime_decision_samples
                .load(Ordering::Relaxed),
            p50_us: state.health.runtime_decision_p50_us.load(Ordering::Relaxed),
            p95_us: state.health.runtime_decision_p95_us.load(Ordering::Relaxed),
            p99_us: state.health.runtime_decision_p99_us.load(Ordering::Relaxed),
            max_us: state.health.runtime_decision_max_us.load(Ordering::Relaxed),
        },
        end_to_end: RuntimeLatencyStats {
            samples: state
                .health
                .runtime_end_to_end_samples
                .load(Ordering::Relaxed),
            p50_us: state
                .health
                .runtime_end_to_end_p50_us
                .load(Ordering::Relaxed),
            p95_us: state
                .health
                .runtime_end_to_end_p95_us
                .load(Ordering::Relaxed),
            p99_us: state
                .health
                .runtime_end_to_end_p99_us
                .load(Ordering::Relaxed),
            max_us: state
                .health
                .runtime_end_to_end_max_us
                .load(Ordering::Relaxed),
        },
        execution_intent_to_sent: RuntimeLatencyStats {
            samples: state
                .health
                .runtime_execution_intent_to_sent_samples
                .load(Ordering::Relaxed),
            p50_us: state
                .health
                .runtime_execution_intent_to_sent_p50_us
                .load(Ordering::Relaxed),
            p95_us: state
                .health
                .runtime_execution_intent_to_sent_p95_us
                .load(Ordering::Relaxed),
            p99_us: state
                .health
                .runtime_execution_intent_to_sent_p99_us
                .load(Ordering::Relaxed),
            max_us: state
                .health
                .runtime_execution_intent_to_sent_max_us
                .load(Ordering::Relaxed),
        },
    };
    let runtime_drift_ms = RuntimeDriftSnapshot {
        samples: state.health.runtime_drift_samples.load(Ordering::Relaxed),
        avg_ms: state.health.runtime_drift_avg_ms.load(Ordering::Relaxed),
        p50_ms: state.health.runtime_drift_p50_ms.load(Ordering::Relaxed),
        p95_ms: state.health.runtime_drift_p95_ms.load(Ordering::Relaxed),
        p99_ms: state.health.runtime_drift_p99_ms.load(Ordering::Relaxed),
        abs_p99_ms: state
            .health
            .runtime_drift_abs_p99_ms
            .load(Ordering::Relaxed),
        abs_max_ms: state
            .health
            .runtime_drift_abs_max_ms
            .load(Ordering::Relaxed),
    };
    let runtime_backlog_depth = RuntimeBacklogDepth {
        binance_msg_queue_depth: state
            .health
            .runtime_binance_msg_queue_depth
            .load(Ordering::Relaxed),
        gate_msg_queue_depth: state
            .health
            .runtime_gate_msg_queue_depth
            .load(Ordering::Relaxed),
        signal_backlog_depth: state
            .health
            .runtime_signal_backlog_depth
            .load(Ordering::Relaxed),
        control_update_queue_depth: state
            .health
            .runtime_control_queue_depth
            .load(Ordering::Relaxed),
        execution_intent_queue_depth: state
            .health
            .runtime_execution_queue_depth
            .load(Ordering::Relaxed),
    };
    let mut rm4_breach_streak = state
        .health
        .runtime_rm4_breach_streak
        .load(Ordering::Relaxed);
    let mut rm4_breached = state
        .health
        .runtime_rm4_last_window_breached
        .load(Ordering::Relaxed);
    let mut hft_mode_degraded = state
        .health
        .runtime_hft_mode_degraded
        .load(Ordering::Relaxed);
    if try_claim_rm4_window_eval(&state.health, now_ms) {
        let execution_dropped_delta = counter_delta_and_update_last(
            execution_dropped_intents,
            &state.health.runtime_last_eval_execution_dropped_intents,
        );
        let execution_timeout_delta = counter_delta_and_update_last(
            execution_send_timeouts,
            &state.health.runtime_last_eval_execution_send_timeouts,
        );
        let control_dropped_delta = counter_delta_and_update_last(
            control_dropped_updates,
            &state.health.runtime_last_eval_control_dropped_updates,
        );
        rm4_breached = rm4_slo_breached(
            &runtime_latency_us,
            &runtime_backlog_depth,
            execution_dropped_delta,
            execution_timeout_delta,
            control_dropped_delta,
        );
        state
            .health
            .runtime_rm4_last_window_breached
            .store(rm4_breached, Ordering::Relaxed);
        rm4_breach_streak = if rm4_breached {
            state
                .health
                .runtime_rm4_breach_streak
                .fetch_add(1, Ordering::AcqRel)
                + 1
        } else {
            state
                .health
                .runtime_rm4_breach_streak
                .store(0, Ordering::Release);
            0
        };
        hft_mode_degraded = rm4_breach_streak >= RM4_BREACH_WINDOW_THRESHOLD;
        if hft_mode_degraded {
            state
                .health
                .runtime_hft_mode_ever_degraded
                .store(true, Ordering::Relaxed);
        }
        state
            .health
            .runtime_hft_mode_degraded
            .store(hft_mode_degraded, Ordering::Relaxed);
    }
    let hft_mode_ever_degraded = state
        .health
        .runtime_hft_mode_ever_degraded
        .load(Ordering::Relaxed);
    let hft_mode_status = if hft_mode_degraded {
        "degraded_non_hft"
    } else {
        "hft"
    };
    let now_ns = now_ns();
    let state_updated_age_ms =
        stage_age_ms_from_ns(now_ns, runtime_stage_timestamps.state_updated_ts_ns);
    let signal_decided_age_ms =
        stage_age_ms_from_ns(now_ns, runtime_stage_timestamps.signal_decided_ts_ns);
    let order_intent_enqueued_age_ms =
        stage_age_ms_from_ns(now_ns, runtime_stage_timestamps.order_intent_enqueued_ts_ns);
    let order_intent_sent_age_ms =
        stage_age_ms_from_ns(now_ns, runtime_stage_timestamps.order_intent_sent_ts_ns);

    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    if !binance_connected {
        issues.push("binance_disconnected");
    } else if binance_last_tick_age_ms > STALE_TICK_THRESHOLD_MS {
        issues.push("binance_stale");
    }
    if !gate_connected {
        issues.push("gate_disconnected");
    } else if gate_last_tick_age_ms > STALE_TICK_THRESHOLD_MS {
        issues.push("gate_stale");
    }
    if binance_dropped_messages > 0 {
        issues.push("binance_dropped_messages");
    }
    if gate_dropped_messages > 0 {
        issues.push("gate_dropped_messages");
    }
    if db_saturation.drop_budget_exhausted {
        issues.push("db_drop_budget_exhausted");
    }
    if db_saturation.overflow_warn {
        warnings.push("db_overflow_batches_high");
    }
    if db_writer_backlog_seq > 0
        && db_writer_last_progress_age_ms
            .map(|age| age > DB_WRITER_STALL_THRESHOLD_MS)
            .unwrap_or(false)
    {
        issues.push("db_writer_stall");
    }
    if runtime_drift_ms.samples > 0 && runtime_drift_ms.abs_p99_ms > DRIFT_ABS_P99_WARN_MS {
        warnings.push("drift_p99_high");
    }
    if execution_send_timeouts > 0 {
        warnings.push("execution_send_timeouts_present");
    }
    if execution_dropped_intents > 0 {
        warnings.push("execution_intents_dropped");
    }
    if control_dropped_updates > 0 {
        warnings.push("control_updates_dropped");
    }
    if execution_kill_switch_active {
        issues.push("execution_kill_switch_active");
    }
    if hft_mode_degraded {
        issues.push("hft_slo_degraded_non_hft");
    } else if rm4_breached {
        warnings.push("hft_slo_window_breach");
    }
    if binance
        && gate
        && state_updated_age_ms
            .map(|age| age > ENGINE_STALL_THRESHOLD_MS)
            .unwrap_or(true)
    {
        issues.push("engine_state_stall");
    }
    if runtime_backlog_depth.signal_backlog_depth > 0
        && signal_decided_age_ms
            .map(|age| age > SIGNAL_LOOP_STALL_THRESHOLD_MS)
            .unwrap_or(true)
    {
        issues.push("signal_loop_stall");
    }
    if runtime_backlog_depth.execution_intent_queue_depth > 0 {
        let enqueued_stalled = order_intent_enqueued_age_ms
            .map(|age| age > EXECUTION_LOOP_STALL_THRESHOLD_MS)
            .unwrap_or(true);
        let sent_stalled = order_intent_sent_age_ms
            .map(|age| age > EXECUTION_LOOP_STALL_THRESHOLD_MS)
            .unwrap_or(true);
        if enqueued_stalled && sent_stalled {
            issues.push("execution_loop_stall");
        }
    }
    if trial_queue_depth >= TRIAL_QUEUE_DEPTH_WARN_THRESHOLD {
        warnings.push("trial_queue_depth_high");
    }
    if trial_queue_quarantined > 0 {
        warnings.push("trial_queue_quarantined_present");
    }
    if trial_last_ack_error {
        warnings.push("trial_last_ack_error");
    }
    if trial_queue_depth > 0 {
        match trial_last_ack_age_ms {
            Some(age_ms) if age_ms > TRIAL_ACK_STALE_WARN_MS => {
                warnings.push("trial_ack_stale");
            }
            None => warnings.push("trial_ack_missing_for_queued_batches"),
            _ => {}
        }
    }

    let healthy = issues.is_empty();
    let status = if healthy { "ok" } else { "degraded" };
    let alert_level = if !issues.is_empty() {
        "critical"
    } else if !warnings.is_empty() {
        "warn"
    } else {
        "ok"
    };
    let code = if healthy {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        HealthResponse {
            status,
            alert_level,
            binance,
            gate,
            binance_last_tick_age_ms,
            gate_last_tick_age_ms,
            trial_queue_depth,
            trial_queue_quarantined,
            trial_last_ack_age_ms,
            trial_last_ack_status,
            trial_active_run_id,
            binance_dropped_messages,
            gate_dropped_messages,
            control_dropped_updates,
            db_dropped_batches,
            db_overflowed_batches,
            db_dropped_batch_budget: DbWriter::dropped_batch_budget(),
            db_overflow_warn_threshold: DbWriter::overflow_warn_threshold(),
            db_writer_enqueued_seq,
            db_writer_observed_seq,
            db_writer_backlog_seq,
            db_writer_last_progress_age_ms,
            execution_sent_intents,
            execution_dropped_intents,
            execution_send_timeouts,
            execution_kill_switch_active,
            runtime_stage_timestamps,
            runtime_latency_us,
            runtime_drift_ms,
            runtime_backlog_depth,
            hft_mode_status,
            hft_mode_ever_degraded,
            rm4_breach_streak,
            rm4_window_threshold: RM4_BREACH_WINDOW_THRESHOLD,
            rm4_window_interval_ms: RM4_EVAL_INTERVAL_MS,
            issues,
            warnings,
        },
    )
}
