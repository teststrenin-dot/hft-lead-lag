use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::infrastructure::db::DbWriter;
use crate::infrastructure::enrichment;
use crate::infrastructure::exchanges::{BinanceMarketData, GateMarketData};

use super::{
    DbSaturationHealth, HealthResponse, HttpState, RuntimeBacklogDepth, RuntimeLatencySnapshot,
    RuntimeLatencyStats, RuntimeStageTimestamps,
};

pub(super) const FALLBACK_ROWS_TTL_MS: i64 = 5_000;

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
    let db_dropped_batches = DbWriter::dropped_batches();
    let db_overflowed_batches = DbWriter::overflowed_batches();
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
        execution_intent_queue_depth: state
            .health
            .runtime_execution_queue_depth
            .load(Ordering::Relaxed),
    };

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
    if execution_send_timeouts > 0 {
        warnings.push("execution_send_timeouts_present");
    }
    if execution_dropped_intents > 0 {
        warnings.push("execution_intents_dropped");
    }
    if execution_kill_switch_active {
        issues.push("execution_kill_switch_active");
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
    let code = if healthy {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        HealthResponse {
            status,
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
            db_dropped_batches,
            db_overflowed_batches,
            db_dropped_batch_budget: DbWriter::dropped_batch_budget(),
            db_overflow_warn_threshold: DbWriter::overflow_warn_threshold(),
            execution_sent_intents,
            execution_dropped_intents,
            execution_send_timeouts,
            execution_kill_switch_active,
            runtime_stage_timestamps,
            runtime_latency_us,
            runtime_backlog_depth,
            issues,
            warnings,
        },
    )
}
