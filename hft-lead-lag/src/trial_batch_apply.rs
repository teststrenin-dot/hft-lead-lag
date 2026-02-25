use super::{build_trial_batch_patch_plan, EventLoopState, ScreenerStore, TrialAck, TrialBatch};
use hft_lead_lag::domain::screener::TraderConfig;
use hft_lead_lag::infrastructure::db::TrialPatchMeta;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

fn upsert_runtime_configs(db_path: &Path, configs: &[TraderConfig]) -> Result<(), String> {
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
        .map_err(|e| format!("open db {}: {e}", db_path.display()))?;
    hft_lead_lag::infrastructure::db::upsert_configs(&conn, configs)
        .map_err(|e| format!("upsert runtime configs: {e}"))?;
    Ok(())
}

pub(super) async fn upsert_runtime_configs_async(
    db_path: PathBuf,
    configs: Vec<TraderConfig>,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || upsert_runtime_configs(&db_path, &configs))
        .await
        .map_err(|e| format!("runtime-config upsert task join error: {e}"))?
}

fn upsert_trial_run_meta(
    db_path: &Path,
    run_id: &str,
    submitted_config_count: usize,
    applied_at_ms: i64,
    drained_trades: usize,
    patch: TrialPatchMeta<'_>,
) -> Result<(), String> {
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
        .map_err(|e| format!("open db {}: {e}", db_path.display()))?;
    hft_lead_lag::infrastructure::db::upsert_trial_run_meta(
        &conn,
        run_id,
        submitted_config_count,
        applied_at_ms,
        drained_trades,
        patch,
    )
    .map_err(|e| format!("upsert trial run meta: {e}"))?;
    Ok(())
}

async fn upsert_trial_run_meta_async(
    db_path: PathBuf,
    run_id: String,
    submitted_config_count: usize,
    applied_at_ms: i64,
    drained_trades: usize,
    patch: TrialPatchMeta<'static>,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        upsert_trial_run_meta(
            &db_path,
            &run_id,
            submitted_config_count,
            applied_at_ms,
            drained_trades,
            patch,
        )
    })
    .await
    .map_err(|e| format!("trial-run meta upsert task join error: {e}"))?
}

fn close_trial_run_meta(db_path: &Path, run_id: &str, closed_at_ms: i64) -> Result<(), String> {
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
        .map_err(|e| format!("open db {}: {e}", db_path.display()))?;
    hft_lead_lag::infrastructure::db::close_trial_run_meta(&conn, run_id, closed_at_ms)
        .map_err(|e| format!("close trial run meta: {e}"))?;
    Ok(())
}

pub(super) async fn close_trial_run_meta_async(
    db_path: PathBuf,
    run_id: String,
    closed_at_ms: i64,
) -> Result<(), String> {
    tokio::task::spawn_blocking(move || close_trial_run_meta(&db_path, &run_id, closed_at_ms))
        .await
        .map_err(|e| format!("trial-run close task join error: {e}"))?
}

pub(super) fn validate_trial_batch_run_lease(
    active_run_id: Option<&str>,
    incoming_run_id: &str,
    allow_run_id_takeover: bool,
) -> Result<(), String> {
    let Some(active_run_id) = active_run_id.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    if active_run_id == incoming_run_id || allow_run_id_takeover {
        return Ok(());
    }
    Err(format!(
        "active run_id lease held by {active_run_id}; reject run_id={incoming_run_id} (set allow_run_id_takeover=true to override)"
    ))
}

pub(super) async fn apply_trial_batch(
    screener: &ScreenerStore,
    db_path: PathBuf,
    batch: TrialBatch,
) -> TrialAck {
    let run_id = batch.run_id.clone();
    let submission_id = batch.submission_id.clone();
    let previous_run_id = screener.current_run_id();
    if let Err(e) = validate_trial_batch_run_lease(
        previous_run_id.as_deref(),
        &run_id,
        batch.allow_run_id_takeover,
    ) {
        warn!("trial-batch: {e}");
        return TrialAck::error(run_id, e, submission_id);
    }
    let config_count = batch.configs.len();
    let patch_plan = match build_trial_batch_patch_plan(&batch) {
        Ok(plan) => plan,
        Err(e) => {
            warn!("trial-batch: invalid payload: {e}");
            return TrialAck::error(run_id, e, submission_id);
        }
    };
    let mode = patch_plan.mode;
    let runtime_configs = batch.configs.clone();
    let report = match screener.try_apply_fleet_patch(batch.configs, patch_plan) {
        Ok(report) => report,
        Err(e) => {
            warn!("trial-batch: patch rejected: {e}");
            return TrialAck::error(run_id, e.to_string(), submission_id);
        }
    };
    if let Err(e) = upsert_runtime_configs_async(db_path.clone(), runtime_configs).await {
        warn!("trial-batch: db upsert failed after patch apply: {e}");
    }
    let applied_at_ms = EventLoopState::now_ms();
    if let Some(previous_run_id) = previous_run_id.as_ref() {
        if previous_run_id != &run_id {
            if let Err(e) =
                close_trial_run_meta_async(db_path.clone(), previous_run_id.clone(), applied_at_ms)
                    .await
            {
                warn!("trial-batch: failed to close previous run_id={previous_run_id}: {e}");
            }
        }
    }
    screener.set_run_id(Some(run_id.clone()));
    screener.flush_db_writer().await;
    if let Err(e) = upsert_trial_run_meta_async(
        db_path,
        run_id.clone(),
        config_count,
        applied_at_ms,
        report.drained_trades,
        TrialPatchMeta {
            apply_mode: mode.as_str(),
            symbols_reset: report.symbols_reset,
            changed_ids_requested: report.changed_ids_requested,
            matched_changed_ids_old: report.matched_changed_ids_old,
            matched_changed_ids_new: report.matched_changed_ids_new,
            unmatched_changed_ids: report.unmatched_changed_ids,
            scope_symbols_requested: report.scope_symbols_requested,
            scope_symbols_matched: report.scope_symbols_matched,
        },
    )
    .await
    {
        warn!("trial-batch: meta upsert failed: {e}");
    }
    info!(
        "trial-batch: applied run_id={run_id} mode={} configs={config_count} \
         symbols_reset={} drained_trades={} \
         changed_ids_requested={} matched_old={} matched_new={} unmatched_changed_ids={} \
         scope_symbols={}/{}",
        mode.as_str(),
        report.symbols_reset,
        report.drained_trades,
        report.changed_ids_requested,
        report.matched_changed_ids_old,
        report.matched_changed_ids_new,
        report.unmatched_changed_ids,
        report.scope_symbols_matched,
        report.scope_symbols_requested
    );
    if report.unmatched_changed_ids > 0 {
        warn!(
            "trial-batch: changed_config_ids include {} unknown ids (run_id={run_id})",
            report.unmatched_changed_ids
        );
    }
    if report.scope_symbols_requested > 0 && report.scope_symbols_matched == 0 {
        warn!(
            "trial-batch: symbol scope matched nothing run_id={run_id} requested_scope_symbols={}",
            report.scope_symbols_requested
        );
    }
    TrialAck::success(
        run_id,
        applied_at_ms,
        config_count,
        report.drained_trades,
        submission_id,
    )
}
