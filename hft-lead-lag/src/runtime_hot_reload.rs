use super::*;

const HOT_RELOAD_DEFAULT_SLEEP_MS: u64 = 5_000;
const HOT_RELOAD_MIN_SLEEP_MS: u64 = 500;
const TRIAL_BATCH_WATCH_SLEEP_MS: u64 = 500;
const TRIAL_CONTROL_WATCH_SLEEP_MS: u64 = 500;
const RUNTIME_GRID_RESET_CHANNEL_CAPACITY: usize = 32;

#[derive(Default)]
struct TrialBatchWatchState {
    last_trial_modified: Option<FileFingerprint>,
}

struct RuntimeGridWatchState {
    last_modified: Option<FileFingerprint>,
    last_applied_signature: Option<u64>,
    pending: Option<RuntimeGridGeneration>,
    last_apply_ms: i64,
}

fn record_trial_ack_health(health_state: &HealthState, ack: &TrialAck) {
    health_state
        .trial_last_ack_ms
        .store(ack.applied_at_ms, Ordering::Relaxed);
    health_state
        .trial_last_ack_error
        .store(ack.status != "ok", Ordering::Relaxed);
}

fn update_trial_queue_depth_health(health_state: &HealthState, config_dir: &Path) {
    let depth = list_trial_batch_queue_files(config_dir).len() as u64;
    health_state
        .trial_queue_depth
        .store(depth, Ordering::Relaxed);
}

async fn maybe_handle_trial_control(
    screener: &ScreenerStore,
    db_path: &Path,
    trial_control_path: &Path,
    last_trial_control_modified: &mut Option<FileFingerprint>,
) {
    let trial_control_modified = read_file_fingerprint(trial_control_path);
    let control_changed =
        file_fingerprint_changed(*last_trial_control_modified, trial_control_modified);
    if !control_changed {
        return;
    }
    *last_trial_control_modified = trial_control_modified;
    match load_trial_control(trial_control_path) {
        Ok(control) => {
            if !control.clear_run_id {
                return;
            }
            let active_run_id = screener.current_run_id();
            match active_run_id {
                Some(active) => {
                    let request_matches = control
                        .run_id
                        .as_ref()
                        .is_none_or(|requested| requested == &active);
                    if request_matches {
                        let closed_at_ms = EventLoopState::now_ms();
                        if let Err(e) = close_trial_run_meta_async(
                            db_path.to_path_buf(),
                            active.clone(),
                            closed_at_ms,
                        )
                        .await
                        {
                            warn!("trial-control: failed to close run {active}: {e}");
                        }
                        screener.set_run_id(None);
                        info!("trial-control: cleared run_id={active} closed_at_ms={closed_at_ms}");
                    } else if let Some(requested) = control.run_id {
                        warn!(
                            "trial-control: requested run_id={} does not match active run_id={}",
                            requested, active
                        );
                    }
                }
                None => info!("trial-control: no active run_id to clear"),
            }
        }
        Err(e) => warn!("trial-control: {e}"),
    }
}

async fn load_apply_ack_trial_batch(
    screener: &ScreenerStore,
    db_path: &Path,
    health_state: &HealthState,
    batch_path: &Path,
    ack_dir: &Path,
    is_queue_mode: bool,
) -> bool {
    let ack = match load_trial_batch(batch_path) {
        Ok(batch) => apply_trial_batch(screener, db_path.to_path_buf(), batch).await,
        Err(e) => {
            if is_queue_mode {
                warn!(
                    "trial-batch queue: invalid payload {}: {e}",
                    batch_path.display()
                );
            } else {
                warn!("trial-batch: {e}");
            }
            build_trial_batch_error_ack(batch_path, is_queue_mode, e)
        }
    };
    record_trial_ack_health(health_state, &ack);
    let is_ok = ack.status == "ok";
    write_trial_ack(ack_dir, &ack);
    is_ok
}

async fn maybe_handle_trial_batch_file(
    screener: &ScreenerStore,
    db_path: &Path,
    health_state: &HealthState,
    trial_batch_path: &Path,
    last_trial_modified: &mut Option<FileFingerprint>,
) -> bool {
    let trial_modified = read_file_fingerprint(trial_batch_path);
    let trial_changed = file_fingerprint_changed(*last_trial_modified, trial_modified);
    if !trial_changed {
        return false;
    }
    *last_trial_modified = trial_modified;
    let ack_dir = trial_batch_path.parent().unwrap_or(Path::new("."));
    load_apply_ack_trial_batch(
        screener,
        db_path,
        health_state,
        trial_batch_path,
        ack_dir,
        false,
    )
    .await
}

async fn maybe_handle_trial_batch_queue(
    screener: &ScreenerStore,
    db_path: &Path,
    health_state: &HealthState,
    config_dir: &Path,
) -> bool {
    let Some(queued_batch_path) = list_trial_batch_queue_files(config_dir).into_iter().next()
    else {
        return false;
    };
    let is_ok = load_apply_ack_trial_batch(
        screener,
        db_path,
        health_state,
        &queued_batch_path,
        config_dir,
        true,
    )
    .await;
    archive_trial_batch_queue_file(config_dir, &queued_batch_path, is_ok);
    is_ok
}

async fn maybe_refresh_pending_runtime_grid(
    config_path: &Path,
    last_modified: &mut Option<FileFingerprint>,
    pending: &mut Option<RuntimeGridGeneration>,
) {
    let modified = read_file_fingerprint(config_path);
    let changed = file_fingerprint_changed(*last_modified, modified);
    if !changed {
        return;
    }
    *last_modified = modified;
    match load_runtime_grid_generation_async(config_path.to_path_buf()).await {
        Ok(generation) => {
            if generation.config.enabled {
                info!(
                    "runtime-grid: detected update, pending apply configs={} max_configs={} apply_interval_ms={}",
                    generation.configs.len(),
                    generation.config.max_configs,
                    generation.config.apply_interval_ms
                );
                *pending = Some(generation);
            } else {
                info!("runtime-grid: disabled in {}", config_path.display());
                *pending = None;
            }
        }
        Err(e) => {
            warn!("runtime-grid: invalid update ignored: {e}");
        }
    }
}

async fn maybe_apply_pending_runtime_grid(
    screener: &ScreenerStore,
    db_path: &Path,
    pending: &mut Option<RuntimeGridGeneration>,
    last_apply_ms: &mut i64,
    last_applied_signature: &mut Option<u64>,
) {
    let Some(generation) = pending.as_ref() else {
        return;
    };
    let now_ms = EventLoopState::now_ms();
    let apply_interval_ms = generation.config.apply_interval_ms.max(1_000) as i64;
    if now_ms.saturating_sub(*last_apply_ms) < apply_interval_ms {
        return;
    }
    if Some(generation.signature) == *last_applied_signature {
        *pending = None;
        return;
    }
    if let Err(e) =
        upsert_runtime_configs_async(db_path.to_path_buf(), generation.configs.clone()).await
    {
        warn!("runtime-grid: apply postponed, db upsert failed: {e}");
        return;
    }
    let report = screener.replace_fleet_configs(generation.configs.clone());
    screener.flush_db_writer().await;
    *last_apply_ms = now_ms;
    *last_applied_signature = Some(generation.signature);
    info!(
        "runtime-grid: applied configs old={} new={} symbols_reset={} drained_trades={} (flushed)",
        report.old_config_count,
        report.new_config_count,
        report.symbols_reset,
        report.drained_trades
    );
    *pending = None;
}

pub(super) fn drain_runtime_grid_reset_signals(
    grid_reset_rx: &mut tokio::sync::mpsc::Receiver<()>,
) -> bool {
    let mut had_signal = false;
    loop {
        match grid_reset_rx.try_recv() {
            Ok(()) => had_signal = true,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    had_signal
}

pub(super) fn runtime_grid_sleep_ms(pending: Option<&RuntimeGridGeneration>) -> u64 {
    pending
        .map(|generation| generation.config.watch_interval_ms)
        .unwrap_or(HOT_RELOAD_DEFAULT_SLEEP_MS)
        .max(HOT_RELOAD_MIN_SLEEP_MS)
}

async fn run_trial_control_watch_loop(
    screener: ScreenerStore,
    db_path: PathBuf,
    trial_control_path: PathBuf,
) {
    let mut last_trial_control_modified: Option<FileFingerprint> = None;
    loop {
        maybe_handle_trial_control(
            &screener,
            &db_path,
            &trial_control_path,
            &mut last_trial_control_modified,
        )
        .await;

        tokio::time::sleep(tokio::time::Duration::from_millis(
            TRIAL_CONTROL_WATCH_SLEEP_MS,
        ))
        .await;
    }
}

async fn run_trial_batch_watch_loop(
    screener: ScreenerStore,
    db_path: PathBuf,
    health_state: Arc<HealthState>,
    trial_batch_path: PathBuf,
    grid_reset_tx: tokio::sync::mpsc::Sender<()>,
) {
    let mut state = TrialBatchWatchState::default();
    let config_dir = trial_batch_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    loop {
        update_trial_queue_depth_health(health_state.as_ref(), &config_dir);

        let mut clear_runtime_grid_pending = false;
        if maybe_handle_trial_batch_file(
            &screener,
            &db_path,
            health_state.as_ref(),
            &trial_batch_path,
            &mut state.last_trial_modified,
        )
        .await
        {
            clear_runtime_grid_pending = true;
        }

        if maybe_handle_trial_batch_queue(&screener, &db_path, health_state.as_ref(), &config_dir)
            .await
        {
            clear_runtime_grid_pending = true;
        }

        update_trial_queue_depth_health(health_state.as_ref(), &config_dir);

        if clear_runtime_grid_pending {
            let _ = grid_reset_tx.try_send(());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(
            TRIAL_BATCH_WATCH_SLEEP_MS,
        ))
        .await;
    }
}

async fn run_runtime_grid_watch_loop(
    screener: ScreenerStore,
    db_path: PathBuf,
    config_path: PathBuf,
    initial_modified: Option<FileFingerprint>,
    initial_signature: Option<u64>,
    mut grid_reset_rx: tokio::sync::mpsc::Receiver<()>,
) {
    let mut state = RuntimeGridWatchState {
        last_modified: initial_modified,
        last_applied_signature: initial_signature,
        pending: None,
        last_apply_ms: EventLoopState::now_ms(),
    };

    loop {
        if drain_runtime_grid_reset_signals(&mut grid_reset_rx) {
            state.pending = None;
        }

        maybe_refresh_pending_runtime_grid(
            &config_path,
            &mut state.last_modified,
            &mut state.pending,
        )
        .await;
        maybe_apply_pending_runtime_grid(
            &screener,
            &db_path,
            &mut state.pending,
            &mut state.last_apply_ms,
            &mut state.last_applied_signature,
        )
        .await;

        tokio::time::sleep(tokio::time::Duration::from_millis(runtime_grid_sleep_ms(
            state.pending.as_ref(),
        )))
        .await;
    }
}

#[derive(Debug)]
pub(super) struct RuntimeGridHotReloadSpec {
    pub(super) config_path: PathBuf,
    pub(super) trial_batch_path: PathBuf,
    pub(super) trial_control_path: PathBuf,
    pub(super) initial_modified: Option<FileFingerprint>,
    pub(super) initial_signature: Option<u64>,
}

pub(super) fn spawn_runtime_grid_hot_reload(
    screener: ScreenerStore,
    db_path: PathBuf,
    health_state: Arc<HealthState>,
    spec: RuntimeGridHotReloadSpec,
) {
    let RuntimeGridHotReloadSpec {
        config_path,
        trial_batch_path,
        trial_control_path,
        initial_modified,
        initial_signature,
    } = spec;
    let (grid_reset_tx, grid_reset_rx) =
        tokio::sync::mpsc::channel::<()>(RUNTIME_GRID_RESET_CHANNEL_CAPACITY);

    let trial_screener = screener.clone();
    let trial_db_path = db_path.clone();
    let trial_health_state = health_state.clone();
    let trial_batch_path_clone = trial_batch_path.clone();
    let trial_control_path_clone = trial_control_path.clone();
    let control_screener = screener.clone();
    let control_db_path = db_path.clone();

    tokio::spawn(async move {
        run_trial_batch_watch_loop(
            trial_screener,
            trial_db_path,
            trial_health_state,
            trial_batch_path_clone,
            grid_reset_tx,
        )
        .await;
    });

    tokio::spawn(async move {
        run_trial_control_watch_loop(control_screener, control_db_path, trial_control_path_clone)
            .await;
    });

    tokio::spawn(async move {
        run_runtime_grid_watch_loop(
            screener,
            db_path,
            config_path,
            initial_modified,
            initial_signature,
            grid_reset_rx,
        )
        .await;
    });
}
