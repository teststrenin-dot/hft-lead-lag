//! Embedded trial runner for launching ray_driver phases from the API.

mod command;

use crate::domain::screener::{TraderConfig, CONFIG_ID_CONTRACT_VERSION};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{oneshot, Mutex};

pub const DEFAULT_SCOUT_DURATION_S: u64 = 900;
pub const DEFAULT_SCOUT_CYCLES: u64 = 1;
pub const DEFAULT_EXPAND_DURATION_S: u64 = 900;
pub const DEFAULT_EXPAND_CYCLES: u64 = 1;
pub const DEFAULT_FORWARD_MAX_BUDGET_S: u64 = 240;
pub const DEFAULT_FORWARD_GRACE_PERIOD_S: u64 = 60;
pub const DEFAULT_FORWARD_REPORT_INTERVAL_S: u64 = 30;
pub const DEFAULT_FORWARD_MAX_REFS: u64 = 64;
pub const DEFAULT_FORWARD_MAX_CONFIGS: u64 = 1200;
pub const FORWARD_MAX_REFS_HARD_CAP: u64 = 256;
pub const FORWARD_MAX_CONFIGS_HARD_CAP: u64 = 5000;
pub const DEFAULT_PROMOTE_TOP_K: u64 = 50;
pub const DEFAULT_PROMOTE_MIN_TRADES: u64 = 5;
pub const DEFAULT_PROMOTE_MIN_PNL: f64 = 0.0;

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerStartRequest {
    pub phase: String,
    pub duration: Option<u64>,
    pub cycles: Option<u64>,
    pub max_budget: Option<u64>,
    pub grace_period: Option<u64>,
    pub report_interval: Option<u64>,
    pub max_refs: Option<u64>,
    pub max_configs: Option<u64>,
    pub run_id: Option<String>,
    pub top_k: Option<u64>,
    pub min_trades: Option<u64>,
    pub min_pnl: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerStartResponse {
    pub job_id: u64,
    pub phase: String,
    pub command: String,
    pub started_at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerStopResponse {
    pub stop_requested: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerPhaseDefaults {
    pub name: String,
    pub duration: Option<u64>,
    pub cycles: Option<u64>,
    pub max_budget: Option<u64>,
    pub grace_period: Option<u64>,
    pub report_interval: Option<u64>,
    pub max_refs: Option<u64>,
    pub max_configs: Option<u64>,
    pub top_k: Option<u64>,
    pub min_trades: Option<u64>,
    pub min_pnl: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerUiConfig {
    pub phases: Vec<RunnerPhaseDefaults>,
}

#[derive(Debug, Clone)]
pub struct RunnerCommandSpec {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ForwardPhaseOptions {
    max_budget_s: u64,
    grace_period_s: u64,
    report_interval_s: u64,
    max_refs: u64,
    max_configs: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ScoutReferenceRow {
    config_id: i64,
    trades: u64,
    avg_pnl_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct TrialBatchPayload<'a> {
    run_id: &'a str,
    configs: &'a [TraderConfig],
    config_id_contract_version: u16,
    submission_id: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct TrialControlPayload<'a> {
    clear_run_id: bool,
    run_id: &'a str,
}

#[derive(Debug, Clone, Deserialize)]
struct TrialAckPayload {
    run_id: String,
    status: String,
    error: Option<String>,
    config_count: Option<usize>,
    drained_trades: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerErrorKind {
    BadRequest,
    Conflict,
    Internal,
}

#[derive(Debug, Clone)]
pub struct RunnerError {
    pub kind: RunnerErrorKind,
    pub message: String,
}

impl RunnerError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            kind: RunnerErrorKind::BadRequest,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: RunnerErrorKind::Conflict,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: RunnerErrorKind::Internal,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunnerJobState {
    Running,
    Success,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerLogEntry {
    pub ts_ms: i64,
    pub stream: String,
    pub line: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerJobView {
    pub job_id: u64,
    pub phase: String,
    pub command: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    pub state: RunnerJobState,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunnerStatusResponse {
    pub running: bool,
    pub active_job: Option<RunnerJobView>,
    pub recent_jobs: Vec<RunnerJobView>,
    pub logs: Vec<RunnerLogEntry>,
}

#[derive(Debug, Clone)]
struct RunnerJob {
    job_id: u64,
    phase: String,
    command: String,
    started_at_ms: i64,
    finished_at_ms: Option<i64>,
    state: RunnerJobState,
    exit_code: Option<i32>,
    error: Option<String>,
}

impl From<RunnerJob> for RunnerJobView {
    fn from(value: RunnerJob) -> Self {
        Self {
            job_id: value.job_id,
            phase: value.phase,
            command: value.command,
            started_at_ms: value.started_at_ms,
            finished_at_ms: value.finished_at_ms,
            state: value.state,
            exit_code: value.exit_code,
            error: value.error,
        }
    }
}

#[derive(Debug)]
struct RunnerInner {
    next_job_id: u64,
    active_job: Option<RunnerJob>,
    history: VecDeque<RunnerJob>,
    logs: VecDeque<RunnerLogEntry>,
    stop_tx: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Clone)]
pub struct TrialRunnerManager {
    inner: Arc<Mutex<RunnerInner>>,
    workdir: PathBuf,
    max_logs: usize,
    max_history: usize,
}

impl TrialRunnerManager {
    pub fn new(workdir: PathBuf) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RunnerInner {
                next_job_id: 1,
                active_job: None,
                history: VecDeque::new(),
                logs: VecDeque::new(),
                stop_tx: None,
            })),
            workdir,
            max_logs: 500,
            max_history: 20,
        }
    }

    pub fn ui_config() -> RunnerUiConfig {
        runner_ui_config()
    }

    pub async fn start(&self, req: RunnerStartRequest) -> Result<RunnerStartResponse, RunnerError> {
        let phase = req.phase.trim().to_lowercase();
        let cmd = build_trial_runner_command(&req).map_err(RunnerError::bad_request)?;
        let forward_opts = if phase == "forward" {
            Some(forward_phase_options(&req)?)
        } else {
            None
        };
        if !self.workdir.join("ray_driver").exists() {
            return Err(RunnerError::internal(format!(
                "ray_driver directory not found in {}",
                self.workdir.display()
            )));
        }

        {
            let inner = self.inner.lock().await;
            if inner
                .active_job
                .as_ref()
                .is_some_and(|job| job.state == RunnerJobState::Running)
            {
                return Err(RunnerError::conflict("runner job already active"));
            }
        }

        validate_phase_prerequisites(&self.workdir, &phase)?;

        let mut inner = self.inner.lock().await;
        if inner
            .active_job
            .as_ref()
            .is_some_and(|job| job.state == RunnerJobState::Running)
        {
            return Err(RunnerError::conflict("runner job already active"));
        }

        let job_id = inner.next_job_id;
        inner.next_job_id = inner.next_job_id.saturating_add(1);
        let started_at_ms = crate::domain::screener::utils::now_ms();
        let command = if let Some(opts) = forward_opts {
            build_forward_display_command(&opts)
        } else {
            format!("{} {}", cmd.program, cmd.args.join(" "))
        };

        let job = RunnerJob {
            job_id,
            phase: phase.clone(),
            command: command.clone(),
            started_at_ms,
            finished_at_ms: None,
            state: RunnerJobState::Running,
            exit_code: None,
            error: None,
        };
        inner.active_job = Some(job);
        inner.logs.clear();
        inner.logs.push_back(RunnerLogEntry {
            ts_ms: started_at_ms,
            stream: "sys".to_string(),
            line: format!("starting: {}", command),
        });

        let (stop_tx, stop_rx) = oneshot::channel();
        inner.stop_tx = Some(stop_tx);
        drop(inner);

        if let Some(opts) = forward_opts {
            self.spawn_forward_job(job_id, opts, stop_rx);
        } else {
            self.spawn_job(job_id, cmd, stop_rx);
        }

        Ok(RunnerStartResponse {
            job_id,
            phase,
            command,
            started_at_ms,
        })
    }

    pub async fn stop(&self) -> Result<RunnerStopResponse, RunnerError> {
        let mut inner = self.inner.lock().await;
        if !inner
            .active_job
            .as_ref()
            .is_some_and(|job| job.state == RunnerJobState::Running)
        {
            return Ok(RunnerStopResponse {
                stop_requested: false,
            });
        }

        let requested = if let Some(tx) = inner.stop_tx.take() {
            tx.send(()).is_ok()
        } else {
            false
        };
        Ok(RunnerStopResponse {
            stop_requested: requested,
        })
    }

    pub async fn status(&self, tail: usize) -> RunnerStatusResponse {
        let inner = self.inner.lock().await;
        runner_status_from_inner(&inner, tail)
    }

    fn spawn_job(&self, job_id: u64, cmd: RunnerCommandSpec, mut stop_rx: oneshot::Receiver<()>) {
        let manager = self.clone();
        tokio::spawn(async move {
            manager
                .append_log(
                    job_id,
                    "sys",
                    format!("workdir: {}", manager.workdir.display()),
                )
                .await;

            let mut child = match Command::new(&cmd.program)
                .args(&cmd.args)
                .current_dir(&manager.workdir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(e) => {
                    manager
                        .finish_job(
                            job_id,
                            RunnerJobState::Failed,
                            None,
                            Some(format!("spawn failed: {e}")),
                        )
                        .await;
                    return;
                }
            };

            let stdout_task = child.stdout.take().map(|stdout| {
                let manager = manager.clone();
                tokio::spawn(async move {
                    manager.read_stream(job_id, "stdout", stdout).await;
                })
            });
            let stderr_task = child.stderr.take().map(|stderr| {
                let manager = manager.clone();
                tokio::spawn(async move {
                    manager.read_stream(job_id, "stderr", stderr).await;
                })
            });

            let mut stopped = false;
            let wait_result = tokio::select! {
                status = child.wait() => status,
                _ = &mut stop_rx => {
                    stopped = true;
                    manager.append_log(job_id, "sys", "stop requested".to_string()).await;
                    let _ = child.kill().await;
                    child.wait().await
                }
            };

            if let Some(task) = stdout_task {
                let _ = task.await;
            }
            if let Some(task) = stderr_task {
                let _ = task.await;
            }

            match wait_result {
                Ok(status) => {
                    let exit_code = status.code();
                    if stopped {
                        manager
                            .finish_job(job_id, RunnerJobState::Stopped, exit_code, None)
                            .await;
                    } else if status.success() {
                        manager
                            .finish_job(job_id, RunnerJobState::Success, exit_code, None)
                            .await;
                    } else {
                        manager
                            .finish_job(
                                job_id,
                                RunnerJobState::Failed,
                                exit_code,
                                Some(format!("process exited with status {status}")),
                            )
                            .await;
                    }
                }
                Err(e) => {
                    manager
                        .finish_job(
                            job_id,
                            RunnerJobState::Failed,
                            None,
                            Some(format!("wait failed: {e}")),
                        )
                        .await;
                }
            }
        });
    }

    fn spawn_forward_job(
        &self,
        job_id: u64,
        opts: ForwardPhaseOptions,
        mut stop_rx: oneshot::Receiver<()>,
    ) {
        let manager = self.clone();
        tokio::spawn(async move {
            manager
                .append_log(
                    job_id,
                    "sys",
                    format!(
                        "forward internal mode: budget={}s grace={}s report={}s refs={} max_configs={}",
                        opts.max_budget_s,
                        opts.grace_period_s,
                        opts.report_interval_s,
                        opts.max_refs,
                        opts.max_configs
                    ),
                )
                .await;

            let refs_path = manager.workdir.join("data/scout-references.json");
            let db_path = manager.workdir.join("data/optimizer.db");
            let config_dir = manager.workdir.join("config");
            let run_id = generate_forward_run_id();
            let submission_id = format!("{}-{}", run_id, monotonic_ns());

            let refs = match load_scout_references(&refs_path) {
                Ok(rows) => rows,
                Err(e) => {
                    manager
                        .finish_job(job_id, RunnerJobState::Failed, None, Some(e.message))
                        .await;
                    return;
                }
            };
            let mut selected_ids = select_reference_ids(&refs, opts.max_refs);
            if selected_ids.is_empty() {
                manager
                    .finish_job(
                        job_id,
                        RunnerJobState::Failed,
                        None,
                        Some("forward internal: no valid scout references selected".to_string()),
                    )
                    .await;
                return;
            }
            manager
                .append_log(
                    job_id,
                    "sys",
                    format!(
                        "forward internal: selected_refs={} from_scout_rows={}",
                        selected_ids.len(),
                        refs.len()
                    ),
                )
                .await;

            let mut configs =
                load_configs_for_reference_ids(&db_path, &selected_ids, opts.max_configs);
            if configs.is_err() && selected_ids.len() < refs.len() {
                let fallback_ids = select_reference_ids(&refs, refs.len() as u64);
                if fallback_ids.len() > selected_ids.len() {
                    manager
                        .append_log(
                            job_id,
                            "sys",
                            format!(
                                "forward internal: top refs missing in db; retrying with all valid refs ({})",
                                fallback_ids.len()
                            ),
                        )
                        .await;
                    selected_ids = fallback_ids;
                    configs =
                        load_configs_for_reference_ids(&db_path, &selected_ids, opts.max_configs);
                }
            }
            let configs = match configs {
                Ok(configs) => configs,
                Err(e) => {
                    manager
                        .finish_job(job_id, RunnerJobState::Failed, None, Some(e.message))
                        .await;
                    return;
                }
            };
            manager
                .append_log(
                    job_id,
                    "sys",
                    format!("forward internal: resolved configs={}", configs.len()),
                )
                .await;

            let ack_path = config_dir
                .join("trial-acks")
                .join(format!("{submission_id}.json"));
            if let Err(e) = enqueue_trial_batch(&config_dir, &run_id, &submission_id, &configs) {
                manager
                    .finish_job(job_id, RunnerJobState::Failed, None, Some(e.message))
                    .await;
                return;
            }
            manager
                .append_log(
                    job_id,
                    "sys",
                    format!("forward internal: queued submission_id={submission_id}"),
                )
                .await;

            let ack_timeout = Duration::from_secs(30);
            let ack_deadline = Instant::now() + ack_timeout;
            let ack = loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        if let Err(e) = write_trial_control_clear_run(&config_dir, &run_id) {
                            manager.append_log(job_id, "stderr", format!("forward internal: stop clear run failed: {}", e.message)).await;
                        }
                        manager.finish_job(job_id, RunnerJobState::Stopped, None, None).await;
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                }

                if Instant::now() >= ack_deadline {
                    manager
                        .finish_job(
                            job_id,
                            RunnerJobState::Failed,
                            None,
                            Some(format!(
                                "forward internal: no trial ack for run_id={} submission_id={} within {}s",
                                run_id,
                                submission_id,
                                ack_timeout.as_secs()
                            )),
                        )
                        .await;
                    return;
                }

                match try_read_trial_ack(&ack_path, &run_id) {
                    Ok(Some(ack)) => break ack,
                    Ok(None) => {}
                    Err(e) => {
                        manager
                            .finish_job(job_id, RunnerJobState::Failed, None, Some(e.message))
                            .await;
                        return;
                    }
                }
            };

            manager
                .append_log(
                    job_id,
                    "sys",
                    format!(
                        "forward internal: ack ok configs={} drained_trades={}",
                        ack.config_count.unwrap_or(0),
                        ack.drained_trades.unwrap_or(0)
                    ),
                )
                .await;

            let run_budget = Duration::from_secs(forward_run_budget_s(&opts));
            let report_interval = Duration::from_secs(opts.report_interval_s.max(1));
            let start_ts = Instant::now();
            let mut last_report = Instant::now();

            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        if let Err(e) = write_trial_control_clear_run(&config_dir, &run_id) {
                            manager.append_log(job_id, "stderr", format!("forward internal: stop clear run failed: {}", e.message)).await;
                        }
                        manager.finish_job(job_id, RunnerJobState::Stopped, None, None).await;
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }

                let elapsed = start_ts.elapsed();
                if elapsed >= run_budget {
                    break;
                }
                if last_report.elapsed() >= report_interval {
                    last_report = Instant::now();
                    if let Ok(total_trades) = query_total_trades_for_run(&db_path, &run_id) {
                        manager
                            .append_log(
                                job_id,
                                "stdout",
                                format!(
                                    "forward internal progress: run_id={} elapsed={}s trades={}",
                                    run_id,
                                    elapsed.as_secs(),
                                    total_trades
                                ),
                            )
                            .await;
                    }
                }
            }

            if let Err(e) = write_trial_control_clear_run(&config_dir, &run_id) {
                manager
                    .finish_job(job_id, RunnerJobState::Failed, None, Some(e.message))
                    .await;
                return;
            }
            manager
                .append_log(
                    job_id,
                    "sys",
                    format!("forward internal: run cleared run_id={run_id}"),
                )
                .await;
            manager
                .finish_job(job_id, RunnerJobState::Success, Some(0), None)
                .await;
        });
    }

    async fn read_stream<R>(&self, job_id: u64, stream: &'static str, reader: R)
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            self.append_log(job_id, stream, line).await;
        }
    }

    async fn append_log(&self, job_id: u64, stream: impl Into<String>, line: impl Into<String>) {
        let mut inner = self.inner.lock().await;
        if inner.active_job.as_ref().map(|job| job.job_id) != Some(job_id) {
            return;
        }
        inner.logs.push_back(RunnerLogEntry {
            ts_ms: crate::domain::screener::utils::now_ms(),
            stream: stream.into(),
            line: line.into(),
        });
        while inner.logs.len() > self.max_logs {
            inner.logs.pop_front();
        }
    }

    async fn finish_job(
        &self,
        job_id: u64,
        state: RunnerJobState,
        exit_code: Option<i32>,
        error: Option<String>,
    ) {
        let mut inner = self.inner.lock().await;
        let Some(active) = inner.active_job.as_mut() else {
            return;
        };
        if active.job_id != job_id {
            return;
        }

        active.state = state;
        active.exit_code = exit_code;
        active.finished_at_ms = Some(crate::domain::screener::utils::now_ms());
        active.error = error.clone();
        let summary = match (&active.state, active.exit_code, error.as_deref()) {
            (RunnerJobState::Success, Some(code), _) => format!("completed: exit_code={code}"),
            (RunnerJobState::Stopped, Some(code), _) => format!("stopped: exit_code={code}"),
            (RunnerJobState::Failed, Some(code), Some(err)) => {
                format!("failed: exit_code={code} error={err}")
            }
            (RunnerJobState::Failed, _, Some(err)) => format!("failed: {err}"),
            _ => "job finished".to_string(),
        };
        inner.stop_tx = None;
        inner.logs.push_back(RunnerLogEntry {
            ts_ms: crate::domain::screener::utils::now_ms(),
            stream: "sys".to_string(),
            line: summary,
        });
        if let Some(done) = inner.active_job.clone() {
            if done.state != RunnerJobState::Running {
                inner.history.push_back(done);
                while inner.history.len() > self.max_history {
                    inner.history.pop_front();
                }
            }
        }
        while inner.logs.len() > self.max_logs {
            inner.logs.pop_front();
        }
    }
}

fn monotonic_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn generate_forward_run_id() -> String {
    format!("forward-{}-{:x}", monotonic_ns(), std::process::id())
}

fn build_forward_display_command(opts: &ForwardPhaseOptions) -> String {
    format!(
        "internal-forward --max-budget {} --grace-period {} --report-interval {} --max-refs {} --max-configs {}",
        opts.max_budget_s,
        opts.grace_period_s,
        opts.report_interval_s,
        opts.max_refs,
        opts.max_configs
    )
}

fn forward_phase_options(req: &RunnerStartRequest) -> Result<ForwardPhaseOptions, RunnerError> {
    let max_budget_s = req.max_budget.unwrap_or(DEFAULT_FORWARD_MAX_BUDGET_S);
    let grace_period_s = req.grace_period.unwrap_or(DEFAULT_FORWARD_GRACE_PERIOD_S);
    let report_interval_s = req
        .report_interval
        .unwrap_or(DEFAULT_FORWARD_REPORT_INTERVAL_S);
    let max_refs = req.max_refs.unwrap_or(DEFAULT_FORWARD_MAX_REFS);
    let max_configs = req.max_configs.unwrap_or(DEFAULT_FORWARD_MAX_CONFIGS);

    if max_budget_s == 0 {
        return Err(RunnerError::bad_request("max_budget must be >= 1"));
    }
    if grace_period_s == 0 {
        return Err(RunnerError::bad_request("grace_period must be >= 1"));
    }
    if report_interval_s == 0 {
        return Err(RunnerError::bad_request("report_interval must be >= 1"));
    }

    Ok(ForwardPhaseOptions {
        max_budget_s,
        grace_period_s,
        report_interval_s,
        max_refs: max_refs.clamp(1, FORWARD_MAX_REFS_HARD_CAP),
        max_configs: max_configs.clamp(1, FORWARD_MAX_CONFIGS_HARD_CAP),
    })
}

fn load_scout_references(path: &Path) -> Result<Vec<ScoutReferenceRow>, RunnerError> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        RunnerError::bad_request(format!(
            "forward requires readable {} (error: {e})",
            path.display()
        ))
    })?;
    serde_json::from_str::<Vec<ScoutReferenceRow>>(&raw).map_err(|e| {
        RunnerError::bad_request(format!(
            "forward requires valid JSON list in {} (error: {e})",
            path.display()
        ))
    })
}

fn select_reference_ids(rows: &[ScoutReferenceRow], max_refs: u64) -> Vec<u64> {
    let mut selected: Vec<ScoutReferenceRow> = rows
        .iter()
        .filter(|row| row.config_id != 0 && row.trades > 0 && row.avg_pnl_pct.is_finite())
        .cloned()
        .collect();
    selected.sort_by(|a, b| {
        b.avg_pnl_pct
            .partial_cmp(&a.avg_pnl_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.trades.cmp(&a.trades))
            .then_with(|| a.config_id.cmp(&b.config_id))
    });
    selected
        .into_iter()
        .take(max_refs as usize)
        .map(|row| row.config_id as u64)
        .collect()
}

fn load_configs_for_reference_ids(
    db_path: &Path,
    ids: &[u64],
    max_configs: u64,
) -> Result<Vec<TraderConfig>, RunnerError> {
    let conn = crate::infrastructure::db::open_db_readonly(db_path)
        .map_err(|e| RunnerError::internal(format!("open db {}: {e}", db_path.display())))?;

    let mut stmt = conn
        .prepare(
            "SELECT spike_threshold_bps, target_ratio, stop_loss_bps, max_hold_ms,
                    max_spread_bps, trailing_decay_ratio, baseline_window_ms,
                    fill_delay_ms, cooldown_ms, warmup_ms, quote_freshness_ms,
                    taker_fee, min_baseline_samples
             FROM configs WHERE id = ?1",
        )
        .map_err(|e| RunnerError::internal(format!("prepare config lookup failed: {e}")))?;

    let mut result = Vec::new();
    let mut seen = HashSet::new();
    for config_id in ids.iter().copied() {
        if result.len() >= max_configs as usize {
            break;
        }
        if !seen.insert(config_id) {
            continue;
        }
        let cfg = stmt
            .query_row(rusqlite::params![config_id as i64], |row| {
                Ok(TraderConfig {
                    spike_threshold_bps: row.get(0)?,
                    target_ratio: row.get(1)?,
                    stop_loss_bps: row.get(2)?,
                    max_hold_ms: row.get(3)?,
                    max_spread_bps: row.get(4)?,
                    trailing_decay_ratio: row.get(5)?,
                    baseline_window_ms: row.get(6)?,
                    fill_delay_ms: row.get(7)?,
                    cooldown_ms: row.get(8)?,
                    warmup_ms: row.get(9)?,
                    quote_freshness_ms: row.get(10)?,
                    taker_fee: row.get(11)?,
                    min_baseline_samples: row.get::<_, i64>(12)? as usize,
                })
            })
            .optional()
            .map_err(|e| RunnerError::internal(format!("load config_id={config_id}: {e}")))?;
        if let Some(cfg) = cfg {
            result.push(cfg);
        }
    }
    if result.is_empty() {
        return Err(RunnerError::bad_request(format!(
            "forward requires at least one config_id from scout refs present in {}",
            db_path.display()
        )));
    }
    Ok(result)
}

fn enqueue_trial_batch(
    config_dir: &Path,
    run_id: &str,
    submission_id: &str,
    configs: &[TraderConfig],
) -> Result<(), RunnerError> {
    let queue_dir = config_dir.join("trial-batches");
    std::fs::create_dir_all(&queue_dir).map_err(|e| {
        RunnerError::internal(format!("create queue dir {}: {e}", queue_dir.display()))
    })?;

    let batch = TrialBatchPayload {
        run_id,
        configs,
        config_id_contract_version: CONFIG_ID_CONTRACT_VERSION,
        submission_id,
    };
    let payload = serde_json::to_vec_pretty(&batch)
        .map_err(|e| RunnerError::internal(format!("serialize forward trial batch failed: {e}")))?;
    let batch_path = queue_dir.join(format!("{submission_id}.json"));
    let tmp = batch_path.with_extension("tmp");
    std::fs::write(&tmp, payload)
        .map_err(|e| RunnerError::internal(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, &batch_path).map_err(|e| {
        RunnerError::internal(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            batch_path.display()
        ))
    })?;
    Ok(())
}

fn forward_run_budget_s(opts: &ForwardPhaseOptions) -> u64 {
    opts.max_budget_s
}

fn try_read_trial_ack(
    ack_path: &Path,
    expected_run_id: &str,
) -> Result<Option<TrialAckPayload>, RunnerError> {
    let raw = match std::fs::read_to_string(ack_path) {
        Ok(raw) => raw,
        Err(_) => return Ok(None),
    };
    let ack: TrialAckPayload = match serde_json::from_str(&raw) {
        Ok(ack) => ack,
        Err(_) => return Ok(None),
    };
    let _ = std::fs::remove_file(ack_path);

    if ack.run_id != expected_run_id {
        return Err(RunnerError::internal(format!(
            "ack run_id mismatch: expected {}, got {}",
            expected_run_id, ack.run_id
        )));
    }
    if ack.status != "ok" {
        return Err(RunnerError::bad_request(format!(
            "forward batch rejected: {}",
            ack.error.unwrap_or_else(|| "unknown error".to_string())
        )));
    }
    Ok(Some(ack))
}

fn query_total_trades_for_run(db_path: &Path, run_id: &str) -> Result<u64, RunnerError> {
    let conn = crate::infrastructure::db::open_db_readonly(db_path)
        .map_err(|e| RunnerError::internal(format!("open db {}: {e}", db_path.display())))?;
    let total: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM trades WHERE run_id = ?1",
            rusqlite::params![run_id],
            |row| row.get(0),
        )
        .map_err(|e| RunnerError::internal(format!("count trades for run_id={run_id}: {e}")))?;
    Ok(total.max(0) as u64)
}

fn write_trial_control_clear_run(config_dir: &Path, run_id: &str) -> Result<(), RunnerError> {
    std::fs::create_dir_all(config_dir).map_err(|e| {
        RunnerError::internal(format!("ensure config dir {}: {e}", config_dir.display()))
    })?;
    let control_path = config_dir.join("trial-control.json");
    let tmp = control_path.with_extension("tmp");
    let payload = TrialControlPayload {
        clear_run_id: true,
        run_id,
    };
    let raw = serde_json::to_vec_pretty(&payload)
        .map_err(|e| RunnerError::internal(format!("serialize trial control failed: {e}")))?;
    std::fs::write(&tmp, raw)
        .map_err(|e| RunnerError::internal(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, &control_path).map_err(|e| {
        RunnerError::internal(format!(
            "rename {} -> {}: {e}",
            tmp.display(),
            control_path.display()
        ))
    })?;
    Ok(())
}

fn validate_phase_prerequisites(workdir: &Path, phase: &str) -> Result<(), RunnerError> {
    if phase != "forward" {
        return Ok(());
    }

    let refs_path = workdir.join("data/scout-references.json");
    if !refs_path.exists() {
        return Err(RunnerError::bad_request(format!(
            "forward requires {} (run scout first)",
            refs_path.display()
        )));
    }

    let rows = load_scout_references(&refs_path)?;
    let valid_rows = rows
        .iter()
        .filter(|row| row.config_id != 0 && row.trades > 0 && row.avg_pnl_pct.is_finite())
        .count();

    if valid_rows == 0 {
        return Err(RunnerError::bad_request(format!(
            "forward requires valid reference rows in {} (run scout and get references)",
            refs_path.display()
        )));
    }

    Ok(())
}

fn runner_status_from_inner(inner: &RunnerInner, tail: usize) -> RunnerStatusResponse {
    let active_job = inner.active_job.clone().map(RunnerJobView::from);

    let mut logs: Vec<RunnerLogEntry> = inner.logs.iter().cloned().collect();
    let limit = if tail == 0 { 200 } else { tail };
    if logs.len() > limit {
        logs = logs.split_off(logs.len() - limit);
    }

    let running = active_job
        .as_ref()
        .is_some_and(|job| job.state == RunnerJobState::Running);

    let recent_jobs: Vec<RunnerJobView> = inner
        .history
        .iter()
        .rev()
        .cloned()
        .map(RunnerJobView::from)
        .collect();

    RunnerStatusResponse {
        running,
        active_job,
        recent_jobs,
        logs,
    }
}

pub fn resolve_runner_workdir() -> PathBuf {
    command::resolve_runner_workdir()
}

#[cfg(test)]
fn find_workdir_from_candidates(candidates: &[PathBuf]) -> Option<PathBuf> {
    command::find_workdir_from_candidates(candidates)
}

pub fn build_trial_runner_command(req: &RunnerStartRequest) -> Result<RunnerCommandSpec, String> {
    command::build_trial_runner_command(req)
}

fn runner_ui_config() -> RunnerUiConfig {
    command::runner_ui_config()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn build_scout_command_with_default_duration() {
        let req = RunnerStartRequest {
            phase: "scout".to_string(),
            duration: None,
            cycles: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: None,
            max_configs: None,
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let cmd = build_trial_runner_command(&req).expect("command");
        assert_eq!(cmd.program, "python3");
        assert_eq!(
            cmd.args,
            vec![
                "-m".to_string(),
                "ray_driver".to_string(),
                "scout".to_string(),
                "--duration".to_string(),
                "900".to_string(),
                "--cycles".to_string(),
                "1".to_string(),
            ]
        );
    }

    #[test]
    fn build_scout_command_with_custom_cycles() {
        let req = RunnerStartRequest {
            phase: "scout".to_string(),
            duration: Some(60),
            cycles: Some(303),
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: None,
            max_configs: None,
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let cmd = build_trial_runner_command(&req).expect("command");
        assert_eq!(
            cmd.args,
            vec![
                "-m".to_string(),
                "ray_driver".to_string(),
                "scout".to_string(),
                "--duration".to_string(),
                "60".to_string(),
                "--cycles".to_string(),
                "303".to_string(),
            ]
        );
    }

    #[test]
    fn build_forward_command_with_defaults() {
        let req = RunnerStartRequest {
            phase: "forward".to_string(),
            duration: None,
            cycles: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: None,
            max_configs: None,
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let cmd = build_trial_runner_command(&req).expect("command");
        assert_eq!(
            cmd.args,
            vec![
                "-m".to_string(),
                "ray_driver".to_string(),
                "forward".to_string(),
                "--max-budget".to_string(),
                DEFAULT_FORWARD_MAX_BUDGET_S.to_string(),
                "--grace-period".to_string(),
                DEFAULT_FORWARD_GRACE_PERIOD_S.to_string(),
                "--report-interval".to_string(),
                DEFAULT_FORWARD_REPORT_INTERVAL_S.to_string(),
                "--max-refs".to_string(),
                DEFAULT_FORWARD_MAX_REFS.to_string(),
                "--max-configs".to_string(),
                DEFAULT_FORWARD_MAX_CONFIGS.to_string(),
            ]
        );
    }

    #[test]
    fn build_forward_command_clamps_hard_caps() {
        let req = RunnerStartRequest {
            phase: "forward".to_string(),
            duration: None,
            cycles: None,
            max_budget: Some(720),
            grace_period: Some(120),
            report_interval: Some(15),
            max_refs: Some(FORWARD_MAX_REFS_HARD_CAP + 50),
            max_configs: Some(FORWARD_MAX_CONFIGS_HARD_CAP + 100),
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let cmd = build_trial_runner_command(&req).expect("command");
        assert_eq!(
            cmd.args,
            vec![
                "-m".to_string(),
                "ray_driver".to_string(),
                "forward".to_string(),
                "--max-budget".to_string(),
                "720".to_string(),
                "--grace-period".to_string(),
                "120".to_string(),
                "--report-interval".to_string(),
                "15".to_string(),
                "--max-refs".to_string(),
                FORWARD_MAX_REFS_HARD_CAP.to_string(),
                "--max-configs".to_string(),
                FORWARD_MAX_CONFIGS_HARD_CAP.to_string(),
            ]
        );
    }

    #[test]
    fn non_scout_phase_is_rejected_for_expand() {
        let req = RunnerStartRequest {
            phase: "expand".to_string(),
            duration: Some(300),
            cycles: Some(7),
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: None,
            max_configs: None,
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let err = build_trial_runner_command(&req).expect_err("must reject");
        assert!(err.contains("Unsupported phase: expand"));
    }

    #[test]
    fn non_scout_phase_is_rejected_for_promote() {
        let req = RunnerStartRequest {
            phase: "promote".to_string(),
            duration: None,
            cycles: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: None,
            max_configs: None,
            run_id: Some("forward-1".to_string()),
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let err = build_trial_runner_command(&req).expect_err("must reject");
        assert!(err.contains("Unsupported phase: promote"));
    }

    #[test]
    fn unknown_phase_rejected() {
        let req = RunnerStartRequest {
            phase: "hack".to_string(),
            duration: None,
            cycles: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: None,
            max_configs: None,
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let err = build_trial_runner_command(&req).expect_err("must fail");
        assert!(err.contains("Unsupported phase"));
    }

    #[test]
    fn ui_config_matches_command_defaults() {
        let cfg = runner_ui_config();

        assert_eq!(cfg.phases.len(), 2);
        let scout = cfg.phases.first().expect("scout phase");
        let forward = cfg.phases.get(1).expect("forward phase");

        assert_eq!(scout.duration, Some(DEFAULT_SCOUT_DURATION_S));
        assert_eq!(scout.cycles, Some(DEFAULT_SCOUT_CYCLES));
        assert_eq!(scout.name, "scout");
        assert_eq!(forward.name, "forward");
        assert_eq!(forward.max_budget, Some(DEFAULT_FORWARD_MAX_BUDGET_S));
        assert_eq!(forward.grace_period, Some(DEFAULT_FORWARD_GRACE_PERIOD_S));
        assert_eq!(
            forward.report_interval,
            Some(DEFAULT_FORWARD_REPORT_INTERVAL_S)
        );
        assert_eq!(forward.max_refs, Some(DEFAULT_FORWARD_MAX_REFS));
        assert_eq!(forward.max_configs, Some(DEFAULT_FORWARD_MAX_CONFIGS));
    }

    #[test]
    fn find_workdir_from_candidates_prefers_existing_ray_driver() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_test_{ts}"));
        let c1 = base.join("a");
        let c2 = base.join("b");
        fs::create_dir_all(&c1).expect("mkdir c1");
        fs::create_dir_all(c2.join("ray_driver")).expect("mkdir c2 ray");

        let found = find_workdir_from_candidates(&[c1.clone(), c2.clone()]);
        assert_eq!(found, Some(c2.clone()));

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn forward_phase_requires_scout_references_file() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_refs_missing_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");

        let err = validate_phase_prerequisites(&base, "forward").expect_err("must fail");
        assert!(err.message.contains("run scout first"));

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn forward_phase_accepts_existing_scout_references_file() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_refs_ok_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        fs::create_dir_all(base.join("data")).expect("mkdir data");
        fs::write(
            base.join("data/scout-references.json"),
            r#"[{"config_id":1,"trades":10,"avg_pnl_pct":0.1}]"#,
        )
        .expect("write refs");

        validate_phase_prerequisites(&base, "forward").expect("must pass");

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn forward_phase_rejects_empty_scout_references_file() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_refs_empty_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        fs::create_dir_all(base.join("data")).expect("mkdir data");
        fs::write(base.join("data/scout-references.json"), "[]").expect("write refs");

        let err = validate_phase_prerequisites(&base, "forward").expect_err("must fail");
        assert!(err.message.contains("valid reference rows"));

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn forward_phase_rejects_invalid_reference_rows() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_refs_invalid_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        fs::create_dir_all(base.join("data")).expect("mkdir data");
        fs::write(base.join("data/scout-references.json"), r#"[{}]"#).expect("write refs");

        let err = validate_phase_prerequisites(&base, "forward").expect_err("must fail");
        assert!(err.message.contains("valid JSON list"));

        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn manager_rejects_forward_start_without_scout_references() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_mgr_refs_missing_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        let manager = TrialRunnerManager::new(base.clone());

        let err = manager
            .start(RunnerStartRequest {
                phase: "forward".to_string(),
                duration: None,
                cycles: None,
                max_budget: None,
                grace_period: None,
                report_interval: None,
                max_refs: None,
                max_configs: None,
                run_id: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            })
            .await
            .expect_err("must reject");

        assert_eq!(err.kind, RunnerErrorKind::BadRequest);
        assert!(err.message.contains("run scout first"));

        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn manager_reports_conflict_before_forward_prerequisites() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_mgr_conflict_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        let manager = TrialRunnerManager::new(base.clone());

        {
            let mut inner = manager.inner.lock().await;
            inner.active_job = Some(RunnerJob {
                job_id: 7,
                phase: "scout".to_string(),
                command: "python3 -m ray_driver scout".to_string(),
                started_at_ms: 1,
                finished_at_ms: None,
                state: RunnerJobState::Running,
                exit_code: None,
                error: None,
            });
        }

        let err = manager
            .start(RunnerStartRequest {
                phase: "forward".to_string(),
                duration: None,
                cycles: None,
                max_budget: None,
                grace_period: None,
                report_interval: None,
                max_refs: None,
                max_configs: None,
                run_id: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            })
            .await
            .expect_err("must reject");

        assert_eq!(err.kind, RunnerErrorKind::Conflict);
        assert!(err.message.contains("already active"));

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn runner_status_includes_recent_jobs() {
        let mut history = VecDeque::new();
        history.push_back(RunnerJob {
            job_id: 1,
            phase: "scout".to_string(),
            command: "python3 -m ray_driver scout".to_string(),
            started_at_ms: 100,
            finished_at_ms: Some(200),
            state: RunnerJobState::Success,
            exit_code: Some(0),
            error: None,
        });
        history.push_back(RunnerJob {
            job_id: 2,
            phase: "expand".to_string(),
            command: "python3 -m ray_driver expand".to_string(),
            started_at_ms: 300,
            finished_at_ms: Some(350),
            state: RunnerJobState::Failed,
            exit_code: Some(1),
            error: Some("bad".to_string()),
        });

        let inner = RunnerInner {
            next_job_id: 3,
            active_job: None,
            history,
            logs: VecDeque::new(),
            stop_tx: None,
        };

        let status = runner_status_from_inner(&inner, 50);
        assert!(!status.running);
        assert_eq!(status.recent_jobs.len(), 2);
        assert_eq!(status.recent_jobs[0].job_id, 2);
        assert_eq!(status.recent_jobs[1].job_id, 1);
    }

    #[test]
    fn forward_phase_options_apply_defaults_and_caps() {
        let req = RunnerStartRequest {
            phase: "forward".to_string(),
            duration: None,
            cycles: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
            max_refs: Some(FORWARD_MAX_REFS_HARD_CAP + 1000),
            max_configs: Some(FORWARD_MAX_CONFIGS_HARD_CAP + 1000),
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let opts = forward_phase_options(&req).expect("options");
        assert_eq!(opts.max_budget_s, DEFAULT_FORWARD_MAX_BUDGET_S);
        assert_eq!(opts.grace_period_s, DEFAULT_FORWARD_GRACE_PERIOD_S);
        assert_eq!(opts.report_interval_s, DEFAULT_FORWARD_REPORT_INTERVAL_S);
        assert_eq!(opts.max_refs, FORWARD_MAX_REFS_HARD_CAP);
        assert_eq!(opts.max_configs, FORWARD_MAX_CONFIGS_HARD_CAP);
    }

    #[test]
    fn select_reference_ids_orders_by_avg_then_trades_and_filters_invalid() {
        let rows = vec![
            ScoutReferenceRow {
                config_id: 1,
                trades: 10,
                avg_pnl_pct: 0.1,
            },
            ScoutReferenceRow {
                config_id: 2,
                trades: 50,
                avg_pnl_pct: 0.1,
            },
            ScoutReferenceRow {
                config_id: 3,
                trades: 3,
                avg_pnl_pct: 0.4,
            },
            ScoutReferenceRow {
                config_id: 0,
                trades: 10,
                avg_pnl_pct: 0.9,
            },
            ScoutReferenceRow {
                config_id: 4,
                trades: 0,
                avg_pnl_pct: 0.9,
            },
        ];

        let ids = select_reference_ids(&rows, 3);
        assert_eq!(ids, vec![3, 2, 1]);
    }

    #[test]
    fn load_scout_references_accepts_signed_config_ids() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_scout_refs_legacy_{ts}"));
        fs::create_dir_all(&base).expect("mkdir base");
        let refs_path = base.join("scout-references.json");
        fs::write(
            &refs_path,
            r#"
            [
              {"config_id": -8141442055384427616, "trades": 12, "avg_pnl_pct": 0.8},
              {"config_id": 5, "trades": 20, "avg_pnl_pct": 0.5}
            ]
            "#,
        )
        .expect("write refs");

        let rows = load_scout_references(&refs_path).expect("must parse legacy refs");
        let ids = select_reference_ids(&rows, 10);
        assert_eq!(
            ids,
            vec![(-8141442055384427616_i64) as u64, 5_u64],
            "signed config_ids must remain eligible (except zero)"
        );

        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn forward_display_command_is_internal_not_python() {
        let opts = ForwardPhaseOptions {
            max_budget_s: 120,
            grace_period_s: 30,
            report_interval_s: 5,
            max_refs: 16,
            max_configs: 200,
        };
        let cmd = build_forward_display_command(&opts);
        assert!(cmd.starts_with("internal-forward"));
        assert!(!cmd.contains("python3 -m ray_driver"));
    }

    #[test]
    fn forward_run_budget_must_not_exceed_max_budget() {
        let opts = ForwardPhaseOptions {
            max_budget_s: 120,
            grace_period_s: 600,
            report_interval_s: 5,
            max_refs: 16,
            max_configs: 200,
        };

        let run_budget = Duration::from_secs(forward_run_budget_s(&opts));
        assert_eq!(run_budget.as_secs(), 120);
    }

    #[test]
    fn try_read_trial_ack_tolerates_invalid_json_until_ack_is_ready() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_ack_invalid_{ts}"));
        fs::create_dir_all(&base).expect("mkdir base");
        let ack_path = base.join("ack.json");
        fs::write(&ack_path, "{").expect("write invalid ack");

        let result = try_read_trial_ack(&ack_path, "run-1");
        assert!(result.is_ok(), "invalid json should not fail immediately");
        assert!(
            result.expect("ok").is_none(),
            "invalid json is treated as pending"
        );

        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn forward_internal_runner_smoke_e2e_from_scout_refs_to_success() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_forward_e2e_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        fs::create_dir_all(base.join("data")).expect("mkdir data");
        fs::create_dir_all(base.join("config/trial-batches")).expect("mkdir trial-batches");
        fs::create_dir_all(base.join("config/trial-acks")).expect("mkdir trial-acks");

        let db_path = base.join("data/optimizer.db");
        let conn = crate::infrastructure::db::open_db(&db_path).expect("open db");
        let cfg = TraderConfig::default();
        let cfg_id = cfg.config_id() as i64;
        crate::infrastructure::db::upsert_configs(&conn, &[cfg]).expect("upsert config");
        drop(conn);

        fs::write(
            base.join("data/scout-references.json"),
            format!(r#"[{{"config_id":{cfg_id},"trades":12,"avg_pnl_pct":0.4}}]"#),
        )
        .expect("write scout refs");

        let ack_base = base.clone();
        tokio::spawn(async move {
            let batch_dir = ack_base.join("config/trial-batches");
            let ack_dir = ack_base.join("config/trial-acks");
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if Instant::now() >= deadline {
                    break;
                }
                if let Ok(entries) = std::fs::read_dir(&batch_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) != Some("json") {
                            continue;
                        }
                        let Some(file_name) = path.file_name() else {
                            continue;
                        };
                        let Ok(raw) = std::fs::read_to_string(&path) else {
                            continue;
                        };
                        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&raw) else {
                            continue;
                        };
                        let Some(run_id) = payload.get("run_id").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        if run_id.is_empty() {
                            continue;
                        }
                        let ack_path = ack_dir.join(file_name);
                        let ack = serde_json::json!({
                            "run_id": run_id,
                            "status": "ok",
                            "config_count": 1,
                            "drained_trades": 0
                        });
                        if let Ok(bytes) = serde_json::to_vec_pretty(&ack) {
                            let _ = std::fs::write(&ack_path, bytes);
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        let manager = TrialRunnerManager::new(base.clone());
        manager
            .start(RunnerStartRequest {
                phase: "forward".to_string(),
                duration: None,
                cycles: None,
                max_budget: Some(1),
                grace_period: Some(1),
                report_interval: Some(1),
                max_refs: Some(1),
                max_configs: Some(1),
                run_id: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            })
            .await
            .expect("start forward");

        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let status = manager.status(200).await;
            if !status.running {
                let done = status.recent_jobs.first().expect("recent job");
                assert_eq!(done.phase, "forward");
                assert_eq!(done.state, RunnerJobState::Success);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "forward runner did not complete before deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn forward_internal_runner_falls_back_when_top_refs_missing_in_db() {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let base = std::env::temp_dir().join(format!("hft_runner_forward_fallback_{ts}"));
        fs::create_dir_all(base.join("ray_driver")).expect("mkdir ray_driver");
        fs::create_dir_all(base.join("data")).expect("mkdir data");
        fs::create_dir_all(base.join("config/trial-batches")).expect("mkdir trial-batches");
        fs::create_dir_all(base.join("config/trial-acks")).expect("mkdir trial-acks");

        let db_path = base.join("data/optimizer.db");
        let conn = crate::infrastructure::db::open_db(&db_path).expect("open db");
        let cfg = TraderConfig::default();
        let cfg_id = cfg.config_id() as i64;
        crate::infrastructure::db::upsert_configs(&conn, &[cfg]).expect("upsert config");
        drop(conn);

        fs::write(
            base.join("data/scout-references.json"),
            format!(
                r#"[{{"config_id":123456789,"trades":50,"avg_pnl_pct":9.9}},{{"config_id":{cfg_id},"trades":12,"avg_pnl_pct":0.4}}]"#
            ),
        )
        .expect("write scout refs");

        let ack_base = base.clone();
        tokio::spawn(async move {
            let batch_dir = ack_base.join("config/trial-batches");
            let ack_dir = ack_base.join("config/trial-acks");
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if Instant::now() >= deadline {
                    break;
                }
                if let Ok(entries) = std::fs::read_dir(&batch_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) != Some("json") {
                            continue;
                        }
                        let Some(file_name) = path.file_name() else {
                            continue;
                        };
                        let Ok(raw) = std::fs::read_to_string(&path) else {
                            continue;
                        };
                        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&raw) else {
                            continue;
                        };
                        let Some(run_id) = payload.get("run_id").and_then(|v| v.as_str()) else {
                            continue;
                        };
                        if run_id.is_empty() {
                            continue;
                        }
                        let ack_path = ack_dir.join(file_name);
                        let ack = serde_json::json!({
                            "run_id": run_id,
                            "status": "ok",
                            "config_count": 1,
                            "drained_trades": 0
                        });
                        if let Ok(bytes) = serde_json::to_vec_pretty(&ack) {
                            let _ = std::fs::write(&ack_path, bytes);
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        let manager = TrialRunnerManager::new(base.clone());
        manager
            .start(RunnerStartRequest {
                phase: "forward".to_string(),
                duration: None,
                cycles: None,
                max_budget: Some(1),
                grace_period: Some(1),
                report_interval: Some(1),
                max_refs: Some(1),
                max_configs: Some(1),
                run_id: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            })
            .await
            .expect("start forward");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = manager.status(200).await;
            if let Some(job) = status.recent_jobs.first() {
                if job.state != RunnerJobState::Running {
                    assert_eq!(
                        job.state,
                        RunnerJobState::Success,
                        "job logs: {:?}",
                        status.logs
                    );
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "forward job did not finish in time"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let _ = fs::remove_dir_all(base);
    }
}
