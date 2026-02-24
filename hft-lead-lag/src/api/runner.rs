//! Embedded trial runner for launching ray_driver phases from the API.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{oneshot, Mutex};

pub const DEFAULT_SCOUT_DURATION_S: u64 = 900;
pub const DEFAULT_EXPAND_DURATION_S: u64 = 900;
pub const DEFAULT_FORWARD_MAX_BUDGET_S: u64 = 240;
pub const DEFAULT_FORWARD_GRACE_PERIOD_S: u64 = 60;
pub const DEFAULT_FORWARD_REPORT_INTERVAL_S: u64 = 30;
pub const DEFAULT_PROMOTE_TOP_K: u64 = 50;
pub const DEFAULT_PROMOTE_MIN_TRADES: u64 = 5;
pub const DEFAULT_PROMOTE_MIN_PNL: f64 = 0.0;

#[derive(Debug, Clone, Deserialize)]
pub struct RunnerStartRequest {
    pub phase: String,
    pub duration: Option<u64>,
    pub max_budget: Option<u64>,
    pub grace_period: Option<u64>,
    pub report_interval: Option<u64>,
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
    pub max_budget: Option<u64>,
    pub grace_period: Option<u64>,
    pub report_interval: Option<u64>,
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
        let cmd = build_trial_runner_command(&req).map_err(RunnerError::bad_request)?;
        if !self.workdir.join("ray_driver").exists() {
            return Err(RunnerError::internal(format!(
                "ray_driver directory not found in {}",
                self.workdir.display()
            )));
        }

        let mut inner = self.inner.lock().await;
        if inner
            .active_job
            .as_ref()
            .is_some_and(|job| job.state == RunnerJobState::Running)
        {
            return Err(RunnerError::conflict("runner job already active"));
        }

        let phase = req.phase.to_lowercase();
        let job_id = inner.next_job_id;
        inner.next_job_id = inner.next_job_id.saturating_add(1);
        let started_at_ms = crate::domain::screener::utils::now_ms();
        let command = format!("{} {}", cmd.program, cmd.args.join(" "));

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

        self.spawn_job(job_id, cmd, stop_rx);

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

fn runner_ui_config() -> RunnerUiConfig {
    RunnerUiConfig {
        phases: vec![
            RunnerPhaseDefaults {
                name: "scout".to_string(),
                duration: Some(DEFAULT_SCOUT_DURATION_S),
                max_budget: None,
                grace_period: None,
                report_interval: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            },
            RunnerPhaseDefaults {
                name: "expand".to_string(),
                duration: Some(DEFAULT_EXPAND_DURATION_S),
                max_budget: None,
                grace_period: None,
                report_interval: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            },
            RunnerPhaseDefaults {
                name: "forward".to_string(),
                duration: None,
                max_budget: Some(DEFAULT_FORWARD_MAX_BUDGET_S),
                grace_period: Some(DEFAULT_FORWARD_GRACE_PERIOD_S),
                report_interval: Some(DEFAULT_FORWARD_REPORT_INTERVAL_S),
                top_k: None,
                min_trades: None,
                min_pnl: None,
            },
            RunnerPhaseDefaults {
                name: "promote".to_string(),
                duration: None,
                max_budget: None,
                grace_period: None,
                report_interval: None,
                top_k: Some(DEFAULT_PROMOTE_TOP_K),
                min_trades: Some(DEFAULT_PROMOTE_MIN_TRADES),
                min_pnl: Some(DEFAULT_PROMOTE_MIN_PNL),
            },
        ],
    }
}

pub fn resolve_runner_workdir() -> PathBuf {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(root) = std::env::var("HFT_LEAD_LAG_ROOT") {
        let p = PathBuf::from(root);
        if !p.as_os_str().is_empty() {
            candidates.push(p);
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.clone());
        candidates.push(cwd.join("hft-lead-lag"));
    }

    if let Ok(exe) = std::env::current_exe() {
        for anc in exe.ancestors() {
            candidates.push(anc.to_path_buf());
            candidates.push(anc.join("hft-lead-lag"));
        }
    }

    if let Some(found) = find_workdir_from_candidates(&candidates) {
        return found;
    }

    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn find_workdir_from_candidates(candidates: &[PathBuf]) -> Option<PathBuf> {
    for c in candidates {
        if c.join("ray_driver").is_dir() {
            return Some(c.clone());
        }
    }
    None
}

pub fn build_trial_runner_command(req: &RunnerStartRequest) -> Result<RunnerCommandSpec, String> {
    let phase = req.phase.trim().to_lowercase();
    let mut args = vec!["-m".to_string(), "ray_driver".to_string()];

    match phase.as_str() {
        "scout" => {
            args.push("scout".to_string());
            args.push("--duration".to_string());
            args.push(req.duration.unwrap_or(DEFAULT_SCOUT_DURATION_S).to_string());
        }
        "expand" => {
            args.push("expand".to_string());
            args.push("--duration".to_string());
            args.push(req.duration.unwrap_or(DEFAULT_EXPAND_DURATION_S).to_string());
        }
        "forward" => {
            args.push("forward".to_string());
            args.push("--max-budget".to_string());
            args.push(
                req.max_budget
                    .unwrap_or(DEFAULT_FORWARD_MAX_BUDGET_S)
                    .to_string(),
            );
            args.push("--grace-period".to_string());
            args.push(
                req.grace_period
                    .unwrap_or(DEFAULT_FORWARD_GRACE_PERIOD_S)
                    .to_string(),
            );
            args.push("--report-interval".to_string());
            args.push(
                req.report_interval
                    .unwrap_or(DEFAULT_FORWARD_REPORT_INTERVAL_S)
                    .to_string(),
            );
        }
        "promote" => {
            let run_id = req
                .run_id
                .as_ref()
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| "promote requires run_id".to_string())?;
            args.push("promote".to_string());
            args.push(run_id.to_string());
            args.push("--top-k".to_string());
            args.push(req.top_k.unwrap_or(DEFAULT_PROMOTE_TOP_K).to_string());
            args.push("--min-trades".to_string());
            args.push(
                req.min_trades
                    .unwrap_or(DEFAULT_PROMOTE_MIN_TRADES)
                    .to_string(),
            );
            args.push("--min-pnl".to_string());
            args.push(format!("{}", req.min_pnl.unwrap_or(DEFAULT_PROMOTE_MIN_PNL)));
        }
        _ => return Err(format!("Unsupported phase: {phase}")),
    }

    Ok(RunnerCommandSpec {
        program: "python3".to_string(),
        args,
    })
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
            max_budget: None,
            grace_period: None,
            report_interval: None,
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
            ]
        );
    }

    #[test]
    fn build_forward_command_with_overrides() {
        let req = RunnerStartRequest {
            phase: "forward".to_string(),
            duration: None,
            max_budget: Some(720),
            grace_period: Some(120),
            report_interval: Some(15),
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
            ]
        );
    }

    #[test]
    fn promote_requires_run_id() {
        let req = RunnerStartRequest {
            phase: "promote".to_string(),
            duration: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
            run_id: None,
            top_k: None,
            min_trades: None,
            min_pnl: None,
        };

        let err = build_trial_runner_command(&req).expect_err("must fail");
        assert!(err.contains("run_id"));
    }

    #[test]
    fn unknown_phase_rejected() {
        let req = RunnerStartRequest {
            phase: "hack".to_string(),
            duration: None,
            max_budget: None,
            grace_period: None,
            report_interval: None,
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

        let scout = cfg
            .phases
            .iter()
            .find(|p| p.name == "scout")
            .expect("scout phase");
        let forward = cfg
            .phases
            .iter()
            .find(|p| p.name == "forward")
            .expect("forward phase");
        let promote = cfg
            .phases
            .iter()
            .find(|p| p.name == "promote")
            .expect("promote phase");

        assert_eq!(scout.duration, Some(DEFAULT_SCOUT_DURATION_S));
        assert_eq!(forward.max_budget, Some(DEFAULT_FORWARD_MAX_BUDGET_S));
        assert_eq!(forward.grace_period, Some(DEFAULT_FORWARD_GRACE_PERIOD_S));
        assert_eq!(
            forward.report_interval,
            Some(DEFAULT_FORWARD_REPORT_INTERVAL_S)
        );
        assert_eq!(promote.top_k, Some(DEFAULT_PROMOTE_TOP_K));
        assert_eq!(promote.min_trades, Some(DEFAULT_PROMOTE_MIN_TRADES));
        assert_eq!(promote.min_pnl, Some(DEFAULT_PROMOTE_MIN_PNL));
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
}
