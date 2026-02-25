use super::{CONFIG_ID_CONTRACT_VERSION, EventLoopState, FleetPatchMode, FleetPatchPlan, TraderConfig};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct TrialBatch {
    pub(super) run_id: String,
    pub(super) configs: Vec<TraderConfig>,
    #[serde(default)]
    pub(super) mode: Option<String>,
    #[serde(default)]
    pub(super) changed_config_ids: Option<Vec<u64>>,
    #[serde(default)]
    pub(super) symbols: Option<Vec<String>>,
    #[serde(default)]
    pub(super) config_id_contract_version: Option<u16>,
    #[serde(default)]
    pub(super) submission_id: Option<String>,
    #[serde(default)]
    pub(super) allow_run_id_takeover: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TrialBatchMode {
    FullReplace,
    Incremental,
}

impl TrialBatchMode {
    fn from_strict(mode: Option<&str>) -> Result<Self, String> {
        let Some(raw) = mode.map(str::trim) else {
            return Ok(Self::FullReplace);
        };
        if raw.eq_ignore_ascii_case("full_replace") {
            Ok(Self::FullReplace)
        } else if raw.eq_ignore_ascii_case("incremental") {
            Ok(Self::Incremental)
        } else {
            Err(format!(
                "trial batch mode must be full_replace|incremental, got {raw}"
            ))
        }
    }
}

impl TrialBatch {
    pub(super) fn parse_mode_strict(&self) -> Result<TrialBatchMode, String> {
        TrialBatchMode::from_strict(self.mode.as_deref())
    }

    fn validate_contract_version(&self) -> Result<(), String> {
        let requested = self
            .config_id_contract_version
            .unwrap_or(CONFIG_ID_CONTRACT_VERSION);
        if requested != CONFIG_ID_CONTRACT_VERSION {
            return Err(format!(
                "config_id contract version mismatch: got {requested}, expected {CONFIG_ID_CONTRACT_VERSION}"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub(super) struct TrialControl {
    pub(super) clear_run_id: bool,
    pub(super) run_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(super) struct TrialAck {
    pub(super) run_id: String,
    pub(super) applied_at_ms: i64,
    pub(super) config_count: usize,
    pub(super) drained_trades: usize,
    pub(super) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) submission_id: Option<String>,
}

impl TrialAck {
    pub(super) fn success(
        run_id: String,
        applied_at_ms: i64,
        config_count: usize,
        drained_trades: usize,
        submission_id: Option<String>,
    ) -> Self {
        Self {
            run_id,
            applied_at_ms,
            config_count,
            drained_trades,
            status: "ok".to_string(),
            error: None,
            submission_id,
        }
    }

    pub(super) fn error(run_id: String, error: String, submission_id: Option<String>) -> Self {
        Self {
            run_id,
            applied_at_ms: EventLoopState::now_ms(),
            config_count: 0,
            drained_trades: 0,
            status: "error".to_string(),
            error: Some(error),
            submission_id,
        }
    }
}

pub(super) fn load_trial_batch(path: &Path) -> Result<TrialBatch, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read trial batch {}: {e}", path.display()))?;
    let batch: TrialBatch = serde_json::from_str(&content)
        .map_err(|e| format!("parse trial batch {}: {e}", path.display()))?;
    if batch.configs.is_empty() {
        return Err("trial batch has no configs".to_string());
    }
    if batch.run_id.is_empty() {
        return Err("trial batch run_id is empty".to_string());
    }
    Ok(batch)
}

pub(super) fn build_trial_batch_patch_plan(batch: &TrialBatch) -> Result<FleetPatchPlan, String> {
    batch.validate_contract_version()?;
    let mode = batch.parse_mode_strict()?;
    match mode {
        TrialBatchMode::FullReplace => Ok(FleetPatchPlan::new(
            FleetPatchMode::FullReplace,
            Vec::<u64>::new(),
            None::<Vec<String>>,
        )),
        TrialBatchMode::Incremental => {
            let changed_config_ids = batch
                .changed_config_ids
                .as_ref()
                .ok_or_else(|| "incremental mode requires changed_config_ids".to_string())?;
            if changed_config_ids.is_empty() {
                return Err("incremental mode requires non-empty changed_config_ids".to_string());
            }
            let plan = FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                changed_config_ids.iter().copied(),
                batch.symbols.clone(),
            );
            if batch.symbols.is_some() && !plan.has_symbol_scope() {
                return Err(
                    "incremental mode symbols must contain at least one non-empty symbol"
                        .to_string(),
                );
            }
            Ok(plan)
        }
    }
}

pub(super) fn load_trial_control(path: &Path) -> Result<TrialControl, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read trial control {}: {e}", path.display()))?;
    serde_json::from_str::<TrialControl>(&content)
        .map_err(|e| format!("parse trial control {}: {e}", path.display()))
}
