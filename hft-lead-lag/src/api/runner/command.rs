use super::*;

pub(super) fn runner_ui_config() -> RunnerUiConfig {
    RunnerUiConfig {
        phases: vec![
            RunnerPhaseDefaults {
                name: "scout".to_string(),
                duration: Some(DEFAULT_SCOUT_DURATION_S),
                cycles: Some(DEFAULT_SCOUT_CYCLES),
                max_budget: None,
                grace_period: None,
                report_interval: None,
                max_refs: None,
                max_configs: None,
                top_k: None,
                min_trades: None,
                min_pnl: None,
            },
            RunnerPhaseDefaults {
                name: "forward".to_string(),
                duration: None,
                cycles: None,
                max_budget: Some(DEFAULT_FORWARD_MAX_BUDGET_S),
                grace_period: Some(DEFAULT_FORWARD_GRACE_PERIOD_S),
                report_interval: Some(DEFAULT_FORWARD_REPORT_INTERVAL_S),
                max_refs: Some(DEFAULT_FORWARD_MAX_REFS),
                max_configs: Some(DEFAULT_FORWARD_MAX_CONFIGS),
                top_k: None,
                min_trades: None,
                min_pnl: None,
            },
        ],
    }
}

pub(super) fn resolve_runner_workdir() -> PathBuf {
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

pub(super) fn find_workdir_from_candidates(candidates: &[PathBuf]) -> Option<PathBuf> {
    for c in candidates {
        if c.join("ray_driver").is_dir() {
            return Some(c.clone());
        }
    }
    None
}

pub(super) fn build_trial_runner_command(
    req: &RunnerStartRequest,
) -> Result<RunnerCommandSpec, String> {
    let phase = req.phase.trim().to_lowercase();
    let mut args = vec!["-m".to_string(), "ray_driver".to_string()];

    match phase.as_str() {
        "scout" => {
            let cycles = req.cycles.unwrap_or(DEFAULT_SCOUT_CYCLES);
            if cycles == 0 {
                return Err("cycles must be >= 1".to_string());
            }
            args.push("scout".to_string());
            args.push("--duration".to_string());
            args.push(req.duration.unwrap_or(DEFAULT_SCOUT_DURATION_S).to_string());
            args.push("--cycles".to_string());
            args.push(cycles.to_string());
        }
        "forward" => {
            let max_budget = req.max_budget.unwrap_or(DEFAULT_FORWARD_MAX_BUDGET_S);
            let grace_period = req.grace_period.unwrap_or(DEFAULT_FORWARD_GRACE_PERIOD_S);
            let report_interval = req
                .report_interval
                .unwrap_or(DEFAULT_FORWARD_REPORT_INTERVAL_S);
            let max_refs = req.max_refs.unwrap_or(DEFAULT_FORWARD_MAX_REFS);
            let max_configs = req.max_configs.unwrap_or(DEFAULT_FORWARD_MAX_CONFIGS);

            if max_budget == 0 {
                return Err("max_budget must be >= 1".to_string());
            }
            if grace_period == 0 {
                return Err("grace_period must be >= 1".to_string());
            }
            if report_interval == 0 {
                return Err("report_interval must be >= 1".to_string());
            }

            args.push("forward".to_string());
            args.push("--max-budget".to_string());
            args.push(max_budget.to_string());
            args.push("--grace-period".to_string());
            args.push(grace_period.to_string());
            args.push("--report-interval".to_string());
            args.push(report_interval.to_string());
            args.push("--max-refs".to_string());
            args.push(max_refs.clamp(1, FORWARD_MAX_REFS_HARD_CAP).to_string());
            args.push("--max-configs".to_string());
            args.push(
                max_configs
                    .clamp(1, FORWARD_MAX_CONFIGS_HARD_CAP)
                    .to_string(),
            );
        }
        _ => return Err(format!("Unsupported phase: {phase}")),
    }

    Ok(RunnerCommandSpec {
        program: "python3".to_string(),
        args,
    })
}
