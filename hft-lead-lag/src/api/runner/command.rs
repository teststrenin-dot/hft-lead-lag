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
                top_k: None,
                min_trades: None,
                min_pnl: None,
            },
            RunnerPhaseDefaults {
                name: "expand".to_string(),
                duration: Some(DEFAULT_EXPAND_DURATION_S),
                cycles: Some(DEFAULT_EXPAND_CYCLES),
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
                cycles: None,
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
                cycles: None,
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
        "expand" => {
            let cycles = req.cycles.unwrap_or(DEFAULT_EXPAND_CYCLES);
            if cycles == 0 {
                return Err("cycles must be >= 1".to_string());
            }
            args.push("expand".to_string());
            args.push("--duration".to_string());
            args.push(req.duration.unwrap_or(DEFAULT_EXPAND_DURATION_S).to_string());
            args.push("--cycles".to_string());
            args.push(cycles.to_string());
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
