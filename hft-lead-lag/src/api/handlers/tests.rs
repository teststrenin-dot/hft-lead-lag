use super::*;
use axum::extract::State;
use dashmap::DashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn compute_fleet_stats_handles_zero_trades() {
    let stats = compute_fleet_stats(0, 0, 42.0);
    assert_eq!(stats.win_rate_pct, 0.0);
    assert_eq!(stats.avg_pnl_pct, 0.0);
}

#[test]
fn compute_fleet_stats_calculates_win_rate_and_avg() {
    let stats = compute_fleet_stats(20, 5, 10.0);
    assert_eq!(stats.win_rate_pct, 25.0);
    assert_eq!(stats.avg_pnl_pct, 0.5);
}

#[test]
fn evaluate_db_saturation_health_marks_drop_budget_exhausted() {
    let policy =
        evaluate_db_saturation_health(DbWriter::dropped_batch_budget().saturating_add(1), 0);
    assert!(policy.drop_budget_exhausted);
    assert!(!policy.overflow_warn);
}

#[test]
fn evaluate_db_saturation_health_marks_overflow_warning_at_threshold() {
    let policy = evaluate_db_saturation_health(0, DbWriter::overflow_warn_threshold());
    assert!(!policy.drop_budget_exhausted);
    assert!(policy.overflow_warn);
}

#[test]
fn fallback_cache_refresh_policy_refreshes_when_cache_empty() {
    assert!(should_refresh_fallback_rows_cache(10_000, 9_000, true));
}

#[test]
fn fallback_cache_refresh_policy_refreshes_when_cache_stale() {
    assert!(should_refresh_fallback_rows_cache(
        20_000,
        20_000 - FALLBACK_ROWS_TTL_MS - 1,
        false
    ));
}

#[test]
fn fallback_cache_refresh_policy_skips_when_cache_is_fresh() {
    assert!(!should_refresh_fallback_rows_cache(
        20_000,
        20_000 - FALLBACK_ROWS_TTL_MS + 1,
        false
    ));
}

#[tokio::test]
async fn health_returns_degraded_when_feed_is_stale() {
    let health_state = Arc::new(HealthState::new());
    health_state
        .binance_connected
        .store(true, Ordering::Relaxed);
    health_state.gate_connected.store(true, Ordering::Relaxed);
    health_state
        .binance_last_tick_ms
        .store(1, Ordering::Relaxed);
    health_state.gate_last_tick_ms.store(1, Ordering::Relaxed);

    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let (code, Json(resp)) = health(State(state)).await;
    assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.status, "degraded");
    assert!(resp.issues.contains(&"binance_stale"));
    assert!(resp.issues.contains(&"gate_stale"));
}

#[tokio::test]
async fn health_reports_drop_counters() {
    let health_state = Arc::new(HealthState::new());
    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let (_code, Json(resp)) = health(State(state)).await;
    assert_eq!(
        resp.binance_dropped_messages,
        crate::infrastructure::exchanges::BinanceMarketData::dropped_messages()
    );
    assert_eq!(
        resp.gate_dropped_messages,
        crate::infrastructure::exchanges::GateMarketData::dropped_messages()
    );
    assert_eq!(
        resp.db_dropped_batches,
        crate::infrastructure::db::DbWriter::dropped_batches()
    );
    assert_eq!(
        resp.db_overflowed_batches,
        crate::infrastructure::db::DbWriter::overflowed_batches()
    );
    assert_eq!(
        resp.db_dropped_batch_budget,
        DbWriter::dropped_batch_budget()
    );
    assert_eq!(
        resp.db_overflow_warn_threshold,
        DbWriter::overflow_warn_threshold()
    );
    let expected =
        evaluate_db_saturation_health(resp.db_dropped_batches, resp.db_overflowed_batches);
    assert_eq!(
        resp.issues.contains(&"db_drop_budget_exhausted"),
        expected.drop_budget_exhausted
    );
    assert_eq!(
        resp.warnings.contains(&"db_overflow_batches_high"),
        expected.overflow_warn
    );
    assert_eq!(resp.trial_queue_depth, 0);
    assert_eq!(resp.trial_last_ack_status, "unknown");
    assert_eq!(resp.trial_active_run_id, None);
}

#[tokio::test]
async fn health_reports_trial_lifecycle_telemetry() {
    let health_state = Arc::new(HealthState::new());
    let now_ms = crate::domain::screener::utils::now_ms();
    health_state
        .binance_connected
        .store(true, Ordering::Relaxed);
    health_state.gate_connected.store(true, Ordering::Relaxed);
    health_state
        .binance_last_tick_ms
        .store(now_ms, Ordering::Relaxed);
    health_state
        .gate_last_tick_ms
        .store(now_ms, Ordering::Relaxed);
    health_state
        .trial_last_ack_ms
        .store(now_ms.saturating_sub(1_000), Ordering::Relaxed);
    health_state
        .trial_last_ack_error
        .store(true, Ordering::Relaxed);
    health_state.trial_queue_depth.store(12, Ordering::Relaxed);

    let screener = ScreenerStore::default();
    screener.set_run_id(Some("run-health-telemetry".to_string()));
    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener,
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let (_code, Json(resp)) = health(State(state)).await;
    assert_eq!(resp.trial_queue_depth, 12);
    assert_eq!(resp.trial_last_ack_status, "error");
    assert_eq!(
        resp.trial_active_run_id.as_deref(),
        Some("run-health-telemetry")
    );
    assert!(resp.warnings.contains(&"trial_last_ack_error"));
    assert!(resp.warnings.contains(&"trial_queue_depth_high"));
}

#[tokio::test]
async fn fleet_policy_endpoint_returns_empty_for_unknown_symbol() {
    let health_state = Arc::new(HealthState::new());
    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let Json(rows) = get_fleet_policy_for_symbol(
        State(state),
        axum::extract::Path("BTCUSDT".to_string()),
        Query(FleetPolicyQuery { top_k: Some(5) }),
    )
    .await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn fleet_policy_overview_returns_empty_without_fleets() {
    let health_state = Arc::new(HealthState::new());
    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let Json(rows) = get_fleet_policy_overview(
        State(state),
        Query(FleetPolicyOverviewQuery {
            top_k: Some(5),
            max_symbols: Some(10),
        }),
    )
    .await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn fleet_policy_overview_returns_symbol_rows_with_policies() {
    let health_state = Arc::new(HealthState::new());
    let screener = ScreenerStore::default();
    screener.update("BTCUSDT", "binance", 100.0, 100.1, 1_000_000, 1_000_000);
    screener.update("BTCUSDT", "gate", 100.0, 100.1, 1_001_000, 1_001_000);
    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener,
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let Json(rows) = get_fleet_policy_overview(
        State(state),
        Query(FleetPolicyOverviewQuery {
            top_k: Some(3),
            max_symbols: Some(10),
        }),
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].symbol, "BTCUSDT");
    assert!(rows[0].policies.len() <= 3);
}

#[tokio::test]
async fn trial_runs_expose_patch_level_metadata() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let db_path = std::env::temp_dir().join(format!("trial-runs-meta-{unique}.db"));
    let conn = crate::infrastructure::db::open_db(&db_path).expect("open db");
    crate::infrastructure::db::upsert_trial_run_meta(
        &conn,
        "scout-1",
        10,
        1000,
        2,
        crate::infrastructure::db::TrialPatchMeta::default(),
    )
    .expect("upsert trial run");
    drop(conn);

    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: Arc::new(HealthState::new()),
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: db_path.clone(),
    });

    let Json(runs) = get_trial_runs(State(state)).await.expect("trial runs");
    let run = runs
        .iter()
        .find(|r| r.run_id == "scout-1")
        .expect("run present");
    assert_eq!(run.apply_mode, "full_replace");
    assert_eq!(run.symbols_reset, 0);
    assert_eq!(run.changed_ids_requested, 0);
    assert_eq!(run.matched_changed_ids_old, 0);
    assert_eq!(run.matched_changed_ids_new, 0);
    assert_eq!(run.unmatched_changed_ids, 0);
    assert_eq!(run.scope_symbols_requested, 0);
    assert_eq!(run.scope_symbols_matched, 0);

    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[tokio::test]
async fn portfolio_active_endpoint_returns_a_and_b_slots() {
    let health_state = Arc::new(HealthState::new());
    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let Json(resp) = get_portfolio_active(State(state)).await;
    assert_eq!(resp.portfolios.len(), 2);
    assert_eq!(resp.portfolios[0].portfolio_id, "A");
    assert_eq!(resp.portfolios[1].portfolio_id, "B");
}

#[tokio::test]
async fn portfolio_candidates_endpoint_returns_derived_metrics() {
    let health_state = Arc::new(HealthState::new());
    let screener = ScreenerStore::default();
    screener.update("BTCUSDT", "binance", 100.0, 100.1, 1_000_000, 1_000_000);
    screener.portfolio_observe_closed_trade_v1("BTCUSDT", 0.20, false, 1_001_000);
    screener.portfolio_observe_closed_trade_v1("BTCUSDT", -0.10, true, 1_002_000);
    screener.portfolio_observe_closed_trade_v1("BTCUSDT", 0.00, false, 1_003_000);

    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener,
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let Json(resp) = get_portfolio_candidates(State(state)).await;
    assert_eq!(resp.total_candidates, 1);
    assert_eq!(resp.rows[0].symbol, "BTCUSDT");
    assert_eq!(resp.rows[0].pm_raw, 0);
    assert!((resp.rows[0].useful_winrate - (1.0 / 3.0)).abs() < 1e-9);
}

#[tokio::test]
async fn portfolio_guards_endpoint_reports_cooldown_state() {
    let health_state = Arc::new(HealthState::new());
    let screener = ScreenerStore::default();
    let base_ts = crate::domain::screener::utils::now_ms();
    for i in 0..5 {
        screener.portfolio_observe_closed_trade_v1(
            "ETHUSDT",
            -0.05,
            true,
            base_ts + i * 1_000,
        );
    }

    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener,
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: health_state,
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: PathBuf::from("data/optimizer.db"),
    });

    let Json(resp) = get_portfolio_guards(State(state)).await;
    assert_eq!(resp.total_symbols, 1);
    assert_eq!(resp.rows[0].symbol, "ETHUSDT");
    assert!(resp.rows[0].cooldown_until_ms.is_some());
    assert!(resp.rows[0].in_cooldown);
}

#[tokio::test]
async fn portfolio_active_endpoint_falls_back_to_db_state_snapshot() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let db_path = std::env::temp_dir().join(format!("portfolio-active-fallback-{unique}.db"));
    let conn = crate::infrastructure::db::open_db(&db_path).expect("open db");
    crate::infrastructure::db::replace_portfolio_state_v1(
        &conn,
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["BTCUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 1_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec!["ETHUSDT".to_string()],
                active_symbols: vec![],
                updated_at_ms: 1_000,
            },
        ],
    )
    .expect("seed portfolio state");
    drop(conn);

    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: Arc::new(HealthState::new()),
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: db_path.clone(),
    });

    let Json(resp) = get_portfolio_active(State(state)).await;
    let a = resp
        .portfolios
        .iter()
        .find(|p| p.portfolio_id == "A")
        .expect("portfolio A");
    let b = resp
        .portfolios
        .iter()
        .find(|p| p.portfolio_id == "B")
        .expect("portfolio B");
    assert_eq!(a.active_symbols, vec!["BTCUSDT".to_string()]);
    assert_eq!(b.shortlist, vec!["ETHUSDT".to_string()]);

    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[tokio::test]
async fn portfolio_guards_endpoint_falls_back_to_db_state_snapshot() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let db_path = std::env::temp_dir().join(format!("portfolio-guards-fallback-{unique}.db"));
    let conn = crate::infrastructure::db::open_db(&db_path).expect("open db");
    crate::infrastructure::db::replace_portfolio_guards_v1(
        &conn,
        &[crate::infrastructure::db::PortfolioGuardRecordV1 {
            symbol: "SOLUSDT".to_string(),
            streak_count: 0,
            first_streak_ts_ms: None,
            cooldown_until_ms: Some(crate::domain::screener::utils::now_ms() + 60_000),
            updated_at_ms: 1_000,
        }],
    )
    .expect("seed portfolio guards");
    drop(conn);

    let state = Arc::new(HttpState {
        min_volume_usd: 1_000_000.0,
        screener: ScreenerStore::default(),
        natr_cache: Arc::new(DashMap::new()),
        fallback_rows_cache: Arc::new(ArcSwap::from_pointee(Vec::new())),
        fallback_rows_last_refresh_ms: Arc::new(AtomicI64::new(0)),
        fallback_rows_refresh_in_flight: Arc::new(AtomicBool::new(false)),
        health: Arc::new(HealthState::new()),
        trial_runner: TrialRunnerManager::new(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        ),
        db_path: db_path.clone(),
    });

    let Json(resp) = get_portfolio_guards(State(state)).await;
    assert_eq!(resp.total_symbols, 1);
    assert_eq!(resp.rows[0].symbol, "SOLUSDT");
    assert!(resp.rows[0].in_cooldown);

    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}
