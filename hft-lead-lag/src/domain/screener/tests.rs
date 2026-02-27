use super::{
    shadow_fleet::{FleetTrade, ShadowFleet},
    FleetPatchApplyError, FleetPatchMode, FleetPatchPlan, ScreenerStore, SymbolState, TraderConfig,
};
use crate::domain::screener::shadow_trader::{ClosedTrade, Direction, ExitReason};

fn config_with_gap(spike_threshold_bps: f64) -> TraderConfig {
    TraderConfig {
        spike_threshold_bps,
        ..TraderConfig::default()
    }
}

fn with_symbol_fleet(store: &ScreenerStore, symbol: &str, configs: &[TraderConfig]) {
    let state = SymbolState {
        fleet: Some(ShadowFleet::new(configs)),
        ..SymbolState::default()
    };
    store.symbols.insert(symbol.to_string(), state);
}

fn sample_closed_trade(ts_ms: i64) -> ClosedTrade {
    ClosedTrade {
        direction: Direction::Long,
        entry_ts_ms: ts_ms - 500,
        ts_ms,
        entry_price: 100.0,
        exit_price: 100.2,
        spike_bps: 50.0,
        pnl_pct: 0.2,
        exit_reason: ExitReason::TrailingTake,
        catchup_pct: 0.2,
        catchup_ms: 500,
        gate_spread_at_entry_bps: 1.0,
        gate_natr_30m_pct_at_entry: 0.0,
        hold_ms: 500,
        early_stop_churn: false,
    }
}

#[test]
fn top_policy_configs_returns_none_for_unknown_symbol() {
    let store = ScreenerStore::default();
    assert!(store.top_policy_configs("BTCUSDT", 5).is_none());
}

#[test]
fn top_policy_configs_returns_some_for_known_symbol_fleet() {
    let store = ScreenerStore::default();
    with_symbol_fleet(&store, "BTCUSDT", &[config_with_gap(50.0)]);
    let rows = store
        .top_policy_configs("btcusdt", 5)
        .expect("policy rows for known symbol");
    assert!(rows.is_empty());
}

#[test]
fn fleet_policy_overview_sorts_symbols_and_applies_limit() {
    let store = ScreenerStore::default();
    with_symbol_fleet(&store, "BTCUSDT", &[config_with_gap(50.0)]);
    with_symbol_fleet(&store, "ADAUSDT", &[config_with_gap(60.0)]);

    let overview = store.fleet_policy_overview(5, 1);
    assert_eq!(overview.len(), 1);
    assert_eq!(overview[0].0, "ADAUSDT");
}

#[test]
fn prune_symbol_catalog_with_limits_drops_stale_symbols() {
    let store = ScreenerStore::default();
    let stale = SymbolState {
        updated_at_ms: 1_000,
        ..SymbolState::default()
    };
    let fresh = SymbolState {
        updated_at_ms: 9_950,
        ..SymbolState::default()
    };
    store.symbols.insert("STALE".to_string(), stale);
    store.symbols.insert("FRESH".to_string(), fresh);

    let removed = store.prune_symbol_catalog_with_limits(10_000, 500, 10);

    assert_eq!(removed, 1);
    assert!(store.symbols.get("STALE").is_none());
    assert!(store.symbols.get("FRESH").is_some());
}

#[test]
fn prune_symbol_catalog_with_limits_drops_stale_candidate_accumulator() {
    let store = ScreenerStore::default();
    store.symbols.insert(
        "STALE".to_string(),
        SymbolState {
            updated_at_ms: 1_000,
            first_tick_ms: Some(1_000),
            ..SymbolState::default()
        },
    );
    store.observe_closed_trade_for_portfolio("STALE", 0.20, false, 2_000);
    assert!(store.trade_accumulators.get("STALE").is_some());

    let removed = store.prune_symbol_catalog_with_limits(10_000, 500, 10);
    assert_eq!(removed, 1);
    assert!(store.symbols.get("STALE").is_none());
    assert!(store.trade_accumulators.get("STALE").is_none());

    let candidates = store.portfolio_candidate_stats_v1(10_000);
    assert!(
        candidates.iter().all(|stats| stats.symbol != "STALE"),
        "pruned symbol must not stay in candidate stats"
    );
}

#[test]
fn prune_symbol_catalog_with_limits_enforces_cardinality_cap() {
    let store = ScreenerStore::default();
    for idx in 0..5 {
        let state = SymbolState {
            updated_at_ms: 1_000 + idx,
            ..SymbolState::default()
        };
        store.symbols.insert(format!("SYM{idx}"), state);
    }

    let removed = store.prune_symbol_catalog_with_limits(2_000, 10_000, 3);

    assert_eq!(removed, 2);
    assert_eq!(store.symbols.len(), 3);
    assert!(store.symbols.get("SYM0").is_none());
    assert!(store.symbols.get("SYM1").is_none());
    assert!(store.symbols.get("SYM2").is_some());
    assert!(store.symbols.get("SYM3").is_some());
    assert!(store.symbols.get("SYM4").is_some());
}

#[test]
fn update_drains_pending_fleet_trades_even_without_db_writer() {
    let store = ScreenerStore::default();
    let cfg = config_with_gap(55.0);
    with_symbol_fleet(&store, "BTCUSDT", &[cfg]);
    {
        let mut state = store.symbols.get_mut("BTCUSDT").expect("BTCUSDT state");
        let fleet = state.fleet.as_mut().expect("fleet");
        fleet.push_pending_trade_for_test(FleetTrade {
            config_id: cfg.config_id(),
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: sample_closed_trade(2_000),
        });
        assert_eq!(fleet.pending_trades_len(), 1);
    }

    let ts_ns = 1_700_000_000_000_000_000_i64;
    store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
    store.update(
        "BTCUSDT",
        "gate",
        100.0,
        100.1,
        ts_ns + 1_000_000,
        ts_ns + 1_000_000,
    );

    let state = store.symbols.get("BTCUSDT").expect("BTCUSDT state");
    let fleet = state.fleet.as_ref().expect("fleet");
    assert_eq!(fleet.pending_trades_len(), 0);
}

#[test]
fn update_partial_book_with_existing_portfolio_stats_does_not_block() {
    let store = ScreenerStore::default();
    // Pre-create candidate stats so update path traverses accumulators,
    // but rebalance now runs only from dedicated scheduler ticks.
    store.observe_closed_trade_for_portfolio("BTCUSDT", 0.10, false, 1_000);
    let ts_ns = 1_700_000_100_000_000_000_i64;
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let store_clone = store.clone();

    std::thread::spawn(move || {
        store_clone.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("update() must not block on partial-book path");
    assert_eq!(store.portfolio_last_rebalance_ms(), None);
}

#[test]
fn partial_book_update_does_not_emit_live_row() {
    let store = ScreenerStore::default();
    let ts_ns = 1_700_000_100_000_000_000_i64;

    store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);

    let rows = store.rows_sorted();
    assert!(
        rows.is_empty(),
        "single-sided book must not be exposed as ws_live row"
    );
}

#[test]
fn update_rejects_dirty_spread_and_does_not_create_state() {
    let store = ScreenerStore::default();
    let ts_ns = 1_700_000_000_000_000_000_i64;

    store.update("BTCUSDT", "binance", 100.1, 100.0, ts_ns, ts_ns);

    assert!(
        store.symbols.get("BTCUSDT").is_none(),
        "ask < bid must be rejected before symbol state allocation"
    );
}

#[test]
fn update_rejects_exchange_timestamp_regression_per_side() {
    let store = ScreenerStore::default();
    let ts_ns = 1_700_000_000_000_000_000_i64;

    store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
    store.update(
        "BTCUSDT",
        "gate",
        100.0,
        100.1,
        ts_ns + 1_000_000,
        ts_ns + 1_000_000,
    );

    let before_binance = {
        let before = store
            .symbols
            .get("BTCUSDT")
            .expect("state must exist after valid updates");
        before.binance.as_ref().expect("binance quote").clone()
    };

    // Older exchange timestamp for the same side must be ignored.
    store.update(
        "BTCUSDT",
        "binance",
        101.0,
        101.1,
        ts_ns - 1_000_000,
        ts_ns - 1_000_000,
    );

    let after = store
        .symbols
        .get("BTCUSDT")
        .expect("state must still exist");
    let after_binance = after.binance.as_ref().expect("binance quote");
    assert_eq!(
        after_binance.ts_ms, before_binance.ts_ms,
        "older binance quote timestamp must not overwrite last accepted timestamp"
    );
    assert_eq!(
        after_binance.bid, before_binance.bid,
        "older binance quote must not overwrite bid"
    );
    assert_eq!(
        after_binance.ask, before_binance.ask,
        "older binance quote must not overwrite ask"
    );
}

#[test]
fn corrected_timestamp_step_back_does_not_drop_newer_raw_events() {
    let store = ScreenerStore::default();
    let base_ms = 1_700_000_000_000_i64;

    // First sample establishes a large positive offset.
    store.update(
        "BTCUSDT",
        "binance",
        100.0,
        100.1,
        (base_ms + 10) * 1_000_000,
        (base_ms + 1_010) * 1_000_000,
    );

    // Feed enough near-zero-offset samples to trigger median recompute.
    for i in 0..64_i64 {
        let event_ms = base_ms + 20 + i;
        store.update(
            "BTCUSDT",
            "binance",
            100.0 + i as f64 * 0.001,
            100.1 + i as f64 * 0.001,
            event_ms * 1_000_000,
            event_ms * 1_000_000,
        );
    }

    // This update has newer raw exchange timestamp and must be accepted even if corrected ts steps back.
    store.update(
        "BTCUSDT",
        "binance",
        123.0,
        123.1,
        (base_ms + 200) * 1_000_000,
        (base_ms + 200) * 1_000_000,
    );

    let state = store.symbols.get("BTCUSDT").expect("state");
    let binance = state.binance.as_ref().expect("binance quote");
    assert_eq!(
        binance.bid, 123.0,
        "newer raw event must not be rejected by corrected-ts step-back"
    );
}

#[test]
fn rows_sorted_marks_live_ws_source_and_update_time() {
    let store = ScreenerStore::default();
    let ts_ns = 1_700_000_000_000_000_000_i64;
    store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
    store.update(
        "BTCUSDT",
        "gate",
        100.0,
        100.1,
        ts_ns + 1_000_000,
        ts_ns + 1_000_000,
    );

    let rows = store.rows_sorted();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.symbol, "BTCUSDT");
    assert_eq!(row.data_source, "ws_live");
    assert!(!row.is_fallback);
    assert!(row.last_update_ms > 0);
}

#[test]
fn rows_sorted_uses_exchange_offset_correction_for_leader_and_lag() {
    let store = ScreenerStore::default();
    let base_ms = 1_700_000_000_000_i64;
    let gate_clock_ahead_ms = 3_600_000_i64; // +1h

    // Gate quote arrives first locally, but exchange clock is far ahead.
    store.update(
        "BTCUSDT",
        "gate",
        100.0,
        100.1,
        (base_ms + 10 + gate_clock_ahead_ms) * 1_000_000,
        (base_ms + 10) * 1_000_000,
    );
    // Binance quote arrives later locally; with offset correction it should be leader.
    store.update(
        "BTCUSDT",
        "binance",
        100.0,
        100.1,
        (base_ms + 20) * 1_000_000,
        (base_ms + 20) * 1_000_000,
    );

    let rows = store.rows_sorted();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row.leader_exchange, "binance",
        "leader must follow corrected timeline, not raw cross-exchange clock skew"
    );
    assert!(
        row.lag_ms < 100.0,
        "lag should stay near local receive delta, got {}ms",
        row.lag_ms
    );
}

#[test]
fn rows_sorted_uses_snapshot_within_rebuild_interval() {
    let store = ScreenerStore::default();
    let ts_ns = 1_700_000_000_000_000_000_i64;
    store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
    store.update(
        "BTCUSDT",
        "gate",
        100.0,
        100.1,
        ts_ns + 1_000_000,
        ts_ns + 1_000_000,
    );

    let first = store.rows_sorted();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].volume_24h_usd, 0.0);

    store.set_volumes(&[("BTCUSDT".to_string(), 42.0)]);
    let second = store.rows_sorted();
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].volume_24h_usd, 0.0,
        "within cache interval rows must come from previous snapshot"
    );

    std::thread::sleep(std::time::Duration::from_millis(350));
    let third = store.rows_sorted();
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].volume_24h_usd, 42.0);
}

#[test]
fn full_replace_resets_all_symbol_fleets() {
    let store = ScreenerStore::default();
    let old_a = config_with_gap(31.0);
    let old_b = config_with_gap(32.0);
    with_symbol_fleet(&store, "BTCUSDT", &[old_a]);
    with_symbol_fleet(&store, "ETHUSDT", &[old_b]);

    let report = store.apply_fleet_patch(
        vec![config_with_gap(40.0)],
        FleetPatchPlan::new(
            FleetPatchMode::FullReplace,
            Vec::<u64>::new(),
            None::<Vec<String>>,
        ),
    );

    assert_eq!(report.symbols_reset, 2);
    assert_eq!(report.drained_trades, 0);
    assert_eq!(report.changed_ids_requested, 0);
    assert!(store
        .symbols
        .get("BTCUSDT")
        .expect("BTCUSDT state")
        .fleet
        .is_none());
    assert!(store
        .symbols
        .get("ETHUSDT")
        .expect("ETHUSDT state")
        .fleet
        .is_none());
}

#[test]
fn full_replace_drained_trades_update_portfolio_stats() {
    let store = ScreenerStore::default();
    let cfg = config_with_gap(41.0);
    with_symbol_fleet(&store, "BTCUSDT", &[cfg]);
    {
        let mut state = store.symbols.get_mut("BTCUSDT").expect("BTCUSDT state");
        let fleet = state.fleet.as_mut().expect("fleet");
        fleet.push_pending_trade_for_test(FleetTrade {
            config_id: cfg.config_id(),
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: sample_closed_trade(2_000),
        });
    }

    let report = store.apply_fleet_patch(
        vec![cfg],
        FleetPatchPlan::new(
            FleetPatchMode::FullReplace,
            Vec::<u64>::new(),
            None::<Vec<String>>,
        ),
    );

    assert_eq!(report.drained_trades, 1);
    let stats = store.portfolio_candidate_stats_v1(10_000);
    let btc = stats
        .iter()
        .find(|row| row.symbol == "BTCUSDT")
        .expect("candidate stats must include drained reset trade");
    assert_eq!(btc.closed_trades, 1);
    assert_eq!(btc.profitable_trades, 1);
}

#[test]
fn incremental_resets_only_symbols_with_touched_configs() {
    let store = ScreenerStore::default();
    let touched_cfg = config_with_gap(51.0);
    let untouched_cfg = config_with_gap(61.0);
    with_symbol_fleet(&store, "BTCUSDT", &[touched_cfg]);
    with_symbol_fleet(&store, "ETHUSDT", &[untouched_cfg]);

    let report = store.apply_fleet_patch(
        vec![touched_cfg, untouched_cfg],
        FleetPatchPlan::new(
            FleetPatchMode::Incremental,
            [touched_cfg.config_id()],
            None::<Vec<String>>,
        ),
    );

    assert_eq!(report.symbols_reset, 1);
    assert_eq!(report.matched_changed_ids_old, 1);
    assert_eq!(report.matched_changed_ids_new, 1);
    assert_eq!(report.unmatched_changed_ids, 0);
    assert!(store
        .symbols
        .get("BTCUSDT")
        .expect("BTCUSDT state")
        .fleet
        .is_none());
    assert!(store
        .symbols
        .get("ETHUSDT")
        .expect("ETHUSDT state")
        .fleet
        .is_some());
}

#[test]
fn incremental_preserves_unaffected_symbol_state_and_does_not_drain() {
    let store = ScreenerStore::default();
    let touched_cfg = config_with_gap(71.0);
    let untouched_cfg = config_with_gap(81.0);
    with_symbol_fleet(&store, "BTCUSDT", &[touched_cfg]);
    with_symbol_fleet(&store, "ETHUSDT", &[untouched_cfg]);

    let report = store.apply_fleet_patch(
        vec![touched_cfg, untouched_cfg],
        FleetPatchPlan::new(
            FleetPatchMode::Incremental,
            [touched_cfg.config_id()],
            Some(vec!["BTCUSDT".to_string()]),
        ),
    );

    assert_eq!(report.symbols_reset, 1);
    assert_eq!(report.drained_trades, 0);
    assert_eq!(report.scope_symbols_requested, 1);
    assert_eq!(report.scope_symbols_matched, 1);
    assert_eq!(report.unmatched_changed_ids, 0);
    let eth = store.symbols.get("ETHUSDT").expect("ETHUSDT state");
    assert!(eth.fleet.is_some());
    assert_eq!(eth.fleet.as_ref().expect("ETH fleet").len(), 1);
}

#[test]
fn incremental_matches_changed_ids_from_old_or_new_configs() {
    let store = ScreenerStore::default();
    let old_cfg = config_with_gap(91.0);
    with_symbol_fleet(&store, "BTCUSDT", &[old_cfg]);
    with_symbol_fleet(&store, "ETHUSDT", &[old_cfg]);

    let new_cfg = TraderConfig {
        spike_threshold_bps: 92.0,
        ..old_cfg
    };
    let report = store
        .try_apply_fleet_patch(
            vec![new_cfg],
            FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                [new_cfg.config_id()],
                Some(vec!["BTCUSDT".to_string()]),
            ),
        )
        .expect("incremental patch should apply when ids match new configs");

    assert_eq!(report.matched_changed_ids_old, 0);
    assert_eq!(report.matched_changed_ids_new, 1);
    assert_eq!(report.unmatched_changed_ids, 0);
    assert_eq!(report.symbols_reset, 1);
}

#[test]
fn incremental_allows_removing_existing_fleet_config_without_symbol_state() {
    let store = ScreenerStore::default();
    let cfg_a = config_with_gap(141.0);
    let cfg_b = config_with_gap(142.0);

    store.replace_fleet_configs(vec![cfg_a, cfg_b]);
    assert_eq!(store.symbols.len(), 0, "test expects no symbol fleets");

    let report = store
        .try_apply_fleet_patch(
            vec![cfg_b],
            FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                [cfg_a.config_id()],
                None::<Vec<String>>,
            ),
        )
        .expect("removing existing fleet config_id should be accepted");

    assert_eq!(report.matched_changed_ids_old, 1);
    assert_eq!(report.matched_changed_ids_new, 0);
    assert_eq!(report.unmatched_changed_ids, 0);
    assert_eq!(report.symbols_reset, 0);
}

#[test]
fn incremental_rejects_when_changed_ids_match_nothing() {
    let store = ScreenerStore::default();
    let old_cfg = config_with_gap(101.0);
    with_symbol_fleet(&store, "BTCUSDT", &[old_cfg]);

    let err = store
        .try_apply_fleet_patch(
            vec![old_cfg],
            FleetPatchPlan::new(FleetPatchMode::Incremental, [u64::MAX], None::<Vec<String>>),
        )
        .expect_err("incremental patch should reject unmatched changed ids");

    assert!(matches!(
        err,
        FleetPatchApplyError::IncrementalNoMatchedChangedConfigIds {
            changed_ids_requested: 1,
            ..
        }
    ));
    assert!(store
        .symbols
        .get("BTCUSDT")
        .expect("BTCUSDT state")
        .fleet
        .is_some());
}

#[test]
fn incremental_rejects_new_only_ids_without_symbol_scope() {
    let store = ScreenerStore::default();
    let old_cfg = config_with_gap(131.0);
    with_symbol_fleet(&store, "BTCUSDT", &[old_cfg]);
    with_symbol_fleet(&store, "ETHUSDT", &[old_cfg]);
    let new_cfg = TraderConfig {
        spike_threshold_bps: 132.0,
        ..old_cfg
    };

    let err = store
        .try_apply_fleet_patch(
            vec![new_cfg],
            FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                [new_cfg.config_id()],
                None::<Vec<String>>,
            ),
        )
        .expect_err("new-only ids without symbol scope must fail");

    assert!(matches!(
        err,
        FleetPatchApplyError::IncrementalNewConfigIdsRequireSymbolScope { .. }
    ));
}

#[test]
fn incremental_with_mixed_old_and_new_ids_resets_symbol_for_new_only_id() {
    let store = ScreenerStore::default();
    let old_a = config_with_gap(111.0);
    let old_b = config_with_gap(121.0);
    with_symbol_fleet(&store, "BTCUSDT", &[old_a]);
    with_symbol_fleet(&store, "ETHUSDT", &[old_b]);

    let new_b = TraderConfig {
        spike_threshold_bps: 122.0,
        ..old_b
    };
    let report = store
        .try_apply_fleet_patch(
            vec![old_a, new_b],
            FleetPatchPlan::new(
                FleetPatchMode::Incremental,
                [old_a.config_id(), new_b.config_id()],
                Some(vec!["ETHUSDT".to_string()]),
            ),
        )
        .expect("mixed old/new incremental patch should apply");

    assert_eq!(report.matched_changed_ids_old, 1);
    assert_eq!(report.matched_changed_ids_new, 2);
    assert_eq!(report.unmatched_changed_ids, 0);
    assert_eq!(report.scope_symbols_requested, 1);
    assert_eq!(report.scope_symbols_matched, 1);
    assert_eq!(report.symbols_reset, 1);
    assert!(
        store
            .symbols
            .get("ETHUSDT")
            .expect("ETHUSDT state")
            .fleet
            .is_none(),
        "new-only changed id must reset in-scope symbol fleet"
    );
}

#[test]
fn fleet_patch_rejects_invalid_trader_config() {
    let store = ScreenerStore::default();
    let invalid = TraderConfig {
        stop_loss_bps: 0.0,
        ..TraderConfig::default()
    };
    let err = store
        .try_apply_fleet_patch(
            vec![invalid],
            FleetPatchPlan::new(
                FleetPatchMode::FullReplace,
                Vec::<u64>::new(),
                None::<Vec<String>>,
            ),
        )
        .expect_err("invalid config must be rejected");
    assert!(matches!(
        err,
        FleetPatchApplyError::InvalidConfig {
            index: 0,
            field: "stop_loss_bps",
            ..
        }
    ));
}

#[test]
fn fleet_patch_rejects_duplicate_config_ids() {
    let store = ScreenerStore::default();
    let cfg = TraderConfig::default();
    let err = store
        .try_apply_fleet_patch(
            vec![cfg, cfg],
            FleetPatchPlan::new(
                FleetPatchMode::FullReplace,
                Vec::<u64>::new(),
                None::<Vec<String>>,
            ),
        )
        .expect_err("duplicate configs must be rejected");
    assert!(matches!(
        err,
        FleetPatchApplyError::DuplicateConfigId { .. }
    ));
}

#[test]
fn portfolio_candidate_age_uses_first_tick_timestamp() {
    let store = ScreenerStore::default();
    let ts_ns = 1_700_000_000_000_000_000_i64;
    let first_tick_ms = ts_ns / 1_000_000;

    store.update("BTCUSDT", "binance", 100.0, 100.1, ts_ns, ts_ns);
    store.update(
        "BTCUSDT",
        "gate",
        100.0,
        100.1,
        ts_ns + 1_000_000,
        ts_ns + 1_000_000,
    );

    let state = store.symbols.get("BTCUSDT").expect("BTCUSDT state");
    assert_eq!(state.first_tick_ms, Some(first_tick_ms));

    store.observe_closed_trade_for_portfolio("BTCUSDT", 0.10, false, first_tick_ms + 1_000);
    let stats = store.portfolio_candidate_stats_v1(first_tick_ms + 6 * 60_000);
    let btc = stats
        .iter()
        .find(|row| row.symbol == "BTCUSDT")
        .expect("candidate stats for BTCUSDT");
    assert!(btc.age_minutes_from_first_tick >= 6);
}

#[test]
fn portfolio_candidate_stats_accumulate_full_history() {
    let store = ScreenerStore::default();
    store.symbols.insert(
        "BTCUSDT".to_string(),
        SymbolState {
            first_tick_ms: Some(0),
            ..SymbolState::default()
        },
    );

    store.observe_closed_trade_for_portfolio("BTCUSDT", 0.20, false, 10_000);
    store.observe_closed_trade_for_portfolio("BTCUSDT", -0.10, true, 20_000);
    store.observe_closed_trade_for_portfolio("BTCUSDT", 0.0, false, 30_000);

    let stats = store.portfolio_candidate_stats_v1(600_000);
    let btc = stats
        .iter()
        .find(|row| row.symbol == "BTCUSDT")
        .expect("candidate stats for BTCUSDT");
    assert_eq!(btc.closed_trades, 3);
    assert_eq!(btc.profitable_trades, 1);
    assert_eq!(btc.losing_trades, 1);
    assert!((btc.avg_pnl_pct - (0.10 / 3.0)).abs() < 1e-12);
}

#[test]
fn portfolio_candidate_history_restore_bootstraps_stats_without_live_ticks() {
    let store = ScreenerStore::default();
    store.restore_portfolio_candidate_history_v1_from_db_rows(&[
        crate::infrastructure::db::PortfolioCandidateHistoryRecordV1 {
            symbol: "BTCUSDT".to_string(),
            closed_trades: 8,
            profitable_trades: 4,
            losing_trades: 1,
            pnl_sum_pct: 0.24,
            first_trade_ts_ms: Some(0),
        },
    ]);

    let stats = store.portfolio_candidate_stats_v1(600_000);
    let btc = stats
        .iter()
        .find(|row| row.symbol == "BTCUSDT")
        .expect("candidate stats for BTCUSDT");
    assert_eq!(btc.closed_trades, 8);
    assert_eq!(btc.profitable_trades, 4);
    assert_eq!(btc.losing_trades, 1);
    assert!((btc.avg_pnl_pct - 0.03).abs() < 1e-12);
    assert_eq!(btc.age_minutes_from_first_tick, 10);

    store.maybe_rebalance_portfolios(600_000);
    let assignment = store.portfolio_assignment_v1();
    let assigned = assignment.values().any(|state| {
        state
            .active_symbols
            .iter()
            .any(|symbol| symbol == "BTCUSDT")
    });
    assert!(assigned, "restored symbol must be eligible for assignment");
}

#[test]
fn setting_run_id_resets_candidate_history_when_run_changes() {
    let store = ScreenerStore::default();
    store.observe_closed_trade_for_portfolio("BTCUSDT", 0.25, false, 10_000);
    assert_eq!(store.portfolio_candidate_stats_v1(60_000).len(), 1);

    store.set_run_id(Some("run-a".to_string()));

    assert!(
        store.portfolio_candidate_stats_v1(60_000).is_empty(),
        "candidate history must reset when switching into a specific run scope"
    );
}

#[test]
fn drained_trades_ignore_non_active_run_for_candidate_math() {
    let store = ScreenerStore::default();
    store.set_run_id(Some("forward-1".to_string()));

    let mut trade = sample_closed_trade(50_000);
    trade.entry_ts_ms = 10_000;
    store.handle_drained_fleet_trades(vec![FleetTrade {
        config_id: TraderConfig::default().config_id(),
        symbol: "BTCUSDT".to_string(),
        run_id: Some("forward-other".to_string()),
        trade,
    }]);

    assert!(
        store.portfolio_candidate_stats_v1(120_000).is_empty(),
        "candidate stats must not ingest trades from a different run_id"
    );
}

#[test]
fn drained_trades_collapse_same_symbol_timestamp_for_candidate_math() {
    let store = ScreenerStore::default();
    let mut first = sample_closed_trade(80_000);
    first.entry_ts_ms = 20_000;
    first.pnl_pct = 0.4;
    first.exit_reason = ExitReason::TrailingTake;

    let mut second = sample_closed_trade(80_000);
    second.entry_ts_ms = 20_000;
    second.pnl_pct = -0.2;
    second.exit_reason = ExitReason::StopLoss;

    store.handle_drained_fleet_trades(vec![
        FleetTrade {
            config_id: 11,
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: first,
        },
        FleetTrade {
            config_id: 12,
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: second,
        },
    ]);

    let stats = store.portfolio_candidate_stats_v1(300_000);
    let btc = stats
        .iter()
        .find(|row| row.symbol == "BTCUSDT")
        .expect("candidate stats for BTCUSDT");
    assert_eq!(
        btc.closed_trades, 1,
        "same-symbol same-ts closes should contribute one candidate event"
    );
    assert!((btc.avg_pnl_pct - 0.1).abs() < 1e-12);
}

#[test]
fn drained_trade_candidate_age_uses_entry_timestamp_basis() {
    let store = ScreenerStore::default();
    let mut trade = sample_closed_trade(300_000);
    trade.entry_ts_ms = 0;
    trade.pnl_pct = 0.3;

    store.handle_drained_fleet_trades(vec![FleetTrade {
        config_id: TraderConfig::default().config_id(),
        symbol: "ETHUSDT".to_string(),
        run_id: None,
        trade,
    }]);

    let stats = store.portfolio_candidate_stats_v1(360_000);
    let eth = stats
        .iter()
        .find(|row| row.symbol == "ETHUSDT")
        .expect("candidate stats for ETHUSDT");
    assert_eq!(
        eth.age_minutes_from_first_tick, 6,
        "candidate age basis must stay consistent with restore semantics"
    );
}

#[test]
fn portfolio_rebalance_cadence_and_no_overlap_active_symbols() {
    let store = ScreenerStore::default();
    for symbol in ["AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EEEUSDT"] {
        store.symbols.insert(
            symbol.to_string(),
            SymbolState {
                first_tick_ms: Some(0),
                ..SymbolState::default()
            },
        );
        for idx in 0..8 {
            store.observe_closed_trade_for_portfolio(symbol, 0.10, false, 1_000 + idx * 1_000);
        }
        for idx in 0..2 {
            store.observe_closed_trade_for_portfolio(symbol, -0.02, true, 20_000 + idx * 1_000);
        }
    }

    store.maybe_rebalance_portfolios(600_000);
    assert_eq!(store.portfolio_last_rebalance_ms(), Some(600_000));

    let first = store.portfolio_assignment_v1();
    let a_first = first.get("A").expect("portfolio A");
    let b_first = first.get("B").expect("portfolio B");
    assert!(
        !a_first.active_symbols.is_empty(),
        "portfolio A must receive active symbols when enough candidates exist"
    );
    assert!(
        !b_first.active_symbols.is_empty(),
        "portfolio B must receive active symbols when enough candidates exist"
    );

    let overlap_first: Vec<&String> = a_first
        .active_symbols
        .iter()
        .filter(|s| b_first.active_symbols.contains(*s))
        .collect();
    assert!(
        overlap_first.is_empty(),
        "portfolio active symbols must not overlap"
    );

    store.maybe_rebalance_portfolios(650_000);
    assert_eq!(
        store.portfolio_last_rebalance_ms(),
        Some(600_000),
        "rebalance must not run before 2 minutes"
    );
}

#[test]
fn portfolio_rebalance_cadence_skips_candidate_build_when_not_due() {
    let store = ScreenerStore::default();
    store.symbols.insert(
        "BTCUSDT".to_string(),
        SymbolState {
            first_tick_ms: Some(0),
            ..SymbolState::default()
        },
    );
    for idx in 0..8 {
        store.observe_closed_trade_for_portfolio("BTCUSDT", 0.1, false, 1_000 + idx * 1_000);
    }

    store.maybe_rebalance_portfolios(600_000);
    assert_eq!(store.portfolio_candidate_build_count_v1(), 1);

    store.maybe_rebalance_portfolios(650_000);
    assert_eq!(
        store.portfolio_candidate_build_count_v1(),
        1,
        "candidate stats build should be skipped when cadence gate rejects rebalance"
    );
}

#[test]
fn portfolio_runtime_restore_from_db_rows_sets_assignment_and_guards() {
    let store = ScreenerStore::default();
    store.restore_portfolio_runtime_v1_from_db_rows(
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 700_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec!["SOLUSDT".to_string()],
                active_symbols: vec!["SOLUSDT".to_string()],
                updated_at_ms: 700_000,
            },
        ],
        &[crate::infrastructure::db::PortfolioGuardRecordV1 {
            symbol: "BTCUSDT".to_string(),
            streak_count: 2,
            first_streak_ts_ms: Some(650_000),
            cooldown_until_ms: Some(900_000),
            updated_at_ms: 800_000,
        }],
    );

    let assignment = store.portfolio_assignment_v1();
    let a = assignment.get("A").expect("portfolio A");
    let b = assignment.get("B").expect("portfolio B");
    assert_eq!(a.active_symbols, vec!["BTCUSDT".to_string()]);
    assert_eq!(b.shortlist, vec!["SOLUSDT".to_string()]);

    let guards = store.portfolio_guard_states_v1();
    assert_eq!(guards.len(), 1);
    assert_eq!(guards[0].0, "BTCUSDT");
    assert_eq!(guards[0].1.streak_count, 2);
    assert_eq!(store.portfolio_last_rebalance_ms(), Some(800_000));
}

#[test]
fn screener_portfolio_runtime_supports_dynamic_portfolio_ids() {
    let store = ScreenerStore::default();
    store.set_portfolio_ids_v1(vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    assert_eq!(
        store.portfolio_ids_v1(),
        vec!["A".to_string(), "B".to_string(), "C".to_string()]
    );

    for symbol in [
        "AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT", "EEEUSDT", "FFFUSDT",
    ] {
        store.symbols.insert(
            symbol.to_string(),
            SymbolState {
                first_tick_ms: Some(0),
                ..SymbolState::default()
            },
        );
        for idx in 0..8 {
            store.observe_closed_trade_for_portfolio(symbol, 0.10, false, 1_000 + idx * 1_000);
        }
    }

    store.maybe_rebalance_portfolios(600_000);
    let assignment = store.portfolio_assignment_v1();
    assert!(assignment.contains_key("A"));
    assert!(assignment.contains_key("B"));
    assert!(assignment.contains_key("C"));
}

#[test]
fn portfolio_paper_money_updates_only_for_active_symbols() {
    let store = ScreenerStore::default();
    store.restore_portfolio_runtime_v1_from_db_rows(
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["BTCUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 600_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec!["ETHUSDT".to_string()],
                active_symbols: vec!["ETHUSDT".to_string()],
                updated_at_ms: 600_000,
            },
        ],
        &[],
    );

    let before = store.portfolio_paper_states_v1();
    let a_before = before.get("A").expect("portfolio A paper state");
    let b_before = before.get("B").expect("portfolio B paper state");
    assert!((a_before.equity_usd - 10_000.0).abs() < 1e-9);
    assert!((b_before.equity_usd - 10_000.0).abs() < 1e-9);
    assert_eq!(a_before.closed_trades, 0);
    assert_eq!(b_before.closed_trades, 0);

    store.observe_closed_trade_for_portfolio("BTCUSDT", 1.0, false, 700_000);
    store.observe_closed_trade_for_portfolio("ETHUSDT", -2.0, true, 710_000);
    store.observe_closed_trade_for_portfolio("XRPUSDT", 5.0, false, 720_000);

    let after = store.portfolio_paper_states_v1();
    let a_after = after.get("A").expect("portfolio A paper state");
    let b_after = after.get("B").expect("portfolio B paper state");

    assert!((a_after.realized_pnl_usd - 1.0).abs() < 1e-9);
    assert!((a_after.equity_usd - 10_001.0).abs() < 1e-9);
    assert_eq!(a_after.closed_trades, 1);
    assert_eq!(a_after.profitable_trades, 1);
    assert_eq!(a_after.losing_trades, 0);

    assert!((b_after.realized_pnl_usd + 2.0).abs() < 1e-9);
    assert!((b_after.equity_usd - 9_998.0).abs() < 1e-9);
    assert_eq!(b_after.closed_trades, 1);
    assert_eq!(b_after.profitable_trades, 0);
    assert_eq!(b_after.losing_trades, 1);
}

#[test]
fn portfolio_paper_money_resets_with_new_portfolio_ids() {
    let store = ScreenerStore::default();
    store.restore_portfolio_runtime_v1_from_db_rows(
        &[crate::infrastructure::db::PortfolioStateRecordV1 {
            portfolio_id: "A".to_string(),
            shortlist: vec!["BTCUSDT".to_string()],
            active_symbols: vec!["BTCUSDT".to_string()],
            updated_at_ms: 600_000,
        }],
        &[],
    );
    store.observe_closed_trade_for_portfolio("BTCUSDT", 1.0, false, 700_000);
    let before = store.portfolio_paper_states_v1();
    assert!((before.get("A").expect("portfolio A").equity_usd - 10_001.0).abs() < 1e-9);

    store.set_portfolio_ids_v1(vec!["X".to_string(), "Y".to_string()]);
    let after = store.portfolio_paper_states_v1();
    assert_eq!(after.len(), 2);
    assert!(!after.contains_key("A"));
    assert!(after.contains_key("X"));
    assert!(after.contains_key("Y"));
    for state in after.values() {
        assert!((state.equity_usd - 10_000.0).abs() < 1e-9);
        assert!((state.realized_pnl_usd - 0.0).abs() < 1e-9);
        assert_eq!(state.closed_trades, 0);
    }
}

#[test]
fn portfolio_paper_money_attributes_to_owner_at_entry_not_close() {
    let store = ScreenerStore::default();
    store.restore_portfolio_runtime_v1_from_db_rows(
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["BTCUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 1_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec![],
                active_symbols: vec![],
                updated_at_ms: 1_000,
            },
        ],
        &[],
    );

    // Simulate reassignment after entry: close should still be attributed to A.
    store.restore_portfolio_runtime_v1_from_db_rows(
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec![],
                active_symbols: vec![],
                updated_at_ms: 2_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec!["BTCUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 2_000,
            },
        ],
        &[],
    );

    let mut trade = sample_closed_trade(2_500);
    trade.entry_ts_ms = 1_500;
    store.handle_drained_fleet_trades(vec![FleetTrade {
        config_id: TraderConfig::default().config_id(),
        symbol: "BTCUSDT".to_string(),
        run_id: None,
        trade,
    }]);

    let paper = store.portfolio_paper_states_v1();
    let a = paper.get("A").expect("portfolio A");
    let b = paper.get("B").expect("portfolio B");
    assert_eq!(a.closed_trades, 1);
    assert_eq!(b.closed_trades, 0);
}

#[test]
fn portfolio_runtime_restore_with_paper_rows_restores_totals() {
    let store = ScreenerStore::default();
    store.restore_portfolio_runtime_v1_from_db_rows_with_paper(
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["BTCUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 1_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec![],
                active_symbols: vec![],
                updated_at_ms: 1_000,
            },
        ],
        &[],
        &[crate::infrastructure::db::PortfolioPaperStateRecordV1 {
            portfolio_id: "A".to_string(),
            equity_usd: 10_123.0,
            realized_pnl_usd: 123.0,
            closed_trades: 9,
            profitable_trades: 6,
            losing_trades: 3,
            last_trade_ts_ms: Some(950),
            updated_at_ms: 1_000,
        }],
    );

    let paper = store.portfolio_paper_states_v1();
    let a = paper.get("A").expect("portfolio A");
    let b = paper.get("B").expect("portfolio B");
    assert!((a.equity_usd - 10_123.0).abs() < 1e-9);
    assert!((a.realized_pnl_usd - 123.0).abs() < 1e-9);
    assert_eq!(a.closed_trades, 9);
    assert_eq!(a.profitable_trades, 6);
    assert_eq!(a.losing_trades, 3);
    assert_eq!(a.last_trade_ts_ms, Some(950));
    assert!((b.equity_usd - 10_000.0).abs() < 1e-9);
    assert_eq!(b.closed_trades, 0);
}

#[test]
fn portfolio_paper_money_falls_back_to_active_owner_when_entry_snapshot_missing() {
    let store = ScreenerStore::default();
    store.restore_portfolio_runtime_v1_from_db_rows(
        &[
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec![],
                active_symbols: vec![],
                updated_at_ms: 2_000,
            },
            crate::infrastructure::db::PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec!["BTCUSDT".to_string()],
                active_symbols: vec!["BTCUSDT".to_string()],
                updated_at_ms: 2_000,
            },
        ],
        &[],
    );

    let mut trade = sample_closed_trade(2_500);
    trade.entry_ts_ms = 1_000; // older than only known assignment snapshot
    store.handle_drained_fleet_trades(vec![FleetTrade {
        config_id: TraderConfig::default().config_id(),
        symbol: "BTCUSDT".to_string(),
        run_id: None,
        trade,
    }]);

    let paper = store.portfolio_paper_states_v1();
    let a = paper.get("A").expect("portfolio A");
    let b = paper.get("B").expect("portfolio B");
    assert_eq!(a.closed_trades, 0);
    assert_eq!(b.closed_trades, 1);
}

#[test]
fn drained_trades_apply_guard_logic_in_chronological_order() {
    let store = ScreenerStore::default();
    let config_id = TraderConfig::default().config_id();

    let mut loss_one = sample_closed_trade(2_200);
    loss_one.pnl_pct = -0.2;
    loss_one.exit_reason = ExitReason::StopLoss;

    let mut loss_two = sample_closed_trade(2_300);
    loss_two.pnl_pct = -0.2;
    loss_two.exit_reason = ExitReason::StopLoss;

    let mut win_earlier = sample_closed_trade(2_000);
    win_earlier.pnl_pct = 0.3;
    win_earlier.exit_reason = ExitReason::TrailingTake;

    // Deliberately out-of-order input (wins/losses interleaved by config traversal order).
    store.handle_drained_fleet_trades(vec![
        FleetTrade {
            config_id,
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: loss_one,
        },
        FleetTrade {
            config_id,
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: loss_two,
        },
        FleetTrade {
            config_id,
            symbol: "BTCUSDT".to_string(),
            run_id: None,
            trade: win_earlier,
        },
    ]);

    let guard_state = store.portfolio_guard_states_v1();
    let btc_guard = guard_state
        .iter()
        .find(|(symbol, _)| symbol == "BTCUSDT")
        .expect("BTCUSDT guard");
    assert_eq!(
        btc_guard.1.streak_count, 2,
        "guard streak must reflect chronological processing (win first, then two stop-losses)"
    );
}

#[test]
fn stop_loss_streak_triggers_cooldown_exclusion_until_expiry() {
    let store = ScreenerStore::default();
    store.symbols.insert(
        "XRPUSDT".to_string(),
        SymbolState {
            first_tick_ms: Some(0),
            ..SymbolState::default()
        },
    );

    for idx in 0..12 {
        store.observe_closed_trade_for_portfolio("XRPUSDT", 0.20, false, 1_000 + idx * 1_000);
    }

    store.maybe_rebalance_portfolios(600_000);
    let baseline = store.portfolio_assignment_v1();
    let baseline_has_symbol = baseline.values().any(|state| {
        state
            .active_symbols
            .iter()
            .any(|symbol| symbol == "XRPUSDT")
    });
    assert!(
        baseline_has_symbol,
        "symbol must be assigned before cooldown trigger"
    );

    for ts in [800_000_i64, 820_000, 840_000, 860_000, 880_000] {
        store.observe_closed_trade_for_portfolio("XRPUSDT", -0.05, true, ts);
    }

    store.maybe_rebalance_portfolios(900_000);
    let during = store.portfolio_assignment_v1();
    let during_has_symbol = during.values().any(|state| {
        state
            .active_symbols
            .iter()
            .any(|symbol| symbol == "XRPUSDT")
    });
    assert!(
        !during_has_symbol,
        "symbol must be excluded while cooldown active"
    );

    store.maybe_rebalance_portfolios(1_200_001);
    let after = store.portfolio_assignment_v1();
    let after_has_symbol = after.values().any(|state| {
        state
            .active_symbols
            .iter()
            .any(|symbol| symbol == "XRPUSDT")
    });
    assert!(
        after_has_symbol,
        "symbol should be eligible again after cooldown expiry"
    );
}
