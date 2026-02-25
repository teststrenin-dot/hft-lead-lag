use super::{
    shadow_fleet::{FleetTrade, ShadowFleet},
    FleetPatchApplyError, FleetPatchMode, FleetPatchPlan, ScreenerStore, SymbolState,
    TraderConfig,
};
use crate::domain::screener::shadow_trader::{ClosedTrade, Direction};

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
        exit_reason: "trailing_take",
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
