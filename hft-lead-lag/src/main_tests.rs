
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn write_temp_config(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hft-lead-lag-main-{name}-{}.toml",
            std::process::id()
        ));
        fs::write(&path, content).expect("write temp config");
        path
    }

    #[test]
    fn reconcile_volume_symbols_uses_fallback_when_binance_missing() {
        let (binance, gate, outcome) =
            reconcile_volume_symbols(Vec::new(), vec!["XRPUSDT".to_string()]);
        assert_eq!(outcome, SymbolReconcileOutcome::BinanceMissing);
        assert_eq!(binance, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(gate, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn reconcile_volume_symbols_uses_fallback_when_gate_missing() {
        let (binance, gate, outcome) =
            reconcile_volume_symbols(vec!["XRPUSDT".to_string()], Vec::new());
        assert_eq!(outcome, SymbolReconcileOutcome::GateMissing);
        assert_eq!(binance, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(gate, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn reconcile_volume_symbols_keeps_lists_when_both_present() {
        let (binance, gate, outcome) =
            reconcile_volume_symbols(vec!["XRPUSDT".to_string()], vec!["XRPUSDT".to_string()]);
        assert_eq!(outcome, SymbolReconcileOutcome::Ok);
        assert_eq!(binance, vec!["XRPUSDT".to_string()]);
        assert_eq!(gate, vec!["XRPUSDT".to_string()]);
    }

    #[test]
    fn reconcile_volume_symbols_uses_fallback_when_both_missing() {
        let (binance, gate, outcome) = reconcile_volume_symbols(Vec::new(), Vec::new());
        assert_eq!(outcome, SymbolReconcileOutcome::BothMissing);
        assert_eq!(binance, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(gate, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn event_loop_metrics_returns_no_data_when_empty() {
        let mut metrics = EventLoopMetrics::new();
        assert_eq!(metrics.drift_stats_string_and_reset(), "no_data");
    }

    #[test]
    fn event_loop_metrics_formats_stats_and_clears_samples() {
        let mut metrics = EventLoopMetrics::new();
        metrics.record_tick_drift(130, 100_000_000);
        metrics.record_tick_drift(120, 110_000_000);
        metrics.record_tick_drift(130, 110_000_000);

        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=3 avg=20ms p50=20ms p95=30ms p99=30ms max=30ms"
        );
        assert_eq!(metrics.drift_stats_string_and_reset(), "no_data");
    }

    #[test]
    fn event_loop_metrics_snapshot_rolls_interval_count() {
        let mut metrics = EventLoopMetrics::new();
        assert_eq!(metrics.snapshot_and_roll_status(10), 10);
        assert_eq!(metrics.snapshot_and_roll_status(16), 6);
        assert_eq!(metrics.snapshot_and_roll_status(8), 0);
    }

    #[tokio::test]
    async fn event_loop_state_starts_clean() {
        let mut state = EventLoopState::new();
        assert_eq!(state.ticker_count, 0);
        assert_eq!(state.signal_count, 0);
        assert!(state.latest_bn.is_empty());
        assert!(state.latest_gt.is_empty());
        assert_eq!(state.metrics.drift_stats_string_and_reset(), "no_data");
    }

    #[test]
    fn event_loop_state_now_ms_is_positive() {
        assert!(EventLoopState::now_ms() > 0);
    }

    #[tokio::test]
    async fn event_loop_state_process_exchange_result_updates_binance_map() {
        let mut state = EventLoopState::new();
        let screener = ScreenerStore::default();
        let (ws_tx, _ws_rx) = tokio::sync::broadcast::channel(8);

        let updated_symbols = state
            .process_exchange_result(
                ExchangeSide::Binance,
                Ok(test_ticker("BTCUSDT", 100_000_000)),
                vec![test_ticker("ETHUSDT", 110_000_000)],
                &screener,
                &ws_tx,
            )
            .expect("exchange result should parse");

        assert_eq!(
            updated_symbols,
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
        assert_eq!(state.latest_bn.len(), 2);
        assert!(state.latest_gt.is_empty());
        assert_eq!(state.ticker_count, 2);
    }

    #[tokio::test]
    async fn event_loop_state_process_exchange_result_propagates_error() {
        let mut state = EventLoopState::new();
        let screener = ScreenerStore::default();
        let (ws_tx, _ws_rx) = tokio::sync::broadcast::channel(8);

        let result = state.process_exchange_result(
            ExchangeSide::Gate,
            Err(hft_lead_lag::domain::ExchangeError::Timeout(
                "test".to_string(),
            )),
            Vec::new(),
            &screener,
            &ws_tx,
        );

        assert!(matches!(
            result,
            Err(hft_lead_lag::domain::ExchangeError::Timeout(msg)) if msg == "test"
        ));
        assert_eq!(state.ticker_count, 0);
        assert!(state.latest_bn.is_empty());
        assert!(state.latest_gt.is_empty());
    }

    fn test_ticker(symbol: &str, exchange_ts_ns: i64) -> hft_lead_lag::domain::BookTicker {
        hft_lead_lag::domain::BookTicker::new(
            bytes::Bytes::copy_from_slice(symbol.as_bytes()),
            100,
            101,
            1,
            1,
            exchange_ts_ns,
            exchange_ts_ns + 1,
        )
    }

    #[test]
    fn rebuild_latest_map_preserves_old_entries() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("OLD".to_string(), test_ticker("OLD", 1));

        rebuild_latest_map(&mut latest, test_ticker("BTCUSDT", 10), Vec::new());

        assert!(latest.contains_key("OLD"));
        assert!(latest.contains_key("BTCUSDT"));
    }

    #[test]
    fn rebuild_latest_map_keeps_latest_ticker_per_symbol() {
        let mut latest = std::collections::HashMap::new();
        rebuild_latest_map(
            &mut latest,
            test_ticker("BTCUSDT", 10),
            vec![test_ticker("BTCUSDT", 20), test_ticker("ETHUSDT", 30)],
        );

        assert_eq!(latest.len(), 2);
        assert_eq!(latest["BTCUSDT"].exchange_ts_ns, 20);
        assert_eq!(latest["ETHUSDT"].exchange_ts_ns, 30);
    }

    #[test]
    fn process_exchange_batch_preserves_cached_symbols_and_ingests_only_updates() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("OLD".to_string(), test_ticker("OLD", 1));

        let mut ticker_count = 0usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;
        let mut ctx = BatchIngestContext {
            exchange: "binance",
            ticker_count: &mut ticker_count,
            metrics: &mut metrics,
            now_ms: &now_ms,
            screener: &screener,
            ws_tx: &ws_tx,
        };

        process_exchange_batch(
            &mut latest,
            test_ticker("BTCUSDT", 100_000_000),
            Vec::new(),
            &mut ctx,
        );

        assert!(
            latest.contains_key("OLD"),
            "latest cache should preserve non-updated symbols"
        );
        assert!(latest.contains_key("BTCUSDT"));
        assert_eq!(ticker_count, 1);

        let event = ws_rx.try_recv().expect("ws event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert!(matches!(
            ws_rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn updated_symbols_from_batch_deduplicates_and_sorts() {
        let symbols = updated_symbols_from_batch(
            &test_ticker("BTCUSDT", 10),
            &[
                test_ticker("ETHUSDT", 20),
                test_ticker("BTCUSDT", 30),
                test_ticker("ADAUSDT", 40),
            ],
        );
        assert_eq!(
            symbols,
            vec![
                "ADAUSDT".to_string(),
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string()
            ]
        );
    }

    #[test]
    fn select_runtime_symbols_uses_common_when_present() {
        let common = vec!["XRPUSDT".to_string(), "ADAUSDT".to_string()];
        let (strategy, screener, used_fallback) = select_runtime_symbols(&common);

        assert!(!used_fallback);
        assert_eq!(strategy, common);
        assert_eq!(screener, common);
    }

    #[test]
    fn select_runtime_symbols_uses_fallback_when_common_empty() {
        let common: Vec<String> = Vec::new();
        let (strategy, screener, used_fallback) = select_runtime_symbols(&common);

        assert!(used_fallback);
        assert_eq!(strategy, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(screener, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn compute_common_symbols_filters_blacklist_and_sorts() {
        let binance_symbols = vec![
            "XRPUSDT".to_string(),
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
        ];
        let gate_symbols = vec![
            "ETHUSDT".to_string(),
            "XRPUSDT".to_string(),
            "ADAUSDT".to_string(),
        ];
        let blacklist: std::collections::HashSet<&str> = ["ETHUSDT"].into_iter().collect();

        let common = compute_common_symbols(&binance_symbols, &gate_symbols, &blacklist);
        assert_eq!(common, vec!["XRPUSDT".to_string()]);
    }

    #[test]
    fn compute_common_symbols_returns_empty_when_no_overlap() {
        let binance_symbols = vec!["BTCUSDT".to_string()];
        let gate_symbols = vec!["ETHUSDT".to_string()];
        let blacklist: std::collections::HashSet<&str> = std::collections::HashSet::new();

        let common = compute_common_symbols(&binance_symbols, &gate_symbols, &blacklist);
        assert!(common.is_empty());
    }

    #[test]
    fn strategy_ticks_in_order_skips_missing_symbols() {
        let strategy_symbols = vec!["BTCUSDT", "ETHUSDT"];
        let mut latest = std::collections::HashMap::new();
        latest.insert("BTCUSDT".to_string(), test_ticker("BTCUSDT", 10));

        let ticks: Vec<i64> = strategy_ticks_in_order(&strategy_symbols, &latest)
            .map(|t| t.exchange_ts_ns)
            .collect();
        assert_eq!(ticks, vec![10]);
    }

    #[test]
    fn strategy_ticks_in_order_preserves_strategy_order() {
        let strategy_symbols = vec!["ETHUSDT", "BTCUSDT"];
        let mut latest = std::collections::HashMap::new();
        latest.insert("BTCUSDT".to_string(), test_ticker("BTCUSDT", 10));
        latest.insert("ETHUSDT".to_string(), test_ticker("ETHUSDT", 20));

        let symbols: Vec<String> = strategy_ticks_in_order(&strategy_symbols, &latest)
            .map(|t| String::from_utf8_lossy(&t.symbol).to_string())
            .collect();
        assert_eq!(symbols, vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);
    }

    #[test]
    fn ingest_latest_batch_is_noop_for_empty_map() {
        let latest = std::collections::HashMap::new();
        let mut ticker_count = 3usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;
        let mut ctx = BatchIngestContext {
            exchange: "binance",
            ticker_count: &mut ticker_count,
            metrics: &mut metrics,
            now_ms: &now_ms,
            screener: &screener,
            ws_tx: &ws_tx,
        };

        ingest_latest_batch(&latest, &mut ctx);

        assert_eq!(ticker_count, 3);
        assert_eq!(metrics.drift_stats_string_and_reset(), "no_data");
        assert!(screener.rows_sorted().is_empty());
        assert!(matches!(
            ws_rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn ingest_latest_batch_updates_counter_metrics_screener_and_ws() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("BTCUSDT".to_string(), test_ticker("BTCUSDT", 100_000_000));
        let mut ticker_count = 0usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;
        let mut ctx = BatchIngestContext {
            exchange: "gate",
            ticker_count: &mut ticker_count,
            metrics: &mut metrics,
            now_ms: &now_ms,
            screener: &screener,
            ws_tx: &ws_tx,
        };

        ingest_latest_batch(&latest, &mut ctx);

        assert_eq!(ticker_count, 1);
        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=1 avg=30ms p50=30ms p95=30ms p99=30ms max=30ms"
        );

        let event = ws_rx.try_recv().expect("market data event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.exchange, "gate");
        assert_eq!(event.timestamp_ns, 100_000_000);

        let rows = screener.rows_sorted();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "BTCUSDT");
        assert_eq!(rows[0].leader_exchange, "gate");
    }

    #[test]
    fn process_exchange_batch_rebuilds_and_ingests_latest_state() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("OLD".to_string(), test_ticker("OLD", 1));
        let mut ticker_count = 5usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 150i64;
        let mut ctx = BatchIngestContext {
            exchange: "binance",
            ticker_count: &mut ticker_count,
            metrics: &mut metrics,
            now_ms: &now_ms,
            screener: &screener,
            ws_tx: &ws_tx,
        };

        process_exchange_batch(
            &mut latest,
            test_ticker("BTCUSDT", 100_000_000),
            vec![
                test_ticker("ETHUSDT", 110_000_000),
                test_ticker("BTCUSDT", 120_000_000),
            ],
            &mut ctx,
        );

        assert!(latest.contains_key("OLD"));
        assert_eq!(latest.len(), 3);
        assert_eq!(latest["BTCUSDT"].exchange_ts_ns, 120_000_000);
        assert_eq!(ticker_count, 7);
        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=2 avg=35ms p50=40ms p95=40ms p99=40ms max=40ms"
        );

        let mut events = [
            ws_rx.try_recv().expect("first ws event"),
            ws_rx.try_recv().expect("second ws event"),
        ];
        events.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        assert_eq!(events[0].symbol, "BTCUSDT");
        assert_eq!(events[0].exchange, "binance");
        assert_eq!(events[0].timestamp_ns, 120_000_000);
        assert_eq!(events[1].symbol, "ETHUSDT");
        assert_eq!(events[1].exchange, "binance");
        assert_eq!(events[1].timestamp_ns, 110_000_000);
        assert!(matches!(
            ws_rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let rows = screener.rows_sorted();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].leader_exchange, "binance");
        assert_eq!(rows[1].leader_exchange, "binance");
    }

    #[test]
    fn process_exchange_batch_with_single_tick_updates_once() {
        let mut latest = std::collections::HashMap::new();
        let mut ticker_count = 0usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;
        let mut ctx = BatchIngestContext {
            exchange: "gate",
            ticker_count: &mut ticker_count,
            metrics: &mut metrics,
            now_ms: &now_ms,
            screener: &screener,
            ws_tx: &ws_tx,
        };

        process_exchange_batch(
            &mut latest,
            test_ticker("BTCUSDT", 100_000_000),
            Vec::new(),
            &mut ctx,
        );

        assert_eq!(latest.len(), 1);
        assert_eq!(ticker_count, 1);
        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=1 avg=30ms p50=30ms p95=30ms p99=30ms max=30ms"
        );
        let event = ws_rx.try_recv().expect("ws event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.exchange, "gate");
    }

    #[test]
    fn gate_subscribe_delay_applies_after_timeout() {
        assert!(should_delay_after_gate_subscribe_attempt(
            GateSubscribeAttempt::Timeout
        ));
    }

    #[test]
    fn gate_subscribe_delay_applies_after_success_and_error() {
        assert!(should_delay_after_gate_subscribe_attempt(
            GateSubscribeAttempt::Success
        ));
        assert!(should_delay_after_gate_subscribe_attempt(
            GateSubscribeAttempt::Error
        ));
    }

    #[test]
    fn exchange_side_marks_health_on_success() {
        let health = HealthState::new();
        ExchangeSide::Binance.mark_alive(&health, 1234);
        ExchangeSide::Gate.mark_alive(&health, 5678);
        assert!(health.binance_connected.load(Ordering::Relaxed));
        assert!(health.gate_connected.load(Ordering::Relaxed));
        assert_eq!(health.binance_last_tick_ms.load(Ordering::Relaxed), 1234);
        assert_eq!(health.gate_last_tick_ms.load(Ordering::Relaxed), 5678);
    }

    #[test]
    fn exchange_side_marks_disconnected_on_connectivity_error() {
        let health = HealthState::new();
        ExchangeSide::Binance.mark_alive(&health, 1234);
        ExchangeSide::Binance.maybe_mark_disconnected(
            &health,
            &hft_lead_lag::domain::ExchangeError::Timeout("timeout".to_string()),
        );
        assert!(!health.binance_connected.load(Ordering::Relaxed));
    }

    #[test]
    fn runtime_strategy_builder_loads_lead_lag_classic() {
        let path = write_temp_config(
            "strategy-default",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []
"#,
        );
        let manager =
            ConfigManager::from_file(path.to_str().expect("utf-8 path")).expect("load config");

        let strategy = hft_lead_lag::build_runtime_strategy(&manager, vec!["BTCUSDT".to_string()])
            .expect("lead-lag strategy should build");
        assert_eq!(strategy.strategy_name(), "lead_lag_classic");

        fs::remove_file(path).expect("cleanup temp config");
    }

    #[test]
    fn runtime_strategy_builder_rejects_unimplemented_strategy() {
        let path = write_temp_config(
            "strategy-unimplemented",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []

[strategy]
active = "dislocation_reversion"
"#,
        );
        let manager =
            ConfigManager::from_file(path.to_str().expect("utf-8 path")).expect("load config");

        let result = hft_lead_lag::build_runtime_strategy(&manager, vec!["BTCUSDT".to_string()]);
        match result {
            Ok(_) => panic!("unimplemented strategy should fail"),
            Err(err) => {
                assert!(
                    err.to_string().contains("not implemented"),
                    "unexpected error: {err}"
                );
            }
        }

        fs::remove_file(path).expect("cleanup temp config");
    }
