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
fn parse_portfolio_ids_deduplicates_and_skips_empty_tokens() {
    let parsed = parse_portfolio_ids("A, B, ,A, C ,,");
    assert_eq!(
        parsed,
        vec!["A".to_string(), "B".to_string(), "C".to_string()]
    );
}

#[test]
fn parse_portfolio_ids_from_env_returns_none_for_empty_input() {
    std::env::set_var(PORTFOLIO_IDS_ENV, " ,  ,");
    assert!(portfolio_ids_from_env().is_none());
    std::env::remove_var(PORTFOLIO_IDS_ENV);
}

#[test]
fn load_trial_batch_parses_incremental_mode() {
    let config = TraderConfig::default();
    let path = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-trial-batch-incremental-{}-{}.json",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let payload = serde_json::json!({
        "run_id": "scout-1",
        "mode": "incremental",
        "changed_config_ids": [config.config_id()],
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "configs": [config],
    });
    fs::write(
        &path,
        serde_json::to_string(&payload).expect("serialize trial batch"),
    )
    .expect("write trial batch");

    let batch = load_trial_batch(&path).expect("load trial batch");

    assert_eq!(
        batch.parse_mode_strict().expect("parse trial mode"),
        TrialBatchMode::Incremental
    );
    assert_eq!(batch.changed_config_ids, Some(vec![config.config_id()]));
    assert_eq!(
        batch.symbols,
        Some(vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()])
    );

    let _ = fs::remove_file(path);
}

#[test]
fn load_trial_batch_defaults_to_full_replace_when_mode_missing() {
    let config = TraderConfig::default();
    let path = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-trial-batch-default-{}-{}.json",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let payload = serde_json::json!({
        "run_id": "scout-2",
        "configs": [config],
    });
    fs::write(
        &path,
        serde_json::to_string(&payload).expect("serialize trial batch"),
    )
    .expect("write trial batch");

    let batch = load_trial_batch(&path).expect("load trial batch");

    assert_eq!(
        batch.parse_mode_strict().expect("parse trial mode"),
        TrialBatchMode::FullReplace
    );
    assert_eq!(batch.changed_config_ids, None);
    assert_eq!(batch.symbols, None);

    let _ = fs::remove_file(path);
}

#[test]
fn file_fingerprint_change_detects_same_mtime_different_size() {
    let ts = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
    let prev = Some(FileFingerprint {
        modified: ts,
        len: 100,
        content_hash: 1,
    });
    let current = Some(FileFingerprint {
        modified: ts,
        len: 101,
        content_hash: 1,
    });
    assert!(file_fingerprint_changed(prev, current));
}

#[test]
fn file_fingerprint_change_detects_same_size_same_mtime_different_content() {
    let ts = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
    let prev = Some(FileFingerprint {
        modified: ts,
        len: 100,
        content_hash: 1,
    });
    let current = Some(FileFingerprint {
        modified: ts,
        len: 100,
        content_hash: 2,
    });
    assert!(file_fingerprint_changed(prev, current));
}

#[test]
fn file_fingerprint_change_skips_when_fingerprint_same() {
    let ts = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
    let prev = Some(FileFingerprint {
        modified: ts,
        len: 100,
        content_hash: 1,
    });
    let current = Some(FileFingerprint {
        modified: ts,
        len: 100,
        content_hash: 1,
    });
    assert!(!file_fingerprint_changed(prev, current));
}

#[test]
fn file_fingerprint_change_detects_file_disappearance() {
    let ts = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10);
    let prev = Some(FileFingerprint {
        modified: ts,
        len: 100,
        content_hash: 1,
    });
    let current = None;
    assert!(file_fingerprint_changed(prev, current));
}

#[test]
fn trial_ack_failure_serialization_includes_status_and_error() {
    let ack = TrialAck::error("run-err".to_string(), "invalid payload".to_string(), None);
    let json = serde_json::to_value(&ack).expect("serialize");
    assert_eq!(json.get("status").and_then(|v| v.as_str()), Some("error"));
    assert_eq!(
        json.get("error").and_then(|v| v.as_str()),
        Some("invalid payload")
    );
}

#[test]
fn write_trial_ack_uses_submission_scoped_ack_file_when_present() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-ack-scope-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    let ack = TrialAck::success("run-1".to_string(), 1_000, 3, 0, Some("sub-1".to_string()));

    write_trial_ack(&dir, &ack);

    assert!(dir.join("trial-acks").join("sub-1.json").exists());
    assert!(!dir.join(".trial-ack").exists());
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn write_trial_ack_sanitizes_submission_id_path_fragments() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-ack-sanitize-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    fs::create_dir_all(&dir).expect("create temp dir");

    let ack = TrialAck::success(
        "run-1".to_string(),
        1_000,
        1,
        0,
        Some("../evil/../../sub:1".to_string()),
    );
    write_trial_ack(&dir, &ack);

    let ack_dir = dir.join("trial-acks");
    let files: Vec<PathBuf> = fs::read_dir(&ack_dir)
        .expect("read ack dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    assert_eq!(files.len(), 1);
    let file_name = files[0]
        .file_name()
        .and_then(|name| name.to_str())
        .expect("utf-8 file name");
    assert_eq!(files[0].parent(), Some(ack_dir.as_path()));
    assert!(file_name.ends_with(".json"));
    assert!(!file_name.contains('/'));
    assert!(!file_name.contains('\\'));
    assert!(!file_name.starts_with(".."));
    assert!(!dir.join(".trial-ack").exists());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn list_trial_batch_queue_files_returns_sorted_json_files() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-queue-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");
    fs::write(queue_dir.join("run-z-10.json"), "{}").expect("write run-z");
    fs::write(queue_dir.join("run-a-20.json"), "{}").expect("write run-a");
    fs::write(queue_dir.join("zzz.json"), "{}").expect("write zzz");
    fs::write(queue_dir.join("aaa.json"), "{}").expect("write aaa");
    fs::write(queue_dir.join("ignore.txt"), "{}").expect("write txt");

    let files = list_trial_batch_queue_files(&dir);
    let names: Vec<String> = files
        .iter()
        .filter_map(|path| path.file_name().and_then(|n| n.to_str()))
        .map(ToString::to_string)
        .collect();
    assert_eq!(
        names[..2],
        ["run-z-10.json".to_string(), "run-a-20.json".to_string()]
    );
    assert!(names[2..].iter().any(|name| name == "aaa.json"));
    assert!(names[2..].iter().any(|name| name == "zzz.json"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn list_trial_batch_queue_files_skips_quarantined_json_files() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-list-quarantine-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");

    let quarantined = queue_dir.join("run-q-1.json");
    let healthy = queue_dir.join("run-q-2.json");
    fs::write(&quarantined, "{}").expect("write quarantined json");
    fs::write(&healthy, "{}").expect("write healthy json");
    fs::write(
        queue_dir.join("run-q-1.json.archive-quarantine"),
        "quarantined",
    )
    .expect("write quarantine marker");

    let listed = list_trial_batch_queue_files(&dir);
    assert_eq!(listed, vec![healthy]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn count_trial_batch_quarantine_markers_counts_marker_files() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-quarantine-count-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");

    fs::write(queue_dir.join("run-a-1.json.archive-quarantine"), "q1").expect("write marker1");
    fs::write(queue_dir.join("run-a-2.json.archive-quarantine"), "q2").expect("write marker2");
    fs::write(queue_dir.join("run-a-3.json"), "{}").expect("write payload");
    fs::write(queue_dir.join("ignore.txt"), "x").expect("write ignore");

    assert_eq!(count_trial_batch_quarantine_markers(&dir), 2);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn list_trial_batch_queue_files_uses_mtime_fallback_for_non_timestamp_files() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-queue-fallback-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");

    let manual = queue_dir.join("manual.json");
    fs::write(&manual, "{}").expect("write manual");
    std::thread::sleep(std::time::Duration::from_millis(5));

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("now after epoch")
        .as_nanos();
    let timed = queue_dir.join(format!("run-a-{}.json", now_ns + 1000));
    fs::write(&timed, "{}").expect("write timed");

    let files = list_trial_batch_queue_files(&dir);
    let names: Vec<String> = files
        .iter()
        .filter_map(|path| path.file_name().and_then(|n| n.to_str()))
        .map(ToString::to_string)
        .collect();
    assert_eq!(
        names,
        vec![
            "manual.json".to_string(),
            timed
                .file_name()
                .and_then(|n| n.to_str())
                .expect("timed filename")
                .to_string(),
        ]
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn list_trial_batch_queue_files_orders_non_timestamp_files_by_mtime_fifo() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-queue-nonts-fifo-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");

    let older = queue_dir.join("zzz.json");
    let newer = queue_dir.join("aaa.json");
    fs::write(&older, "{}").expect("write older");
    std::thread::sleep(std::time::Duration::from_millis(5));
    fs::write(&newer, "{}").expect("write newer");

    let files = list_trial_batch_queue_files(&dir);
    let names: Vec<String> = files
        .iter()
        .filter_map(|path| path.file_name().and_then(|n| n.to_str()))
        .map(ToString::to_string)
        .collect();
    assert_eq!(names, vec!["zzz.json".to_string(), "aaa.json".to_string()]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn archive_trial_batch_queue_file_moves_file_into_archive_bucket() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-archive-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");
    let queued_file = queue_dir.join("run-a-1.json");
    fs::write(&queued_file, r#"{"run_id":"run-a","configs":[]}"#).expect("write queued file");

    archive_trial_batch_queue_file(&dir, &queued_file, true);

    assert!(!queued_file.exists());
    let archive_dir = trial_batch_archive_dir(&dir, true);
    let archived_files: Vec<String> = fs::read_dir(&archive_dir)
        .expect("read archive dir")
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(ToString::to_string)
        })
        .collect();
    assert_eq!(archived_files.len(), 1);
    assert!(archived_files[0].contains("run-a-1.json"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn archive_trial_batch_queue_file_prunes_archive_to_max_files() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-archive-prune-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");

    let file_count = TRIAL_BATCH_ARCHIVE_MAX_FILES + 8;
    for idx in 0..file_count {
        let queued_file = queue_dir.join(format!("run-a-{idx}.json"));
        fs::write(&queued_file, "{}").expect("write queued file");
        archive_trial_batch_queue_file(&dir, &queued_file, true);
    }

    let archive_dir = trial_batch_archive_dir(&dir, true);
    let archived_count = fs::read_dir(&archive_dir)
        .expect("read archive dir")
        .filter_map(Result::ok)
        .count();
    assert_eq!(archived_count, TRIAL_BATCH_ARCHIVE_MAX_FILES);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn archive_trial_batch_queue_file_stashes_payload_when_archive_dir_unavailable() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-archive-stash-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");
    let queued_file = queue_dir.join("run-stash-1.json");
    fs::write(&queued_file, r#"{"run_id":"run-stash","configs":[]}"#).expect("write queued file");

    // Block `create_dir_all(config/trial-batches-archive/ok)` by placing a file
    // where the archive root directory should be.
    let archive_root = dir.join("trial-batches-archive");
    fs::write(&archive_root, "blocked").expect("create archive root blocker file");

    archive_trial_batch_queue_file(&dir, &queued_file, true);

    assert!(
        !queued_file.exists(),
        "raw queue json should not remain after stashing"
    );
    let stashed: Vec<PathBuf> = fs::read_dir(&queue_dir)
        .expect("read queue dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("run-stash-1.json.archive-pending"))
        })
        .collect();
    assert_eq!(
        stashed.len(),
        1,
        "payload must be preserved exactly once on archive failure"
    );
    let listed = list_trial_batch_queue_files(&dir);
    assert!(
        listed.is_empty(),
        "stashed payload must not be re-consumed as queue JSON"
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn build_trial_batch_error_ack_uses_payload_identity_when_available() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-ack-id-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    fs::create_dir_all(&dir).expect("create temp dir");
    let payload_path = dir.join(".trial-batch");
    fs::write(
        &payload_path,
        r#"{"run_id":"run-42","submission_id":"sub-42","configs":[]}"#,
    )
    .expect("write payload");

    let ack = build_trial_batch_error_ack(
        &payload_path,
        false,
        "trial batch has no configs".to_string(),
    );

    assert_eq!(ack.run_id, "run-42".to_string());
    assert_eq!(ack.submission_id, Some("sub-42".to_string()));
    assert_eq!(ack.status, "error");
    assert_eq!(ack.error, Some("trial batch has no configs".to_string()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn build_trial_batch_error_ack_uses_queue_filename_identity_when_payload_malformed() {
    let dir = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-batch-ack-fallback-{}-{}",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let queue_dir = trial_batch_queue_dir(&dir);
    fs::create_dir_all(&queue_dir).expect("create queue dir");
    let payload_path = queue_dir.join("run-x-12345.json");
    fs::write(&payload_path, "{").expect("write malformed payload");

    let ack = build_trial_batch_error_ack(&payload_path, true, "parse error".to_string());

    assert_eq!(ack.run_id, "run-x".to_string());
    assert_eq!(ack.submission_id, Some("run-x-12345".to_string()));
    assert_eq!(ack.status, "error");

    let weird_path = queue_dir.join("???bad-name???.json");
    fs::write(&weird_path, "{").expect("write malformed payload");
    let weird_ack = build_trial_batch_error_ack(&weird_path, true, "parse error".to_string());
    assert_eq!(weird_ack.run_id, "unknown".to_string());
    assert_eq!(weird_ack.submission_id, Some("???bad-name???".to_string()));

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn build_trial_batch_patch_plan_rejects_incremental_without_changed_ids() {
    let batch = TrialBatch {
        run_id: "run-missing".to_string(),
        configs: vec![TraderConfig::default()],
        mode: Some("incremental".to_string()),
        changed_config_ids: None,
        symbols: None,
        config_id_contract_version: None,
        submission_id: None,
        allow_run_id_takeover: false,
    };
    let err = build_trial_batch_patch_plan(&batch).expect_err("expected validation error");
    assert!(
        err.contains("requires changed_config_ids"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_trial_batch_patch_plan_rejects_incremental_with_empty_changed_ids() {
    let batch = TrialBatch {
        run_id: "run-empty-ids".to_string(),
        configs: vec![TraderConfig::default()],
        mode: Some("incremental".to_string()),
        changed_config_ids: Some(Vec::new()),
        symbols: None,
        config_id_contract_version: None,
        submission_id: None,
        allow_run_id_takeover: false,
    };
    let err = build_trial_batch_patch_plan(&batch).expect_err("expected validation error");
    assert!(
        err.contains("requires non-empty changed_config_ids"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_trial_batch_patch_plan_rejects_incremental_with_empty_symbols_after_trim() {
    let cfg = TraderConfig::default();
    let batch = TrialBatch {
        run_id: "run-empty-symbols".to_string(),
        configs: vec![cfg],
        mode: Some("incremental".to_string()),
        changed_config_ids: Some(vec![cfg.config_id()]),
        symbols: Some(vec![" ".to_string(), "".to_string()]),
        config_id_contract_version: None,
        submission_id: None,
        allow_run_id_takeover: false,
    };
    let err = build_trial_batch_patch_plan(&batch).expect_err("expected validation error");
    assert!(
        err.contains("symbols must contain"),
        "unexpected error: {err}"
    );
}

#[test]
fn build_trial_batch_patch_plan_rejects_config_id_contract_version_mismatch() {
    let cfg = TraderConfig::default();
    let batch = TrialBatch {
        run_id: "run-version-mismatch".to_string(),
        configs: vec![cfg],
        mode: Some("incremental".to_string()),
        changed_config_ids: Some(vec![cfg.config_id()]),
        symbols: None,
        config_id_contract_version: Some(999),
        submission_id: None,
        allow_run_id_takeover: false,
    };
    let err = build_trial_batch_patch_plan(&batch).expect_err("expected validation error");
    assert!(
        err.contains("config_id contract version mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn validate_trial_batch_run_lease_allows_when_no_active_run() {
    let result = validate_trial_batch_run_lease(None, "run-new", false);
    assert!(result.is_ok(), "expected ok, got: {result:?}");
}

#[test]
fn validate_trial_batch_run_lease_allows_when_active_equals_incoming() {
    let result = validate_trial_batch_run_lease(Some("run-1"), "run-1", false);
    assert!(result.is_ok(), "expected ok, got: {result:?}");
}

#[test]
fn validate_trial_batch_run_lease_rejects_mismatched_run_without_takeover() {
    let err = validate_trial_batch_run_lease(Some("run-active"), "run-next", false)
        .expect_err("expected lease reject");
    assert!(
        err.contains("active run_id lease held by run-active"),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("allow_run_id_takeover=true"),
        "unexpected error: {err}"
    );
}

#[test]
fn validate_trial_batch_run_lease_allows_mismatched_run_with_takeover() {
    let result = validate_trial_batch_run_lease(Some("run-active"), "run-next", true);
    assert!(result.is_ok(), "expected ok, got: {result:?}");
}

#[tokio::test]
async fn apply_trial_batch_reject_does_not_upsert_runtime_configs() {
    let db_path = std::env::temp_dir().join(format!(
        "hft-lead-lag-main-trial-reject-no-upsert-{}-{}.sqlite",
        std::process::id(),
        EventLoopState::now_ms()
    ));
    let pre_conn = hft_lead_lag::infrastructure::db::open_db(&db_path).expect("open db");
    drop(pre_conn);

    let screener = ScreenerStore::default();
    let cfg = TraderConfig::default();
    let changed_id = cfg.config_id();
    let batch = TrialBatch {
        run_id: "run-reject".to_string(),
        configs: vec![cfg],
        mode: Some("incremental".to_string()),
        changed_config_ids: Some(vec![changed_id]),
        symbols: None,
        config_id_contract_version: Some(CONFIG_ID_CONTRACT_VERSION),
        submission_id: Some("sub-reject".to_string()),
        allow_run_id_takeover: false,
    };

    let ack = apply_trial_batch(&screener, db_path.clone(), batch).await;
    assert_eq!(ack.status, "error");

    let conn = hft_lead_lag::infrastructure::db::open_db(&db_path).expect("open db");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM configs", [], |row| row.get(0))
        .expect("count configs");
    assert_eq!(
        count, 0,
        "rejected trial batch must not upsert runtime configs into db"
    );

    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

#[tokio::test]
async fn apply_trial_batch_returns_error_when_runtime_config_durability_fails() {
    let screener = ScreenerStore::default();
    let before_configs = screener.fleet_configs();
    let before_config_id = before_configs
        .first()
        .expect("default config should exist")
        .config_id();
    let next_cfg = TraderConfig {
        spike_threshold_bps: 77.0,
        ..TraderConfig::default()
    };

    let batch = TrialBatch {
        run_id: "run-durability-fail".to_string(),
        configs: vec![next_cfg],
        mode: None,
        changed_config_ids: None,
        symbols: None,
        config_id_contract_version: Some(CONFIG_ID_CONTRACT_VERSION),
        submission_id: Some("sub-durability-fail".to_string()),
        allow_run_id_takeover: false,
    };

    // Directory path is not a valid sqlite file target for open_db().
    let ack = apply_trial_batch(&screener, std::env::temp_dir(), batch).await;
    assert_eq!(ack.status, "error");
    assert!(
        ack.error
            .as_deref()
            .unwrap_or_default()
            .contains("durability"),
        "unexpected error: {:?}",
        ack.error
    );

    let after_configs = screener.fleet_configs();
    let after_config_id = after_configs
        .first()
        .expect("config should still exist")
        .config_id();
    assert_eq!(
        after_config_id, before_config_id,
        "failed durability must not mutate in-memory runtime configs"
    );
    assert_eq!(
        screener.current_run_id(),
        None,
        "run_id must not advance on durability failure"
    );
}

fn prime_symbol_fleet(screener: &ScreenerStore, symbol: &str, exchange_ts_ns: i64) {
    screener.update(
        symbol,
        "binance",
        100.0,
        101.0,
        exchange_ts_ns,
        exchange_ts_ns + 10_000,
    );
    screener.update(
        symbol,
        "gate",
        100.1,
        101.1,
        exchange_ts_ns,
        exchange_ts_ns + 20_000,
    );
}

#[test]
fn trial_batch_patch_plan_full_replace_resets_all_symbols() {
    let screener = ScreenerStore::default();
    let cfg_a = TraderConfig {
        spike_threshold_bps: 31.0,
        ..TraderConfig::default()
    };
    let cfg_b = TraderConfig {
        spike_threshold_bps: 41.0,
        ..TraderConfig::default()
    };
    screener.replace_fleet_configs(vec![cfg_a, cfg_b]);
    prime_symbol_fleet(&screener, "BTCUSDT", 1_000_000_000);
    prime_symbol_fleet(&screener, "ETHUSDT", 1_000_100_000);

    let batch = TrialBatch {
        run_id: "run-full".to_string(),
        configs: vec![cfg_a, cfg_b],
        mode: None,
        changed_config_ids: None,
        symbols: None,
        config_id_contract_version: None,
        submission_id: None,
        allow_run_id_takeover: false,
    };
    let plan = build_trial_batch_patch_plan(&batch).expect("build patch plan");
    assert!(matches!(plan.mode, FleetPatchMode::FullReplace));

    let report = screener.apply_fleet_patch(batch.configs.clone(), plan);
    assert_eq!(report.old_config_count, 2);
    assert_eq!(report.new_config_count, 2);
    assert_eq!(report.symbols_reset, 2);
}

#[test]
fn trial_batch_patch_plan_incremental_respects_symbol_scope() {
    let screener = ScreenerStore::default();
    let cfg_a = TraderConfig {
        spike_threshold_bps: 51.0,
        ..TraderConfig::default()
    };
    let cfg_b = TraderConfig {
        spike_threshold_bps: 61.0,
        ..TraderConfig::default()
    };
    screener.replace_fleet_configs(vec![cfg_a, cfg_b]);
    prime_symbol_fleet(&screener, "BTCUSDT", 2_000_000_000);
    prime_symbol_fleet(&screener, "ETHUSDT", 2_000_100_000);

    let batch = TrialBatch {
        run_id: "run-inc".to_string(),
        configs: vec![cfg_a, cfg_b],
        mode: Some("incremental".to_string()),
        changed_config_ids: Some(vec![cfg_a.config_id()]),
        symbols: Some(vec!["btcusdt".to_string()]),
        config_id_contract_version: None,
        submission_id: None,
        allow_run_id_takeover: false,
    };
    let plan = build_trial_batch_patch_plan(&batch).expect("build patch plan");
    assert!(matches!(plan.mode, FleetPatchMode::Incremental));

    let report = screener.apply_fleet_patch(batch.configs.clone(), plan);
    assert_eq!(report.old_config_count, 2);
    assert_eq!(report.new_config_count, 2);
    assert_eq!(report.symbols_reset, 1);
    assert_eq!(report.drained_trades, 0);
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
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Binance, 0)
        .is_none());
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Gate, 0)
        .is_none());
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
    let strategy_symbol_index =
        StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

    let processed = state
        .process_exchange_result(
            ExchangeSide::Binance,
            Ok(test_ticker("BTCUSDT", 100_000_000)),
            vec![test_ticker("ETHUSDT", 110_000_000)],
            &strategy_symbol_index,
            &screener,
            Some(&ws_tx),
        )
        .expect("exchange result should parse");

    assert_eq!(processed.updated_strategy_symbol_ids, vec![0, 1]);
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Binance, 0)
        .is_some());
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Binance, 1)
        .is_some());
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Gate, 0)
        .is_none());
    assert_eq!(state.ticker_count, 2);
}

#[tokio::test]
async fn event_loop_state_exposes_latest_books_by_symbol_id() {
    let mut state = EventLoopState::new();
    let screener = ScreenerStore::default();
    let strategy_symbol_index =
        StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

    state
        .process_exchange_result(
            ExchangeSide::Binance,
            Ok(test_ticker("BTCUSDT", 100_000_000)),
            vec![test_ticker("ETHUSDT", 110_000_000)],
            &strategy_symbol_index,
            &screener,
            None,
        )
        .expect("exchange result should parse");

    let btc = state
        .latest_book_for_strategy_symbol(ExchangeSide::Binance, 0)
        .expect("btc present");
    let eth = state
        .latest_book_for_strategy_symbol(ExchangeSide::Binance, 1)
        .expect("eth present");

    assert_eq!(btc.symbol, sym("BTCUSDT"));
    assert_eq!(eth.symbol, sym("ETHUSDT"));
}

#[tokio::test]
async fn event_loop_state_process_exchange_result_propagates_error() {
    let mut state = EventLoopState::new();
    let screener = ScreenerStore::default();
    let (ws_tx, _ws_rx) = tokio::sync::broadcast::channel(8);
    let strategy_symbol_index = StrategySymbolIndex::new(&["BTCUSDT".to_string()]);

    let result = state.process_exchange_result(
        ExchangeSide::Gate,
        Err(hft_lead_lag::domain::ExchangeError::Timeout(
            "test".to_string(),
        )),
        Vec::new(),
        &strategy_symbol_index,
        &screener,
        Some(&ws_tx),
    );

    assert!(matches!(
        result,
        Err(hft_lead_lag::domain::ExchangeError::Timeout(msg)) if msg == "test"
    ));
    assert_eq!(state.ticker_count, 0);
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Binance, 0)
        .is_none());
    assert!(state
        .latest_book_for_strategy_symbol(ExchangeSide::Gate, 0)
        .is_none());
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

fn sym(symbol: &str) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(symbol.as_bytes())
}

#[test]
fn rebuild_latest_map_preserves_old_entries() {
    let mut latest = std::collections::HashMap::new();
    latest.insert(sym("OLD"), test_ticker("OLD", 1));

    rebuild_latest_map(&mut latest, test_ticker("BTCUSDT", 10), Vec::new());

    assert!(latest.contains_key("OLD".as_bytes()));
    assert!(latest.contains_key("BTCUSDT".as_bytes()));
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
    assert_eq!(latest["BTCUSDT".as_bytes()].exchange_ts_ns, 20);
    assert_eq!(latest["ETHUSDT".as_bytes()].exchange_ts_ns, 30);
}

#[test]
fn process_exchange_batch_preserves_cached_symbols_and_ingests_only_updates() {
    let mut latest = std::collections::HashMap::new();
    latest.insert(sym("OLD"), test_ticker("OLD", 1));

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
        ws_tx: Some(&ws_tx),
    };

    process_exchange_batch(
        &mut latest,
        test_ticker("BTCUSDT", 100_000_000),
        Vec::new(),
        &mut ctx,
    );

    assert!(
        latest.contains_key("OLD".as_bytes()),
        "latest cache should preserve non-updated symbols"
    );
    assert!(latest.contains_key("BTCUSDT".as_bytes()));
    assert_eq!(ticker_count, 1);

    let event = ws_rx.try_recv().expect("ws event");
    assert_eq!(event.symbol, "BTCUSDT");
    assert!(matches!(
        ws_rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn updated_symbols_from_batch_deduplicates_and_preserves_first_seen_order() {
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
        vec![sym("BTCUSDT"), sym("ETHUSDT"), sym("ADAUSDT")]
    );
}

#[test]
fn updated_strategy_symbol_ids_from_batch_deduplicates_and_filters_unknown_symbols() {
    let index = StrategySymbolIndex::new(&[
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "ADAUSDT".to_string(),
    ]);
    let ids = updated_strategy_symbol_ids_from_batch(
        &test_ticker("BTCUSDT", 10),
        &[
            test_ticker("ETHUSDT", 20),
            test_ticker("BTCUSDT", 30),
            test_ticker("DOGEUSDT", 40),
            test_ticker("ADAUSDT", 50),
        ],
        &index,
    );
    assert_eq!(ids, vec![0, 1, 2]);
}

#[test]
fn strategy_symbol_updates_from_batch_preserves_latest_duplicate_update() {
    let index = StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    let (ids, updates) = strategy_symbol_updates_from_batch(
        test_ticker("BTCUSDT", 10),
        vec![
            test_ticker("ETHUSDT", 20),
            test_ticker("BTCUSDT", 30),
            test_ticker("DOGEUSDT", 40),
        ],
        &index,
    );

    assert_eq!(ids, vec![0, 1]);
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].0, 0);
    assert_eq!(updates[1].0, 1);
    assert_eq!(updates[0].1.exchange_ts_ns, 30);
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
fn build_runtime_universe_fallback_respects_blacklist() {
    let path = write_temp_config(
        "runtime-universe-fallback-blacklist",
        r#"
[binance]
enabled = true
blacklist = ["BTCUSDT", "ETHUSDT"]

[gate]
enabled = true
blacklist = ["BTCUSDT", "ETHUSDT"]
"#,
    );
    let manager =
        ConfigManager::from_file(path.to_str().expect("utf-8 path")).expect("load config");

    let universe = build_runtime_universe(&manager, 50_000_000.0, Vec::new(), Vec::new());
    assert!(
        universe.strategy_symbols.is_empty(),
        "blacklist must apply to fallback symbols too"
    );
    assert!(
        universe.screener_symbols.is_empty(),
        "blacklist must apply to screener fallback symbols too"
    );

    fs::remove_file(path).expect("cleanup temp config");
}

#[test]
fn strategy_ticks_in_order_skips_missing_symbols() {
    let btc = sym("BTCUSDT");
    let eth = sym("ETHUSDT");
    let strategy_symbols = vec![&btc, &eth];
    let mut latest = std::collections::HashMap::new();
    latest.insert(sym("BTCUSDT"), test_ticker("BTCUSDT", 10));

    let ticks: Vec<i64> = strategy_ticks_in_order(&strategy_symbols, &latest)
        .map(|t| t.exchange_ts_ns)
        .collect();
    assert_eq!(ticks, vec![10]);
}

#[test]
fn strategy_ticks_in_order_preserves_strategy_order() {
    let eth = sym("ETHUSDT");
    let btc = sym("BTCUSDT");
    let strategy_symbols = vec![&eth, &btc];
    let mut latest = std::collections::HashMap::new();
    latest.insert(sym("BTCUSDT"), test_ticker("BTCUSDT", 10));
    latest.insert(sym("ETHUSDT"), test_ticker("ETHUSDT", 20));

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
        ws_tx: Some(&ws_tx),
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
    latest.insert(sym("BTCUSDT"), test_ticker("BTCUSDT", 100_000_000));
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
        ws_tx: Some(&ws_tx),
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
    assert!(
        rows.is_empty(),
        "single-exchange batch must not emit ws_live screener rows"
    );
}

#[test]
fn process_exchange_batch_rebuilds_and_ingests_latest_state() {
    let mut latest = std::collections::HashMap::new();
    latest.insert(sym("OLD"), test_ticker("OLD", 1));
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
        ws_tx: Some(&ws_tx),
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

    assert!(latest.contains_key("OLD".as_bytes()));
    assert_eq!(latest.len(), 3);
    assert_eq!(latest["BTCUSDT".as_bytes()].exchange_ts_ns, 120_000_000);
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
    assert!(
        rows.is_empty(),
        "single-exchange batch must not emit ws_live screener rows"
    );
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
        ws_tx: Some(&ws_tx),
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
fn ingest_exchange_batch_deduplicates_symbol_and_keeps_latest_tick() {
    let first = test_ticker("BTCUSDT", 100_000_000);
    let drained = vec![
        test_ticker("ETHUSDT", 110_000_000),
        test_ticker("BTCUSDT", 120_000_000),
    ];
    let mut ticker_count = 0usize;
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
        ws_tx: Some(&ws_tx),
    };

    ingest_exchange_batch(&first, &drained, &mut ctx);

    assert_eq!(ticker_count, 2);
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
    assert_eq!(events[0].timestamp_ns, 120_000_000);
    assert_eq!(events[1].symbol, "ETHUSDT");
    assert_eq!(events[1].timestamp_ns, 110_000_000);
    assert!(matches!(
        ws_rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
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

#[test]
fn strategy_exchange_routing_defaults_when_lead_lag_config_missing() {
    let path = write_temp_config(
        "strategy-routing-default",
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

    let routing = resolve_strategy_exchange_routing(&manager);
    assert_eq!(routing.primary, ExchangeSide::Binance);
    assert_eq!(routing.hedge, ExchangeSide::Gate);

    fs::remove_file(path).expect("cleanup temp config");
}

#[test]
fn strategy_exchange_routing_respects_swapped_lead_lag_config() {
    let path = write_temp_config(
        "strategy-routing-swapped",
        r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []

[lead_lag]
primary_exchange = "gate"
hedge_exchange = "binance"
trigger_spread_bps = 35.0
max_position_age_ms = 5000
symbols = ["BTCUSDT"]
"#,
    );
    let manager =
        ConfigManager::from_file(path.to_str().expect("utf-8 path")).expect("load config");

    let routing = resolve_strategy_exchange_routing(&manager);
    assert_eq!(routing.primary, ExchangeSide::Gate);
    assert_eq!(routing.hedge, ExchangeSide::Binance);

    fs::remove_file(path).expect("cleanup temp config");
}

#[derive(Default)]
struct RecordingRuntimeStrategy {
    primary_symbols: Vec<String>,
    hedge_symbols: Vec<String>,
    checked_symbols: Vec<hft_lead_lag::domain::SymbolId>,
}

impl RuntimeStrategy for RecordingRuntimeStrategy {
    fn strategy_name(&self) -> &'static str {
        "recording"
    }

    fn on_primary_book(&mut self, ticker: hft_lead_lag::domain::BookTicker) {
        self.primary_symbols
            .push(String::from_utf8_lossy(&ticker.symbol).to_string());
    }

    fn on_hedge_book(&mut self, ticker: hft_lead_lag::domain::BookTicker) {
        self.hedge_symbols
            .push(String::from_utf8_lossy(&ticker.symbol).to_string());
    }

    fn check_signal(
        &mut self,
        symbol_id: hft_lead_lag::domain::SymbolId,
        _now_ns: i64,
    ) -> Option<hft_lead_lag::StrategySignal> {
        self.checked_symbols.push(symbol_id);
        None
    }
}

#[tokio::test]
async fn update_strategy_books_routes_by_configured_exchange_roles() {
    let mut state = EventLoopState::new();
    let mut strategy = RecordingRuntimeStrategy::default();
    let strategy_symbol_index =
        StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    let screener = ScreenerStore::default();
    state
        .process_exchange_result(
            ExchangeSide::Binance,
            Ok(test_ticker("BTCUSDT", 100_000_000)),
            Vec::new(),
            &strategy_symbol_index,
            &screener,
            None,
        )
        .expect("binance result");
    state
        .process_exchange_result(
            ExchangeSide::Gate,
            Ok(test_ticker("ETHUSDT", 100_000_000)),
            Vec::new(),
            &strategy_symbol_index,
            &screener,
            None,
        )
        .expect("gate result");
    let updated_binance = vec![sym("BTCUSDT")];
    let updated_gate = vec![sym("ETHUSDT")];

    state.update_strategy_books(
        ExchangeSide::Binance,
        &mut strategy,
        &strategy_symbol_index.symbol_ids(&updated_binance),
        StrategyExchangeRouting {
            primary: ExchangeSide::Gate,
            hedge: ExchangeSide::Binance,
        },
    );
    state.update_strategy_books(
        ExchangeSide::Gate,
        &mut strategy,
        &strategy_symbol_index.symbol_ids(&updated_gate),
        StrategyExchangeRouting {
            primary: ExchangeSide::Gate,
            hedge: ExchangeSide::Binance,
        },
    );

    let primary = strategy.primary_symbols.clone();
    let hedge = strategy.hedge_symbols.clone();
    assert_eq!(primary, vec!["ETHUSDT".to_string()]);
    assert_eq!(hedge, vec!["BTCUSDT".to_string()]);
}

#[tokio::test]
async fn strategy_update_queue_enqueues_and_flushes_updates() {
    let mut state = EventLoopState::new();
    let mut strategy = RecordingRuntimeStrategy::default();
    let strategy_symbol_index =
        StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    let screener = ScreenerStore::default();

    let binance = state
        .process_exchange_result(
            ExchangeSide::Binance,
            Ok(test_ticker("BTCUSDT", 100_000_000)),
            Vec::new(),
            &strategy_symbol_index,
            &screener,
            None,
        )
        .expect("binance result");
    let gate = state
        .process_exchange_result(
            ExchangeSide::Gate,
            Ok(test_ticker("ETHUSDT", 100_000_000)),
            Vec::new(),
            &strategy_symbol_index,
            &screener,
            None,
        )
        .expect("gate result");

    state.enqueue_strategy_updates(ExchangeSide::Binance, &binance.updated_strategy_symbol_ids);
    state.enqueue_strategy_updates(ExchangeSide::Gate, &gate.updated_strategy_symbol_ids);
    state.flush_strategy_updates(
        &mut strategy,
        StrategyExchangeRouting {
            primary: ExchangeSide::Gate,
            hedge: ExchangeSide::Binance,
        },
    );

    assert_eq!(strategy.primary_symbols, vec!["ETHUSDT".to_string()]);
    assert_eq!(strategy.hedge_symbols, vec!["BTCUSDT".to_string()]);
}

#[tokio::test]
async fn handle_signal_tick_checks_only_pending_symbols() {
    let mut state = EventLoopState::new();
    let mut strategy = RecordingRuntimeStrategy::default();
    let health = HealthState::new();

    let strategy_symbol_index =
        StrategySymbolIndex::new(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    let updated = vec![
        sym("BTCUSDT"),
        sym("SOLUSDT"),
        sym("ETHUSDT"),
        sym("BTCUSDT"),
    ];
    state.mark_pending_signal_symbols(&strategy_symbol_index.symbol_ids(&updated));

    state.handle_signal_tick(&mut strategy, &health);

    let checked = strategy.checked_symbols.clone();
    assert_eq!(checked, vec![0, 1]);
}

#[tokio::test]
async fn handle_signal_tick_skips_when_no_pending_symbols() {
    let mut state = EventLoopState::new();
    let mut strategy = RecordingRuntimeStrategy::default();
    let health = HealthState::new();

    state.handle_signal_tick(&mut strategy, &health);

    let checked = strategy.checked_symbols.clone();
    assert!(checked.is_empty());
}

#[tokio::test]
async fn handle_signal_tick_respects_budget_and_keeps_backlog() {
    let mut state = EventLoopState::new();
    let mut strategy = RecordingRuntimeStrategy::default();
    let health = HealthState::new();
    let total = SIGNAL_CHECK_BUDGET_PER_TICK + 2;
    let symbols: Vec<String> = (0..total).map(|idx| format!("SYM{idx:04}")).collect();
    let strategy_symbol_index = StrategySymbolIndex::new(&symbols);

    for idx in 0..total {
        let symbol = format!("SYM{idx:04}");
        state.pending_signal_symbols.insert(
            strategy_symbol_index
                .symbol_id(symbol.as_bytes())
                .expect("symbol id exists"),
        );
    }

    state.handle_signal_tick(&mut strategy, &health);
    assert_eq!(state.pending_signal_symbols.len(), 2);
    let checked_after_first_tick = strategy.checked_symbols.len();
    assert_eq!(checked_after_first_tick, SIGNAL_CHECK_BUDGET_PER_TICK);

    state.handle_signal_tick(&mut strategy, &health);
    assert!(state.pending_signal_symbols.is_empty());
    let checked_after_second_tick = strategy.checked_symbols.len();
    assert_eq!(checked_after_second_tick, total);
}

#[tokio::test]
async fn drain_runtime_grid_reset_signals_reports_presence() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    tx.try_send(()).expect("send reset signal");
    tx.try_send(()).expect("send reset signal");

    assert!(drain_runtime_grid_reset_signals(&mut rx));
    assert!(!drain_runtime_grid_reset_signals(&mut rx));
}

#[test]
fn runtime_grid_sleep_ms_uses_pending_watch_interval_with_min_bound() {
    let generation = RuntimeGridGeneration {
        signature: 42,
        config: RuntimeGridConfig {
            watch_interval_ms: 250,
            ..RuntimeGridConfig::default()
        },
        configs: Vec::new(),
        modified: FileFingerprint {
            modified: std::time::SystemTime::UNIX_EPOCH,
            len: 0,
            content_hash: 0,
        },
    };

    assert_eq!(runtime_grid_sleep_ms(Some(&generation)), 500);
    assert_eq!(runtime_grid_sleep_ms(None), 5_000);
}
