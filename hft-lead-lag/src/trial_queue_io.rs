use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{EventLoopState, TrialAck};
use tracing::warn;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TrialBatchIdentity {
    run_id: Option<String>,
    submission_id: Option<String>,
}

const UNKNOWN_TRIAL_RUN_ID: &str = "unknown";
pub(super) const TRIAL_BATCH_ARCHIVE_MAX_FILES: usize = 256;

fn parse_ascii_u128(raw: &str) -> Option<u128> {
    if raw.is_empty() || !raw.as_bytes().iter().all(u8::is_ascii_digit) {
        return None;
    }
    raw.parse::<u128>().ok()
}

fn queue_submission_id_from_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .map(str::trim)
        .filter(|stem| !stem.is_empty())
        .map(|stem| stem.to_string())
}

fn queue_submission_timestamp(path: &Path) -> Option<u128> {
    let submission_id = queue_submission_id_from_path(path)?;
    submission_timestamp_from_id(&submission_id)
}

fn system_time_to_unix_ns(ts: SystemTime) -> Option<u128> {
    ts.duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .map(|delta| delta.as_nanos())
}

fn queue_modified_timestamp(path: &Path) -> Option<u128> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    system_time_to_unix_ns(modified)
}

fn queue_order_timestamp(path: &Path) -> Option<u128> {
    queue_submission_timestamp(path).or_else(|| queue_modified_timestamp(path))
}

fn submission_timestamp_from_id(submission_id: &str) -> Option<u128> {
    if let Some((run_or_prefix, suffix)) = submission_id.rsplit_once('-') {
        if !run_or_prefix.trim().is_empty() {
            if let Some(ts) = parse_ascii_u128(suffix.trim()) {
                return Some(ts);
            }
        }
    }
    if let Some((prefix, run_or_suffix)) = submission_id.split_once('-') {
        if !run_or_suffix.trim().is_empty() {
            if let Some(ts) = parse_ascii_u128(prefix.trim()) {
                return Some(ts);
            }
        }
    }
    parse_ascii_u128(submission_id.trim())
}

fn run_id_from_submission_id(submission_id: &str) -> Option<String> {
    if let Some((run_id, suffix)) = submission_id.rsplit_once('-') {
        let run_id = run_id.trim();
        if !run_id.is_empty() && parse_ascii_u128(suffix.trim()).is_some() {
            return Some(run_id.to_string());
        }
    }
    if let Some((prefix, run_id)) = submission_id.split_once('-') {
        let run_id = run_id.trim();
        if !run_id.is_empty() && parse_ascii_u128(prefix.trim()).is_some() {
            return Some(run_id.to_string());
        }
    }
    None
}

fn extract_trial_batch_identity_from_payload(path: &Path) -> TrialBatchIdentity {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(_) => return TrialBatchIdentity::default(),
    };
    let json: serde_json::Value = match serde_json::from_str(&content) {
        Ok(json) => json,
        Err(_) => return TrialBatchIdentity::default(),
    };
    let run_id = json
        .get("run_id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let submission_id = json
        .get("submission_id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    TrialBatchIdentity {
        run_id,
        submission_id,
    }
}

pub(super) fn build_trial_batch_error_ack(
    path: &Path,
    is_queue_mode: bool,
    error: String,
) -> TrialAck {
    let mut identity = extract_trial_batch_identity_from_payload(path);
    if is_queue_mode {
        if identity.submission_id.is_none() {
            identity.submission_id = queue_submission_id_from_path(path);
        }
        if identity.run_id.is_none() {
            identity.run_id = identity
                .submission_id
                .as_deref()
                .and_then(run_id_from_submission_id);
        }
    }
    TrialAck::error(
        identity
            .run_id
            .unwrap_or_else(|| UNKNOWN_TRIAL_RUN_ID.to_string()),
        error,
        identity.submission_id,
    )
}

pub(super) fn trial_batch_queue_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("trial-batches")
}

pub(super) fn trial_batch_archive_dir(config_dir: &Path, success: bool) -> PathBuf {
    let bucket = if success { "ok" } else { "error" };
    config_dir.join("trial-batches-archive").join(bucket)
}

fn quarantine_marker_path(batch_json_path: &Path) -> PathBuf {
    let file_name = batch_json_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("batch.json");
    batch_json_path.with_file_name(format!("{file_name}.archive-quarantine"))
}

fn trial_ack_queue_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("trial-acks")
}

fn sanitize_submission_id_for_ack_path(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut sanitized = String::with_capacity(trimmed.len().min(128));
    for ch in trimmed.chars().take(128) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    while sanitized.starts_with('.') {
        sanitized.remove(0);
    }
    while sanitized.ends_with('.') {
        sanitized.pop();
    }

    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        return None;
    }
    Some(sanitized)
}

pub(super) fn list_trial_batch_queue_files(config_dir: &Path) -> Vec<PathBuf> {
    let queue_dir = trial_batch_queue_dir(config_dir);
    let mut files = Vec::new();
    let entries = match std::fs::read_dir(queue_dir) {
        Ok(entries) => entries,
        Err(_) => return files,
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
            && !quarantine_marker_path(&path).exists()
        {
            files.push(path);
        }
    }
    files.sort_by(|left, right| {
        match (queue_order_timestamp(left), queue_order_timestamp(right)) {
            (Some(left_ts), Some(right_ts)) => left_ts.cmp(&right_ts).then_with(|| left.cmp(right)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    });
    files
}

pub(super) fn count_trial_batch_quarantine_markers(config_dir: &Path) -> u64 {
    let queue_dir = trial_batch_queue_dir(config_dir);
    let entries = match std::fs::read_dir(queue_dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".archive-quarantine"))
        })
        .count() as u64
}

fn prune_trial_batch_archive_dir(archive_dir: &Path, max_files: usize) {
    let entries = match std::fs::read_dir(archive_dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    let mut files: Vec<(SystemTime, PathBuf)> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        })
        .map(|path| {
            let modified = std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            (modified, path)
        })
        .collect();
    if files.len() <= max_files {
        return;
    }
    files.sort_by(|(left_ts, left_path), (right_ts, right_path)| {
        left_ts
            .cmp(right_ts)
            .then_with(|| left_path.cmp(right_path))
    });
    let remove_count = files.len().saturating_sub(max_files);
    for (_, path) in files.into_iter().take(remove_count) {
        if let Err(error) = std::fs::remove_file(&path) {
            warn!(
                "trial-batch queue: failed to prune archived file {}: {error}",
                path.display()
            );
        }
    }
}

pub(super) fn archive_trial_batch_queue_file(
    config_dir: &Path,
    queued_batch_path: &Path,
    success: bool,
) {
    fn stash_unarchived_batch(queued_batch_path: &Path) {
        let file_name = queued_batch_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("batch");
        let stashed_path = queued_batch_path.with_file_name(format!(
            "{}-{file_name}.archive-pending",
            EventLoopState::now_ms()
        ));
        if let Err(error) = std::fs::rename(queued_batch_path, &stashed_path) {
            let quarantine_marker = quarantine_marker_path(queued_batch_path);
            if let Err(marker_error) = std::fs::write(
                &quarantine_marker,
                format!(
                    "quarantined_at_ms={}\narchive_error={}\n",
                    EventLoopState::now_ms(),
                    error
                ),
            ) {
                warn!(
                    "trial-batch queue: archive+stash failed, and marker write failed {}: {marker_error}",
                    quarantine_marker.display()
                );
            } else {
                warn!(
                    "trial-batch queue: archive+stash failed, marked payload as quarantined via {}",
                    quarantine_marker.display()
                );
            }
            warn!(
                "trial-batch queue: archive failed and stash rename failed {} -> {}: {error}",
                queued_batch_path.display(),
                stashed_path.display()
            );
        } else {
            warn!(
                "trial-batch queue: archive failed, stashed unarchived payload at {}",
                stashed_path.display()
            );
        }
    }

    let archive_dir = trial_batch_archive_dir(config_dir, success);
    if let Err(error) = std::fs::create_dir_all(&archive_dir) {
        warn!(
            "trial-batch queue: failed to create archive dir {}: {error}",
            archive_dir.display()
        );
        stash_unarchived_batch(queued_batch_path);
        return;
    }
    let file_name = queued_batch_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("batch.json");
    let archived_path = archive_dir.join(format!("{}-{file_name}", EventLoopState::now_ms()));
    if let Err(error) = std::fs::rename(queued_batch_path, &archived_path) {
        warn!(
            "trial-batch queue: failed to archive {} -> {}: {error}",
            queued_batch_path.display(),
            archived_path.display()
        );
        stash_unarchived_batch(queued_batch_path);
        return;
    }
    prune_trial_batch_archive_dir(&archive_dir, TRIAL_BATCH_ARCHIVE_MAX_FILES);
}

pub(super) fn write_trial_ack(dir: &Path, ack: &TrialAck) {
    let path = match ack
        .submission_id
        .as_deref()
        .and_then(sanitize_submission_id_for_ack_path)
    {
        Some(submission_id) => {
            let ack_dir = trial_ack_queue_dir(dir);
            if let Err(error) = std::fs::create_dir_all(&ack_dir) {
                warn!(
                    "trial-ack: failed to create queue dir {}: {error}",
                    ack_dir.display()
                );
            }
            ack_dir.join(format!("{submission_id}.json"))
        }
        None => dir.join(".trial-ack"),
    };
    match serde_json::to_string_pretty(ack) {
        Ok(json) => {
            if let Err(error) = std::fs::write(&path, json) {
                warn!("trial-ack: failed to write {}: {error}", path.display());
            }
        }
        Err(error) => warn!("trial-ack: serialize error: {error}"),
    }
}
