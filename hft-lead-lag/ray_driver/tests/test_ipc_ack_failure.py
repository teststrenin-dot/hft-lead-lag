"""Tests for failure ack handling in file IPC."""

import json

import pytest

from ray_driver.ipc import FleetIPC


def test_wait_ack_parses_submission_id_and_removes_consumed_queue_ack(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    ipc = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")
    ack_path = config_dir / "trial-acks" / "sub-1.json"
    ack_path.parent.mkdir(parents=True, exist_ok=True)
    ack_path.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "status": "ok",
                "applied_at_ms": 123,
                "config_count": 2,
                "drained_trades": 0,
                "submission_id": "sub-1",
            }
        )
    )

    ack = ipc._wait_ack("run-1", timeout_s=0.2, ack_path=ack_path)

    assert ack.submission_id == "sub-1"
    assert not ack_path.exists()


def test_wait_ack_raises_runtime_error_on_failure_ack(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    ipc = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")
    ack_path = config_dir / ".trial-ack"

    ack_path.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "status": "error",
                "error": "invalid payload",
                "applied_at_ms": 0,
                "config_count": 0,
                "drained_trades": 0,
            }
        )
    )

    with pytest.raises(RuntimeError, match="invalid payload"):
        ipc._wait_ack("run-1", timeout_s=0.2)
    assert not ack_path.exists()


def test_clear_ack_keeps_submission_scoped_queue_ack_files(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    ipc = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")
    queue_dir = config_dir / "trial-acks"
    queue_dir.mkdir(parents=True, exist_ok=True)
    queued_a = queue_dir / "a.json"
    queued_b = queue_dir / "b.json"
    keep_txt = queue_dir / "keep.txt"
    queued_a.write_text("{}")
    queued_b.write_text("{}")
    keep_txt.write_text("keep")

    ipc.clear_ack()

    assert queued_a.exists()
    assert queued_b.exists()
    assert keep_txt.exists()
