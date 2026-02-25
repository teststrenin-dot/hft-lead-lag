"""Tests for failure ack handling in file IPC."""

import json

import pytest

from ray_driver.ipc import FleetIPC


def test_wait_ack_raises_runtime_error_on_failure_ack(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    ipc = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")

    (config_dir / ".trial-ack").write_text(
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
