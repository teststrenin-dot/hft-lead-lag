"""Tests for FleetIPC submission lock behavior."""

import json
import threading
import time

from ray_driver.ipc import CONFIG_ID_CONTRACT_VERSION, FleetIPC, TrialAck


def test_submit_batch_serializes_parallel_calls(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    ipc1 = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")
    ipc2 = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")

    def fake_wait_ack(self, run_id, timeout_s, ack_path=None):
        time.sleep(0.25)
        return TrialAck(
            run_id=run_id,
            applied_at_ms=0,
            config_count=1,
            drained_trades=0,
        )

    monkeypatch.setattr(FleetIPC, "_wait_ack", fake_wait_ack)

    started = threading.Event()

    def run_first():
        started.set()
        ipc1.submit_batch("run-1", [{"k": 1}])

    t = threading.Thread(target=run_first)
    t.start()
    started.wait(timeout=1.0)

    time.sleep(0.05)
    t0 = time.monotonic()
    ipc2.submit_batch("run-2", [{"k": 2}])
    elapsed = time.monotonic() - t0

    t.join(timeout=2.0)
    assert not t.is_alive()

    # With serialization lock, second call waits for first (~0.20s left)
    # plus its own wait (~0.25s). Without lock this is ~0.25s.
    assert elapsed >= 0.40


def test_submit_batch_writes_config_contract_version(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    ipc = FleetIPC(config_dir=config_dir, db_path=tmp_path / "optimizer.db")

    monkeypatch.setattr(
        FleetIPC,
        "_wait_ack",
        lambda self, run_id, timeout_s, ack_path=None: TrialAck(
            run_id=run_id,
            applied_at_ms=0,
            config_count=1,
            drained_trades=0,
        ),
    )

    ipc.submit_batch("run-1", [{"k": 1}])
    queue_files = list((config_dir / "trial-batches").glob("*.json"))
    assert len(queue_files) == 1
    payload = json.loads(queue_files[0].read_text())
    assert payload["config_id_contract_version"] == CONFIG_ID_CONTRACT_VERSION
