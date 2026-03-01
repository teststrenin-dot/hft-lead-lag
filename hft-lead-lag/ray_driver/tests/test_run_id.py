"""Tests for unique run_id generation and usage in phases."""

from dataclasses import dataclass

from ray_driver import expand, scout
from ray_driver.ipc import RunMetrics, TrialAck
from ray_driver.run_id import generate_run_id


@dataclass
class _FakeIPC:
    submitted: list[str]
    submitted_kwargs: list[dict]

    def __init__(self):
        self.submitted = []
        self.submitted_kwargs = []
        self.db_path = None

    def clear_ack(self):
        return None

    def submit_batch(self, run_id, configs, timeout_s=30.0, **kwargs):
        self.submitted.append(run_id)
        self.submitted_kwargs.append(dict(kwargs))
        return TrialAck(
            run_id=run_id,
            applied_at_ms=0,
            config_count=len(configs),
            drained_trades=0,
        )

    def query_run_metrics(self, run_id):
        return [
            RunMetrics(
                config_id=1,
                trades=2,
                avg_pnl_pct=0.1,
                win_rate_pct=50.0,
                total_pnl_pct=0.2,
                stop_loss_share_pct=0.0,
            )
        ]

    def clear_active_run(self, run_id=None):
        return None


def test_generate_run_id_is_unique_and_prefixed():
    ids = [generate_run_id("scout") for _ in range(20)]
    assert len(set(ids)) == len(ids)
    assert all(run_id.startswith("scout-") for run_id in ids)


def test_run_scout_uses_generate_run_id(monkeypatch):
    ipc = _FakeIPC()
    monkeypatch.setattr(scout, "generate_scout_configs", lambda max_configs=5000: [{"k": 1}])
    monkeypatch.setattr(scout.time, "sleep", lambda _: None)
    monkeypatch.setattr(scout, "generate_run_id", lambda phase: "scout-fixed-1")

    run_id, alive = scout.run_scout(ipc, duration_s=0)

    assert run_id == "scout-fixed-1"
    assert ipc.submitted == ["scout-fixed-1"]
    assert len(alive) == 1


def test_run_scout_enables_run_id_takeover(monkeypatch):
    ipc = _FakeIPC()
    monkeypatch.setattr(scout, "generate_scout_configs", lambda max_configs=5000: [{"k": 1}])
    monkeypatch.setattr(scout.time, "sleep", lambda _: None)
    monkeypatch.setattr(scout, "generate_run_id", lambda phase: "scout-fixed-2")

    scout.run_scout(ipc, duration_s=0)

    assert ipc.submitted_kwargs, "submit_batch must be called"
    assert ipc.submitted_kwargs[0].get("allow_run_id_takeover") is True


def test_run_expand_uses_generate_run_id(monkeypatch):
    ipc = _FakeIPC()
    refs = [
        RunMetrics(
            config_id=1,
            trades=2,
            avg_pnl_pct=0.1,
            win_rate_pct=50.0,
            total_pnl_pct=0.2,
            stop_loss_share_pct=0.0,
        )
    ]
    monkeypatch.setattr(expand, "expand_around_references", lambda references, db_path, n_steps=1: [{"k": 1}])
    monkeypatch.setattr(expand.time, "sleep", lambda _: None)
    monkeypatch.setattr(expand, "generate_run_id", lambda phase: "expand-fixed-1")

    alive = expand.run_expand(ipc, refs, duration_s=0)

    assert ipc.submitted == ["expand-fixed-1"]
    assert len(alive) == 1
