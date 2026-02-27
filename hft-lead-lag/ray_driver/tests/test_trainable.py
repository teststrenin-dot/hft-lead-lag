"""Tests for per-config forward trial metrics in FleetTrial."""

import sys
import types

from ray_driver.ipc import RunMetrics


def install_fake_ray(monkeypatch):
    ray_mod = types.ModuleType("ray")
    tune_mod = types.ModuleType("ray.tune")

    class DummyTrainable:  # pragma: no cover - shim for import compatibility
        pass

    tune_mod.Trainable = DummyTrainable
    ray_mod.tune = tune_mod
    monkeypatch.setitem(sys.modules, "ray", ray_mod)
    monkeypatch.setitem(sys.modules, "ray.tune", tune_mod)


def test_fleet_trial_reports_only_assigned_config(monkeypatch):
    install_fake_ray(monkeypatch)
    import ray_driver.trainable as trainable

    class FakeIPC:
        def __init__(self, *_args, **_kwargs):
            pass

        def query_run_metrics(self, run_id):
            assert run_id == "forward-test"
            return [
                RunMetrics(1, 10, 0.10, 40.0, 1.0, 20.0),
                RunMetrics(2, 5, 0.25, 60.0, 1.25, 10.0),
            ]

    monkeypatch.setattr(trainable, "FleetIPC", FakeIPC)
    monkeypatch.setattr(trainable.time, "sleep", lambda _s: None)

    trial = trainable.FleetTrial()
    trial.setup({"run_id": "forward-test", "config_id": 2, "report_interval_s": 5})
    metrics = trial.step()

    assert metrics["config_id"] == 2
    assert metrics["total_trades"] == 5
    assert metrics["avg_pnl_pct"] == 0.25
    assert metrics["avg_win_rate_pct"] == 60.0
    assert metrics["total_pnl_pct"] == 1.25
    assert metrics["stop_loss_share_pct"] == 10.0
    assert metrics["configs_with_trades"] == 1


def test_fleet_trial_returns_zero_metrics_when_config_absent(monkeypatch):
    install_fake_ray(monkeypatch)
    import ray_driver.trainable as trainable

    class FakeIPC:
        def __init__(self, *_args, **_kwargs):
            pass

        def query_run_metrics(self, _run_id):
            return [RunMetrics(1, 3, 0.1, 33.0, 0.3, 0.0)]

    monkeypatch.setattr(trainable, "FleetIPC", FakeIPC)
    monkeypatch.setattr(trainable.time, "sleep", lambda _s: None)

    trial = trainable.FleetTrial()
    trial.setup({"run_id": "forward-test", "config_id": 99, "report_interval_s": 5})
    metrics = trial.step()

    assert metrics["config_id"] == 99
    assert metrics["total_trades"] == 0
    assert metrics["avg_pnl_pct"] == 0.0
    assert metrics["avg_win_rate_pct"] == 0.0
    assert metrics["total_pnl_pct"] == 0.0
    assert metrics["stop_loss_share_pct"] == 0.0
    assert metrics["configs_with_trades"] == 0


def test_fleet_trial_uses_configured_ipc_paths(monkeypatch, tmp_path):
    install_fake_ray(monkeypatch)
    import ray_driver.trainable as trainable

    captured = {}

    class FakeIPC:
        def __init__(self, config_dir, db_path):
            captured["config_dir"] = str(config_dir)
            captured["db_path"] = str(db_path)

        def query_run_metrics(self, _run_id):
            return []

    monkeypatch.setattr(trainable, "FleetIPC", FakeIPC)
    monkeypatch.setattr(trainable.time, "sleep", lambda _s: None)

    cfg_dir = tmp_path / "cfg"
    db_path = tmp_path / "data" / "optimizer.db"
    trial = trainable.FleetTrial()
    trial.setup(
        {
            "run_id": "forward-test",
            "config_id": 7,
            "report_interval_s": 1,
            "config_dir": str(cfg_dir),
            "db_path": str(db_path),
        }
    )
    trial.step()

    assert captured["config_dir"] == str(cfg_dir)
    assert captured["db_path"] == str(db_path)
