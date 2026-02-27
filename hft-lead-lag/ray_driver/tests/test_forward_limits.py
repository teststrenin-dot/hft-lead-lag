"""Tests for bounded forward-run config preparation."""

import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

from ray_driver import cli, expand
from ray_driver.ipc import RunMetrics


def metric(config_id: int) -> RunMetrics:
    return RunMetrics(
        config_id=config_id,
        trades=10,
        avg_pnl_pct=0.2,
        win_rate_pct=55.0,
        total_pnl_pct=2.0,
        stop_loss_share_pct=10.0,
    )


def install_fake_ray(monkeypatch):
    captured = {}

    ray_mod = types.ModuleType("ray")
    tune_mod = types.ModuleType("ray.tune")
    sched_mod = types.ModuleType("ray.tune.schedulers")

    class DummyTrainable:  # pragma: no cover - marker base for import compatibility
        pass

    class DummyASHA:  # pragma: no cover - just captures constructor args
        def __init__(self, **kwargs):
            captured["scheduler_kwargs"] = kwargs

    def run(trainable, **kwargs):
        captured["trainable"] = trainable
        captured["run_kwargs"] = kwargs
        return SimpleNamespace()

    def grid_search(values):
        captured["grid_search_values"] = list(values)
        return {"grid_search": list(values)}

    tune_mod.Trainable = DummyTrainable
    tune_mod.run = run
    tune_mod.grid_search = grid_search
    sched_mod.ASHAScheduler = DummyASHA
    tune_mod.schedulers = sched_mod
    ray_mod.tune = tune_mod

    monkeypatch.setitem(sys.modules, "ray", ray_mod)
    monkeypatch.setitem(sys.modules, "ray.tune", tune_mod)
    monkeypatch.setitem(sys.modules, "ray.tune.schedulers", sched_mod)

    return captured


def test_expand_around_references_enforces_caps_without_first_ref_bias(monkeypatch):
    fetched_ids = []

    def fake_fetch(_db_path: Path, config_id: int):
        fetched_ids.append(config_id)
        center = 20.0 if config_id == 1 else 90.0
        return {
            "spike_threshold_bps": center,
            "target_ratio": 0.3,
            "stop_loss_bps": 10.0,
            "max_hold_ms": 10_000,
            "max_spread_bps": 3.0,
            "trailing_decay_ratio": 0.3,
            "baseline_window_ms": 20_000,
        }

    monkeypatch.setattr(expand, "fetch_config_params", fake_fetch)

    refs = [metric(i) for i in range(1, 11)]
    cfgs = expand.expand_around_references(
        refs,
        Path("data/optimizer.db"),
        n_steps=1,
        max_refs=2,
        max_configs=10,
    )

    assert len(cfgs) == 10
    assert set(fetched_ids) == {1, 2}
    assert len(set(fetched_ids)) <= 2
    low_band = [cfg for cfg in cfgs if cfg["spike_threshold_bps"] <= 30.0]
    high_band = [cfg for cfg in cfgs if cfg["spike_threshold_bps"] >= 80.0]
    assert low_band
    assert high_band


def test_cmd_forward_applies_ref_and_config_caps(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "scout-references.json").write_text(
        json.dumps(
            [
                {"config_id": 1, "trades": 3, "avg_pnl_pct": 0.11},
                {"config_id": 2, "trades": 4, "avg_pnl_pct": 0.12},
                {"config_id": 3, "trades": 5, "avg_pnl_pct": 0.13},
            ]
        )
    )

    captured = install_fake_ray(monkeypatch)
    expand_calls = {}
    submitted = {}

    def fake_expand(references, db_path, n_steps=1, max_refs=None, max_configs=None):
        expand_calls["refs_len"] = len(references)
        expand_calls["db_path"] = str(db_path)
        expand_calls["n_steps"] = n_steps
        expand_calls["max_refs"] = max_refs
        expand_calls["max_configs"] = max_configs
        return [{"cfg": 1}, {"cfg": 2}]

    monkeypatch.setattr(expand, "expand_around_references", fake_expand)

    class FakeIPC:
        def __init__(self, _config_dir: Path, db_path: Path):
            self.db_path = db_path

        def clear_ack(self):
            submitted["cleared"] = True

        def submit_batch(self, run_id, configs):
            submitted["run_id"] = run_id
            submitted["configs"] = list(configs)
            return SimpleNamespace(config_count=len(configs), drained_trades=0)

        def clear_active_run(self, run_id=None):
            submitted["cleared_run_id"] = run_id

    monkeypatch.setattr(cli, "FleetIPC", FakeIPC)
    monkeypatch.setattr(
        cli,
        "resolve_config_pairs_for_configs",
        lambda _db, _cfgs: [(101, {"cfg": 1}), (102, {"cfg": 2})],
    )

    args = SimpleNamespace(
        config_dir="config",
        db_path="data/optimizer.db",
        max_budget=100,
        grace_period=20,
        report_interval=5,
        max_refs=2,
        max_configs=111,
    )
    cli.cmd_forward(args)

    assert expand_calls["refs_len"] == 3
    assert expand_calls["n_steps"] == 1
    assert expand_calls["max_refs"] == 2
    assert expand_calls["max_configs"] == 111

    assert submitted["cleared"] is True
    assert submitted["configs"] == [{"cfg": 1}, {"cfg": 2}]

    run_cfg = captured["run_kwargs"]["config"]
    assert run_cfg["run_id"] == submitted["run_id"]
    assert run_cfg["report_interval_s"] == 5
    assert run_cfg["config_id"] == {"grid_search": [101, 102]}
    assert captured["grid_search_values"] == [101, 102]
    assert captured["run_kwargs"]["num_samples"] == 1
    assert submitted["cleared_run_id"] == submitted["run_id"]


def test_cmd_forward_clamps_limits_to_hard_caps(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "scout-references.json").write_text(
        json.dumps([{"config_id": 1, "trades": 3, "avg_pnl_pct": 0.11}])
    )

    install_fake_ray(monkeypatch)
    expand_calls = {}

    def fake_expand(references, db_path, n_steps=1, max_refs=None, max_configs=None):
        expand_calls["max_refs"] = max_refs
        expand_calls["max_configs"] = max_configs
        return [{"cfg": 1}]

    monkeypatch.setattr(expand, "expand_around_references", fake_expand)

    class FakeIPC:
        def __init__(self, _config_dir: Path, db_path: Path):
            self.db_path = db_path

        def clear_ack(self):
            return None

        def submit_batch(self, _run_id, _configs):
            return SimpleNamespace(config_count=1, drained_trades=0)

        def clear_active_run(self, _run_id=None):
            return None

    monkeypatch.setattr(cli, "FleetIPC", FakeIPC)
    monkeypatch.setattr(
        cli,
        "resolve_config_pairs_for_configs",
        lambda _db, _cfgs: [(1, {"cfg": 1})],
    )

    args = SimpleNamespace(
        config_dir="config",
        db_path="data/optimizer.db",
        max_budget=100,
        grace_period=20,
        report_interval=5,
        max_refs=999_999,
        max_configs=999_999,
    )
    cli.cmd_forward(args)

    assert expand_calls["max_refs"] == cli.FORWARD_MAX_REFS_HARD_CAP
    assert expand_calls["max_configs"] == cli.FORWARD_MAX_CONFIGS_HARD_CAP


def test_cmd_forward_fails_when_config_pairs_not_resolved(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "scout-references.json").write_text(
        json.dumps([{"config_id": 1, "trades": 3, "avg_pnl_pct": 0.11}])
    )

    install_fake_ray(monkeypatch)
    monkeypatch.setattr(expand, "expand_around_references", lambda *_args, **_kwargs: [{"cfg": 1}])

    class FakeIPC:
        def __init__(self, _config_dir: Path, db_path: Path):
            self.db_path = db_path

        def clear_ack(self):
            return None

        def submit_batch(self, _run_id, _configs):
            return SimpleNamespace(config_count=1, drained_trades=0)

        def clear_active_run(self, _run_id=None):
            return None

    monkeypatch.setattr(cli, "FleetIPC", FakeIPC)
    monkeypatch.setattr(cli, "resolve_config_pairs_for_configs", lambda _db, _cfgs: [])

    args = SimpleNamespace(
        config_dir="config",
        db_path="data/optimizer.db",
        max_budget=100,
        grace_period=20,
        report_interval=5,
        max_refs=2,
        max_configs=10,
    )
    try:
        cli.cmd_forward(args)
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("cmd_forward must exit when no config pairs resolved")


def test_forward_prune_callback_submits_incremental_patch_for_early_stop():
    calls = []

    class FakeIPC:
        def submit_batch(self, run_id, configs, **kwargs):
            calls.append(
                {
                    "run_id": run_id,
                    "configs": list(configs),
                    "kwargs": dict(kwargs),
                }
            )
            return SimpleNamespace(config_count=len(configs), drained_trades=0)

    cb = cli.ForwardRuntimePruneCallback(
        ipc=FakeIPC(),
        run_id="forward-test",
        configs_by_id={1: {"cfg": 1}, 2: {"cfg": 2}, 3: {"cfg": 3}},
        max_budget_s=1000,
        grace_period_s=120,
        reduction_factor=2,
        min_patch_interval_s=1.0,
    )

    early_trial = SimpleNamespace(config={"config_id": 1})
    cb.on_trial_result(
        iteration=1,
        trials=[],
        trial=early_trial,
        result={"done": True, "time_budget_s": 240},
    )

    assert len(calls) == 1
    patch = calls[0]
    assert patch["run_id"] == "forward-test"
    assert patch["kwargs"]["mode"] == "incremental"
    assert patch["kwargs"]["changed_config_ids"] == [1]
    assert patch["configs"] == [{"cfg": 2}, {"cfg": 3}]


def test_forward_prune_callback_does_not_prune_terminal_budget():
    calls = []

    class FakeIPC:
        def submit_batch(self, run_id, configs, **kwargs):
            calls.append((run_id, configs, kwargs))
            return SimpleNamespace(config_count=len(configs), drained_trades=0)

    cb = cli.ForwardRuntimePruneCallback(
        ipc=FakeIPC(),
        run_id="forward-test",
        configs_by_id={1: {"cfg": 1}, 2: {"cfg": 2}},
        max_budget_s=1000,
        grace_period_s=120,
        reduction_factor=2,
        min_patch_interval_s=1.0,
    )
    # For max=1000, grace=120, reduction=2 terminal rung is 960.
    terminal_trial = SimpleNamespace(config={"config_id": 1})
    cb.on_trial_result(
        iteration=1,
        trials=[],
        trial=terminal_trial,
        result={"done": True, "time_budget_s": 960},
    )
    assert calls == []
