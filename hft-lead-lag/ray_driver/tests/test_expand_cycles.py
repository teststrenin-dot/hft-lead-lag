"""Tests for expand cycles CLI behavior."""

import json
from types import SimpleNamespace

from ray_driver import cli
from ray_driver.ipc import RunMetrics


def metric(config_id: int = 1) -> RunMetrics:
    return RunMetrics(
        config_id=config_id,
        trades=1,
        avg_pnl_pct=0.1,
        win_rate_pct=50.0,
        total_pnl_pct=0.1,
        stop_loss_share_pct=0.0,
    )


def test_cmd_expand_runs_requested_cycles(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "scout-references.json").write_text(
        json.dumps([{"config_id": 1, "trades": 2, "avg_pnl_pct": 0.25}])
    )

    calls = []

    def fake_run_expand(ipc, refs, duration_s):
        calls.append((duration_s, len(refs)))
        return [metric()]

    monkeypatch.setattr(cli, "run_expand", fake_run_expand)

    args = SimpleNamespace(
        config_dir="config",
        db_path="data/optimizer.db",
        duration=60,
        cycles=3,
    )
    cli.cmd_expand(args)

    assert len(calls) == 3
    assert all(duration == 60 for duration, _ in calls)
    assert all(ref_count == 1 for _, ref_count in calls)

    out = capsys.readouterr().out
    assert "[expand] cycle 3/3" in out
    assert "[expand] completed cycles=3" in out


def test_cmd_expand_cumulates_alive_refs_between_cycles(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "scout-references.json").write_text(
        json.dumps([{"config_id": 1, "trades": 2, "avg_pnl_pct": 0.25}])
    )

    seen_refs: list[list[int]] = []

    def fake_run_expand(ipc, refs, duration_s):
        seen_refs.append([r.config_id for r in refs])
        if len(seen_refs) == 1:
            return [metric(config_id=2)]
        return []

    monkeypatch.setattr(cli, "run_expand", fake_run_expand)

    args = SimpleNamespace(
        config_dir="config",
        db_path="data/optimizer.db",
        duration=60,
        cycles=2,
    )
    cli.cmd_expand(args)

    assert seen_refs == [[1], [1, 2]]
