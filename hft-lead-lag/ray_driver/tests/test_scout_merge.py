"""Tests for cumulative scout references merge logic."""

import pytest

from ray_driver.cli import merge_scout_references
from ray_driver.ipc import RunMetrics


def metric(config_id: int, trades: int, avg_pnl_pct: float) -> RunMetrics:
    return RunMetrics(
        config_id=config_id,
        trades=trades,
        avg_pnl_pct=avg_pnl_pct,
        win_rate_pct=0.0,
        total_pnl_pct=0.0,
        stop_loss_share_pct=0.0,
    )


def test_merge_scout_references_adds_new_configs():
    existing = [{"config_id": 10, "trades": 3, "avg_pnl_pct": 0.2}]
    fresh = [metric(11, 2, 0.5)]

    merged = merge_scout_references(existing, fresh)
    by_id = {row["config_id"]: row for row in merged}

    assert set(by_id.keys()) == {10, 11}
    assert by_id[10]["trades"] == 3
    assert by_id[10]["avg_pnl_pct"] == pytest.approx(0.2)
    assert by_id[11]["trades"] == 2
    assert by_id[11]["avg_pnl_pct"] == pytest.approx(0.5)


def test_merge_scout_references_uses_trade_weighted_average():
    existing = [{"config_id": 42, "trades": 3, "avg_pnl_pct": 1.0}]
    fresh = [metric(42, 1, -1.0)]

    merged = merge_scout_references(existing, fresh)
    by_id = {row["config_id"]: row for row in merged}

    # (3 * 1.0 + 1 * -1.0) / 4 = 0.5
    assert by_id[42]["trades"] == 4
    assert by_id[42]["avg_pnl_pct"] == pytest.approx(0.5)
