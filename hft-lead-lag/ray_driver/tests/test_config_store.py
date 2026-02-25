"""Tests for shared config parameter loaders used by expand/promote."""

import sqlite3
from pathlib import Path

from ray_driver.config_store import fetch_config_params, fetch_config_params_many
from ray_driver.ipc import RunMetrics
from ray_driver.promote import promote_top_configs


def _seed_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """CREATE TABLE configs (
                id INTEGER PRIMARY KEY,
                spike_threshold_bps REAL NOT NULL,
                target_ratio REAL NOT NULL,
                stop_loss_bps REAL NOT NULL,
                max_hold_ms INTEGER NOT NULL,
                max_spread_bps REAL NOT NULL,
                trailing_decay_ratio REAL NOT NULL,
                baseline_window_ms INTEGER NOT NULL
            )"""
        )
        conn.executemany(
            """INSERT INTO configs (
                id, spike_threshold_bps, target_ratio, stop_loss_bps,
                max_hold_ms, max_spread_bps, trailing_decay_ratio, baseline_window_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (1, 30.0, 0.3, 8.0, 5_000, 3.0, 0.3, 10_000),
                (2, 50.0, 0.5, 15.0, 10_000, 5.0, 0.7, 20_000),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_fetch_config_params_returns_none_for_missing_id(tmp_path):
    db_path = tmp_path / "optimizer.db"
    _seed_db(db_path)
    assert fetch_config_params(db_path, 999) is None


def test_fetch_config_params_many_returns_only_existing_configs(tmp_path):
    db_path = tmp_path / "optimizer.db"
    _seed_db(db_path)
    rows = fetch_config_params_many(db_path, [2, 1, 1, 999])
    assert set(rows.keys()) == {1, 2}
    assert rows[1]["spike_threshold_bps"] == 30.0
    assert rows[2]["baseline_window_ms"] == 20_000


class _FakeIPC:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def query_run_metrics(self, run_id: str):
        assert run_id == "run-1"
        return [
            RunMetrics(
                config_id=2,
                trades=10,
                avg_pnl_pct=0.2,
                win_rate_pct=55.0,
                total_pnl_pct=2.0,
                stop_loss_share_pct=10.0,
            ),
            RunMetrics(
                config_id=1,
                trades=8,
                avg_pnl_pct=0.1,
                win_rate_pct=50.0,
                total_pnl_pct=0.8,
                stop_loss_share_pct=20.0,
            ),
        ]


def test_promote_top_configs_uses_shared_loader_and_preserves_order(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "optimizer.db"
    _seed_db(db_path)
    ipc = _FakeIPC(db_path)

    promoted = promote_top_configs(
        ipc,
        run_id="run-1",
        top_k=2,
        min_trades=1,
        min_avg_pnl=0.0,
    )

    assert [row["config_id"] for row in promoted] == [2, 1]
    assert promoted[0]["params"]["spike_threshold_bps"] == 50.0
    assert promoted[1]["params"]["baseline_window_ms"] == 10_000
