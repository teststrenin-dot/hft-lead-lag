"""Tests for config-id resolution from concrete config payloads."""

import sqlite3

from ray_driver.config_store import resolve_config_ids_for_configs


def test_resolve_config_ids_for_configs_matches_rows(tmp_path):
    db_path = tmp_path / "optimizer.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE configs (
            id INTEGER PRIMARY KEY,
            strategy_kind TEXT NOT NULL DEFAULT 'baseline_gap',
            spike_threshold_bps REAL NOT NULL,
            target_ratio REAL NOT NULL,
            stop_loss_bps REAL NOT NULL,
            max_hold_ms INTEGER NOT NULL,
            max_spread_bps REAL NOT NULL,
            trailing_decay_ratio REAL NOT NULL,
            baseline_window_ms INTEGER NOT NULL,
            fill_delay_ms INTEGER NOT NULL,
            cooldown_ms INTEGER NOT NULL,
            warmup_ms INTEGER NOT NULL,
            quote_freshness_ms INTEGER NOT NULL,
            taker_fee REAL NOT NULL,
            min_baseline_samples INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO configs (
            id, strategy_kind, spike_threshold_bps, target_ratio, stop_loss_bps,
            max_hold_ms, max_spread_bps, trailing_decay_ratio, baseline_window_ms,
            fill_delay_ms, cooldown_ms, warmup_ms, quote_freshness_ms,
            taker_fee, min_baseline_samples
        ) VALUES (?, 'baseline_gap', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            101,
            30.0,
            0.3,
            10.0,
            10_000,
            3.0,
            0.3,
            20_000,
            6,
            3_000,
            30_000,
            1_000,
            0.0005,
            20,
        ),
    )
    conn.commit()
    conn.close()

    configs = [
        {
            "spike_threshold_bps": 30.0,
            "target_ratio": 0.3,
            "stop_loss_bps": 10.0,
            "max_hold_ms": 10_000,
            "max_spread_bps": 3.0,
            "trailing_decay_ratio": 0.3,
            "baseline_window_ms": 20_000,
            "fill_delay_ms": 6,
            "cooldown_ms": 3_000,
            "warmup_ms": 30_000,
            "quote_freshness_ms": 1_000,
            "taker_fee": 0.0005,
            "min_baseline_samples": 20,
        }
    ]
    resolved = resolve_config_ids_for_configs(db_path, configs)
    assert resolved == [101]
