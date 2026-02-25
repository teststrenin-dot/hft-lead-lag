"""Read config parameter sets from optimizer.db."""

import sqlite3
from pathlib import Path

CONFIG_PARAM_KEYS = [
    "spike_threshold_bps",
    "target_ratio",
    "stop_loss_bps",
    "max_hold_ms",
    "max_spread_bps",
    "trailing_decay_ratio",
    "baseline_window_ms",
]

_SELECT_CONFIG_PARAMS = """SELECT spike_threshold_bps, target_ratio, stop_loss_bps,
                                  max_hold_ms, max_spread_bps, trailing_decay_ratio,
                                  baseline_window_ms
                           FROM configs WHERE id = ?"""


def fetch_config_params(db_path: Path, config_id: int) -> dict | None:
    """Load one config parameter set by config_id."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        row = conn.execute(_SELECT_CONFIG_PARAMS, (config_id,)).fetchone()
        if not row:
            return None
        return dict(zip(CONFIG_PARAM_KEYS, row))
    finally:
        conn.close()


def fetch_config_params_many(db_path: Path, config_ids: list[int]) -> dict[int, dict]:
    """Load many config parameter sets by config_id in one connection."""
    if not config_ids:
        return {}
    unique_ids = sorted(set(config_ids))
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        rows: dict[int, dict] = {}
        for config_id in unique_ids:
            row = conn.execute(_SELECT_CONFIG_PARAMS, (config_id,)).fetchone()
            if row:
                rows[config_id] = dict(zip(CONFIG_PARAM_KEYS, row))
        return rows
    finally:
        conn.close()
