"""Read config parameter sets from optimizer.db."""

import sqlite3
from pathlib import Path

from .bounds import FIXED_DEFAULTS

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

CONFIG_ROW_KEYS = [
    "spike_threshold_bps",
    "target_ratio",
    "stop_loss_bps",
    "max_hold_ms",
    "max_spread_bps",
    "trailing_decay_ratio",
    "baseline_window_ms",
    "fill_delay_ms",
    "cooldown_ms",
    "warmup_ms",
    "quote_freshness_ms",
    "taker_fee",
    "min_baseline_samples",
]

_SELECT_CONFIG_ROWS = """SELECT id, spike_threshold_bps, target_ratio, stop_loss_bps,
                                max_hold_ms, max_spread_bps, trailing_decay_ratio,
                                baseline_window_ms, fill_delay_ms, cooldown_ms,
                                warmup_ms, quote_freshness_ms, taker_fee, min_baseline_samples
                         FROM configs"""


def _cfg_tuple_key(cfg: dict) -> tuple:
    normalized = dict(FIXED_DEFAULTS)
    normalized.update(cfg)
    return tuple(normalized[k] for k in CONFIG_ROW_KEYS)


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


def resolve_config_pairs_for_configs(
    db_path: Path, configs: list[dict]
) -> list[tuple[int, dict]]:
    """Resolve `(config_id, config_payload)` pairs by exact parameter tuples."""
    if not configs:
        return []

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = conn.execute(_SELECT_CONFIG_ROWS).fetchall()
    finally:
        conn.close()

    by_tuple: dict[tuple, int] = {}
    for row in rows:
        cfg_id = int(row[0])
        by_tuple[tuple(row[1:])] = cfg_id

    resolved: list[tuple[int, dict]] = []
    seen_ids: set[int] = set()
    for cfg in configs:
        cfg_id = by_tuple.get(_cfg_tuple_key(cfg))
        if cfg_id is None:
            continue
        if cfg_id in seen_ids:
            continue
        seen_ids.add(cfg_id)
        resolved.append((cfg_id, dict(cfg)))
    return resolved


def resolve_config_ids_for_configs(db_path: Path, configs: list[dict]) -> list[int]:
    """Resolve DB config IDs for concrete config payloads by exact parameter tuples."""
    resolved = resolve_config_pairs_for_configs(db_path, configs)
    return [config_id for config_id, _cfg in resolved]
