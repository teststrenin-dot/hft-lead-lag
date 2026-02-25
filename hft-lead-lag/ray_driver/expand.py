"""Expand phase — grow parameter ranges around live scout references."""

import itertools
import time
from pathlib import Path

from .bounds import AXES, FIXED_DEFAULTS
from .config_store import fetch_config_params
from .ipc import FleetIPC, RunMetrics
from .run_id import generate_run_id


def expand_around_references(
    references: list[RunMetrics],
    db_path: Path,
    n_steps: int = 1,
) -> list[dict]:
    """Generate neighbor configs around each reference, clipped to hard bounds."""
    seen: set[tuple] = set()
    expanded: list[dict] = []

    for ref in references:
        center = fetch_config_params(db_path, ref.config_id)
        if not center:
            continue

        per_axis_values: dict[str, list[float]] = {}
        for name, bounds in AXES.items():
            per_axis_values[name] = bounds.expand_around(
                center[name], n_steps
            )

        keys = list(per_axis_values.keys())
        for combo in itertools.product(*(per_axis_values[k] for k in keys)):
            cfg = dict(zip(keys, combo))
            cfg.update(FIXED_DEFAULTS)
            key = tuple(combo)
            if key not in seen:
                seen.add(key)
                expanded.append(cfg)

    return expanded


def run_expand(
    ipc: FleetIPC,
    references: list[RunMetrics],
    duration_s: int = 600,
    min_trades: int = 1,
    n_steps: int = 1,
    max_configs: int = 2000,
) -> list[RunMetrics]:
    """Expand around references, run, return configs with trades."""
    configs = expand_around_references(references, ipc.db_path, n_steps)
    if len(configs) > max_configs:
        stride = len(configs) / max_configs
        configs = [configs[int(i * stride)] for i in range(max_configs)]

    run_id = generate_run_id("expand")
    print(f"[expand] submitting {len(configs)} configs, run_id={run_id}")
    ipc.clear_ack()
    ack = ipc.submit_batch(run_id, configs)
    print(f"[expand] ack: {ack.config_count} configs applied")

    try:
        print(f"[expand] waiting {duration_s}s...")
        time.sleep(duration_s)

        metrics = ipc.query_run_metrics(run_id)
        alive = [m for m in metrics if m.trades >= min_trades]
        print(
            f"[expand] {len(alive)}/{len(metrics)} configs had ≥{min_trades} trades"
        )
        return alive
    finally:
        try:
            ipc.clear_active_run(run_id)
        except Exception as e:
            print(f"[warn] failed to clear active run_id={run_id}: {e}")
