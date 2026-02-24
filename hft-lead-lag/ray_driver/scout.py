"""Scout phase — coarse grid scan to find parameter regions with trades."""

import itertools
import random
import time

from .bounds import AXES, FIXED_DEFAULTS
from .ipc import FleetIPC, RunMetrics

MAX_SCOUT_CONFIGS = 3000


def generate_scout_configs(max_configs: int = MAX_SCOUT_CONFIGS) -> list[dict]:
    """Generate scout configs via Latin Hypercube sampling from init ranges."""
    axis_values = {name: ax.init_values() for name, ax in AXES.items()}
    keys = list(axis_values.keys())

    # Full cartesian product size
    full_size = 1
    for v in axis_values.values():
        full_size *= len(v)

    if full_size <= max_configs:
        # Small enough — use full grid
        configs = []
        for combo in itertools.product(*(axis_values[k] for k in keys)):
            cfg = dict(zip(keys, combo))
            cfg.update(FIXED_DEFAULTS)
            configs.append(cfg)
        return configs

    # Latin Hypercube: stratified sampling per axis
    configs = []
    for _ in range(max_configs):
        cfg = {k: random.choice(axis_values[k]) for k in keys}
        cfg.update(FIXED_DEFAULTS)
        configs.append(cfg)

    # Deduplicate
    seen = set()
    unique = []
    for cfg in configs:
        key = tuple(cfg[k] for k in keys)
        if key not in seen:
            seen.add(key)
            unique.append(cfg)
    return unique


def run_scout(
    ipc: FleetIPC,
    duration_s: int = 600,
    min_trades: int = 1,
) -> tuple[str, list[RunMetrics]]:
    """Submit scout grid, wait, return configs that produced trades."""
    configs = generate_scout_configs()
    run_id = f"scout-{int(time.time())}"

    print(f"[scout] submitting {len(configs)} configs, run_id={run_id}")
    ipc.clear_ack()
    ack = ipc.submit_batch(run_id, configs)
    print(f"[scout] ack: {ack.config_count} configs applied")

    print(f"[scout] waiting {duration_s}s for trades to accumulate...")
    time.sleep(duration_s)

    metrics = ipc.query_run_metrics(run_id)
    alive = [m for m in metrics if m.trades >= min_trades]

    print(
        f"[scout] {len(alive)}/{len(metrics)} configs had ≥{min_trades} trades"
    )
    return run_id, alive
