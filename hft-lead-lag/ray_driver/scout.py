"""Scout phase — coarse grid scan to find parameter regions with trades."""

import itertools
import time

from .bounds import AXES, FIXED_DEFAULTS
from .ipc import FleetIPC, RunMetrics


def generate_scout_configs() -> list[dict]:
    """Cartesian product over init ranges for all axes."""
    axis_values = {name: ax.init_values() for name, ax in AXES.items()}
    keys = list(axis_values.keys())
    configs = []
    for combo in itertools.product(*(axis_values[k] for k in keys)):
        cfg = dict(zip(keys, combo))
        cfg.update(FIXED_DEFAULTS)
        configs.append(cfg)
    return configs


def run_scout(
    ipc: FleetIPC,
    duration_s: int = 600,
    min_trades: int = 1,
) -> list[RunMetrics]:
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
    return alive
