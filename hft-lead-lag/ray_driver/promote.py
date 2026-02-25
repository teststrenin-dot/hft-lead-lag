"""Promote top configs from a completed run into a reviewable export."""

import json
from pathlib import Path

from .config_store import fetch_config_params_many
from .ipc import FleetIPC


def promote_top_configs(
    ipc: FleetIPC,
    run_id: str,
    top_k: int = 50,
    min_trades: int = 5,
    min_avg_pnl: float = 0.0,
) -> list[dict]:
    """
    Read top-K configs from a run, filter by quality, export as JSON.

    Does NOT write to runtime-grid.toml — outputs a JSON file
    that can be reviewed before manual promotion.
    """
    metrics = ipc.query_run_metrics(run_id)
    qualified = [
        m for m in metrics
        if m.trades >= min_trades and m.avg_pnl_pct >= min_avg_pnl
    ]
    qualified.sort(key=lambda m: m.avg_pnl_pct, reverse=True)
    top = qualified[:top_k]

    params_map = fetch_config_params_many(ipc.db_path, [m.config_id for m in top])
    promoted = []
    for m in top:
        params = params_map.get(m.config_id)
        if not params:
            continue
        promoted.append(
            {
                "config_id": m.config_id,
                "trades": m.trades,
                "avg_pnl_pct": m.avg_pnl_pct,
                "win_rate_pct": m.win_rate_pct,
                "params": params,
            }
        )

    out = Path(f"data/promoted-{run_id}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(promoted, indent=2))
    print(f"[promote] {len(promoted)} configs saved to {out}")
    return promoted
