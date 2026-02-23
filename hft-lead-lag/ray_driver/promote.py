"""Promote top configs from a completed run into a reviewable export."""

import json
import sqlite3
from pathlib import Path

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

    conn = sqlite3.connect(
        f"file:{ipc.db_path}?mode=ro", uri=True, timeout=5.0
    )
    try:
        promoted = []
        for m in top:
            row = conn.execute(
                """SELECT spike_threshold_bps, target_ratio, stop_loss_bps,
                          max_hold_ms, max_spread_bps, trailing_decay_ratio,
                          baseline_window_ms
                   FROM configs WHERE id = ?""",
                (m.config_id,),
            ).fetchone()
            if row:
                promoted.append({
                    "config_id": m.config_id,
                    "trades": m.trades,
                    "avg_pnl_pct": m.avg_pnl_pct,
                    "win_rate_pct": m.win_rate_pct,
                    "params": {
                        "spike_threshold_bps": row[0],
                        "target_ratio": row[1],
                        "stop_loss_bps": row[2],
                        "max_hold_ms": row[3],
                        "max_spread_bps": row[4],
                        "trailing_decay_ratio": row[5],
                        "baseline_window_ms": row[6],
                    },
                })
    finally:
        conn.close()

    out = Path(f"data/promoted-{run_id}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(promoted, indent=2))
    print(f"[promote] {len(promoted)} configs saved to {out}")
    return promoted
