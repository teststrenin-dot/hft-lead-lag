"""CLI entry point: scout → expand → ASHA forward-test → promote."""

import argparse
import json
import sys
import time
from pathlib import Path

from .ipc import FleetIPC, RunMetrics
from .scout import run_scout
from .expand import run_expand


def load_scout_references(path: Path) -> list[dict]:
    """Load scout references JSON as a list; return empty on malformed input."""
    if not path.exists():
        return []

    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        print(f"[warn] malformed {path}; starting fresh")
        return []

    if not isinstance(payload, list):
        print(f"[warn] expected list in {path}; starting fresh")
        return []
    return payload


def merge_scout_references(
    existing_rows: list[dict],
    fresh_metrics: list[RunMetrics],
) -> list[dict]:
    """Merge references by config_id with trade-weighted avg pnl."""
    agg: dict[int, dict[str, float]] = {}

    for row in existing_rows:
        try:
            config_id = int(row["config_id"])
            trades = int(row.get("trades", 0))
            avg_pnl_pct = float(row.get("avg_pnl_pct", 0.0))
        except (KeyError, TypeError, ValueError):
            continue

        if trades <= 0:
            continue

        total_pnl = avg_pnl_pct * trades
        cur = agg.setdefault(config_id, {"trades": 0.0, "total_pnl": 0.0})
        cur["trades"] += trades
        cur["total_pnl"] += total_pnl

    for metric in fresh_metrics:
        if metric.trades <= 0:
            continue
        cur = agg.setdefault(metric.config_id, {"trades": 0.0, "total_pnl": 0.0})
        cur["trades"] += metric.trades
        cur["total_pnl"] += metric.avg_pnl_pct * metric.trades

    merged = []
    for config_id, stats in agg.items():
        trades = int(stats["trades"])
        if trades <= 0:
            continue
        merged.append(
            {
                "config_id": config_id,
                "trades": trades,
                "avg_pnl_pct": stats["total_pnl"] / trades,
            }
        )

    merged.sort(key=lambda row: (row["avg_pnl_pct"], row["trades"]), reverse=True)
    return merged


def cmd_scout(args):
    ipc = FleetIPC(Path(args.config_dir), Path(args.db_path))
    cycles = max(1, int(args.cycles))
    out = Path("data/scout-references.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    merged = load_scout_references(out)

    for idx in range(cycles):
        print(f"\n[scout] cycle {idx + 1}/{cycles}")
        run_id, alive = run_scout(ipc, duration_s=args.duration)
        print(f"\n[result] {len(alive)} reference configs found")
        for m in sorted(alive, key=lambda x: x.avg_pnl_pct, reverse=True)[:20]:
            print(
                f"  config_id={m.config_id} trades={m.trades} "
                f"avg_pnl={m.avg_pnl_pct:.4f}% win={m.win_rate_pct:.1f}%"
            )

        prev_len = len(merged)
        merged = merge_scout_references(merged, alive)
        out.write_text(json.dumps(merged, indent=2))
        print(
            f"[saved] {out} run_id={run_id} cycle={idx + 1}/{cycles} "
            f"(prev={prev_len} new={len(alive)} total={len(merged)})"
        )

    print(
        f"\n[scout] completed cycles={cycles}, duration={args.duration}s, "
        f"cumulative_refs={len(merged)}"
    )


def cmd_expand(args):
    ipc = FleetIPC(Path(args.config_dir), Path(args.db_path))
    refs_path = Path("data/scout-references.json")
    if not refs_path.exists():
        print("[error] run scout first — no data/scout-references.json")
        sys.exit(1)

    refs = [
        RunMetrics(config_id=r["config_id"], trades=r["trades"],
                   avg_pnl_pct=r["avg_pnl_pct"], win_rate_pct=0,
                   total_pnl_pct=0, stop_loss_share_pct=0)
        for r in json.loads(refs_path.read_text())
    ]
    alive = run_expand(ipc, refs, duration_s=args.duration)
    print(f"\n[result] {len(alive)} expanded configs alive")


def cmd_forward(args):
    """ASHA forward testing on expanded configs."""
    from ray import tune
    from ray.tune.schedulers import ASHAScheduler

    refs_path = Path("data/scout-references.json")
    if not refs_path.exists():
        print("[error] run scout first")
        sys.exit(1)

    ipc = FleetIPC(Path(args.config_dir), Path(args.db_path))
    refs = [
        RunMetrics(config_id=r["config_id"], trades=r["trades"],
                   avg_pnl_pct=r["avg_pnl_pct"], win_rate_pct=0,
                   total_pnl_pct=0, stop_loss_share_pct=0)
        for r in json.loads(refs_path.read_text())
    ]
    from .expand import expand_around_references
    configs = expand_around_references(refs, ipc.db_path, n_steps=1)

    scheduler = ASHAScheduler(
        time_attr="time_budget_s",
        max_t=args.max_budget,
        grace_period=args.grace_period,
        reduction_factor=2,
        mode="max",
        metric="avg_pnl_pct",
    )

    from .trainable import FleetTrial

    tune.run(
        FleetTrial,
        config={
            "configs": configs,
            "run_id": f"forward-{int(time.time())}",
            "report_interval_s": args.report_interval,
        },
        scheduler=scheduler,
        num_samples=1,
        verbose=1,
    )


def cmd_promote(args):
    """Export top configs from a completed run."""
    from .promote import promote_top_configs
    ipc = FleetIPC(Path(args.config_dir), Path(args.db_path))
    promoted = promote_top_configs(
        ipc, args.run_id, top_k=args.top_k,
        min_trades=args.min_trades, min_avg_pnl=args.min_pnl,
    )
    print(f"\n[result] {len(promoted)} configs promoted")


def main():
    p = argparse.ArgumentParser(description="Ray fleet optimizer")
    p.add_argument("--config-dir", default="config")
    p.add_argument("--db-path", default="data/optimizer.db")

    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("scout", help="Coarse scan for reference configs")
    s.add_argument("--duration", type=int, default=600, help="Scout duration (s)")
    s.add_argument("--cycles", type=int, default=1, help="Scout cycles to run sequentially")
    s.set_defaults(func=cmd_scout)

    e = sub.add_parser("expand", help="Expand around scout references")
    e.add_argument("--duration", type=int, default=600, help="Expand duration (s)")
    e.set_defaults(func=cmd_expand)

    f = sub.add_parser("forward", help="ASHA forward test")
    f.add_argument("--max-budget", type=int, default=240, help="Max time budget (s)")
    f.add_argument("--grace-period", type=int, default=60, help="ASHA grace period (s)")
    f.add_argument("--report-interval", type=int, default=30, help="Metric report interval (s)")
    f.set_defaults(func=cmd_forward)

    pr = sub.add_parser("promote", help="Export top configs from a run")
    pr.add_argument("run_id", help="Run ID to promote from")
    pr.add_argument("--top-k", type=int, default=50)
    pr.add_argument("--min-trades", type=int, default=5)
    pr.add_argument("--min-pnl", type=float, default=0.0)
    pr.set_defaults(func=cmd_promote)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
