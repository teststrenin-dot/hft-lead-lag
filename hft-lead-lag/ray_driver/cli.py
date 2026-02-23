"""CLI entry point: scout → expand → ASHA forward-test → promote."""

import argparse
import json
import sys
import time
from pathlib import Path

from .ipc import FleetIPC, RunMetrics
from .scout import run_scout
from .expand import run_expand


def cmd_scout(args):
    ipc = FleetIPC(Path(args.config_dir), Path(args.db_path))
    alive = run_scout(ipc, duration_s=args.duration)
    print(f"\n[result] {len(alive)} reference configs found")
    for m in sorted(alive, key=lambda x: x.avg_pnl_pct, reverse=True)[:20]:
        print(
            f"  config_id={m.config_id} trades={m.trades} "
            f"avg_pnl={m.avg_pnl_pct:.4f}% win={m.win_rate_pct:.1f}%"
        )
    out = Path("data/scout-references.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(
        [{"config_id": m.config_id, "trades": m.trades,
          "avg_pnl_pct": m.avg_pnl_pct} for m in alive],
        indent=2,
    ))
    print(f"[saved] {out}")


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
    s.set_defaults(func=cmd_scout)

    e = sub.add_parser("expand", help="Expand around scout references")
    e.add_argument("--duration", type=int, default=600, help="Expand duration (s)")
    e.set_defaults(func=cmd_expand)

    f = sub.add_parser("forward", help="ASHA forward test")
    f.add_argument("--max-budget", type=int, default=21600, help="Max time budget (s)")
    f.add_argument("--grace-period", type=int, default=600, help="ASHA grace period (s)")
    f.add_argument("--report-interval", type=int, default=60, help="Metric report interval (s)")
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
