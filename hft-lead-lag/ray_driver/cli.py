"""CLI entry point: scout → expand → ASHA forward-test → promote."""

import argparse
import json
import sys
import time
from pathlib import Path

from .config_store import resolve_config_pairs_for_configs
from .ipc import FleetIPC, RunMetrics
from .scout import run_scout
from .expand import run_expand
from .run_id import generate_run_id

FORWARD_MAX_REFS_HARD_CAP = 256
FORWARD_MAX_CONFIGS_HARD_CAP = 5000
FORWARD_MAX_CONCURRENT_TRIALS = 16


def _asha_terminal_budget_s(max_budget_s: int, grace_period_s: int, reduction_factor: int) -> int:
    """Last full ASHA rung at or below max budget (trials here are not early-pruned)."""
    max_budget_s = max(1, int(max_budget_s))
    grace_period_s = max(1, int(grace_period_s))
    reduction_factor = max(2, int(reduction_factor))
    budget = grace_period_s
    while budget * reduction_factor <= max_budget_s:
        budget *= reduction_factor
    return min(max_budget_s, budget)


class ForwardRuntimePruneCallback:
    """Batch runtime incremental patches for ASHA early-stopped config_ids."""

    def __init__(
        self,
        ipc: FleetIPC,
        run_id: str,
        configs_by_id: dict[int, dict],
        max_budget_s: int,
        grace_period_s: int,
        reduction_factor: int = 2,
        min_patch_interval_s: float = 15.0,
    ):
        self.ipc = ipc
        self.run_id = run_id
        self.configs_by_id = {int(k): dict(v) for k, v in configs_by_id.items()}
        self.active_ids = set(self.configs_by_id.keys())
        self.pending_remove: set[int] = set()
        self.min_patch_interval_s = max(1.0, float(min_patch_interval_s))
        self.last_patch_ts = 0.0
        self.terminal_budget_s = _asha_terminal_budget_s(
            max_budget_s=max_budget_s,
            grace_period_s=grace_period_s,
            reduction_factor=reduction_factor,
        )

    # Tune callback compatibility hooks (no-ops unless explicitly used below).
    def setup(self, **info):
        return None

    def on_step_begin(self, **info):
        return None

    def on_step_end(self, **info):
        return None

    def on_trial_start(self, **info):
        return None

    def on_trial_restore(self, **info):
        return None

    def on_trial_save(self, **info):
        return None

    def on_trial_complete(self, **info):
        return None

    def on_trial_recover(self, **info):
        return None

    def on_trial_error(self, **info):
        return None

    def on_checkpoint(self, **info):
        return None

    def get_state(self):
        return None

    def set_state(self, state):
        return None

    @staticmethod
    def _trial_config_id(trial) -> int | None:
        cfg = getattr(trial, "config", {}) or {}
        raw = cfg.get("config_id")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def _flush_prune(self, force: bool = False) -> None:
        if not self.pending_remove:
            return
        now = time.monotonic()
        if not force and now - self.last_patch_ts < self.min_patch_interval_s:
            return

        removable = sorted(cid for cid in self.pending_remove if cid in self.active_ids)
        if not removable:
            self.pending_remove.clear()
            return

        remaining_ids = sorted(self.active_ids - set(removable))
        if not remaining_ids:
            # Keep at least one config active; drop only extras from pending.
            removable = removable[:-1]
            if not removable:
                return
            remaining_ids = sorted(self.active_ids - set(removable))

        remaining_configs = [self.configs_by_id[cid] for cid in remaining_ids]
        try:
            ack = self.ipc.submit_batch(
                self.run_id,
                remaining_configs,
                timeout_s=15.0,
                mode="incremental",
                changed_config_ids=removable,
            )
        except Exception as exc:
            print(
                "[forward-prune] warning: incremental runtime patch failed "
                f"run_id={self.run_id} remove={len(removable)} err={exc}"
            )
            return

        self.active_ids = set(remaining_ids)
        for cid in removable:
            self.pending_remove.discard(cid)
        self.last_patch_ts = now
        print(
            f"[forward-prune] run_id={self.run_id} removed={len(removable)} "
            f"active={len(self.active_ids)} ack_configs={ack.config_count}"
        )

    def on_trial_result(self, iteration, trials, trial, result, **info):  # noqa: D401
        cfg_id = self._trial_config_id(trial)
        if cfg_id is None or cfg_id not in self.active_ids:
            return
        if not result.get("done"):
            return
        budget_s = float(result.get("time_budget_s", 0.0))
        # Only prune ASHA early-stops; full-budget terminal trials should stay.
        if budget_s >= float(self.terminal_budget_s):
            return
        self.pending_remove.add(cfg_id)
        self._flush_prune(force=False)

    def on_experiment_end(self, trials, **info):  # noqa: D401
        self._flush_prune(force=True)


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


def rows_to_metrics(rows: list[dict]) -> list[RunMetrics]:
    """Convert persisted reference rows to RunMetrics records."""
    return [
        RunMetrics(
            config_id=r["config_id"],
            trades=r["trades"],
            avg_pnl_pct=r["avg_pnl_pct"],
            win_rate_pct=0,
            total_pnl_pct=0,
            stop_loss_share_pct=0,
        )
        for r in rows
    ]


def metrics_to_rows(metrics: list[RunMetrics]) -> list[dict]:
    """Convert RunMetrics to persisted reference row format."""
    return [
        {
            "config_id": m.config_id,
            "trades": m.trades,
            "avg_pnl_pct": m.avg_pnl_pct,
        }
        for m in metrics
    ]


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

    refs = rows_to_metrics(json.loads(refs_path.read_text()))
    cycles = max(1, int(args.cycles))
    total_alive = 0
    for idx in range(cycles):
        print(f"\n[expand] cycle {idx + 1}/{cycles}")
        alive = run_expand(ipc, refs, duration_s=args.duration)
        total_alive += len(alive)
        refs = rows_to_metrics(
            merge_scout_references(metrics_to_rows(refs), alive)
        )
        print(f"[result] cycle {idx + 1}/{cycles}: {len(alive)} expanded configs alive")

    print(
        f"\n[expand] completed cycles={cycles}, duration={args.duration}s, "
        f"total_alive={total_alive}"
    )


def cmd_forward(args):
    """ASHA forward testing on expanded configs."""
    from ray import tune
    from ray.tune.schedulers import ASHAScheduler

    refs_path = Path("data/scout-references.json")
    if not refs_path.exists():
        print("[error] run scout first")
        sys.exit(1)

    ipc = FleetIPC(Path(args.config_dir), Path(args.db_path))
    refs = rows_to_metrics(json.loads(refs_path.read_text()))
    requested_max_refs = int(args.max_refs)
    requested_max_configs = int(args.max_configs)
    max_refs = min(FORWARD_MAX_REFS_HARD_CAP, max(1, requested_max_refs))
    max_configs = min(FORWARD_MAX_CONFIGS_HARD_CAP, max(1, requested_max_configs))
    if max_refs != requested_max_refs or max_configs != requested_max_configs:
        print(
            f"[forward] limits clamped to safe bounds: "
            f"max_refs={max_refs} (requested={requested_max_refs}), "
            f"max_configs={max_configs} (requested={requested_max_configs})"
        )
    from .expand import expand_around_references
    expanded_configs = expand_around_references(
        refs,
        ipc.db_path,
        n_steps=1,
        max_refs=max_refs,
        max_configs=max_configs,
    )
    if not expanded_configs:
        print("[error] forward config set is empty after limits/filtering")
        sys.exit(1)
    resolved_pairs = resolve_config_pairs_for_configs(ipc.db_path, expanded_configs)
    if not resolved_pairs:
        print("[error] no config ids resolved after expansion; abort forward")
        sys.exit(1)
    configs_by_id = {config_id: cfg for config_id, cfg in resolved_pairs}
    config_ids = sorted(configs_by_id.keys())
    configs = [configs_by_id[cfg_id] for cfg_id in config_ids]

    run_id = generate_run_id("forward")
    ipc.clear_ack()
    ack = ipc.submit_batch(run_id, configs)
    print(
        f"[forward] refs_total={len(refs)} refs_selected={min(len(refs), max_refs)} "
        f"configs_prepared={len(expanded_configs)} (max_configs={max_configs})"
    )
    print(
        f"[forward] run_id={run_id} applied_configs={ack.config_count} "
        f"resolved_trials={len(config_ids)}"
    )

    scheduler = ASHAScheduler(
        time_attr="time_budget_s",
        max_t=args.max_budget,
        grace_period=args.grace_period,
        reduction_factor=2,
        mode="max",
        metric="avg_pnl_pct",
    )

    from .trainable import FleetTrial
    prune_callback = ForwardRuntimePruneCallback(
        ipc=ipc,
        run_id=run_id,
        configs_by_id=configs_by_id,
        max_budget_s=args.max_budget,
        grace_period_s=args.grace_period,
        reduction_factor=2,
        min_patch_interval_s=max(10.0, float(args.report_interval) * 2.0),
    )

    tune.run(
        FleetTrial,
        config={
            "run_id": run_id,
            "config_id": tune.grid_search(config_ids),
            "report_interval_s": args.report_interval,
        },
        scheduler=scheduler,
        num_samples=1,
        max_concurrent_trials=FORWARD_MAX_CONCURRENT_TRIALS,
        callbacks=[prune_callback],
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
    e.add_argument("--cycles", type=int, default=1, help="Expand cycles to run sequentially")
    e.set_defaults(func=cmd_expand)

    f = sub.add_parser("forward", help="ASHA forward test")
    f.add_argument("--max-budget", type=int, default=240, help="Max time budget (s)")
    f.add_argument("--grace-period", type=int, default=60, help="ASHA grace period (s)")
    f.add_argument("--report-interval", type=int, default=30, help="Metric report interval (s)")
    f.add_argument(
        "--max-refs",
        type=int,
        default=64,
        help="Max scout references to expand for forward (hard cap: 256)",
    )
    f.add_argument(
        "--max-configs",
        type=int,
        default=1200,
        help="Hard cap on prepared forward configs (safe cap: 5000)",
    )
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
