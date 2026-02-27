"""Ray Tune Trainable — wraps a fleet trial as a long-running ASHA-compatible trial."""

import time
from pathlib import Path

from ray import tune

from .ipc import FleetIPC


class FleetTrial(tune.Trainable):
    """
    One ASHA trial = one config_id from the currently active forward run.

    Reports intermediate metrics at report_interval_s intervals.
    ASHA uses time_budget_s as the time attribute for successive halving.
    """

    def setup(self, config: dict):
        config_dir = Path(config.get("config_dir", "config"))
        db_path = Path(config.get("db_path", "data/optimizer.db"))
        self.ipc = FleetIPC(config_dir=config_dir, db_path=db_path)
        self.run_id = config["run_id"]
        self.config_id = int(config["config_id"])
        self.report_interval_s = config.get("report_interval_s", 60)
        self.elapsed_s = 0

    def step(self) -> dict:
        """Sleep for one reporting interval, then query metrics for this config."""
        time.sleep(self.report_interval_s)
        self.elapsed_s += self.report_interval_s

        metrics = self.ipc.query_run_metrics(self.run_id)
        metric = next(
            (row for row in metrics if int(row.config_id) == self.config_id),
            None,
        )
        if metric is None:
            trades = 0
            avg_pnl = 0.0
            avg_win_rate = 0.0
            total_pnl = 0.0
            stop_loss_share = 0.0
        else:
            trades = int(metric.trades)
            avg_pnl = float(metric.avg_pnl_pct)
            avg_win_rate = float(metric.win_rate_pct)
            total_pnl = float(metric.total_pnl_pct)
            stop_loss_share = float(metric.stop_loss_share_pct)

        return {
            "time_budget_s": self.elapsed_s,
            "config_id": self.config_id,
            "total_trades": trades,
            "configs_with_trades": 1 if trades > 0 else 0,
            "avg_pnl_pct": avg_pnl,
            "avg_win_rate_pct": avg_win_rate,
            "total_pnl_pct": total_pnl,
            "stop_loss_share_pct": stop_loss_share,
        }
