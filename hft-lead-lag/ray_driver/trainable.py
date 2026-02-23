"""Ray Tune Trainable — wraps a fleet trial as a long-running ASHA-compatible trial."""

import time

from ray import tune

from .ipc import FleetIPC


class FleetTrial(tune.Trainable):
    """
    One ASHA trial = one config batch running on the live fleet.

    Reports intermediate metrics at report_interval_s intervals.
    ASHA uses time_budget_s as the time attribute for successive halving.
    """

    def setup(self, config: dict):
        self.ipc = FleetIPC()
        self.trial_configs = config["configs"]
        self.run_id = config["run_id"]
        self.report_interval_s = config.get("report_interval_s", 60)
        self.elapsed_s = 0

        self.ipc.clear_ack()
        self.ipc.submit_batch(self.run_id, self.trial_configs)

    def step(self) -> dict:
        """Sleep for one reporting interval, then query metrics."""
        time.sleep(self.report_interval_s)
        self.elapsed_s += self.report_interval_s

        metrics = self.ipc.query_run_metrics(self.run_id)
        total_trades = sum(m.trades for m in metrics)
        configs_with_trades = sum(1 for m in metrics if m.trades > 0)

        if total_trades > 0:
            avg_pnl = sum(m.avg_pnl_pct * m.trades for m in metrics) / total_trades
            avg_win_rate = (
                sum(m.win_rate_pct * m.trades for m in metrics) / total_trades
            )
        else:
            avg_pnl = 0.0
            avg_win_rate = 0.0

        return {
            "time_budget_s": self.elapsed_s,
            "total_trades": total_trades,
            "configs_with_trades": configs_with_trades,
            "avg_pnl_pct": avg_pnl,
            "avg_win_rate_pct": avg_win_rate,
        }
