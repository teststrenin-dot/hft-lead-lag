"""File-based IPC with Rust fleet + SQLite metrics reader."""

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import fcntl


@dataclass
class TrialAck:
    run_id: str
    applied_at_ms: int
    config_count: int
    drained_trades: int
    status: str = "ok"
    error: str | None = None


@dataclass
class RunMetrics:
    config_id: int
    trades: int
    avg_pnl_pct: float
    win_rate_pct: float
    total_pnl_pct: float
    stop_loss_share_pct: float


class FleetIPC:
    """Communicate with the Rust fleet via file IPC + SQLite reads."""

    def __init__(
        self,
        config_dir: Path = Path("config"),
        db_path: Path = Path("data/optimizer.db"),
    ):
        self.config_dir = config_dir
        self.db_path = db_path
        self.batch_path = config_dir / "trial-batch.json"
        self.control_path = config_dir / "trial-control.json"
        self.ack_path = config_dir / ".trial-ack"
        self.lock_path = config_dir / ".trial-lock"

    def submit_batch(
        self,
        run_id: str,
        configs: list[dict],
        timeout_s: float = 30.0,
    ) -> TrialAck:
        """Write trial-batch.json and wait for .trial-ack from Rust."""
        with self._submission_lock():
            batch = {"run_id": run_id, "configs": configs}
            tmp = self.batch_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(batch, indent=2))
            tmp.rename(self.batch_path)  # atomic on same FS

            return self._wait_ack(run_id, timeout_s)

    @contextmanager
    def _submission_lock(self):
        """Serialize batch submissions across parallel driver processes."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        lock_file = self.lock_path.open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()

    def _wait_ack(self, run_id: str, timeout_s: float) -> TrialAck:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.ack_path.exists():
                try:
                    ack = json.loads(self.ack_path.read_text())
                    if ack.get("run_id") == run_id:
                        if ack.get("status") == "error":
                            raise RuntimeError(
                                f"Batch rejected for run_id={run_id}: "
                                f"{ack.get('error', 'unknown error')}"
                            )
                        return TrialAck(**ack)
                except (json.JSONDecodeError, KeyError):
                    pass
            time.sleep(0.5)
        raise TimeoutError(f"No ack for run_id={run_id} within {timeout_s}s")

    def query_run_metrics(self, run_id: str) -> list[RunMetrics]:
        """Read per-config metrics for a run from optimizer.db."""
        conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, timeout=5.0
        )
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            rows = conn.execute(
                """
                SELECT config_id,
                       COUNT(*) as trades,
                       AVG(pnl_pct) as avg_pnl,
                       SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) * 100.0
                           / COUNT(*) as win_rate,
                       SUM(pnl_pct) as total_pnl,
                       SUM(CASE WHEN exit_reason = 'stop_loss' THEN 1 ELSE 0 END)
                           * 100.0 / COUNT(*) as sl_share
                FROM trades
                WHERE run_id = ?
                GROUP BY config_id
                """,
                (run_id,),
            ).fetchall()
            return [
                RunMetrics(
                    config_id=r[0], trades=r[1], avg_pnl_pct=r[2],
                    win_rate_pct=r[3], total_pnl_pct=r[4],
                    stop_loss_share_pct=r[5],
                )
                for r in rows
            ]
        finally:
            conn.close()

    def total_trades_for_run(self, run_id: str) -> int:
        """Quick count of trades for a run."""
        conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, timeout=5.0
        )
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE run_id = ?", (run_id,)
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def clear_ack(self):
        """Remove stale ack file."""
        self.ack_path.unlink(missing_ok=True)

    def clear_active_run(self, run_id: str | None = None):
        """Request runtime to clear current run_id (best effort)."""
        payload: dict[str, object] = {"clear_run_id": True}
        if run_id:
            payload["run_id"] = run_id
        tmp = self.control_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.rename(self.control_path)  # atomic on same FS
