"""File-based IPC with Rust fleet + SQLite metrics reader."""

import json
import sqlite3
import time
from dataclasses import dataclass, fields
from pathlib import Path

CONFIG_ID_CONTRACT_VERSION = 1


@dataclass
class TrialAck:
    run_id: str
    applied_at_ms: int
    config_count: int
    drained_trades: int
    status: str = "ok"
    error: str | None = None
    submission_id: str | None = None

    @classmethod
    def from_payload(cls, payload: dict) -> "TrialAck":
        allowed_fields = {field.name for field in fields(cls)}
        filtered_payload = {
            key: value for key, value in payload.items() if key in allowed_fields
        }
        return cls(**filtered_payload)


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
        self.batch_queue_dir = config_dir / "trial-batches"
        self.control_path = config_dir / "trial-control.json"
        self.ack_path = config_dir / ".trial-ack"
        self.ack_queue_dir = config_dir / "trial-acks"

    def submit_batch(
        self,
        run_id: str,
        configs: list[dict],
        timeout_s: float = 30.0,
        mode: str | None = None,
        changed_config_ids: list[int] | None = None,
        symbols: list[str] | None = None,
        allow_run_id_takeover: bool = False,
    ) -> TrialAck:
        """Write trial-batch.json and wait for .trial-ack from Rust."""
        self.batch_queue_dir.mkdir(parents=True, exist_ok=True)
        self.ack_queue_dir.mkdir(parents=True, exist_ok=True)
        submission_id = f"{run_id}-{time.time_ns()}"
        batch = {
            "run_id": run_id,
            "configs": configs,
            "config_id_contract_version": CONFIG_ID_CONTRACT_VERSION,
            "submission_id": submission_id,
        }
        if mode:
            batch["mode"] = mode
        if changed_config_ids:
            batch["changed_config_ids"] = changed_config_ids
        if symbols:
            batch["symbols"] = symbols
        if allow_run_id_takeover:
            batch["allow_run_id_takeover"] = True
        batch_path = self.batch_queue_dir / f"{submission_id}.json"
        ack_path = self.ack_queue_dir / f"{submission_id}.json"
        tmp = batch_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(batch, indent=2))
        tmp.rename(batch_path)  # atomic on same FS

        return self._wait_ack(run_id, timeout_s, ack_path=ack_path)

    def _wait_ack(
        self, run_id: str, timeout_s: float, ack_path: Path | None = None
    ) -> TrialAck:
        deadline = time.monotonic() + timeout_s
        target_ack = ack_path or self.ack_path
        submission_scoped = ack_path is not None
        while time.monotonic() < deadline:
            if target_ack.exists():
                try:
                    ack = json.loads(target_ack.read_text())
                    if ack.get("run_id") != run_id:
                        if submission_scoped:
                            self._consume_ack_file(target_ack)
                            raise RuntimeError(
                                f"Ack run_id mismatch for {target_ack.name}: "
                                f"expected {run_id}, got {ack.get('run_id')!r}"
                            )
                        continue

                    if ack.get("status") == "error":
                        self._consume_ack_file(target_ack)
                        raise RuntimeError(
                            f"Batch rejected for run_id={run_id}: "
                            f"{ack.get('error', 'unknown error')}"
                        )

                    parsed_ack = TrialAck.from_payload(ack)
                    self._consume_ack_file(target_ack)
                    return parsed_ack
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass
            time.sleep(0.5)
        raise TimeoutError(f"No ack for run_id={run_id} within {timeout_s}s")

    @staticmethod
    def _consume_ack_file(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

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
        """Remove stale legacy ack file."""
        self.ack_path.unlink(missing_ok=True)

    def clear_active_run(self, run_id: str | None = None):
        """Request runtime to clear current run_id (best effort)."""
        payload: dict[str, object] = {"clear_run_id": True}
        if run_id:
            payload["run_id"] = run_id
        tmp = self.control_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.rename(self.control_path)  # atomic on same FS
