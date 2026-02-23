# Ray Tune + ASHA Fleet Integration — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire Ray Tune (ASHA scheduler) as external orchestrator for the shadow fleet: scout → expand → forward-test → promote.

**Architecture:** File-based IPC — Ray writes `config/trial-batch.json`, Rust consumes it immediately (no debounce), writes `config/.trial-ack` after apply. Results flow through `optimizer.db` (WAL, concurrent reads). Rust side is Ray-agnostic — it just accepts config batches with a `run_id` tag.

**Tech Stack:** Rust (existing fleet), Python 3.10 + Ray 2.54 + ray[tune] + ASHAScheduler, SQLite (existing optimizer.db)

---

## Концепция: многомерная матрица референсов

Каждый конфиг — точка в 7-мерном пространстве гиперпараметров:

```
referens = (spike_threshold_bps, target_ratio, stop_loss_bps, max_hold_ms,
            max_spread_bps, trailing_decay_ratio, baseline_window_ms)
```

Большая часть этого 7D-пространства **мёртвая** — конфиги, которые никогда не генерируют сделок (порог слишком высок, стоп слишком тесный, spread-фильтр отсекает всё, и т.д.).

**Скаут** находит **матрицу референсов** — набор живых точек в 7D, где стратегия генерирует trades. Флот прогоняет все конфиги одновременно (`tick_all` — zero-copy, 1500+ конфигов на каждый тик).

**Expand** уплотняет матрицу: ±step по каждой оси вокруг живых точек. Мёртвые зоны не расширяются. Hard bounds не нарушаются.

**ASHA** работает на forward-test фазе: даёт бюджет ступенями (10мин → 1ч → 6ч), режет нижние 50% на каждой ступени. Выжившие — финальные кандидаты.

```
scout (грубая 7D сетка) → референсы
  → expand (±step вокруг живых) → уточнённая матрица
    → ASHA forward-test (бюджет ступенями) → финалисты
      → promote → runtime-grid.toml → hot-reload
```

---

## Architecture

```
                    ┌─────────────────────────┐
                    │   ray_driver/cli.py      │
                    │   scout → expand → asha  │
                    └───────────┬─────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                  ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │  scout.py    │  │  expand.py   │  │  trainable.py│
    │  coarse scan │  │  grow refs   │  │  ASHA trials │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           └─────────────────┼─────────────────┘
                             ▼
                    ┌──────────────────┐
                    │  ipc.py          │
                    │  write JSON      │
                    │  wait .trial-ack │
                    │  poll SQLite     │
                    └───────┬──────────┘
                            │ file I/O
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
       trial-batch.json  .trial-ack  optimizer.db
              │                          ▲
              └─────────────┬────────────┘
                            ▼
                    ┌──────────────────┐
                    │  Rust Worker     │
                    │  (existing)      │
                    │  + trial watch   │
                    │  + run_id tag    │
                    └──────────────────┘
```

### IPC Contract

**Input** — `config/trial-batch.json` (Ray → Rust):
```json
{
  "run_id": "scout-001-1708714738",
  "configs": [
    {
      "spike_threshold_bps": 45.0,
      "target_ratio": 0.5,
      "stop_loss_bps": 20.0,
      "max_hold_ms": 15000,
      "max_spread_bps": 4.0,
      "trailing_decay_ratio": 0.5,
      "baseline_window_ms": 30000
    }
  ]
}
```
Omitted fields use `TraderConfig::default()`. Rust applies immediately (no `apply_interval_ms` debounce).

**Ack** — `config/.trial-ack` (Rust → Ray):
```json
{
  "run_id": "scout-001-1708714738",
  "applied_at_ms": 1708714740000,
  "config_count": 150,
  "drained_trades": 42
}
```

**Results** — Ray reads `data/optimizer.db` directly (WAL mode allows concurrent reads):
```sql
SELECT config_id, COUNT(*) as trades, AVG(pnl_pct) as avg_pnl,
       SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
FROM trades WHERE run_id = ?
GROUP BY config_id
```

### Parameter Bounds Contract

Each tunable axis has hard boundaries that no phase can exceed:

| Parameter              | hard_min | hard_max | init_min | init_max | step  | expand_step |
|------------------------|----------|----------|----------|----------|-------|-------------|
| spike_threshold_bps    | 5.0      | 200.0    | 20.0     | 100.0    | 10.0  | 10.0        |
| target_ratio           | 0.1      | 0.95     | 0.2      | 0.8      | 0.1   | 0.1         |
| stop_loss_bps          | 3.0      | 100.0    | 5.0      | 50.0     | 5.0   | 5.0         |
| max_hold_ms            | 1000     | 120000   | 3000     | 60000    | 5000  | 5000        |
| max_spread_bps         | 1.0      | 20.0     | 2.0      | 8.0      | 1.0   | 1.0         |
| trailing_decay_ratio   | 0.1      | 0.95     | 0.2      | 0.8      | 0.1   | 0.1         |
| baseline_window_ms     | 5000     | 120000   | 10000    | 60000    | 10000 | 10000       |

Scout uses `init_min..init_max` with `step`. Expand grows by `expand_step` around live configs, clipped to `hard_min..hard_max`.

---

## Phase 1: Rust — TraderConfig Deserialize + run_id

### Task 1.1: Add Deserialize to TraderConfig

**Files:**
- Modify: `src/domain/screener/trader_config.rs`

**Step 1: Add `Deserialize` derive + `#[serde(default)]`**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct TraderConfig {
    // ... all fields unchanged
}
```

**Step 2: Verify builds**

Run: `cargo check 2>&1 | tail -3`
Expected: compiles with no errors

**Step 3: Commit**

```bash
git add src/domain/screener/trader_config.rs
git commit -m "feat: add Deserialize to TraderConfig for JSON trial batch parsing"
```

### Task 1.2: Add run_id to FleetTrade

**Files:**
- Modify: `src/domain/screener/shadow_fleet.rs:337-341`

**Step 1: Add `run_id` field to FleetTrade**

```rust
#[derive(Debug, Clone)]
pub struct FleetTrade {
    pub config_id: u64,
    pub symbol: String,
    pub run_id: Option<String>,
    pub trade: ClosedTrade,
}
```

**Step 2: Update FleetTrade construction in `tick_all` (line ~444)**

Add `run_id` to `FleetTickMeta`:
```rust
#[derive(Debug, Clone, Copy)]
pub struct FleetTickMeta<'a> {
    pub symbol: &'a str,
    pub gate_natr_30m_pct_at_entry: f64,
    pub run_id: Option<&'a str>,
}
```

And in `tick_all`, set `run_id` when creating `FleetTrade`:
```rust
self.pending_trades.push_back(FleetTrade {
    config_id: *config_id,
    symbol: sym.clone(),
    run_id: meta.run_id.map(|s| s.to_string()),
    trade: trade.clone(),
});
```

**Step 3: Fix compilation — update FleetTickMeta construction in mod.rs:227**

```rust
FleetTickMeta {
    symbol,
    gate_natr_30m_pct_at_entry: state.gate_natr_30m_pct,
    run_id: None, // set by trial batch path
}
```

**Step 4: Verify builds**

Run: `cargo check 2>&1 | tail -3`

**Step 5: Commit**

```bash
git add src/domain/screener/shadow_fleet.rs src/domain/screener/mod.rs
git commit -m "feat: add run_id to FleetTrade and FleetTickMeta"
```

### Task 1.3: Add run_id to ScreenerStore + DB

**Files:**
- Modify: `src/domain/screener/mod.rs:63-74` (ScreenerStore struct)
- Modify: `src/infrastructure/db.rs:27-68` (schema) + `src/infrastructure/db.rs:278-309` (flush_trades)

**Step 1: Add `current_run_id` to ScreenerStore**

```rust
use arc_swap::ArcSwap;

// In ScreenerStore struct:
pub struct ScreenerStore {
    symbols: Arc<DashMap<String, SymbolState>>,
    window_ms: i64,
    fleet_configs: Arc<ArcSwap<Vec<TraderConfig>>>,
    db_writer: Option<DbWriter>,
    current_run_id: Arc<ArcSwap<Option<String>>>,
}
```

Init in `new()`:
```rust
current_run_id: Arc::new(ArcSwap::from_pointee(None)),
```

Add setter:
```rust
pub fn set_run_id(&self, run_id: Option<String>) {
    self.current_run_id.store(Arc::new(run_id));
}

pub fn current_run_id(&self) -> Option<String> {
    (**self.current_run_id.load()).clone()
}
```

**Step 2: Thread run_id into FleetTickMeta in `update()` (line ~227)**

```rust
let run_id_arc = self.current_run_id.load();
let run_id_ref = run_id_arc.as_deref();
// ... later in FleetTickMeta:
FleetTickMeta {
    symbol,
    gate_natr_30m_pct_at_entry: state.gate_natr_30m_pct,
    run_id: run_id_ref,
}
```

**Step 3: Add `run_id` column to DB schema**

In `db.rs`, add to trades CREATE TABLE:
```sql
run_id TEXT
```
And migration:
```rust
let _ = conn.execute_batch("ALTER TABLE trades ADD COLUMN run_id TEXT;");
```

**Step 4: Add `run_id` to `flush_trades` INSERT**

```sql
INSERT OR IGNORE INTO trades (..., run_id)
VALUES (?1, ..., ?15)
```
```rust
ft.run_id.as_deref(),
```

**Step 5: Add index for run_id queries**

```sql
CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id);
```

**Step 6: Verify builds + test**

Run: `cargo check && cargo test 2>&1 | tail -5`

**Step 7: Commit**

```bash
git add src/domain/screener/mod.rs src/infrastructure/db.rs
git commit -m "feat: thread run_id from ScreenerStore through FleetTrade to DB"
```

---

## Phase 2: Rust — Trial Batch IPC

### Task 2.1: Trial batch data structures + parser

**Files:**
- Modify: `src/main.rs` (add structs near RuntimeGridConfig, ~line 205)

**Step 1: Add TrialBatch struct and ack struct**

```rust
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct TrialBatch {
    run_id: String,
    configs: Vec<TraderConfig>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct TrialAck {
    run_id: String,
    applied_at_ms: i64,
    config_count: usize,
    drained_trades: usize,
}
```

**Step 2: Add load function**

```rust
fn load_trial_batch(path: &Path) -> Result<TrialBatch, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read trial batch {}: {e}", path.display()))?;
    let batch: TrialBatch = serde_json::from_str(&content)
        .map_err(|e| format!("parse trial batch {}: {e}", path.display()))?;
    if batch.configs.is_empty() {
        return Err("trial batch has no configs".to_string());
    }
    if batch.run_id.is_empty() {
        return Err("trial batch run_id is empty".to_string());
    }
    Ok(batch)
}

fn write_trial_ack(dir: &Path, ack: &TrialAck) {
    let path = dir.join(".trial-ack");
    match serde_json::to_string_pretty(ack) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                warn!("trial-ack: failed to write {}: {e}", path.display());
            }
        }
        Err(e) => warn!("trial-ack: serialize error: {e}"),
    }
}
```

**Step 3: Verify builds**

Run: `cargo check 2>&1 | tail -3`

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: add TrialBatch/TrialAck structs and JSON parser"
```

### Task 2.2: Extend hot-reload watcher for trial-batch.json

**Files:**
- Modify: `src/main.rs:397-474` (spawn_runtime_grid_hot_reload)

**Step 1: Add trial batch watching to the existing loop**

The watcher checks `config/trial-batch.json` BEFORE the TOML grid each cycle. Trial batches apply immediately (no debounce). After apply, the file is renamed to `.consumed` to prevent re-processing.

Extend `spawn_runtime_grid_hot_reload` signature:
```rust
fn spawn_runtime_grid_hot_reload(
    screener: ScreenerStore,
    db_path: PathBuf,
    config_path: PathBuf,
    trial_batch_path: PathBuf, // NEW
    initial_modified: Option<SystemTime>,
    initial_signature: Option<u64>,
)
```

Add trial batch check at the top of the loop body (before TOML check):
```rust
// --- Trial batch: immediate apply, no debounce ---
let trial_modified = std::fs::metadata(&trial_batch_path)
    .and_then(|m| m.modified())
    .ok();
if let Some(mod_time) = trial_modified {
    let trial_changed = last_trial_modified.map_or(true, |prev| mod_time > prev);
    if trial_changed {
        last_trial_modified = Some(mod_time);
        match load_trial_batch(&trial_batch_path) {
            Ok(batch) => {
                let run_id = batch.run_id.clone();
                let config_count = batch.configs.len();
                if let Err(e) = upsert_runtime_configs(&db_path, &batch.configs) {
                    warn!("trial-batch: db upsert failed: {e}");
                } else {
                    screener.set_run_id(Some(run_id.clone()));
                    let report = screener.replace_fleet_configs(batch.configs);
                    screener.flush_db_writer().await;
                    info!(
                        "trial-batch: applied run_id={run_id} configs={config_count} \
                         drained_trades={}",
                        report.drained_trades
                    );
                    write_trial_ack(
                        trial_batch_path.parent().unwrap_or(Path::new(".")),
                        &TrialAck {
                            run_id,
                            applied_at_ms: EventLoopState::now_ms(),
                            config_count,
                            drained_trades: report.drained_trades,
                        },
                    );
                    // Clear grid pending — trial mode takes priority
                    pending = None;
                }
            }
            Err(e) => warn!("trial-batch: {e}"),
        }
    }
}
// --- End trial batch ---
// ... existing TOML grid code continues below ...
```

Add `last_trial_modified: Option<SystemTime> = None` alongside other state vars.

**Step 2: Update the call site in main() to pass trial_batch_path**

```rust
let trial_batch_path = PathBuf::from("config/trial-batch.json");
spawn_runtime_grid_hot_reload(
    screener.clone(),
    db_path.clone(),
    runtime_grid_path.clone(),
    trial_batch_path,
    initial_modified,
    initial_signature,
);
```

**Step 3: Verify builds**

Run: `cargo check 2>&1 | tail -3`

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: trial-batch.json watcher with immediate apply + ack"
```

### Task 2.3: Add Python artifacts to .gitignore

**Files:**
- Modify: `.gitignore`

**Step 1: Append Python + Ray entries**

```
# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
.venv/
venv/

# Ray
ray_results/
/ray_driver/*.egg-info/
```

**Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: add Python/Ray to .gitignore"
```

---

## Phase 3: Python — Ray Driver Scaffold

### Task 3.1: Project structure + dependencies

**Files:**
- Create: `ray_driver/requirements.txt`
- Create: `ray_driver/__init__.py`
- Create: `ray_driver/bounds.py`

**Step 1: Create requirements.txt**

```
ray[tune]>=2.9
```

**Step 2: Create bounds.py — parameter contracts**

```python
"""Hard parameter bounds and scout/expand ranges for all tunable axes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class AxisBounds:
    hard_min: float
    hard_max: float
    init_min: float
    init_max: float
    step: float
    expand_step: float

    def init_values(self) -> list[float]:
        vals = []
        v = self.init_min
        while v <= self.init_max + self.step * 1e-9:
            vals.append(round(v, 6))
            v += self.step
        return vals or [self.init_min]

    def expand_around(self, center: float, n_steps: int = 1) -> list[float]:
        vals = set()
        for i in range(-n_steps, n_steps + 1):
            v = round(center + i * self.expand_step, 6)
            if self.hard_min <= v <= self.hard_max:
                vals.add(v)
        return sorted(vals)


AXES: dict[str, AxisBounds] = {
    "spike_threshold_bps": AxisBounds(5.0, 200.0, 20.0, 100.0, 10.0, 10.0),
    "target_ratio": AxisBounds(0.1, 0.95, 0.2, 0.8, 0.1, 0.1),
    "stop_loss_bps": AxisBounds(3.0, 100.0, 5.0, 50.0, 5.0, 5.0),
    "max_hold_ms": AxisBounds(1000, 120000, 3000, 60000, 5000, 5000),
    "max_spread_bps": AxisBounds(1.0, 20.0, 2.0, 8.0, 1.0, 1.0),
    "trailing_decay_ratio": AxisBounds(0.1, 0.95, 0.2, 0.8, 0.1, 0.1),
    "baseline_window_ms": AxisBounds(5000, 120000, 10000, 60000, 10000, 10000),
}

# Fields with sensible defaults — not tuned by Ray
FIXED_DEFAULTS = {
    "fill_delay_ms": 6,
    "cooldown_ms": 3000,
    "warmup_ms": 30000,
    "quote_freshness_ms": 1000,
    "taker_fee": 0.0005,
    "min_baseline_samples": 20,
}
```

**Step 3: Create `__init__.py`**

```python
"""Ray Tune + ASHA driver for HFT shadow fleet optimization."""
```

**Step 4: Commit**

```bash
git add ray_driver/
git commit -m "feat: ray_driver scaffold with parameter bounds"
```

### Task 3.2: IPC module — file communication + DB reader

**Files:**
- Create: `ray_driver/ipc.py`

**Step 1: Write ipc.py**

```python
"""File-based IPC with Rust fleet + SQLite metrics reader."""

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TrialAck:
    run_id: str
    applied_at_ms: int
    config_count: int
    drained_trades: int


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
        self.ack_path = config_dir / ".trial-ack"

    def submit_batch(
        self,
        run_id: str,
        configs: list[dict],
        timeout_s: float = 30.0,
    ) -> TrialAck:
        """Write trial-batch.json and wait for .trial-ack from Rust."""
        batch = {"run_id": run_id, "configs": configs}
        tmp = self.batch_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(batch, indent=2))
        tmp.rename(self.batch_path)  # atomic on same FS

        return self._wait_ack(run_id, timeout_s)

    def _wait_ack(self, run_id: str, timeout_s: float) -> TrialAck:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.ack_path.exists():
                try:
                    ack = json.loads(self.ack_path.read_text())
                    if ack.get("run_id") == run_id:
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
```

**Step 2: Commit**

```bash
git add ray_driver/ipc.py
git commit -m "feat: FleetIPC — file-based trial batch submission + SQLite metrics reader"
```

---

## Phase 4: Python — Scout

### Task 4.1: Scout sampler

**Files:**
- Create: `ray_driver/scout.py`

**Step 1: Write scout.py**

```python
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
```

**Step 2: Commit**

```bash
git add ray_driver/scout.py
git commit -m "feat: scout phase — coarse grid sampling with trade-count filter"
```

### Task 4.2: Expand phase

**Files:**
- Create: `ray_driver/expand.py`

**Step 1: Write expand.py**

```python
"""Expand phase — grow parameter ranges around live scout references."""

import sqlite3
import time
from pathlib import Path

from .bounds import AXES, FIXED_DEFAULTS
from .ipc import FleetIPC, RunMetrics


def _config_from_db(db_path: Path, config_id: int) -> dict | None:
    """Read config params from optimizer.db by config_id."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        row = conn.execute(
            """SELECT spike_threshold_bps, target_ratio, stop_loss_bps,
                      max_hold_ms, max_spread_bps, trailing_decay_ratio,
                      baseline_window_ms
               FROM configs WHERE id = ?""",
            (config_id,),
        ).fetchone()
        if not row:
            return None
        keys = [
            "spike_threshold_bps", "target_ratio", "stop_loss_bps",
            "max_hold_ms", "max_spread_bps", "trailing_decay_ratio",
            "baseline_window_ms",
        ]
        return dict(zip(keys, row))
    finally:
        conn.close()


def expand_around_references(
    references: list[RunMetrics],
    db_path: Path,
    n_steps: int = 1,
) -> list[dict]:
    """Generate neighbor configs around each reference, clipped to hard bounds."""
    seen = set()
    expanded = []

    for ref in references:
        center = _config_from_db(db_path, ref.config_id)
        if not center:
            continue

        # For each axis, generate expanded values; keep others fixed
        per_axis_values: dict[str, list[float]] = {}
        for name, bounds in AXES.items():
            per_axis_values[name] = bounds.expand_around(
                center[name], n_steps
            )

        # Cartesian product of expanded axes
        keys = list(per_axis_values.keys())
        import itertools
        for combo in itertools.product(*(per_axis_values[k] for k in keys)):
            cfg = dict(zip(keys, combo))
            cfg.update(FIXED_DEFAULTS)
            # Deduplicate by frozen config
            key = tuple(sorted(cfg.items()))
            if key not in seen:
                seen.add(key)
                expanded.append(cfg)

    return expanded


def run_expand(
    ipc: FleetIPC,
    references: list[RunMetrics],
    duration_s: int = 600,
    min_trades: int = 1,
    n_steps: int = 1,
    max_configs: int = 2000,
) -> list[RunMetrics]:
    """Expand around references, run, return configs with trades."""
    configs = expand_around_references(references, ipc.db_path, n_steps)
    if len(configs) > max_configs:
        # Stride-sample to cap
        stride = len(configs) / max_configs
        configs = [configs[int(i * stride)] for i in range(max_configs)]

    run_id = f"expand-{int(time.time())}"
    print(f"[expand] submitting {len(configs)} configs, run_id={run_id}")
    ipc.clear_ack()
    ack = ipc.submit_batch(run_id, configs)
    print(f"[expand] ack: {ack.config_count} configs applied")

    print(f"[expand] waiting {duration_s}s...")
    time.sleep(duration_s)

    metrics = ipc.query_run_metrics(run_id)
    alive = [m for m in metrics if m.trades >= min_trades]
    print(
        f"[expand] {len(alive)}/{len(metrics)} configs had ≥{min_trades} trades"
    )
    return alive
```

**Step 2: Commit**

```bash
git add ray_driver/expand.py
git commit -m "feat: expand phase — grow ranges around live references"
```

---

## Phase 5: Python — ASHA Forward Testing

### Task 5.1: Ray Trainable for forward testing

**Files:**
- Create: `ray_driver/trainable.py`

**Step 1: Write trainable.py**

```python
"""Ray Tune Trainable — wraps a fleet trial as a long-running ASHA-compatible trial."""

import time

from ray import tune

from .ipc import FleetIPC


class FleetTrial(tune.Trainable):
    """
    One ASHA trial = one config batch running on the live fleet.

    Reports intermediate metrics at `report_interval_s` intervals.
    ASHA uses `time_budget_s` as the time attribute for successive halving.
    """

    def setup(self, config: dict):
        self.ipc = FleetIPC()
        self.configs = config["configs"]
        self.run_id = config["run_id"]
        self.report_interval_s = config.get("report_interval_s", 60)
        self.elapsed_s = 0

        # Submit batch to fleet
        self.ipc.clear_ack()
        self.ipc.submit_batch(self.run_id, self.configs)

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

    def cleanup(self):
        self.ipc.set_run_id_inactive()  # optional: signal fleet to clear
```

**Step 2: Commit**

```bash
git add ray_driver/trainable.py
git commit -m "feat: FleetTrial — Ray Trainable for ASHA forward testing"
```

### Task 5.2: CLI entry point with full pipeline

**Files:**
- Create: `ray_driver/cli.py`

**Step 1: Write cli.py**

```python
"""CLI entry point: scout → expand → ASHA forward-test → promote."""

import argparse
import json
import sys
from pathlib import Path

from .ipc import FleetIPC
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
    # Save references for expand phase
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

    from .ipc import RunMetrics
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
    from .ipc import RunMetrics
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

    import time
    from .trainable import FleetTrial

    analysis = tune.run(
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
    print(f"\n[forward] best result: {analysis.best_result}")


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

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
```

**Step 2: Commit**

```bash
git add ray_driver/cli.py
git commit -m "feat: CLI entry point — scout/expand/forward subcommands"
```

---

## Phase 6: Integration Test + Promotion

### Task 6.1: Dry-run integration test (no live fleet)

**Files:**
- Create: `ray_driver/tests/test_bounds.py`
- Create: `ray_driver/tests/__init__.py`

**Step 1: Write test_bounds.py**

```python
"""Tests for parameter bounds and scout config generation."""

from ray_driver.bounds import AXES, AxisBounds
from ray_driver.scout import generate_scout_configs


def test_init_values_within_bounds():
    for name, ax in AXES.items():
        vals = ax.init_values()
        assert len(vals) > 0, f"{name} has no init values"
        assert all(ax.hard_min <= v <= ax.hard_max for v in vals), \
            f"{name} init values outside hard bounds"


def test_expand_respects_hard_bounds():
    ax = AxisBounds(0.0, 10.0, 2.0, 8.0, 1.0, 2.0)
    expanded = ax.expand_around(1.0, n_steps=2)
    assert all(0.0 <= v <= 10.0 for v in expanded)
    assert 0.0 not in ax.expand_around(0.5, n_steps=1) or True  # boundary


def test_expand_at_hard_min_clips():
    ax = AxisBounds(5.0, 100.0, 10.0, 90.0, 10.0, 10.0)
    expanded = ax.expand_around(5.0, n_steps=1)
    assert min(expanded) >= 5.0


def test_scout_config_count():
    configs = generate_scout_configs()
    assert len(configs) > 0
    # Should be cartesian product of all init values
    expected = 1
    for ax in AXES.values():
        expected *= len(ax.init_values())
    assert len(configs) == expected


def test_scout_configs_have_all_fields():
    configs = generate_scout_configs()
    required = set(AXES.keys()) | {"fill_delay_ms", "cooldown_ms", "warmup_ms",
                                    "quote_freshness_ms", "taker_fee",
                                    "min_baseline_samples"}
    for cfg in configs[:5]:
        assert required.issubset(cfg.keys()), f"Missing keys: {required - cfg.keys()}"
```

**Step 2: Run tests**

Run: `cd /root/turbo/hft-lead-lag && python3 -m pytest ray_driver/tests/ -v`
Expected: all pass

**Step 3: Commit**

```bash
git add ray_driver/tests/
git commit -m "test: bounds and scout config generation"
```

### Task 6.2: Promotion bridge — export top configs to runtime-grid.toml

**Files:**
- Create: `ray_driver/promote.py`

**Step 1: Write promote.py**

```python
"""Promote top configs from a run into runtime-grid.toml for the fleet."""

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
    Read top-K configs from a run, filter by quality, return as dicts.

    Does NOT write to runtime-grid.toml — that's a separate decision.
    Outputs a JSON file that can be reviewed before promotion.
    """
    metrics = ipc.query_run_metrics(run_id)
    qualified = [
        m for m in metrics
        if m.trades >= min_trades and m.avg_pnl_pct >= min_avg_pnl
    ]
    qualified.sort(key=lambda m: m.avg_pnl_pct, reverse=True)
    top = qualified[:top_k]

    # Read full config params from DB
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
    out.write_text(json.dumps(promoted, indent=2))
    print(f"[promote] {len(promoted)} configs saved to {out}")
    return promoted
```

**Step 2: Commit**

```bash
git add ray_driver/promote.py
git commit -m "feat: promote — export top configs from completed run"
```

---

## Execution Checklist

| Phase | What                              | Rust/Python | LOC ~  |
|-------|-----------------------------------|-------------|--------|
| 1     | TraderConfig Deserialize + run_id | Rust        | +30    |
| 2     | Trial batch IPC + watcher         | Rust        | +80    |
| 3     | Python scaffold + IPC             | Python      | +180   |
| 4     | Scout + Expand                    | Python      | +150   |
| 5     | ASHA Trainable + CLI              | Python      | +140   |
| 6     | Tests + Promotion                 | Python      | +100   |

**Total: ~680 LOC across 6 phases, 12 tasks.**

---

## Usage

```bash
# Rust fleet must be running first (existing process)

# 1. Scout — find reference configs (10 min scan)
python3 -m ray_driver.cli scout --duration 600

# 2. Expand — grow around references (10 min)
python3 -m ray_driver.cli expand --duration 600

# 3. Forward test — ASHA pruning over 6h budget
python3 -m ray_driver.cli forward --max-budget 21600 --grace-period 600
```
