# RM4 Evidence — `hft_core` Live SLO Validation (2-core host)

Date: 2026-02-28
Scope: validate frozen RM4 SLO envelope in real `RUNTIME_PLANE_MODE=hft_core` runtime window.
Status note: historical baseline before CP7 block2 signal-loop fix; current compliance snapshot is in `2026-02-28-cp7-block2-event-driven-signal-loop-evidence.md`.

## 1) Run setup
Command:
```bash
cd /root/turbo/hft-lead-lag
RUNTIME_PLANE_MODE=hft_core cargo run --quiet
```

Sampling endpoint:
- `GET /health` every 5s after warm-up.
- HTTP `200` and `503` are both treated as valid windows for RM4 evaluation.

Raw artifacts:
- runtime log: `/tmp/hft_core_run_1772283089.log`
- health samples: `/tmp/hft_core_health_1772283089.jsonl`

## 2) Window samples (warm run)
SLO targets (`hft_core`):
- `ingest.p99 <= 1500us`
- `decision.p99 <= 1500us`
- `end_to_end.p99 <= 2000us`
- escalation: `3` consecutive breached windows => `degraded_non_hft`.

| sample | http | mode | rm4_streak | ingest_p99_us | decision_p99_us | e2e_p99_us | signal_backlog |
|---|---:|---|---:|---:|---:|---:|---:|
| 1 | 200 | hft | 1 | 356 | 98931 | 99075 | 1 |
| 2 | 200 | hft | 2 | 472 | 98392 | 98617 | 9 |
| 3 | 503 | degraded_non_hft | 3 | 415 | 98671 | 98767 | 4 |
| 4 | 503 | degraded_non_hft | 4 | 384 | 97440 | 97493 | 21 |
| 5 | 503 | degraded_non_hft | 5 | 447 | 97768 | 98053 | 19 |

## 3) Result
1. RM4 control logic works as designed:
   - breach streak increments each window,
   - on streak `=3`, runtime flips to `degraded_non_hft`, `/health` returns `503`.
2. Host does **not** meet `hft_core` latency envelope yet:
   - `decision/end_to_end p99` are ~`97-99ms` (target is `<=1.5-2.0ms`).
3. Backlog/drops are not the blocker in this run:
   - queue depths are low, drops/timeouts are zero.

## 4) Immediate focus for next remediation step
1. Reduce decision-loop tail (`HFT-CP7` block2): profile and cut per-symbol decision cost in hot path.
2. Keep RM4 gate active as production safety contract during optimization.
