# CP7 Block2 Evidence — Event-Driven Signal Loop (hft_core)

Date: 2026-02-28
Scope: remove timer-based signal decision jitter in `hft_core` by switching signal checks from fixed interval to exchange-event driven flow.

## 1) Change summary
Code change:
- removed `signal_interval` (`100ms`) from `EventLoopState`.
- `handle_signal_tick(...)` is now called immediately after each processed Binance/Gate batch.

Files:
- `src/event_loop_core.rs`
- `src/event_loop_runtime.rs`

## 2) Why this was required
Before change, decision checks were paced by a `100ms` ticker, which dominated decision/end-to-end p99 and triggered RM4 degradation despite low backlog.

Baseline failure evidence:
- `docs/status/dynamics/2026-02-28-rm4-hft-core-live-slo-validation.md`

## 3) Live validation (`hft_core`, target host)
Run command:
```bash
cd /root/turbo/hft-lead-lag
RUNTIME_PLANE_MODE=hft_core cargo run --quiet
```

Artifacts:
- runtime log: `/tmp/hft_core_run_1772283313.log`
- samples: `/tmp/hft_core_health_1772283313.jsonl`

Warm samples (5 windows, 5s cadence):

| sample | http | mode | rm4_streak | ingest_p99_us | decision_p99_us | e2e_p99_us | signal_backlog |
|---|---:|---|---:|---:|---:|---:|---:|
| 1 | 200 | hft | 0 | 459 | 218 | 530 | 0 |
| 2 | 200 | hft | 0 | 345 | 249 | 448 | 0 |
| 3 | 200 | hft | 0 | 408 | 243 | 504 | 0 |
| 4 | 200 | hft | 0 | 403 | 239 | 493 | 0 |
| 5 | 200 | hft | 0 | 416 | 227 | 515 | 0 |

## 4) Result vs RM4 envelope
Targets (`hft_core`):
- ingest p99 <= `1500us`
- decision p99 <= `1500us`
- end_to_end p99 <= `2000us`

Observed status:
1. All 5 windows satisfy all latency thresholds.
2. `hft_mode_status` remains `hft` throughout.
3. `rm4_breach_streak` remains `0`.

Conclusion: block2 fix removes timer-induced jitter and restores `hft_core` latency compliance on current host profile.
