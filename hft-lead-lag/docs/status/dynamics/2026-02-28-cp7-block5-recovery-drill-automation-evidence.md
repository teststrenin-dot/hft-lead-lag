# CP7 Block5 Evidence — Recovery Drill Automation

Date: 2026-02-28
Status: Completed
Scope: automate deterministic recovery validation from CP7 runbook

## What was implemented
1. Added executable ops script:
   - `scripts/ops/health_recovery_drill.sh`
2. Script behavior:
   - polls `/health` by window (`--samples`, `--interval-sec`);
   - supports warmup skip (`--warmup-samples`);
   - stores raw JSON samples to JSONL (`--out`);
   - fails if post-warmup windows violate recovery criteria:
     - `status != ok`
     - `hft_mode_status != hft`
     - any watchdog issue in:
       - `hft_slo_degraded_non_hft`
       - `engine_state_stall`
       - `signal_loop_stall`
       - `execution_loop_stall`

## Verification
Commands run:

```bash
bash -n scripts/ops/health_recovery_drill.sh
scripts/ops/health_recovery_drill.sh --help
```

Result:
1. script syntax is valid;
2. CLI contract is exposed and stable for operator use.

## Exit-gate impact
1. CP7 recovery flow is no longer documentation-only; it has executable validation primitive.
2. Remaining CP7 operations tail:
   - DB-writer watchdog coverage in health signals;
   - drift-specific alert thresholds/escalation contract closure.
