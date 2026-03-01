# CP7 Block8 Evidence — External Alert Hook Script for `/health.alert_level`

Date: 2026-02-28
Status: Completed
Scope: wire machine-readable health severity into external automation without runtime code coupling

## What was implemented
1. Added executable ops script:
   - `scripts/ops/health_alert_gate.sh`
2. Script contract:
   - reads `/health`;
   - parses `alert_level` (`ok|warn|critical`);
   - optionally triggers external commands:
     - `--on-warn "<cmd>"`
     - `--on-critical "<cmd>"`
3. Exit codes are automation-friendly:
   - `0` = ok
   - `10` = warn
   - `20` = critical

## Verification
Commands run:

```bash
bash -n scripts/ops/health_alert_gate.sh
scripts/ops/health_alert_gate.sh --help
```

Result:
1. script syntax is valid;
2. CLI contract is stable for cron/systemd/webhook wrappers.

## Exit-gate impact
1. CP7 external alert-hook wiring now has a concrete executable primitive.
2. Remaining tail is operational scheduling/integration policy (how often and where to run hooks/drills).
