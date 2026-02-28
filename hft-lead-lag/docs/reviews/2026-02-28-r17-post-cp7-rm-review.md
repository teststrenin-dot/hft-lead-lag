# Review Round R17 — Post RM/CP7 Block1

Date: 2026-02-28
Scope: latest runtime and status-doc changes before consolidation commit
Method: parallel deep review by 3 independent reviewers (runtime, API/health, docs)

## Findings and actions
1. `P1` RM4 window semantics bound to `/health` call rate.
   - Action: introduced fixed evaluation interval and single-window claim logic in health evaluation path.
2. `P1` potential race on RM4 streak updates under concurrent `/health` requests.
   - Action: serialized per-window evaluation with CAS-based claim; non-eval requests are read-only snapshots.
3. `P1` control-plane overflow key used symbol-only and could overwrite exchange side.
   - Action: overflow key changed to `(symbol, exchange)` and test coverage extended.
4. `P1` mixed-mode subscription could miss capped strategy symbols.
   - Action: mixed-mode now subscribes to union of strategy + screener symbols.
5. `P2` docs inconsistencies (`CP7` exit-gate wording, `RM4` missing-vs-done wording, stale delivery steps).
   - Action: status docs normalized and evidence links updated.
6. `P2` RM3 evidence had truncated test command filters.
   - Action: replaced with explicit test names.

## Verification
Executed after fixes:

```bash
cargo fmt --all
cargo test -q
cargo check --all-targets
```

Result: green.
