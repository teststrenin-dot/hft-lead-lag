# CP0 Contract Freeze v1

Date: 2026-02-27
Contract version: `cp0-v1`
Pinned baseline commit: `3c501387d99e27a67c329c07b23bdcd3ad77347a`

## 1) API Surface (Frozen)

### Core
- `GET /health`
- `GET /api/v1/symbols`
- `GET /api/v1/screener`
- `GET /screener`

### Portfolio Runtime
- `GET /api/v1/portfolio/active`
- `GET /api/v1/portfolio/candidates`
- `GET /api/v1/portfolio/performance`
- `GET /api/v1/portfolio/guards`

### Shadow/Fleet
- `GET /api/v1/shadow/:symbol`
- `GET /api/v1/chart/:symbol`
- `GET /api/v1/fleet`
- `GET /api/v1/fleet/ranked`
- `GET /api/v1/fleet/symbols`
- `GET /api/v1/fleet/policy`
- `GET /api/v1/fleet/policy/:symbol`
- `GET /fleet`

### Forward/Trials
- `GET /api/v1/forward/runs`
- `GET /api/v1/forward/symbols`
- `GET /api/v1/trials`
- `GET /api/v1/trials/axes`
- `GET /api/v1/trials/:run_id`
- `GET /api/v1/trials/runner/config`
- `GET /api/v1/trials/runner/status`
- `POST /api/v1/trials/runner/start`
- `POST /api/v1/trials/runner/stop`
- `GET /trials`

## 2) Portfolio Identity Contract (Frozen)
- Portfolio IDs come from `PORTFOLIO_IDS` env var.
- Default fallback is `A,B`.
- `/api/v1/portfolio/active` returns runtime portfolio IDs only; DB fallback must not add foreign IDs.

## 3) Candidate History Contract (Frozen)
- Candidate stats are event-level, not raw-trade-level.
- Event key is `(symbol, exit_ts_ms)`.
- Event pnl is `AVG(pnl_pct)` over all closes in that key.
- Aggregate counters (`closed/profitable/losing/pnl_sum`) are built over events.
- Age basis is `MIN(entry_ts_ms)` across contributing events.

## 4) Shadow Exit Contract (Frozen)
Domain `ExitReason` values:
- `stop_loss`
- `breakeven`
- `trailing_take`
- `timeout`

Boundary rule:
- Inside domain: typed enum.
- At storage/API boundaries: serialized as frozen snake_case strings above.

## 5) Change Rule
Any contract change in sections 1-4 requires:
1. Version bump (`cp0-vN`).
2. Regression tests for affected contract.
3. Docs sync in:
   - `docs/README.md`
   - `docs/status/2026-02-26-business-logic-v1-implementation-status.md`
   - `docs/status/2026-02-26-project-math-model.md`
