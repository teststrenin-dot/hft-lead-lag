# Sprint 008: Deal-Hunt Data Foundation (NATR in DB)

## Scope
1. Persist NATR at entry for every trade.
2. Persist hold duration and `early_stop_churn` (`stop_loss` with `hold_ms <= 500`).
3. Add runtime periodic NATR refresh into symbol state for entry snapshots.
4. Prepare short-run methodology for searching high-trade regions.

## Run Methodology (Phase A)
1. Duration per run: 10 minutes.
2. Config budget: up to 1500 per run.
3. Re-evaluation cadence: every 5-10 minutes.
4. Pruning rule on Phase A:
   - remove configs with zero trades first.
5. Ultra-govno monitoring:
   - metric only (initially), not hard pruning gate.

## Data Contract
Per trade row must include:
1. `gate_natr_30m_pct_at_entry`
2. `hold_ms`
3. `early_stop_churn`

## Quick SQL Checks
```sql
-- Coverage of new fields
SELECT COUNT(*) AS total,
       SUM(CASE WHEN gate_natr_30m_pct_at_entry > 0 THEN 1 ELSE 0 END) AS natr_non_zero,
       SUM(early_stop_churn) AS early_stop_count
FROM trades;

-- Configs with zero trades (for first-pass pruning)
SELECT c.id
FROM configs c
LEFT JOIN trades t ON t.config_id = c.id
GROUP BY c.id
HAVING COUNT(t.id) = 0;

-- Early stop churn share by config
SELECT config_id,
       COUNT(*) AS total,
       SUM(early_stop_churn) AS early_stop,
       1.0 * SUM(early_stop_churn) / COUNT(*) AS early_stop_share
FROM trades
GROUP BY config_id
HAVING COUNT(*) >= 20
ORDER BY early_stop_share DESC;
```

## Exit Criteria
1. New columns are populated in DB for newly generated trades.
2. Runtime logs show NATR refresh cycles.
3. Verification suite is green (`fmt`, `clippy`, `test`).
