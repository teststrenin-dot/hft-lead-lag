# Family + Symbol Cluster Portfolio Design

Date: 2026-02-24  
Status: Draft for approval

## 1) Context and Objective

Current system optimizes parameter configs (`config_id`) and evaluates performance across all symbols.

Target state:

1. Each shadow bot (config) trades a small symbol portfolio (`<= 4` symbols).
2. Portfolio is mostly static, but rebalances when symbol/config metrics indicate degradation.
3. Symbol selection should be behavior-driven, not manually tied to a specific coin.
4. Avoid duplicated logic from many near-identical configs.

## 2) Agreed Decisions (Locked)

1. `useful_winrate = profitable_trades / all_trades`, where profitable means `pnl_pct > 0`.
2. Primary symbol score:
   `score = useful_winrate * ln(1 + trades)`.
3. Portfolio size per bot: up to `4` symbols.
4. Fast degradation checks: on each closed trade.
5. Slow health loop: every `5` minutes.
6. Fast symbol eject:
   - `4` stop-losses in a row, or
   - `useful_winrate(last_12) < 0.30`.
7. Config eject (quarantine):
   - `useful_winrate(last_40) < 0.40` and
   - `total_pnl(last_40) < 0`.
8. Adaptive revive from quarantine:
   - `useful_winrate(last_40) >= 0.45`,
   - `total_pnl(last_40) >= +0.20%`,
   - `trades_window >= 10`,
   - conditions pass for `2` consecutive windows.
9. Cooldown after symbol replacement: `5` minutes.
10. Introduce `20` config families, but:
   - dedupe near-identical configs first,
   - if unique configs `< 20`, use fewer families.

## 3) Why Family Layer Exists

Problem without families:

1. Many neighboring configs create near-duplicate symbol clusters.
2. Compute and storage grow quickly.
3. Portfolio behavior becomes unstable due to tiny parameter perturbations.

Family layer solves this by:

1. Grouping near-identical configs into shared behavior buckets.
2. Building symbol behavior clusters at family level.
3. Reusing cluster structure for all configs in the same family.

## 4) Considered Approaches

### Option A: Cluster symbols per each `config_id`

Pros:
1. Highest granularity.

Cons:
1. Heavy compute.
2. Low sample stability for sparse configs.
3. Large duplicated clusters.

### Option B: One global symbol clustering for all configs

Pros:
1. Lowest complexity.

Cons:
1. Ignores configuration-specific behavior regimes.

### Option C (Recommended): Family-level symbol clustering

Pros:
1. Strong balance of signal specificity and stability.
2. Much less duplication than per-config clustering.
3. Supports dynamic portfolio management with bounded complexity.

Cons:
1. Requires one extra layer (`config -> family` mapping).

## 5) Data Model

## 5.1 New artifacts

1. `data/config-families.json`
2. `data/family-symbol-clusters.json`
3. `data/portfolio-state.json`

## 5.2 `config-families.json` (example)

```json
{
  "version": 1,
  "generated_at_ms": 1772000000000,
  "family_count": 20,
  "families": [
    {
      "family_id": "F03",
      "member_config_ids": [101, 121, 148],
      "weights": {"101": 37, "121": 11, "148": 8},
      "centroid": {
        "spike_threshold_bps": 10.0,
        "target_ratio": 1.4,
        "stop_loss_bps": 8.0,
        "max_hold_ms": 15000
      }
    }
  ]
}
```

## 5.3 `family-symbol-clusters.json` (example)

```json
{
  "version": 1,
  "generated_at_ms": 1772000000000,
  "families": [
    {
      "family_id": "F03",
      "clusters": [
        {
          "cluster_id": "F03-C1",
          "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
          "cluster_stats": {
            "useful_winrate": 0.54,
            "avg_pnl_pct": 0.031,
            "stop_loss_share_pct": 0.22
          }
        }
      ]
    }
  ]
}
```

## 5.4 `portfolio-state.json` (example)

```json
{
  "version": 1,
  "updated_at_ms": 1772000000000,
  "active": [
    {
      "config_id": 101,
      "family_id": "F03",
      "cluster_id": "F03-C1",
      "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"],
      "cooldown_until_ms": 1772000300000,
      "quarantined": false
    }
  ],
  "quarantine": []
}
```

## 6) Computation Flow

## 6.1 Family build flow

1. Read candidate configs (from scout/expand/forward outputs).
2. Build a config fingerprint from parameter buckets.
3. Dedupe by fingerprint and keep duplicate count as `weight`.
4. Cluster unique configs into `min(20, unique_count)` families.
5. Persist `config_id -> family_id` mapping.

## 6.2 Symbol behavior aggregation

Per `(family_id, symbol)` keep rolling metrics:

1. `trades_last_12`, `profitable_last_12`, `sl_streak`
2. `trades_last_40`, `profitable_last_40`, `total_pnl_last_40`
3. `useful_winrate_last_12`, `useful_winrate_last_40`
4. `avg_pnl_last_40`, `stop_loss_share_last_40`

## 6.3 Family symbol clustering

For each family:

1. Build feature vector per symbol:
   - `useful_winrate_last_40`
   - `avg_pnl_last_40`
   - `stop_loss_share_last_40`
   - `trades_per_min`
2. Exclude symbols with too little data (`trades_window < 10`).
3. Cluster symbols (small K, deterministic seed).
4. Mark best cluster(s) by aggregate score.

## 6.4 Portfolio assignment

Per active config:

1. Resolve its `family_id`.
2. Pick best family cluster.
3. Rank symbols in that cluster by:
   `score = useful_winrate * ln(1 + trades)`.
4. Take top symbols up to max `4`.
5. Apply cooldown constraints.

## 7) Runtime Decision Logic

## 7.1 Fast-path (on each closed trade)

Inputs:
1. latest trade for `(config_id, symbol)`
2. rolling counters

Rules:
1. If `sl_streak >= 4` -> immediate `symbol_eject`.
2. If `useful_winrate(last_12) < 0.30` and enough samples -> immediate `symbol_eject`.
3. If config-level `useful_winrate(last_40) < 0.40` and `total_pnl(last_40) < 0` -> `config_quarantine`.

## 7.2 Slow-path (every 5 minutes)

1. Recompute rolling metrics.
2. Re-evaluate active symbols and config health.
3. Perform at most one symbol replacement per config per loop.
4. Enforce `cooldown = 5m`.

## 7.3 Replacement policy

On `symbol_eject`:
1. Remove symbol from active set.
2. Refill from same family cluster candidate queue.
3. If no candidate passes filters, keep fewer than 4 symbols temporarily.

## 7.4 Quarantine and revive

On `config_quarantine`:
1. Stop trading this config.
2. Track rolling recovery counters.
3. Revive only when:
   - `useful_winrate(last_40) >= 0.45`,
   - `total_pnl(last_40) >= +0.20%`,
   - `trades_window >= 10`,
   - above holds for `2` consecutive windows.
4. After revive, config returns to candidate pool (not forced directly to top priority).

## 8) Integration with Existing Pipeline

Current pipeline:

`scout -> expand -> forward -> promote`

Proposed extension:

1. `scout/expand/forward` keep collecting raw performance as now.
2. New post-processing stage builds:
   - config families,
   - family symbol clusters,
   - active portfolio assignments.
3. `promote` can optionally promote at family granularity:
   - top config(s) per family,
   - with chosen symbol portfolio snapshots.

No required breaking change to existing run IDs or trade schema in phase 1.

## 9) API/UI Design

## 9.1 New API endpoints (phase 1)

1. `GET /api/v1/portfolio/families`
2. `GET /api/v1/portfolio/clusters?family_id=...`
3. `GET /api/v1/portfolio/active`
4. `GET /api/v1/portfolio/quarantine`

## 9.2 Trials UI additions

On existing `/trials` page add a new tab `Portfolios`:

1. Family table:
   - `family_id`
   - member count
   - weighted performance
2. Cluster table per family:
   - cluster stats
   - symbol list
3. Active portfolio table:
   - `config_id`
   - family
   - selected symbols (<= 4)
   - cooldown, health flags
4. Quarantine table:
   - config
   - reason
   - revive progress (windows passed / 2)

## 10) Failure and Safety Behavior

1. Missing cluster data:
   - fallback to best known symbols by score in family.
2. Too few symbols with valid data:
   - allow undersized portfolio (<4).
3. Data lag / stale window:
   - freeze replacement decisions until fresh data appears.
4. Corrupt state files:
   - load last valid snapshot and continue read-only mode for decisions.

## 11) Rollout Plan

## Phase 1: Offline analytics only

1. Build family and cluster artifacts from existing DB history.
2. Expose read-only API/UI views.
3. Validate stability and quality.

## Phase 2: Shadow portfolio decisions

1. Compute replacements and quarantine decisions.
2. Log decisions, do not enforce trading changes yet.
3. Compare expected vs actual outcomes.

## Phase 3: Active enforcement

1. Enable symbol eject/replacement in runtime.
2. Enable config quarantine/revive.
3. Keep kill-switch to revert to static behavior.

## 12) Verification Checklist

1. Family dedupe works: 500 duplicate configs do not create extra families.
2. Family count is bounded by unique config count and max 20.
3. Fast-path eject triggers on exact thresholds.
4. Slow-path never violates cooldown.
5. Quarantine revive requires 2 consecutive windows.
6. Portfolio never exceeds 4 symbols.
7. System behaves safely with sparse trades.

## 13) Open Items

1. Exact clustering algorithm for symbols (k-means vs k-medoids vs bucket-based).
2. Whether to store state in DB tables or JSON snapshots in phase 1.
3. Promotion policy: top-1 per family vs top-N per family.

## 14) Recommendation

For first implementation:

1. Keep family count cap at 20 with pre-dedupe.
2. Start with deterministic bucket-based symbol clustering (simpler, debuggable).
3. Enforce thresholds already agreed above.
4. Ship phase 1 and phase 2 before enabling active enforcement.

