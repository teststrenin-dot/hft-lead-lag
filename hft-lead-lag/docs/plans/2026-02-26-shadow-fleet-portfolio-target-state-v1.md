# Shadow Fleet Portfolio Target State v1

Date: 2026-02-26  
Status: In progress implementation (runtime+DB+API complete, shadow drill/go-live gate pending)

## 1) Objective

Build a trading system where shadow fleet first accumulates reliable symbol/config statistics, then eligible symbols are transferred into active bot portfolios.

## 2) Operating Model

1. Shadow fleet trades and accumulates statistics.
2. Symbols become candidates only after passing transfer gates.
3. Active portfolios are built from eligible symbols.
4. Portfolio race is maintained for analytics/leaderboard now, and for capital rebalance later.

## 3) Portfolio Topology

1. Exactly 2 portfolios are active at a time.
2. Mapping is strict: 1 portfolio = 1 bot.
3. Portfolio size is dynamic from 0 to 4 symbols.

## 4) Core Metric Definitions

1. `useful_winrate = profitable_trades / total_trades`
2. `profitable_trades` means trades with `pnl > 0`.
3. Minimum useful winrate for eligibility: `useful_winrate >= 30%`.
4. Auxiliary balance metric:
   - `pm_raw = profitable_trades - losing_trades`,
   - where `losing_trades` means trades with `pnl < 0`.
5. Target quality is defined as stable joint behavior of:
   - `pnl`
   - `useful_winrate`
6. Selection preference:
   - higher `useful_winrate` is always better,
   - higher `pm_raw` is better as additional strength signal,
   - `avg_pnl_pct` is used as tie-breaker and must be non-negative.

## 5) Symbol Transfer Rules (Fleet -> Portfolio)

A symbol is transferable only if all minimum conditions pass:

1. Symbol has been observed for more than 5 minutes.
2. Symbol has more than 5 closed trades in shadow fleet.
3. Entry metrics are computed globally across the full shadow fleet.
4. Entry metrics are computed on full cumulative history (no rolling window in v1).
5. Symbol has minimum useful winrate (`useful_winrate >= 30%`).
6. Symbol has non-negative average pnl (`avg_pnl_pct >= 0`).
7. `useful_winrate` is used for ranking (higher is better).
8. Candidate competition:
   - symbols compete for portfolio slots by "best quality",
   - each portfolio builds its own shortlist top-5,
   - the same symbol cannot be active in both portfolios at the same time,
   - if both portfolios request the same symbol, it is assigned to the portfolio with better metrics by tuple:
     `(useful_winrate desc, pm_raw desc, avg_pnl_pct desc, closed_trades desc)`,
   - final portfolio includes up to 4 symbols.

Formal entry check:

`eligible(symbol) = (age_minutes_from_first_tick > 5) AND (closed_trades > 5) AND (useful_winrate >= 0.30) AND (avg_pnl_pct >= 0)`

## 6) Eject and Reset Rules

1. Eject trigger (only rule for reject in v1):
   - symbol is removed on stop-loss streak trigger.
2. Stop-loss streak scope:
   - streak is tracked per symbol,
   - streak is reset by a profitable trade (`pnl > 0`).
3. Hard reset:
   - symbol-level only (no full-portfolio reset),
   - uses the same stop-loss trigger as eject.
4. Stop-loss streak trigger:
   - fast trigger: 5 stop-losses in a row within 2 minutes,
   - persistent trigger: if fast trigger did not fire, hard reset fires on the 6th stop-loss in the same streak (regardless of elapsed time).

## 7) Re-Entry Rules

After eject/reset, symbol can return only after cooldown >= 5 minutes.
After cooldown, symbol returns to the common candidate pool first and must pass `eligible(symbol)` again before portfolio assignment.

## 8) Rebalance Cadence

Background portfolio rebalance loop runs every 2 minutes.

## 9) Portfolio Race Policy

1. Current phase: race affects analytics only (leaderboard/stats), not money allocation.
2. Future phase: race can drive dynamic capital rebalance.

## 10) Scope Notes

1. Dynamic hyperparameter policy is explicitly out of scope for this version.
2. This document captures only locked business behavior and operational rules.
3. Any adaptive/dynamic threshold model will be defined in a separate v2 document.
