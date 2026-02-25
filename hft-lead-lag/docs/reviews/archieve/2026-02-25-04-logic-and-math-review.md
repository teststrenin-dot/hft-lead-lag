# Review: Логика и Математика

## Findings

### P2

1. Baseline gap усредняется через общий `count`, даже когда вклад в ask/bid-суммы неполный.
   - Path:
     - `src/domain/screener/shadow_trader.rs:430`
   - Риск: baseline занижается, сигнал искусственно завышается.

2. Policy score смешивает единицы (`avg_pnl_pct` как percent и `win_rate/stop_loss_share` как ratio).
   - Path:
     - `src/domain/screener/shadow_fleet.rs:228`
   - Риск: веса фактически не отражают ожидаемую относительную важность компонентов.

## Verdict

Критических математических аварий не выявлено, но найдено два системных искажения, влияющих на качество сигналов и ранжирование конфигов.
