# Dead Code Review (R4)

## Findings
- **P3** `src/domain/messages.rs:170-177`  
  `SubscriptionResult` type is defined but unused.

- **P3** `src/infrastructure/rest/mod.rs:14-30`  
  `RestConfig` exists but has no active usage path.

- **P3** `src/config/mod.rs:33-47`  
  `VolumeFilter` and `TradingConfig` are declared but not consumed.

- **P2** `src/infrastructure/db.rs:103-141`, `src/infrastructure/db.rs:894-905`  
  `config_families`, `family_symbol_clusters`, and `portfolio_state` schema are created/tested but have no active read/write runtime flow.

## Decision Needed
- Remove unused types/tables, or wire them into active product paths with tests.
