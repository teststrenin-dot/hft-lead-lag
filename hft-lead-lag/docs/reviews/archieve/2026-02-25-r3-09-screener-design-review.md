# Screener Design Review (R3)

## Findings
- **P2** `src/domain/screener/mod.rs:385,444`  
  Unbounded symbol retention + full-map rows build/sort each request (`O(n log n)`) under 1s polling UI cadence.

- **P2** `src/infrastructure/enrichment.rs:26-80`, `src/api/handlers.rs:186-201`  
  Fallback NATR enrichment fetches only up to six symbols per cycle, leaving many rows with placeholder `0.0` and no explicit staleness/source signal for that field.

- **P3** `src/domain/screener/state.rs:46-194`, `src/api/templates/screener.html:56-133`  
  Undefined/not-ready lag/drift states are rendered as numeric zeros, ambiguous with real zero values.

## Recommendations
- Add row snapshot caching and symbol eviction policy.
- Add `gate_natr_status`/staleness markers in DTO.
- Use nullable metrics or explicit readiness flags in UI contract.
