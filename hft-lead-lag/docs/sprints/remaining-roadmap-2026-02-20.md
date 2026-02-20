# Remaining Development Roadmap (2026-02-20)

Цель: перевести проект из режима "working paper" в режим "robust profit extraction" с контролируемым качеством данных, адаптивной policy-логикой и готовностью к pre-live.

Основано на:

1. `docs/review-2026-02-20-comprehensive-audit.md`
2. `docs/review-2026-02-20-profit-deep-dive.md`

---

## 1) Последовательность спринтов

### Sprint 004 — Correctness Hardening Foundation

1. Фокус: корректность парсинга, единая time policy, config/db контракт, quality-gates.
2. Основной риск, который закрываем: "ложная альфа из-за грязных данных".
3. Детальный план: `docs/sprints/sprint-004-correctness-hardening.md`

### Sprint 005 — Policy Allocator and Symbol Gates

1. Фокус: динамический отбор конфигов по символам/режимам, gating убыточных символов.
2. Основной риск, который закрываем: "деградация при regime shift и концентрация в 1-2 символах".
3. Детальный план: `docs/sprints/sprint-005-policy-allocator-and-gates.md`

### Sprint 006 — Dislocation Reversion Strategy + A/B

1. Фокус: вторая стратегия (`P90/P10 -> P50`, dwell filter), сравнительный shadow A/B.
2. Основной риск, который закрываем: "одностратегийная хрупкость".
3. Детальный план: `docs/sprints/sprint-006-dislocation-reversion-ab.md`

### Sprint 007 — Pre-Live Reliability and Release Readiness

1. Фокус: архитектурные швы, hot-path cost control, CI/CD quality gates, ops runbooks.
2. Основной риск, который закрываем: "неуправляемый переход paper -> live".
3. Детальный план: `docs/sprints/sprint-007-prelive-reliability.md`

---

## 2) Зависимости между спринтами

1. Sprint 005 зависит от Sprint 004.
: policy нельзя строить на ненадежных parser/time данных.
2. Sprint 006 зависит от Sprint 004 и частично от Sprint 005.
: стратегия B должна сравниваться на том же scoring/policy каркасе.
3. Sprint 007 зависит от Sprint 004-006.
: pre-live readiness требует стабилизированного behavior и метрик.

---

## 3) Общие метрики прогресса (cross-sprint)

1. Качество инженерного контура:
: `cargo test` green, `cargo clippy --all-targets -- -D warnings` green, deterministic tests.
2. Качество торгового контура:
: rolling expectancy per symbol, stop_loss share, concentration (HHI), regime stability.
3. Эксплуатационная готовность:
: rollback path, incident runbook, alerting on drops/drift.

---

## 4) Контрольные точки (milestones)

1. M1 (после Sprint 004): Data correctness baseline proven.
2. M2 (после Sprint 005): Adaptive policy in production paper loop.
3. M3 (после Sprint 006): Strategy A/B evidence collected and ranked.
4. M4 (после Sprint 007): Pre-live checklist completed with go/no-go packet.

---

## 5) Правило поставки

Для каждого спринта обязательны:

1. phase-by-phase commit trail,
2. evidence-блок с командами и результатами,
3. обновление документации в `docs/` (не только код),
4. явный список out-of-scope для контроля расползания.
