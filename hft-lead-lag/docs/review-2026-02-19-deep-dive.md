# Deep-Dive Review: Project Status (Current)

**Date:** 2026-02-19  
**Scope:** strategy math, fleet optimizer, DB/API consistency, live runtime behavior  
**Code base:** `main @ 807178a`

---

## 1) Executive verdict

Проект в рабочем состоянии для paper-loop и online optimizer цикла.

Сильные стороны:

- pipeline end-to-end стабилен (market data -> shadow/fleet -> DB -> ranking/UI),
- математика стратегии последовательна и прозрачна,
- baseline-window hyperparameter успешно интегрирован в code + DB + API.

Главный системный риск сейчас не в "поломке кода", а в **coverage**:

- много символов без сигналов при текущих порогах,
- ranking строится на узком активном подмножестве.

---

## 2) Что реально изменилось (важное)

1. **Baseline window стал гиперпараметром**
   - `baseline_window_ms` добавлен в:
     - `TraderConfig`
     - `config_id()`
     - grid generation
     - DB schema/migrations/upsert
     - API ranking payloads

2. **detect_gap() фикс**
   - baseline считается по срезу `now - baseline_window_ms`, а не по всей retention-истории.

3. **Грид синхронизирован под тайминг 10s..60s**
   - текущий размер: **2304 configs**.

4. **Endpoint surface**
   - рабочие:
     - `/api/v1/fleet`
     - `/api/v1/fleet/ranked`
     - `/api/v1/fleet/symbols`
     - `/fleet`

---

## 3) Математика и логика — ревью

### 3.1 Entry

Текущая формулировка корректна для lead-lag гипотезы:

- сигнал = текущий gap относительно baseline gap,
- baseline и threshold в bps консистентны,
- long/short симметрия соблюдена.

### 3.2 Exit

Двухфазная exit-модель (SL/timeout -> breakeven/trailing) реализована корректно и практична:

- `target_ratio` не является TP-выходом, а порогом перехода в фазу защиты прибыли,
- trailing логика не конфликтует с breakeven.

### 3.3 PnL

- bid/ask-aware расчёт,
- двухсторонняя комиссия,
- session-метрики консистентны.

Итог: явных математических багов в текущей ветке не обнаружено.

---

## 4) Runtime status snapshot

Срез с живого сервера на момент обновления:

- universe: **53 symbols**
- symbols with single-shadow trades: **11**
- symbols without single-shadow activity: **42**
- fleet ranked configs (`>=10 trades`): **100**
- best-by-symbol (`/fleet/symbols`): **3 symbols**

Top ranked config:

- `gap=30`, `target=0.7`, `sl=40`, `hold=5s`, `spread=5`, `trailing=0.7`, `baseline=60s`
- `trades=35`, `win_rate≈60%`, `avg_pnl≈0.0199%`

---

## 5) Ключевая диагностика текущего bottleneck

Почему кажется, что система "торгует мало":

1. Большая часть universe не достигает signal threshold с текущим grid-профилем.
2. В ranking попадают только конфиги с `>=10 trades`, поэтому видим узкую выборку.
3. Отдельные волатильные символы (малые/средние) вытягивают основную активность.

Это не ошибка DB/API; это сочетание параметров и рыночного режима.

---

## 6) Latency notes (проверено)

- Gate WS drift по screener в текущем запуске: низкие десятки ms (не сотни).
- REST-запросы ордерного API имеют другой профиль и не эквивалентны WS feed latency.
- `fill_delay_ms=6` остаётся paper-level допущением; для реал-исполнения нужен отдельный calibration слой.

---

## 7) Что хорошо / что сдерживает

### Хорошо

- Полный optimizer loop работает.
- Доки синхронизированы с текущим кодом.
- Введён полезный гиперпараметр тайминга baseline.

### Сдерживает

- Coverage символов ограничен.
- Нет adaptive capital allocation между топ-конфигами.
- Нет отдельного режима для low-gap/high-liquidity symbols.

---

## 8) P0 (обновлённый, практический)

1. **Coverage P0:** добавить более мягкие gap уровни для части grid (например 20 bps сегмент) и валидировать прирост активных символов.
2. **Execution realism P0:** вынести `fill_delay_ms` в отдельный profile (paper-fast vs realistic).
3. **Stability P0:** закрепить текущий ranked-score как основной endpoint для отбора конфигов в UI/ops.

---

## 9) Финальный вывод

Проект уже не в стадии "сломано/не работает"; он в стадии **quality scaling**:

- ядро работает,
- метрики собираются,
- параметрический контур живой,
- следующий выигрыш даёт расширение coverage и execution realism.

---

## 10) Addendum — commit review 2026-02-20 (`9ba7ee1..5c69ec1`)

Проведён отдельный аудит серии cognitive-complexity refactor + phase10.

### Что проверено

- Диффы коммитов в диапазоне `9ba7ee1..5c69ec1`.
- Поведенческая эквивалентность ключевых extraction-блоков (`main.rs`, `shadow_trader.rs`, `gate/mod.rs`).
- Полная валидация:
  - `cargo check --all-targets`
  - `cargo build`
  - `cargo test`

### Результат валидации

- Новых регрессий уровня **P0/P1**, внесённых этой серией коммитов, не подтверждено.
- Подтверждённый риск P1 в bootstrap подписках Gate **исправлен**:
  - `hft-lead-lag/src/main.rs` (`subscribe_gate_symbols`, timeout-ветка)
  - удалён `continue`, который пропускал `SUBSCRIBE_DELAY_MS` после timeout;
  - теперь delay применяется после каждой попытки подписки (success/error/timeout).
- Добавлены regression-тесты:
  - `gate_subscribe_delay_applies_after_timeout`
  - `gate_subscribe_delay_applies_after_success_and_error`

### Статус рекомендации

Рекомендация реализована: инвариант "delay обязателен после любой попытки подписки" зафиксирован в коде и тестах.

---

*Last updated: 2026-02-20 (Gate subscribe timeout-delay fix documented and validated)*
