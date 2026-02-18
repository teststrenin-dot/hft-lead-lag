# Task: Web Screener + Lead-Lag Logic (Test Checkpoints)

**Статус**: 🟡 In progress (2026-02-18: Screener MVP + live coverage fix)  
**Приоритет**: P0  
**Порядок выполнения**:
1. **Задача #1** — Web Screener
2. **Задача #2** — Lead-Lag logic

---

## Цель

Сделать простой веб-скринер и формализовать lead-lag логику так, чтобы поведение можно было проверить через четкие тестовые checkpoint’ы.

---

## Задача #1: Web Screener (самый простой UI)

### Что должно быть в скринере

- Таблица метрик с колонками:
  - `Монета` (`symbol`)
  - `Кто лид` (`leader_exchange`: `binance` / `gate`)
  - `Время отставания, ms` (`lag_ms`)
  - `Half-life entry window, ms` (`entry_half_life_ms`)
  - `Entry w, ms` (`entry_w_ms`)
- Сортировка по `lag_ms` **по убыванию** (от наибольшей к меньшей) по умолчанию.
- В выборке только инструменты с `24h quote_volume >= 1_000_000 USD`.
- Определение `entry_half_life_ms`: среднее время от события расхождения `P90` (entry trigger) до события схождения `P50` (exit trigger) в скользящем окне `p=10m`.
- Определение `entry_w_ms`: среднее время нахождения в зоне `spread_bid_ask >= P90` в окне `p=10m` (длительность окна, где лимитный вход потенциально может быть исполнен).

### Фактический статус на 2026-02-18

- ✅ Реализованы `/screener` и `/api/v1/screener`.
- ✅ Исправлено ограничение live-потока «только 8 монет»: скринер подписывается на весь `common_symbols`, Binance подписка батчами.
- ✅ В runtime-проверке после фикса: `total_rows=536`, `non_zero_lag_rows=104`.
- ⬜ Runtime-вычисление `entry_w_ms` в API/UI еще не завершено.

### Тестируемые checkpoint’ы (Задача #1)

1. **SCREENER-CP-01 / Структура данных**  
   Каждая строка содержит `symbol`, `leader_exchange`, `lag_ms`, `entry_half_life_ms`, `entry_w_ms`.

2. **SCREENER-CP-02 / Volume filter**  
   В выдаче нет символов с `quote_volume < 1_000_000`.

3. **SCREENER-CP-03 / Сортировка**  
   `lag_ms[i] >= lag_ms[i+1]` для всей таблицы.

4. **SCREENER-CP-04 / Валидность lag**  
   `lag_ms >= 0`, `leader_exchange ∈ {binance, gate}`.

5. **SCREENER-CP-05 / Время отклика**  
   Получение данных для UI в пределах рабочего бюджета (операционный целевой SLA: до ~5s в checkpoint-режиме).

6. **SCREENER-CP-06 / Half-life корректность**  
   `entry_half_life_ms` считается как `AVG(t_p50_convergence - t_p90_divergence)` по валидным циклам в окне `p=10m`, значение неотрицательное.

7. **SCREENER-CP-07 / Entry-w корректность**  
   `entry_w_ms` считается как `AVG(duration(spread_bid_ask >= P90))` по завершенным интервалам в окне `p=10m`, значение неотрицательное.

---

## Задача #2: Lead-Lag Logic (правила входа/выхода)

### Параметры

- Период `p` = **10 скользящих минут**.
- Percentile-уровни в окне `p`:
  - `Entry threshold` = `P90`
  - `Exit threshold` = `P50`

### Определения спредов

Для пары лидер/лаггер:

- `spread_bid_ask = bid(leader) - ask(lagger)`  
- `spread_ask_bid = ask(leader) - bid(lagger)`

### Правила

1. **Вход (на раскоре bid/ask P90)**  
   Вход в позицию, когда `spread_bid_ask >= P90(spread_bid_ask, window=10m)`.

2. **Выход (на ask/bid P50)**  
   Выход из позиции, когда `spread_ask_bid <= P50(spread_ask_bid, window=10m)`.

3. **Обратное направление — аналогично**  
   Для реверса (когда лидером становится вторая биржа) применяются те же правила симметрично.

### Тестируемые checkpoint’ы (Задача #2)

1. **LEADLAG-CP-01 / Rolling window**  
   Percentile считается строго по последним 10 минутам.

2. **LEADLAG-CP-02 / Entry trigger**  
   Сигнал входа срабатывает только при достижении/превышении `P90` по `spread_bid_ask`.

3. **LEADLAG-CP-03 / Exit trigger**  
   Сигнал выхода срабатывает при возврате к `P50` по `spread_ask_bid`.

4. **LEADLAG-CP-04 / Reverse symmetry**  
   Для обратного направления правила идентичны.

5. **LEADLAG-CP-05 / Проверяемость решения**  
   Для каждого сигнала доступны: `symbol`, `leader`, `lag_ms`, `entry_percentile`, `exit_percentile`, `window_start/end`.

---

## Подход разработки

- Для реализации используем **superpowers** (планирование/дебаг/верификация) и **сабагентов** (исследование/ревью/проверка гипотез).
- Разработка ведется последовательно по задачам: сначала Screener, затем Lead-Lag logic.

---

*Created: 2026-02-18*
