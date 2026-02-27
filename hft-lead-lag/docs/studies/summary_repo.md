Открыл твой zip и реально посмотрел именно то, что ты сказал:

* `docs/status/core/2026-02-27-business-objective-economic-control-map.md`
* `docs/status/core/2026-02-26-business-logic-roadmap.md`
* `docs/status/core/2026-02-26-business-logic-v1-implementation-status.md`
* `docs/status/dynamics/2026-02-26-delivery-contract-first-playbook.md`
* `docs/status/dynamics/2026-02-27-cp0-contract-freeze-v2.md`
* плюс код, который “материализует” эти правила: `src/domain/screener/portfolio_runtime.rs` и интеграцию в `src/domain/screener/mod.rs`.

Ниже — мои рекомендации **именно уровня “протянуть весь проектный бизнес‑процесс под бизнес‑задачу”**, а не тактика внутри фичи.

---

## 1) У тебя уже есть то, чего обычно нет: “Strategic Anchor” и цепь управления

Это реально сильная штука: **Economic Control Map** с фиксированной цепочкой
`Signal -> Validation -> Competition -> Risk -> Capital -> Feedback`
и правило “каждая задача должна маппиться на узел карты” (это прям правильная мета‑стратегия).
См. `docs/status/core/2026-02-27-business-objective-economic-control-map.md` и `docs/status/dynamics/2026-02-26-delivery-contract-first-playbook.md`.

**Главная проблема сейчас не “нет стратегии”, а:**

1. стратегия не доведена до *жёстких, измеримых gate’ов* (SLO/acceptance) на узлах,
2. и местами есть *семантический дрейф между доками и кодом*.

Дальше — что именно я бы подкрутил.

---

## 2) P0: Свести в один “Operating Model Spec” весь процесс (как ты написал 1–12), но *с привязкой к артефактам*

Сейчас процесс описан в нескольких местах (roadmap/status/README/runbooks). Это хорошо, но для “протянуть весь бизнес‑процесс” нужен **один канонический документ**, где на одной странице:

### 2.1. Поток создания ценности (вход → выход)

Прям как твои пункты 1–12, но с полями:

* **Input** (что приходит)
* **Transform** (какая функция)
* **Output** (что считается результатом)
* **State** (где хранится)
* **API/UX surface** (где это видно оператору)
* **Failure modes** (как ломается)
* **Acceptance / SLO** (как считаем “ок”)

### 2.2. Привязка к узлам Control Map

Каждый шаг помечается как Signal/Validation/… чтобы “мета‑стратегия” была прямо внутри процесса.

### 2.3. Привязка к реальным контрактам

Ссылки на:

* endpoints из `cp0-contract-freeze-v2.md`,
* таблицы (типа `portfolio_state_v1`, `portfolio_symbol_guard_v1`, `trades`, `trial_runs_meta`),
* и на runbook проверки (shadow drill / live gate).

**Почему это важно:**
это превращает “описание” в **исполняемую спецификацию** проекта. Тогда эволюционная разработка резко падает, потому что любой новый кусок обязан занять место в этом потоке и получить acceptance.

---

## 3) P0: Устранить дрейф “семантика Competition” между докой и реализацией

В статус/планах местами звучит так, будто портфели “запрашивают” топ‑символы и потом конфликт решается по tuple.

Но в текущем коде `src/domain/screener/portfolio_runtime.rs` логика такая:

1. строится **единый глобальный ranked pool** кандидатов
2. **shortlist’ы делаются disjoint** через round‑robin по пулу (`build_shortlists_no_overlap`) — т.е. один и тот же символ **не попадает** в shortlist двух портфелей
3. active тоже disjoint (`assign_active_symbols_no_overlap`)

Это *другая семантика Competition*, чем “независимые shortlist’ы + конфликт резолвим”.

### Рекомендация:

Выбери и зафиксируй **одну** из двух моделей:

**Модель A (как сейчас в коде): “allocation”**

* портфели = не конкурируют за один символ на shortlist-уровне,
* ты скорее распределяешь внимание между портфелями,
* проще, стабильнее, меньше конфликтов.

**Модель B (как в исходной формулировке): “competition”**

* каждый портфель строит свой top‑K из общего пула,
* overlap разрешён на shortlist, но запрещён на active,
* нужен явный конфликт‑резолвер по tuple (или по “portfolio score”).

**Что сделать прямо сейчас (P0):**

* обновить один канонический документ (см. пункт 2) и статус (`implementation-status.md`), чтобы они **буквально совпадали** с тем, что делает runtime;
* и добавить 1–2 контрактных теста, которые фиксируют выбранную семантику (иначе через месяц ты “эволюционно” уедешь в третью модель).

---

## 4) P0: “Validation” слишком шумный из-за малого n — добавь penalization за малую статистику (без усложнения v1)

Сейчас gate:

* `age > 5m`
* `closed_trades > 5`
* `useful_winrate >= 0.30`
* `avg_pnl_pct >= 0`

Для бизнеса это очень ранняя фильтрация: 6 сделок — это почти ничего, и “avg_pnl >= 0” при таком n часто просто случайность.

Но ты правильно хочешь “протянуть бизнес‑процесс”: значит Validation должен **гарантировать качество допуска**, а не просто “не совсем мусор”.

### Минимальная правка, не ломая v1:

Добавь *в score/ranking* (не обязательно в gate) штраф за малый объём:

В духе того, что у тебя уже было как идея в `docs/plans/2026-02-24-family-cluster-portfolio-design.md`:

* `score = useful_winrate * ln(1 + trades)`
  или
* `score = useful_winrate - k / sqrt(trades)` (penalty)

Это не требует сложной статистики, но резко снижает “выбор по шуму”.

**Важно:** даже если gate оставишь как есть (чтобы не убить приток кандидатов), ranking пусть перестанет быть “winrate first без доверия”.

---

## 5) P1: В “Competition” добавь контроль churn/turnover (иначе 2‑мин ребаланс будет пожирать качество)

Сейчас ребаланс каждые 2 минуты (`PORTFOLIO_REBALANCE_INTERVAL_MS` в `src/domain/screener/mod.rs`).

Если без “гистерезиса”/стикости, будет:

* прыжки shortlist/active,
* ломаться причинно‑следственная связь “портфель X лучший” (он просто постоянно другой),
* и в будущей live‑фазе это станет комиссионной мясорубкой.

### Минимально (P1) — добавить один из механизмов:

* **min tenure**: символ нельзя выкинуть из active раньше, чем через N минут, если нет eject/reset
  или
* **swap threshold**: заменяем актив, только если новый кандидат лучше текущего на Δ по score
  или
* **turnover budget**: в один ребаланс менять не больше 1–2 символов на портфель

И обязательно вывести метрику:

* `portfolio_turnover_per_hour`
* `active_symbol_half_life`

Это прям “Competition quality KPI” из твоего control-map.

---

## 6) P1: “Risk” — сейчас reset/cooldown есть, но re-entry на полной истории делает cooldown почти косметикой

В `PortfolioEngineV1::can_reenter()` после cooldown ты снова проверяешь `eligible(stats)`, но `stats` — кумулятивные по всей истории.

То есть символ после плохой серии:

* подождёт 5 минут
* и почти гарантированно снова пройдёт gate, потому что его “общая история” всё ещё ок.

**Это норм как v1**, если цель — просто “fail-fast и не держать в active в моменте”.

Но если цель Risk‑узла — “contain degradation”, то в v2 тебе почти неизбежно нужен один из вариантов:

* **epoch stats**: метрики “с последнего reset” (или хотя бы после cooldown)
* либо rolling window stats (last N trades / last T minutes)
* либо “quarantine” режим (как у тебя в draft про family design)

### Рекомендация по процессу:

Не тащи это в v1 (иначе снова эволюционный раздув).
Но добавь в CP7/V2 backlog как **Risk‑upgrade обязательный** + KPI “post‑cooldown relapse rate”.

---

## 7) P0/P1: Paper performance сейчас — это “атрибуция”, а не “изолированное исполнение”. Зафиксируй это как инвариант до CP4→CP5

Ты сам пишешь, что хочешь “1 portfolio = 1 bot runtime context” — и в статусе это open gap (`Implementation Status Tracker`, блок “Current Open Gaps”).

Сейчас по факту:

* сделки приходят из общего execution engine (fleet),
* портфельная “торговля” — это присвоение сделок активному владельцу (через `assignment_history`).

Это ок как этап “Competition analytics”, но важно **не перепутать** это с реальной изоляцией портфельного бота.

### Что я бы сделал:

В твоём каноническом Operating Model Spec (см. пункт 2) явно написать:

* CP4 = **portfolio race analytics via attribution**
* CP4.Х/CP5 = **portfolio isolation / separate execution loops**

И добавить acceptance для перехода:

* “портфельный PnL детерминированно воспроизводится при replay”
* “нет двойного учёта при рестарте” (см. следующий пункт)

---

## 8) P0: CP5 “Reliability” — главный скрытый риск: идемпотентность snapshot + отсутствие double count

Ты правильно фиксируешь “silent-loss incidence = zero” в KPI envelope.

Но есть симметричный риск: **silent duplication** (двойной учёт) при рестартах/повторах drained trades.

Я вижу, что для candidate history у тебя есть явный contract freeze: event-collapse по `(symbol, exit_ts_ms)` (см. `cp0-contract-freeze-v2.md`).

### Рекомендация:

Сделай аналогичный “event key” контракт для:

* обновления guard state
* обновления paper_state

И в CP5 добавь тестовый сценарий:

* имитируем повторную подачу одного и того же drained trade batch → состояние не меняется второй раз

Это сильно повышает “Feedback/Risk integrity” и закрывает класс багов, которые потом адски трудно отлавливать.

---

## 9) CP6: Сделай UI/Telemetry не “страницами”, а “панелью узлов Control Map”

Сейчас `/portfolio`, `/fleet`, `/trials` уже есть (и это круто для оператора).

Но чтобы это стало **верхнеуровневым управлением бизнес‑процессом**, добавь один “Control Map dashboard”, где по каждому узлу:

* Signal: feed alive, drift, lag, quote skew, freshness
* Validation: admission rate, distribution winrate/avg_pnl, “new candidates/day”
* Competition: utilization, turnover, active count, top candidates
* Risk: resets/hour, cooldown hit rate, relapse after cooldown
* Feedback: restart OK, db dropped batches, stale run lease (у тебя уже были инциденты с lease — это прям must-have сигнал)
* Capital: locked/planned + readiness checklist state

Это превращает проект в **управляемую систему**, а не набор страниц.

---

## 10) Что бы я поставил как “следующие 3 стратегические задачи” (без расширения scope)

Если коротко и по делу:

1. **Зафиксировать канонический Operating Model Spec** (п.2) + синхронизировать семантику Competition (п.3)
2. **Сделать churn control** в Competition (п.5) — иначе гонка будет нестабильной и непригодной для будущего капитала
3. **CP5: идемпотентность и анти-double-count** для portfolio state (п.8) — это фундамент для всего live

---

Если хочешь, я могу сделать тебе прямо готовый черновик **Operating Model Spec v1** (в формате твоих status-доков) — на основе твоего описания 1–12 + того, что реально в коде/контрактах сейчас. Это будет “одна страница истины”, которую ты потом просто держишь frozen и маппишь на неё CP4–CP7.
