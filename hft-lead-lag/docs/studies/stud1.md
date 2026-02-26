Ок. Ниже — **“как под PR”**: чёткий контракт ответа `/api/v1/portfolio/active`, план **snapshot V1→V2**, и **детерминированный no‑overlap allocator** + набор тестов Given/When/Then, привязанный к твоей текущей математике (useful_winrate / pm_raw / eligibility gate / cooldown).

---

# 1) API контракт: `/api/v1/portfolio/active` (V2 payload)

### Цели контракта (CP1.1–CP1.3)

* UI рендерит **произвольное N портфелей** без хардкода `A/B`.
* На каждый портфель отдаём:

  * `shortlist` (с метриками и eligibility)
  * `active` (уникальные символы без пересечений)
  * `guards` (хотя бы cooldown окна/статус)
  * `runtime` (last_rebalance, tick cadence — чтобы дебажить)
* **Детерминизм**: порядок портфелей и shortlist уже отсортированы.

---

## 1.1 JSON Schema (Draft 2020-12, упрощённый)

> Это “жёсткое” описание. Поля можно сузить, но лучше не расширять без version bump.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "portfolio_active_v2.schema.json",
  "title": "ActivePortfoliosResponseV2",
  "type": "object",
  "required": ["schema_version", "ts_ms", "portfolios"],
  "properties": {
    "schema_version": {
      "type": "string",
      "const": "portfolio_active.v2"
    },
    "ts_ms": { "type": "integer", "minimum": 0 },

    "runtime": {
      "type": "object",
      "required": ["event_loop_tick_every_ms", "rebalance_min_interval_ms"],
      "properties": {
        "event_loop_tick_every_ms": { "type": "integer", "minimum": 1 },
        "rebalance_min_interval_ms": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": false
    },

    "portfolios": {
      "type": "array",
      "items": { "$ref": "#/$defs/PortfolioView" }
    }
  },
  "additionalProperties": false,

  "$defs": {
    "PortfolioView": {
      "type": "object",
      "required": [
        "id",
        "updated_ts_ms",
        "last_rebalance_ts_ms",
        "shortlist",
        "active"
      ],
      "properties": {
        "id": { "type": "string", "minLength": 1 },

        "updated_ts_ms": { "type": "integer", "minimum": 0 },
        "last_rebalance_ts_ms": { "type": "integer", "minimum": 0 },

        "shortlist_limit": { "type": "integer", "minimum": 0 },
        "active_limit": { "type": "integer", "minimum": 0 },

        "active": {
          "type": "array",
          "items": { "$ref": "#/$defs/ActiveSymbolView" }
        },

        "shortlist": {
          "type": "array",
          "items": { "$ref": "#/$defs/CandidateView" }
        },

        "guards": {
          "type": "object",
          "properties": {
            "cooldown_active": { "type": "boolean" },
            "cooldown_until_ts_ms": { "type": ["integer", "null"], "minimum": 0 },
            "notes": { "type": "string" }
          },
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    },

    "ActiveSymbolView": {
      "type": "object",
      "required": ["symbol"],
      "properties": {
        "symbol": { "type": "string", "minLength": 1 },

        "cooldown_until_ts_ms": { "type": ["integer", "null"], "minimum": 0 },

        "useful_winrate": { "type": ["number", "null"], "minimum": 0, "maximum": 1 },
        "pm_raw": { "type": ["integer", "null"] },
        "avg_pnl_pct": { "type": ["number", "null"] },
        "closed_trades": { "type": ["integer", "null"], "minimum": 0 }
      },
      "additionalProperties": false
    },

    "CandidateView": {
      "type": "object",
      "required": [
        "symbol",
        "eligibility",
        "useful_winrate",
        "pm_raw",
        "avg_pnl_pct",
        "closed_trades",
        "age_minutes"
      ],
      "properties": {
        "symbol": { "type": "string", "minLength": 1 },

        "eligibility": {
          "type": "object",
          "required": ["passed", "reasons"],
          "properties": {
            "passed": { "type": "boolean" },
            "reasons": { "type": "array", "items": { "type": "string" } }
          },
          "additionalProperties": false
        },

        "useful_winrate": { "type": "number", "minimum": 0, "maximum": 1 },
        "pm_raw": { "type": "integer" },
        "avg_pnl_pct": { "type": "number" },
        "closed_trades": { "type": "integer", "minimum": 0 },
        "profitable_trades": { "type": ["integer", "null"], "minimum": 0 },
        "losing_trades": { "type": ["integer", "null"], "minimum": 0 },

        "first_ts_ms": { "type": ["integer", "null"], "minimum": 0 },
        "age_minutes": { "type": "number", "minimum": 0 },

        "cooldown_until_ts_ms": { "type": ["integer", "null"], "minimum": 0 },
        "stop_loss_streak": { "type": ["integer", "null"], "minimum": 0 }
      },
      "additionalProperties": false
    }
  }
}
```

**Почему так:**

* `schema_version` позволяет спокойно эволюционировать контракт.
* `active` возвращаем не просто символами, а объектами: UI сможет показать quick‑метрики без поиска в shortlist.
* `eligibility.reasons` — это “debuggable бизнес‑логика”: почему кандидат вылетел (возраст, трейды, winrate, avg_pnl).

---

## 1.2 Пример payload (для 3 портфелей)

```json
{
  "schema_version": "portfolio_active.v2",
  "ts_ms": 1760000000000,
  "runtime": {
    "event_loop_tick_every_ms": 120000,
    "rebalance_min_interval_ms": 120000
  },
  "portfolios": [
    {
      "id": "A",
      "updated_ts_ms": 1760000000000,
      "last_rebalance_ts_ms": 1759999880000,
      "shortlist_limit": 10,
      "active_limit": 2,
      "active": [
        {
          "symbol": "BTCUSDT",
          "cooldown_until_ts_ms": null,
          "useful_winrate": 0.46,
          "pm_raw": 7,
          "avg_pnl_pct": 0.031,
          "closed_trades": 52
        },
        {
          "symbol": "SOLUSDT",
          "cooldown_until_ts_ms": null,
          "useful_winrate": 0.41,
          "pm_raw": 3,
          "avg_pnl_pct": 0.012,
          "closed_trades": 33
        }
      ],
      "shortlist": [
        {
          "symbol": "BTCUSDT",
          "eligibility": { "passed": true, "reasons": [] },
          "useful_winrate": 0.46,
          "pm_raw": 7,
          "avg_pnl_pct": 0.031,
          "closed_trades": 52,
          "profitable_trades": 29,
          "losing_trades": 22,
          "first_ts_ms": 1759996000000,
          "age_minutes": 66.7,
          "cooldown_until_ts_ms": null,
          "stop_loss_streak": 0
        },
        {
          "symbol": "ETHUSDT",
          "eligibility": { "passed": true, "reasons": [] },
          "useful_winrate": 0.44,
          "pm_raw": 5,
          "avg_pnl_pct": 0.019,
          "closed_trades": 41,
          "profitable_trades": 23,
          "losing_trades": 18,
          "first_ts_ms": 1759996100000,
          "age_minutes": 65.0,
          "cooldown_until_ts_ms": 1760000100000,
          "stop_loss_streak": 6
        }
      ],
      "guards": {
        "cooldown_active": false,
        "cooldown_until_ts_ms": null,
        "notes": ""
      }
    },

    {
      "id": "B",
      "updated_ts_ms": 1760000000000,
      "last_rebalance_ts_ms": 1759999880000,
      "shortlist_limit": 10,
      "active_limit": 2,
      "active": [
        {
          "symbol": "ETHUSDT",
          "cooldown_until_ts_ms": null,
          "useful_winrate": 0.40,
          "pm_raw": 2,
          "avg_pnl_pct": 0.010,
          "closed_trades": 28
        }
      ],
      "shortlist": [
        {
          "symbol": "ETHUSDT",
          "eligibility": { "passed": true, "reasons": [] },
          "useful_winrate": 0.40,
          "pm_raw": 2,
          "avg_pnl_pct": 0.010,
          "closed_trades": 28,
          "profitable_trades": 15,
          "losing_trades": 13,
          "first_ts_ms": 1759996400000,
          "age_minutes": 60.0,
          "cooldown_until_ts_ms": null,
          "stop_loss_streak": 1
        },
        {
          "symbol": "BTCUSDT",
          "eligibility": { "passed": true, "reasons": [] },
          "useful_winrate": 0.39,
          "pm_raw": 1,
          "avg_pnl_pct": 0.007,
          "closed_trades": 31,
          "profitable_trades": 16,
          "losing_trades": 15,
          "first_ts_ms": 1759995000000,
          "age_minutes": 83.3,
          "cooldown_until_ts_ms": null,
          "stop_loss_streak": 0
        }
      ],
      "guards": { "cooldown_active": false, "cooldown_until_ts_ms": null, "notes": "" }
    },

    {
      "id": "C",
      "updated_ts_ms": 1760000000000,
      "last_rebalance_ts_ms": 1759999880000,
      "shortlist_limit": 10,
      "active_limit": 2,
      "active": [],
      "shortlist": [
        {
          "symbol": "BTCUSDT",
          "eligibility": { "passed": false, "reasons": ["avg_pnl_pct < 0", "closed_trades <= 5"] },
          "useful_winrate": 0.20,
          "pm_raw": -3,
          "avg_pnl_pct": -0.021,
          "closed_trades": 5,
          "profitable_trades": 1,
          "losing_trades": 4,
          "first_ts_ms": 1759999900000,
          "age_minutes": 1.6,
          "cooldown_until_ts_ms": null,
          "stop_loss_streak": 0
        }
      ],
      "guards": { "cooldown_active": false, "cooldown_until_ts_ms": null, "notes": "no eligible candidates" }
    }
  ]
}
```

---

# 2) Snapshot V1 → V2: формат и миграция

## 2.1 Почему вообще нужна версионизация

CP1.1 требует:

* N портфелей вместо A/B
* совместимость restore после рестарта

Самый дешёвый и надёжный путь: **в snapshot всегда писать `version`**, а при загрузке — мигрировать в память до V2.

---

## 2.2 Пример Snapshot V1 (как было “A/B hardcoded”)

*(упрощённо — структура иллюстративная)*

```json
{
  "version": 1,
  "saved_ts_ms": 1760000000000,
  "A": { "last_rebalance_ts_ms": 1759999880000, "active": ["BTCUSDT"], "cooldowns": { "ETHUSDT": 1760000100000 } },
  "B": { "last_rebalance_ts_ms": 1759999880000, "active": ["ETHUSDT"], "cooldowns": {} }
}
```

## 2.3 Snapshot V2 (динамический)

```json
{
  "version": 2,
  "saved_ts_ms": 1760000000000,
  "portfolios": [
    {
      "id": "A",
      "last_rebalance_ts_ms": 1759999880000,
      "active": ["BTCUSDT"],
      "cooldowns": { "ETHUSDT": 1760000100000 }
    },
    {
      "id": "B",
      "last_rebalance_ts_ms": 1759999880000,
      "active": ["ETHUSDT"],
      "cooldowns": {}
    }
  ]
}
```

---

## 2.4 Миграция V1 → V2 (детерминированно, без сюрпризов)

### Правила миграции

1. Если `version` отсутствует → трактуем как V1 (или “legacy”).
2. V1 содержит ключи `A`, `B` → конвертируем в массив `portfolios` с `id="A"`, `id="B"`.
3. При **увеличении** числа портфелей в конфиге:

   * добавляем новые `PortfolioState` с дефолтами (empty shortlist/active, пустые cooldowns).
4. При **уменьшении** числа портфелей:

   * грузим только те `id`, которые есть в конфиге (остальные игнорируем).
   * (если хочешь сохранить — можно оставить “orphaned” в snapshot, но это усложнение; для CP1 лучше выкинуть)

### Псевдокод миграции

```text
fn load_snapshot(config_ids: [PortfolioId]) -> SnapshotV2 {
  raw = read_json()

  if raw.version == 2:
     s2 = parse_v2(raw)
  else:
     s1 = parse_v1_legacy(raw)
     s2 = migrate_v1_to_v2(s1)

  // reconcile with config:
  map = by_id(s2.portfolios)
  out = []
  for id in stable_order(config_ids):
     if map.contains(id):
        out.push(map[id])
     else:
        out.push(default_state(id))
  return SnapshotV2 { version:2, portfolios: out }
}
```

**Обязательная деталь для детерминизма:** `stable_order(config_ids)` — это либо порядок в конфиге, либо `sort()` по id. Не HashMap iteration.

---

# 3) Deterministic no-overlap allocator (CP1.2)

## 3.1 Входы/выходы

**Вход:**

* для каждого портфеля `p`: `shortlist[p]` уже отсортирован по ranking tuple
* `active_limit` (сколько символов активировать в портфеле)
* `cooldown` информация (пер‑символ, пер‑портфель)

**Выход:**

* `active[p]` так, что `active[p_i] ∩ active[p_j] = ∅`.

---

## 3.2 Ranking и eligibility (как в твоей математике)

### Производные метрики кандидата

* `useful_winrate = profitable_trades / closed_trades`
  (если `closed_trades==0` → 0)
* `pm_raw = profitable_trades - losing_trades`
* `avg_pnl_pct = pnl_sum_pct / closed_trades`
  (если `closed_trades==0` → 0)

### Eligibility gate

Кандидат допускается только если:

* `age_minutes > 5`
* `closed_trades > 5`
* `useful_winrate >= 0.30`
* `avg_pnl_pct >= 0`

### Ranking tuple (descending)

1. `useful_winrate` (desc)
2. `pm_raw` (desc)
3. `avg_pnl_pct` (desc)
4. `closed_trades` (desc)
5. `symbol` (lexicographic ASC как tie-break)

> Важно: tie-break по `symbol` делает порядок **полным** → даже unstable sort даст детерминизм.

---

## 3.3 Алгоритм no-overlap (greedy, воспроизводимый)

### Инварианты детерминизма

* портфели обходятся в **стабильном порядке**: либо порядок в конфиге, либо сортировка по `id`.
* shortlist каждого портфеля отсортирован по tuple выше.
* символы уникальны внутри shortlist (один symbol — один candidate).

### Псевдокод

```text
fn allocate_active(portfolios_in_stable_order):
    taken = HashSet<Symbol>()

    for p in portfolios_in_stable_order:
        active = []
        for cand in p.shortlist:
            if active.len == p.active_limit:
                break

            if cand.eligibility.passed == false:
                continue

            if cand.cooldown_until_ts_ms != null and cand.cooldown_until_ts_ms > now_ms:
                continue

            if taken.contains(cand.symbol):
                continue

            active.push(cand.symbol)
            taken.insert(cand.symbol)

        p.active = active
```

**Сложность:** `O(P * K)`, где `P` — число портфелей, `K` — размер shortlist. Для CP1 это идеально: просто, быстро, тестируемо.

---

## 3.4 Почему этого достаточно для CP1

CP1 не требует “оптимального” распределения символов между портфелями, требует:

* независимый race per portfolio
* disjoint active sets
* воспроизводимость

Greedy покрывает 100% acceptance CP1.2.

---

# 4) Тесты (Given/When/Then), которые закрывают CP1.1–CP1.2

Ниже — минимальный, но “убойный” набор, который реально предотвращает регрессии и экономит дни.

---

## 4.1 CP1.1 — Dynamic portfolio count / API / snapshot

### Test: config changes without rebuild

**Given:** config portfolios = `[A,B,C]`
**When:** runtime start
**Then:** `/api/v1/portfolio/active.portfolios.len == 3`
**And:** ids == `["A","B","C"]` (стабильный порядок)

---

### Test: snapshot V1 migrates to V2

**Given:** snapshot json (version 1) с ключами `A,B`
**And:** config portfolios = `[A,B,C]`
**When:** load_snapshot()
**Then:** state содержит `A,B` восстановленными
**And:** `C` существует и равен `default_state("C")`
**And:** сериализация в файл пишет `version:2`

---

### Test: snapshot V2 reconciles with config

**Given:** snapshot V2 содержит `[A,B,C,D]`
**And:** config portfolios = `[A,B]`
**When:** load_snapshot()
**Then:** runtime имеет ровно `A,B`
**And:** никаких паник/ошибок

---

## 4.2 CP1.2 — eligibility/ranking/allocator

### Test: eligibility gate boundaries

**Given:** candidate with

* age_minutes = 5.0, closed_trades = 6, useful_winrate = 0.31, avg_pnl_pct = 0.0
  **When:** evaluate eligibility
  **Then:** `passed == false` (потому что age_minutes должно быть **> 5**, не >=)

**Given:** age_minutes = 5.01 (и остальные проходят)
**Then:** `passed == true`

---

### Test: ranking tuple correctness

**Given:** candidates (в одном портфеле)

1. X: useful_winrate=0.40 pm_raw=1 avg_pnl=0.01 closed=10
2. Y: useful_winrate=0.40 pm_raw=2 avg_pnl=0.00 closed=100
3. Z: useful_winrate=0.39 pm_raw=100 avg_pnl=1.00 closed=1000

**When:** sort by tuple
**Then:** порядок `Y, X, Z` (потому что winrate доминирует, потом pm_raw)

---

### Test: no-overlap allocation (simple)

**Given:** portfolios A и B, active_limit=1
A.shortlist = [BTC, ETH]
B.shortlist = [BTC, SOL]
(все eligible и не на cooldown)

**When:** allocate_active in stable order [A,B]
**Then:** A.active = [BTC]
**And:** B.active = [SOL]
**And:** intersection пустой

---

### Test: cooldown skips candidates

**Given:** portfolio A, active_limit=1
A.shortlist = [BTC(cooldown_until > now), ETH(no cooldown)]
**When:** allocate_active
**Then:** A.active = [ETH]

---

### Test: determinism across runs (важно!)

**Given:** фиксированный input shortlists, now_ms, config order
**When:** allocate_active вызывается 100 раз
**Then:** результат идентичен каждый раз (байт-в-байт)

*(Особенно полезно, если где-то случайно HashMap iteration протекла в порядок портфелей.)*

---

### Property test: “никогда нет overlap”

(если используешь proptest/quickcheck)

**Given:** случайно сгенерированные shortlists для P портфелей
**When:** allocate_active
**Then:** для всех i≠j, `active[i] ∩ active[j] == ∅`

---

# 5) Маленькие практичные “грабли”, которые стоит зафиксировать прямо сейчас

1. **Не итерироваться по HashMap** при формировании ответа API и аллокации — всегда сортируй ids.
2. Comparator для ranking должен задавать **полный порядок** (иначе unstable sort даст дрожание). Tie-break по `symbol` это решает.
3. В ответе API лучше отдавать shortlist уже отсортированным — UI тогда тупой и стабильный.
4. Деление на `closed_trades` — всегда safe‑guard (0 → 0), иначе краш/NaN поломает сортировку.

---

Если хочешь, следующим сообщением я могу:

* сделать **супер‑конкретный “PR plan” по файлам** (что именно меняется в runtime/API/tests/snapshot),
* или набросать **Rust‑структуры DTO + сериализацию** под этот schema (serde), включая миграцию snapshot через enum `Snapshot { V1, V2 }`.
