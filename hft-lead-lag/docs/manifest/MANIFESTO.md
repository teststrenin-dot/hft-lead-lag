# HFT Lead-Lag Manifest (Current)

## Миссия
Построить HFT-oriented lead-lag платформу с измеримой задержкой, предсказуемым runtime и быстрым циклом fine-tune через Shadow Fleet.

---

## Принципы

1. **MVP-first, no overengineering**
   - Добавляем только то, что даёт измеримый результат в runtime.

2. **Bid/Ask truth, no fake mid assumptions**
   - Сигналы и сделки считаются от реальных сторон стакана.

3. **Hot path must stay cheap**
   - Shared samples, bounded queues, async persistence.

4. **Measured over guessed**
   - Решения подтверждаются метриками, логами и runtime evidence.

5. **State clarity over cleverness**
   - Явные lifecycle шаги: tick/fill/exit/entry/drain/flush.

6. **Docs must match code**
   - Документация синхронизируется с фактическими route/параметрами/модулями.

---

## Текущий фокус

- Shadow Fleet parameter exploration (1152 configs)
- Persistence and ranking
- Робастный fine-tune в условиях 2 vCPU / 3.8 GiB

---

## Source of truth

- `docs/README.md`
- `docs/shadow-fleet-deep-dive.md`
- `docs/review-2026-02-19-deep-dive.md`

---

*Manifest v2.0 — updated for Shadow Fleet phase*
