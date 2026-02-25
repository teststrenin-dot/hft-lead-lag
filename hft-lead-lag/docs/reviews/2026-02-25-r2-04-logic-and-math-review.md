# Review: Логика и Математика (Round 2)

## Findings

### P2

Подтверждённых новых P2+ проблем не выявлено.

## Проверка предыдущих фиксов

1. Baseline gap считает ask/bid через отдельные валидные счётчики.
   - Path:
     - `src/domain/screener/shadow_trader.rs:432`

2. Policy score приводит проценты к ratio перед взвешиванием.
   - Path:
     - `src/domain/screener/shadow_fleet.rs:243`

## Verdict

Фиксы предыдущего раунда по математике выглядят корректными, регрессий в ключевых формулах не обнаружено.
