"""Hard parameter bounds and scout/expand ranges for all tunable axes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class AxisBounds:
    hard_min: float
    hard_max: float
    init_min: float
    init_max: float
    step: float
    expand_step: float

    def init_values(self) -> list[float]:
        """Generate values within init range at step intervals."""
        vals = []
        v = self.init_min
        while v <= self.init_max + self.step * 1e-9:
            vals.append(round(v, 6))
            v += self.step
        return vals or [self.init_min]

    def expand_around(self, center: float, n_steps: int = 1) -> list[float]:
        """Generate neighbor values around center, clipped to hard bounds."""
        vals = set()
        for i in range(-n_steps, n_steps + 1):
            v = round(center + i * self.expand_step, 6)
            if self.hard_min <= v <= self.hard_max:
                vals.add(v)
        return sorted(vals)


AXES: dict[str, AxisBounds] = {
    "spike_threshold_bps": AxisBounds(5.0, 200.0, 20.0, 100.0, 10.0, 10.0),
    "target_ratio": AxisBounds(0.1, 0.95, 0.2, 0.8, 0.1, 0.1),
    "stop_loss_bps": AxisBounds(3.0, 100.0, 5.0, 50.0, 5.0, 5.0),
    "max_hold_ms": AxisBounds(1000, 120000, 3000, 60000, 5000, 5000),
    "max_spread_bps": AxisBounds(1.0, 20.0, 2.0, 8.0, 1.0, 1.0),
    "trailing_decay_ratio": AxisBounds(0.1, 0.95, 0.2, 0.8, 0.1, 0.1),
    "baseline_window_ms": AxisBounds(5000, 120000, 10000, 60000, 10000, 10000),
}

FIXED_DEFAULTS = {
    "fill_delay_ms": 6,
    "cooldown_ms": 3000,
    "warmup_ms": 30000,
    "quote_freshness_ms": 1000,
    "taker_fee": 0.0005,
    "min_baseline_samples": 20,
}
