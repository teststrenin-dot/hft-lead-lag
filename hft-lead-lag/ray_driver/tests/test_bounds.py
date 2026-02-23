"""Tests for parameter bounds and scout config generation."""

from ray_driver.bounds import AXES, AxisBounds


def test_init_values_within_bounds():
    for name, ax in AXES.items():
        vals = ax.init_values()
        assert len(vals) > 0, f"{name} has no init values"
        assert all(ax.hard_min <= v <= ax.hard_max for v in vals), \
            f"{name} init values outside hard bounds"


def test_expand_respects_hard_bounds():
    ax = AxisBounds(0.0, 10.0, 2.0, 8.0, 1.0, 2.0)
    expanded = ax.expand_around(1.0, n_steps=2)
    assert all(0.0 <= v <= 10.0 for v in expanded)


def test_expand_at_hard_min_clips():
    ax = AxisBounds(5.0, 100.0, 10.0, 90.0, 10.0, 10.0)
    expanded = ax.expand_around(5.0, n_steps=1)
    assert min(expanded) >= 5.0


def test_expand_at_hard_max_clips():
    ax = AxisBounds(5.0, 100.0, 10.0, 90.0, 10.0, 10.0)
    expanded = ax.expand_around(100.0, n_steps=1)
    assert max(expanded) <= 100.0


def test_init_values_non_empty():
    for name, ax in AXES.items():
        assert len(ax.init_values()) >= 1, f"{name} must have at least 1 init value"
