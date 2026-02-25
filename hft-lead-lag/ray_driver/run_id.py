"""Run ID utilities."""

from __future__ import annotations

import time
import uuid


def generate_run_id(phase: str) -> str:
    """Generate a collision-resistant run id with phase prefix."""
    normalized = phase.strip().lower()
    if not normalized:
        raise ValueError("phase must be non-empty")
    return f"{normalized}-{time.time_ns()}-{uuid.uuid4().hex[:8]}"
