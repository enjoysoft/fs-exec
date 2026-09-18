from __future__ import annotations

import math
import time
from dataclasses import dataclass
from pathlib import Path

from .util import append_jsonl, tail_jsonl


@dataclass(frozen=True)
class LatencyStats:
    samples: int
    p50: float | None
    p95: float | None
    p99: float | None
    request_p50: float | None
    request_p95: float | None
    request_p99: float | None
    result_p50: float | None
    result_p95: float | None
    result_p99: float | None
    age: float | None
    stale: bool
    recommended_overhead: float


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


class LatencyStore:
    def __init__(self, path: Path, *, stale_after: float = 900, fallback: float = 30) -> None:
        self.path = path
        self.stale_after = stale_after
        self.fallback = fallback

    def record(self, request_visibility: float, result_visibility: float) -> None:
        append_jsonl(self.path, {"at": time.time(), "request": request_visibility, "result": result_visibility})

    def stats(self, *, queue_margin: float = 2, safety_margin: float = 2) -> LatencyStats:
        rows = list(tail_jsonl(self.path))
        request = [float(row["request"]) for row in rows if "request" in row and "result" in row]
        result = [float(row["result"]) for row in rows if "request" in row and "result" in row]
        values = [left + right for left, right in zip(request, result)]
        newest = max((float(row.get("at", 0)) for row in rows), default=0)
        age = time.time() - newest if newest else None
        stale = age is None or age > self.stale_after or len(values) < 3
        p99 = percentile(values, 0.99)
        request_p99 = percentile(request, 0.99)
        result_p99 = percentile(result, 0.99)
        recommended = self.fallback if stale else max(1.0, (request_p99 or 0) + (result_p99 or 0) + queue_margin + safety_margin)
        return LatencyStats(
            len(values), percentile(values, 0.5), percentile(values, 0.95), p99,
            percentile(request, 0.5), percentile(request, 0.95), request_p99,
            percentile(result, 0.5), percentile(result, 0.95), result_p99,
            age, stale, recommended,
        )
