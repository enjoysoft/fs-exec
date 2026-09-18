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

    def record(self, request_visibility: float, result_visibility: float, *, round_trip: float | None = None) -> None:
        row = {"at": time.time(), "request": request_visibility, "result": result_visibility}
        if round_trip is not None:
            row["round_trip"] = round_trip
        append_jsonl(self.path, row)

    def stats(self, *, queue_margin: float = 2, safety_margin: float = 2) -> LatencyStats:
        rows = [row for row in tail_jsonl(self.path) if "request" in row and "result" in row]
        request = [float(row["request"]) for row in rows]
        result = [float(row["result"]) for row in rows]
        # New probes measure RTT with the client's monotonic clock. One-way
        # wall-clock measurements are diagnostic and require synchronized hosts.
        values = [float(row.get("round_trip", left + right)) for row, left, right in zip(rows, request, result)]
        newest = max((float(row.get("at", 0)) for row in rows), default=0)
        age = time.time() - newest if newest else None
        stale = age is None or age > self.stale_after or len(values) < 3
        p99 = percentile(values, 0.99)
        request_p99 = percentile(request, 0.99)
        result_p99 = percentile(result, 0.99)
        measured = all("round_trip" in row for row in rows)
        transport = (p99 or 0) if measured else max(p99 or 0, (request_p99 or 0) + (result_p99 or 0))
        recommended = self.fallback if stale else max(1.0, transport + queue_margin + safety_margin)
        return LatencyStats(
            len(values), percentile(values, 0.5), percentile(values, 0.95), p99,
            percentile(request, 0.5), percentile(request, 0.95), request_p99,
            percentile(result, 0.5), percentile(result, 0.95), result_p99,
            age, stale, recommended,
        )
