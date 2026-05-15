"""
aggregator.py — 1-minute tumbling window aggregation per (sensor_id, metric_name).

Design:
  - One collections.deque per (sensor, metric) key — O(1) append/evict
  - Windows are TUMBLING (non-overlapping, flush-and-reset)
  - statistics computed without numpy for portability; numpy path shown in comments
  - Thread-safe via per-key locks (dict of locks) for high-cardinality scenarios
"""

from __future__ import annotations
import logging
import math
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional, Tuple

from config import CONFIG, AggregationConfig
from models import AggWindow, TelemetryRecord

log = logging.getLogger(__name__)

_SensorKey = Tuple[str, str]   # (sensor_id, metric_name)


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _std(values: List[float], mu: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mu) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _percentile(sorted_vals: List[float], pct: float) -> float:
    """Linear interpolation percentile — mirrors numpy.percentile default."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = pct / 100 * (n - 1)
    lo, hi = int(idx), min(int(idx) + 1, n - 1)
    frac = idx - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


class WindowAggregator:
    """
    Maintains a rolling buffer of TelemetryRecords per (sensor_id, metric_name).
    Call .add() for each record; call .flush() to extract completed windows.

    Buffer structure per key:
        deque of (timestamp, value) — bounded to window_seconds
    """

    def __init__(self, cfg: AggregationConfig = CONFIG.aggregation) -> None:
        self._window_s = cfg.window_seconds
        self._min_samples = cfg.min_samples_required

        # (sensor_id, metric_name) -> deque[(timestamp, value)]
        self._buffers: Dict[_SensorKey, Deque[Tuple[datetime, float]]] = defaultdict(deque)
        self._window_starts: Dict[_SensorKey, datetime] = {}
        self._lock = threading.Lock()
        self._total_windows = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, record: TelemetryRecord) -> None:
        """Append a record to its sensor/metric buffer."""
        key = (record.sensor_id, record.metric_name)
        with self._lock:
            if key not in self._window_starts:
                self._window_starts[key] = record.timestamp
            self._buffers[key].append((record.timestamp, record.value))

    def flush(self, now: Optional[datetime] = None) -> List[AggWindow]:
        """
        Called periodically (e.g. every 10s). Returns completed AggWindow
        objects for any (sensor, metric) whose window has expired.
        Expired entries are pruned from the buffer.
        """
        now = now or datetime.utcnow()
        completed: List[AggWindow] = []

        with self._lock:
            for key, buf in list(self._buffers.items()):
                window_start = self._window_starts.get(key)
                if window_start is None:
                    continue

                # Check if window has elapsed
                elapsed = (now - window_start).total_seconds()
                if elapsed < self._window_s:
                    continue                # window still open

                # Collect values within the closed window before pruning
                window_end_ts = window_start + timedelta(seconds=self._window_s)
                values = [v for ts, v in buf if ts <= window_end_ts]

                # Prune consumed entries from buffer
                while buf and buf[0][0] <= window_end_ts:
                    buf.popleft()

                if len(values) < self._min_samples:
                    log.warning(
                        "Window for %s::%s has only %d samples (min %d) — skipping",
                        key[0], key[1], len(values), self._min_samples
                    )
                    # Reset window start even if skipped
                    self._window_starts[key] = now
                    continue

                mu = _mean(values)
                sd = _std(values, mu)
                sorted_v = sorted(values)

                window = AggWindow(
                    sensor_id=key[0],
                    metric_name=key[1],
                    window_start=window_start,
                    window_end=now,
                    sample_count=len(values),
                    mean=round(mu, 4),
                    minimum=round(sorted_v[0], 4),
                    maximum=round(sorted_v[-1], 4),
                    std_dev=round(sd, 4),
                    p95=round(_percentile(sorted_v, 95), 4),
                )
                completed.append(window)
                self._total_windows += 1
                self._window_starts[key] = window_end_ts  # start next window at boundary

                log.debug(
                    "Window closed: %s n=%d mean=%.4f p95=%.4f",
                    window.window_key, window.sample_count, window.mean, window.p95
                )

        return completed

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "active_keys": len(self._buffers),
                "total_windows_flushed": self._total_windows,
                "buffer_sizes": {
                    f"{k[0]}::{k[1]}": len(v)
                    for k, v in self._buffers.items()
                },
            }

# ---------------------------------------------------------------------------
# numpy / pandas path
# ---------------------------------------------------------------------------
# For very large windows or vectorised operations, replace the pure-Python
# stats with:
#
#   import numpy as np
#   arr = np.array(values)
#   mu, sd = arr.mean(), arr.std(ddof=1)
#   p95 = np.percentile(arr, 95)
#
# For streaming in Kafka Streams or Flink, this entire class maps to a
# TimeWindow + Reducer operator, which handles state across restarts.
