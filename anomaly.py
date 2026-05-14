"""
anomaly.py — Real-time anomaly detection on individual records.

Two complementary strategies:
  1. Z-score against a rolling baseline (online, per-record)
  2. AggWindow-level spike detection (post-aggregation)

Design:
  - Rolling baseline per (sensor_id, metric_name) using a fixed-size deque
  - Welford's online algorithm for numerically stable incremental mean/variance
  - No numpy dependency — pure stdlib for portability
  - Separate detector class per strategy so they can be used independently
"""

from __future__ import annotations
import logging
import math
import threading
from collections import defaultdict, deque
from datetime import datetime
from typing import Deque, Dict, List, Optional, Tuple

from config import CONFIG, AnomalyConfig
from models import Alert, AnomalyType, AggWindow, TelemetryRecord

log = logging.getLogger(__name__)

_SensorKey = Tuple[str, str]   # (sensor_id, metric_name)


# ---------------------------------------------------------------------------
# Welford's online mean/variance — numerically stable, O(1) per update
# ---------------------------------------------------------------------------

class WelfordStats:
    """
    Incremental mean and sample variance using Welford's algorithm.
    Far more numerically stable than accumulating sum-of-squares.
    """
    __slots__ = ("n", "mean", "_M2")

    def __init__(self) -> None:
        self.n = 0
        self.mean = 0.0
        self._M2 = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self._M2 += delta * delta2

    @property
    def variance(self) -> float:
        return self._M2 / (self.n - 1) if self.n >= 2 else 0.0

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


# ---------------------------------------------------------------------------
# Per-record anomaly detector (Z-score on rolling window)
# ---------------------------------------------------------------------------

class RollingZScoreDetector:
    """
    Maintains a rolling baseline of the last N readings per (sensor, metric).
    Flags a new reading as anomalous if |Z| > threshold.

    Rolling window eviction: pops oldest value and subtracts its contribution
    from a separate Welford tracker. Simple fixed-deque approach here — for
    ultra-high cardinality use reservoir sampling or exponential smoothing.
    """

    def __init__(self, cfg: AnomalyConfig = CONFIG.anomaly) -> None:
        self._threshold = cfg.zscore_threshold
        self._window = cfg.rolling_window
        self._min_samples = cfg.min_baseline_samples

        # Per-key circular buffer of recent values
        self._history: Dict[_SensorKey, Deque[float]] = defaultdict(
            lambda: deque(maxlen=self._window)
        )
        self._lock = threading.Lock()
        self._total_alerts = 0

    def inspect(self, record: TelemetryRecord) -> Optional[Alert]:
        """
        Check a single record. Returns Alert if anomalous, else None.
        Thread-safe.
        """
        key = (record.sensor_id, record.metric_name)

        with self._lock:
            buf = self._history[key]

            if len(buf) < self._min_samples:
                # Baseline not yet warm — accumulate without flagging
                buf.append(record.value)
                return None

            # Compute baseline stats from current window (before inserting new value)
            n = len(buf)
            mu = sum(buf) / n
            variance = sum((v - mu) ** 2 for v in buf) / max(n - 1, 1)
            sigma = math.sqrt(variance)

            alert: Optional[Alert] = None

            if sigma > 1e-9:   # avoid division by near-zero std
                z = (record.value - mu) / sigma
                if abs(z) > self._threshold:
                    severity = "CRITICAL" if abs(z) > self._threshold * 1.5 else "WARNING"
                    alert = Alert(
                        sensor_id=record.sensor_id,
                        metric_name=record.metric_name,
                        anomaly_type=AnomalyType.ZSCORE,
                        detected_at=record.timestamp,
                        observed_value=record.value,
                        baseline_mean=round(mu, 4),
                        baseline_std=round(sigma, 4),
                        zscore=round(z, 3),
                        message=(
                            f"Value {record.value:.4f} deviates {z:+.2f}σ "
                            f"from baseline μ={mu:.4f} σ={sigma:.4f}"
                        ),
                        severity=severity,
                    )
                    self._total_alerts += 1
                    log.warning("%s", alert)

            buf.append(record.value)   # update baseline with new observation
            return alert

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "tracked_keys": len(self._history),
                "total_alerts": self._total_alerts,
            }


# ---------------------------------------------------------------------------
# Post-aggregation spike detector (operates on AggWindow objects)
# ---------------------------------------------------------------------------

class WindowSpikeDetector:
    """
    Compares a completed AggWindow against a longer-term mean of prior windows.
    Flags if the window's mean deviates > threshold σ from the historical norm.
    Useful for catching sustained anomalies that individual Z-scores might miss.
    """

    def __init__(
        self,
        cfg: AnomalyConfig = CONFIG.anomaly,
        history_depth: int = 30,   # how many prior windows to track
    ) -> None:
        self._threshold = cfg.zscore_threshold
        self._history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=history_depth)
        )
        self._lock = threading.Lock()

    def inspect(self, window: AggWindow) -> Optional[Alert]:
        key = window.window_key

        with self._lock:
            hist = self._history[key]

            if len(hist) < 5:
                hist.append(window.mean)
                return None

            n = len(hist)
            mu = sum(hist) / n
            sigma = math.sqrt(sum((v - mu) ** 2 for v in hist) / max(n - 1, 1))

            hist.append(window.mean)  # update history

            if sigma < 1e-9:
                return None

            z = (window.mean - mu) / sigma
            if abs(z) <= self._threshold:
                return None

            return Alert(
                sensor_id=window.sensor_id,
                metric_name=window.metric_name,
                anomaly_type=AnomalyType.SPIKE,
                detected_at=window.window_end,
                observed_value=window.mean,
                baseline_mean=round(mu, 4),
                baseline_std=round(sigma, 4),
                zscore=round(z, 3),
                message=(
                    f"Window mean {window.mean:.4f} deviates {z:+.2f}σ "
                    f"from {n}-window baseline μ={mu:.4f}"
                ),
                severity="CRITICAL" if abs(z) > self._threshold * 2 else "WARNING",
            )


# ---------------------------------------------------------------------------
# Composite detector — convenience wrapper used by pipeline.py
# ---------------------------------------------------------------------------

class AnomalyDetector:
    """Single entry point that runs both detectors and returns all alerts."""

    def __init__(self) -> None:
        self.zscore = RollingZScoreDetector()
        self.window_spike = WindowSpikeDetector()

    def inspect_record(self, record: TelemetryRecord) -> List[Alert]:
        alert = self.zscore.inspect(record)
        return [alert] if alert else []

    def inspect_window(self, window: AggWindow) -> List[Alert]:
        alert = self.window_spike.inspect(window)
        return [alert] if alert else []

    @property
    def stats(self) -> dict:
        return {
            "zscore_detector": self.zscore.stats,
            "window_spike_detector": {"tracked_keys": len(self.window_spike._history)},
        }
