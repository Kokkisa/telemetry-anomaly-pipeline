"""
models.py — Typed data models for every stage of the pipeline.
Using dataclasses for zero-overhead structure without ORM overhead.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class AnomalyType(str, Enum):
    ZSCORE = "zscore"
    MISSING = "missing_data"
    MALFORMED = "malformed_record"
    SPIKE = "value_spike"


@dataclass
class TelemetryRecord:
    """
    Canonical record after parsing a raw Kafka message.
    All fields are validated before instantiation; invalid raw messages
    produce a MalformedRecord sentinel instead.
    """
    message_id: str          # unique per emission — used for deduplication
    sensor_id: str
    metric_name: str
    value: float
    unit: str
    timestamp: datetime
    tags: dict = field(default_factory=dict)   # e.g. {"plant": "KDL", "line": "A"}

    def age_seconds(self, now: datetime) -> float:
        return (now - self.timestamp).total_seconds()


@dataclass
class MalformedRecord:
    """Sentinel for records that failed parsing — never enters the pipeline."""
    raw_payload: str
    reason: str
    received_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AggWindow:
    """
    Output of the 1-minute tumbling aggregation window per (sensor_id, metric_name).
    """
    sensor_id: str
    metric_name: str
    window_start: datetime
    window_end: datetime
    sample_count: int
    mean: float
    minimum: float
    maximum: float
    std_dev: float
    p95: float                       # 95th percentile for SLA monitoring

    @property
    def window_key(self) -> str:
        return f"{self.sensor_id}::{self.metric_name}"


@dataclass
class Alert:
    """Emitted by the anomaly detector for any flagged condition."""
    sensor_id: str
    metric_name: str
    anomaly_type: AnomalyType
    detected_at: datetime
    observed_value: Optional[float]
    baseline_mean: Optional[float]
    baseline_std: Optional[float]
    zscore: Optional[float]
    message: str
    severity: str = "WARNING"        # INFO | WARNING | CRITICAL

    def __str__(self) -> str:
        z_str = f"{self.zscore:.2f}" if self.zscore is not None else "n/a"
        return (
            f"[{self.severity}] {self.anomaly_type.value.upper()} "
            f"sensor={self.sensor_id} metric={self.metric_name} "
            f"value={self.observed_value} z={z_str} "
            f"at {self.detected_at.isoformat()}"
        )
