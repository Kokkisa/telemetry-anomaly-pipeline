"""
config.py — Central configuration for the telemetry pipeline.
All tuneable constants live here; nothing is hardcoded in business logic.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "telemetry.raw"
    group_id: str = "telemetry-pipeline-v1"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False          # manual commit for exactly-once semantics
    max_poll_records: int = 500
    session_timeout_ms: int = 30_000


@dataclass(frozen=True)
class DeduplicationConfig:
    # Sliding-window dedup: ignore duplicate message_ids within this window
    ttl_seconds: int = 300                   # 5-minute dedup window
    max_cache_size: int = 100_000            # evict oldest when exceeded


@dataclass(frozen=True)
class AggregationConfig:
    window_seconds: int = 60                 # 1-minute tumbling window
    min_samples_required: int = 3            # skip window if too few readings


@dataclass(frozen=True)
class AnomalyConfig:
    zscore_threshold: float = 3.0           # flag if |Z| > 3σ
    rolling_window: int = 20               # samples for rolling baseline
    min_baseline_samples: int = 10          # don't flag until baseline is warm


@dataclass(frozen=True)
class PipelineConfig:
    kafka: KafkaConfig = KafkaConfig()
    dedup: DeduplicationConfig = DeduplicationConfig()
    aggregation: AggregationConfig = AggregationConfig()
    anomaly: AnomalyConfig = AnomalyConfig()
    log_level: str = "INFO"
    simulate: bool = True                   # True = use built-in data simulator | False = for Real Kafka Data


# Singleton
CONFIG = PipelineConfig()
