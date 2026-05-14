"""
main.py — Entry point. Configure logging, wire up pipeline, run.

Run locally:
    python main.py                      # simulator mode (CONFIG.simulate=True)
    SIMULATE=false python main.py       # Kafka mode (needs local broker)
"""

import logging
import os
import sys

from config import CONFIG
from pipeline import TelemetryPipeline


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)-24s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Quieten noisy library loggers
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


def main() -> None:
    configure_logging(CONFIG.log_level)
    log = logging.getLogger(__name__)

    log.info("=" * 60)
    log.info("Telemetry Pipeline  |  simulate=%s", CONFIG.simulate)
    log.info("Dedup TTL: %ds  |  Agg window: %ds  |  Z-threshold: %.1fσ",
             CONFIG.dedup.ttl_seconds,
             CONFIG.aggregation.window_seconds,
             CONFIG.anomaly.zscore_threshold)
    log.info("=" * 60)

    pipeline = TelemetryPipeline(flush_interval_s=10.0)
    pipeline.run()


if __name__ == "__main__":
    main()
