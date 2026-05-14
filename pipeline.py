"""
pipeline.py — Orchestrates ingestor → deduplicator → aggregator → anomaly detector.

Processing loop:
  for each message from ingestor:
    1. Skip MalformedRecord (already logged by ingestor)
    2. Deduplicate — drop if message_id seen within TTL
    3. Anomaly check on the raw record (per-record Z-score)
    4. Add to aggregation window buffer
    5. Periodically flush completed windows → post-aggregation spike detection
    6. Emit alerts and window summaries

This module has NO business logic of its own — it's pure orchestration.
Each concern is encapsulated in its own module.
"""

from __future__ import annotations
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Callable, List, Optional

from aggregator import WindowAggregator
from anomaly import AnomalyDetector
from config import CONFIG
from deduplicator import Deduplicator
from ingestor import get_stream
from models import AggWindow, Alert, MalformedRecord, TelemetryRecord

log = logging.getLogger(__name__)

# Type aliases for callback hooks (useful for tests and downstream sinks)
AlertCallback = Callable[[Alert], None]
WindowCallback = Callable[[AggWindow], None]


class TelemetryPipeline:
    """
    Main pipeline class. Compose components, run the event loop.

    Usage:
        pipeline = TelemetryPipeline()
        pipeline.run()
    """

    def __init__(
        self,
        on_alert: Optional[AlertCallback] = None,
        on_window: Optional[WindowCallback] = None,
        flush_interval_s: float = 10.0,
    ) -> None:
        self.deduplicator = Deduplicator()
        self.aggregator = WindowAggregator()
        self.detector = AnomalyDetector()
        self._on_alert = on_alert or self._default_alert_sink
        self._on_window = on_window or self._default_window_sink
        self._flush_interval = flush_interval_s
        self._last_flush = time.monotonic()
        self._last_stats_log = time.monotonic()
        self._stats_interval = 30.0    # log pipeline stats every 30s

        # Counters
        self.total_received = 0
        self.total_malformed = 0
        self.total_dupes = 0
        self.total_processed = 0
        self.total_alerts = 0

        # Graceful shutdown
        self._running = False
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the pipeline. Blocks until SIGINT/SIGTERM."""
        log.info(
            "Pipeline starting — simulate=%s flush_interval=%.1fs",
            CONFIG.simulate, self._flush_interval
        )
        self._running = True
        stream = get_stream()

        for message in stream:
            if not self._running:
                log.info("Pipeline shutting down cleanly")
                break

            self.total_received += 1
            self._process(message)

            # Periodic flush
            now = time.monotonic()
            if now - self._last_flush >= self._flush_interval:
                self._do_flush()
                self._last_flush = now

            # Periodic stats
            if now - self._last_stats_log >= self._stats_interval:
                self._log_stats()
                self._last_stats_log = now

        self._do_flush()   # final flush on exit
        self._log_stats()

    # ------------------------------------------------------------------
    # Per-message processing
    # ------------------------------------------------------------------

    def _process(self, message) -> None:
        # Stage 1: Malformed record gate
        if isinstance(message, MalformedRecord):
            self.total_malformed += 1
            log.debug("Dropped malformed: %s", message.reason)
            return

        record: TelemetryRecord = message

        # Stage 2: Deduplication
        if self.deduplicator.is_duplicate(record):
            self.total_dupes += 1
            return

        # Stage 3: Per-record anomaly detection (before aggregation)
        record_alerts: List[Alert] = self.detector.inspect_record(record)
        for alert in record_alerts:
            self.total_alerts += 1
            self._on_alert(alert)

        # Stage 4: Feed into aggregation window
        self.aggregator.add(record)
        self.total_processed += 1

    # ------------------------------------------------------------------
    # Periodic flush
    # ------------------------------------------------------------------

    def _do_flush(self) -> None:
        """Flush completed aggregation windows and run post-window anomaly checks."""
        windows = self.aggregator.flush()
        for window in windows:
            # Post-aggregation spike detection
            window_alerts = self.detector.inspect_window(window)
            for alert in window_alerts:
                self.total_alerts += 1
                self._on_alert(alert)

            self._on_window(window)

    # ------------------------------------------------------------------
    # Default sinks (override with real DB / message bus writers)
    # ------------------------------------------------------------------

    @staticmethod
    def _default_alert_sink(alert: Alert) -> None:
        print(f"ALERT  | {alert}")

    @staticmethod
    def _default_window_sink(window: AggWindow) -> None:
        print(
            f"WINDOW | {window.window_key} "
            f"n={window.sample_count} "
            f"mean={window.mean:.4f} "
            f"p95={window.p95:.4f} "
            f"[{window.window_start.strftime('%H:%M:%S')}–{window.window_end.strftime('%H:%M:%S')}]"
        )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def _log_stats(self) -> None:
        malformed_pct = 100 * self.total_malformed / max(self.total_received, 1)
        dupe_pct = 100 * self.total_dupes / max(self.total_received, 1)
        log.info(
            "Pipeline stats | received=%d processed=%d malformed=%d(%.1f%%) "
            "dupes=%d(%.1f%%) alerts=%d | dedup=%s | agg=%s",
            self.total_received,
            self.total_processed,
            self.total_malformed, malformed_pct,
            self.total_dupes, dupe_pct,
            self.total_alerts,
            self.deduplicator.stats,
            self.aggregator.stats,
        )

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------

    def _handle_shutdown(self, signum, frame) -> None:
        log.info("Received signal %s — initiating shutdown", signum)
        self._running = False
