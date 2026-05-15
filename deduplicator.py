"""
deduplicator.py — Sliding-window deduplication using a TTL-bounded seen-set.

Design decisions :
  - TTL eviction instead of unbounded set: memory-safe under high cardinality
  - Insertion-order deque for O(1) eviction of expired entries
  - O(1) average-case lookup via dict (hash map under the hood)
  - Thread-safe via lock — safe for multi-threaded pipeline consumers

Alternative mentioned in comments: Bloom filter for ultra-high-cardinality
scenarios where a small false-positive rate is acceptable.
"""

from __future__ import annotations
import logging
import threading
from collections import deque
from datetime import datetime
from typing import Deque, Dict, Tuple

from config import CONFIG, DeduplicationConfig
from models import TelemetryRecord

log = logging.getLogger(__name__)


class Deduplicator:
    """
    Rejects records whose message_id has been seen within the last TTL seconds.

    Data structure:
        seen: dict[message_id -> inserted_at]   — O(1) lookup
        queue: deque[(inserted_at, message_id)] — ordered for O(1) eviction

    Memory bound: at most max_cache_size entries; oldest evicted when full.
    """

    def __init__(self, cfg: DeduplicationConfig = CONFIG.dedup) -> None:
        self._ttl = cfg.ttl_seconds
        self._max_size = cfg.max_cache_size
        self._seen: Dict[str, datetime] = {}
        self._queue: Deque[Tuple[datetime, str]] = deque()
        self._lock = threading.Lock()
        self._total_seen = 0
        self._total_dupes = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_duplicate(self, record: TelemetryRecord) -> bool:
        """
        Returns True if this record's message_id was already processed
        within the TTL window. Also evicts expired entries as a side-effect.
        """
        with self._lock:
            now = datetime.utcnow()
            self._evict_expired(now)

            self._total_seen += 1

            if record.message_id in self._seen:
                self._total_dupes += 1
                log.debug(
                    "Duplicate detected: message_id=%s sensor=%s",
                    record.message_id, record.sensor_id
                )
                return True

            self._insert(record.message_id, now)
            return False

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "cache_size": len(self._seen),
                "total_seen": self._total_seen,
                "total_dupes": self._total_dupes,
                "dupe_rate_pct": round(
                    100 * self._total_dupes / max(self._total_seen, 1), 2
                ),
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_expired(self, now: datetime) -> None:
        """Remove entries older than TTL from both structures. O(k) where k = expired count."""
        while self._queue:
            inserted_at, msg_id = self._queue[0]
            age = (now - inserted_at).total_seconds()
            if age <= self._ttl:
                break
            self._queue.popleft()
            self._seen.pop(msg_id, None)

    def _insert(self, message_id: str, now: datetime) -> None:
        """Insert a new message_id. Evicts oldest entry if at capacity."""
        if len(self._seen) >= self._max_size:
            # Capacity eviction: drop oldest (front of queue)
            _, oldest_id = self._queue.popleft()
            self._seen.pop(oldest_id, None)
            log.warning("Dedup cache at capacity — evicting oldest entry")

        self._seen[message_id] = now
        self._queue.append((now, message_id))


# ---------------------------------------------------------------------------
# Bloom Filter alternative
# ---------------------------------------------------------------------------
# For cardinalities > 10M msg/sec where even the dict becomes large:
#
#   from pybloom_live import ScalableBloomFilter
#   bloom = ScalableBloomFilter(initial_capacity=100_000, error_rate=0.001)
#
#   def is_duplicate(message_id):
#       if message_id in bloom:
#           return True
#       bloom.add(message_id)
#       return False
#
# Trade-off: ~0.1% false positives (legitimate records dropped) in exchange
# for O(1) fixed memory regardless of cardinality. Acceptable in telemetry
# where occasional missed records are less costly than OOM crashes.
