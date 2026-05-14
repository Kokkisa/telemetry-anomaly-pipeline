"""
tests.py — Unit tests for deduplication, aggregation, and anomaly detection.

Run with:  python -m pytest tests.py -v
Or:        python tests.py
"""

import math
import time
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from config import AggregationConfig, AnomalyConfig, DeduplicationConfig
from deduplicator import Deduplicator
from aggregator import WindowAggregator, _mean, _std, _percentile
from anomaly import RollingZScoreDetector, WindowSpikeDetector, AnomalyDetector
from ingestor import parse_message
from models import AggWindow, TelemetryRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_record(
    sensor_id="MFM_KDL_INLET_01",
    metric="pressure_bar",
    value=10.0,
    message_id=None,
    offset_seconds=0,
) -> TelemetryRecord:
    import uuid
    return TelemetryRecord(
        message_id=message_id or str(uuid.uuid4()),
        sensor_id=sensor_id,
        metric_name=metric,
        value=value,
        unit="bar",
        timestamp=datetime.utcnow() + timedelta(seconds=offset_seconds),
    )


# ---------------------------------------------------------------------------
# Parsing tests
# ---------------------------------------------------------------------------

class TestParsing(unittest.TestCase):

    def test_valid_record(self):
        import json, uuid
        raw = json.dumps({
            "message_id": str(uuid.uuid4()),
            "sensor_id": "s1",
            "metric_name": "pressure",
            "value": 12.5,
            "unit": "bar",
            "timestamp": datetime.utcnow().isoformat(),
        })
        rec = parse_message(raw)
        self.assertIsInstance(rec, TelemetryRecord)
        self.assertAlmostEqual(rec.value, 12.5)

    def test_invalid_json(self):
        from models import MalformedRecord
        rec = parse_message("not json {{{")
        self.assertIsInstance(rec, MalformedRecord)
        self.assertIn("JSON parse error", rec.reason)

    def test_missing_fields(self):
        from models import MalformedRecord
        rec = parse_message('{"sensor_id": "s1"}')
        self.assertIsInstance(rec, MalformedRecord)
        self.assertIn("Missing fields", rec.reason)

    def test_non_numeric_value(self):
        from models import MalformedRecord
        import json
        raw = json.dumps({
            "message_id": "x",
            "sensor_id": "s1",
            "metric_name": "p",
            "value": "oops",
            "unit": "bar",
            "timestamp": datetime.utcnow().isoformat(),
        })
        rec = parse_message(raw)
        self.assertIsInstance(rec, MalformedRecord)
        self.assertIn("Non-numeric", rec.reason)

    def test_nan_value_rejected(self):
        from models import MalformedRecord
        import json
        raw = '{"message_id":"a","sensor_id":"s","metric_name":"m","value":null,"unit":"u","timestamp":"bad"}'
        rec = parse_message(raw)
        self.assertIsInstance(rec, MalformedRecord)

    def test_unix_timestamp(self):
        import json, uuid
        raw = json.dumps({
            "message_id": str(uuid.uuid4()),
            "sensor_id": "s1",
            "metric_name": "p",
            "value": 5.0,
            "unit": "bar",
            "timestamp": time.time(),
        })
        rec = parse_message(raw)
        self.assertIsInstance(rec, TelemetryRecord)


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------

class TestDeduplicator(unittest.TestCase):

    def setUp(self):
        self.dedup = Deduplicator(DeduplicationConfig(ttl_seconds=5, max_cache_size=10))

    def test_first_occurrence_not_duplicate(self):
        rec = make_record()
        self.assertFalse(self.dedup.is_duplicate(rec))

    def test_same_id_is_duplicate(self):
        rec = make_record(message_id="abc-123")
        self.assertFalse(self.dedup.is_duplicate(rec))
        # Same object (same message_id) → duplicate
        self.assertTrue(self.dedup.is_duplicate(rec))

    def test_different_ids_not_duplicate(self):
        r1 = make_record(message_id="id-1")
        r2 = make_record(message_id="id-2")
        self.assertFalse(self.dedup.is_duplicate(r1))
        self.assertFalse(self.dedup.is_duplicate(r2))

    def test_expired_entry_not_duplicate(self):
        """After TTL expires, same id should be accepted again."""
        dedup = Deduplicator(DeduplicationConfig(ttl_seconds=1, max_cache_size=100))
        rec = make_record(message_id="ttl-test")
        self.assertFalse(dedup.is_duplicate(rec))
        time.sleep(1.2)
        # TTL expired — should not be considered duplicate
        self.assertFalse(dedup.is_duplicate(rec))

    def test_capacity_eviction(self):
        """Cache should not grow beyond max_cache_size."""
        for i in range(20):    # max is 10
            self.dedup.is_duplicate(make_record(message_id=f"id-{i}"))
        self.assertLessEqual(len(self.dedup._seen), 10)

    def test_stats_accuracy(self):
        r1 = make_record(message_id="s1")
        r2 = make_record(message_id="s1")   # duplicate
        r3 = make_record(message_id="s2")
        self.dedup.is_duplicate(r1)
        self.dedup.is_duplicate(r2)
        self.dedup.is_duplicate(r3)
        stats = self.dedup.stats
        self.assertEqual(stats["total_seen"], 3)
        self.assertEqual(stats["total_dupes"], 1)


# ---------------------------------------------------------------------------
# Aggregation tests
# ---------------------------------------------------------------------------

class TestAggregator(unittest.TestCase):

    def setUp(self):
        cfg = AggregationConfig(window_seconds=5, min_samples_required=2)
        self.agg = WindowAggregator(cfg)

    def test_no_flush_before_window_closes(self):
        self.agg.add(make_record(value=10.0))
        self.agg.add(make_record(value=20.0))
        # Flush immediately — window hasn't elapsed
        windows = self.agg.flush(now=datetime.utcnow())
        self.assertEqual(len(windows), 0)

    def test_flush_after_window_closes(self):
        now = datetime.utcnow()
        self.agg.add(make_record(value=10.0))
        self.agg.add(make_record(value=20.0))
        self.agg.add(make_record(value=30.0))
        # Advance time past window
        future = now + timedelta(seconds=6)
        windows = self.agg.flush(now=future)
        self.assertEqual(len(windows), 1)
        w = windows[0]
        self.assertAlmostEqual(w.mean, 20.0)
        self.assertAlmostEqual(w.minimum, 10.0)
        self.assertAlmostEqual(w.maximum, 30.0)

    def test_multiple_sensors_separate_windows(self):
        self.agg.add(make_record(sensor_id="MFM_KDL_INLET_01", value=11.0))
        self.agg.add(make_record(sensor_id="MFM_RJY_XFER_01", value=11.5))
        self.agg.add(make_record(sensor_id="MFM_KDL_INLET_01", value=10.8))
        self.agg.add(make_record(sensor_id="MFM_RJY_XFER_01", value=11.2))
        future = datetime.utcnow() + timedelta(seconds=10)
        windows = self.agg.flush(now=future)
        self.assertEqual(len(windows), 2)
        keys = {w.sensor_id for w in windows}
        self.assertIn("MFM_KDL_INLET_01", keys)
        self.assertIn("MFM_RJY_XFER_01", keys)

    def test_below_min_samples_skipped(self):
        cfg = AggregationConfig(window_seconds=5, min_samples_required=5)
        agg = WindowAggregator(cfg)
        agg.add(make_record(value=10.0))   # only 1 sample
        future = datetime.utcnow() + timedelta(seconds=10)
        windows = agg.flush(now=future)
        self.assertEqual(len(windows), 0)

    def test_stats_helper(self):
        self.assertEqual(_mean([2.0, 4.0, 6.0]), 4.0)
        self.assertAlmostEqual(_std([2.0, 4.0, 6.0], 4.0), 2.0)
        self.assertAlmostEqual(_percentile([1.0, 2.0, 3.0, 4.0, 5.0], 95), 4.8)


# ---------------------------------------------------------------------------
# Anomaly detection tests
# ---------------------------------------------------------------------------

class TestAnomalyDetector(unittest.TestCase):

    def _warm_detector(self, detector, sensor="MFM_KDL_INLET_01", metric="pressure_bar", base=10.0, n=15):
        """Feed n normal readings to warm up the baseline."""
        import random
        for _ in range(n):
            r = make_record(sensor_id=sensor, metric=metric,
                            value=base + random.gauss(0, 0.3))
            detector.inspect(r)

    def test_no_alert_before_baseline_warm(self):
        detector = RollingZScoreDetector(AnomalyConfig(min_baseline_samples=10))
        for _ in range(9):
            alert = detector.inspect(make_record(value=1000.0))  # huge value
            self.assertIsNone(alert)   # no alert — baseline not warm yet

    def test_normal_values_no_alert(self):
        detector = RollingZScoreDetector(AnomalyConfig(zscore_threshold=3.0, min_baseline_samples=10))
        self._warm_detector(detector, n=15)
        alert = detector.inspect(make_record(value=10.05))   # tiny deviation
        self.assertIsNone(alert)

    def test_spike_triggers_alert(self):
        detector = RollingZScoreDetector(AnomalyConfig(zscore_threshold=3.0, min_baseline_samples=10))
        self._warm_detector(detector, n=20)
        # 50x normal value — should trigger
        alert = detector.inspect(make_record(value=500.0))
        self.assertIsNotNone(alert)
        self.assertGreater(abs(alert.zscore), 3.0)

    def test_alert_includes_baseline_stats(self):
        detector = RollingZScoreDetector(AnomalyConfig(zscore_threshold=3.0, min_baseline_samples=10))
        self._warm_detector(detector, n=20)
        alert = detector.inspect(make_record(value=500.0))
        self.assertIsNotNone(alert.baseline_mean)
        self.assertIsNotNone(alert.baseline_std)
        self.assertIsNotNone(alert.zscore)

    def test_window_spike_detector(self):
        detector = WindowSpikeDetector(AnomalyConfig(zscore_threshold=3.0))
        import random
        random.seed(42)
        for i in range(10):
            w = AggWindow("s1", "p",
                          datetime.utcnow(), datetime.utcnow(),
                          sample_count=60, mean=10.0 + random.gauss(0, 0.5),
                          minimum=9.0, maximum=11.0,
                          std_dev=0.3, p95=10.5)
            detector.inspect(w)
        # Now spike to 100
        spike_window = AggWindow("s1", "p",
                                 datetime.utcnow(), datetime.utcnow(),
                                 sample_count=60, mean=100.0,
                                 minimum=90.0, maximum=110.0,
                                 std_dev=2.0, p95=105.0)
        alert = detector.inspect(spike_window)
        self.assertIsNotNone(alert)

    def test_composite_detector(self):
        import random
        random.seed(99)
        det = AnomalyDetector()
        for _ in range(20):
            det.inspect_record(make_record(value=10.0 + random.gauss(0, 0.5)))
        alerts = det.inspect_record(make_record(value=9999.0))
        self.assertTrue(len(alerts) > 0)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
