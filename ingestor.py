"""
ingestor.py — Kafka consumer with a built-in simulator for local dev/testing.

Production path  : reads from Kafka, yields TelemetryRecord | MalformedRecord
Simulation path  : generates synthetic telemetry with deliberate noise and bad records
"""

from __future__ import annotations
import json
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Generator, Union

from config import CONFIG, KafkaConfig
from models import MalformedRecord, TelemetryRecord

log = logging.getLogger(__name__)

RawMessage = Union[TelemetryRecord, MalformedRecord]

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = {"message_id", "sensor_id", "metric_name", "value", "unit", "timestamp"}


def parse_message(raw: str) -> RawMessage:
    """
    Parse a raw JSON string into a TelemetryRecord.
    Returns MalformedRecord on any failure — never raises.

    Handles:
      - Invalid JSON
      - Missing required fields
      - Wrong value types (value must be numeric)
      - Unparseable timestamps
      - Non-finite floats (NaN, Inf)
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return MalformedRecord(raw_payload=raw[:500], reason=f"JSON parse error: {exc}")

    if not isinstance(payload, dict):
        return MalformedRecord(raw_payload=raw[:500], reason="Payload is not a JSON object")

    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        return MalformedRecord(raw_payload=raw[:500], reason=f"Missing fields: {missing}")

    # Validate value
    try:
        value = float(payload["value"])
    except (TypeError, ValueError):
        return MalformedRecord(raw_payload=raw[:500], reason=f"Non-numeric value: {payload['value']!r}")

    import math
    if not math.isfinite(value):
        return MalformedRecord(raw_payload=raw[:500], reason=f"Non-finite value: {value}")

    # Validate timestamp — accept ISO 8601 string or unix epoch float
    ts_raw = payload["timestamp"]
    try:
        if isinstance(ts_raw, (int, float)):
            timestamp = datetime.utcfromtimestamp(ts_raw)
        else:
            timestamp = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
    except (ValueError, OSError, OverflowError):
        return MalformedRecord(raw_payload=raw[:500], reason=f"Unparseable timestamp: {ts_raw!r}")

    return TelemetryRecord(
        message_id=str(payload["message_id"]),
        sensor_id=str(payload["sensor_id"]),
        metric_name=str(payload["metric_name"]),
        value=value,
        unit=str(payload["unit"]),
        timestamp=timestamp,
        tags=payload.get("tags", {}),
    )


# ---------------------------------------------------------------------------
# Kafka consumer (production)
# ---------------------------------------------------------------------------

def kafka_consumer(cfg: KafkaConfig) -> Generator[RawMessage, None, None]:
    """
    Wraps confluent_kafka.Consumer.  Commits offsets only after successful
    downstream processing (caller's responsibility to call commit()).
    """
    try:
        from confluent_kafka import Consumer, KafkaException
    except ImportError:
        raise RuntimeError(
            "confluent_kafka not installed. Run: pip install confluent-kafka"
        )

    consumer = Consumer({
        "bootstrap.servers": cfg.bootstrap_servers,
        "group.id": cfg.group_id,
        "auto.offset.reset": cfg.auto_offset_reset,
        "enable.auto.commit": cfg.enable_auto_commit,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": cfg.session_timeout_ms,
    })
    consumer.subscribe([cfg.topic])
    log.info("Kafka consumer subscribed to topic=%s group=%s", cfg.topic, cfg.group_id)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka error: %s", msg.error())
                continue

            raw = msg.value().decode("utf-8", errors="replace")
            record = parse_message(raw)

            if isinstance(record, MalformedRecord):
                log.warning("Malformed record at offset %s: %s", msg.offset(), record.reason)
            else:
                log.debug("Parsed %s from %s", record.message_id, record.sensor_id)

            yield record

            # Manual commit — ensures at-least-once with idempotent dedup downstream
            consumer.commit(message=msg, asynchronous=True)

    except KeyboardInterrupt:
        log.info("Consumer interrupted")
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# Simulator (local dev / assessment demo)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Mass Flow Meter sensor definitions
#
# Sensors represent Coriolis mass flow meters installed at key points on an
# LPG pipeline network — the same architecture used at HPCL plants.
# Each entry: (sensor_id, metric_name, unit, normal_min, normal_max)
#
# Pipeline segments modelled:
#   KDL = Kondapalli plant (Andhra Pradesh)
#   RJY = Rajahmundry terminal (Andhra Pradesh)
#   MNG = Mangalore terminal (Karnataka)
# ---------------------------------------------------------------------------

MFM_SENSORS = [
    # Kondapalli — inlet header and two filling lines
    ("MFM_KDL_INLET_01",  "mass_flow_rate_kgh",  "kg/h",  4200.0, 4800.0),
    ("MFM_KDL_INLET_01",  "temperature_c",        "°C",    32.0,   38.0),
    ("MFM_KDL_INLET_01",  "pressure_bar",         "bar",   10.5,   11.5),
    ("MFM_KDL_FILL_01",   "mass_flow_rate_kgh",   "kg/h",  2050.0, 2200.0),
    ("MFM_KDL_FILL_01",   "pressure_bar",         "bar",   9.8,    10.4),

    # Rajahmundry — main transfer line
    ("MFM_RJY_XFER_01",   "mass_flow_rate_kgh",   "kg/h",  5100.0, 5600.0),
    ("MFM_RJY_XFER_01",   "temperature_c",        "°C",    29.0,   35.0),
    ("MFM_RJY_XFER_01",   "pressure_bar",         "bar",   11.0,   12.0),
    ("MFM_RJY_XFER_01",   "density_kgm3",         "kg/m³", 508.0,  522.0),

    # Mangalore — outlet header and compressor discharge
    ("MFM_MNG_OUTLET_01", "mass_flow_rate_kgh",   "kg/h",  3800.0, 4100.0),
    ("MFM_MNG_OUTLET_01", "pressure_bar",         "bar",   8.5,    9.5),
    ("MFM_MNG_COMP_01",   "pressure_bar",         "bar",   13.0,   14.5),
    ("MFM_MNG_COMP_01",   "temperature_c",        "°C",    45.0,   52.0),
]

# Lookup: sensor_id+metric -> (unit, normal_min, normal_max)
_MFM_LOOKUP: dict = {
    (sid, metric): (unit, lo, hi)
    for sid, metric, unit, lo, hi in MFM_SENSORS
}


def _make_mfm_record(sensor_id: str, metric: str, unit: str,
                     lo: float, hi: float) -> str:
    """
    Generate one realistic mass flow meter reading.
    Normal values follow a Gaussian centred in (lo+hi)/2.
    Anomaly injected 3% of the time — value jumps 3-5x outside normal range,
    simulating a pressure surge, cavitation event, or flow blockage.
    """
    base = (lo + hi) / 2.0
    sigma = (hi - lo) / 6.0          # ±3σ stays inside [lo, hi] normally
    value = random.gauss(base, sigma)

    if random.random() < 0.03:       # 3% anomaly injection
        spike_dir = random.choice([-1, 1])
        value = base + spike_dir * random.uniform(4.0, 6.0) * sigma

    plant = sensor_id.split("_")[1]  # KDL / RJY / MNG

    return json.dumps({
        "message_id": str(uuid.uuid4()),
        "sensor_id": sensor_id,
        "metric_name": metric,
        "value": round(value, 3),
        "unit": unit,
        "timestamp": datetime.utcnow().isoformat(),
        "tags": {
            "plant": plant,
            "equipment_type": "coriolis_mass_flow_meter",
            "network": "lpg_distribution",
        },
    })


_BAD_PAYLOADS = [
    '{"sensor_id": "MFM_KDL_INLET_01", "value": "COMM_ERROR"}',
    'this is not json at all }{',
    '{"message_id": "x", "sensor_id": "s", "metric_name": "m", "value": null, "unit": "u", "timestamp": "bad-ts"}',
    '{}',
    '{"message_id":"dup","sensor_id":"MFM_RJY_XFER_01","metric_name":"pressure_bar","value":999999,"unit":"bar","timestamp":"not-a-date"}',
]


def simulated_stream(rate_hz: float = 5.0) -> Generator[RawMessage, None, None]:
    """
    Generates synthetic LPG pipeline telemetry at ~rate_hz records/second.

    Injects deliberately:
      - 4% malformed payloads  (SCADA comm errors, truncated packets)
      - 5% duplicate message_ids (Kafka at-least-once redelivery)
      - 3% anomalous values   (pressure surges, flow blockages, cavitation)
    """
    sleep_s = 1.0 / rate_hz
    last_ids: list[str] = []
    msg_count = 0

    log.info(
        "MFM simulator started — %d sensor/metric pairs at %.1f Hz",
        len(MFM_SENSORS), rate_hz
    )

    while True:
        # Inject malformed record (simulates SCADA comms error / truncated packet)
        if random.random() < 0.04:
            yield parse_message(random.choice(_BAD_PAYLOADS))
            time.sleep(sleep_s)
            continue

        sensor_id, metric, unit, lo, hi = random.choice(MFM_SENSORS)

        # Inject duplicate (Kafka redelivery simulation)
        if last_ids and random.random() < 0.05:
            raw_dict = json.loads(_make_mfm_record(sensor_id, metric, unit, lo, hi))
            raw_dict["message_id"] = random.choice(last_ids)
            raw = json.dumps(raw_dict)
        else:
            raw = _make_mfm_record(sensor_id, metric, unit, lo, hi)

        record = parse_message(raw)
        if isinstance(record, TelemetryRecord):
            last_ids = (last_ids + [record.message_id])[-50:]

        msg_count += 1
        if msg_count % 100 == 0:
            log.info("MFM simulator: %d messages emitted", msg_count)

        yield record
        time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def get_stream() -> Generator[RawMessage, None, None]:
    """Entry point: returns simulator or Kafka consumer based on CONFIG."""
    if CONFIG.simulate:
        return simulated_stream(rate_hz=10.0)
    return kafka_consumer(CONFIG.kafka)
