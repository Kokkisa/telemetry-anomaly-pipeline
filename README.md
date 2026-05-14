# Telemetry Anomaly Pipeline

**Real-time streaming pipeline for LPG pipeline telemetry — deduplication, aggregation, and anomaly detection on Coriolis mass flow meter data.**

Built in Python using only stdlib — no Spark, no Flink, no heavy frameworks — to demonstrate production-grade streaming architecture at the component level.

---

## Problem Statement

Industrial LPG pipelines generate continuous telemetry from Coriolis mass flow meters installed at every segment: inlet headers, filling lines, transfer lines, and compressor discharge points. Each sensor streams **mass flow rate, temperature, pressure, and fluid density** every few seconds — 24×7.

The raw stream has three problems that make it unusable without a pipeline:

| Problem | Real-world cause | Pipeline solution |
|---|---|---|
| **Duplicate records** | Kafka at-least-once delivery; SCADA retry on comm timeout | TTL-bounded deduplication |
| **Malformed records** | Truncated packets, COMM_ERROR payloads, sensor offline | Parse-time validation with `MalformedRecord` sentinel |
| **Undetected anomalies** | Pressure surges, flow blockages, cavitation events | Rolling Z-score + post-window spike detection |

---

## Architecture

```
Mass Flow Meter (Coriolis)
         │
         ▼
   [ Kafka Topic ]           ← at-least-once delivery
         │
         ▼
   [ Ingestor ]              ← parse JSON, validate all fields, reject malformed
         │
         ▼
   [ Deduplicator ]          ← TTL-bounded seen-set, O(1) lookup, memory-capped
         │
         ▼
   [ Anomaly Detector ]      ← per-record Z-score vs rolling 20-sample baseline
         │
         ▼
   [ Window Aggregator ]     ← 1-min tumbling windows: mean / min / max / p95
         │
         ▼
   [ Window Spike Detector ] ← compares window mean vs 30-window historical norm
         │
         ▼
   Alerts  ──────────────────── downstream: PagerDuty / InfluxDB / TimescaleDB
   Windows ─────────────────────────────────────────────── dashboard / BI layer
```

---

## Sensor Configuration

Sensors model three plants on the HPCL LPG distribution network:

| Sensor ID | Location | Metrics |
|---|---|---|
| `MFM_KDL_INLET_01` | Kondapalli — inlet header | mass flow rate, temperature, pressure |
| `MFM_KDL_FILL_01` | Kondapalli — filling line | mass flow rate, pressure |
| `MFM_RJY_XFER_01` | Rajahmundry — transfer line | mass flow rate, temperature, pressure, density |
| `MFM_MNG_OUTLET_01` | Mangalore — outlet header | mass flow rate, pressure |
| `MFM_MNG_COMP_01` | Mangalore — compressor discharge | pressure, temperature |

Normal operating ranges follow real LPG pipeline parameters:
- **Mass flow rate**: 2,000 – 5,600 kg/h depending on line
- **Pressure**: 8.5 – 14.5 bar
- **Temperature**: 29 – 52 °C
- **Density**: 508 – 522 kg/m³ (Rajahmundry LPG grade)

---

## File Structure

```
telemetry_pipeline/
├── config.py          # All constants — dedup TTL, window size, Z-threshold
├── models.py          # TelemetryRecord · MalformedRecord · AggWindow · Alert
├── ingestor.py        # Kafka consumer + MFM simulator with injected faults
├── deduplicator.py    # TTL-bounded seen-set (dict + deque, O(1) ops)
├── aggregator.py      # Tumbling window aggregation per (sensor, metric)
├── anomaly.py         # Rolling Z-score + post-window spike detector
├── pipeline.py        # Orchestration — wires all stages, graceful shutdown
├── main.py            # Entry point
├── tests.py           # 23 unit tests — all passing
└── sample_output.txt  # Real output from a 300-message simulation run
```

---

## Key Design Decisions

### 1. Deduplication — TTL seen-set vs Bloom filter

The deduplicator uses a `dict` (O(1) lookup) paired with a `deque` (O(1) expiry eviction). Memory is bounded to `max_cache_size = 100,000` entries.

For cardinalities above ~10M msg/sec, a Bloom filter is preferable — fixed memory, ~0.1% false-positive rate (legitimate record occasionally dropped), but no OOM risk under high cardinality.

```python
# Current: exact dedup, bounded memory
if record.message_id in self._seen:
    return True   # duplicate

# Alternative: Bloom filter for ultra-high cardinality
# from pybloom_live import ScalableBloomFilter
```

### 2. Aggregation — tumbling windows, boundary-correct flush

Windows are tumbling (non-overlapping). Flush collects records within `[window_start, window_start + window_seconds]` before pruning the buffer — this prevents data loss on window boundaries, a common bug in naive implementations.

The next window starts at the closed window's boundary, not at `now()` — prevents accumulating drift across long-running pipelines.

### 3. Anomaly detection — two complementary detectors

**Per-record Z-score** catches sudden spikes immediately:

```
Z = (observed_value − rolling_mean) / rolling_std
Flag if |Z| > 3.0 (configurable)
```

**Post-window spike detector** catches sustained anomalies that individual Z-scores miss — compares each window's mean against the last 30 window means. A sensor running 15% high for 30 minutes would pass per-record checks but fail the window-level check.

### 4. Reliability — at-least-once + idempotent dedup = effectively exactly-once

In Kafka mode, offsets are committed manually after successful processing. Combined with the deduplicator's TTL window, this gives effectively exactly-once semantics without requiring Kafka transactions — which have significant throughput overhead.

---

## Running Locally

```bash
# Install dependencies (stdlib only for core — Kafka optional)
pip install confluent-kafka   # only needed for real Kafka mode

# Run in simulator mode (no Kafka needed)
python main.py

# Run tests
python -m pytest tests.py -v
```

**Simulator mode** (default, `CONFIG.simulate = True`) generates realistic MFM telemetry at 10 Hz with deliberate faults injected:
- **4%** malformed records (SCADA comm errors, truncated packets)
- **5%** duplicate message IDs (Kafka redelivery simulation)
- **3%** anomalous values (pressure surges, flow blockages, cavitation)

**Kafka mode**: set `simulate = False` in `config.py` and point `bootstrap_servers` at your broker.

---

## Sample Output

See [`sample_output.txt`](sample_output.txt) for a real 300-message run.

```
========================================================================
  LPG PIPELINE TELEMETRY — Mass Flow Meter Anomaly Detection Demo
  Sensors: KDL (Kondapalli) · RJY (Rajahmundry) · MNG (Mangalore)
========================================================================

  [MALFORMED]  JSON parse error: Expecting value: line 1 column 1
  [DUPLICATE]  msg_id=813dbe81-cd4a-44...  sensor=MFM_MNG_COMP_01
  [ALERT] CRITICAL  MFM_MNG_COMP_01    temperature_c    value=    54.308  Z=+4.91σ
  [ALERT] CRITICAL  MFM_KDL_INLET_01   mass_flow_rate   value=  5092.076  Z=+5.62σ
  [WINDOW] MFM_RJY_XFER_01   mass_flow_rate_kgh   n= 11  mean=5261.470  p95=5405.365
  ...
  SUMMARY: received=300  processed=265  malformed=11  duplicates=17  alerts=8
========================================================================
```

---

## Real-World Context

This project directly mirrors systems built during 12 years at **HPCL (Hindustan Petroleum Corporation Limited)** — a Fortune Global 500 energy company operating a 55-plant national LPG distribution network processing ~1.5 million cylinders daily.

Specifically, the anomaly detection logic in `anomaly.py` is the software equivalent of:
- **IoT sensor bottom corrosion detection** — Isolation Forest on continuous sensor streams across 1.5M daily operations
- **Predictive maintenance network-wide** — continuous IoT sensor data with dual-layer rule-based + ML alerting replacing manual gauge monitoring across 55 plants

The mass flow meter parameters (pressure ranges, flow rates, LPG density) are modelled on real LPG transfer line specifications.

---

## Test Coverage

```
tests.py::TestParsing              — 6 tests  (JSON validation, field checks, type coercion)
tests.py::TestDeduplicator         — 6 tests  (TTL eviction, capacity, stats accuracy)
tests.py::TestAggregator           — 5 tests  (window timing, multi-sensor, min-samples guard)
tests.py::TestAnomalyDetector      — 6 tests  (baseline warm-up, Z-score trigger, window spike)

23 passed in < 2 seconds
```

---

## Configuration Reference

All tuneable parameters in `config.py`:

| Parameter | Default | Description |
|---|---|---|
| `dedup.ttl_seconds` | 300 | Dedup window (5 min matches Kafka max redelivery lag) |
| `dedup.max_cache_size` | 100,000 | Memory cap on seen-set |
| `aggregation.window_seconds` | 60 | Tumbling window size |
| `aggregation.min_samples_required` | 3 | Skip window if too sparse |
| `anomaly.zscore_threshold` | 3.0 | Flag if \|Z\| exceeds this |
| `anomaly.rolling_window` | 20 | Baseline sample count per sensor/metric |
| `anomaly.min_baseline_samples` | 10 | Don't alert until baseline is warm |
