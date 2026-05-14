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

### Real-World Production Architecture
*How this pipeline runs in a live industrial environment*

```
  ┌─────────────────────────────────────────────────────────┐
  │                    PLANT FLOOR                          │
  │                                                         │
  │  [MFM_KDL_INLET_01]  [MFM_RJY_XFER_01]  [MFM_MNG_*]  │
  │   Coriolis Sensor      Coriolis Sensor    Coriolis ...  │
  │   (pressure, temp,     (flow, density,    (pressure,    │
  │    flow, density)       pressure, temp)    temp, flow)  │
  └──────────┬──────────────────┬─────────────────┬────────┘
             │                  │                 │
             ▼                  ▼                 ▼
  ┌─────────────────────────────────────────────────────────┐
  │              SCADA / IoT GATEWAY LAYER                  │
  │   Converts raw sensor signals to structured JSON        │
  │   Publishes to Kafka over MQTT / OPC-UA / Modbus        │
  └─────────────────────────┬───────────────────────────────┘
                            │  JSON messages, ~10 Hz per sensor
                            ▼
  ┌─────────────────────────────────────────────────────────┐
  │                  KAFKA BROKER                           │
  │   Topic: lpg.mfm.telemetry.raw                         │
  │   Partitioned by sensor_id                              │
  │   Retention: 7 days  |  Replication factor: 3          │
  │   Delivery guarantee: at-least-once                     │
  └─────────────────────────┬───────────────────────────────┘
                            │  Consumer group: telemetry-pipeline-v1
                            ▼
  ┌─────────────────────────────────────────────────────────┐
  │              THIS PIPELINE (ingestor.py)                │
  │                                                         │
  │  parse JSON → validate fields → reject malformed        │
  │         ↓                         ↓                     │
  │   TelemetryRecord           MalformedRecord             │
  │         ↓                    logged + counted           │
  │   [Deduplicator]                                        │
  │   TTL seen-set (Redis in multi-instance prod)           │
  │         ↓                                               │
  │   [Anomaly Detector]  → Alert fired immediately         │
  │   per-record Z-score                                    │
  │         ↓                                               │
  │   [Window Aggregator]                                   │
  │   1-min tumbling windows                                │
  │         ↓                                               │
  │   [Window Spike Detector] → Alert on sustained drift    │
  │   30-window historical norm                             │
  └────────┬──────────────────────┬──────────────────────── ┘
           │                      │
           ▼                      ▼
  ┌──────────────────┐  ┌────────────────────────────┐
  │   ALERT SINK     │  │        DATA SINK            │
  │  PagerDuty       │  │  TimescaleDB / InfluxDB     │
  │  Slack / Email   │  │  Window summaries + alerts  │
  └──────────────────┘  │  Grafana / Power BI         │
                        │  Operations Dashboard        │
                        └────────────────────────────┘
```

**Production config** — environment variables, broker address never in GitHub:
```bash
SIMULATE=false
KAFKA_BROKER=kafka-prod.plant.internal:9092
KAFKA_TOPIC=lpg.mfm.telemetry.raw
KAFKA_GROUP_ID=telemetry-pipeline-v1
```

---

### Demo / Portfolio Architecture
*Runs locally — no Kafka broker, no external dependencies*

```
  ┌─────────────────────────────────────────────────────────┐
  │         BUILT-IN MFM SIMULATOR (ingestor.py)            │
  │   Generates realistic Coriolis MFM data at 10 Hz        │
  │   Injects: 4% malformed · 5% duplicates · 3% anomalies  │
  │   Plants: KDL (Kondapalli) · RJY (Rajahmundry) · MNG    │
  └─────────────────────────┬───────────────────────────────┘
                            │  No Kafka broker needed
                            ▼
             [ Ingestor — parse + validate ]
                            │
                            ▼
             [ Deduplicator — in-memory dict + deque ]
                            │
                            ▼
             [ Anomaly Detector — rolling Z-score ]
                            │
                            ▼
             [ Window Aggregator — 60s tumbling ]
                            │
                            ▼
             [ Window Spike Detector ]
                   ┌────────┴────────┐
                   ▼                 ▼
            Alerts → stdout     Windows → stdout
       [CRITICAL] ZSCORE        [WINDOW] MFM_KDL_INLET_01
       MFM_MNG_COMP_01          mean=11.243  p95=12.891
       value=54.3  Z=+4.91σ     n=47  min=10.1  max=13.4
```

**Demo config:**
```python
simulate: bool = True   # flip to False + set KafkaConfig for real Kafka
```

**Run immediately:**
```bash
python main.py
```

---

### Switching Demo to Production — 3 Steps, One File

| Step | What changes | Location |
|---|---|---|
| 1 | `simulate = False` | `config.py` |
| 2 | `bootstrap_servers` → real broker address | `config.py` → `KafkaConfig` |
| 3 | `topic` → real topic name (agreed with SCADA team) | `config.py` → `KafkaConfig` |

**Zero changes** to deduplicator, aggregator, anomaly detector, or pipeline logic.
The pipeline is completely decoupled from the data source by design.

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

## Production Deployment Guide
*Three-layer pattern to convert this prototype into a real production pipeline*

This section documents every change needed to go from the demo simulator to a
live industrial deployment receiving real Kafka telemetry. Nothing in the core
pipeline logic changes — only configuration and infrastructure wiring.

---

### Layer 1 — The On/Off Switch

In `config.py`, `PipelineConfig`:

```python
# DEMO (current)
simulate: bool = True    # True = built-in MFM simulator | False = real Kafka data

# PRODUCTION
simulate: bool = False   # pipeline now reads from real Kafka broker
```

---

### Layer 2 — Kafka Broker Configuration

In `config.py`, `KafkaConfig` — point to your real broker and topic:

```python
# DEMO (current)
@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"   # local placeholder
    topic: str = "telemetry.raw"                # placeholder topic
    group_id: str = "telemetry-pipeline-v1"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False            # keep False — manual commit always
    max_poll_records: int = 500
    session_timeout_ms: int = 30_000

# PRODUCTION — values provided by your Kafka/infrastructure team
@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = "kafka-prod.plant.internal:9092"  # real broker
    topic: str = "lpg.mfm.telemetry.raw"                       # agreed with SCADA team
    group_id: str = "telemetry-pipeline-v1"                    # no change
    auto_offset_reset: str = "earliest"                        # no change
    enable_auto_commit: bool = False                           # never change this
    max_poll_records: int = 500                                # tune based on throughput
    session_timeout_ms: int = 30_000                          # no change
```

> **Note:** `enable_auto_commit` must always stay `False` in production.
> Manual offset commit + deduplicator = effectively exactly-once semantics.
> Auto-commit risks silent data loss on crash.

---

### Layer 3 — Environment Variables (never hardcode secrets in GitHub)

In production, broker addresses, credentials, and topic names must never
be hardcoded in source files that go into version control.

**Step 1 — Update `config.py` to read from environment:**

```python
import os

@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "telemetry.raw")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "telemetry-pipeline-v1")

@dataclass(frozen=True)
class PipelineConfig:
    ...
    simulate: bool = os.getenv("SIMULATE", "true").lower() == "true"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
```

**Step 2 — Set environment variables on the production server:**

```bash
# On the production server / in your Docker container / Kubernetes ConfigMap
export SIMULATE=false
export KAFKA_BROKER=kafka-prod.plant.internal:9092
export KAFKA_TOPIC=lpg.mfm.telemetry.raw
export KAFKA_GROUP_ID=telemetry-pipeline-v1
export LOG_LEVEL=INFO
```

**Step 3 — For Kubernetes deployment, use a ConfigMap:**

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: telemetry-pipeline-config
data:
  SIMULATE: "false"
  KAFKA_BROKER: "kafka-prod.plant.internal:9092"
  KAFKA_TOPIC: "lpg.mfm.telemetry.raw"
  KAFKA_GROUP_ID: "telemetry-pipeline-v1"
  LOG_LEVEL: "INFO"
```

---

### Additional Production Upgrades (Beyond Config)

These are architectural upgrades for high-scale deployments — not required
for correctness, but important for resilience and scale:

| Component | Demo (current) | Production upgrade | Why |
|---|---|---|---|
| **Deduplicator state** | In-memory dict+deque | Redis with TTL keys | Multiple consumer instances share dedup state |
| **Aggregator state** | In-memory deque per sensor | Kafka Streams / Flink state store | Survives consumer restarts, no window data loss |
| **Alert sink** | `print()` to stdout | PagerDuty + Slack webhooks | Real on-call alerting |
| **Window sink** | `print()` to stdout | TimescaleDB / InfluxDB writer | Queryable history for dashboards |
| **Schema validation** | Manual field checks in `parse_message()` | Avro + Schema Registry | Contract enforcement between SCADA publisher and pipeline consumer |
| **Observability** | `logging` every 30s | Prometheus metrics + Grafana | Throughput, lag, alert rate, dedup rate — all queryable |
| **Anomaly baseline** | Rolling Z-score from cold start | EWMA + periodic Isolation Forest retraining | Adapts to slow drift; catches multivariate anomalies |
| **Scaling** | Single process | Kafka partition-per-sensor + N consumer instances | Linear horizontal scale with no shared state |

---

### What Never Changes

Regardless of whether you are in demo or full production scale —
the following components require **zero modifications**:

- `models.py` — data shapes are environment-agnostic
- `deduplicator.py` — TTL logic is identical (only backing store changes)
- `aggregator.py` — window logic is identical (only state persistence changes)
- `anomaly.py` — Z-score detection logic is identical
- `pipeline.py` — orchestration flow is identical
- `tests.py` — all 23 tests remain valid against production logic

The core pipeline is **infrastructure-agnostic by design.**
Only the edges (data source, state store, alert/window sinks) are swappable.

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
