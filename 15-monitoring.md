# Design Logging & Monitoring System

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Log Ingestion Pipeline](#log-ingestion-pipeline)
5. [Structured Logging & SDK](#structured-logging--sdk)
6. [Log Storage & Indexing (Elasticsearch)](#log-storage--indexing-elasticsearch)
7. [Metrics Collection & Time-Series (Prometheus)](#metrics-collection--time-series-prometheus)
8. [Alerting Engine](#alerting-engine)
9. [Log Query & Search API](#log-query--search-api)
10. [Dashboards & Visualization (Grafana)](#dashboards--visualization-grafana)
11. [Distributed Tracing](#distributed-tracing)
12. [Log Retention & Lifecycle](#log-retention--lifecycle)
13. [Docker & Docker Compose](#docker--docker-compose)
14. [Testing](#testing)
15. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
16. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement             | Description                                                             |
| ----------------------- | ----------------------------------------------------------------------- |
| Log Ingestion           | Accept logs from thousands of services via HTTP and gRPC endpoints      |
| Structured Logging      | Enforce a schema (timestamp, service, level, trace_id, message, meta)   |
| Full-Text Search        | Query logs by keyword, service name, level, time range, trace ID        |
| Metrics Collection      | Collect counters, gauges, and histograms from services (Prometheus)     |
| Alerting                | Trigger alerts when metrics breach thresholds or error rates spike      |
| Dashboards              | Real-time dashboards for system health, error rates, latency            |
| Distributed Tracing     | Correlate logs across services using trace IDs (OpenTelemetry)          |
| Log Retention           | Configurable retention policies (hot → warm → cold → delete)           |
| Multi-Tenancy           | Isolate logs by service/team with RBAC (who can see what)               |
| Tail & Stream           | Real-time log tailing via WebSockets for live debugging                 |

### Non-Functional Requirements

| Requirement      | Target                                                    |
| ---------------- | --------------------------------------------------------- |
| Ingestion Rate   | 2 M log events per second at peak                         |
| Query Latency    | Full-text search returns in < 2 seconds for 7-day window  |
| Availability     | 99.99% uptime (losing logs during an outage is worst-case)|
| Durability       | Zero log loss once acknowledged by the ingestion pipeline |
| Retention        | Hot: 7 days, Warm: 30 days, Cold: 365 days (GCS)         |
| Scalability      | Handle 500+ services sending logs simultaneously          |

### Back-of-the-Envelope Estimations

```
Log events per day              ≈ 50 B (across all services)
Log events per second           ≈ 50 B / 86400 ≈ 580 K (avg), ~2 M (peak)
Average log event size          ≈ 500 bytes (structured JSON)
Daily raw log volume            ≈ 50 B × 500 B = 25 TB/day
Elasticsearch index size (7d)   ≈ 25 TB × 7 = 175 TB (hot storage)
Warm storage (30 days)          ≈ 25 TB × 30 = 750 TB
Cold storage (365 days, compressed) ≈ 25 TB × 365 × 0.3 = ~2.7 PB
Metrics time-series             ≈ 10 M active series
Kafka throughput                ≈ 2 M events/sec × 500 B = 1 GB/sec
Alerts evaluated per minute     ≈ 50 K rules
```

At 2 M events/sec peak, a single Elasticsearch cluster cannot keep up. The architecture uses Kafka as a buffer between producers and consumers, absorbing spikes without backpressure. Elasticsearch handles indexing at a steady rate from Kafka consumers. Prometheus stores metrics separately (not logs) for fast alerting queries. Cold storage on GCS uses compressed Parquet for cost-efficient long-term retention at ~$0.02/GB/month.

---

## High-Level Architecture

```
                        ┌──────────────────┐
                        │    Services      │
                        │ (Apps, APIs,     │
                        │  Infrastructure) │
                        └────────┬─────────┘
                                 │
                   ┌─────────────┼─────────────┐
                   │             │             │
            ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
            │  Log SDK    │ │Metrics │ │  Trace SDK  │
            │  (HTTP/gRPC)│ │Exporter│ │(OpenTelemetry)│
            └──────┬──────┘ └───┬────┘ └──────┬──────┘
                   │            │             │
            ┌──────▼──────┐    │       ┌──────▼──────┐
            │ Ingestion   │    │       │   Jaeger /  │
            │ API (FastAPI)│   │       │   Tempo     │
            └──────┬──────┘    │       └─────────────┘
                   │            │
            ┌──────▼──────┐    │
            │   Kafka     │    │
            │ (Buffer +   │    │
            │  Partition) │    │
            └──────┬──────┘    │
                   │            │
         ┌─────────┼──────┐    │
         │         │      │    │
   ┌─────▼───┐ ┌──▼───┐ ┌▼────▼──────┐
   │Elastic- │ │ GCS  │ │ Prometheus │
   │search   │ │(Cold │ │ (Metrics   │
   │(Hot +   │ │ Arch-│ │  TSDB)     │
   │ Warm)   │ │ ive) │ │            │
   └────┬────┘ └──────┘ └──────┬─────┘
        │                       │
   ┌────▼────────────────┐ ┌───▼─────────┐
   │  Query API (FastAPI)│ │  Alertmanager│
   │  Full-text search   │ │  (Rules +   │
   │  + aggregations     │ │   Notify)   │
   └────┬────────────────┘ └───┬─────────┘
        │                       │
   ┌────▼──────────────────────▼──────┐
   │           Grafana                │
   │  Dashboards + Explore + Alerts   │
   └──────────────────────────────────┘
```

Services emit logs via the Log SDK (structured JSON over HTTP or gRPC) and metrics via Prometheus exporters. The Ingestion API validates, enriches (adds timestamps, trace IDs), and publishes logs to Kafka. Kafka acts as a durable buffer, decoupling producers from consumers so a spike in log volume doesn't overwhelm Elasticsearch. Kafka consumers index logs into Elasticsearch for full-text search (hot/warm) and archive to GCS for long-term cold storage. Prometheus scrapes metrics from services and the monitoring platform itself. Alertmanager evaluates rules and sends notifications via Slack, PagerDuty, or email. Grafana provides dashboards for both logs (via Elasticsearch) and metrics (via Prometheus). OpenTelemetry trace IDs correlate logs across services.

---

## Project Setup

### Project Structure

```
monitoring/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models (alert rules, retention)
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── receiver.py       # HTTP/gRPC log receiver endpoint
│   │   ├── validator.py      # Schema validation and enrichment
│   │   └── publisher.py      # Kafka producer (publish validated logs)
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── elasticsearch.py  # Elasticsearch client and indexing
│   │   ├── consumer.py       # Kafka consumer (index into ES)
│   │   └── archiver.py       # GCS cold storage archiver
│   ├── query/
│   │   ├── __init__.py
│   │   ├── search.py         # Full-text search and aggregations
│   │   └── tail.py           # Real-time WebSocket log tailing
│   ├── metrics/
│   │   ├── __init__.py
│   │   ├── collector.py      # Prometheus metrics registry
│   │   └── exporters.py      # Custom metric exporters
│   ├── alerting/
│   │   ├── __init__.py
│   │   ├── engine.py         # Alert rule evaluation engine
│   │   ├── rules.py          # Alert rule CRUD and storage
│   │   └── notifier.py       # Notification channels (Slack, email)
│   ├── tracing/
│   │   ├── __init__.py
│   │   └── otel.py           # OpenTelemetry setup and propagation
│   └── routers/
│       ├── __init__.py
│       ├── ingest.py         # Log ingestion endpoints
│       ├── logs.py           # Log query and search endpoints
│       ├── alerts.py         # Alert rule management endpoints
│       ├── dashboards.py     # Dashboard configuration endpoints
│       └── health.py         # Health check and readiness endpoints
├── workers/
│   ├── __init__.py
│   ├── indexer.py            # Kafka → Elasticsearch indexing worker
│   └── archiver.py           # Elasticsearch → GCS archiver worker
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_ingestion.py     # Tests for log ingestion
│   ├── test_search.py        # Tests for log search
│   ├── test_alerting.py      # Tests for alert engine
│   └── test_metrics.py       # Tests for metrics collection
├── dashboards/
│   └── system_overview.json  # Grafana dashboard JSON
├── alerting_rules/
│   └── default_rules.yml     # Default Prometheus alert rules
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir monitoring && cd monitoring
python -m venv venv
source venv/bin/activate              # macOS/Linux
```

### `requirements.txt`

```text
fastapi[standard]==0.115.0
sqlalchemy==2.0.35
psycopg2-binary==2.9.10
alembic==1.13.3
kafka-python==2.0.2
elasticsearch==8.15.1
redis==5.1.1
prometheus-client==0.21.0
opentelemetry-api==1.27.0
opentelemetry-sdk==1.27.0
opentelemetry-exporter-otlp==1.27.0
opentelemetry-instrumentation-fastapi==0.48b0
google-cloud-storage==2.18.2
httpx==0.27.2
websockets==13.1
python-dotenv==1.0.1
jinja2==3.1.4
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `kafka-python` is the Python client for Apache Kafka. `elasticsearch` is the official Elasticsearch client. `prometheus-client` exposes metrics in Prometheus format. `opentelemetry-*` packages provide distributed tracing with auto-instrumentation for FastAPI. `google-cloud-storage` is the GCS client for cold storage archiving. `websockets` enables real-time log tailing. `jinja2` renders alert notification templates.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/monitoring
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_LOGS=logs-raw
KAFKA_CONSUMER_GROUP=log-indexers
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX_PREFIX=logs
REDIS_URL=redis://localhost:6379/0
GCS_BUCKET=monitoring-cold-archive
GCS_ARCHIVE_AFTER_DAYS=30
RETENTION_HOT_DAYS=7
RETENTION_WARM_DAYS=30
RETENTION_COLD_DAYS=365
ALERT_EVALUATION_INTERVAL=60
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz
PAGERDUTY_ROUTING_KEY=your-pagerduty-key
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
LOG_LEVEL=INFO
```

`KAFKA_BOOTSTRAP_SERVERS` is the Kafka broker address. `KAFKA_TOPIC_LOGS` is the topic where raw logs are published. `ELASTICSEARCH_INDEX_PREFIX` prefixes daily indices like `logs-2025.01.15`. `GCS_BUCKET` stores cold archives. `RETENTION_HOT_DAYS` controls how long logs stay in Elasticsearch's hot tier (SSD-backed) before moving to warm (HDD-backed). `ALERT_EVALUATION_INTERVAL` is how often (in seconds) the alert engine evaluates rules. `OTEL_EXPORTER_OTLP_ENDPOINT` points to the OpenTelemetry collector for trace export.

---

## Log Ingestion Pipeline

The ingestion pipeline is the front door of the system. It receives logs from services, validates them, enriches them with metadata, and publishes them to Kafka for downstream processing.

### `app/config.py`

```python
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    database_url: str = "postgresql://user:pass@localhost:5432/monitoring"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_logs: str = "logs-raw"
    kafka_consumer_group: str = "log-indexers"
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index_prefix: str = "logs"
    redis_url: str = "redis://localhost:6379/0"
    gcs_bucket: str = "monitoring-cold-archive"
    gcs_archive_after_days: int = 30
    retention_hot_days: int = 7
    retention_warm_days: int = 30
    retention_cold_days: int = 365
    alert_evaluation_interval: int = 60
    slack_webhook_url: str = ""
    pagerduty_routing_key: str = ""
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads environment variables and `.env` files. `@lru_cache` ensures the settings object is created once and reused across the application, avoiding redundant parsing on every request.

### `app/schemas.py`

```python
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from typing import Any


class LogLevel(str, Enum):
    """Standard log levels matching syslog severity."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogEntry(BaseModel):
    """A single structured log event from a service."""

    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp of the event",
    )
    service: str = Field(
        ..., min_length=1, max_length=128,
        description="Name of the service emitting the log",
    )
    level: LogLevel = Field(
        ..., description="Log severity level",
    )
    message: str = Field(
        ..., min_length=1, max_length=10000,
        description="Human-readable log message",
    )
    trace_id: str | None = Field(
        default=None, max_length=64,
        description="Distributed trace ID for correlation",
    )
    span_id: str | None = Field(
        default=None, max_length=32,
        description="Span ID within the trace",
    )
    host: str | None = Field(
        default=None, max_length=256,
        description="Hostname or IP of the source machine",
    )
    environment: str = Field(
        default="production",
        description="Deployment environment (production, staging, dev)",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary key-value pairs for context",
    )


class LogBatch(BaseModel):
    """A batch of log entries for bulk ingestion."""

    logs: list[LogEntry] = Field(
        ..., min_length=1, max_length=10000,
        description="List of log entries (max 10,000 per batch)",
    )


class LogSearchRequest(BaseModel):
    """Query parameters for searching logs."""

    query: str = Field(
        default="*", description="Full-text search query (Lucene syntax)",
    )
    service: str | None = Field(
        default=None, description="Filter by service name",
    )
    level: LogLevel | None = Field(
        default=None, description="Filter by log level",
    )
    trace_id: str | None = Field(
        default=None, description="Filter by trace ID",
    )
    start_time: datetime | None = Field(
        default=None, description="Start of time range (UTC)",
    )
    end_time: datetime | None = Field(
        default=None, description="End of time range (UTC)",
    )
    size: int = Field(
        default=100, ge=1, le=10000,
        description="Number of results to return",
    )
    offset: int = Field(
        default=0, ge=0,
        description="Offset for pagination",
    )
    sort_order: str = Field(
        default="desc",
        description="Sort by timestamp: asc or desc",
    )


class LogSearchResponse(BaseModel):
    """Response from a log search query."""

    total: int
    took_ms: int
    logs: list[LogEntry]


class AlertRule(BaseModel):
    """A rule that defines when to fire an alert."""

    name: str = Field(..., min_length=1, max_length=256)
    description: str = Field(default="")
    query: str = Field(
        ..., description="Elasticsearch query or PromQL expression",
    )
    condition: str = Field(
        ..., description="Threshold condition: gt, lt, eq, gte, lte",
    )
    threshold: float = Field(
        ..., description="Numeric threshold to compare against",
    )
    window_seconds: int = Field(
        default=300, description="Evaluation window in seconds",
    )
    severity: str = Field(
        default="warning",
        description="Alert severity: info, warning, critical",
    )
    channels: list[str] = Field(
        default_factory=lambda: ["slack"],
        description="Notification channels: slack, pagerduty, email",
    )
    enabled: bool = Field(default=True)
    cooldown_seconds: int = Field(
        default=300,
        description="Minimum seconds between repeat alerts",
    )


class AlertEvent(BaseModel):
    """An alert that has been triggered."""

    rule_name: str
    severity: str
    message: str
    current_value: float
    threshold: float
    triggered_at: datetime
    resolved_at: datetime | None = None


class MetricPoint(BaseModel):
    """A single metric data point."""

    name: str
    labels: dict[str, str] = Field(default_factory=dict)
    value: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthResponse(BaseModel):
    """Health check response with component status."""

    status: str
    kafka: str
    elasticsearch: str
    redis: str
    postgres: str
    uptime_seconds: float
```

`LogEntry` enforces a strict schema for every log event. `service` identifies which microservice emitted the log. `trace_id` and `span_id` enable distributed tracing correlation — you can search for all logs from a single request across multiple services. `metadata` is a flexible dict for service-specific fields like `user_id`, `request_path`, or `duration_ms`. `LogBatch` accepts up to 10,000 logs per request for efficient bulk ingestion. `LogSearchRequest` supports Lucene query syntax for full-text search, plus structured filters for service, level, and trace ID. `AlertRule` defines a threshold-based alert: "if error count in the last 5 minutes exceeds 100, send a Slack notification."

### `app/ingestion/validator.py`

```python
import uuid
import hashlib
from datetime import datetime, timezone

from app.schemas import LogEntry


def enrich_log(log: LogEntry, source_ip: str | None = None) -> dict:
    """
    Enrich a log entry with server-side metadata.

    Adds fields that the client should not control:
    - _id: deterministic dedup key
    - _received_at: server-side timestamp
    - _source_ip: sender's IP for audit
    """
    log_dict = log.model_dump(mode="json")

    # Server-side receive timestamp
    log_dict["_received_at"] = datetime.now(timezone.utc).isoformat()

    # Source IP for audit trail
    if source_ip:
        log_dict["_source_ip"] = source_ip

    # Generate trace_id if not provided
    if not log_dict.get("trace_id"):
        log_dict["trace_id"] = uuid.uuid4().hex

    # Deterministic dedup ID based on content
    dedup_input = f"{log_dict['timestamp']}{log_dict['service']}{log_dict['message']}"
    log_dict["_id"] = hashlib.sha256(dedup_input.encode()).hexdigest()[:20]

    return log_dict


def validate_batch(logs: list[LogEntry]) -> tuple[list[dict], list[dict]]:
    """
    Validate and enrich a batch of logs.

    Returns:
        (valid_logs, rejected_logs) — rejected logs include the reason
    """
    valid = []
    rejected = []

    for log in logs:
        try:
            enriched = enrich_log(log)
            valid.append(enriched)
        except Exception as e:
            rejected.append({
                "log": log.model_dump(mode="json"),
                "reason": str(e),
            })

    return valid, rejected
```

`enrich_log` adds server-controlled fields that clients cannot spoof. `_received_at` is the server-side timestamp — different from the client's `timestamp` — used to detect clock drift between services. `_id` is a deterministic dedup key: if the same log is sent twice (network retry), it generates the same `_id`, and Elasticsearch upserts instead of creating a duplicate. `validate_batch` processes logs in bulk, separating valid from rejected so a single bad log doesn't fail the entire batch.

### `app/ingestion/publisher.py`

```python
import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config import get_settings

logger = logging.getLogger(__name__)

_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    """Get or create the Kafka producer singleton."""
    global _producer
    if _producer is None:
        settings = get_settings()
        _producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",                       # Wait for all replicas
            retries=5,                        # Retry on transient failures
            retry_backoff_ms=100,
            max_in_flight_requests_per_connection=5,
            linger_ms=10,                     # Batch messages for 10 ms
            batch_size=65536,                 # 64 KB batch size
            compression_type="lz4",           # Compress for throughput
            buffer_memory=134217728,          # 128 MB buffer
        )
    return _producer


def publish_logs(logs: list[dict], topic: str | None = None) -> int:
    """
    Publish a batch of validated log events to Kafka.

    Args:
        logs: List of enriched log dicts
        topic: Override topic (defaults to config)

    Returns:
        Number of successfully published logs
    """
    settings = get_settings()
    topic = topic or settings.kafka_topic_logs
    producer = get_producer()
    published = 0

    for log in logs:
        try:
            # Partition by service name for ordering within a service
            key = log.get("service", "unknown")
            producer.send(topic, key=key, value=log)
            published += 1
        except KafkaError as e:
            logger.error(
                "Failed to publish log to Kafka",
                extra={"service": log.get("service"), "error": str(e)},
            )

    # Flush to ensure all messages are sent
    producer.flush(timeout=10)
    return published


def close_producer():
    """Close the Kafka producer gracefully."""
    global _producer
    if _producer:
        _producer.flush(timeout=30)
        _producer.close(timeout=30)
        _producer = None
```

`acks="all"` ensures a log is written to all Kafka replicas before acknowledging — this guarantees durability (zero log loss). `linger_ms=10` waits 10 ms to batch multiple messages into a single network request, dramatically improving throughput at the cost of 10 ms added latency. `compression_type="lz4"` compresses batches, reducing network bandwidth by ~60% for JSON logs. `key=service` partitions logs by service name, so all logs from the same service land on the same partition, preserving ordering within a service. The 128 MB buffer (`buffer_memory`) absorbs bursts without blocking the ingestion API.

### `app/routers/ingest.py`

```python
import logging
import time

from fastapi import APIRouter, Request, HTTPException, status

from app.schemas import LogEntry, LogBatch
from app.ingestion.validator import enrich_log, validate_batch
from app.ingestion.publisher import publish_logs
from app.metrics.collector import (
    LOGS_RECEIVED,
    LOGS_PUBLISHED,
    LOGS_REJECTED,
    INGESTION_LATENCY,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["ingestion"])


@router.post("/logs", status_code=status.HTTP_202_ACCEPTED)
async def ingest_single(log: LogEntry, request: Request):
    """
    Ingest a single log event.

    The log is validated, enriched, and published to Kafka.
    Returns 202 Accepted immediately — processing is async.
    """
    start = time.perf_counter()
    source_ip = request.client.host if request.client else None

    LOGS_RECEIVED.labels(service=log.service, level=log.level.value).inc()

    enriched = enrich_log(log, source_ip=source_ip)
    published = publish_logs([enriched])

    if published == 0:
        LOGS_REJECTED.labels(service=log.service, reason="kafka_error").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to publish log to Kafka",
        )

    LOGS_PUBLISHED.labels(service=log.service).inc()
    INGESTION_LATENCY.labels(service=log.service).observe(
        time.perf_counter() - start,
    )

    return {"status": "accepted", "id": enriched["_id"]}


@router.post("/logs/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(batch: LogBatch, request: Request):
    """
    Ingest a batch of log events (up to 10,000).

    Each log is independently validated. Valid logs are published
    to Kafka; rejected logs are returned in the response.
    """
    start = time.perf_counter()
    source_ip = request.client.host if request.client else None

    # Enrich all logs with source IP
    enriched_logs = []
    rejected_logs = []

    for log in batch.logs:
        try:
            LOGS_RECEIVED.labels(
                service=log.service, level=log.level.value,
            ).inc()
            enriched = enrich_log(log, source_ip=source_ip)
            enriched_logs.append(enriched)
        except Exception as e:
            LOGS_REJECTED.labels(
                service=log.service, reason="validation",
            ).inc()
            rejected_logs.append({
                "service": log.service,
                "message": log.message[:100],
                "reason": str(e),
            })

    # Publish valid logs
    published = publish_logs(enriched_logs) if enriched_logs else 0

    for log in enriched_logs[:published]:
        LOGS_PUBLISHED.labels(service=log.get("service", "unknown")).inc()

    elapsed = time.perf_counter() - start
    INGESTION_LATENCY.labels(service="batch").observe(elapsed)

    return {
        "status": "accepted",
        "total": len(batch.logs),
        "published": published,
        "rejected": len(rejected_logs),
        "rejected_logs": rejected_logs,
        "took_ms": round(elapsed * 1000, 2),
    }
```

The ingestion endpoint returns HTTP 202 (Accepted) immediately, not 200. This signals to the client that the log has been received and queued, but not yet indexed into Elasticsearch. This decoupling is critical for performance — the client doesn't wait for Elasticsearch indexing. Batch ingestion processes each log independently; a malformed log in position 50 doesn't reject the other 9,999 logs. The rejected logs are returned in the response so the client knows exactly what failed and why. Prometheus counters track received, published, and rejected counts per service.

---

## Structured Logging & SDK

Provide a Python SDK that services import to emit structured logs without worrying about the wire format.

### `app/ingestion/sdk.py`

```python
"""
Lightweight logging SDK for services to send structured logs.

Usage:
    from monitoring_sdk import MonitoringLogger

    logger = MonitoringLogger(
        service="payment-api",
        endpoint="https://monitoring.internal/api/v1/logs/batch",
        environment="production",
    )

    logger.info("Payment processed", user_id="u123", amount=99.99)
    logger.error("Payment failed", user_id="u123", error="insufficient_funds")

    # Flush buffered logs (called automatically at interval)
    logger.flush()
"""

import time
import json
import logging
import threading
import traceback
from datetime import datetime, timezone
from typing import Any

import httpx


class MonitoringLogger:
    """
    Structured logging client that buffers and batches log events.

    Logs are buffered in memory and flushed to the monitoring API
    every `flush_interval` seconds or when the buffer reaches
    `buffer_size` entries, whichever comes first.
    """

    def __init__(
        self,
        service: str,
        endpoint: str,
        environment: str = "production",
        buffer_size: int = 500,
        flush_interval: float = 5.0,
        timeout: float = 10.0,
    ):
        self.service = service
        self.endpoint = endpoint
        self.environment = environment
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.timeout = timeout

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._client = httpx.Client(timeout=timeout)

        # Background flush thread
        self._running = True
        self._flush_thread = threading.Thread(
            target=self._periodic_flush, daemon=True,
        )
        self._flush_thread.start()

        # Standard Python logger fallback
        self._fallback = logging.getLogger(f"monitoring.{service}")

    def _build_entry(
        self, level: str, message: str, **kwargs: Any,
    ) -> dict:
        """Build a structured log entry dict."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service,
            "level": level,
            "message": message,
            "environment": self.environment,
            "metadata": kwargs,
        }

    def debug(self, message: str, **kwargs: Any):
        self._emit("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs: Any):
        self._emit("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs: Any):
        self._emit("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs: Any):
        # Auto-capture stack trace for errors
        if "stack_trace" not in kwargs:
            kwargs["stack_trace"] = traceback.format_exc()
        self._emit("ERROR", message, **kwargs)

    def critical(self, message: str, **kwargs: Any):
        if "stack_trace" not in kwargs:
            kwargs["stack_trace"] = traceback.format_exc()
        self._emit("CRITICAL", message, **kwargs)

    def _emit(self, level: str, message: str, **kwargs: Any):
        """Add a log entry to the buffer."""
        entry = self._build_entry(level, message, **kwargs)

        with self._lock:
            self._buffer.append(entry)

            if len(self._buffer) >= self.buffer_size:
                self._flush_buffer()

        # Also log to stdout for local development
        self._fallback.log(
            getattr(logging, level, logging.INFO),
            message,
            extra=kwargs,
        )

    def _periodic_flush(self):
        """Background thread that flushes the buffer at intervals."""
        while self._running:
            time.sleep(self.flush_interval)
            with self._lock:
                if self._buffer:
                    self._flush_buffer()

    def _flush_buffer(self):
        """Send buffered logs to the monitoring API."""
        if not self._buffer:
            return

        batch = self._buffer.copy()
        self._buffer.clear()

        try:
            response = self._client.post(
                self.endpoint,
                json={"logs": batch},
                timeout=self.timeout,
            )
            if response.status_code != 202:
                self._fallback.error(
                    f"Monitoring API returned {response.status_code}: "
                    f"{response.text}",
                )
        except Exception as e:
            self._fallback.error(f"Failed to flush logs: {e}")
            # Re-add failed logs to buffer (with cap to prevent OOM)
            with self._lock:
                remaining_capacity = self.buffer_size * 3 - len(self._buffer)
                if remaining_capacity > 0:
                    self._buffer.extend(batch[:remaining_capacity])

    def flush(self):
        """Manually flush the buffer."""
        with self._lock:
            self._flush_buffer()

    def close(self):
        """Flush remaining logs and stop the background thread."""
        self._running = False
        self.flush()
        self._client.close()
```

The SDK buffers logs in memory and flushes them in batches every 5 seconds or when the buffer hits 500 entries. This reduces network overhead — instead of one HTTP request per log, one request carries hundreds. The background daemon thread handles periodic flushing so the application never blocks on log shipping. `error()` and `critical()` automatically capture the stack trace. If the monitoring API is unreachable, failed logs are re-added to the buffer (up to 3× buffer size) to survive transient failures without risking out-of-memory. `_fallback` logs to stdout as a safety net so logs are never silently lost, even if the monitoring API is completely down.

---

## Log Storage & Indexing (Elasticsearch)

### `app/storage/elasticsearch.py`

```python
import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers

from app.config import get_settings

logger = logging.getLogger(__name__)

_es_client: Elasticsearch | None = None


def get_es_client() -> Elasticsearch:
    """Get or create the Elasticsearch client singleton."""
    global _es_client
    if _es_client is None:
        settings = get_settings()
        _es_client = Elasticsearch(
            settings.elasticsearch_url,
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True,
        )
    return _es_client


def get_index_name(prefix: str | None = None, date: datetime | None = None) -> str:
    """
    Generate a daily index name.

    Format: logs-2025.01.15
    Daily indices enable efficient retention management — drop an entire
    index instead of deleting individual documents.
    """
    settings = get_settings()
    prefix = prefix or settings.elasticsearch_index_prefix
    date = date or datetime.now(timezone.utc)
    return f"{prefix}-{date.strftime('%Y.%m.%d')}"


# Index mapping (schema) for log documents
LOG_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "_received_at": {"type": "date"},
            "service": {
                "type": "keyword",           # Exact match, aggregations
            },
            "level": {
                "type": "keyword",           # Filter by ERROR, WARNING, etc.
            },
            "message": {
                "type": "text",              # Full-text search
                "analyzer": "standard",
                "fields": {
                    "keyword": {             # Also allow exact match
                        "type": "keyword",
                        "ignore_above": 256,
                    }
                },
            },
            "trace_id": {"type": "keyword"},
            "span_id": {"type": "keyword"},
            "host": {"type": "keyword"},
            "environment": {"type": "keyword"},
            "_source_ip": {"type": "ip"},
            "_id": {"type": "keyword"},
            "metadata": {
                "type": "object",
                "enabled": True,             # Index all nested fields
            },
        }
    },
    "settings": {
        "number_of_shards": 5,               # Parallel indexing/search
        "number_of_replicas": 1,             # One replica for durability
        "refresh_interval": "5s",            # Balance between real-time and perf
        "index.lifecycle.name": "log-lifecycle",  # ILM policy
        "index.codec": "best_compression",   # DEFLATE for smaller indices
    },
}


def ensure_index(index_name: str):
    """Create the index with mapping if it doesn't exist."""
    es = get_es_client()
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=LOG_INDEX_MAPPING)
        logger.info(f"Created index: {index_name}")


def bulk_index(logs: list[dict]) -> tuple[int, int]:
    """
    Bulk index log documents into Elasticsearch.

    Returns:
        (success_count, error_count)
    """
    es = get_es_client()

    actions = []
    for log in logs:
        index_name = get_index_name(
            date=datetime.fromisoformat(log["timestamp"]),
        )
        ensure_index(index_name)

        actions.append({
            "_index": index_name,
            "_id": log.get("_id"),           # Dedup by ID
            "_source": log,
        })

    success, errors = helpers.bulk(
        es,
        actions,
        raise_on_error=False,
        raise_on_exception=False,
        chunk_size=1000,                     # 1000 docs per bulk request
        request_timeout=60,
    )

    if errors:
        logger.error(
            f"Bulk indexing errors: {len(errors)} failures",
            extra={"errors": errors[:5]},    # Log first 5 errors
        )

    return success, len(errors) if isinstance(errors, list) else 0
```

Elasticsearch uses daily indices (`logs-2025.01.15`) instead of a single massive index. Daily indices make retention trivial — to delete logs older than 7 days, just drop the index. No expensive delete-by-query. `service` and `level` are `keyword` type for exact matching and aggregations (count errors per service). `message` is `text` type for full-text search with a sub-field `keyword` for exact matching. `number_of_shards=5` parallelizes indexing and search across 5 shards. `refresh_interval=5s` means new logs are searchable within 5 seconds — the trade-off between real-time visibility and indexing performance. `best_compression` uses DEFLATE instead of LZ4, reducing index size by ~30% at the cost of slightly slower indexing. `_id` is set to our deterministic dedup key so re-indexing the same log overwrites instead of duplicating.

### `workers/indexer.py`

```python
"""
Kafka → Elasticsearch indexing worker.

Consumes log events from Kafka and bulk-indexes them into
Elasticsearch. Runs as a separate process from the API server.

Usage:
    python -m workers.indexer
"""

import json
import signal
import logging
import time

from kafka import KafkaConsumer

from app.config import get_settings
from app.storage.elasticsearch import bulk_index
from app.metrics.collector import (
    LOGS_INDEXED,
    LOGS_INDEX_ERRORS,
    INDEX_LATENCY,
    KAFKA_LAG,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("indexer")


class LogIndexer:
    """
    Consumes logs from Kafka and indexes them into Elasticsearch.

    Uses micro-batching: collects messages for up to `batch_timeout_ms`
    or until `batch_size` is reached, then bulk-indexes the batch.
    """

    def __init__(
        self,
        batch_size: int = 2000,
        batch_timeout_ms: int = 5000,
    ):
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self._running = True

        settings = get_settings()
        self.consumer = KafkaConsumer(
            settings.kafka_topic_logs,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            group_id=settings.kafka_consumer_group,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",    # Start from beginning if new group
            enable_auto_commit=False,        # Manual commit after indexing
            max_poll_records=batch_size,
            fetch_max_wait_ms=batch_timeout_ms,
            session_timeout_ms=30000,
        )

        # Handle graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        """Handle shutdown signal gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False

    def run(self):
        """Main consumer loop."""
        logger.info(
            f"Indexer started. Batch size: {self.batch_size}, "
            f"timeout: {self.batch_timeout_ms}ms",
        )

        while self._running:
            try:
                # Poll for a batch of messages
                records = self.consumer.poll(
                    timeout_ms=self.batch_timeout_ms,
                    max_records=self.batch_size,
                )

                if not records:
                    continue

                # Flatten all partitions into a single batch
                batch = []
                for topic_partition, messages in records.items():
                    for message in messages:
                        batch.append(message.value)

                    # Track consumer lag
                    KAFKA_LAG.labels(
                        partition=str(topic_partition.partition),
                    ).set(
                        messages[-1].offset - messages[0].offset,
                    )

                if not batch:
                    continue

                # Bulk index into Elasticsearch
                start = time.perf_counter()
                success, errors = bulk_index(batch)
                elapsed = time.perf_counter() - start

                LOGS_INDEXED.inc(success)
                LOGS_INDEX_ERRORS.inc(errors)
                INDEX_LATENCY.observe(elapsed)

                logger.info(
                    f"Indexed batch: {success} success, {errors} errors, "
                    f"{elapsed:.2f}s",
                )

                # Commit offsets only after successful indexing
                self.consumer.commit()

            except Exception as e:
                logger.error(f"Indexer error: {e}", exc_info=True)
                time.sleep(5)                # Back off on errors

        # Clean shutdown
        self.consumer.close()
        logger.info("Indexer stopped.")


if __name__ == "__main__":
    indexer = LogIndexer()
    indexer.run()
```

The indexer runs as a separate process from the API server. This decoupling is essential — if Elasticsearch is slow, the ingestion API still accepts logs (Kafka buffers them). `enable_auto_commit=False` with manual commit after `bulk_index` guarantees at-least-once delivery: if the indexer crashes mid-batch, Kafka replays those messages. `max_poll_records` limits how many messages are consumed per poll to control batch size. The consumer tracks Kafka lag (difference between latest offset and committed offset) as a metric, which is the most important health indicator — growing lag means indexing can't keep up with ingestion.

---

## Metrics Collection & Time-Series (Prometheus)

### `app/metrics/collector.py`

```python
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    Info,
    generate_latest,
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
)

# ──────────────────────────────────────────────────────────
# Ingestion Metrics
# ──────────────────────────────────────────────────────────

LOGS_RECEIVED = Counter(
    "logs_received_total",
    "Total number of log events received by the ingestion API",
    ["service", "level"],
)

LOGS_PUBLISHED = Counter(
    "logs_published_total",
    "Total number of log events successfully published to Kafka",
    ["service"],
)

LOGS_REJECTED = Counter(
    "logs_rejected_total",
    "Total number of log events rejected during validation",
    ["service", "reason"],
)

INGESTION_LATENCY = Histogram(
    "ingestion_latency_seconds",
    "Time spent processing a log ingestion request",
    ["service"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# ──────────────────────────────────────────────────────────
# Indexing Metrics
# ──────────────────────────────────────────────────────────

LOGS_INDEXED = Counter(
    "logs_indexed_total",
    "Total number of log events successfully indexed into Elasticsearch",
)

LOGS_INDEX_ERRORS = Counter(
    "logs_index_errors_total",
    "Total number of log events that failed to index",
)

INDEX_LATENCY = Histogram(
    "index_latency_seconds",
    "Time spent bulk-indexing a batch into Elasticsearch",
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

KAFKA_LAG = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag per partition",
    ["partition"],
)

# ──────────────────────────────────────────────────────────
# Query Metrics
# ──────────────────────────────────────────────────────────

SEARCH_REQUESTS = Counter(
    "search_requests_total",
    "Total number of log search requests",
    ["service_filter", "level_filter"],
)

SEARCH_LATENCY = Histogram(
    "search_latency_seconds",
    "Time spent executing a log search query",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

SEARCH_RESULTS = Histogram(
    "search_results_count",
    "Number of results returned per search query",
    buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000],
)

# ──────────────────────────────────────────────────────────
# Alert Metrics
# ──────────────────────────────────────────────────────────

ALERTS_FIRED = Counter(
    "alerts_fired_total",
    "Total number of alerts triggered",
    ["rule_name", "severity"],
)

ALERTS_RESOLVED = Counter(
    "alerts_resolved_total",
    "Total number of alerts resolved",
    ["rule_name"],
)

ALERT_EVALUATION_LATENCY = Histogram(
    "alert_evaluation_latency_seconds",
    "Time spent evaluating alert rules",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0],
)

# ──────────────────────────────────────────────────────────
# System Metrics
# ──────────────────────────────────────────────────────────

ACTIVE_WEBSOCKETS = Gauge(
    "active_websocket_connections",
    "Number of active WebSocket connections for log tailing",
)

ES_CLUSTER_HEALTH = Gauge(
    "elasticsearch_cluster_health",
    "Elasticsearch cluster health (0=red, 1=yellow, 2=green)",
)

SYSTEM_INFO = Info(
    "monitoring_system",
    "Information about the monitoring system build",
)
```

Every metric follows the Prometheus naming convention: `<namespace>_<name>_<unit>`. Counters monotonically increase (total logs received). Histograms track distributions (ingestion latency with custom buckets tuned for millisecond-level operations). Gauges represent current values that go up and down (active WebSocket connections, Kafka lag). Labels add dimensions — `LOGS_RECEIVED` is labeled by `service` and `level`, letting you query "how many ERROR logs did payment-api emit in the last hour?" The custom histogram buckets are tuned for each metric's expected range: ingestion latency buckets start at 1 ms (fast path), while index latency buckets go up to 10 seconds (bulk operations are slower).

### `app/metrics/exporters.py`

```python
import logging
import time
import threading

from app.config import get_settings
from app.storage.elasticsearch import get_es_client
from app.metrics.collector import ES_CLUSTER_HEALTH, SYSTEM_INFO

logger = logging.getLogger(__name__)


def collect_es_health():
    """Collect Elasticsearch cluster health as a Prometheus gauge."""
    try:
        es = get_es_client()
        health = es.cluster.health()
        status_map = {"green": 2, "yellow": 1, "red": 0}
        ES_CLUSTER_HEALTH.set(status_map.get(health["status"], -1))
    except Exception as e:
        ES_CLUSTER_HEALTH.set(-1)
        logger.warning(f"Failed to check ES health: {e}")


def start_system_metrics_collector(interval: int = 30):
    """
    Start a background thread that collects system-level metrics.

    Runs every `interval` seconds, collecting:
    - Elasticsearch cluster health
    - System info labels
    """
    SYSTEM_INFO.info({
        "version": "1.0.0",
        "environment": get_settings().log_level,
    })

    def _collect():
        while True:
            collect_es_health()
            time.sleep(interval)

    thread = threading.Thread(target=_collect, daemon=True)
    thread.start()
    logger.info(f"System metrics collector started (interval={interval}s)")
```

The Elasticsearch health collector runs as a daemon thread, updating the `ES_CLUSTER_HEALTH` gauge every 30 seconds. This metric is critical for alerting — a `red` cluster means data loss is possible (some shards are unassigned). Prometheus scrapes the `/metrics` endpoint and stores these as time-series data, enabling queries like "when did the cluster health drop to yellow in the last 24 hours?"

---

## Alerting Engine

### `app/alerting/engine.py`

```python
import logging
import time
import threading
from datetime import datetime, timezone, timedelta

from app.config import get_settings
from app.alerting.notifier import send_alert_notification
from app.storage.elasticsearch import get_es_client
from app.metrics.collector import (
    ALERTS_FIRED,
    ALERTS_RESOLVED,
    ALERT_EVALUATION_LATENCY,
)

logger = logging.getLogger(__name__)


class AlertEngine:
    """
    Periodically evaluates alert rules against Elasticsearch data.

    For each rule:
    1. Execute the query against Elasticsearch
    2. Compare result against the threshold
    3. If condition is met and cooldown has passed, fire the alert
    4. Track firing state to detect resolved alerts
    """

    def __init__(self):
        self._rules: list[dict] = []
        self._firing: dict[str, datetime] = {}   # rule_name → last_fired_at
        self._running = True

    def load_rules(self, rules: list[dict]):
        """Load alert rules (from DB or config file)."""
        self._rules = rules
        logger.info(f"Loaded {len(rules)} alert rules")

    def evaluate_rule(self, rule: dict) -> dict | None:
        """
        Evaluate a single alert rule.

        Returns an alert event dict if the rule fires, None otherwise.
        """
        es = get_es_client()
        settings = get_settings()

        # Time range for the evaluation window
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(seconds=rule["window_seconds"])

        # Build Elasticsearch query
        es_query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {
                            "gte": window_start.isoformat(),
                            "lte": now.isoformat(),
                        }}},
                    ],
                },
            },
            "size": 0,                       # We only need the count
        }

        # Parse the rule query (supports simple patterns)
        rule_query = rule["query"]

        if rule_query.startswith("count:"):
            # count:service=payment-api,level=ERROR
            filters = rule_query.replace("count:", "").split(",")
            for f in filters:
                field, value = f.split("=")
                es_query["query"]["bool"]["must"].append(
                    {"term": {field.strip(): value.strip()}},
                )
        elif rule_query.startswith("search:"):
            # search:OutOfMemoryError
            search_term = rule_query.replace("search:", "").strip()
            es_query["query"]["bool"]["must"].append(
                {"match": {"message": search_term}},
            )

        try:
            # Search across all daily indices in the window
            index_pattern = f"{settings.elasticsearch_index_prefix}-*"
            result = es.search(index=index_pattern, body=es_query)
            current_value = result["hits"]["total"]["value"]
        except Exception as e:
            logger.error(
                f"Alert rule query failed: {rule['name']}: {e}",
            )
            return None

        # Compare against threshold
        condition = rule["condition"]
        threshold = rule["threshold"]
        fired = False

        if condition == "gt" and current_value > threshold:
            fired = True
        elif condition == "gte" and current_value >= threshold:
            fired = True
        elif condition == "lt" and current_value < threshold:
            fired = True
        elif condition == "lte" and current_value <= threshold:
            fired = True
        elif condition == "eq" and current_value == threshold:
            fired = True

        if fired:
            # Check cooldown
            last_fired = self._firing.get(rule["name"])
            cooldown = rule.get("cooldown_seconds", 300)

            if last_fired:
                elapsed = (now - last_fired).total_seconds()
                if elapsed < cooldown:
                    return None    # Still in cooldown period

            self._firing[rule["name"]] = now

            return {
                "rule_name": rule["name"],
                "severity": rule.get("severity", "warning"),
                "message": (
                    f"Alert: {rule['name']} — "
                    f"{rule['query']} returned {current_value} "
                    f"(threshold: {condition} {threshold})"
                ),
                "current_value": current_value,
                "threshold": threshold,
                "triggered_at": now.isoformat(),
            }
        else:
            # Check if a previously firing alert is now resolved
            if rule["name"] in self._firing:
                del self._firing[rule["name"]]
                ALERTS_RESOLVED.labels(rule_name=rule["name"]).inc()
                logger.info(f"Alert resolved: {rule['name']}")

            return None

    def evaluate_all(self):
        """Evaluate all enabled rules."""
        start = time.perf_counter()
        fired_alerts = []

        for rule in self._rules:
            if not rule.get("enabled", True):
                continue

            alert = self.evaluate_rule(rule)
            if alert:
                fired_alerts.append(alert)
                ALERTS_FIRED.labels(
                    rule_name=alert["rule_name"],
                    severity=alert["severity"],
                ).inc()
                logger.warning(
                    f"Alert fired: {alert['rule_name']} "
                    f"(value={alert['current_value']})",
                )

                # Send notification
                channels = rule.get("channels", ["slack"])
                send_alert_notification(alert, channels)

        ALERT_EVALUATION_LATENCY.observe(time.perf_counter() - start)
        return fired_alerts

    def run(self, interval: int | None = None):
        """
        Main loop: evaluate all rules at a fixed interval.

        Runs as a blocking loop — start in a background thread
        or separate process.
        """
        interval = interval or get_settings().alert_evaluation_interval
        logger.info(f"Alert engine started (interval={interval}s)")

        while self._running:
            try:
                self.evaluate_all()
            except Exception as e:
                logger.error(f"Alert evaluation cycle failed: {e}")

            time.sleep(interval)

    def stop(self):
        """Stop the alert evaluation loop."""
        self._running = False
```

The alert engine follows a pull-based model: it evaluates rules at a fixed interval (default 60 seconds) instead of evaluating on every log event. This is critical at scale — evaluating 50 K rules on every one of 2 M logs/sec is impossible. `_firing` tracks which rules are currently in the firing state, enabling resolved detection: if a rule was firing and its condition is no longer met, it's marked as resolved. The cooldown prevents alert storms — if an error rate is consistently above the threshold, you get one alert every 5 minutes, not one per evaluation cycle. The query parser supports simple patterns (`count:service=payment-api,level=ERROR`) that translate to Elasticsearch queries.

### `app/alerting/notifier.py`

```python
import json
import logging
from datetime import datetime

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


SEVERITY_COLORS = {
    "critical": "#FF0000",
    "warning": "#FFA500",
    "info": "#36A2EB",
}

SEVERITY_EMOJI = {
    "critical": "🚨",
    "warning": "⚠️",
    "info": "ℹ️",
}


def send_slack_alert(alert: dict):
    """Send an alert notification to Slack via incoming webhook."""
    settings = get_settings()
    if not settings.slack_webhook_url:
        logger.warning("Slack webhook URL not configured, skipping")
        return

    severity = alert.get("severity", "warning")
    emoji = SEVERITY_EMOJI.get(severity, "⚠️")
    color = SEVERITY_COLORS.get(severity, "#FFA500")

    payload = {
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {alert['rule_name']}",
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Severity:*\n{severity.upper()}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"*Current Value:*\n"
                                f"{alert['current_value']}"
                            ),
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Threshold:*\n{alert['threshold']}",
                        },
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"*Triggered At:*\n"
                                f"{alert['triggered_at']}"
                            ),
                        },
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"```{alert['message']}```",
                    },
                },
            ],
        }],
    }

    try:
        response = httpx.post(
            settings.slack_webhook_url,
            json=payload,
            timeout=10,
        )
        if response.status_code != 200:
            logger.error(
                f"Slack notification failed: {response.status_code}",
            )
    except Exception as e:
        logger.error(f"Slack notification error: {e}")


def send_pagerduty_alert(alert: dict):
    """Send an alert to PagerDuty via Events API v2."""
    settings = get_settings()
    if not settings.pagerduty_routing_key:
        logger.warning("PagerDuty routing key not configured, skipping")
        return

    severity_map = {
        "critical": "critical",
        "warning": "warning",
        "info": "info",
    }

    payload = {
        "routing_key": settings.pagerduty_routing_key,
        "event_action": "trigger",
        "payload": {
            "summary": alert["message"],
            "severity": severity_map.get(
                alert.get("severity", "warning"), "warning",
            ),
            "source": "monitoring-system",
            "component": alert["rule_name"],
            "custom_details": {
                "current_value": alert["current_value"],
                "threshold": alert["threshold"],
                "triggered_at": alert["triggered_at"],
            },
        },
    }

    try:
        response = httpx.post(
            "https://events.pagerduty.com/v2/enqueue",
            json=payload,
            timeout=10,
        )
        if response.status_code != 202:
            logger.error(
                f"PagerDuty notification failed: {response.status_code}",
            )
    except Exception as e:
        logger.error(f"PagerDuty notification error: {e}")


def send_alert_notification(alert: dict, channels: list[str]):
    """Dispatch an alert to all configured notification channels."""
    dispatchers = {
        "slack": send_slack_alert,
        "pagerduty": send_pagerduty_alert,
    }

    for channel in channels:
        dispatcher = dispatchers.get(channel)
        if dispatcher:
            try:
                dispatcher(alert)
                logger.info(
                    f"Alert sent via {channel}: {alert['rule_name']}",
                )
            except Exception as e:
                logger.error(
                    f"Failed to send alert via {channel}: {e}",
                )
        else:
            logger.warning(f"Unknown notification channel: {channel}")
```

The Slack notification uses Block Kit for rich formatting with color-coded severity (red for critical, orange for warning). PagerDuty uses the Events API v2, which creates an incident that pages on-call engineers for critical alerts. The dispatcher pattern makes adding new channels trivial — implement a new function, add it to the `dispatchers` dict. Each channel is called independently, so a Slack failure doesn't prevent PagerDuty from firing.

---

## Log Query & Search API

### `app/query/search.py`

```python
import logging
import time
from datetime import datetime, timezone, timedelta

from app.config import get_settings
from app.storage.elasticsearch import get_es_client
from app.metrics.collector import SEARCH_LATENCY, SEARCH_RESULTS

logger = logging.getLogger(__name__)


def search_logs(
    query: str = "*",
    service: str | None = None,
    level: str | None = None,
    trace_id: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    size: int = 100,
    offset: int = 0,
    sort_order: str = "desc",
) -> dict:
    """
    Search logs in Elasticsearch with full-text and structured filters.

    Args:
        query: Lucene query string for full-text search
        service: Filter by exact service name
        level: Filter by log level
        trace_id: Filter by distributed trace ID
        start_time: Start of time range
        end_time: End of time range
        size: Number of results to return
        offset: Pagination offset
        sort_order: Sort by timestamp (asc/desc)

    Returns:
        Dict with total count, query time, and log entries
    """
    es = get_es_client()
    settings = get_settings()
    start = time.perf_counter()

    # Default time range: last 24 hours
    if not start_time:
        start_time = datetime.now(timezone.utc) - timedelta(hours=24)
    if not end_time:
        end_time = datetime.now(timezone.utc)

    # Build Elasticsearch query
    must_clauses = [
        {"range": {"timestamp": {
            "gte": start_time.isoformat(),
            "lte": end_time.isoformat(),
        }}},
    ]

    # Full-text search on message field
    if query and query != "*":
        must_clauses.append(
            {"query_string": {
                "query": query,
                "default_field": "message",
                "analyze_wildcard": True,
            }},
        )

    # Structured filters (exact match)
    filter_clauses = []
    if service:
        filter_clauses.append({"term": {"service": service}})
    if level:
        filter_clauses.append({"term": {"level": level}})
    if trace_id:
        filter_clauses.append({"term": {"trace_id": trace_id}})

    es_query = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses,
            },
        },
        "sort": [{"timestamp": {"order": sort_order}}],
        "from": offset,
        "size": size,
        "highlight": {
            "fields": {
                "message": {
                    "fragment_size": 200,
                    "number_of_fragments": 3,
                },
            },
        },
    }

    try:
        index_pattern = f"{settings.elasticsearch_index_prefix}-*"
        result = es.search(index=index_pattern, body=es_query)

        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        total = result["hits"]["total"]["value"]

        logs = []
        for hit in result["hits"]["hits"]:
            log = hit["_source"]
            # Include highlighted fragments if available
            if "highlight" in hit:
                log["_highlights"] = hit["highlight"]
            logs.append(log)

        SEARCH_LATENCY.observe(elapsed_ms / 1000)
        SEARCH_RESULTS.observe(len(logs))

        return {
            "total": total,
            "took_ms": elapsed_ms,
            "logs": logs,
        }

    except Exception as e:
        logger.error(f"Search query failed: {e}", exc_info=True)
        return {"total": 0, "took_ms": 0, "logs": [], "error": str(e)}


def get_log_aggregations(
    service: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> dict:
    """
    Get aggregated log statistics.

    Returns counts by level, top services, error rate over time,
    and other useful aggregations for dashboards.
    """
    es = get_es_client()
    settings = get_settings()

    if not start_time:
        start_time = datetime.now(timezone.utc) - timedelta(hours=24)
    if not end_time:
        end_time = datetime.now(timezone.utc)

    must_clauses = [
        {"range": {"timestamp": {
            "gte": start_time.isoformat(),
            "lte": end_time.isoformat(),
        }}},
    ]
    if service:
        must_clauses.append({"term": {"service": service}})

    es_query = {
        "query": {"bool": {"must": must_clauses}},
        "size": 0,
        "aggs": {
            "by_level": {
                "terms": {"field": "level", "size": 10},
            },
            "by_service": {
                "terms": {"field": "service", "size": 50},
            },
            "over_time": {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": "5m",
                },
                "aggs": {
                    "by_level": {
                        "terms": {"field": "level"},
                    },
                },
            },
            "error_rate": {
                "filters": {
                    "filters": {
                        "errors": {"terms": {
                            "level": ["ERROR", "CRITICAL"],
                        }},
                        "total": {"match_all": {}},
                    },
                },
            },
        },
    }

    try:
        index_pattern = f"{settings.elasticsearch_index_prefix}-*"
        result = es.search(index=index_pattern, body=es_query)

        aggs = result.get("aggregations", {})
        return {
            "by_level": {
                b["key"]: b["doc_count"]
                for b in aggs.get("by_level", {}).get("buckets", [])
            },
            "by_service": {
                b["key"]: b["doc_count"]
                for b in aggs.get("by_service", {}).get("buckets", [])
            },
            "over_time": [
                {
                    "timestamp": b["key_as_string"],
                    "count": b["doc_count"],
                    "by_level": {
                        lb["key"]: lb["doc_count"]
                        for lb in b.get("by_level", {}).get("buckets", [])
                    },
                }
                for b in aggs.get("over_time", {}).get("buckets", [])
            ],
        }

    except Exception as e:
        logger.error(f"Aggregation query failed: {e}")
        return {}
```

`search_logs` combines full-text search with structured filters. `query_string` uses Lucene syntax so operators like `AND`, `OR`, `NOT`, wildcards (`*`), and phrase matching (`"exact phrase"`) work. Structured filters (`term`) use keyword fields for exact matching — searching for service `payment-api` doesn't match `payment-api-v2`. `highlight` returns snippets of the matching text with the search terms wrapped in `<em>` tags, useful for displaying search results in a dashboard. `get_log_aggregations` provides the data for dashboards: log counts by level (how many ERRORs?), by service (which service logs the most?), and over time (5-minute buckets for time-series charts). `error_rate` gives the ratio of ERROR+CRITICAL to total logs.

### `app/query/tail.py`

```python
import json
import asyncio
import logging
from datetime import datetime, timezone

from fastapi import WebSocket, WebSocketDisconnect

from app.config import get_settings
from app.storage.elasticsearch import get_es_client
from app.metrics.collector import ACTIVE_WEBSOCKETS

logger = logging.getLogger(__name__)


class LogTailManager:
    """
    Manages WebSocket connections for real-time log tailing.

    Clients connect via WebSocket and receive new log events
    matching their filter criteria in real time.
    """

    def __init__(self):
        self._connections: dict[str, dict] = {}  # conn_id → {ws, filters}

    async def connect(
        self,
        websocket: WebSocket,
        conn_id: str,
        filters: dict | None = None,
    ):
        """Accept a WebSocket connection and register it."""
        await websocket.accept()
        self._connections[conn_id] = {
            "ws": websocket,
            "filters": filters or {},
        }
        ACTIVE_WEBSOCKETS.inc()
        logger.info(
            f"Log tail connected: {conn_id} "
            f"(filters={filters})",
        )

    def disconnect(self, conn_id: str):
        """Remove a WebSocket connection."""
        if conn_id in self._connections:
            del self._connections[conn_id]
            ACTIVE_WEBSOCKETS.dec()
            logger.info(f"Log tail disconnected: {conn_id}")

    def _matches_filters(self, log: dict, filters: dict) -> bool:
        """Check if a log entry matches the connection's filters."""
        for key, value in filters.items():
            if key in log and log[key] != value:
                return False
        return True

    async def broadcast(self, log: dict):
        """
        Send a log event to all connected clients whose filters match.

        Disconnected clients are cleaned up automatically.
        """
        disconnected = []

        for conn_id, conn in self._connections.items():
            if not self._matches_filters(log, conn["filters"]):
                continue

            try:
                await conn["ws"].send_json(log)
            except WebSocketDisconnect:
                disconnected.append(conn_id)
            except Exception as e:
                logger.error(f"WebSocket send error for {conn_id}: {e}")
                disconnected.append(conn_id)

        for conn_id in disconnected:
            self.disconnect(conn_id)

    async def tail_elasticsearch(
        self,
        filters: dict | None = None,
        poll_interval: float = 1.0,
    ):
        """
        Poll Elasticsearch for new logs and broadcast to clients.

        Uses a cursor-based approach: tracks the latest timestamp
        and only fetches logs newer than that.
        """
        es = get_es_client()
        settings = get_settings()
        last_timestamp = datetime.now(timezone.utc).isoformat()

        while self._connections:
            try:
                query = {
                    "query": {
                        "bool": {
                            "must": [
                                {"range": {"timestamp": {
                                    "gt": last_timestamp,
                                }}},
                            ],
                        },
                    },
                    "sort": [{"timestamp": {"order": "asc"}}],
                    "size": 100,
                }

                # Apply filters if provided
                if filters:
                    for key, value in filters.items():
                        query["query"]["bool"]["must"].append(
                            {"term": {key: value}},
                        )

                index_pattern = f"{settings.elasticsearch_index_prefix}-*"
                result = es.search(index=index_pattern, body=query)

                for hit in result["hits"]["hits"]:
                    log = hit["_source"]
                    await self.broadcast(log)
                    last_timestamp = log["timestamp"]

            except Exception as e:
                logger.error(f"Tail poll error: {e}")

            await asyncio.sleep(poll_interval)


# Global singleton
tail_manager = LogTailManager()
```

Real-time log tailing uses WebSockets so engineers can watch logs stream in during debugging — similar to `kubectl logs -f` but across all services. Each connection has filters (e.g., `service=payment-api`, `level=ERROR`) so you only see relevant logs. The tail poller queries Elasticsearch every second for new logs, using a cursor (`last_timestamp`) to avoid re-sending old logs. `broadcast` sends matching logs to all connected clients simultaneously. Disconnected clients are detected when `send_json` raises an exception and are cleaned up automatically.

### `app/routers/logs.py`

```python
import uuid
import time

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.schemas import LogSearchRequest, LogSearchResponse
from app.query.search import search_logs, get_log_aggregations
from app.query.tail import tail_manager
from app.metrics.collector import SEARCH_REQUESTS

router = APIRouter(prefix="/api/v1", tags=["logs"])


@router.post("/logs/search", response_model=LogSearchResponse)
async def search(request: LogSearchRequest):
    """
    Search logs with full-text and structured filters.

    Supports Lucene query syntax for the `query` field.
    """
    SEARCH_REQUESTS.labels(
        service_filter=request.service or "all",
        level_filter=request.level.value if request.level else "all",
    ).inc()

    result = search_logs(
        query=request.query,
        service=request.service,
        level=request.level.value if request.level else None,
        trace_id=request.trace_id,
        start_time=request.start_time,
        end_time=request.end_time,
        size=request.size,
        offset=request.offset,
        sort_order=request.sort_order,
    )

    return result


@router.get("/logs/aggregations")
async def aggregations(
    service: str | None = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
):
    """
    Get aggregated log statistics for dashboards.

    Returns counts by level, top services, and error rate over time.
    """
    from datetime import datetime, timezone, timedelta

    start_time = datetime.now(timezone.utc) - timedelta(hours=hours)
    return get_log_aggregations(service=service, start_time=start_time)


@router.get("/logs/trace/{trace_id}")
async def get_trace(trace_id: str):
    """
    Get all logs for a specific distributed trace.

    Returns logs from all services involved in the trace,
    sorted chronologically.
    """
    result = search_logs(
        trace_id=trace_id,
        size=1000,
        sort_order="asc",
    )
    return result


@router.websocket("/ws/logs/tail")
async def tail_logs(websocket: WebSocket):
    """
    WebSocket endpoint for real-time log tailing.

    Connect and send a JSON message with filter criteria:
    {"service": "payment-api", "level": "ERROR"}

    Logs matching the filters will be streamed in real time.
    """
    conn_id = uuid.uuid4().hex[:12]

    try:
        # Wait for the client to send filter criteria
        await websocket.accept()
        filters_msg = await websocket.receive_text()

        import json
        filters = json.loads(filters_msg) if filters_msg else {}

        await tail_manager.connect(websocket, conn_id, filters)

        # Keep the connection alive
        while True:
            # Listen for client messages (ping/pong or filter updates)
            try:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break

    except WebSocketDisconnect:
        pass
    finally:
        tail_manager.disconnect(conn_id)
```

The trace endpoint (`/logs/trace/{trace_id}`) is one of the most valuable features. An engineer pastes a trace ID from an error report and instantly sees every log from every service involved in that request, ordered chronologically. This is distributed debugging — without trace IDs, you'd search each service's logs individually.

---

## Distributed Tracing

### `app/tracing/otel.py`

```python
import logging

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositeHTTPPropagator
from opentelemetry.trace.propagation import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator

from app.config import get_settings

logger = logging.getLogger(__name__)


def setup_tracing(app, service_name: str = "monitoring-system"):
    """
    Configure OpenTelemetry distributed tracing.

    Sets up:
    - W3C Trace Context propagation (standard trace headers)
    - OTLP exporter to send spans to a collector (Jaeger, Tempo)
    - Auto-instrumentation for FastAPI requests
    """
    settings = get_settings()

    # Resource identifies this service in traces
    resource = Resource.create({
        "service.name": service_name,
        "service.version": "1.0.0",
        "deployment.environment": "production",
    })

    # Create tracer provider
    provider = TracerProvider(resource=resource)

    # OTLP exporter sends spans to the collector
    exporter = OTLPSpanExporter(
        endpoint=settings.otel_exporter_otlp_endpoint,
        insecure=True,                       # Use TLS in production
    )

    # BatchSpanProcessor batches spans before exporting
    processor = BatchSpanProcessor(
        exporter,
        max_queue_size=2048,
        max_export_batch_size=512,
        schedule_delay_millis=5000,
    )
    provider.add_span_processor(processor)

    # Set as global tracer provider
    trace.set_tracer_provider(provider)

    # W3C Trace Context propagation (traceparent header)
    set_global_textmap(CompositeHTTPPropagator([
        TraceContextTextMapPropagator(),
        W3CBaggagePropagator(),
    ]))

    # Auto-instrument FastAPI
    FastAPIInstrumentor.instrument_app(app)

    logger.info(
        f"OpenTelemetry tracing configured "
        f"(exporter={settings.otel_exporter_otlp_endpoint})",
    )

    return provider


def get_current_trace_id() -> str | None:
    """Get the current trace ID from the active span."""
    span = trace.get_current_span()
    if span and span.get_span_context().is_valid:
        return format(span.get_span_context().trace_id, "032x")
    return None


def get_current_span_id() -> str | None:
    """Get the current span ID from the active span."""
    span = trace.get_current_span()
    if span and span.get_span_context().is_valid:
        return format(span.get_span_context().span_id, "016x")
    return None
```

OpenTelemetry is the industry-standard for distributed tracing (merged from OpenTracing and OpenCensus). `FastAPIInstrumentor` automatically creates a span for every incoming HTTP request, capturing method, path, status code, and duration. `W3C Trace Context` propagation means the `traceparent` header (e.g., `00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01`) is automatically parsed on incoming requests and forwarded to outgoing requests. `BatchSpanProcessor` buffers spans and exports them every 5 seconds, reducing the performance impact on the application. The trace ID and span ID helpers let the ingestion pipeline automatically attach trace context to log entries, enabling the "search all logs for this trace" feature.

---

## Log Retention & Lifecycle

### `workers/archiver.py`

```python
"""
Elasticsearch → GCS archiver worker.

Moves old log indices from Elasticsearch to Google Cloud Storage
for long-term cold storage. Runs on a daily schedule.

Usage:
    python -m workers.archiver
"""

import io
import json
import gzip
import logging
from datetime import datetime, timezone, timedelta

from elasticsearch import helpers
from google.cloud import storage

from app.config import get_settings
from app.storage.elasticsearch import get_es_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("archiver")


class LogArchiver:
    """
    Archives old Elasticsearch indices to GCS and manages retention.

    Lifecycle:
    1. Hot (0-7 days): SSD-backed ES indices, full speed
    2. Warm (7-30 days): HDD-backed ES indices, slower but cheaper
    3. Cold (30-365 days): Compressed JSON in GCS, query via BigQuery
    4. Delete (365+ days): Permanently removed
    """

    def __init__(self):
        self.settings = get_settings()
        self.es = get_es_client()
        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.bucket(self.settings.gcs_bucket)

    def get_indices_older_than(self, days: int) -> list[str]:
        """Find log indices older than N days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y.%m.%d")

        indices = self.es.indices.get(
            index=f"{self.settings.elasticsearch_index_prefix}-*",
        )

        old_indices = []
        for index_name in indices:
            # Extract date from index name: logs-2025.01.15
            try:
                date_part = index_name.split("-", 1)[1]
                if date_part <= cutoff_str:
                    old_indices.append(index_name)
            except (IndexError, ValueError):
                continue

        return sorted(old_indices)

    def archive_index_to_gcs(self, index_name: str) -> int:
        """
        Export an Elasticsearch index to a compressed JSON file in GCS.

        Returns:
            Number of documents archived
        """
        logger.info(f"Archiving index: {index_name}")

        # Stream documents from Elasticsearch
        buffer = io.BytesIO()
        doc_count = 0

        with gzip.open(buffer, "wt", encoding="utf-8") as gz:
            for doc in helpers.scan(
                self.es,
                index=index_name,
                query={"query": {"match_all": {}}},
                scroll="5m",
                size=5000,
            ):
                gz.write(json.dumps(doc["_source"]) + "\n")
                doc_count += 1

                if doc_count % 100_000 == 0:
                    logger.info(
                        f"  Archived {doc_count} docs from {index_name}",
                    )

        # Upload to GCS
        buffer.seek(0)
        blob_name = f"logs/{index_name}.jsonl.gz"
        blob = self.bucket.blob(blob_name)
        blob.upload_from_file(buffer, content_type="application/gzip")

        logger.info(
            f"Archived {doc_count} docs from {index_name} → "
            f"gs://{self.settings.gcs_bucket}/{blob_name}",
        )

        return doc_count

    def move_to_warm(self):
        """
        Move hot indices older than RETENTION_HOT_DAYS to warm tier.

        Warm tier uses force-merge to reduce segment count (faster
        searches on older data) and shrinks replicas to save storage.
        """
        indices = self.get_indices_older_than(self.settings.retention_hot_days)

        for index_name in indices:
            try:
                # Reduce replicas (warm data doesn't need fast failover)
                self.es.indices.put_settings(
                    index=index_name,
                    body={"number_of_replicas": 0},
                )

                # Force merge to 1 segment (faster read performance)
                self.es.indices.forcemerge(
                    index=index_name,
                    max_num_segments=1,
                )

                logger.info(f"Moved to warm: {index_name}")

            except Exception as e:
                logger.error(f"Warm migration failed for {index_name}: {e}")

    def archive_to_cold(self):
        """
        Archive indices older than GCS_ARCHIVE_AFTER_DAYS to GCS,
        then delete them from Elasticsearch.
        """
        indices = self.get_indices_older_than(
            self.settings.gcs_archive_after_days,
        )

        for index_name in indices:
            try:
                # Archive to GCS
                doc_count = self.archive_index_to_gcs(index_name)

                # Verify upload exists before deleting
                blob_name = f"logs/{index_name}.jsonl.gz"
                blob = self.bucket.blob(blob_name)
                if blob.exists():
                    # Delete from Elasticsearch
                    self.es.indices.delete(index=index_name)
                    logger.info(
                        f"Deleted archived index: {index_name} "
                        f"({doc_count} docs now in GCS)",
                    )
                else:
                    logger.error(
                        f"GCS upload verification failed for {index_name}, "
                        f"keeping ES index",
                    )

            except Exception as e:
                logger.error(f"Cold archive failed for {index_name}: {e}")

    def delete_expired(self):
        """Delete GCS archives older than RETENTION_COLD_DAYS."""
        cutoff = datetime.now(timezone.utc) - timedelta(
            days=self.settings.retention_cold_days,
        )
        cutoff_str = cutoff.strftime("%Y.%m.%d")

        blobs = self.bucket.list_blobs(prefix="logs/")
        deleted = 0

        for blob in blobs:
            try:
                # Extract date from blob name: logs/logs-2025.01.15.jsonl.gz
                date_part = blob.name.split("-", 2)[-1].replace(
                    ".jsonl.gz", "",
                )
                if date_part <= cutoff_str:
                    blob.delete()
                    deleted += 1
                    logger.info(f"Deleted expired archive: {blob.name}")
            except (IndexError, ValueError):
                continue

        if deleted:
            logger.info(f"Deleted {deleted} expired archives from GCS")

    def run(self):
        """Run the full lifecycle management cycle."""
        logger.info("Starting lifecycle management cycle")

        # Step 1: Move old hot indices to warm
        self.move_to_warm()

        # Step 2: Archive warm indices to GCS cold storage
        self.archive_to_cold()

        # Step 3: Delete expired cold archives
        self.delete_expired()

        logger.info("Lifecycle management cycle complete")


if __name__ == "__main__":
    archiver = LogArchiver()
    archiver.run()
```

The lifecycle policy implements tiered storage to control costs. **Hot** indices (0–7 days) sit on SSDs with full replicas for fast queries. **Warm** indices (7–30 days) are force-merged into a single segment (reducing overhead from many small segments) and replicas are dropped to zero — the data is still in Elasticsearch but cheaper to store. **Cold** storage (30–365 days) exports to GCS as gzip-compressed JSONL files at ~$0.02/GB/month — 100× cheaper than Elasticsearch storage. The archiver **verifies** the GCS upload exists before deleting the Elasticsearch index — this prevents data loss if the upload fails silently. `helpers.scan` streams documents from Elasticsearch without loading them all into memory, critical for indices with billions of documents. After 365 days, archives are permanently deleted.

---

## Dashboards & Visualization (Grafana)

### `dashboards/system_overview.json`

```json
{
  "dashboard": {
    "title": "Monitoring System Overview",
    "uid": "monitoring-overview",
    "timezone": "utc",
    "refresh": "30s",
    "panels": [
      {
        "title": "Log Ingestion Rate",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
        "targets": [
          {
            "expr": "rate(logs_received_total[5m])",
            "legendFormat": "{{service}} - {{level}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "ops",
            "custom": {"drawStyle": "line", "fillOpacity": 20}
          }
        }
      },
      {
        "title": "Kafka Consumer Lag",
        "type": "gauge",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
        "targets": [
          {
            "expr": "sum(kafka_consumer_lag)",
            "legendFormat": "Total Lag"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "thresholds": {
              "steps": [
                {"color": "green", "value": 0},
                {"color": "yellow", "value": 10000},
                {"color": "red", "value": 100000}
              ]
            }
          }
        }
      },
      {
        "title": "Ingestion Latency (p99)",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
        "targets": [
          {
            "expr": "histogram_quantile(0.99, rate(ingestion_latency_seconds_bucket[5m]))",
            "legendFormat": "p99 latency"
          }
        ],
        "fieldConfig": {
          "defaults": {"unit": "s"}
        }
      },
      {
        "title": "Error Rate by Service",
        "type": "barchart",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
        "targets": [
          {
            "expr": "sum by (service) (rate(logs_received_total{level=~\"ERROR|CRITICAL\"}[5m]))",
            "legendFormat": "{{service}}"
          }
        ]
      },
      {
        "title": "Alerts Fired",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 0, "y": 16},
        "targets": [
          {
            "expr": "sum(increase(alerts_fired_total[24h]))",
            "legendFormat": "Alerts (24h)"
          }
        ]
      },
      {
        "title": "Active WebSocket Connections",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 6, "y": 16},
        "targets": [
          {
            "expr": "active_websocket_connections",
            "legendFormat": "Connections"
          }
        ]
      },
      {
        "title": "Elasticsearch Cluster Health",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 12, "y": 16},
        "targets": [
          {
            "expr": "elasticsearch_cluster_health",
            "legendFormat": "Health"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "mappings": [
              {"type": "value", "options": {"0": {"text": "RED"}, "1": {"text": "YELLOW"}, "2": {"text": "GREEN"}}}
            ],
            "thresholds": {
              "steps": [
                {"color": "red", "value": 0},
                {"color": "yellow", "value": 1},
                {"color": "green", "value": 2}
              ]
            }
          }
        }
      },
      {
        "title": "Search Latency Distribution",
        "type": "histogram",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 20},
        "targets": [
          {
            "expr": "rate(search_latency_seconds_bucket[5m])",
            "legendFormat": "{{le}}"
          }
        ]
      },
      {
        "title": "Indexing Throughput",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 20},
        "targets": [
          {
            "expr": "rate(logs_indexed_total[5m])",
            "legendFormat": "Indexed/sec"
          },
          {
            "expr": "rate(logs_index_errors_total[5m])",
            "legendFormat": "Errors/sec"
          }
        ]
      }
    ]
  }
}
```

### Default Alert Rules (`alerting_rules/default_rules.yml`)

```yaml
groups:
  - name: monitoring-system-alerts
    rules:
      - alert: HighErrorRate
        expr: |
          sum(rate(logs_received_total{level="ERROR"}[5m]))
          / sum(rate(logs_received_total[5m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Error rate above 5%"
          description: "Error rate is {{ $value | humanizePercentage }} over the last 5 minutes"

      - alert: KafkaLagHigh
        expr: sum(kafka_consumer_lag) > 100000
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "Kafka consumer lag exceeds 100K"
          description: "Log indexing is falling behind. Current lag: {{ $value }}"

      - alert: IngestionLatencyHigh
        expr: |
          histogram_quantile(0.99, rate(ingestion_latency_seconds_bucket[5m])) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "p99 ingestion latency above 500ms"
          description: "Ingestion pipeline is slow. p99: {{ $value }}s"

      - alert: ElasticsearchUnhealthy
        expr: elasticsearch_cluster_health < 2
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Elasticsearch cluster is not green"
          description: "Cluster health: {{ $value }} (0=red, 1=yellow, 2=green)"

      - alert: NoLogsReceived
        expr: rate(logs_received_total[10m]) == 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "No logs received in 10 minutes"
          description: "The ingestion pipeline may be down"

      - alert: HighIndexErrorRate
        expr: |
          rate(logs_index_errors_total[5m])
          / rate(logs_indexed_total[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Index error rate above 1%"
          description: "{{ $value | humanizePercentage }} of logs are failing to index"
```

The Grafana dashboard provides the five critical views: ingestion rate (are logs flowing?), Kafka lag (is indexing keeping up?), ingestion latency (is the pipeline fast?), error rate by service (who is unhealthy?), and Elasticsearch health (is storage okay?). Prometheus alert rules define thresholds with `for` durations — the condition must be true for 5 minutes before firing, preventing flapping alerts from transient spikes. The most critical alert is `NoLogsReceived` — if zero logs arrive for 10 minutes, the entire pipeline is likely down.

---

## Docker & Docker Compose

### `Dockerfile`

```dockerfile
FROM python:3.14-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.14-slim

WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

EXPOSE 8000

CMD ["fastapi", "run", "app/main.py", "--port", "8000"]
```

Multi-stage build keeps the final image small. The builder stage installs dependencies into `/install`, then only the installed packages are copied to the production stage. This excludes pip's build artifacts, compiler tools, and cache from the final image.

### `.dockerignore`

```text
# Virtual environment
venv/
.venv/

# Python cache
__pycache__/
*.pyc

# Git / environment files
.git/
.env

# IDE files
.vscode/
.idea/
```

### `compose.yaml`

```yaml
services:
  # ─── FastAPI Application ───────────────────────────
  api:
    build: .
    ports:
      - "8000:8000"
    env_file:
      - .env
    depends_on:
      kafka:
        condition: service_healthy
      elasticsearch:
        condition: service_healthy
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - .:/app
    restart: unless-stopped

  # ─── Kafka Consumer (Indexer Worker) ───────────────
  indexer:
    build: .
    command: python -m workers.indexer
    env_file:
      - .env
    depends_on:
      kafka:
        condition: service_healthy
      elasticsearch:
        condition: service_healthy
    restart: unless-stopped

  # ─── Archiver Worker (Daily Cron) ─────────────────
  archiver:
    build: .
    command: python -m workers.archiver
    env_file:
      - .env
    depends_on:
      elasticsearch:
        condition: service_healthy
    profiles:
      - maintenance

  # ─── Apache Kafka ─────────────────────────────────
  kafka:
    image: confluentinc/cp-kafka:7.7.1
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      CLUSTER_ID: "monitoring-cluster-001"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: kafka-topics --bootstrap-server localhost:9092 --list || exit 1
      interval: 10s
      timeout: 5s
      retries: 5

  # ─── Elasticsearch ────────────────────────────────
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.3
    ports:
      - "9200:9200"
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - xpack.security.enrollment.enabled=false
      - "ES_JAVA_OPTS=-Xms1g -Xmx1g"
    volumes:
      - es-data:/usr/share/elasticsearch/data
    healthcheck:
      test: curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green"\|"status":"yellow"'
      interval: 10s
      timeout: 5s
      retries: 10

  # ─── PostgreSQL ───────────────────────────────────
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
      POSTGRES_DB: monitoring
    volumes:
      - pg-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: pg_isready -U user -d monitoring
      interval: 5s
      timeout: 3s
      retries: 5

  # ─── Redis ────────────────────────────────────────
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: redis-cli ping | grep PONG
      interval: 5s
      timeout: 3s
      retries: 5

  # ─── Prometheus ───────────────────────────────────
  prometheus:
    image: prom/prometheus:v2.54.1
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./alerting_rules:/etc/prometheus/rules
      - prom-data:/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.retention.time=30d"
      - "--web.enable-lifecycle"

  # ─── Grafana ──────────────────────────────────────
  grafana:
    image: grafana/grafana:11.2.2
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./dashboards:/etc/grafana/provisioning/dashboards
    depends_on:
      - prometheus
      - elasticsearch

  # ─── Jaeger (Tracing) ─────────────────────────────
  jaeger:
    image: jaegertracing/all-in-one:1.62
    ports:
      - "16686:16686"      # Jaeger UI
      - "4317:4317"        # OTLP gRPC
      - "4318:4318"        # OTLP HTTP
    environment:
      COLLECTOR_OTLP_ENABLED: "true"

volumes:
  kafka-data:
  es-data:
  pg-data:
  redis-data:
  prom-data:
  grafana-data:
```

### `prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/rules/*.yml

scrape_configs:
  - job_name: "monitoring-api"
    static_configs:
      - targets: ["api:8000"]
    metrics_path: /metrics

  - job_name: "monitoring-indexer"
    static_configs:
      - targets: ["indexer:8000"]
    metrics_path: /metrics
```

The compose file runs the full monitoring stack locally. Kafka uses KRaft mode (no ZooKeeper dependency) for simpler setup. Elasticsearch runs as single-node with security disabled for development. Health checks with `condition: service_healthy` ensure services start in the correct order — the API doesn't start until Kafka, Elasticsearch, PostgreSQL, and Redis are all healthy. The archiver runs under the `maintenance` profile so it doesn't start by default (`docker compose --profile maintenance run archiver`). Prometheus scrapes the API and indexer every 15 seconds and evaluates alert rules. Grafana auto-provisions dashboards from the `dashboards/` directory. Jaeger receives OpenTelemetry traces via OTLP on port 4317.

### Running the Stack

```bash
# Start all services
docker compose up -d

# Watch logs
docker compose logs -f api indexer

# Start with rebuild
docker compose up --build -d

# Run the archiver manually
docker compose --profile maintenance run archiver

# Stop everything
docker compose down

# Stop and remove volumes (full reset)
docker compose down -v
```

---

## Main Application

### `app/main.py`

```python
import time
import logging

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from app.config import get_settings
from app.routers import ingest, logs, alerts, health
from app.tracing.otel import setup_tracing
from app.metrics.exporters import start_system_metrics_collector
from app.metrics.collector import SYSTEM_INFO
from app.alerting.engine import AlertEngine
from app.ingestion.publisher import close_producer

import threading

logger = logging.getLogger(__name__)

# Global alert engine
alert_engine = AlertEngine()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    settings = get_settings()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info("Starting Monitoring System...")

    # Start system metrics collector
    start_system_metrics_collector()

    # Load default alert rules
    default_rules = [
        {
            "name": "high-error-rate",
            "query": "count:level=ERROR",
            "condition": "gt",
            "threshold": 1000,
            "window_seconds": 300,
            "severity": "warning",
            "channels": ["slack"],
            "enabled": True,
            "cooldown_seconds": 300,
        },
        {
            "name": "critical-errors-spike",
            "query": "count:level=CRITICAL",
            "condition": "gt",
            "threshold": 10,
            "window_seconds": 60,
            "severity": "critical",
            "channels": ["slack", "pagerduty"],
            "enabled": True,
            "cooldown_seconds": 60,
        },
    ]
    alert_engine.load_rules(default_rules)

    # Start alert engine in background thread
    alert_thread = threading.Thread(
        target=alert_engine.run, daemon=True,
    )
    alert_thread.start()

    logger.info("Monitoring System started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Monitoring System...")
    alert_engine.stop()
    close_producer()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Monitoring & Logging System",
    description="Centralized log aggregation, metrics, and alerting platform",
    version="1.0.0",
    lifespan=lifespan,
)

# Setup OpenTelemetry tracing
setup_tracing(app)

# Include routers
app.include_router(ingest.router)
app.include_router(logs.router)
app.include_router(alerts.router)
app.include_router(health.router)


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
```

The `lifespan` context manager replaces the deprecated `@app.on_event("startup")` pattern. It starts the system metrics collector and the alert engine as background threads. The alert engine runs in a daemon thread so it doesn't block application shutdown. `close_producer` flushes any remaining Kafka messages before the process exits. The `/metrics` endpoint returns Prometheus text format, which Prometheus scrapes at the configured interval.

### `app/routers/alerts.py`

```python
from fastapi import APIRouter, HTTPException, status

from app.schemas import AlertRule, AlertEvent
from app.main import alert_engine

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


# In-memory store (use PostgreSQL in production)
_rules_store: dict[str, dict] = {}


@router.post("/rules", status_code=status.HTTP_201_CREATED)
async def create_rule(rule: AlertRule):
    """Create a new alert rule."""
    rule_dict = rule.model_dump()
    _rules_store[rule.name] = rule_dict

    # Reload rules in the engine
    alert_engine.load_rules(list(_rules_store.values()))

    return {"status": "created", "rule": rule.name}


@router.get("/rules")
async def list_rules():
    """List all alert rules."""
    return {"rules": list(_rules_store.values())}


@router.get("/rules/{rule_name}")
async def get_rule(rule_name: str):
    """Get a specific alert rule."""
    rule = _rules_store.get(rule_name)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule '{rule_name}' not found",
        )
    return rule


@router.put("/rules/{rule_name}")
async def update_rule(rule_name: str, rule: AlertRule):
    """Update an existing alert rule."""
    if rule_name not in _rules_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule '{rule_name}' not found",
        )

    rule_dict = rule.model_dump()
    _rules_store[rule_name] = rule_dict
    alert_engine.load_rules(list(_rules_store.values()))

    return {"status": "updated", "rule": rule_name}


@router.delete("/rules/{rule_name}")
async def delete_rule(rule_name: str):
    """Delete an alert rule."""
    if rule_name not in _rules_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule '{rule_name}' not found",
        )

    del _rules_store[rule_name]
    alert_engine.load_rules(list(_rules_store.values()))

    return {"status": "deleted", "rule": rule_name}


@router.get("/firing")
async def get_firing_alerts():
    """Get all currently firing alerts."""
    return {
        "firing": [
            {"rule_name": name, "since": ts.isoformat()}
            for name, ts in alert_engine._firing.items()
        ],
    }
```

### `app/routers/health.py`

```python
import time
import logging

from fastapi import APIRouter

from app.config import get_settings
from app.schemas import HealthResponse
from app.storage.elasticsearch import get_es_client

import redis

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])

_start_time = time.time()


def _check_component(name: str, check_fn) -> str:
    """Check a component's health, return 'healthy' or error message."""
    try:
        check_fn()
        return "healthy"
    except Exception as e:
        logger.warning(f"{name} health check failed: {e}")
        return f"unhealthy: {e}"


@router.get("/health", response_model=HealthResponse)
async def health():
    """
    Comprehensive health check for all dependencies.

    Returns the status of each component. Used by load balancers
    and Kubernetes liveness/readiness probes.
    """
    settings = get_settings()

    # Check Kafka
    kafka_status = _check_component("kafka", lambda: None)  # Checked via producer

    # Check Elasticsearch
    def check_es():
        es = get_es_client()
        es.cluster.health(request_timeout=5)

    es_status = _check_component("elasticsearch", check_es)

    # Check Redis
    def check_redis():
        r = redis.from_url(settings.redis_url)
        r.ping()

    redis_status = _check_component("redis", check_redis)

    # Check PostgreSQL
    def check_pg():
        from sqlalchemy import create_engine, text
        engine = create_engine(settings.database_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    pg_status = _check_component("postgres", check_pg)

    # Overall status
    all_healthy = all(
        s == "healthy"
        for s in [kafka_status, es_status, redis_status, pg_status]
    )

    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        kafka=kafka_status,
        elasticsearch=es_status,
        redis=redis_status,
        postgres=pg_status,
        uptime_seconds=round(time.time() - _start_time, 2),
    )


@router.get("/ready")
async def readiness():
    """
    Readiness probe — returns 200 only if the service can handle traffic.

    Unlike /health, this returns a simple 200/503 without details.
    """
    h = await health()
    if h.status != "healthy":
        from fastapi import HTTPException, status
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not ready",
        )
    return {"status": "ready"}
```

The health endpoint checks every dependency and returns a degraded status if any component is unhealthy. This distinction matters for load balancers — a degraded service might still handle queries (Elasticsearch is up) but can't ingest (Kafka is down). The readiness probe (`/ready`) is used by Kubernetes and Cloud Run to decide whether to route traffic to this instance.

---

## Testing

### `tests/conftest.py`

```python
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from app.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI application."""
    return TestClient(app)


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer to avoid real Kafka connections in tests."""
    with patch("app.ingestion.publisher._producer") as mock:
        mock.send = MagicMock()
        mock.flush = MagicMock()
        yield mock


@pytest.fixture
def mock_es_client():
    """Mock Elasticsearch client."""
    with patch("app.storage.elasticsearch._es_client") as mock:
        yield mock


@pytest.fixture
def sample_log():
    """A valid sample log entry."""
    return {
        "service": "payment-api",
        "level": "ERROR",
        "message": "Payment processing failed: timeout after 30s",
        "trace_id": "abc123def456",
        "host": "payment-api-pod-7f8d9",
        "environment": "production",
        "metadata": {
            "user_id": "u_12345",
            "amount": 99.99,
            "currency": "USD",
        },
    }


@pytest.fixture
def sample_log_batch():
    """A batch of sample log entries."""
    return {
        "logs": [
            {
                "service": "payment-api",
                "level": "INFO",
                "message": "Payment processed successfully",
            },
            {
                "service": "payment-api",
                "level": "ERROR",
                "message": "Database connection timeout",
            },
            {
                "service": "user-service",
                "level": "WARNING",
                "message": "Cache miss ratio above 50%",
            },
        ],
    }
```

### `tests/test_ingestion.py`

```python
from unittest.mock import patch, MagicMock

from app.ingestion.validator import enrich_log, validate_batch
from app.schemas import LogEntry


def test_enrich_log_adds_server_metadata():
    """Test that server-side metadata is added to log entries."""
    log = LogEntry(
        service="test-service",
        level="INFO",
        message="Test message",
    )

    enriched = enrich_log(log, source_ip="192.168.1.1")

    assert "_received_at" in enriched
    assert enriched["_source_ip"] == "192.168.1.1"
    assert "_id" in enriched
    assert len(enriched["_id"]) == 20


def test_enrich_log_generates_trace_id_if_missing():
    """Test that a trace ID is generated when not provided."""
    log = LogEntry(
        service="test-service",
        level="INFO",
        message="No trace ID",
    )

    enriched = enrich_log(log)

    assert enriched["trace_id"] is not None
    assert len(enriched["trace_id"]) == 32      # UUID hex


def test_enrich_log_preserves_existing_trace_id():
    """Test that existing trace IDs are not overwritten."""
    log = LogEntry(
        service="test-service",
        level="INFO",
        message="Has trace ID",
        trace_id="existing-trace-123",
    )

    enriched = enrich_log(log)

    assert enriched["trace_id"] == "existing-trace-123"


def test_dedup_id_is_deterministic():
    """Test that the same log always produces the same dedup ID."""
    log = LogEntry(
        service="test-service",
        level="INFO",
        message="Deterministic test",
    )

    enriched1 = enrich_log(log)
    enriched2 = enrich_log(log)

    assert enriched1["_id"] == enriched2["_id"]


def test_validate_batch_separates_valid_and_rejected():
    """Test that batch validation correctly separates valid logs."""
    logs = [
        LogEntry(service="svc-a", level="INFO", message="Valid log 1"),
        LogEntry(service="svc-b", level="ERROR", message="Valid log 2"),
    ]

    valid, rejected = validate_batch(logs)

    assert len(valid) == 2
    assert len(rejected) == 0


def test_ingest_single_returns_202(client, sample_log):
    """Test that single log ingestion returns 202 Accepted."""
    with patch("app.routers.ingest.publish_logs", return_value=1):
        response = client.post("/api/v1/logs", json=sample_log)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert "id" in response.json()


def test_ingest_batch_returns_summary(client, sample_log_batch):
    """Test that batch ingestion returns published/rejected counts."""
    with patch("app.routers.ingest.publish_logs", return_value=3):
        response = client.post(
            "/api/v1/logs/batch", json=sample_log_batch,
        )

    assert response.status_code == 202
    body = response.json()
    assert body["total"] == 3
    assert body["published"] == 3
    assert body["rejected"] == 0


def test_ingest_returns_503_when_kafka_down(client, sample_log):
    """Test that 503 is returned when Kafka is unreachable."""
    with patch("app.routers.ingest.publish_logs", return_value=0):
        response = client.post("/api/v1/logs", json=sample_log)

    assert response.status_code == 503
```

### `tests/test_search.py`

```python
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from app.query.search import search_logs


def test_search_returns_results():
    """Test that search returns formatted results."""
    mock_es_response = {
        "hits": {
            "total": {"value": 2},
            "hits": [
                {
                    "_source": {
                        "timestamp": "2025-01-15T10:00:00Z",
                        "service": "payment-api",
                        "level": "ERROR",
                        "message": "Connection timeout",
                    },
                },
                {
                    "_source": {
                        "timestamp": "2025-01-15T10:00:01Z",
                        "service": "payment-api",
                        "level": "ERROR",
                        "message": "Retry failed",
                    },
                },
            ],
        },
    }

    with patch("app.query.search.get_es_client") as mock_es:
        mock_es.return_value.search.return_value = mock_es_response

        result = search_logs(
            query="timeout",
            service="payment-api",
            level="ERROR",
        )

    assert result["total"] == 2
    assert len(result["logs"]) == 2
    assert result["logs"][0]["service"] == "payment-api"


def test_search_with_trace_id():
    """Test searching logs by distributed trace ID."""
    mock_es_response = {
        "hits": {
            "total": {"value": 5},
            "hits": [
                {
                    "_source": {
                        "timestamp": "2025-01-15T10:00:00Z",
                        "service": "api-gateway",
                        "level": "INFO",
                        "message": "Request received",
                        "trace_id": "trace-abc-123",
                    },
                },
            ],
        },
    }

    with patch("app.query.search.get_es_client") as mock_es:
        mock_es.return_value.search.return_value = mock_es_response

        result = search_logs(trace_id="trace-abc-123")

    assert result["total"] == 5


def test_search_defaults_to_last_24h():
    """Test that search defaults to a 24-hour time range."""
    mock_es_response = {
        "hits": {"total": {"value": 0}, "hits": []},
    }

    with patch("app.query.search.get_es_client") as mock_es:
        mock_es.return_value.search.return_value = mock_es_response
        search_logs()

    call_args = mock_es.return_value.search.call_args
    query = call_args.kwargs.get("body") or call_args[1].get("body")
    range_filter = query["query"]["bool"]["must"][0]["range"]["timestamp"]
    assert "gte" in range_filter
    assert "lte" in range_filter


def test_search_handles_es_failure():
    """Test graceful handling of Elasticsearch errors."""
    with patch("app.query.search.get_es_client") as mock_es:
        mock_es.return_value.search.side_effect = Exception("ES down")

        result = search_logs(query="test")

    assert result["total"] == 0
    assert "error" in result
```

### `tests/test_alerting.py`

```python
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from app.alerting.engine import AlertEngine


def make_es_search_result(total: int):
    """Create a mock Elasticsearch search result."""
    return {
        "hits": {
            "total": {"value": total},
            "hits": [],
        },
    }


def test_alert_fires_when_threshold_exceeded():
    """Test that an alert fires when the value exceeds the threshold."""
    engine = AlertEngine()
    rule = {
        "name": "test-high-errors",
        "query": "count:level=ERROR",
        "condition": "gt",
        "threshold": 100,
        "window_seconds": 300,
        "severity": "warning",
        "channels": ["slack"],
        "enabled": True,
        "cooldown_seconds": 0,
    }

    with patch("app.alerting.engine.get_es_client") as mock_es:
        mock_es.return_value.search.return_value = make_es_search_result(150)

        alert = engine.evaluate_rule(rule)

    assert alert is not None
    assert alert["rule_name"] == "test-high-errors"
    assert alert["current_value"] == 150


def test_alert_does_not_fire_below_threshold():
    """Test that no alert fires when the value is below the threshold."""
    engine = AlertEngine()
    rule = {
        "name": "test-low-errors",
        "query": "count:level=ERROR",
        "condition": "gt",
        "threshold": 100,
        "window_seconds": 300,
        "severity": "warning",
        "channels": ["slack"],
        "enabled": True,
    }

    with patch("app.alerting.engine.get_es_client") as mock_es:
        mock_es.return_value.search.return_value = make_es_search_result(50)

        alert = engine.evaluate_rule(rule)

    assert alert is None


def test_cooldown_prevents_repeat_alerts():
    """Test that cooldown prevents rapid re-firing."""
    engine = AlertEngine()
    rule = {
        "name": "test-cooldown",
        "query": "count:level=ERROR",
        "condition": "gt",
        "threshold": 10,
        "window_seconds": 300,
        "severity": "warning",
        "channels": ["slack"],
        "enabled": True,
        "cooldown_seconds": 300,
    }

    with patch("app.alerting.engine.get_es_client") as mock_es:
        mock_es.return_value.search.return_value = make_es_search_result(50)

        # First evaluation should fire
        alert1 = engine.evaluate_rule(rule)
        assert alert1 is not None

        # Second evaluation should be suppressed (cooldown)
        alert2 = engine.evaluate_rule(rule)
        assert alert2 is None


def test_alert_resolves_when_condition_clears():
    """Test that a firing alert is resolved when the condition clears."""
    engine = AlertEngine()
    rule = {
        "name": "test-resolve",
        "query": "count:level=ERROR",
        "condition": "gt",
        "threshold": 100,
        "window_seconds": 300,
        "severity": "warning",
        "channels": ["slack"],
        "enabled": True,
        "cooldown_seconds": 0,
    }

    with patch("app.alerting.engine.get_es_client") as mock_es:
        # First: fire the alert
        mock_es.return_value.search.return_value = make_es_search_result(150)
        alert = engine.evaluate_rule(rule)
        assert alert is not None
        assert "test-resolve" in engine._firing

        # Second: condition clears
        mock_es.return_value.search.return_value = make_es_search_result(50)
        alert = engine.evaluate_rule(rule)
        assert alert is None
        assert "test-resolve" not in engine._firing


def test_disabled_rules_are_skipped():
    """Test that disabled rules are not evaluated."""
    engine = AlertEngine()
    engine.load_rules([
        {
            "name": "disabled-rule",
            "query": "count:level=ERROR",
            "condition": "gt",
            "threshold": 0,
            "window_seconds": 300,
            "severity": "warning",
            "channels": ["slack"],
            "enabled": False,
        },
    ])

    with patch("app.alerting.engine.get_es_client"):
        alerts = engine.evaluate_all()

    assert len(alerts) == 0
```

### `tests/test_metrics.py`

```python
def test_metrics_endpoint_returns_prometheus_format(client):
    """Test that /metrics returns valid Prometheus text format."""
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "logs_received_total" in response.text
    assert "ingestion_latency_seconds" in response.text


def test_health_endpoint_returns_component_status(client):
    """Test that /health returns status of all components."""
    from unittest.mock import patch

    with patch("app.routers.health._check_component", return_value="healthy"):
        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert "status" in body
    assert "uptime_seconds" in body
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_ingest"                       # Run only ingestion tests
pytest --cov=app --cov-report=html            # Coverage report
```

---

## Deploying to GCP (Cloud Run)

### Prerequisites

```bash
# Install Google Cloud SDK
brew install google-cloud-sdk                 # macOS
# Or download from https://cloud.google.com/sdk/docs/install

gcloud init                                   # Authenticate and set project
gcloud auth configure-docker                  # Configure Docker for GCR
```

### Set Up Cloud SQL (PostgreSQL)

```bash
# Create a Cloud SQL instance
gcloud sql instances create monitoring-db \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1 \
  --storage-size=10GB \
  --storage-auto-increase

# Create the database
gcloud sql databases create monitoring \
  --instance=monitoring-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=monitoring-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-f1-micro` is the smallest instance for development. PostgreSQL stores alert rules, dashboard configurations, and audit logs — not the hot-path log data (that's in Elasticsearch).

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create monitoring-cache \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe monitoring-cache \
  --region=us-central1 \
  --format="value(host)"
```

Redis is used for caching search results, rate limiting the ingestion API, and temporary state for the alert engine cooldown tracking. `--tier=STANDARD` provides a replica for failover.

### Set Up Elasticsearch (Elastic Cloud on GCP)

```bash
# Option 1: Elastic Cloud (managed) — recommended
# Create at https://cloud.elastic.co/
# Choose GCP region: us-central1
# Get the Cloud ID and API key

# Option 2: Self-managed on GKE
gcloud container clusters create monitoring-es \
  --zone=us-central1-a \
  --num-nodes=3 \
  --machine-type=e2-standard-4

# Deploy Elasticsearch via Helm
helm repo add elastic https://helm.elastic.co
helm install elasticsearch elastic/elasticsearch \
  --set replicas=3 \
  --set resources.requests.memory=4Gi
```

Elastic Cloud is recommended for production because it handles upgrades, snapshots, and scaling automatically. Self-managed on GKE gives more control but requires significant operational expertise.

### Set Up Kafka (Confluent Cloud or Managed)

```bash
# Option 1: Confluent Cloud (managed) — recommended
# Create at https://confluent.cloud/
# Get bootstrap servers and API credentials

# Option 2: Google Managed Service for Apache Kafka
gcloud managed-kafka clusters create monitoring-kafka \
  --location=us-central1 \
  --cpu=3 \
  --memory=3GB \
  --subnets=default
```

### Create GCS Bucket for Cold Storage

```bash
gcloud storage buckets create gs://YOUR_PROJECT_ID-monitoring-archive \
  --location=us-central1 \
  --storage-class=NEARLINE \
  --uniform-bucket-level-access

# Set lifecycle policy to delete after 365 days
cat > /tmp/lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 365}
      }
    ]
  }
}
EOF

gcloud storage buckets update gs://YOUR_PROJECT_ID-monitoring-archive \
  --lifecycle-file=/tmp/lifecycle.json
```

`NEARLINE` storage is optimized for data accessed less than once a month, at ~$0.01/GB/month. The lifecycle policy automatically deletes objects after 365 days, matching our retention policy without manual cleanup.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/monitoring/api
```

### Deploy to Cloud Run

```bash
gcloud run deploy monitoring-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/monitoring/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 50 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:monitoring-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/monitoring?host=/cloudsql/YOUR_PROJECT_ID:us-central1:monitoring-db,\
KAFKA_BOOTSTRAP_SERVERS=KAFKA_BOOTSTRAP_SERVERS,\
KAFKA_TOPIC_LOGS=logs-raw,\
ELASTICSEARCH_URL=https://ELASTIC_CLOUD_ENDPOINT,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
GCS_BUCKET=YOUR_PROJECT_ID-monitoring-archive,\
RETENTION_HOT_DAYS=7,\
RETENTION_WARM_DAYS=30,\
RETENTION_COLD_DAYS=365,\
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/xxx/yyy/zzz,\
LOG_LEVEL=INFO"
```

`--min-instances 2` keeps two instances warm for high availability (if one crashes, the other handles traffic). `--max-instances 50` allows scaling to 50 instances during log spikes. `--memory 1Gi` and `--cpu 2` give each instance enough resources for concurrent log ingestion and Kafka publishing.

### Deploy the Indexer Worker

```bash
# The indexer runs as a long-lived process (not request-driven)
# Deploy as a Cloud Run Job or GKE Deployment

# Option: Cloud Run Job (for batch-style)
gcloud run jobs create monitoring-indexer \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/monitoring/api \
  --command "python" \
  --args "-m,workers.indexer" \
  --region us-central1 \
  --memory 2Gi \
  --cpu 2 \
  --max-retries 3 \
  --set-env-vars "\
KAFKA_BOOTSTRAP_SERVERS=KAFKA_BOOTSTRAP_SERVERS,\
ELASTICSEARCH_URL=https://ELASTIC_CLOUD_ENDPOINT,\
KAFKA_CONSUMER_GROUP=log-indexers"
```

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis) and Kafka
gcloud compute networks vpc-access connectors create monitoring-vpc-connector \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update monitoring-api \
  --vpc-connector=monitoring-vpc-connector \
  --region=us-central1
```

---

## Scaling & Production Considerations

### Kafka Scaling

| Strategy               | When to Use                                | How                                              |
| ---------------------- | ------------------------------------------ | ------------------------------------------------ |
| Add Partitions         | Single partition can't handle 2 M msgs/sec | Increase to 50-100 partitions per topic           |
| Consumer Groups        | One indexer can't keep up                  | Run 50 indexer instances (one per partition)       |
| Multi-Topic            | Different retention per log level           | `logs-error` (30d), `logs-info` (7d), `logs-debug` (1d) |
| Tiered Storage         | Kafka retention exceeds local disk         | Enable Confluent Tiered Storage → GCS             |
| Compression            | Network is the bottleneck                  | Use LZ4 (fastest) or ZSTD (best ratio)           |

### Elasticsearch Scaling

| Strategy               | When to Use                                | How                                              |
| ---------------------- | ------------------------------------------ | ------------------------------------------------ |
| Hot-Warm-Cold Nodes    | 175 TB hot storage is too expensive         | Hot (SSD), Warm (HDD), Cold (frozen)             |
| Index Sharding         | Single shard query latency > 2 seconds     | 5 shards per index, 1 shard ≈ 50 GB max          |
| Index Lifecycle Mgmt   | Manual retention is error-prone            | ILM policy: hot(7d) → warm(30d) → delete          |
| Cross-Cluster Search   | Single cluster can't hold all data         | Federated search across regional clusters         |
| Rollover Indices       | Daily indices too large or too small       | Rollover at 50 GB or 1 day, whichever comes first |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ logs.xyz       │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud Armor    │
                     │ (DDoS + WAF +  │
                     │  rate limit)   │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Global Load   │
                     │ Balancer      │
                     └──────┬─────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
       ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
       │ Cloud Run   │ │Cloud   │ │ Cloud Run   │
       │ Ingestion   │ │Run #2  │ │ Ingestion   │
       │ API (x50)   │ │        │ │ API (xN)    │
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │         Kafka Cluster              │
         │    (50 partitions, 3 brokers)      │
         │    Confluent Cloud / GKE            │
         └──────┬──────────────┬──────────────┘
                │              │
     ┌──────────▼──┐    ┌──────▼──────────────┐
     │  Indexer    │    │   Stream Processor  │
     │  Workers    │    │   (Flink / Dataflow)│
     │  (x50, one  │    │   Real-time agg,    │
     │   per part) │    │   anomaly detection │
     └──────┬──────┘    └──────┬──────────────┘
            │                   │
     ┌──────▼──────────────────▼───────────────┐
     │            VPC Network                 │
     │                                         │
     │  ┌──────────────────────────────────┐  │
     │  │    Elasticsearch Cluster         │  │
     │  │    (Elastic Cloud on GCP)        │  │
     │  │                                  │  │
     │  │  Hot Nodes (x6, SSD, 7 days)     │  │
     │  │  Warm Nodes (x4, HDD, 30 days)   │  │
     │  │  Frozen (x2, GCS-backed, 365d)   │  │
     │  └──────────────────────────────────┘  │
     │                                         │
     │  ┌──────────┐  ┌──────────────────┐    │
     │  │ GCS Cold │  │  Cloud SQL       │    │
     │  │ Archive  │  │  (Rules, Config) │    │
     │  │ (NEARLINE)│ │  + BigQuery for  │    │
     │  │          │  │  archive queries │    │
     │  └──────────┘  └──────────────────┘    │
     │                                         │
     │  ┌──────────┐  ┌──────────────────┐    │
     │  │Memorystore│ │  Grafana Cloud   │    │
     │  │ Redis    │  │  (Dashboards)    │    │
     │  └──────────┘  └──────────────────┘    │
     └─────────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud Armor** provides L7 DDoS protection and rate limiting on the ingestion endpoint itself, preventing a misbehaving service from flooding the pipeline.
- **50 Kafka partitions** with one indexer per partition enables 50× parallel indexing. Each indexer independently consumes and indexes its partition, achieving aggregate throughput of 2 M+ events/sec.
- **Stream Processor (Flink/Dataflow)** sits alongside the indexers, computing real-time aggregations (error rate per service per minute) and anomaly detection (sudden spike in ERROR logs) without querying Elasticsearch.
- **Elasticsearch Hot-Warm-Cold** architecture: hot nodes use SSDs for the latest 7 days, warm nodes use HDDs for days 8-30, frozen nodes use GCS-backed storage for days 31-365 (searchable but slow). This reduces storage costs by 10×.
- **BigQuery** enables SQL queries over the GCS cold archive for compliance and forensic analysis without restoring data to Elasticsearch.

### Cost Estimation (at scale)

```
Component                     Monthly Cost (approx)
──────────────────────────────────────────────────
Kafka (Confluent Cloud)       $3,000 - $8,000
Elasticsearch (Elastic Cloud) $5,000 - $15,000
  Hot nodes (6 × 64GB SSD)    $8,000
  Warm nodes (4 × 128GB HDD)  $4,000
  Frozen (GCS-backed)         $500
GCS Cold Storage (750 TB)     $7,500
Cloud Run (50 instances)      $2,000 - $5,000
Cloud SQL (PostgreSQL)        $100 - $500
Memorystore (Redis)           $200 - $500
Grafana Cloud                 $500 - $2,000
BigQuery (ad-hoc queries)     $100 - $1,000
──────────────────────────────────────────────────
Total                         ~$20,000 - $40,000/month
```

At 50 B logs/day, Elasticsearch dominates the cost. The hot-warm-cold architecture is the single most impactful cost optimization — keeping only 7 days on expensive SSDs vs. 365 days saves ~$100 K/month.

### Security Checklist

```
✓ Authenticate the ingestion API (API keys or mTLS between services)
✓ Encrypt logs in transit (TLS everywhere) and at rest (ES + GCS encryption)
✓ Implement RBAC — teams can only query their own services' logs
✓ Mask PII in logs (credit cards, SSNs, passwords) at ingestion time
✓ Audit log access — record who searched for what and when
✓ Use VPC Service Controls to prevent data exfiltration
✓ Store secrets in Secret Manager, not environment variables
✓ Enable Cloud Audit Logs for compliance (SOC 2, HIPAA)
✓ Set up retention policies to auto-delete old data (GDPR right to deletion)
✓ Monitor for log injection attacks (malicious content in log messages)
```

### Monitoring the Monitoring System

```bash
# Create uptime checks for the monitoring system itself
gcloud monitoring uptime create monitoring-health \
  --display-name="Monitoring System Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Kafka consumer lag > 100K for 10 minutes (indexing falling behind)
# - Ingestion API p99 latency > 500ms (pipeline slowdown)
# - Elasticsearch cluster health != green (data loss risk)
# - Zero logs received for 10 minutes (pipeline down)
# - GCS archive upload failures (cold storage broken)
# - Alert engine evaluation failures (alerts not firing)
# - Disk usage on ES nodes > 80% (need more storage or faster archiving)
# - Cloud Run instance count near max-instances (need more capacity)
```

The most important meta-alert is "no logs received in 10 minutes." If the monitoring system itself is down, you won't know about problems in any other service. Use Cloud Monitoring's built-in uptime checks (external to your infrastructure) to monitor the monitoring system — this avoids the circular dependency of a system monitoring itself.
