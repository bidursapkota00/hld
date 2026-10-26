# Design Notification System

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [Notification Templates & Rendering](#notification-templates--rendering)
6. [Channel Providers (Email, SMS, Push)](#channel-providers-email-sms-push)
7. [Priority Queue & Worker Pipeline](#priority-queue--worker-pipeline)
8. [Rate Limiting & Throttling](#rate-limiting--throttling)
9. [User Preferences & Routing](#user-preferences--routing)
10. [Core API – Notifications](#core-api--notifications)
11. [Real-Time Delivery (WebSockets)](#real-time-delivery-websockets)
12. [Retry & Dead Letter Queue](#retry--dead-letter-queue)
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
| Multi-Channel Delivery  | Send notifications via email, SMS, and push in a single API call        |
| Template System         | Define reusable templates with variable substitution (Jinja2)           |
| User Preferences        | Users opt in/out of channels and notification types                     |
| Priority Levels         | Critical (OTP, fraud), high (orders), medium (promos), low (digests)    |
| Rate Limiting           | Per-user and per-channel throttling to prevent spam                     |
| Delivery Tracking       | Track sent, delivered, failed, opened status per notification           |
| Scheduling              | Send notifications at a specific time or after a delay                  |
| Batch Sending           | Send to millions of users for campaigns and announcements               |
| Retry Logic             | Auto-retry failed deliveries with exponential backoff                   |
| Real-Time In-App        | Push notifications to connected clients via WebSockets                  |

### Non-Functional Requirements

| Requirement      | Target                                                    |
| ---------------- | --------------------------------------------------------- |
| Availability     | 99.99% uptime (OTPs and fraud alerts are critical)        |
| Latency          | Critical notifications delivered in < 3 seconds           |
| Throughput       | 1 M notifications/minute at peak                          |
| Ordering         | At-least-once delivery; idempotent processing             |
| Durability       | No notification record should ever be lost                |
| Scalability      | Handle 10 B notifications/day across all channels         |

### Back-of-the-Envelope Estimations

```
Notifications per day              ≈ 10 B
Notifications per second           ≈ 10 B / 86400 ≈ 115 K RPS (avg), ~500 K RPS (peak)
Channel split                      ≈ 60% push, 25% email, 10% SMS, 5% in-app
Push notifications/sec             ≈ 300 K (peak)
Emails/sec                         ≈ 125 K (peak)
SMS/sec                            ≈ 50 K (peak)
Notification record size           ≈ 500 B
Storage per day                    ≈ 10 B × 500 B = 5 TB/day
Storage per year (with retention)  ≈ 5 TB × 90 days retention = 450 TB
Template storage                   ≈ 10 K templates × 5 KB = 50 MB (negligible)
Worker queue depth (burst)         ≈ 5 M messages (Redis/Pub-Sub buffer)
```

500 K notifications per second is far beyond what a single database or queue can handle. The architecture uses Redis as a priority queue for buffering, with dedicated worker pools per channel. PostgreSQL stores notification metadata and delivery status for audit. Batch campaigns are processed by fan-out workers that expand a campaign into individual notifications. Critical notifications (OTPs) bypass the queue and are sent directly for sub-3-second delivery.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │(API / Admin) │
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │        API Servers          │
                │   (FastAPI on Cloud Run)    │
                │                             │
                │  POST /notifications/send   │
                │  POST /notifications/batch  │
                │  GET  /notifications/{id}   │
                │  WS   /ws/live/{user_id}    │
                └──┬──────────┬───────────┬───┘
                   │          │           │
            ┌──────▼───┐ ┌───▼────┐ ┌────▼──────┐
            │  Redis   │ │Postgre-│ │  Pub/Sub  │
            │ (Queue + │ │  SQL   │ │  (Fan-Out │
            │  Rate    │ │(Records│ │  & Events)│
            │  Limit)  │ │& Prefs)│ │           │
            └────┬─────┘ └────────┘ └─────┬─────┘
                 │                        │
        ┌────────▼────────────────────────▼────────┐
        │              Worker Pools                │
        │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
        │  │  Email    │ │   SMS    │ │   Push   │ │
        │  │  Workers  │ │  Workers │ │  Workers │ │
        │  │(SendGrid) │ │ (Twilio) │ │  (FCM)   │ │
        │  └──────────┘ └──────────┘ └──────────┘ │
        └──────────────────────────────────────────┘
```

The API server receives notification requests and enqueues them into Redis priority queues. A router inspects user preferences and splits each notification into channel-specific jobs (email, SMS, push). Dedicated worker pools dequeue jobs and deliver them through third-party providers: SendGrid for email, Twilio for SMS, Firebase Cloud Messaging (FCM) for push. PostgreSQL stores notification records, user preferences, and delivery status. Pub/Sub handles fan-out for batch campaigns (expanding "send to all premium users" into individual notifications). WebSockets deliver real-time in-app notifications to connected clients.

---

## Project Setup

### Project Structure

```
notifications/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── cache.py              # Redis client, queue, and rate limiter
│   ├── templates.py          # Template rendering engine (Jinja2)
│   ├── providers/
│   │   ├── __init__.py
│   │   ├── base.py           # Abstract provider interface
│   │   ├── email.py          # Email provider (SendGrid)
│   │   ├── sms.py            # SMS provider (Twilio)
│   │   └── push.py           # Push provider (FCM)
│   ├── queue.py              # Priority queue and worker logic
│   ├── rate_limiter.py       # Per-user and per-channel rate limiting
│   ├── router_engine.py      # Notification routing (preferences + channels)
│   ├── retry.py              # Retry logic and dead letter queue
│   ├── websocket_manager.py  # WebSocket connection manager
│   └── routers/
│       ├── __init__.py
│       ├── notifications.py  # Send and query notification endpoints
│       ├── templates.py      # Template CRUD endpoints
│       ├── preferences.py    # User preference endpoints
│       └── live.py           # WebSocket real-time delivery
├── worker.py                 # Background worker process entry point
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_templates.py     # Tests for template rendering
│   ├── test_rate_limiter.py  # Tests for rate limiting
│   ├── test_routing.py       # Tests for notification routing
│   └── test_notifications.py # Tests for send and query
├── Dockerfile
├── Dockerfile.worker
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir notifications && cd notifications
python -m venv venv
source venv/bin/activate              # macOS/Linux
```

### `requirements.txt`

```text
fastapi[standard]==0.115.0
sqlalchemy==2.0.35
psycopg2-binary==2.9.10
alembic==1.13.3
redis==5.1.1
python-dotenv==1.0.1
httpx==0.27.2
Jinja2==3.1.4
celery==5.4.0
firebase-admin==6.6.0
sendgrid==6.11.0
twilio==9.3.4
websockets==13.1
croniter==3.0.3
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for the priority queue and rate limiter. `Jinja2` renders notification templates with variable substitution. `celery` is a distributed task queue for processing notification jobs across worker pools. `firebase-admin` is Google's SDK for sending push notifications via FCM. `sendgrid` is the official Python client for the SendGrid email API. `twilio` is the official Python client for the Twilio SMS API. `croniter` parses cron expressions for scheduled notifications.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/notifications
REDIS_URL=redis://localhost:6379/0
BASE_URL=http://localhost:8000
SENDGRID_API_KEY=SG.xxxxx
SENDGRID_FROM_EMAIL=noreply@example.com
TWILIO_ACCOUNT_SID=ACxxxxx
TWILIO_AUTH_TOKEN=xxxxx
TWILIO_FROM_NUMBER=+15551234567
FCM_CREDENTIALS_PATH=./firebase-credentials.json
RATE_LIMIT_EMAIL_PER_HOUR=50
RATE_LIMIT_SMS_PER_HOUR=20
RATE_LIMIT_PUSH_PER_HOUR=100
RATE_LIMIT_USER_PER_MINUTE=30
MAX_RETRY_ATTEMPTS=5
RETRY_BASE_DELAY=60
DLQ_MAX_AGE_HOURS=72
CRITICAL_BYPASS_QUEUE=true
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `SENDGRID_API_KEY` authenticates with the SendGrid email API. `TWILIO_ACCOUNT_SID` and `TWILIO_AUTH_TOKEN` authenticate with the Twilio SMS API. `FCM_CREDENTIALS_PATH` points to the Firebase service account JSON file. `RATE_LIMIT_EMAIL_PER_HOUR` caps emails per user to prevent spam. `RATE_LIMIT_USER_PER_MINUTE` is a global per-user cap across all channels. `MAX_RETRY_ATTEMPTS` is the maximum number of retry attempts for failed deliveries. `RETRY_BASE_DELAY` is the base delay in seconds for exponential backoff. `DLQ_MAX_AGE_HOURS` is how long failed notifications stay in the dead letter queue before being discarded. `CRITICAL_BYPASS_QUEUE` when true, sends critical priority notifications (OTPs) directly without queuing.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│          notifications               │     │      notification_templates          │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            UUID       PRIMARY KEY │     │ id            UUID       PRIMARY KEY │
│ user_id       UUID       NOT NULL    │     │ name          VARCHAR    UNIQUE      │
│ type          VARCHAR    NOT NULL    │     │ slug          VARCHAR    UNIQUE      │
│ channel       ENUM       NOT NULL    │     │ description   TEXT                   │
│ priority      ENUM       NOT NULL    │     │ channel       ENUM       NOT NULL    │
│ template_id   UUID       FK → tmpls  │     │ subject       TEXT                   │
│ subject       TEXT                   │     │ body          TEXT       NOT NULL    │
│ body          TEXT       NOT NULL    │     │ variables     JSONB                  │
│ metadata      JSONB                  │     │ is_active     BOOLEAN    DEFAULT T   │
│ status        ENUM       DEFAULT Q   │     │ created_at    TIMESTAMPTZ            │
│ provider_id   VARCHAR               │     │ updated_at    TIMESTAMPTZ            │
│ error_message TEXT                   │     ├──────────────────────────────────────┤
│ retry_count   INTEGER    DEFAULT 0   │     │ INDEX idx_tmpl_slug (slug)           │
│ scheduled_at  TIMESTAMPTZ           │     │ INDEX idx_tmpl_channel (channel)     │
│ sent_at       TIMESTAMPTZ           │     └──────────────────────────────────────┘
│ delivered_at  TIMESTAMPTZ           │
│ opened_at     TIMESTAMPTZ           │     ┌──────────────────────────────────────┐
│ failed_at     TIMESTAMPTZ           │     │        user_preferences              │
│ created_at    TIMESTAMPTZ           │     ├──────────────────────────────────────┤
│ idempotency_key VARCHAR  UNIQUE     │     │ id            BIGSERIAL  PRIMARY KEY │
├──────────────────────────────────────┤     │ user_id       UUID       NOT NULL    │
│ INDEX idx_user (user_id)             │     │ channel       ENUM       NOT NULL    │
│ INDEX idx_status (status)            │     │ notif_type    VARCHAR    NOT NULL    │
│ INDEX idx_created (created_at)       │     │ enabled       BOOLEAN    DEFAULT T   │
│ INDEX idx_idempotency (idemp_key)    │     │ quiet_start   TIME                   │
│ INDEX idx_scheduled (scheduled_at)   │     │ quiet_end     TIME                   │
└──────────────────────────────────────┘     │ updated_at    TIMESTAMPTZ            │
                                             ├──────────────────────────────────────┤
┌──────────────────────────────────────┐     │ UNIQUE (user_id, channel, notif_type)│
│        delivery_attempts             │     │ INDEX idx_user_prefs (user_id)       │
├──────────────────────────────────────┤     └──────────────────────────────────────┘
│ id            BIGSERIAL  PRIMARY KEY │
│ notification_id UUID     FK → notifs │     ┌──────────────────────────────────────┐
│ attempt_number  INTEGER  NOT NULL    │     │           campaigns                  │
│ channel       ENUM       NOT NULL    │     ├──────────────────────────────────────┤
│ provider      VARCHAR    NOT NULL    │     │ id            UUID       PRIMARY KEY │
│ provider_id   VARCHAR               │     │ name          VARCHAR    NOT NULL    │
│ status        ENUM       NOT NULL    │     │ template_id   UUID       FK → tmpls  │
│ error_code    VARCHAR               │     │ target_query  TEXT       NOT NULL    │
│ error_message TEXT                   │     │ variables     JSONB                  │
│ latency_ms    INTEGER               │     │ priority      ENUM       DEFAULT MED │
│ attempted_at  TIMESTAMPTZ           │     │ status        ENUM       DEFAULT DFT │
├──────────────────────────────────────┤     │ total_count   INTEGER    DEFAULT 0   │
│ INDEX idx_notif_attempts (notif_id)  │     │ sent_count    INTEGER    DEFAULT 0   │
│ INDEX idx_attempt_status (status)    │     │ failed_count  INTEGER    DEFAULT 0   │
└──────────────────────────────────────┘     │ scheduled_at  TIMESTAMPTZ            │
                                             │ started_at    TIMESTAMPTZ            │
                                             │ completed_at  TIMESTAMPTZ            │
                                             │ created_at    TIMESTAMPTZ            │
                                             ├──────────────────────────────────────┤
                                             │ INDEX idx_campaign_status (status)   │
                                             └──────────────────────────────────────┘
```

The `notifications` table is the core entity, storing every notification sent through the system with its delivery status and timestamps for each lifecycle stage (queued → sent → delivered → opened). `idempotency_key` prevents duplicate sends: if the same API call is retried, the existing notification is returned instead of creating a duplicate. `delivery_attempts` logs every attempt to deliver a notification, enabling debugging and SLA tracking (`latency_ms` measures provider response time). `user_preferences` stores per-user, per-channel, per-notification-type opt-in/out settings and quiet hours. `notification_templates` stores reusable Jinja2 templates with declared variables. `campaigns` manages batch sends to user segments.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/notifications"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    sendgrid_api_key: str = ""
    sendgrid_from_email: str = "noreply@example.com"
    twilio_account_sid: str = ""
    twilio_auth_token: str = ""
    twilio_from_number: str = ""
    fcm_credentials_path: str = "./firebase-credentials.json"
    rate_limit_email_per_hour: int = 50
    rate_limit_sms_per_hour: int = 20
    rate_limit_push_per_hour: int = 100
    rate_limit_user_per_minute: int = 30
    max_retry_attempts: int = 5
    retry_base_delay: int = 60
    dlq_max_age_hours: int = 72
    critical_bypass_queue: bool = True

    class Config:
        env_file = ".env"


settings = Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads values from environment variables and `.env` files. Field names are case-insensitive and map to the corresponding `ENV_VAR` names. The `Config` inner class tells Pydantic to also load from the `.env` file.

### `app/database.py`

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings

engine = create_engine(
    settings.database_url,
    pool_size=20,                         # Max persistent connections
    max_overflow=10,                      # Extra connections when pool is full
    pool_pre_ping=True,                   # Test connections before using them
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


def get_db():
    """Dependency that provides a database session per request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

`pool_size=20` keeps 20 connections open and reusable, avoiding the overhead of creating a new connection per request. `max_overflow=10` allows up to 10 additional connections under burst traffic. `pool_pre_ping=True` sends a lightweight query before each checkout to detect stale connections, which is important for cloud databases that may close idle connections. `get_db()` is a FastAPI dependency that yields a session and ensures it is closed after the request, even if an error occurs.

### `app/models.py`

```python
import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Enum, Float,
    ForeignKey, Index, Integer, String, Text, Time, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database import Base


class Channel(str, enum.Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    IN_APP = "in_app"


class Priority(str, enum.Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class NotificationStatus(str, enum.Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    SENT = "sent"
    DELIVERED = "delivered"
    OPENED = "opened"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptStatus(str, enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"


class CampaignStatus(str, enum.Enum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class NotificationTemplate(Base):
    __tablename__ = "notification_templates"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), unique=True, nullable=False)
    slug = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    channel = Column(Enum(Channel), nullable=False)
    subject = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    variables = Column(JSONB, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    notifications = relationship("Notification", back_populates="template")
    campaigns = relationship("Campaign", back_populates="template")

    __table_args__ = (
        Index("idx_tmpl_slug", "slug"),
        Index("idx_tmpl_channel", "channel"),
    )


class Notification(Base):
    __tablename__ = "notifications"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    type = Column(String(100), nullable=False)
    channel = Column(Enum(Channel), nullable=False)
    priority = Column(Enum(Priority), nullable=False, default=Priority.MEDIUM)
    template_id = Column(UUID(as_uuid=True), ForeignKey("notification_templates.id"), nullable=True)
    subject = Column(Text, nullable=True)
    body = Column(Text, nullable=False)
    metadata_ = Column("metadata", JSONB, nullable=True)
    status = Column(Enum(NotificationStatus), nullable=False, default=NotificationStatus.QUEUED)
    provider_id = Column(String(255), nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    opened_at = Column(DateTime(timezone=True), nullable=True)
    failed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    idempotency_key = Column(String(255), unique=True, nullable=True)

    template = relationship("NotificationTemplate", back_populates="notifications")
    attempts = relationship("DeliveryAttempt", back_populates="notification")

    __table_args__ = (
        Index("idx_user", "user_id"),
        Index("idx_status", "status"),
        Index("idx_created", "created_at"),
        Index("idx_idempotency", "idempotency_key"),
        Index("idx_scheduled", "scheduled_at"),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    notification_id = Column(UUID(as_uuid=True), ForeignKey("notifications.id"), nullable=False)
    attempt_number = Column(Integer, nullable=False)
    channel = Column(Enum(Channel), nullable=False)
    provider = Column(String(50), nullable=False)
    provider_id = Column(String(255), nullable=True)
    status = Column(Enum(AttemptStatus), nullable=False)
    error_code = Column(String(50), nullable=True)
    error_message = Column(Text, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    attempted_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    notification = relationship("Notification", back_populates="attempts")

    __table_args__ = (
        Index("idx_notif_attempts", "notification_id"),
        Index("idx_attempt_status", "status"),
    )


class UserPreference(Base):
    __tablename__ = "user_preferences"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    channel = Column(Enum(Channel), nullable=False)
    notif_type = Column(String(100), nullable=False)
    enabled = Column(Boolean, default=True)
    quiet_start = Column(Time, nullable=True)
    quiet_end = Column(Time, nullable=True)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint("user_id", "channel", "notif_type", name="uq_user_channel_type"),
        Index("idx_user_prefs", "user_id"),
    )


class Campaign(Base):
    __tablename__ = "campaigns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    template_id = Column(UUID(as_uuid=True), ForeignKey("notification_templates.id"), nullable=False)
    target_query = Column(Text, nullable=False)
    variables = Column(JSONB, nullable=True)
    priority = Column(Enum(Priority), default=Priority.MEDIUM)
    status = Column(Enum(CampaignStatus), default=CampaignStatus.DRAFT)
    total_count = Column(Integer, default=0)
    sent_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    scheduled_at = Column(DateTime(timezone=True), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    template = relationship("NotificationTemplate", back_populates="campaigns")

    __table_args__ = (
        Index("idx_campaign_status", "status"),
    )
```

`UUID(as_uuid=True)` stores UUIDs natively in PostgreSQL, which is more compact than strings and prevents enumeration attacks. `NotificationStatus` tracks the full lifecycle: `QUEUED → PROCESSING → SENT → DELIVERED → OPENED`. `idempotency_key` is unique and nullable: when provided, it prevents duplicate processing if the same API call is retried. `JSONB` for `metadata` allows flexible data like recipient email addresses, phone numbers, device tokens, and tracking pixels without altering the table schema. `DeliveryAttempt` logs every attempt with `latency_ms` for SLA monitoring. `UserPreference` uses a composite unique constraint on `(user_id, channel, notif_type)` to ensure one preference row per combination. `quiet_start` and `quiet_end` define Do-Not-Disturb hours per channel. `Campaign.target_query` stores a serialized user filter (e.g., `{"country": "US", "plan": "premium"}`) that the fan-out worker expands into individual user IDs.

### `app/schemas.py`

```python
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class NotificationSend(BaseModel):
    user_id: uuid.UUID
    type: str = Field(min_length=1, max_length=100)
    channels: list[str] = Field(default=["push"])
    priority: str = Field("medium", pattern="^(critical|high|medium|low)$")
    template_slug: str | None = None
    subject: str | None = None
    body: str | None = None
    variables: dict | None = None
    metadata: dict | None = None
    scheduled_at: datetime | None = None
    idempotency_key: str | None = None


class NotificationResponse(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    type: str
    channel: str
    priority: str
    subject: str | None
    body: str
    status: str
    provider_id: str | None
    retry_count: int
    created_at: datetime
    sent_at: datetime | None
    delivered_at: datetime | None

    class Config:
        from_attributes = True


class NotificationBatch(BaseModel):
    user_ids: list[uuid.UUID] = Field(min_length=1, max_length=1000)
    type: str = Field(min_length=1, max_length=100)
    channels: list[str] = Field(default=["push"])
    priority: str = Field("medium", pattern="^(critical|high|medium|low)$")
    template_slug: str | None = None
    subject: str | None = None
    body: str | None = None
    variables: dict | None = None


class TemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    slug: str = Field(min_length=1, max_length=255)
    description: str | None = None
    channel: str = Field(pattern="^(email|sms|push|in_app)$")
    subject: str | None = None
    body: str = Field(min_length=1)
    variables: list[str] | None = None


class TemplateResponse(BaseModel):
    id: uuid.UUID
    name: str
    slug: str
    description: str | None
    channel: str
    subject: str | None
    body: str
    variables: list[str] | None
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class TemplateRender(BaseModel):
    template_slug: str
    variables: dict


class PreferenceUpdate(BaseModel):
    channel: str = Field(pattern="^(email|sms|push|in_app)$")
    notif_type: str = Field(min_length=1, max_length=100)
    enabled: bool
    quiet_start: str | None = None
    quiet_end: str | None = None


class PreferenceResponse(BaseModel):
    channel: str
    notif_type: str
    enabled: bool
    quiet_start: str | None
    quiet_end: str | None

    class Config:
        from_attributes = True


class CampaignCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    template_slug: str
    target_query: dict
    variables: dict | None = None
    priority: str = Field("medium", pattern="^(critical|high|medium|low)$")
    scheduled_at: datetime | None = None


class CampaignResponse(BaseModel):
    id: uuid.UUID
    name: str
    status: str
    total_count: int
    sent_count: int
    failed_count: int
    scheduled_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None

    class Config:
        from_attributes = True


class DeliveryStatus(BaseModel):
    notification_id: uuid.UUID
    status: str
    attempts: list[dict]
    created_at: datetime
    sent_at: datetime | None
    delivered_at: datetime | None
    opened_at: datetime | None
```

`NotificationSend` accepts a list of `channels` so a single API call can send across email, SMS, and push simultaneously. The system creates one `Notification` record per channel. `priority` uses a regex pattern to restrict values. `idempotency_key` is optional but recommended for critical notifications: if the client retries, the server checks for an existing notification with the same key and returns it. `NotificationBatch` accepts up to 1000 user IDs for targeted batch sends. `CampaignCreate.target_query` is a JSON filter that the fan-out worker evaluates against the user database.

---

## Notification Templates & Rendering

### `app/templates.py`

```python
from jinja2 import Environment, BaseLoader, TemplateSyntaxError, UndefinedError
from sqlalchemy.orm import Session

from app.models import NotificationTemplate


# ─── Jinja2 Environment ──────────────────────────────────────────

jinja_env = Environment(
    loader=BaseLoader(),
    autoescape=True,                          # Prevent XSS in HTML emails
    undefined=None,                           # Will be set per-render
)


# ─── Template Rendering ──────────────────────────────────────────

def render_template(
    template_slug: str,
    variables: dict,
    db: Session,
) -> dict:
    """
    Render a notification template with the given variables.

    Returns:
        {"subject": "...", "body": "..."} with variables substituted.
    """
    template = (
        db.query(NotificationTemplate)
        .filter(
            NotificationTemplate.slug == template_slug,
            NotificationTemplate.is_active.is_(True),
        )
        .first()
    )

    if not template:
        raise ValueError(f"Template '{template_slug}' not found or inactive.")

    # Validate required variables
    declared_vars = template.variables or []
    missing = [v for v in declared_vars if v not in variables]
    if missing:
        raise ValueError(f"Missing template variables: {', '.join(missing)}")

    # Render body
    try:
        body_tmpl = jinja_env.from_string(template.body)
        rendered_body = body_tmpl.render(**variables)
    except (TemplateSyntaxError, UndefinedError) as e:
        raise ValueError(f"Template rendering failed: {e}")

    # Render subject (if exists)
    rendered_subject = None
    if template.subject:
        try:
            subject_tmpl = jinja_env.from_string(template.subject)
            rendered_subject = subject_tmpl.render(**variables)
        except (TemplateSyntaxError, UndefinedError) as e:
            raise ValueError(f"Subject rendering failed: {e}")

    return {
        "subject": rendered_subject,
        "body": rendered_body,
        "channel": template.channel.value,
    }


# ─── Template Validation ─────────────────────────────────────────

def validate_template(body: str, subject: str | None = None) -> list[str]:
    """
    Validate a template's syntax without rendering.
    Returns a list of errors (empty if valid).
    """
    errors = []

    try:
        jinja_env.parse(body)
    except TemplateSyntaxError as e:
        errors.append(f"Body syntax error: {e}")

    if subject:
        try:
            jinja_env.parse(subject)
        except TemplateSyntaxError as e:
            errors.append(f"Subject syntax error: {e}")

    return errors


def extract_variables(body: str, subject: str | None = None) -> list[str]:
    """
    Extract variable names referenced in a template.
    Used to auto-populate the 'variables' field when creating templates.
    """
    from jinja2 import meta

    variables = set()

    ast = jinja_env.parse(body)
    variables.update(meta.find_undeclared_variables(ast))

    if subject:
        ast = jinja_env.parse(subject)
        variables.update(meta.find_undeclared_variables(ast))

    return sorted(variables)
```

Jinja2 is the template engine used by Flask and Ansible, chosen for its security (`autoescape=True` prevents XSS in HTML emails) and familiarity. Templates use `{{ variable }}` syntax for substitution and `{% if %}` / `{% for %}` for logic. `render_template` first validates that all declared variables are provided, preventing partially rendered notifications. `validate_template` parses the template without rendering to catch syntax errors early, used in the template creation endpoint. `extract_variables` uses Jinja2's AST parser to discover referenced variables automatically. Example template body: `"Hello {{ name }}, your order #{{ order_id }} has been shipped!"`.

---

## Channel Providers (Email, SMS, Push)

### `app/providers/base.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class DeliveryResult:
    """Result of a delivery attempt."""
    success: bool
    provider_id: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    latency_ms: int = 0


class BaseProvider(ABC):
    """Abstract base class for notification providers."""

    @abstractmethod
    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
        metadata: dict | None = None,
    ) -> DeliveryResult:
        """Send a notification and return the result."""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Return the provider name for logging."""
        pass
```

The abstract base class defines a contract that all providers must implement. `DeliveryResult` captures success/failure, the provider's message ID (for tracking), error details, and latency. This uniform interface lets the routing engine swap providers without changing business logic. In production, you would implement multiple providers per channel (e.g., SendGrid + SES for email) for failover.

### `app/providers/email.py`

```python
import time

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from app.config import settings
from app.providers.base import BaseProvider, DeliveryResult


class SendGridProvider(BaseProvider):
    """Email delivery via SendGrid API."""

    def __init__(self):
        self.client = SendGridAPIClient(api_key=settings.sendgrid_api_key)
        self.from_email = settings.sendgrid_from_email

    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
        metadata: dict | None = None,
    ) -> DeliveryResult:
        """
        Send an email via SendGrid.

        Args:
            recipient: Email address (e.g., "user@example.com")
            subject: Email subject line
            body: Email body (HTML or plain text)
            metadata: Optional tracking data
        """
        start = time.time()

        message = Mail(
            from_email=Email(self.from_email),
            to_emails=To(recipient),
            subject=subject or "Notification",
            html_content=Content("text/html", body),
        )

        # Add tracking metadata as custom headers
        if metadata:
            for key, value in metadata.items():
                message.header({f"X-Custom-{key}": str(value)})

        try:
            response = self.client.send(message)
            latency = int((time.time() - start) * 1000)

            if response.status_code in (200, 201, 202):
                return DeliveryResult(
                    success=True,
                    provider_id=response.headers.get("X-Message-Id"),
                    latency_ms=latency,
                )
            else:
                return DeliveryResult(
                    success=False,
                    error_code=str(response.status_code),
                    error_message=response.body.decode() if response.body else "Unknown error",
                    latency_ms=latency,
                )

        except Exception as e:
            latency = int((time.time() - start) * 1000)
            return DeliveryResult(
                success=False,
                error_code="EXCEPTION",
                error_message=str(e),
                latency_ms=latency,
            )

    def get_provider_name(self) -> str:
        return "sendgrid"
```

SendGrid accepts up to 10,000 emails per API call in batch mode, but here we send one at a time for individual tracking. Status codes 200/201/202 indicate success (202 means accepted for delivery). The `X-Message-Id` header is SendGrid's unique ID for tracking delivery status via webhooks. `latency_ms` is measured from request start to response, capturing the full round-trip to SendGrid's API.

### `app/providers/sms.py`

```python
import time

from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

from app.config import settings
from app.providers.base import BaseProvider, DeliveryResult


class TwilioProvider(BaseProvider):
    """SMS delivery via Twilio API."""

    def __init__(self):
        self.client = Client(
            settings.twilio_account_sid,
            settings.twilio_auth_token,
        )
        self.from_number = settings.twilio_from_number

    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
        metadata: dict | None = None,
    ) -> DeliveryResult:
        """
        Send an SMS via Twilio.

        Args:
            recipient: Phone number in E.164 format (e.g., "+14155551234")
            subject: Ignored for SMS
            body: SMS message body (max 1600 characters)
        """
        start = time.time()

        # Truncate to SMS limit
        sms_body = body[:1600]

        try:
            message = self.client.messages.create(
                body=sms_body,
                from_=self.from_number,
                to=recipient,
            )
            latency = int((time.time() - start) * 1000)

            return DeliveryResult(
                success=True,
                provider_id=message.sid,
                latency_ms=latency,
            )

        except TwilioRestException as e:
            latency = int((time.time() - start) * 1000)
            return DeliveryResult(
                success=False,
                error_code=str(e.code),
                error_message=e.msg,
                latency_ms=latency,
            )

        except Exception as e:
            latency = int((time.time() - start) * 1000)
            return DeliveryResult(
                success=False,
                error_code="EXCEPTION",
                error_message=str(e),
                latency_ms=latency,
            )

    def get_provider_name(self) -> str:
        return "twilio"
```

Twilio's `messages.create` sends an SMS and returns a `message.sid` for tracking. SMS bodies are truncated to 1600 characters (Twilio's limit; standard SMS is 160, but Twilio concatenates longer messages automatically). Phone numbers must be in E.164 format (`+` followed by country code and number). `TwilioRestException` catches API errors like invalid numbers (error code 21211) or rate limits (error code 20429).

### `app/providers/push.py`

```python
import time

import firebase_admin
from firebase_admin import credentials, messaging

from app.config import settings
from app.providers.base import BaseProvider, DeliveryResult


# Initialize Firebase Admin SDK
_firebase_app = None

def _get_firebase_app():
    global _firebase_app
    if _firebase_app is None:
        cred = credentials.Certificate(settings.fcm_credentials_path)
        _firebase_app = firebase_admin.initialize_app(cred)
    return _firebase_app


class FCMProvider(BaseProvider):
    """Push notification delivery via Firebase Cloud Messaging."""

    def __init__(self):
        _get_firebase_app()

    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
        metadata: dict | None = None,
    ) -> DeliveryResult:
        """
        Send a push notification via FCM.

        Args:
            recipient: FCM device token
            subject: Notification title
            body: Notification body
            metadata: Additional data payload for the app
        """
        start = time.time()

        message = messaging.Message(
            notification=messaging.Notification(
                title=subject or "Notification",
                body=body,
            ),
            data={str(k): str(v) for k, v in (metadata or {}).items()},
            token=recipient,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    click_action="OPEN_NOTIFICATION",
                    sound="default",
                ),
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        sound="default",
                        badge=1,
                    ),
                ),
            ),
        )

        try:
            response = messaging.send(message)
            latency = int((time.time() - start) * 1000)

            return DeliveryResult(
                success=True,
                provider_id=response,             # FCM returns message ID
                latency_ms=latency,
            )

        except messaging.UnregisteredError:
            latency = int((time.time() - start) * 1000)
            return DeliveryResult(
                success=False,
                error_code="UNREGISTERED",
                error_message="Device token is no longer valid",
                latency_ms=latency,
            )

        except Exception as e:
            latency = int((time.time() - start) * 1000)
            return DeliveryResult(
                success=False,
                error_code="EXCEPTION",
                error_message=str(e),
                latency_ms=latency,
            )

    def get_provider_name(self) -> str:
        return "fcm"
```

Firebase Cloud Messaging supports both Android and iOS through a unified API. The `Message` object includes platform-specific configuration: `AndroidConfig` sets high priority to wake the device, and `APNSConfig` sets the badge count and sound for iOS. `data` is a key-value payload the app receives when the notification is tapped. `UnregisteredError` means the device token is invalid (user uninstalled the app or token expired), and the token should be removed from the database to avoid repeated failures. The Firebase Admin SDK is initialized once using a lazy singleton to avoid multiple initializations.

---

## Priority Queue & Worker Pipeline

### `app/queue.py`

```python
import json
import time
import uuid
from datetime import datetime, timezone

import redis as redis_lib

from app.config import settings

redis_client = redis_lib.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)


# ─── Priority Queue Keys ─────────────────────────────────────────

# Separate sorted sets per priority for weighted processing
QUEUE_KEYS = {
    "critical": "queue:critical",
    "high": "queue:high",
    "medium": "queue:medium",
    "low": "queue:low",
}

PROCESSING_KEY = "queue:processing"               # Set of in-flight job IDs
DLQ_KEY = "queue:dlq"                             # Dead letter queue

# ─── Enqueue ──────────────────────────────────────────────────────

def enqueue(
    notification_id: str,
    channel: str,
    priority: str,
    payload: dict,
    scheduled_at: datetime | None = None,
):
    """
    Add a notification job to the priority queue.

    Jobs are stored in a Redis sorted set, scored by timestamp.
    This allows scheduled notifications (future score) and
    priority ordering (critical queue is polled first).
    """
    score = scheduled_at.timestamp() if scheduled_at else time.time()

    job = json.dumps({
        "notification_id": notification_id,
        "channel": channel,
        "priority": priority,
        "payload": payload,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    })

    queue_key = QUEUE_KEYS.get(priority, QUEUE_KEYS["medium"])
    redis_client.zadd(queue_key, {job: score})


# ─── Dequeue ──────────────────────────────────────────────────────

def dequeue(priority: str, count: int = 10) -> list[dict]:
    """
    Dequeue up to `count` jobs from a priority queue.

    Uses ZPOPMIN to atomically remove the lowest-scored (oldest) jobs.
    Only picks jobs whose score <= current time (handles scheduled).
    """
    queue_key = QUEUE_KEYS.get(priority, QUEUE_KEYS["medium"])
    now = time.time()

    # Get jobs with score <= now (ready to process)
    jobs = redis_client.zrangebyscore(
        queue_key, "-inf", now, start=0, num=count
    )

    if not jobs:
        return []

    pipe = redis_client.pipeline()
    for job in jobs:
        pipe.zrem(queue_key, job)
        pipe.sadd(PROCESSING_KEY, job)
    pipe.execute()

    return [json.loads(j) for j in jobs]


def complete_job(job_json: str):
    """Mark a job as completed by removing from processing set."""
    redis_client.srem(PROCESSING_KEY, job_json)


def fail_job(job_json: str):
    """Move a failed job to the dead letter queue."""
    pipe = redis_client.pipeline()
    pipe.srem(PROCESSING_KEY, job_json)
    pipe.zadd(DLQ_KEY, {job_json: time.time()})
    pipe.execute()


# ─── Queue Stats ──────────────────────────────────────────────────

def get_queue_depths() -> dict:
    """Get the current depth of each priority queue."""
    depths = {}
    for priority, key in QUEUE_KEYS.items():
        depths[priority] = redis_client.zcard(key)
    depths["processing"] = redis_client.scard(PROCESSING_KEY)
    depths["dlq"] = redis_client.zcard(DLQ_KEY)
    return depths


def get_dlq_jobs(count: int = 50) -> list[dict]:
    """Get jobs from the dead letter queue for inspection."""
    jobs = redis_client.zrange(DLQ_KEY, 0, count - 1)
    return [json.loads(j) for j in jobs]


def requeue_dlq_job(job_json: str, priority: str = "medium"):
    """Move a job from the DLQ back to a priority queue for retry."""
    pipe = redis_client.pipeline()
    pipe.zrem(DLQ_KEY, job_json)
    pipe.zadd(QUEUE_KEYS.get(priority, QUEUE_KEYS["medium"]), {job_json: time.time()})
    pipe.execute()
```

The priority queue uses **four Redis sorted sets**, one per priority level. Workers poll in priority order: critical → high → medium → low. Jobs are scored by timestamp, enabling scheduled notifications: a notification scheduled for 3 PM has a score of the 3 PM Unix timestamp, so `ZRANGEBYSCORE` with `max=now` won't return it until 3 PM. `ZRANGEBYSCORE` + `ZREM` is done atomically in a pipeline to prevent multiple workers from dequeuing the same job. The `PROCESSING_KEY` set tracks in-flight jobs; if a worker crashes, a supervisor can detect orphaned jobs and requeue them. The dead letter queue (`DLQ`) stores jobs that have exhausted all retry attempts for manual inspection and reprocessing.

---

## Rate Limiting & Throttling

### `app/rate_limiter.py`

```python
import time

from app.queue import redis_client
from app.config import settings


# ─── Sliding Window Rate Limiter ──────────────────────────────────

def check_rate_limit(
    user_id: str,
    channel: str,
    window_seconds: int | None = None,
    max_requests: int | None = None,
) -> dict:
    """
    Check if a notification is within rate limits.

    Uses a sliding window counter backed by Redis sorted sets.
    Each entry in the sorted set has a timestamp score; entries
    outside the window are pruned on every check.

    Returns:
        {"allowed": bool, "remaining": int, "reset_seconds": int}
    """
    # Determine limits based on channel
    if window_seconds is None or max_requests is None:
        window_seconds, max_requests = _get_channel_limits(channel)

    key = f"ratelimit:{channel}:{user_id}"
    now = time.time()
    window_start = now - window_seconds

    pipe = redis_client.pipeline()

    # Remove expired entries outside the window
    pipe.zremrangebyscore(key, "-inf", window_start)

    # Count remaining entries in the window
    pipe.zcard(key)

    results = pipe.execute()
    current_count = results[1]

    if current_count >= max_requests:
        # Find when the oldest entry expires
        oldest = redis_client.zrange(key, 0, 0, withscores=True)
        reset_at = oldest[0][1] + window_seconds if oldest else now + window_seconds

        return {
            "allowed": False,
            "remaining": 0,
            "reset_seconds": int(reset_at - now),
        }

    # Add this request to the window
    redis_client.zadd(key, {f"{now}:{id(key)}": now})
    redis_client.expire(key, window_seconds + 60)     # TTL = window + buffer

    return {
        "allowed": True,
        "remaining": max_requests - current_count - 1,
        "reset_seconds": window_seconds,
    }


def check_global_user_limit(user_id: str) -> dict:
    """
    Check the per-user global rate limit across all channels.
    Prevents any single user from receiving too many notifications.
    """
    return check_rate_limit(
        user_id=user_id,
        channel="global",
        window_seconds=60,
        max_requests=settings.rate_limit_user_per_minute,
    )


# ─── Channel-Specific Limits ─────────────────────────────────────

def _get_channel_limits(channel: str) -> tuple[int, int]:
    """Return (window_seconds, max_requests) for a channel."""
    limits = {
        "email": (3600, settings.rate_limit_email_per_hour),
        "sms": (3600, settings.rate_limit_sms_per_hour),
        "push": (3600, settings.rate_limit_push_per_hour),
        "in_app": (3600, 200),                # In-app is less restrictive
        "global": (60, settings.rate_limit_user_per_minute),
    }
    return limits.get(channel, (3600, 100))


# ─── Rate Limit Bypass ────────────────────────────────────────────

BYPASS_TYPES = {"otp", "security_alert", "fraud_alert", "password_reset"}

def should_bypass_rate_limit(notif_type: str, priority: str) -> bool:
    """
    Check if a notification should bypass rate limits.
    Critical security notifications are always sent.
    """
    if priority == "critical":
        return True
    if notif_type.lower() in BYPASS_TYPES:
        return True
    return False
```

The rate limiter uses a **sliding window** backed by Redis sorted sets. Each request adds an entry scored by its timestamp. Before each check, expired entries (outside the window) are pruned with `ZREMRANGEBYSCORE`. The count of remaining entries tells us how many requests occurred in the current window. This is more accurate than a fixed-window counter because it doesn't have the boundary problem where a burst at the end of one window and start of the next effectively doubles the limit. Two levels of limiting are applied: per-channel (50 emails/hour, 20 SMS/hour) and per-user global (30 notifications/minute across all channels). Critical notifications like OTPs and fraud alerts bypass all rate limits because blocking an OTP could lock a user out of their account.

---

## User Preferences & Routing

### `app/router_engine.py`

```python
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models import (
    Channel, Notification, NotificationStatus, Priority,
    UserPreference,
)
from app.rate_limiter import check_rate_limit, check_global_user_limit, should_bypass_rate_limit
from app.templates import render_template
from app.queue import enqueue
from app.config import settings


def route_notification(
    user_id: str,
    notif_type: str,
    channels: list[str],
    priority: str,
    template_slug: str | None,
    subject: str | None,
    body: str | None,
    variables: dict | None,
    metadata: dict | None,
    scheduled_at: datetime | None,
    idempotency_key: str | None,
    db: Session,
) -> list[Notification]:
    """
    Route a notification to the appropriate channels.

    Pipeline:
    1. Check idempotency (skip if duplicate)
    2. Render template (if template_slug provided)
    3. For each channel:
       a. Check user preferences (opt-in/out, quiet hours)
       b. Check rate limits
       c. Create notification record
       d. Enqueue for delivery (or send directly if critical)
    """
    notifications = []

    # 1. Idempotency check
    if idempotency_key:
        existing = (
            db.query(Notification)
            .filter(Notification.idempotency_key == idempotency_key)
            .first()
        )
        if existing:
            return [existing]

    for channel_str in channels:
        channel = Channel(channel_str)

        # 2. Render template for this channel
        rendered_subject = subject
        rendered_body = body
        if template_slug:
            try:
                rendered = render_template(template_slug, variables or {}, db)
                rendered_subject = rendered.get("subject") or subject
                rendered_body = rendered.get("body") or body
            except ValueError:
                rendered_body = body or "Notification"

        if not rendered_body:
            continue

        # 3a. Check user preferences
        if not _is_channel_enabled(user_id, channel_str, notif_type, db):
            continue

        if _is_quiet_hours(user_id, channel_str, notif_type, db):
            # Defer to next non-quiet period
            scheduled_at = _next_active_time(user_id, channel_str, notif_type, db)

        # 3b. Check rate limits (bypass for critical)
        bypass = should_bypass_rate_limit(notif_type, priority)
        if not bypass:
            global_limit = check_global_user_limit(user_id)
            if not global_limit["allowed"]:
                continue

            channel_limit = check_rate_limit(user_id, channel_str)
            if not channel_limit["allowed"]:
                continue

        # 3c. Create notification record
        notif = Notification(
            user_id=user_id,
            type=notif_type,
            channel=channel,
            priority=Priority(priority),
            template_id=None,
            subject=rendered_subject,
            body=rendered_body,
            metadata_=metadata,
            status=NotificationStatus.QUEUED,
            scheduled_at=scheduled_at,
            idempotency_key=idempotency_key,
        )
        db.add(notif)
        db.flush()

        # 3d. Enqueue or send directly
        if priority == "critical" and settings.critical_bypass_queue:
            # Critical notifications bypass the queue
            notif.status = NotificationStatus.PROCESSING
            db.flush()
            _send_directly(notif, channel_str, metadata, db)
        else:
            payload = {
                "notification_id": str(notif.id),
                "user_id": str(user_id),
                "channel": channel_str,
                "subject": rendered_subject,
                "body": rendered_body,
                "metadata": metadata,
            }
            enqueue(
                notification_id=str(notif.id),
                channel=channel_str,
                priority=priority,
                payload=payload,
                scheduled_at=scheduled_at,
            )

        notifications.append(notif)

    db.commit()
    return notifications


# ─── Preference Checks ────────────────────────────────────────────

def _is_channel_enabled(
    user_id: str,
    channel: str,
    notif_type: str,
    db: Session,
) -> bool:
    """Check if the user has opted in to this channel for this type."""
    pref = (
        db.query(UserPreference)
        .filter(
            UserPreference.user_id == user_id,
            UserPreference.channel == Channel(channel),
            UserPreference.notif_type == notif_type,
        )
        .first()
    )

    # Default to enabled if no preference is set
    if not pref:
        return True

    return pref.enabled


def _is_quiet_hours(
    user_id: str,
    channel: str,
    notif_type: str,
    db: Session,
) -> bool:
    """Check if the current time falls within the user's quiet hours."""
    pref = (
        db.query(UserPreference)
        .filter(
            UserPreference.user_id == user_id,
            UserPreference.channel == Channel(channel),
            UserPreference.notif_type == notif_type,
        )
        .first()
    )

    if not pref or not pref.quiet_start or not pref.quiet_end:
        return False

    now = datetime.now(timezone.utc).time()

    if pref.quiet_start <= pref.quiet_end:
        # Normal range: e.g., 22:00 to 06:00 → quiet if NOT between
        return pref.quiet_start <= now <= pref.quiet_end
    else:
        # Overnight range: e.g., 22:00 to 06:00
        return now >= pref.quiet_start or now <= pref.quiet_end


def _next_active_time(
    user_id: str,
    channel: str,
    notif_type: str,
    db: Session,
) -> datetime | None:
    """Calculate the next time outside quiet hours for scheduling."""
    pref = (
        db.query(UserPreference)
        .filter(
            UserPreference.user_id == user_id,
            UserPreference.channel == Channel(channel),
            UserPreference.notif_type == notif_type,
        )
        .first()
    )

    if not pref or not pref.quiet_end:
        return None

    now = datetime.now(timezone.utc)
    next_active = now.replace(
        hour=pref.quiet_end.hour,
        minute=pref.quiet_end.minute,
        second=0,
    )

    if next_active <= now:
        from datetime import timedelta
        next_active += timedelta(days=1)

    return next_active


# ─── Direct Send (Critical Bypass) ────────────────────────────────

def _send_directly(
    notif: Notification,
    channel: str,
    metadata: dict | None,
    db: Session,
):
    """Send a critical notification immediately, bypassing the queue."""
    from app.providers.email import SendGridProvider
    from app.providers.sms import TwilioProvider
    from app.providers.push import FCMProvider

    providers = {
        "email": SendGridProvider,
        "sms": TwilioProvider,
        "push": FCMProvider,
    }

    provider_class = providers.get(channel)
    if not provider_class:
        return

    provider = provider_class()
    recipient = _get_recipient(channel, metadata)
    if not recipient:
        return

    result = provider.send(
        recipient=recipient,
        subject=notif.subject,
        body=notif.body,
        metadata=metadata,
    )

    now = datetime.now(timezone.utc)
    if result.success:
        notif.status = NotificationStatus.SENT
        notif.sent_at = now
        notif.provider_id = result.provider_id
    else:
        notif.status = NotificationStatus.FAILED
        notif.failed_at = now
        notif.error_message = result.error_message

    db.flush()


def _get_recipient(channel: str, metadata: dict | None) -> str | None:
    """Extract the recipient address from metadata."""
    if not metadata:
        return None
    recipients = {
        "email": metadata.get("email"),
        "sms": metadata.get("phone"),
        "push": metadata.get("device_token"),
    }
    return recipients.get(channel)
```

The routing engine is the heart of the notification system. It orchestrates the full pipeline: idempotency → template rendering → preference check → quiet hours → rate limiting → enqueue/send. **Idempotency** is checked first because it's the cheapest operation. **Quiet hours** handling converts an immediate notification into a scheduled one: if the user has quiet hours 10 PM–7 AM and the notification arrives at 11 PM, it's scheduled for 7 AM. **Critical bypass** sends OTPs and fraud alerts directly without queuing, ensuring sub-3-second delivery. The `_is_quiet_hours` function handles the overnight case (e.g., 22:00 to 06:00) where the start is after the end. Default behavior when no preference exists is to allow delivery (opt-out model, not opt-in).

---

## Core API – Notifications

### `app/routers/notifications.py`

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Notification, NotificationStatus, DeliveryAttempt
from app.queue import get_queue_depths, get_dlq_jobs
from app.router_engine import route_notification
from app.schemas import (
    NotificationSend, NotificationResponse, NotificationBatch,
    DeliveryStatus,
)

router = APIRouter()


@router.post("/send", status_code=201)
def send_notification(
    payload: NotificationSend,
    db: Session = Depends(get_db),
):
    """
    Send a notification to a user across one or more channels.
    Each channel becomes a separate notification record.
    """
    notifications = route_notification(
        user_id=str(payload.user_id),
        notif_type=payload.type,
        channels=payload.channels,
        priority=payload.priority,
        template_slug=payload.template_slug,
        subject=payload.subject,
        body=payload.body,
        variables=payload.variables,
        metadata=payload.metadata,
        scheduled_at=payload.scheduled_at,
        idempotency_key=payload.idempotency_key,
        db=db,
    )

    if not notifications:
        raise HTTPException(
            status_code=422,
            detail="Notification not sent. User may have opted out or rate limit reached.",
        )

    return {
        "notifications": [
            {
                "id": str(n.id),
                "channel": n.channel.value,
                "status": n.status.value,
            }
            for n in notifications
        ],
    }


@router.post("/batch", status_code=202)
def send_batch(
    payload: NotificationBatch,
    db: Session = Depends(get_db),
):
    """
    Send a notification to multiple users.
    Returns immediately; processing happens asynchronously.
    """
    queued_count = 0
    skipped_count = 0

    for user_id in payload.user_ids:
        notifications = route_notification(
            user_id=str(user_id),
            notif_type=payload.type,
            channels=payload.channels,
            priority=payload.priority,
            template_slug=payload.template_slug,
            subject=payload.subject,
            body=payload.body,
            variables=payload.variables,
            metadata=None,
            scheduled_at=None,
            idempotency_key=None,
            db=db,
        )
        if notifications:
            queued_count += len(notifications)
        else:
            skipped_count += 1

    return {
        "queued": queued_count,
        "skipped": skipped_count,
        "total_users": len(payload.user_ids),
    }


@router.get("/{notification_id}", response_model=NotificationResponse)
def get_notification(
    notification_id: str,
    db: Session = Depends(get_db),
):
    """Get a notification's current status and details."""
    notif = (
        db.query(Notification)
        .filter(Notification.id == notification_id)
        .first()
    )
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found.")

    return notif


@router.get("/{notification_id}/status")
def get_delivery_status(
    notification_id: str,
    db: Session = Depends(get_db),
):
    """Get detailed delivery status including all attempts."""
    notif = (
        db.query(Notification)
        .filter(Notification.id == notification_id)
        .first()
    )
    if not notif:
        raise HTTPException(status_code=404, detail="Notification not found.")

    attempts = (
        db.query(DeliveryAttempt)
        .filter(DeliveryAttempt.notification_id == notification_id)
        .order_by(DeliveryAttempt.attempt_number)
        .all()
    )

    return {
        "notification_id": str(notif.id),
        "status": notif.status.value,
        "attempts": [
            {
                "attempt": a.attempt_number,
                "channel": a.channel.value,
                "provider": a.provider,
                "status": a.status.value,
                "error_code": a.error_code,
                "error_message": a.error_message,
                "latency_ms": a.latency_ms,
                "attempted_at": a.attempted_at.isoformat(),
            }
            for a in attempts
        ],
        "created_at": notif.created_at.isoformat(),
        "sent_at": notif.sent_at.isoformat() if notif.sent_at else None,
        "delivered_at": notif.delivered_at.isoformat() if notif.delivered_at else None,
        "opened_at": notif.opened_at.isoformat() if notif.opened_at else None,
    }


@router.get("/user/{user_id}", response_model=list[NotificationResponse])
def get_user_notifications(
    user_id: str,
    status: str | None = None,
    channel: str | None = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get notification history for a user."""
    q = (
        db.query(Notification)
        .filter(Notification.user_id == user_id)
    )

    if status:
        q = q.filter(Notification.status == NotificationStatus(status))
    if channel:
        q = q.filter(Notification.channel == channel)

    q = q.order_by(Notification.created_at.desc())
    q = q.offset(skip).limit(limit)

    return q.all()


@router.get("/queue/stats")
def queue_stats():
    """Get current queue depths for monitoring."""
    return get_queue_depths()


@router.get("/queue/dlq")
def dead_letter_queue(
    count: int = Query(20, ge=1, le=100),
):
    """Get jobs from the dead letter queue for inspection."""
    return get_dlq_jobs(count)
```

The send endpoint creates one notification record per channel and returns immediately. The batch endpoint processes up to 1000 users synchronously; for larger campaigns, use the campaign API. The delivery status endpoint shows every attempt, enabling debugging: "the first attempt to SendGrid failed with a 429 (rate limited), the retry succeeded 60 seconds later with 200 ms latency." Queue stats expose the current depth of each priority queue, essential for monitoring: if the `critical` queue depth exceeds 100, something is critically wrong. The DLQ endpoint lets operators inspect and requeue failed notifications.

### `app/routers/templates.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import NotificationTemplate, Channel
from app.schemas import TemplateCreate, TemplateResponse, TemplateRender
from app.templates import validate_template, extract_variables, render_template

router = APIRouter()


@router.post("/", response_model=TemplateResponse, status_code=201)
def create_template(payload: TemplateCreate, db: Session = Depends(get_db)):
    """Create a new notification template."""
    # Validate template syntax
    errors = validate_template(payload.body, payload.subject)
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    # Check for duplicate slug
    existing = (
        db.query(NotificationTemplate)
        .filter(NotificationTemplate.slug == payload.slug)
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="Template slug already exists.")

    # Auto-extract variables if not provided
    variables = payload.variables or extract_variables(payload.body, payload.subject)

    template = NotificationTemplate(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        channel=Channel(payload.channel),
        subject=payload.subject,
        body=payload.body,
        variables=variables,
    )

    db.add(template)
    db.commit()
    db.refresh(template)

    return template


@router.get("/{slug}", response_model=TemplateResponse)
def get_template(slug: str, db: Session = Depends(get_db)):
    """Get a template by slug."""
    template = (
        db.query(NotificationTemplate)
        .filter(NotificationTemplate.slug == slug)
        .first()
    )
    if not template:
        raise HTTPException(status_code=404, detail="Template not found.")
    return template


@router.get("/", response_model=list[TemplateResponse])
def list_templates(
    channel: str | None = None,
    db: Session = Depends(get_db),
):
    """List all active templates, optionally filtered by channel."""
    q = db.query(NotificationTemplate).filter(NotificationTemplate.is_active.is_(True))
    if channel:
        q = q.filter(NotificationTemplate.channel == Channel(channel))
    return q.order_by(NotificationTemplate.name).all()


@router.post("/render")
def render(payload: TemplateRender, db: Session = Depends(get_db)):
    """Preview a rendered template with given variables."""
    try:
        result = render_template(payload.template_slug, payload.variables, db)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{slug}")
def deactivate_template(slug: str, db: Session = Depends(get_db)):
    """Soft-delete a template by deactivating it."""
    template = (
        db.query(NotificationTemplate)
        .filter(NotificationTemplate.slug == slug)
        .first()
    )
    if not template:
        raise HTTPException(status_code=404, detail="Template not found.")

    template.is_active = False
    db.commit()

    return {"status": "deactivated", "slug": slug}
```

The template creation endpoint validates Jinja2 syntax before saving, preventing broken templates from reaching production. Variables are auto-extracted from the template body using Jinja2's AST parser if not explicitly provided. The render endpoint lets developers preview notifications without sending them. Deletion is a soft-delete (setting `is_active=False`) so existing notification records that reference the template remain valid.

### `app/routers/preferences.py`

```python
from datetime import time
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import UserPreference, Channel
from app.schemas import PreferenceUpdate, PreferenceResponse

router = APIRouter()


@router.put("/{user_id}")
def update_preference(
    user_id: str,
    payload: PreferenceUpdate,
    db: Session = Depends(get_db),
):
    """Create or update a user's notification preference."""
    channel = Channel(payload.channel)

    pref = (
        db.query(UserPreference)
        .filter(
            UserPreference.user_id == user_id,
            UserPreference.channel == channel,
            UserPreference.notif_type == payload.notif_type,
        )
        .first()
    )

    if pref:
        pref.enabled = payload.enabled
        if payload.quiet_start:
            h, m = payload.quiet_start.split(":")
            pref.quiet_start = time(int(h), int(m))
        if payload.quiet_end:
            h, m = payload.quiet_end.split(":")
            pref.quiet_end = time(int(h), int(m))
    else:
        quiet_start = None
        quiet_end = None
        if payload.quiet_start:
            h, m = payload.quiet_start.split(":")
            quiet_start = time(int(h), int(m))
        if payload.quiet_end:
            h, m = payload.quiet_end.split(":")
            quiet_end = time(int(h), int(m))

        pref = UserPreference(
            user_id=user_id,
            channel=channel,
            notif_type=payload.notif_type,
            enabled=payload.enabled,
            quiet_start=quiet_start,
            quiet_end=quiet_end,
        )
        db.add(pref)

    db.commit()

    return {"status": "updated"}


@router.get("/{user_id}", response_model=list[PreferenceResponse])
def get_preferences(
    user_id: str,
    db: Session = Depends(get_db),
):
    """Get all notification preferences for a user."""
    prefs = (
        db.query(UserPreference)
        .filter(UserPreference.user_id == user_id)
        .all()
    )
    return [
        PreferenceResponse(
            channel=p.channel.value,
            notif_type=p.notif_type,
            enabled=p.enabled,
            quiet_start=p.quiet_start.strftime("%H:%M") if p.quiet_start else None,
            quiet_end=p.quiet_end.strftime("%H:%M") if p.quiet_end else None,
        )
        for p in prefs
    ]


@router.delete("/{user_id}/all")
def opt_out_all(
    user_id: str,
    db: Session = Depends(get_db),
):
    """Opt out of all non-critical notifications (unsubscribe all)."""
    prefs = (
        db.query(UserPreference)
        .filter(UserPreference.user_id == user_id)
        .all()
    )

    if prefs:
        for pref in prefs:
            pref.enabled = False
        db.commit()
    else:
        # Create disabled preferences for all channels
        for channel in Channel:
            pref = UserPreference(
                user_id=user_id,
                channel=channel,
                notif_type="*",
                enabled=False,
            )
            db.add(pref)
        db.commit()

    return {"status": "opted_out_all"}
```

The preferences API uses an upsert pattern: `PUT` creates the preference if it doesn't exist or updates it if it does. Quiet hours are stored as `TIME` types and parsed from `HH:MM` strings. The opt-out-all endpoint sets `notif_type="*"` as a wildcard, which the routing engine checks first. This complies with CAN-SPAM and GDPR requirements for one-click unsubscribe.

---

## Real-Time Delivery (WebSockets)

### `app/websocket_manager.py`

```python
import json
from fastapi import WebSocket


class NotificationConnectionManager:
    """Manages WebSocket connections for real-time in-app notifications."""

    def __init__(self):
        # user_id → list of connected WebSocket clients
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        """Accept a new WebSocket connection for a user."""
        await websocket.accept()
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)

    def disconnect(self, user_id: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        if user_id in self.active_connections:
            self.active_connections[user_id].remove(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    def is_online(self, user_id: str) -> bool:
        """Check if a user has any active WebSocket connections."""
        return user_id in self.active_connections

    async def send_to_user(self, user_id: str, message: dict):
        """Send a notification to all of a user's connected devices."""
        if user_id not in self.active_connections:
            return False

        dead_connections = []
        for connection in self.active_connections[user_id]:
            try:
                await connection.send_json(message)
            except Exception:
                dead_connections.append(connection)

        for conn in dead_connections:
            self.disconnect(user_id, conn)

        return True

    async def send_notification(
        self,
        user_id: str,
        notification_id: str,
        notif_type: str,
        subject: str | None,
        body: str,
        metadata: dict | None = None,
    ):
        """Send a structured notification message to a user."""
        await self.send_to_user(user_id, {
            "type": "notification",
            "data": {
                "id": notification_id,
                "notif_type": notif_type,
                "subject": subject,
                "body": body,
                "metadata": metadata,
            },
        })

    async def broadcast(self, message: dict):
        """Send a message to all connected users."""
        for user_id in list(self.active_connections.keys()):
            await self.send_to_user(user_id, message)

    def get_online_count(self) -> int:
        """Get the number of online users."""
        return len(self.active_connections)


manager = NotificationConnectionManager()
```

The `NotificationConnectionManager` maps `user_id` to a list of WebSocket connections (a user may have multiple devices connected). `is_online` checks if a user has any active connections, which the routing engine can use to prefer in-app delivery over push. Dead connections are cleaned up on send failure. `broadcast` sends system-wide announcements to all connected users. In production with multiple API server instances, use Redis Pub/Sub to fan out WebSocket messages across all instances.

### `app/routers/live.py`

```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/live/{user_id}")
async def live_notifications(user_id: str, websocket: WebSocket):
    """WebSocket endpoint for real-time in-app notifications."""
    await manager.connect(user_id, websocket)

    try:
        while True:
            data = await websocket.receive_json()

            if data.get("type") == "ack":
                # Client acknowledges receipt of a notification
                pass

            elif data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        manager.disconnect(user_id, websocket)
```

The WebSocket endpoint at `/ws/live/{user_id}` accepts connections from the client app. The client sends `ack` messages to acknowledge receipt (used for delivery confirmation and read receipts) and `ping` messages to keep the connection alive. `WebSocketDisconnect` handles both clean closes and network drops.

---

## Retry & Dead Letter Queue

### `app/retry.py`

```python
import math
import json
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.config import settings
from app.models import (
    Notification, NotificationStatus, DeliveryAttempt,
    AttemptStatus, Channel,
)
from app.providers.email import SendGridProvider
from app.providers.sms import TwilioProvider
from app.providers.push import FCMProvider
from app.queue import enqueue, fail_job


# ─── Provider Registry ────────────────────────────────────────────

PROVIDERS = {
    "email": SendGridProvider,
    "sms": TwilioProvider,
    "push": FCMProvider,
}


# ─── Process a Notification Job ───────────────────────────────────

def process_notification(job: dict, db: Session):
    """
    Process a single notification job from the queue.

    Steps:
    1. Load the notification record
    2. Get the appropriate provider
    3. Attempt delivery
    4. Log the attempt
    5. On failure: retry with exponential backoff or move to DLQ
    """
    notif_id = job["notification_id"]
    channel = job["channel"]
    payload = job["payload"]

    # 1. Load notification
    notif = db.query(Notification).filter(Notification.id == notif_id).first()
    if not notif:
        return

    if notif.status in (NotificationStatus.SENT, NotificationStatus.CANCELLED):
        return                                    # Already processed

    notif.status = NotificationStatus.PROCESSING
    db.flush()

    # 2. Get provider
    provider_class = PROVIDERS.get(channel)
    if not provider_class:
        notif.status = NotificationStatus.FAILED
        notif.error_message = f"Unknown channel: {channel}"
        notif.failed_at = datetime.now(timezone.utc)
        db.commit()
        return

    provider = provider_class()
    recipient = _get_recipient(channel, payload.get("metadata"))

    if not recipient:
        notif.status = NotificationStatus.FAILED
        notif.error_message = "No recipient address found"
        notif.failed_at = datetime.now(timezone.utc)
        db.commit()
        return

    # 3. Attempt delivery
    result = provider.send(
        recipient=recipient,
        subject=payload.get("subject"),
        body=payload.get("body"),
        metadata=payload.get("metadata"),
    )

    # 4. Log attempt
    attempt = DeliveryAttempt(
        notification_id=notif.id,
        attempt_number=notif.retry_count + 1,
        channel=Channel(channel),
        provider=provider.get_provider_name(),
        provider_id=result.provider_id,
        status=AttemptStatus.SUCCESS if result.success else AttemptStatus.FAILED,
        error_code=result.error_code,
        error_message=result.error_message,
        latency_ms=result.latency_ms,
    )
    db.add(attempt)

    # 5. Handle result
    now = datetime.now(timezone.utc)

    if result.success:
        notif.status = NotificationStatus.SENT
        notif.sent_at = now
        notif.provider_id = result.provider_id
        db.commit()
    else:
        notif.retry_count += 1

        if notif.retry_count >= settings.max_retry_attempts:
            # Exhausted retries → dead letter queue
            notif.status = NotificationStatus.FAILED
            notif.failed_at = now
            notif.error_message = result.error_message
            db.commit()
            fail_job(json.dumps(job))
        else:
            # Retry with exponential backoff
            delay = _calculate_backoff(notif.retry_count)
            notif.status = NotificationStatus.QUEUED
            db.commit()

            from datetime import timedelta
            retry_at = now + timedelta(seconds=delay)

            enqueue(
                notification_id=str(notif.id),
                channel=channel,
                priority=job.get("priority", "medium"),
                payload=payload,
                scheduled_at=retry_at,
            )


# ─── Exponential Backoff ──────────────────────────────────────────

def _calculate_backoff(retry_count: int) -> int:
    """
    Calculate retry delay using exponential backoff with jitter.

    Formula: base_delay × 2^(retry_count - 1) + random jitter

    Attempt 1: 60s
    Attempt 2: 120s
    Attempt 3: 240s
    Attempt 4: 480s
    Attempt 5: 960s (16 minutes)
    """
    import random

    base = settings.retry_base_delay
    delay = base * (2 ** (retry_count - 1))
    jitter = random.uniform(0, delay * 0.1)       # 10% jitter
    return int(delay + jitter)


def _get_recipient(channel: str, metadata: dict | None) -> str | None:
    """Extract the recipient address from metadata."""
    if not metadata:
        return None
    recipients = {
        "email": metadata.get("email"),
        "sms": metadata.get("phone"),
        "push": metadata.get("device_token"),
    }
    return recipients.get(channel)
```

The retry logic uses **exponential backoff with jitter**. The base delay doubles with each retry (60s → 120s → 240s → 480s → 960s), and 10% random jitter prevents thundering herd when many notifications fail simultaneously (e.g., provider outage). After `MAX_RETRY_ATTEMPTS` (5), the notification is marked as `FAILED` and moved to the dead letter queue. The DLQ preserves the full job payload so operators can inspect why it failed and manually requeue it after fixing the issue. Each delivery attempt is logged with latency, provider response, and error details, enabling SLA reporting like "99.5% of emails are delivered within 10 seconds, 0.3% require at least one retry."

### `worker.py`

```python
"""
Background worker that processes notification jobs from the priority queue.

Run with: python worker.py
Each worker instance polls queues in priority order and processes jobs.
"""
import time
import signal
import sys

from app.database import SessionLocal
from app.queue import dequeue, complete_job
from app.retry import process_notification

# Graceful shutdown
shutdown = False

def signal_handler(sig, frame):
    global shutdown
    print("Shutting down worker...")
    shutdown = True

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ─── Priority Polling Order ───────────────────────────────────────

POLL_ORDER = ["critical", "high", "medium", "low"]
POLL_BATCH_SIZE = 10
POLL_INTERVAL_MS = 100                            # Sleep between polls


def main():
    """Main worker loop: poll queues in priority order."""
    print("Worker started. Polling queues...")

    while not shutdown:
        processed = False

        for priority in POLL_ORDER:
            jobs = dequeue(priority, count=POLL_BATCH_SIZE)

            if not jobs:
                continue

            processed = True

            for job in jobs:
                db = SessionLocal()
                try:
                    process_notification(job, db)
                    complete_job(job)
                except Exception as e:
                    print(f"Error processing job: {e}")
                    db.rollback()
                finally:
                    db.close()

        if not processed:
            # No jobs found, sleep briefly before polling again
            time.sleep(POLL_INTERVAL_MS / 1000)


if __name__ == "__main__":
    main()
```

The worker polls queues in strict priority order: `critical → high → medium → low`. This ensures OTP and fraud alert notifications are always processed before promotional emails. Each poll dequeues up to 10 jobs in a batch. If no jobs are found across all queues, the worker sleeps for 100 ms to avoid busy-waiting. `SIGTERM` and `SIGINT` handlers enable graceful shutdown: the worker finishes its current job before exiting, preventing half-processed notifications. Each job gets its own database session to isolate failures. In production, you would run multiple worker instances scaled independently per channel.

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

CMD ["fastapi", "run", "app/main.py", "--host", "0.0.0.0", "--port", "8000"]
```

This uses a multi-stage build. The `builder` stage installs dependencies into `/install`. The second stage copies only the installed packages, keeping the final image small by excluding pip caches and build artifacts. `--host 0.0.0.0` binds to all interfaces so the container is reachable from outside.

### `Dockerfile.worker`

```dockerfile
FROM python:3.14-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.14-slim

WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

CMD ["python", "worker.py"]
```

The worker uses the same base image and dependencies as the API server but runs the `worker.py` script instead of FastAPI. This separation allows independent scaling: you can run 2 API instances and 10 worker instances.

### `.dockerignore`

```text
venv/
.venv/
__pycache__/
*.pyc
.git/
.env
.vscode/
.idea/
tests/
*.md
```

### `compose.yaml`

```yaml
services:
  api:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/notifications
      - REDIS_URL=redis://cache:6379/0
      - BASE_URL=http://localhost:8000
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    restart: unless-stopped

  worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/notifications
      - REDIS_URL=redis://cache:6379/0
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    deploy:
      replicas: 3
    restart: unless-stopped

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=notifications
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d notifications"]
      interval: 5s
      timeout: 5s
      retries: 5

  cache:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes --maxmemory 512mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  db-data:
  redis-data:
```

`depends_on` with `condition: service_healthy` ensures the API and worker containers wait until PostgreSQL and Redis pass their health checks. `deploy.replicas: 3` runs three worker instances for parallel processing. `--maxmemory 512mb` caps Redis memory usage; `--maxmemory-policy allkeys-lru` evicts least recently used keys when memory is full, which is safe because queue jobs are consumed and rate limit entries expire naturally. `--appendonly yes` enables Redis AOF persistence so queued jobs survive container restarts. `redis:7-alpine` is a minimal Redis image.

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import notifications, templates, preferences, live


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Notification System API",
    description="Multi-channel notification system with email, SMS, push, and in-app delivery",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(notifications.router, prefix="/notifications", tags=["Notifications"])
app.include_router(templates.router, prefix="/templates", tags=["Templates"])
app.include_router(preferences.router, prefix="/preferences", tags=["Preferences"])
app.include_router(live.router, tags=["Live"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`lifespan` is an async context manager that runs setup code before the app starts accepting requests (`Base.metadata.create_all` creates missing tables) and teardown code after shutdown. `include_router` mounts the route groups with URL prefixes and OpenAPI tags. The health check endpoint at `/health` is used by load balancers and container orchestrators to verify the service is running.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
docker compose logs -f worker                 # Follow worker logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Create an email template
curl -X POST http://localhost:8000/templates/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Order Shipped",
    "slug": "order-shipped",
    "channel": "email",
    "subject": "Your order #{{ order_id }} has shipped!",
    "body": "Hello {{ name }}, your order #{{ order_id }} has been shipped and will arrive by {{ delivery_date }}."
  }'

# Preview a rendered template
curl -X POST http://localhost:8000/templates/render \
  -H "Content-Type: application/json" \
  -d '{
    "template_slug": "order-shipped",
    "variables": {"name": "Alice", "order_id": "12345", "delivery_date": "Oct 25"}
  }'

# Send a notification
curl -X POST http://localhost:8000/notifications/send \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "type": "order_update",
    "channels": ["email", "push"],
    "priority": "high",
    "template_slug": "order-shipped",
    "variables": {"name": "Alice", "order_id": "12345", "delivery_date": "Oct 25"},
    "metadata": {"email": "alice@example.com", "device_token": "fcm-token-xxx"},
    "idempotency_key": "order-12345-shipped"
  }'

# Send a critical OTP (bypasses queue)
curl -X POST http://localhost:8000/notifications/send \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "type": "otp",
    "channels": ["sms"],
    "priority": "critical",
    "body": "Your verification code is 483920. Expires in 5 minutes.",
    "metadata": {"phone": "+14155551234"}
  }'

# Set user preferences (opt out of promotional emails)
curl -X PUT http://localhost:8000/preferences/a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
  -H "Content-Type: application/json" \
  -d '{
    "channel": "email",
    "notif_type": "promotional",
    "enabled": false
  }'

# Set quiet hours
curl -X PUT http://localhost:8000/preferences/a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
  -H "Content-Type: application/json" \
  -d '{
    "channel": "push",
    "notif_type": "order_update",
    "enabled": true,
    "quiet_start": "22:00",
    "quiet_end": "07:00"
  }'

# Check notification status
curl http://localhost:8000/notifications/NOTIFICATION_UUID/status

# Check queue depths
curl http://localhost:8000/notifications/queue/stats
```

---

## Testing

### `tests/conftest.py`

```python
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.main import app

TEST_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestSession = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@pytest.fixture(autouse=True)
def setup_db():
    """Create tables before each test, drop them after."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db():
    """Provide a database session for tests."""
    session = TestSession()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(db):
    """Override the database dependency with the test database."""
    def override_get_db():
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()
```

Tests use SQLite instead of PostgreSQL for speed and isolation. `dependency_overrides` is a FastAPI feature that lets you swap out dependencies during testing. Every test gets a fresh database because `setup_db` creates tables before and drops them after each test. `TestClient` from FastAPI wraps `httpx` and provides a synchronous interface for making requests to the app without running a server.

### `tests/test_templates.py`

```python
from unittest.mock import patch, MagicMock

from app.templates import render_template, validate_template, extract_variables
from app.models import NotificationTemplate, Channel


def test_render_template_substitutes_variables(db):
    """Test that template rendering substitutes all variables."""
    template = NotificationTemplate(
        name="Welcome",
        slug="welcome",
        channel=Channel.EMAIL,
        subject="Welcome, {{ name }}!",
        body="Hello {{ name }}, your account is ready.",
        variables=["name"],
    )
    db.add(template)
    db.commit()

    result = render_template("welcome", {"name": "Alice"}, db)

    assert result["subject"] == "Welcome, Alice!"
    assert result["body"] == "Hello Alice, your account is ready."


def test_render_template_missing_variable(db):
    """Test that rendering fails when a required variable is missing."""
    template = NotificationTemplate(
        name="Order",
        slug="order",
        channel=Channel.EMAIL,
        body="Order #{{ order_id }} is {{ status }}.",
        variables=["order_id", "status"],
    )
    db.add(template)
    db.commit()

    try:
        render_template("order", {"order_id": "123"}, db)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Missing template variables" in str(e)


def test_render_template_not_found(db):
    """Test that rendering fails for a non-existent template."""
    try:
        render_template("nonexistent", {}, db)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "not found" in str(e)


def test_validate_template_valid():
    """Test that valid templates pass validation."""
    errors = validate_template("Hello {{ name }}, your code is {{ code }}.")
    assert errors == []


def test_validate_template_invalid_syntax():
    """Test that invalid syntax is caught."""
    errors = validate_template("Hello {{ name }}, unclosed {% if")
    assert len(errors) > 0


def test_extract_variables():
    """Test that variables are correctly extracted from templates."""
    variables = extract_variables(
        body="Hello {{ name }}, order {{ order_id }}.",
        subject="Order {{ order_id }} update",
    )
    assert "name" in variables
    assert "order_id" in variables
```

### `tests/test_rate_limiter.py`

```python
from unittest.mock import patch

from app.rate_limiter import (
    check_rate_limit,
    check_global_user_limit,
    should_bypass_rate_limit,
)


def test_bypass_for_critical_priority():
    """Test that critical priority always bypasses rate limits."""
    assert should_bypass_rate_limit("order_update", "critical") is True


def test_bypass_for_otp():
    """Test that OTP type always bypasses rate limits."""
    assert should_bypass_rate_limit("otp", "medium") is True


def test_bypass_for_security_alert():
    """Test that security alerts bypass rate limits."""
    assert should_bypass_rate_limit("security_alert", "high") is True


def test_no_bypass_for_promotional():
    """Test that promotional notifications do NOT bypass rate limits."""
    assert should_bypass_rate_limit("promotional", "low") is False


def test_no_bypass_for_medium_order():
    """Test that regular order updates do NOT bypass rate limits."""
    assert should_bypass_rate_limit("order_update", "medium") is False


def test_rate_limit_allows_within_quota():
    """Test that requests within the quota are allowed."""
    with patch("app.rate_limiter.redis_client") as mock_redis:
        mock_pipe = mock_redis.pipeline.return_value
        mock_pipe.execute.return_value = [None, 5]     # 5 requests in window
        mock_redis.zadd.return_value = 1
        mock_redis.expire.return_value = True

        result = check_rate_limit("user-1", "email", window_seconds=3600, max_requests=50)
        assert result["allowed"] is True
        assert result["remaining"] == 44


def test_rate_limit_blocks_over_quota():
    """Test that requests over quota are blocked."""
    with patch("app.rate_limiter.redis_client") as mock_redis:
        mock_pipe = mock_redis.pipeline.return_value
        mock_pipe.execute.return_value = [None, 50]    # 50 requests (= limit)
        mock_redis.zrange.return_value = [("entry", 1000.0)]

        result = check_rate_limit("user-1", "email", window_seconds=3600, max_requests=50)
        assert result["allowed"] is False
        assert result["remaining"] == 0
```

### `tests/test_routing.py`

```python
from unittest.mock import patch, MagicMock

from app.models import UserPreference, Channel


def test_channel_disabled_skips_delivery(db):
    """Test that a disabled channel preference skips delivery."""
    from app.router_engine import _is_channel_enabled

    pref = UserPreference(
        user_id="user-1",
        channel=Channel.EMAIL,
        notif_type="promotional",
        enabled=False,
    )
    db.add(pref)
    db.commit()

    assert _is_channel_enabled("user-1", "email", "promotional", db) is False


def test_channel_enabled_by_default(db):
    """Test that channels are enabled by default when no preference exists."""
    from app.router_engine import _is_channel_enabled

    assert _is_channel_enabled("user-1", "email", "order_update", db) is True


def test_quiet_hours_detected(db):
    """Test that quiet hours are correctly detected."""
    from datetime import time
    from app.router_engine import _is_quiet_hours

    pref = UserPreference(
        user_id="user-1",
        channel=Channel.PUSH,
        notif_type="promotional",
        enabled=True,
        quiet_start=time(22, 0),
        quiet_end=time(7, 0),
    )
    db.add(pref)
    db.commit()

    # The result depends on the current time
    # In a real test, you would mock datetime.now()


def test_no_quiet_hours_when_not_set(db):
    """Test that quiet hours are not active when not configured."""
    from app.router_engine import _is_quiet_hours

    pref = UserPreference(
        user_id="user-1",
        channel=Channel.EMAIL,
        notif_type="order_update",
        enabled=True,
    )
    db.add(pref)
    db.commit()

    assert _is_quiet_hours("user-1", "email", "order_update", db) is False
```

### `tests/test_notifications.py`

```python
import uuid

from app.models import Notification, NotificationStatus, Channel, Priority


def test_create_notification(client, db):
    """Test sending a notification via API."""
    with patch_routing():
        response = client.post("/notifications/send", json={
            "user_id": str(uuid.uuid4()),
            "type": "test",
            "channels": ["push"],
            "priority": "medium",
            "body": "Test notification",
            "metadata": {"device_token": "test-token"},
        })

    assert response.status_code in (201, 422)


def test_get_notification(client, db):
    """Test retrieving a notification by ID."""
    notif = Notification(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        type="test",
        channel=Channel.PUSH,
        priority=Priority.MEDIUM,
        body="Test notification",
        status=NotificationStatus.SENT,
    )
    db.add(notif)
    db.commit()

    response = client.get(f"/notifications/{notif.id}")
    assert response.status_code == 200
    assert response.json()["status"] == "sent"


def test_get_user_notifications(client, db):
    """Test retrieving notification history for a user."""
    user_id = uuid.uuid4()

    for i in range(3):
        notif = Notification(
            user_id=user_id,
            type="test",
            channel=Channel.EMAIL,
            priority=Priority.LOW,
            body=f"Notification {i}",
            status=NotificationStatus.SENT,
        )
        db.add(notif)
    db.commit()

    response = client.get(f"/notifications/user/{user_id}")
    assert response.status_code == 200
    assert len(response.json()) == 3


def test_queue_stats(client):
    """Test queue depth stats endpoint."""
    with patch("app.routers.notifications.get_queue_depths", return_value={
        "critical": 0, "high": 5, "medium": 100, "low": 500,
        "processing": 3, "dlq": 1,
    }):
        response = client.get("/notifications/queue/stats")
        assert response.status_code == 200
        assert response.json()["critical"] == 0


def patch_routing():
    """Context manager to mock the routing engine during tests."""
    from unittest.mock import patch
    return patch("app.routers.notifications.route_notification", return_value=[])
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_rate"                         # Run only rate limiter tests
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
gcloud sql instances create notif-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-4-15360 \
  --region=us-central1 \
  --storage-size=500GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create notifications \
  --instance=notif-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=notif-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-4-15360` provisions 4 vCPUs and 15 GB RAM, suitable for the write-heavy notification workload (logging every notification and delivery attempt). `--availability-type=REGIONAL` enables automatic failover. `--storage-size=500GB` accommodates the high write volume; with 90-day retention and 5 TB/day, you need aggressive partitioning.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create notif-cache \
  --size=5 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe notif-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=5` provisions 5 GB of memory for the priority queues, rate limit windows, and processing set. `--tier=STANDARD` enables replication with automatic failover, critical because losing the Redis queue means losing queued notifications. Memorystore is accessible only from within the same VPC network.

### Build & Push Docker Images

```bash
# Build and push API image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/notif/api \
  --file=Dockerfile

# Build and push worker image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/notif/worker \
  --file=Dockerfile.worker
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build. Two separate images are built: one for the API server and one for the workers.

### Deploy API to Cloud Run

```bash
gcloud run deploy notif-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/notif/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 50 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:notif-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/notifications?host=/cloudsql/YOUR_PROJECT_ID:us-central1:notif-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
BASE_URL=https://your-service-url.run.app,\
SENDGRID_API_KEY=SG.xxxxx,\
TWILIO_ACCOUNT_SID=ACxxxxx,\
TWILIO_AUTH_TOKEN=xxxxx,\
TWILIO_FROM_NUMBER=+15551234567,\
RATE_LIMIT_EMAIL_PER_HOUR=50,\
RATE_LIMIT_SMS_PER_HOUR=20,\
CRITICAL_BYPASS_QUEUE=true"
```

`--min-instances 2` keeps at least two instances warm to avoid cold starts for critical notifications. `--max-instances 50` allows scaling for burst API traffic. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy.

### Deploy Workers to Cloud Run Jobs or GKE

```bash
# Option 1: Cloud Run Jobs (for batch processing)
gcloud run jobs create notif-worker \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/notif/worker \
  --region us-central1 \
  --memory 1Gi \
  --cpu 2 \
  --task-count 10 \
  --max-retries 3 \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/notifications?host=/cloudsql/YOUR_PROJECT_ID:us-central1:notif-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0"

# Option 2: GKE (for always-on workers) — recommended for production
# Workers need to run continuously, polling the queue
```

Cloud Run Jobs are suitable for batch campaign processing. For the always-on notification workers that continuously poll the queue, **GKE (Kubernetes Engine)** is preferred because Cloud Run services have a request timeout and Cloud Run Jobs are designed for batch tasks, not long-running daemons.

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis)
gcloud compute networks vpc-access connectors create notif-vpc-connector \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update notif-api \
  --vpc-connector=notif-vpc-connector \
  --region=us-central1
```

Cloud Run services run in Google's serverless infrastructure outside your VPC by default. A VPC connector creates a bridge so Cloud Run can reach private resources like Memorystore. `--range=10.8.0.0/28` allocates a small IP range for the connector.

### Store Secrets Securely

```bash
# Store API keys in Secret Manager instead of environment variables
gcloud secrets create sendgrid-api-key \
  --data-file=- <<< "SG.xxxxx"

gcloud secrets create twilio-auth-token \
  --data-file=- <<< "xxxxx"

# Grant Cloud Run access to secrets
gcloud secrets add-iam-policy-binding sendgrid-api-key \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Reference secrets in Cloud Run
gcloud run services update notif-api \
  --update-secrets="SENDGRID_API_KEY=sendgrid-api-key:latest,TWILIO_AUTH_TOKEN=twilio-auth-token:latest"
```

API keys for SendGrid, Twilio, and Firebase should NEVER be in plain-text environment variables. Secret Manager encrypts them at rest and provides audit logging. `--update-secrets` mounts secrets as environment variables that are fetched from Secret Manager at container start.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Table Partitioning    | `notifications` exceeds 1 B rows           | Partition by `created_at` month (range partition)  |
| Read Replicas         | Heavy dashboard/analytics queries          | Cloud SQL read replicas for status queries          |
| Connection Pooling    | Many concurrent workers + API connections  | PgBouncer sidecar or Cloud SQL Auth Proxy pooling  |
| Archival              | Retention policy (90 days)                 | Move old records to BigQuery or Cold Storage       |
| CQRS                  | Write-heavy with separate read patterns    | Separate read model for notification history       |

### Queue & Worker Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Channel-Specific Workers | Different throughput per channel         | Separate worker pools for email, SMS, push         |
| Auto-Scaling Workers  | Queue depth fluctuates with traffic        | Scale worker count based on Redis queue depth      |
| Cloud Pub/Sub         | Redis queue can't handle 500 K RPS         | Replace Redis with Pub/Sub for durability + scale  |
| Fan-Out Workers       | Campaigns targeting millions of users      | Dedicated workers that expand campaigns to jobs    |
| Priority Sharding     | Critical queue must never be starved       | Dedicated critical-only workers with reserved CPU  |

### Provider Failover

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Multi-Provider        | Primary provider has an outage             | Fallback: SendGrid → SES, Twilio → Vonage         |
| Health Checks         | Detect provider degradation early          | Monitor provider latency and error rates           |
| Circuit Breaker       | Provider returns errors consistently       | Stop sending after N failures, retry after cooldown|
| Load Balancing        | Single provider rate-limited               | Split traffic across 2+ providers                  |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ notify.xyz     │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Global Load    │
                     │ Balancer       │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │  Cloud Run     │
                     │  (API)         │
                     └──────┬─────────┘
                            │
           ┌────────────────┼────────────────┐
           │            VPC Network          │
           │                                  │
           │  ┌──────────┐ ┌──────────────┐  │
           │  │ Memory-  │ │  Cloud SQL   │  │
           │  │ store    │ │ (Partitioned │  │
           │  │ (Redis)  │ │ + Replicas)  │  │
           │  └────┬─────┘ └──────────────┘  │
           │       │                          │
           │  ┌────▼──────────────────────┐  │
           │  │    Cloud Pub/Sub          │  │
           │  │  ┌──────────────────────┐ │  │
           │  │  │ critical-notifs      │ │  │
           │  │  │ high-notifs          │ │  │
           │  │  │ medium-notifs        │ │  │
           │  │  │ low-notifs           │ │  │
           │  │  │ campaign-fanout      │ │  │
           │  │  └──────────────────────┘ │  │
           │  └────┬──────────────────────┘  │
           │       │                          │
           │  ┌────▼──────────────────────┐  │
           │  │    GKE Worker Pools       │  │
           │  │  ┌────────┐ ┌──────────┐ │  │
           │  │  │ Email  │ │   SMS    │ │  │
           │  │  │Workers │ │ Workers  │ │  │
           │  │  │(SG+SES)│ │(Twi+Von)│ │  │
           │  │  └────────┘ └──────────┘ │  │
           │  │  ┌────────┐ ┌──────────┐ │  │
           │  │  │ Push   │ │ Campaign │ │  │
           │  │  │Workers │ │ Fan-Out  │ │  │
           │  │  │ (FCM)  │ │ Workers  │ │  │
           │  │  └────────┘ └──────────┘ │  │
           │  └───────────────────────────┘  │
           │                                  │
           │  ┌───────────────────────────┐  │
           │  │  Secret Manager           │  │
           │  │  (API keys, credentials)  │  │
           │  └───────────────────────────┘  │
           └──────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud Pub/Sub** replaces Redis as the primary queue for durability (Pub/Sub guarantees at-least-once delivery with persistent storage) and scale (handles millions of messages per second natively). Redis is retained for rate limiting and caching only.
- **GKE Worker Pools** run dedicated, always-on worker deployments per channel. Email workers scale independently from SMS workers based on queue depth. Each pool can have its own resource limits and autoscaling rules.
- **Multi-Provider Failover** runs two providers per channel (SendGrid + SES for email, Twilio + Vonage for SMS). A circuit breaker switches to the backup provider after 5 consecutive failures.
- **Campaign Fan-Out Workers** expand batch campaigns asynchronously. A campaign targeting "all users in the US" (10 M users) is processed by fan-out workers that query the user database in chunks of 1000, create individual notification jobs, and publish them to Pub/Sub.
- **Table Partitioning** on `notifications` by `created_at` month keeps query performance stable as the table grows. Partitions older than 90 days are dropped (or archived to BigQuery for analytics).
- **Secret Manager** stores all API keys (SendGrid, Twilio, Firebase) encrypted at rest with audit logging, replacing plain-text environment variables.

### Security Checklist

```
✓ Store all API keys in Secret Manager (never in env vars or code)
✓ Validate all recipient addresses (email format, E.164 phone format)
✓ Rate limit per user AND per API key to prevent abuse
✓ Log all notifications for audit trail (CAN-SPAM, GDPR compliance)
✓ Implement one-click unsubscribe (CAN-SPAM requirement)
✓ Encrypt data in transit (TLS) and at rest (Cloud SQL encryption)
✓ Sanitize template variables to prevent injection attacks
✓ Restrict WebSocket connections with authentication tokens
✓ Enable Cloud Audit Logs for compliance (SOC 2, PCI DSS)
✓ Mask PII (phone numbers, emails) in logs and monitoring dashboards
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create notif-health \
  --display-name="Notification API Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Critical queue depth > 100 (OTPs are delayed)
# - High queue depth > 10 000 (processing backlog)
# - Delivery failure rate > 5% over 5 minutes
# - Provider latency p99 > 5 seconds
# - Worker crash/restart rate > 2 in 10 minutes
# - Rate limit rejection rate > 20% (possible spam attack)
# - DLQ depth > 1000 (systematic failures)
# - Cloud SQL connections > 90% of max
# - Redis memory usage > 85%
# - Email bounce rate > 3% (affects sender reputation)
# - SMS delivery rate < 95% (carrier issues)
```

Cloud Run and GKE automatically export metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing across the API → Redis → Worker → Provider chain. Track business metrics like delivery rate, open rate, and time-to-delivery in a dashboard. The critical queue depth alert is the most important: if critical notifications (OTPs) are queuing up, users can't log in.
