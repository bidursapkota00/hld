# Design Payment System / UPI-like Gateway

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [Idempotency & Exactly-Once Processing](#idempotency--exactly-once-processing)
6. [Core API – Payments & Transfers](#core-api--payments--transfers)
7. [Payment State Machine](#payment-state-machine)
8. [Saga Pattern – Distributed Transactions](#saga-pattern--distributed-transactions)
9. [Webhook & Callback System](#webhook--callback-system)
10. [Ledger & Double-Entry Bookkeeping](#ledger--double-entry-bookkeeping)
11. [Retry & Dead Letter Queue](#retry--dead-letter-queue)
12. [Rate Limiting & Fraud Detection](#rate-limiting--fraud-detection)
13. [Docker & Docker Compose](#docker--docker-compose)
14. [Testing](#testing)
15. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
16. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement          | Description                                                                  |
| -------------------- | ---------------------------------------------------------------------------- |
| P2P Transfer         | Send money from one user's account to another instantly                      |
| Merchant Payment     | Pay a merchant via a payment link, QR code, or inline checkout               |
| Idempotency          | Duplicate requests (retries, network failures) must not create duplicate charges |
| Refund               | Full or partial refund of a completed payment                                |
| Payment Status       | Query the current state of any payment by its ID                             |
| Webhook Notification | Notify merchants/partners of payment events via HTTP callbacks               |
| Ledger               | Maintain a double-entry ledger for every money movement                      |
| Multi-Currency       | Support payments in multiple currencies with conversion                      |

### Non-Functional Requirements

| Requirement        | Target                                                              |
| ------------------ | ------------------------------------------------------------------- |
| Consistency        | Strong consistency — no lost money, no double-spend, no phantom balances |
| Availability       | 99.99% uptime (downtime = failed transactions = lost revenue)       |
| Latency            | Payment initiation < 200 ms, end-to-end settlement < 5 seconds     |
| Durability         | Zero data loss — every cent must be accounted for, WAL + replication |
| Scalability        | Handle 10 000 transactions per second at peak                       |
| Idempotency        | Exactly-once semantics even under network partitions and retries    |
| Auditability       | Every state change logged with timestamp, actor, and reason          |

### Back-of-the-Envelope Estimations

```
Transactions per day             ≈ 500 million (peak periods: 2-3× average)
Transactions per second (avg)    ≈ ~6 000 TPS
Transactions per second (peak)   ≈ ~15 000 TPS
Average transaction payload      ≈ 1 KB (request + metadata)
Storage per transaction          ≈ 2 KB (payment + ledger entries + events)
Storage per day                  ≈ 500 M × 2 KB = ~1 TB/day
Storage per year                 ≈ ~365 TB (with compression: ~100 TB)
Webhook delivery rate            ≈ ~3 000 / sec (60% of transactions have callbacks)
Idempotency key TTL              ≈ 24-72 hours (covers retry windows)
Refund rate                      ≈ ~2% of completed payments
```

At 500 million transactions per day, the system processes roughly 6000 TPS on average, spiking to 15 000 TPS during peak periods (holidays, flash sales). Each transaction generates at least two ledger entries (debit + credit), so the ledger write rate is 12 000-30 000 entries/sec. Storage grows at 1 TB/day before compression. Idempotency keys must persist for 24-72 hours to cover retry windows from client timeouts and network partitions.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │(App/Merchant)│
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ API Gateway  │
                        │ (GCP LB +    │
                        │  Rate Limit) │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │     Payment Service         │
                │   (FastAPI on Cloud Run)     │
                │                              │
                │  POST /payments     → init   │
                │  GET  /payments/:id → status │
                │  POST /refunds      → refund │
                └──────┬──────┬───────┬────────┘
                       │      │       │
            ┌──────────▼┐  ┌──▼────┐  ├──────────────┐
            │ PostgreSQL│  │ Redis │  │  Pub/Sub      │
            │ (Payments,│  │(Idemp,│  │ (Async Events)│
            │  Ledger)  │  │ Locks)│  │               │
            │ Cloud SQL │  │       │  └──────┬────────┘
            └───────────┘  └───────┘         │
                                      ┌──────▼────────┐
                                      │ Webhook Worker│
                                      │ (Cloud Run Job│
                                      │  / Consumer)  │
                                      └───────────────┘
```

The client sends a payment request to the API gateway, which enforces rate limits and forwards to the payment service. The payment service validates the request, checks the idempotency key in Redis, creates the payment record in PostgreSQL, executes the fund transfer within a database transaction (debit sender, credit receiver, write ledger entries), and publishes a payment event to Pub/Sub. The webhook worker consumes events and delivers HTTP callbacks to merchants. Redis stores idempotency keys (with TTL) and distributed locks (for preventing double-spend on concurrent requests for the same account).

---

## Project Setup

### Project Structure

```
payment-system/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # PostgreSQL connection and session
│   ├── idempotency.py        # Idempotency key management
│   ├── state_machine.py      # Payment state transitions
│   ├── ledger.py             # Double-entry bookkeeping
│   ├── saga.py               # Saga orchestrator for distributed txns
│   ├── webhooks.py           # Webhook dispatch and retry
│   ├── fraud.py              # Basic fraud detection rules
│   └── routers/
│       ├── __init__.py
│       ├── payments.py       # Payment endpoints
│       ├── refunds.py        # Refund endpoints
│       └── admin.py          # Health, metrics, admin endpoints
├── workers/
│   ├── __init__.py
│   ├── webhook_worker.py     # Pub/Sub consumer for webhook delivery
│   └── dlq_processor.py      # Dead letter queue processor
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_payments.py      # Tests for payment flow
│   ├── test_idempotency.py   # Tests for idempotency
│   ├── test_ledger.py        # Tests for ledger balancing
│   ├── test_refunds.py       # Tests for refund flow
│   └── test_state_machine.py # Tests for state transitions
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir payment-system && cd payment-system
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
pydantic-settings==2.5.2
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL where payments, ledger entries, and accounts are stored. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for Redis, used for idempotency keys and distributed locks. `httpx` is used for webhook delivery and testing. `pydantic-settings` handles environment variable loading.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/payments
REDIS_URL=redis://localhost:6379/0
IDEMPOTENCY_TTL_HOURS=24
WEBHOOK_TIMEOUT_SECONDS=10
WEBHOOK_MAX_RETRIES=5
MAX_TRANSACTION_AMOUNT=1000000
MIN_TRANSACTION_AMOUNT=1
DEFAULT_CURRENCY=USD
RATE_LIMIT_PER_MINUTE=120
PORT=8000
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the Redis instance for idempotency and locking. `IDEMPOTENCY_TTL_HOURS=24` keeps idempotency keys for 24 hours to cover retry windows. `WEBHOOK_TIMEOUT_SECONDS=10` is the HTTP timeout for each webhook delivery attempt. `WEBHOOK_MAX_RETRIES=5` retries failed webhook deliveries with exponential backoff. `MAX_TRANSACTION_AMOUNT` and `MIN_TRANSACTION_AMOUNT` enforce business limits on individual payments.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐
│             accounts                  │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ account_id    UUID       UNIQUE       │
│ user_id       VARCHAR(64) NOT NULL    │
│ balance       BIGINT     DEFAULT 0    │  ← stored in smallest unit (cents)
│ currency      VARCHAR(3) DEFAULT USD  │
│ status        VARCHAR(20) DEFAULT act │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
│ updated_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_user_id (user_id)           │
│ CHECK balance >= 0                    │
└──────────────────────────────────────┘
         │
         │ 1:N
         ▼
┌──────────────────────────────────────┐
│             payments                  │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ payment_id    UUID       UNIQUE       │
│ idempotency_key VARCHAR(64) UNIQUE   │
│ sender_id     UUID       FK→accounts │
│ receiver_id   UUID       FK→accounts │
│ amount        BIGINT     NOT NULL     │  ← in smallest unit (cents)
│ currency      VARCHAR(3) NOT NULL     │
│ status        VARCHAR(20) NOT NULL    │  ← PENDING,COMPLETED,FAILED,REFUNDED
│ description   TEXT                    │
│ metadata      JSONB                   │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
│ updated_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_sender (sender_id)          │
│ INDEX idx_receiver (receiver_id)      │
│ INDEX idx_idempotency (idemp_key)     │
│ INDEX idx_status (status)             │
└──────────────────────────────────────┘
         │
         │ 1:N
         ▼
┌──────────────────────────────────────┐
│          ledger_entries               │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ entry_id      UUID       UNIQUE       │
│ payment_id    UUID       FK→payments  │
│ account_id    UUID       FK→accounts  │
│ entry_type    VARCHAR(10) NOT NULL    │  ← DEBIT or CREDIT
│ amount        BIGINT     NOT NULL     │
│ balance_after BIGINT     NOT NULL     │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_payment (payment_id)        │
│ INDEX idx_account (account_id)        │
│ INDEX idx_created (created_at)        │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│         payment_events                │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ event_id      UUID       UNIQUE       │
│ payment_id    UUID       FK→payments  │
│ event_type    VARCHAR(30) NOT NULL    │  ← INITIATED,COMPLETED,FAILED,...
│ payload       JSONB                   │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_event_payment (payment_id)  │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│         webhook_deliveries            │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ delivery_id   UUID       UNIQUE       │
│ payment_id    UUID       FK→payments  │
│ url           TEXT       NOT NULL     │
│ status        VARCHAR(20) NOT NULL    │  ← PENDING,DELIVERED,FAILED
│ attempts      INT        DEFAULT 0    │
│ last_attempt  TIMESTAMPTZ             │
│ next_retry    TIMESTAMPTZ             │
│ response_code INT                     │
│ response_body TEXT                    │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_wh_status (status)          │
│ INDEX idx_wh_next_retry (next_retry)  │
└──────────────────────────────────────┘
```

Money is stored as `BIGINT` in the smallest currency unit (cents for USD, paise for INR). This avoids floating-point precision errors — `$19.99` is stored as `1999`. The `CHECK balance >= 0` constraint on `accounts` enforces at the database level that no account can go negative, acting as a final safety net even if application logic has bugs. `idempotency_key` on `payments` has a unique index — duplicate inserts with the same key will fail with a constraint violation, which is the foundation of exactly-once processing. `ledger_entries` records every debit and credit as a separate row, enabling double-entry bookkeeping and auditing. `balance_after` captures the account balance at the time of the entry, creating a chain that can be verified for consistency.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/payments"
    redis_url: str = "redis://localhost:6379/0"
    idempotency_ttl_hours: int = 24
    webhook_timeout_seconds: int = 10
    webhook_max_retries: int = 5
    max_transaction_amount: int = 1_000_000       # In cents ($10,000.00)
    min_transaction_amount: int = 1               # In cents ($0.01)
    default_currency: str = "USD"
    rate_limit_per_minute: int = 120
    port: int = 8000

    class Config:
        env_file = ".env"


settings = Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads values from environment variables and `.env` files. Field names are case-insensitive and map to the corresponding `ENV_VAR` names.

### `app/database.py`

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings

engine = create_engine(
    settings.database_url,
    pool_size=20,                                 # Max persistent connections
    max_overflow=10,                              # Extra connections under burst
    pool_pre_ping=True,                           # Detect stale connections
    isolation_level="READ COMMITTED",             # Prevent dirty reads
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

`isolation_level="READ COMMITTED"` is the standard for payment systems — it prevents dirty reads while allowing concurrent transactions. We use `SELECT ... FOR UPDATE` for row-level locks during balance updates instead of `SERIALIZABLE` isolation, which would cause excessive retries under high concurrency. `pool_size=20` keeps 20 connections open and reusable. `pool_pre_ping=True` detects stale connections from cloud databases that close idle connections.

### `app/models.py`

```python
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, DateTime,
    ForeignKey, Index, Integer, String, Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from app.database import Base


def generate_uuid():
    return str(uuid.uuid4())


class Account(Base):
    __tablename__ = "accounts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_id = Column(
        String(36), unique=True, nullable=False, default=generate_uuid
    )
    user_id = Column(String(64), nullable=False, index=True)
    balance = Column(BigInteger, default=0)               # In cents
    currency = Column(String(3), default="USD")
    status = Column(String(20), default="active")
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        CheckConstraint("balance >= 0", name="check_non_negative_balance"),
        Index("idx_user_id", "user_id"),
    )


class Payment(Base):
    __tablename__ = "payments"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    payment_id = Column(
        String(36), unique=True, nullable=False, default=generate_uuid
    )
    idempotency_key = Column(String(64), unique=True, nullable=False)
    sender_id = Column(
        String(36), ForeignKey("accounts.account_id"), nullable=False
    )
    receiver_id = Column(
        String(36), ForeignKey("accounts.account_id"), nullable=False
    )
    amount = Column(BigInteger, nullable=False)            # In cents
    currency = Column(String(3), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    description = Column(Text, nullable=True)
    metadata_ = Column("metadata", JSONB, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    events = relationship("PaymentEvent", back_populates="payment")
    ledger_entries = relationship("LedgerEntry", back_populates="payment")

    __table_args__ = (
        Index("idx_sender", "sender_id"),
        Index("idx_receiver", "receiver_id"),
        Index("idx_status", "status"),
    )


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entry_id = Column(
        String(36), unique=True, nullable=False, default=generate_uuid
    )
    payment_id = Column(
        String(36), ForeignKey("payments.payment_id"), nullable=False
    )
    account_id = Column(
        String(36), ForeignKey("accounts.account_id"), nullable=False
    )
    entry_type = Column(String(10), nullable=False)        # DEBIT or CREDIT
    amount = Column(BigInteger, nullable=False)
    balance_after = Column(BigInteger, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    payment = relationship("Payment", back_populates="ledger_entries")

    __table_args__ = (
        Index("idx_ledger_payment", "payment_id"),
        Index("idx_ledger_account", "account_id"),
        Index("idx_ledger_created", "created_at"),
    )


class PaymentEvent(Base):
    __tablename__ = "payment_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_id = Column(
        String(36), unique=True, nullable=False, default=generate_uuid
    )
    payment_id = Column(
        String(36), ForeignKey("payments.payment_id"), nullable=False
    )
    event_type = Column(String(30), nullable=False)
    payload = Column(JSONB, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    payment = relationship("Payment", back_populates="events")

    __table_args__ = (
        Index("idx_event_payment", "payment_id"),
    )


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    delivery_id = Column(
        String(36), unique=True, nullable=False, default=generate_uuid
    )
    payment_id = Column(
        String(36), ForeignKey("payments.payment_id"), nullable=False
    )
    url = Column(Text, nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    attempts = Column(Integer, default=0)
    last_attempt = Column(DateTime(timezone=True), nullable=True)
    next_retry = Column(DateTime(timezone=True), nullable=True)
    response_code = Column(Integer, nullable=True)
    response_body = Column(Text, nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("idx_wh_status", "status"),
        Index("idx_wh_next_retry", "next_retry"),
    )
```

`BigInteger` for `id` supports billions of rows. `String(36)` for UUIDs stores the standard UUID format. `JSONB` stores arbitrary metadata (e.g., merchant order ID, customer notes) in a query-friendly format. `CheckConstraint("balance >= 0")` is the last line of defense against overdrafts — even if application logic has a bug, the database rejects negative balances. `relationship()` sets up ORM-level links for navigating between related records. Column `metadata` is mapped as `metadata_` in Python to avoid conflicts with SQLAlchemy's internal `metadata` attribute.

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, Field


class PaymentCreate(BaseModel):
    idempotency_key: str = Field(
        ..., min_length=8, max_length=64,
        description="Client-generated unique key for exactly-once processing",
    )
    sender_account_id: str
    receiver_account_id: str
    amount: int = Field(..., gt=0, description="Amount in smallest currency unit (cents)")
    currency: str = Field(default="USD", max_length=3)
    description: str | None = None
    metadata: dict | None = None
    webhook_url: str | None = None                # Optional callback URL


class PaymentResponse(BaseModel):
    payment_id: str
    idempotency_key: str
    sender_account_id: str
    receiver_account_id: str
    amount: int
    currency: str
    status: str
    description: str | None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class RefundCreate(BaseModel):
    idempotency_key: str = Field(..., min_length=8, max_length=64)
    payment_id: str
    amount: int | None = Field(
        None, gt=0, description="Partial refund amount; omit for full refund"
    )
    reason: str | None = None


class RefundResponse(BaseModel):
    refund_payment_id: str
    original_payment_id: str
    refund_amount: int
    status: str
    created_at: datetime


class AccountCreate(BaseModel):
    user_id: str
    initial_balance: int = Field(default=0, ge=0)
    currency: str = Field(default="USD", max_length=3)


class AccountResponse(BaseModel):
    account_id: str
    user_id: str
    balance: int
    currency: str
    status: str
    created_at: datetime

    class Config:
        from_attributes = True


class LedgerEntryResponse(BaseModel):
    entry_id: str
    payment_id: str
    account_id: str
    entry_type: str
    amount: int
    balance_after: int
    created_at: datetime

    class Config:
        from_attributes = True


class PaymentEventResponse(BaseModel):
    event_id: str
    payment_id: str
    event_type: str
    payload: dict | None
    created_at: datetime

    class Config:
        from_attributes = True
```

`Field(..., gt=0)` ensures the amount is strictly positive — you cannot create a zero-value or negative payment. `idempotency_key` is required and must be 8-64 characters, client-generated (typically a UUID). `from_attributes = True` (formerly `orm_mode`) tells Pydantic to read data from ORM model attributes instead of expecting a dictionary. `RefundCreate.amount` is optional — omit it for a full refund, or specify a positive integer for a partial refund.

---

## Idempotency & Exactly-Once Processing

Idempotency is the single most critical property of a payment system. Network failures, client retries, and load balancer re-routes mean the same request can arrive multiple times. Without idempotency, a single payment could charge a user twice.

### How It Works

```
Request 1: POST /payments {idempotency_key: "abc-123", amount: 5000}
  → Key "abc-123" not in Redis → process payment → store key + response in Redis → return 201

Request 2: POST /payments {idempotency_key: "abc-123", amount: 5000}   (retry)
  → Key "abc-123" found in Redis → return cached response → return 200 (not 201)
```

The client generates a unique idempotency key for each intended payment (typically a UUID). On the first request, the key is stored in Redis along with the response. On any subsequent request with the same key, the cached response is returned without re-executing the payment logic. The key has a TTL (24 hours) to avoid infinite storage growth.

### `app/idempotency.py`

```python
import json
import logging

import redis

from app.config import settings

logger = logging.getLogger(__name__)

redis_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)

IDEMPOTENCY_PREFIX = "idemp:"
LOCK_PREFIX = "lock:"
TTL_SECONDS = settings.idempotency_ttl_hours * 3600


def check_idempotency(key: str) -> dict | None:
    """
    Check if a request with this idempotency key has already been processed.
    Returns the cached response if found, None otherwise.
    """
    data = redis_client.get(f"{IDEMPOTENCY_PREFIX}{key}")
    if data:
        return json.loads(data)
    return None


def store_idempotency(key: str, response: dict) -> None:
    """
    Store the response for an idempotency key with a TTL.
    Subsequent requests with the same key return this cached response.
    """
    redis_client.setex(
        f"{IDEMPOTENCY_PREFIX}{key}",
        TTL_SECONDS,
        json.dumps(response, default=str),
    )


def acquire_processing_lock(key: str, timeout: int = 30) -> bool:
    """
    Acquire a distributed lock for processing a payment.
    Prevents two instances from processing the same idempotency key
    simultaneously (e.g., rapid duplicate requests hitting different pods).
    Uses Redis SET NX EX (set if not exists, with expiry).
    """
    return redis_client.set(
        f"{LOCK_PREFIX}{key}",
        "locked",
        nx=True,                                  # Only set if key doesn't exist
        ex=timeout,                               # Auto-expire after 30 seconds
    )


def release_processing_lock(key: str) -> None:
    """Release the distributed lock after processing completes."""
    redis_client.delete(f"{LOCK_PREFIX}{key}")
```

`redis.from_url` parses the connection string and creates a connection pool. `check_idempotency` looks up the key in Redis — if found, the payment was already processed and the stored response is returned. `store_idempotency` saves the response with a TTL so keys are automatically cleaned up. `acquire_processing_lock` uses Redis's `SET NX EX` (set-if-not-exists with expiry) to implement a distributed lock. This prevents a race condition where two instances receive the same idempotency key within milliseconds and both start processing. The `ex=30` timeout ensures the lock is automatically released if the process crashes, preventing deadlocks. The combination of idempotency check + lock + idempotency store forms the three-step exactly-once pattern.

---

## Core API – Payments & Transfers

### `app/routers/payments.py`

```python
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.idempotency import (
    acquire_processing_lock,
    check_idempotency,
    release_processing_lock,
    store_idempotency,
)
from app.ledger import create_ledger_entries
from app.models import Account, Payment, PaymentEvent, WebhookDelivery
from app.schemas import (
    AccountCreate,
    AccountResponse,
    PaymentCreate,
    PaymentResponse,
    PaymentEventResponse,
    LedgerEntryResponse,
)
from app.state_machine import PaymentStatus, validate_transition

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/accounts", response_model=AccountResponse, status_code=201)
def create_account(payload: AccountCreate, db: Session = Depends(get_db)):
    """Create a new payment account."""
    account = Account(
        user_id=payload.user_id,
        balance=payload.initial_balance,
        currency=payload.currency,
    )
    db.add(account)
    db.commit()
    db.refresh(account)
    return account


@router.get("/accounts/{account_id}", response_model=AccountResponse)
def get_account(account_id: str, db: Session = Depends(get_db)):
    """Get account details and current balance."""
    account = (
        db.query(Account)
        .filter(Account.account_id == account_id)
        .first()
    )
    if not account:
        raise HTTPException(status_code=404, detail="Account not found.")
    return account


@router.post("/payments", response_model=PaymentResponse, status_code=201)
def create_payment(
    payload: PaymentCreate,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Create a new payment (money transfer).

    This endpoint implements exactly-once semantics through:
    1. Idempotency check (Redis) — return cached result for duplicate keys
    2. Distributed lock (Redis) — prevent concurrent processing of same key
    3. Database transaction — atomic debit + credit + ledger entries
    4. Idempotency store (Redis) — cache the result for future duplicates
    """

    # ── Step 1: Check idempotency ──────────────────────────────────
    cached = check_idempotency(payload.idempotency_key)
    if cached:
        logger.info(f"Idempotent hit: {payload.idempotency_key}")
        return PaymentResponse(**cached)

    # ── Step 2: Acquire processing lock ────────────────────────────
    if not acquire_processing_lock(payload.idempotency_key):
        raise HTTPException(
            status_code=409,
            detail="Payment is already being processed. Retry shortly.",
        )

    try:
        # ── Step 3: Validate business rules ────────────────────────
        if payload.amount < settings.min_transaction_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Amount below minimum: {settings.min_transaction_amount}.",
            )

        if payload.amount > settings.max_transaction_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Amount exceeds maximum: {settings.max_transaction_amount}.",
            )

        if payload.sender_account_id == payload.receiver_account_id:
            raise HTTPException(
                status_code=400,
                detail="Cannot transfer to the same account.",
            )

        # ── Step 4: Lock accounts and execute transfer ─────────────
        # SELECT ... FOR UPDATE locks the rows until the transaction commits,
        # preventing concurrent transactions from reading stale balances.
        sender = (
            db.query(Account)
            .filter(Account.account_id == payload.sender_account_id)
            .with_for_update()                    # Row-level lock
            .first()
        )

        if not sender:
            raise HTTPException(status_code=404, detail="Sender account not found.")

        if sender.status != "active":
            raise HTTPException(status_code=400, detail="Sender account is not active.")

        if sender.balance < payload.amount:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient balance. Available: {sender.balance}, "
                       f"required: {payload.amount}.",
            )

        receiver = (
            db.query(Account)
            .filter(Account.account_id == payload.receiver_account_id)
            .with_for_update()
            .first()
        )

        if not receiver:
            raise HTTPException(status_code=404, detail="Receiver account not found.")

        if receiver.status != "active":
            raise HTTPException(
                status_code=400, detail="Receiver account is not active."
            )

        # ── Step 5: Create payment record ──────────────────────────
        payment = Payment(
            idempotency_key=payload.idempotency_key,
            sender_id=payload.sender_account_id,
            receiver_id=payload.receiver_account_id,
            amount=payload.amount,
            currency=payload.currency,
            status=PaymentStatus.PENDING.value,
            description=payload.description,
            metadata_=payload.metadata,
        )
        db.add(payment)
        db.flush()                                # Get payment_id

        # ── Step 6: Execute the transfer ───────────────────────────
        sender.balance -= payload.amount
        receiver.balance += payload.amount

        # ── Step 7: Create ledger entries ──────────────────────────
        create_ledger_entries(
            db=db,
            payment_id=payment.payment_id,
            sender_account_id=sender.account_id,
            receiver_account_id=receiver.account_id,
            amount=payload.amount,
            sender_balance_after=sender.balance,
            receiver_balance_after=receiver.balance,
        )

        # ── Step 8: Update payment status ──────────────────────────
        payment.status = PaymentStatus.COMPLETED.value

        # ── Step 9: Record payment event ───────────────────────────
        event = PaymentEvent(
            payment_id=payment.payment_id,
            event_type="PAYMENT_COMPLETED",
            payload={
                "amount": payload.amount,
                "currency": payload.currency,
                "sender": payload.sender_account_id,
                "receiver": payload.receiver_account_id,
            },
        )
        db.add(event)

        # ── Step 10: Queue webhook if configured ───────────────────
        if payload.webhook_url:
            webhook = WebhookDelivery(
                payment_id=payment.payment_id,
                url=payload.webhook_url,
                status="PENDING",
            )
            db.add(webhook)

        # ── Step 11: Commit everything atomically ──────────────────
        db.commit()
        db.refresh(payment)

        # ── Step 12: Cache response for idempotency ────────────────
        response = PaymentResponse(
            payment_id=payment.payment_id,
            idempotency_key=payment.idempotency_key,
            sender_account_id=payment.sender_id,
            receiver_account_id=payment.receiver_id,
            amount=payment.amount,
            currency=payment.currency,
            status=payment.status,
            description=payment.description,
            created_at=payment.created_at,
            updated_at=payment.updated_at,
        )

        store_idempotency(payload.idempotency_key, response.model_dump())

        return response

    except HTTPException:
        db.rollback()
        raise

    except Exception as e:
        db.rollback()
        logger.error(f"Payment failed: {e}")

        # Record failure event if payment was created
        try:
            if payment and payment.payment_id:
                payment.status = PaymentStatus.FAILED.value
                fail_event = PaymentEvent(
                    payment_id=payment.payment_id,
                    event_type="PAYMENT_FAILED",
                    payload={"error": str(e)},
                )
                db.add(fail_event)
                db.commit()
        except Exception:
            pass

        raise HTTPException(status_code=500, detail="Payment processing failed.")

    finally:
        release_processing_lock(payload.idempotency_key)


@router.get("/payments/{payment_id}", response_model=PaymentResponse)
def get_payment(payment_id: str, db: Session = Depends(get_db)):
    """Get the current status of a payment."""
    payment = (
        db.query(Payment)
        .filter(Payment.payment_id == payment_id)
        .first()
    )
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found.")

    return PaymentResponse(
        payment_id=payment.payment_id,
        idempotency_key=payment.idempotency_key,
        sender_account_id=payment.sender_id,
        receiver_account_id=payment.receiver_id,
        amount=payment.amount,
        currency=payment.currency,
        status=payment.status,
        description=payment.description,
        created_at=payment.created_at,
        updated_at=payment.updated_at,
    )


@router.get(
    "/payments/{payment_id}/events",
    response_model=list[PaymentEventResponse],
)
def get_payment_events(payment_id: str, db: Session = Depends(get_db)):
    """Get the event history (audit trail) of a payment."""
    payment = (
        db.query(Payment)
        .filter(Payment.payment_id == payment_id)
        .first()
    )
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found.")

    events = (
        db.query(PaymentEvent)
        .filter(PaymentEvent.payment_id == payment_id)
        .order_by(PaymentEvent.created_at.asc())
        .all()
    )
    return events


@router.get(
    "/payments/{payment_id}/ledger",
    response_model=list[LedgerEntryResponse],
)
def get_payment_ledger(payment_id: str, db: Session = Depends(get_db)):
    """Get the ledger entries (debit + credit) for a payment."""
    from app.models import LedgerEntry

    entries = (
        db.query(LedgerEntry)
        .filter(LedgerEntry.payment_id == payment_id)
        .order_by(LedgerEntry.created_at.asc())
        .all()
    )
    if not entries:
        raise HTTPException(status_code=404, detail="No ledger entries found.")
    return entries
```

The 12-step payment flow is the heart of the system. `with_for_update()` acquires a PostgreSQL row-level lock (`SELECT ... FOR UPDATE`) on the sender and receiver accounts. This prevents other transactions from reading or modifying these rows until the current transaction commits, eliminating the double-spend race condition where two concurrent payments both see sufficient balance. Everything from step 5 to step 11 executes within a single database transaction — if any step fails, the entire transaction rolls back: no money is moved, no ledger entries are created, no events are recorded. The idempotency response is cached in Redis only **after** the database commit succeeds (step 12), ensuring the cached response always reflects a completed transaction.

---

## Payment State Machine

Every payment moves through a well-defined set of states. Invalid transitions (e.g., from FAILED to COMPLETED) are rejected. This prevents bugs and makes the system auditable.

### State Transition Diagram

```
                    ┌───────────┐
                    │  PENDING  │
                    └─────┬─────┘
                          │
                 ┌────────┴────────┐
                 │                 │
           ┌─────▼─────┐    ┌─────▼─────┐
           │ COMPLETED │    │  FAILED   │
           └─────┬─────┘    └───────────┘
                 │
        ┌────────┴────────┐
        │                 │
  ┌─────▼──────┐  ┌──────▼──────────┐
  │  REFUNDED  │  │ PARTIALLY       │
  │            │  │ REFUNDED        │
  └────────────┘  └─────────────────┘
```

### `app/state_machine.py`

```python
from enum import Enum

from fastapi import HTTPException


class PaymentStatus(Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"
    PARTIALLY_REFUNDED = "PARTIALLY_REFUNDED"


# Define allowed transitions: current_state → set of valid next states
VALID_TRANSITIONS: dict[PaymentStatus, set[PaymentStatus]] = {
    PaymentStatus.PENDING: {
        PaymentStatus.COMPLETED,
        PaymentStatus.FAILED,
    },
    PaymentStatus.COMPLETED: {
        PaymentStatus.REFUNDED,
        PaymentStatus.PARTIALLY_REFUNDED,
    },
    PaymentStatus.PARTIALLY_REFUNDED: {
        PaymentStatus.REFUNDED,                   # Additional refunds until full
    },
    PaymentStatus.FAILED: set(),                  # Terminal state — no transitions
    PaymentStatus.REFUNDED: set(),                # Terminal state — no transitions
}


def validate_transition(
    current: PaymentStatus,
    target: PaymentStatus,
) -> None:
    """
    Validate that a state transition is allowed.
    Raises HTTPException if the transition is invalid.
    """
    allowed = VALID_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise HTTPException(
            status_code=409,
            detail=f"Invalid state transition: {current.value} → {target.value}. "
                   f"Allowed transitions from {current.value}: "
                   f"{[s.value for s in allowed] if allowed else 'none (terminal state)'}.",
        )


def is_terminal(status: PaymentStatus) -> bool:
    """Check if a payment status is a terminal (final) state."""
    return status in {PaymentStatus.FAILED, PaymentStatus.REFUNDED}
```

The state machine is a simple directed graph. `VALID_TRANSITIONS` maps each state to the set of states it can transition to. `FAILED` and `REFUNDED` are terminal states with no outgoing transitions. `validate_transition` is called before any status update to ensure the transition is legal. This prevents bugs where a failed payment could be marked as completed, or a fully refunded payment could be refunded again.

---

## Saga Pattern – Distributed Transactions

In a microservices architecture, a single payment may span multiple services (payment, wallet, fraud, notification). The **Saga pattern** breaks the transaction into a sequence of local transactions, each with a compensating action that undoes it if a later step fails.

### Saga Flow

```
Step 1: Validate Fraud         → Compensate: Release fraud hold
Step 2: Debit Sender           → Compensate: Credit Sender (reverse debit)
Step 3: Credit Receiver        → Compensate: Debit Receiver (reverse credit)
Step 4: Record Ledger          → Compensate: Delete ledger entries
Step 5: Send Notification      → (no compensation needed)

If Step 3 fails → run compensations for Step 2, Step 1 (in reverse order)
```

### `app/saga.py`

```python
import logging
from dataclasses import dataclass, field
from typing import Callable

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class SagaStep:
    """A single step in a saga with an action and a compensation."""
    name: str
    action: Callable[[Session], None]
    compensation: Callable[[Session], None]


@dataclass
class SagaOrchestrator:
    """
    Orchestrates a saga: executes steps in order.
    If any step fails, compensations are run in reverse order.
    """
    steps: list[SagaStep] = field(default_factory=list)
    completed_steps: list[SagaStep] = field(default_factory=list)

    def add_step(
        self,
        name: str,
        action: Callable[[Session], None],
        compensation: Callable[[Session], None],
    ) -> None:
        """Add a step to the saga."""
        self.steps.append(SagaStep(name=name, action=action, compensation=compensation))

    def execute(self, db: Session) -> None:
        """
        Execute the saga. If any step fails, run compensations
        for all previously completed steps in reverse order.
        """
        for step in self.steps:
            try:
                logger.info(f"Saga: Executing step '{step.name}'")
                step.action(db)
                self.completed_steps.append(step)
            except Exception as e:
                logger.error(f"Saga: Step '{step.name}' failed: {e}")
                self._compensate(db)
                raise

    def _compensate(self, db: Session) -> None:
        """Run compensations in reverse order for completed steps."""
        for step in reversed(self.completed_steps):
            try:
                logger.info(f"Saga: Compensating step '{step.name}'")
                step.compensation(db)
            except Exception as comp_error:
                # Compensation failures are critical — log and continue
                logger.critical(
                    f"Saga: Compensation for '{step.name}' FAILED: {comp_error}. "
                    f"Manual intervention required."
                )


def build_payment_saga(
    sender_account_id: str,
    receiver_account_id: str,
    amount: int,
    payment_id: str,
) -> SagaOrchestrator:
    """
    Build a saga for a payment transfer.
    Each step has an action and a compensating action.
    """
    from app.models import Account, LedgerEntry

    saga = SagaOrchestrator()

    # Step 1: Validate and hold sender funds
    def debit_sender(db: Session):
        sender = (
            db.query(Account)
            .filter(Account.account_id == sender_account_id)
            .with_for_update()
            .first()
        )
        if sender.balance < amount:
            raise ValueError("Insufficient balance")
        sender.balance -= amount
        db.flush()

    def reverse_debit(db: Session):
        sender = (
            db.query(Account)
            .filter(Account.account_id == sender_account_id)
            .with_for_update()
            .first()
        )
        sender.balance += amount
        db.flush()

    saga.add_step("debit_sender", debit_sender, reverse_debit)

    # Step 2: Credit receiver
    def credit_receiver(db: Session):
        receiver = (
            db.query(Account)
            .filter(Account.account_id == receiver_account_id)
            .with_for_update()
            .first()
        )
        receiver.balance += amount
        db.flush()

    def reverse_credit(db: Session):
        receiver = (
            db.query(Account)
            .filter(Account.account_id == receiver_account_id)
            .with_for_update()
            .first()
        )
        receiver.balance -= amount
        db.flush()

    saga.add_step("credit_receiver", credit_receiver, reverse_credit)

    return saga
```

The `SagaOrchestrator` executes steps sequentially. If step N fails, it runs compensations for steps N-1, N-2, ..., 1 in reverse order. Each compensation undoes the effect of its step. This pattern is essential in microservices where a single distributed transaction across databases is not possible. In the monolith version (this project), the Saga pattern adds an extra layer of safety — if the credit fails after the debit, the compensation reverses the debit. In production, compensation failures are logged as `CRITICAL` because they represent a money inconsistency that requires manual intervention (a human operator must reconcile the accounts).

---

## Webhook & Callback System

Merchants need to know when a payment completes, fails, or is refunded. The webhook system delivers HTTP callbacks with retry logic and exponential backoff.

### `app/webhooks.py`

```python
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Payment, PaymentEvent, WebhookDelivery

logger = logging.getLogger(__name__)

WEBHOOK_SECRET = "whsec_your_secret_key_here"     # In production, use Secret Manager

# Exponential backoff intervals: 1min, 5min, 30min, 2hr, 24hr
RETRY_INTERVALS = [60, 300, 1800, 7200, 86400]


def generate_signature(payload: str) -> str:
    """
    Generate an HMAC-SHA256 signature for the webhook payload.
    The merchant verifies this signature to ensure the webhook
    came from our system and was not tampered with.
    """
    return hmac.new(
        WEBHOOK_SECRET.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()


def build_webhook_payload(payment: Payment, event_type: str) -> dict:
    """Build the JSON payload for a webhook delivery."""
    return {
        "event_type": event_type,
        "payment_id": payment.payment_id,
        "amount": payment.amount,
        "currency": payment.currency,
        "status": payment.status,
        "sender_account_id": payment.sender_id,
        "receiver_account_id": payment.receiver_id,
        "created_at": payment.created_at.isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def deliver_webhook(delivery: WebhookDelivery, db: Session) -> bool:
    """
    Attempt to deliver a webhook. Returns True on success.
    Updates the delivery record with the result.
    """
    payment = (
        db.query(Payment)
        .filter(Payment.payment_id == delivery.payment_id)
        .first()
    )

    if not payment:
        logger.error(f"Payment not found for webhook: {delivery.payment_id}")
        delivery.status = "FAILED"
        db.commit()
        return False

    payload = build_webhook_payload(payment, "payment.completed")
    payload_json = json.dumps(payload, default=str)
    signature = generate_signature(payload_json)

    try:
        response = httpx.post(
            delivery.url,
            content=payload_json,
            headers={
                "Content-Type": "application/json",
                "X-Webhook-Signature": signature,
                "X-Webhook-ID": delivery.delivery_id,
            },
            timeout=settings.webhook_timeout_seconds,
        )

        delivery.attempts += 1
        delivery.last_attempt = datetime.now(timezone.utc)
        delivery.response_code = response.status_code
        delivery.response_body = response.text[:1000]     # Cap stored response

        if 200 <= response.status_code < 300:
            delivery.status = "DELIVERED"
            logger.info(f"Webhook delivered: {delivery.delivery_id}")
            db.commit()
            return True

        # Non-2xx response — schedule retry
        logger.warning(
            f"Webhook returned {response.status_code}: {delivery.delivery_id}"
        )

    except httpx.TimeoutException:
        delivery.attempts += 1
        delivery.last_attempt = datetime.now(timezone.utc)
        delivery.response_code = None
        delivery.response_body = "Timeout"
        logger.warning(f"Webhook timed out: {delivery.delivery_id}")

    except Exception as e:
        delivery.attempts += 1
        delivery.last_attempt = datetime.now(timezone.utc)
        delivery.response_body = str(e)[:1000]
        logger.error(f"Webhook delivery error: {e}")

    # Schedule retry with exponential backoff
    if delivery.attempts < settings.webhook_max_retries:
        retry_index = min(delivery.attempts - 1, len(RETRY_INTERVALS) - 1)
        next_interval = RETRY_INTERVALS[retry_index]
        delivery.next_retry = datetime.now(timezone.utc) + timedelta(
            seconds=next_interval
        )
        delivery.status = "PENDING"
        logger.info(
            f"Webhook retry scheduled in {next_interval}s: {delivery.delivery_id}"
        )
    else:
        delivery.status = "FAILED"
        logger.error(
            f"Webhook permanently failed after {delivery.attempts} attempts: "
            f"{delivery.delivery_id}"
        )

    db.commit()
    return False
```

`generate_signature` creates an HMAC-SHA256 hash of the payload using a shared secret. The merchant verifies this signature to confirm the webhook is authentic and unmodified. `RETRY_INTERVALS` implements exponential backoff: 1 minute, 5 minutes, 30 minutes, 2 hours, 24 hours. This gives the merchant's server time to recover from outages without overwhelming it with retries. `response_body` is capped at 1000 characters to prevent large error pages from bloating the database. After the maximum number of retries (5), the delivery is marked `FAILED` and moves to the dead letter queue for manual investigation.

---

## Ledger & Double-Entry Bookkeeping

Every money movement must be recorded as a pair of entries: a debit (money leaves an account) and a credit (money enters an account). The sum of all debits must always equal the sum of all credits. This is the foundation of financial accounting and makes the system auditable.

### Double-Entry Principle

```
Payment: Alice sends $50.00 to Bob

Ledger Entry 1 (DEBIT):   Alice's account  -$50.00  (balance: $150.00 → $100.00)
Ledger Entry 2 (CREDIT):  Bob's account    +$50.00  (balance: $200.00 → $250.00)

Invariant: SUM(debits) = SUM(credits) — always
```

### `app/ledger.py`

```python
import logging

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import Account, LedgerEntry

logger = logging.getLogger(__name__)


def create_ledger_entries(
    db: Session,
    payment_id: str,
    sender_account_id: str,
    receiver_account_id: str,
    amount: int,
    sender_balance_after: int,
    receiver_balance_after: int,
) -> tuple[LedgerEntry, LedgerEntry]:
    """
    Create a pair of ledger entries for a payment.
    Always creates exactly two entries: one DEBIT and one CREDIT.
    """
    debit_entry = LedgerEntry(
        payment_id=payment_id,
        account_id=sender_account_id,
        entry_type="DEBIT",
        amount=amount,
        balance_after=sender_balance_after,
    )

    credit_entry = LedgerEntry(
        payment_id=payment_id,
        account_id=receiver_account_id,
        entry_type="CREDIT",
        amount=amount,
        balance_after=receiver_balance_after,
    )

    db.add(debit_entry)
    db.add(credit_entry)
    db.flush()

    return debit_entry, credit_entry


def verify_ledger_balance(db: Session) -> dict:
    """
    Verify the fundamental accounting equation:
    SUM(all debits) must equal SUM(all credits).
    Returns verification result with totals.
    """
    total_debits = (
        db.query(func.sum(LedgerEntry.amount))
        .filter(LedgerEntry.entry_type == "DEBIT")
        .scalar() or 0
    )

    total_credits = (
        db.query(func.sum(LedgerEntry.amount))
        .filter(LedgerEntry.entry_type == "CREDIT")
        .scalar() or 0
    )

    is_balanced = total_debits == total_credits

    if not is_balanced:
        logger.critical(
            f"LEDGER IMBALANCE: debits={total_debits}, credits={total_credits}, "
            f"difference={total_debits - total_credits}"
        )

    return {
        "total_debits": total_debits,
        "total_credits": total_credits,
        "difference": total_debits - total_credits,
        "is_balanced": is_balanced,
    }


def get_account_ledger(
    db: Session,
    account_id: str,
    limit: int = 50,
) -> list[LedgerEntry]:
    """Get the ledger history for a specific account."""
    return (
        db.query(LedgerEntry)
        .filter(LedgerEntry.account_id == account_id)
        .order_by(LedgerEntry.created_at.desc())
        .limit(limit)
        .all()
    )


def reconcile_account_balance(db: Session, account_id: str) -> dict:
    """
    Reconcile an account's balance by replaying its ledger entries.
    The computed balance should match the stored balance.
    """
    account = (
        db.query(Account)
        .filter(Account.account_id == account_id)
        .first()
    )

    if not account:
        return {"error": "Account not found"}

    # Sum all credits (money in) minus all debits (money out)
    credits = (
        db.query(func.sum(LedgerEntry.amount))
        .filter(LedgerEntry.account_id == account_id)
        .filter(LedgerEntry.entry_type == "CREDIT")
        .scalar() or 0
    )

    debits = (
        db.query(func.sum(LedgerEntry.amount))
        .filter(LedgerEntry.account_id == account_id)
        .filter(LedgerEntry.entry_type == "DEBIT")
        .scalar() or 0
    )

    computed_balance = credits - debits
    stored_balance = account.balance

    if computed_balance != stored_balance:
        logger.critical(
            f"BALANCE MISMATCH for account {account_id}: "
            f"stored={stored_balance}, computed={computed_balance}"
        )

    return {
        "account_id": account_id,
        "stored_balance": stored_balance,
        "computed_balance": computed_balance,
        "total_credits": credits,
        "total_debits": debits,
        "is_consistent": computed_balance == stored_balance,
    }
```

`create_ledger_entries` always creates exactly two entries — a DEBIT and a CREDIT — ensuring the double-entry invariant is maintained at the code level. `verify_ledger_balance` checks the global invariant: the sum of all debits across all accounts must equal the sum of all credits. Any imbalance is logged as `CRITICAL` because it indicates lost or created money. `reconcile_account_balance` verifies a single account by replaying its entire ledger history and comparing the computed balance against the stored balance. Run this as a periodic health check (e.g., daily via Cloud Scheduler) to catch discrepancies early. In production, a ledger imbalance triggers an immediate PagerDuty alert.

---

## Retry & Dead Letter Queue

Failed operations (webhook deliveries, external API calls) must be retried with backoff. After exhausting retries, failures move to a dead letter queue (DLQ) for manual investigation.

### `workers/webhook_worker.py`

```python
import logging
import time
from datetime import datetime, timezone

from app.database import SessionLocal
from app.models import WebhookDelivery
from app.webhooks import deliver_webhook

logger = logging.getLogger(__name__)


def process_pending_webhooks() -> int:
    """
    Process all pending webhooks that are due for delivery or retry.
    Returns the number of webhooks processed.
    """
    db = SessionLocal()
    processed = 0

    try:
        now = datetime.now(timezone.utc)

        # Fetch webhooks that are pending and due for delivery/retry
        pending = (
            db.query(WebhookDelivery)
            .filter(WebhookDelivery.status == "PENDING")
            .filter(
                (WebhookDelivery.next_retry == None)          # New deliveries
                | (WebhookDelivery.next_retry <= now)         # Due for retry
            )
            .order_by(WebhookDelivery.created_at.asc())
            .limit(100)                                       # Batch size
            .all()
        )

        for delivery in pending:
            deliver_webhook(delivery, db)
            processed += 1

    except Exception as e:
        logger.error(f"Webhook worker error: {e}")
    finally:
        db.close()

    return processed


def run_webhook_worker(poll_interval: int = 5) -> None:
    """
    Continuously poll for pending webhooks and deliver them.
    Designed to run as a separate process or Cloud Run Job.
    """
    logger.info("Webhook worker started.")

    while True:
        try:
            processed = process_pending_webhooks()
            if processed > 0:
                logger.info(f"Processed {processed} webhooks.")
        except Exception as e:
            logger.error(f"Webhook worker loop error: {e}")

        time.sleep(poll_interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_webhook_worker()
```

### `workers/dlq_processor.py`

```python
import logging
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import WebhookDelivery

logger = logging.getLogger(__name__)


def get_failed_deliveries(db: Session, limit: int = 100) -> list[WebhookDelivery]:
    """Get all permanently failed webhook deliveries (DLQ)."""
    return (
        db.query(WebhookDelivery)
        .filter(WebhookDelivery.status == "FAILED")
        .order_by(WebhookDelivery.created_at.asc())
        .limit(limit)
        .all()
    )


def retry_failed_delivery(db: Session, delivery_id: str) -> bool:
    """Manually retry a failed webhook delivery from the DLQ."""
    delivery = (
        db.query(WebhookDelivery)
        .filter(WebhookDelivery.delivery_id == delivery_id)
        .first()
    )

    if not delivery:
        return False

    # Reset for retry
    delivery.status = "PENDING"
    delivery.attempts = 0
    delivery.next_retry = None
    db.commit()

    logger.info(f"DLQ: Re-queued delivery {delivery_id} for retry.")
    return True


def get_dlq_stats(db: Session) -> dict:
    """Get statistics about failed deliveries in the DLQ."""
    total_failed = (
        db.query(func.count(WebhookDelivery.id))
        .filter(WebhookDelivery.status == "FAILED")
        .scalar()
    )

    total_pending = (
        db.query(func.count(WebhookDelivery.id))
        .filter(WebhookDelivery.status == "PENDING")
        .scalar()
    )

    total_delivered = (
        db.query(func.count(WebhookDelivery.id))
        .filter(WebhookDelivery.status == "DELIVERED")
        .scalar()
    )

    return {
        "failed": total_failed,
        "pending": total_pending,
        "delivered": total_delivered,
        "total": total_failed + total_pending + total_delivered,
    }
```

The webhook worker polls the database every 5 seconds for pending deliveries. This is simpler than a Pub/Sub consumer for a monolith deployment and guarantees delivery even across process restarts because the state is persisted in PostgreSQL. `process_pending_webhooks` batches up to 100 deliveries per poll to avoid long-running transactions. `retry_failed_delivery` resets a failed delivery for another attempt — this is used by the admin API for manual intervention on DLQ items. In production, you would replace the polling loop with a Pub/Sub consumer for lower latency and better resource efficiency.

---

## Rate Limiting & Fraud Detection

Payment APIs are high-value targets for abuse. Rate limiting prevents brute-force attacks, and basic fraud rules catch suspicious patterns before they succeed.

### `app/fraud.py`

```python
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Payment

logger = logging.getLogger(__name__)


class FraudCheckResult:
    def __init__(self, passed: bool, reason: str = ""):
        self.passed = passed
        self.reason = reason


def check_velocity(
    db: Session,
    sender_account_id: str,
    window_minutes: int = 10,
    max_transactions: int = 20,
) -> FraudCheckResult:
    """
    Velocity check: reject if the sender has too many transactions
    in a short time window.
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)

    count = (
        db.query(func.count(Payment.id))
        .filter(Payment.sender_id == sender_account_id)
        .filter(Payment.created_at >= since)
        .scalar()
    )

    if count >= max_transactions:
        logger.warning(
            f"Velocity check failed: {sender_account_id} has {count} "
            f"transactions in the last {window_minutes} minutes."
        )
        return FraudCheckResult(
            passed=False,
            reason=f"Too many transactions ({count}) in {window_minutes} minutes.",
        )

    return FraudCheckResult(passed=True)


def check_amount_anomaly(
    db: Session,
    sender_account_id: str,
    amount: int,
    multiplier: float = 5.0,
) -> FraudCheckResult:
    """
    Anomaly check: reject if the amount is significantly larger than
    the sender's average transaction.
    """
    avg_amount = (
        db.query(func.avg(Payment.amount))
        .filter(Payment.sender_id == sender_account_id)
        .filter(Payment.status == "COMPLETED")
        .scalar()
    )

    if avg_amount and amount > avg_amount * multiplier:
        logger.warning(
            f"Amount anomaly: {sender_account_id} sending {amount}, "
            f"avg is {avg_amount:.0f} (threshold: {avg_amount * multiplier:.0f})."
        )
        return FraudCheckResult(
            passed=False,
            reason=f"Amount {amount} is {amount / avg_amount:.1f}x the average.",
        )

    return FraudCheckResult(passed=True)


def check_duplicate_receiver(
    db: Session,
    sender_account_id: str,
    receiver_account_id: str,
    amount: int,
    window_minutes: int = 5,
) -> FraudCheckResult:
    """
    Duplicate check: reject if the same sender is sending the same amount
    to the same receiver within a short window (likely accidental double-tap).
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)

    existing = (
        db.query(Payment)
        .filter(Payment.sender_id == sender_account_id)
        .filter(Payment.receiver_id == receiver_account_id)
        .filter(Payment.amount == amount)
        .filter(Payment.created_at >= since)
        .filter(Payment.status.in_(["PENDING", "COMPLETED"]))
        .first()
    )

    if existing:
        logger.warning(
            f"Duplicate detected: {sender_account_id} → {receiver_account_id}, "
            f"amount={amount}, within {window_minutes} minutes."
        )
        return FraudCheckResult(
            passed=False,
            reason="Duplicate payment detected. Same sender, receiver, and amount "
                   f"within {window_minutes} minutes.",
        )

    return FraudCheckResult(passed=True)


def run_fraud_checks(
    db: Session,
    sender_account_id: str,
    receiver_account_id: str,
    amount: int,
) -> FraudCheckResult:
    """Run all fraud checks. Returns the first failure or a pass result."""
    checks = [
        check_velocity(db, sender_account_id),
        check_amount_anomaly(db, sender_account_id, amount),
        check_duplicate_receiver(
            db, sender_account_id, receiver_account_id, amount
        ),
    ]

    for result in checks:
        if not result.passed:
            return result

    return FraudCheckResult(passed=True)
```

`check_velocity` counts how many transactions a sender has initiated in the last 10 minutes and rejects if it exceeds 20. This catches scripted attacks. `check_amount_anomaly` compares the current amount against the sender's historical average and rejects if it's 5× higher, catching stolen account abuse (attacker drains the account with one large transaction). `check_duplicate_receiver` catches accidental double-taps where a user sends the same amount to the same person within 5 minutes. In production, these rule-based checks would be supplemented by a machine learning fraud model that scores transactions in real-time using features like device fingerprint, login location, time-of-day, and social graph analysis.

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
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/payments
      - REDIS_URL=redis://cache:6379/0
      - IDEMPOTENCY_TTL_HOURS=24
      - WEBHOOK_TIMEOUT_SECONDS=10
      - WEBHOOK_MAX_RETRIES=5
      - MAX_TRANSACTION_AMOUNT=1000000
      - DEFAULT_CURRENCY=USD
      - RATE_LIMIT_PER_MINUTE=120
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    restart: unless-stopped

  webhook-worker:
    build: .
    command: ["python", "-m", "workers.webhook_worker"]
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/payments
      - REDIS_URL=redis://cache:6379/0
      - WEBHOOK_TIMEOUT_SECONDS=10
      - WEBHOOK_MAX_RETRIES=5
    depends_on:
      db:
        condition: service_healthy
    restart: unless-stopped

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=payments
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d payments"]
      interval: 5s
      timeout: 5s
      retries: 5

  cache:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  db-data:
  redis-data:
```

`depends_on` with `condition: service_healthy` ensures the API and webhook worker wait until PostgreSQL and Redis pass their health checks before starting. The `webhook-worker` service runs the same Docker image with a different command (`python -m workers.webhook_worker`), sharing the database and configuration but running as a separate process. This separates webhook delivery from request handling, so slow webhooks don't block API responses. `--appendonly yes` enables Redis AOF persistence. `redis:7-alpine` is a minimal Redis image. `restart: unless-stopped` automatically restarts containers after crashes.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
docker compose logs -f webhook-worker         # Follow webhook worker logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Create accounts
curl -X POST http://localhost:8000/accounts \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "initial_balance": 100000}'

curl -X POST http://localhost:8000/accounts \
  -H "Content-Type: application/json" \
  -d '{"user_id": "bob", "initial_balance": 50000}'

# Make a payment (note the idempotency key)
curl -X POST http://localhost:8000/payments \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "pay-001-alice-bob",
    "sender_account_id": "ALICE_ACCOUNT_ID",
    "receiver_account_id": "BOB_ACCOUNT_ID",
    "amount": 5000,
    "currency": "USD",
    "description": "Lunch payment"
  }'

# Retry the same request (should return cached response)
curl -X POST http://localhost:8000/payments \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "pay-001-alice-bob",
    "sender_account_id": "ALICE_ACCOUNT_ID",
    "receiver_account_id": "BOB_ACCOUNT_ID",
    "amount": 5000,
    "currency": "USD",
    "description": "Lunch payment"
  }'

# Check payment status
curl http://localhost:8000/payments/PAYMENT_ID

# View ledger entries
curl http://localhost:8000/payments/PAYMENT_ID/ledger

# View payment events (audit trail)
curl http://localhost:8000/payments/PAYMENT_ID/events

# Check account balance
curl http://localhost:8000/accounts/ALICE_ACCOUNT_ID
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


@pytest.fixture
def alice_account(client):
    """Create Alice's account with $1000.00 balance."""
    response = client.post(
        "/accounts",
        json={"user_id": "alice", "initial_balance": 100000},
    )
    return response.json()


@pytest.fixture
def bob_account(client):
    """Create Bob's account with $500.00 balance."""
    response = client.post(
        "/accounts",
        json={"user_id": "bob", "initial_balance": 50000},
    )
    return response.json()
```

Tests use SQLite instead of PostgreSQL for speed and isolation. `dependency_overrides` is a FastAPI feature that lets you swap out dependencies during testing. Every test gets a fresh database. `alice_account` and `bob_account` fixtures create test accounts for payment tests.

### `tests/test_payments.py`

```python
def test_create_payment(client, alice_account, bob_account):
    """Test successful payment creation."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "test-pay-001",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 5000,
            "currency": "USD",
            "description": "Test payment",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "COMPLETED"
    assert data["amount"] == 5000


def test_insufficient_balance(client, alice_account, bob_account):
    """Test payment rejection when sender has insufficient funds."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "test-pay-002",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 999999999,
            "currency": "USD",
        },
    )
    assert response.status_code == 400
    assert "Insufficient balance" in response.json()["detail"]


def test_self_transfer_rejected(client, alice_account):
    """Test that sending money to yourself is rejected."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "test-pay-003",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": alice_account["account_id"],
            "amount": 1000,
            "currency": "USD",
        },
    )
    assert response.status_code == 400
    assert "same account" in response.json()["detail"]


def test_balance_updated_after_payment(client, alice_account, bob_account):
    """Test that balances are updated correctly after payment."""
    client.post(
        "/payments",
        json={
            "idempotency_key": "test-pay-004",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 10000,
            "currency": "USD",
        },
    )

    alice = client.get(f"/accounts/{alice_account['account_id']}").json()
    bob = client.get(f"/accounts/{bob_account['account_id']}").json()

    assert alice["balance"] == 100000 - 10000     # $900.00
    assert bob["balance"] == 50000 + 10000        # $600.00


def test_invalid_amount(client, alice_account, bob_account):
    """Test that zero or negative amounts are rejected."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "test-pay-005",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 0,
            "currency": "USD",
        },
    )
    assert response.status_code == 422


def test_nonexistent_sender(client, bob_account):
    """Test payment with nonexistent sender account."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "test-pay-006",
            "sender_account_id": "nonexistent-account",
            "receiver_account_id": bob_account["account_id"],
            "amount": 1000,
            "currency": "USD",
        },
    )
    assert response.status_code == 404
```

### `tests/test_idempotency.py`

```python
def test_idempotent_duplicate_returns_same_result(client, alice_account, bob_account):
    """Test that the same idempotency key returns the same result."""
    payload = {
        "idempotency_key": "idemp-test-001",
        "sender_account_id": alice_account["account_id"],
        "receiver_account_id": bob_account["account_id"],
        "amount": 2500,
        "currency": "USD",
    }

    response1 = client.post("/payments", json=payload)
    response2 = client.post("/payments", json=payload)

    assert response1.status_code == 201
    assert response2.status_code == 200 or response2.status_code == 201
    assert response1.json()["payment_id"] == response2.json()["payment_id"]


def test_idempotent_no_double_charge(client, alice_account, bob_account):
    """Test that duplicate requests do not charge the sender twice."""
    payload = {
        "idempotency_key": "idemp-test-002",
        "sender_account_id": alice_account["account_id"],
        "receiver_account_id": bob_account["account_id"],
        "amount": 5000,
        "currency": "USD",
    }

    client.post("/payments", json=payload)
    client.post("/payments", json=payload)         # Duplicate
    client.post("/payments", json=payload)         # Triple

    alice = client.get(f"/accounts/{alice_account['account_id']}").json()
    assert alice["balance"] == 100000 - 5000      # Charged only once


def test_different_keys_are_separate_payments(client, alice_account, bob_account):
    """Test that different idempotency keys create separate payments."""
    client.post(
        "/payments",
        json={
            "idempotency_key": "idemp-test-003a",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 1000,
            "currency": "USD",
        },
    )

    client.post(
        "/payments",
        json={
            "idempotency_key": "idemp-test-003b",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 1000,
            "currency": "USD",
        },
    )

    alice = client.get(f"/accounts/{alice_account['account_id']}").json()
    assert alice["balance"] == 100000 - 2000      # Charged twice
```

### `tests/test_ledger.py`

```python
def test_ledger_entries_created(client, alice_account, bob_account):
    """Test that debit and credit ledger entries are created."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "ledger-test-001",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 3000,
            "currency": "USD",
        },
    )
    payment_id = response.json()["payment_id"]

    ledger = client.get(f"/payments/{payment_id}/ledger").json()
    assert len(ledger) == 2

    types = {e["entry_type"] for e in ledger}
    assert types == {"DEBIT", "CREDIT"}


def test_ledger_amounts_match(client, alice_account, bob_account):
    """Test that debit and credit amounts are equal."""
    response = client.post(
        "/payments",
        json={
            "idempotency_key": "ledger-test-002",
            "sender_account_id": alice_account["account_id"],
            "receiver_account_id": bob_account["account_id"],
            "amount": 7500,
            "currency": "USD",
        },
    )
    payment_id = response.json()["payment_id"]

    ledger = client.get(f"/payments/{payment_id}/ledger").json()
    debit = next(e for e in ledger if e["entry_type"] == "DEBIT")
    credit = next(e for e in ledger if e["entry_type"] == "CREDIT")

    assert debit["amount"] == credit["amount"] == 7500
```

### `tests/test_state_machine.py`

```python
import pytest
from fastapi import HTTPException

from app.state_machine import PaymentStatus, validate_transition, is_terminal


def test_valid_transitions():
    """Test all valid state transitions."""
    validate_transition(PaymentStatus.PENDING, PaymentStatus.COMPLETED)
    validate_transition(PaymentStatus.PENDING, PaymentStatus.FAILED)
    validate_transition(PaymentStatus.COMPLETED, PaymentStatus.REFUNDED)
    validate_transition(PaymentStatus.COMPLETED, PaymentStatus.PARTIALLY_REFUNDED)
    validate_transition(PaymentStatus.PARTIALLY_REFUNDED, PaymentStatus.REFUNDED)


def test_invalid_transition_from_failed():
    """Test that FAILED is a terminal state."""
    with pytest.raises(HTTPException) as exc_info:
        validate_transition(PaymentStatus.FAILED, PaymentStatus.COMPLETED)
    assert exc_info.value.status_code == 409


def test_invalid_transition_from_refunded():
    """Test that REFUNDED is a terminal state."""
    with pytest.raises(HTTPException) as exc_info:
        validate_transition(PaymentStatus.REFUNDED, PaymentStatus.COMPLETED)
    assert exc_info.value.status_code == 409


def test_invalid_reverse_transition():
    """Test that you cannot go from COMPLETED back to PENDING."""
    with pytest.raises(HTTPException) as exc_info:
        validate_transition(PaymentStatus.COMPLETED, PaymentStatus.PENDING)
    assert exc_info.value.status_code == 409


def test_terminal_states():
    """Test terminal state detection."""
    assert is_terminal(PaymentStatus.FAILED) is True
    assert is_terminal(PaymentStatus.REFUNDED) is True
    assert is_terminal(PaymentStatus.COMPLETED) is False
    assert is_terminal(PaymentStatus.PENDING) is False
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_idempotency"                  # Run only idempotency tests
pytest tests/test_ledger.py                   # Run ledger tests only
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
# Create a Cloud SQL instance with high availability
gcloud sql instances create payments-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-2-7680 \
  --region=us-central1 \
  --storage-size=50GB \
  --storage-auto-increase \
  --availability-type=REGIONAL \
  --backup-start-time=02:00 \
  --enable-point-in-time-recovery

# Create the database
gcloud sql databases create payments \
  --instance=payments-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=payments-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-2-7680` provisions 2 vCPUs and 7.5 GB RAM. `--availability-type=REGIONAL` enables automatic failover with a standby replica in another zone. `--backup-start-time=02:00` schedules daily backups at 2 AM UTC. `--enable-point-in-time-recovery` enables WAL-based recovery to any point in the last 7 days. For a payment system, `REGIONAL` availability and point-in-time recovery are non-negotiable — data loss means money loss.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create payments-cache \
  --size=2 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe payments-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=2` provisions 2 GB of memory. `--tier=STANDARD` provides automatic failover with a replica. The `STANDARD` tier is required for production payment systems because Redis stores idempotency keys — if Redis loses data during a failover, duplicate payments could occur.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry (recommended)
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/payments/api

# Or Google Container Registry
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/payment-system
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy to Cloud Run

```bash
# Deploy the API service
gcloud run deploy payment-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/payments/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 20 \
  --concurrency 100 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:payments-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/payments?host=/cloudsql/YOUR_PROJECT_ID:us-central1:payments-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
IDEMPOTENCY_TTL_HOURS=24,\
WEBHOOK_TIMEOUT_SECONDS=10,\
WEBHOOK_MAX_RETRIES=5,\
MAX_TRANSACTION_AMOUNT=1000000,\
DEFAULT_CURRENCY=USD,\
RATE_LIMIT_PER_MINUTE=120"

# Deploy the webhook worker as a separate service
gcloud run deploy webhook-worker \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/payments/api \
  --command "python","-m","workers.webhook_worker" \
  --platform managed \
  --region us-central1 \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 1 \
  --max-instances 5 \
  --no-allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:payments-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/payments?host=/cloudsql/YOUR_PROJECT_ID:us-central1:payments-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
WEBHOOK_TIMEOUT_SECONDS=10,\
WEBHOOK_MAX_RETRIES=5"
```

`--min-instances 2` keeps at least two API instances warm — a single instance is a single point of failure for a payment system. `--concurrency 100` limits each instance to 100 concurrent requests to prevent database connection exhaustion. `--no-allow-unauthenticated` on the webhook worker ensures it's not publicly accessible. The webhook worker runs the same container image with a different entrypoint command.

### Schedule Ledger Reconciliation

```bash
# Create a Cloud Scheduler job to verify the ledger daily
gcloud scheduler jobs create http ledger-reconciliation \
  --schedule="0 3 * * *" \
  --uri="https://your-payment-api.run.app/admin/ledger/verify" \
  --http-method=POST \
  --oidc-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

`--schedule="0 3 * * *"` runs at 3 AM UTC daily. The reconciliation endpoint verifies that the sum of all debits equals the sum of all credits and that every account's stored balance matches its computed balance from ledger entries. A failure triggers an immediate PagerDuty alert.

### Set Up a Custom Domain

```bash
gcloud run domain-mappings create \
  --service payment-api \
  --domain pay.yourdomain.com \
  --region us-central1
```

After running this command, GCP provides DNS records to add at your domain registrar. Once DNS propagates, GCP automatically provisions and renews SSL certificates via Let's Encrypt.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                   | How                                                       |
| --------------------- | --------------------------------------------- | --------------------------------------------------------- |
| Read Replicas         | Analytics queries slow down the primary        | Cloud SQL read replicas for reporting and reconciliation   |
| Connection Pooling    | Many Cloud Run instances exhaust connections   | PgBouncer sidecar or Cloud SQL Auth Proxy pooling          |
| Table Partitioning    | `payments` or `ledger_entries` exceed 1B rows  | Partition by `created_at` month using range partitioning   |
| Vertical Scaling      | Single-query latency is too high              | Upgrade to `db-custom-8-30720` (8 vCPU, 30 GB RAM)        |

### Payment Processing Scaling

| Strategy              | When to Use                                   | How                                                       |
| --------------------- | --------------------------------------------- | --------------------------------------------------------- |
| Event-Driven          | Synchronous processing adds latency           | Pub/Sub for async ledger writes and notifications          |
| CQRS                  | Read and write patterns diverge significantly  | Separate read models for account balance and analytics     |
| Sharding              | Single database can't handle write volume     | Shard by `account_id` hash across multiple database clusters |
| Message Queue         | Webhook delivery contends with API resources   | Cloud Tasks or Pub/Sub for webhook delivery                |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ pay.xyz        │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud Armor    │
                     │ (WAF + DDoS)   │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud Load     │
                     │ Balancer       │
                     └──────┬─────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
       ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
       │ Cloud Run   │ │Cloud   │ │ Cloud Run   │
       │ API #1      │ │Run #2  │ │ API #N      │
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │            VPC Network              │
         │                                      │
         │  ┌──────────┐   ┌────────────────┐  │
         │  │ Memory-  │   │  Cloud SQL     │  │
         │  │ store    │   │  (Primary +    │  │
         │  │ (Redis)  │   │  Standby +     │  │
         │  │ STANDARD │   │  Read Replica) │  │
         │  └──────────┘   └────────────────┘  │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Pub/Sub                │   │
         │  │  (Payment Events)             │   │
         │  └──────────┬────────────────────┘   │
         │             │                        │
         │  ┌──────────▼──────────────────┐     │
         │  │  Webhook Worker (Cloud Run) │     │
         │  └─────────────────────────────┘     │
         │                                      │
         │  ┌──────────────────────────────┐    │
         │  │  Cloud Tasks                 │    │
         │  │  (Retry queue for webhooks)  │    │
         │  └──────────────────────────────┘    │
         │                                      │
         │  ┌──────────────────────────────┐    │
         │  │  Secret Manager              │    │
         │  │  (API keys, DB passwords,    │    │
         │  │   webhook signing secrets)   │    │
         │  └──────────────────────────────┘    │
         │                                      │
         │  ┌──────────────────────────────┐    │
         │  │  Cloud KMS                   │    │
         │  │  (Encryption at rest)        │    │
         │  └──────────────────────────────┘    │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud Armor** provides Web Application Firewall (WAF) and DDoS protection. Payment APIs are prime targets for automated attacks. Cloud Armor applies OWASP rules and rate limiting at the edge before requests reach your API.
- **Cloud Pub/Sub** decouples payment event processing from the API path. The API publishes an event and returns immediately. Webhook delivery, notifications, and analytics run asynchronously.
- **Cloud Tasks** replaces the webhook polling loop with push-based delivery. Each webhook creates a Cloud Task with a configured retry policy (exponential backoff), eliminating the need for a dedicated webhook worker process.
- **Secret Manager** stores database passwords, API keys, and webhook signing secrets. Environment variables in Cloud Run are visible to anyone with deploy access; Secret Manager provides IAM-controlled access with audit logging.
- **Cloud KMS** manages encryption keys for encrypting sensitive data at rest (PII, payment details). Cloud SQL's built-in encryption uses Google-managed keys; KMS lets you use customer-managed keys (CMEK) for compliance.
- **STANDARD tier Redis** provides automatic failover. If the primary Redis node fails, the standby takes over within seconds, preserving idempotency keys and preventing duplicate payments.

### Security Checklist

```
✓ Use HTTPS everywhere (Cloud Run provides TLS by default)
✓ Store all secrets in Secret Manager (never in env vars or code)
✓ Encrypt sensitive fields (card numbers, PII) at the application layer with Cloud KMS
✓ Enable Cloud Audit Logs for all API calls and admin operations
✓ Implement rate limiting per IP and per account
✓ Run fraud checks on every transaction before execution
✓ Use row-level locking (SELECT FOR UPDATE) to prevent double-spend
✓ Validate idempotency keys before processing
✓ Sign webhook payloads with HMAC-SHA256
✓ Set CORS to restrict API access to authorized domains
✓ Enable VPC Service Controls to restrict data access within the network
✓ Comply with PCI DSS if handling raw card numbers (or use a tokenization service)
✓ Implement IP allowlisting for admin and reconciliation endpoints
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create payment-health \
  --display-name="Payment API Health" \
  --uri="https://your-payment-api.run.app/admin/health" \
  --period=60

# View Cloud Run metrics in the console
# → CPU utilization, request count, latency (p50, p95, p99), error rate

# Key metrics to alert on:
# - Payment failure rate > 0.5% over 5 minutes
# - p99 latency > 500 ms
# - Ledger imbalance detected (CRITICAL — immediate page)
# - Idempotency key collision rate > 0
# - Webhook delivery failure rate > 5%
# - DLQ depth > 100 items
# - Database connection pool exhaustion
# - Redis memory usage > 80%
# - Cloud SQL CPU > 70% (lower threshold than typical because of consistency needs)
# - Account balance reconciliation failure (CRITICAL)
```

Cloud Run automatically exports metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. For a payment system, **ledger imbalance** and **account reconciliation failure** should trigger immediate escalation (P0/Sev1) because they indicate potential money loss or creation. Use Cloud Trace for distributed tracing across API → Redis → PostgreSQL → Webhook delivery to identify bottlenecks. Custom metrics for fraud check pass/fail rates, saga compensation counts, and DLQ depth should be exported using the Cloud Monitoring client library.
