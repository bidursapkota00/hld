# Design Distributed Task Scheduler

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [Task Definitions & Serialization](#task-definitions--serialization)
6. [Priority Queue & Delayed Jobs](#priority-queue--delayed-jobs)
7. [Cron & Recurring Schedules](#cron--recurring-schedules)
8. [Worker Pool & Execution Engine](#worker-pool--execution-engine)
9. [Retry & Dead Letter Queue](#retry--dead-letter-queue)
10. [Leader Election & Partition Assignment](#leader-election--partition-assignment)
11. [Core API – Tasks & Schedules](#core-api--tasks--schedules)
12. [Real-Time Status (WebSockets)](#real-time-status-websockets)
13. [Docker & Docker Compose](#docker--docker-compose)
14. [Testing](#testing)
15. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
16. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement              | Description                                                              |
| ------------------------ | ------------------------------------------------------------------------ |
| One-Time Tasks           | Schedule a task to execute once at a specific time or immediately        |
| Recurring Tasks          | Define cron-based schedules (e.g., every 5 minutes, daily at 9 AM)      |
| Delayed Tasks            | Execute a task after a specified delay (e.g., 30 minutes from now)      |
| Priority Levels          | Critical, high, medium, low – higher priority tasks are picked first    |
| Retry with Backoff       | Auto-retry failed tasks with exponential backoff and configurable caps  |
| Dead Letter Queue        | Store tasks that exhaust all retries for manual inspection               |
| Task Dependencies        | Define DAGs where task B runs only after task A succeeds                 |
| Idempotency              | Guarantee exactly-once execution semantics via idempotency keys         |
| Cancellation             | Cancel pending or scheduled tasks before execution                       |
| Real-Time Status         | Stream task status updates to connected clients via WebSockets           |
| Partitioned Execution    | Distribute tasks across worker partitions to prevent hot spots          |
| Callbacks & Webhooks     | Notify external systems on task completion or failure via HTTP callback  |

### Non-Functional Requirements

| Requirement      | Target                                                     |
| ---------------- | ---------------------------------------------------------- |
| Availability     | 99.99% uptime (missed schedules are unacceptable)          |
| Latency          | Task picked up within 1 second of becoming eligible        |
| Throughput       | 100 K tasks/second at peak                                 |
| Ordering         | At-least-once delivery; idempotent task handlers           |
| Durability       | No task record or schedule should ever be lost              |
| Scalability      | Handle 1 B tasks/day across all queues                     |

### Back-of-the-Envelope Estimations

```
Tasks per day                    ≈ 1 B
Tasks per second                 ≈ 1 B / 86400 ≈ 11.5 K RPS (avg), ~100 K RPS (peak)
Recurring schedules              ≈ 10 M active cron schedules
Task record size                 ≈ 1 KB (payload + metadata)
Storage per day                  ≈ 1 B × 1 KB = 1 TB/day
Storage per year (30-day retain) ≈ 1 TB × 30 = 30 TB
Queue depth (burst)              ≈ 10 M messages (Redis sorted set buffer)
Worker count                     ≈ 500–2000 (auto-scaled based on queue depth)
Cron evaluation interval         ≈ Every 1 second (leader polls for due schedules)
Partition count                  ≈ 64 (consistent hashing across workers)
```

100 K tasks per second demands a horizontally scaled architecture. Redis sorted sets serve as priority queues, scored by execution time, enabling both immediate and delayed tasks. PostgreSQL stores task metadata, execution history, and schedule definitions. A dedicated scheduler service evaluates cron expressions every second and enqueues due tasks. Workers pull from partitioned queues, execute task handlers, and report results. Leader election ensures only one scheduler instance evaluates cron schedules at a time to prevent duplicate task creation.

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
                │  POST /tasks               │
                │  POST /schedules           │
                │  GET  /tasks/{id}          │
                │  DELETE /tasks/{id}        │
                │  WS   /ws/status/{task_id} │
                └──┬──────────┬───────────┬──┘
                   │          │           │
            ┌──────▼───┐ ┌───▼────┐ ┌────▼──────┐
            │  Redis   │ │Postgre-│ │  Pub/Sub  │
            │ (Priority│ │  SQL   │ │  (Fan-Out │
            │  Queues  │ │(Tasks, │ │ & Status  │
            │ + Locks) │ │Scheds) │ │  Events)  │
            └────┬─────┘ └────────┘ └─────┬─────┘
                 │                        │
        ┌────────▼────────────────────────▼────────┐
        │              Worker Pools                │
        │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
        │  │ Partition │ │ Partition│ │ Partition│ │
        │  │  0–15    │ │  16–31  │ │  32–63  │ │
        │  │ Workers  │ │ Workers │ │ Workers │ │
        │  └──────────┘ └──────────┘ └──────────┘ │
        └──────────────────────────────────────────┘
                               │
                 ┌─────────────▼──────────────┐
                 │     Scheduler Service      │
                 │  (Leader-elected cron       │
                 │   evaluator, enqueues due   │
                 │   recurring tasks)          │
                 └────────────────────────────┘
```

The API server receives task submissions and stores them in PostgreSQL. Immediate and delayed tasks are enqueued into Redis priority queues, scored by their execution timestamp. Workers poll their assigned partitions, execute task handlers, and update results back to PostgreSQL. A leader-elected scheduler service evaluates cron expressions every second, creating one-time task instances for each due schedule and enqueuing them. Pub/Sub broadcasts status change events for real-time WebSocket delivery. Redis distributed locks prevent duplicate execution during worker failover.

---

## Project Setup

### Project Structure

```
scheduler/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── queue.py              # Redis priority queue operations
│   ├── cron_engine.py        # Cron expression evaluation & scheduling
│   ├── executor.py           # Task execution engine (runs handlers)
│   ├── retry.py              # Retry logic and dead letter queue
│   ├── partitioner.py        # Consistent hashing & partition assignment
│   ├── leader.py             # Leader election via Redis
│   ├── dependencies.py       # DAG resolution & dependency tracking
│   ├── callbacks.py          # Webhook / HTTP callback delivery
│   ├── websocket_manager.py  # WebSocket connection manager
│   └── routers/
│       ├── __init__.py
│       ├── tasks.py          # Task CRUD and submission endpoints
│       ├── schedules.py      # Recurring schedule CRUD endpoints
│       ├── admin.py          # Queue stats, DLQ inspection, requeue
│       └── live.py           # WebSocket real-time status
├── worker.py                 # Background worker process entry point
├── scheduler_service.py      # Cron evaluator service entry point
├── handlers/
│   ├── __init__.py
│   ├── base.py               # Abstract task handler interface
│   ├── email_report.py       # Example: send email report
│   ├── data_export.py        # Example: export data to CSV
│   └── cleanup.py            # Example: database cleanup
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_queue.py         # Tests for priority queue
│   ├── test_cron.py          # Tests for cron evaluation
│   ├── test_retry.py         # Tests for retry logic
│   ├── test_executor.py      # Tests for task execution
│   └── test_tasks.py         # Tests for API endpoints
├── Dockerfile
├── Dockerfile.worker
├── Dockerfile.scheduler
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir scheduler && cd scheduler
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
croniter==3.0.3
apscheduler==3.10.4
websockets==13.1
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for priority queues, distributed locks, and leader election. `croniter` parses and evaluates cron expressions to determine the next execution time. `apscheduler` provides an in-process scheduler loop that triggers the cron evaluator every second. `httpx` is an async HTTP client for delivering webhooks on task completion. `websockets` provides WebSocket support for real-time status updates.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/scheduler
REDIS_URL=redis://localhost:6379/0
BASE_URL=http://localhost:8000
WORKER_ID=worker-1
PARTITION_COUNT=64
POLL_INTERVAL_MS=100
MAX_RETRY_ATTEMPTS=5
RETRY_BASE_DELAY=30
RETRY_MAX_DELAY=3600
DLQ_MAX_AGE_HOURS=168
LEADER_TTL_SECONDS=30
LEADER_RENEW_SECONDS=10
CRON_EVAL_INTERVAL_SECONDS=1
WEBHOOK_TIMEOUT_SECONDS=10
TASK_TIMEOUT_SECONDS=300
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `WORKER_ID` uniquely identifies this worker for partition assignment and lock ownership. `PARTITION_COUNT` sets the number of partitions for consistent hashing; 64 is a good default for up to ~200 workers. `POLL_INTERVAL_MS` controls how often workers check their queues (100 ms = 10 polls/sec). `MAX_RETRY_ATTEMPTS` caps the number of retries before moving to the DLQ. `RETRY_BASE_DELAY` is the initial backoff delay in seconds. `RETRY_MAX_DELAY` caps exponential backoff at 1 hour. `DLQ_MAX_AGE_HOURS` retains failed tasks for 7 days. `LEADER_TTL_SECONDS` is the Redis lock TTL for leader election; if the leader crashes, a new leader takes over after 30 seconds. `LEADER_RENEW_SECONDS` is how often the leader renews its lock. `TASK_TIMEOUT_SECONDS` kills a task execution that exceeds 5 minutes.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│              tasks                   │     │           schedules                  │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            UUID       PRIMARY KEY │     │ id            UUID       PRIMARY KEY │
│ name          VARCHAR    NOT NULL    │     │ name          VARCHAR    NOT NULL    │
│ handler       VARCHAR    NOT NULL    │     │ handler       VARCHAR    NOT NULL    │
│ payload       JSONB                  │     │ cron_expr     VARCHAR    NOT NULL    │
│ priority      ENUM       NOT NULL    │     │ timezone      VARCHAR    DEFAULT UTC│
│ status        ENUM       DEFAULT Q   │     │ payload       JSONB                  │
│ partition_key INTEGER    NOT NULL    │     │ priority      ENUM       DEFAULT MED │
│ result        JSONB                  │     │ max_retries   INTEGER    DEFAULT 3   │
│ error_message TEXT                   │     │ timeout_secs  INTEGER    DEFAULT 300 │
│ retry_count   INTEGER    DEFAULT 0   │     │ is_active     BOOLEAN    DEFAULT T   │
│ max_retries   INTEGER    DEFAULT 3   │     │ last_run_at   TIMESTAMPTZ            │
│ timeout_secs  INTEGER    DEFAULT 300 │     │ next_run_at   TIMESTAMPTZ            │
│ scheduled_at  TIMESTAMPTZ NOT NULL   │     │ callback_url  VARCHAR               │
│ started_at    TIMESTAMPTZ           │     │ created_at    TIMESTAMPTZ            │
│ completed_at  TIMESTAMPTZ           │     │ updated_at    TIMESTAMPTZ            │
│ failed_at     TIMESTAMPTZ           │     ├──────────────────────────────────────┤
│ created_at    TIMESTAMPTZ           │     │ INDEX idx_sched_active (is_active)   │
│ callback_url  VARCHAR               │     │ INDEX idx_sched_next (next_run_at)   │
│ idempotency_key VARCHAR  UNIQUE     │     └──────────────────────────────────────┘
│ schedule_id   UUID       FK → scheds│
│ depends_on    UUID[]                │     ┌──────────────────────────────────────┐
├──────────────────────────────────────┤     │        task_executions               │
│ INDEX idx_status (status)            │     ├──────────────────────────────────────┤
│ INDEX idx_partition (partition_key)   │     │ id            BIGSERIAL  PRIMARY KEY │
│ INDEX idx_scheduled (scheduled_at)   │     │ task_id       UUID       FK → tasks  │
│ INDEX idx_idempotency (idemp_key)    │     │ attempt_number INTEGER   NOT NULL    │
│ INDEX idx_depends (depends_on)       │     │ worker_id     VARCHAR    NOT NULL    │
│ INDEX idx_schedule (schedule_id)     │     │ status        ENUM       NOT NULL    │
└──────────────────────────────────────┘     │ error_code    VARCHAR               │
                                             │ error_message TEXT                   │
┌──────────────────────────────────────┐     │ result        JSONB                  │
│         dead_letter_queue            │     │ duration_ms   INTEGER               │
├──────────────────────────────────────┤     │ started_at    TIMESTAMPTZ            │
│ id            BIGSERIAL  PRIMARY KEY │     │ completed_at  TIMESTAMPTZ            │
│ task_id       UUID       FK → tasks  │     ├──────────────────────────────────────┤
│ original_payload JSONB   NOT NULL    │     │ INDEX idx_exec_task (task_id)        │
│ error_history    JSONB   NOT NULL    │     │ INDEX idx_exec_status (status)       │
│ exhausted_at  TIMESTAMPTZ           │     └──────────────────────────────────────┘
│ requeued_at   TIMESTAMPTZ           │
│ requeue_count INTEGER    DEFAULT 0   │
├──────────────────────────────────────┤
│ INDEX idx_dlq_exhausted (exhausted)  │
└──────────────────────────────────────┘
```

The `tasks` table is the core entity, storing every task submitted to the scheduler with its execution status and lifecycle timestamps. `partition_key` is a hash of the task ID modulo `PARTITION_COUNT`, used to distribute tasks across worker partitions and prevent hot spots. `depends_on` is a PostgreSQL array of task UUIDs; a task with dependencies stays in `PENDING` until all dependencies reach `COMPLETED`. `idempotency_key` prevents duplicate task creation: if the same API call is retried, the existing task is returned. `schedules` stores recurring cron definitions with their next computed run time; the scheduler service queries `next_run_at <= now` every second. `task_executions` logs every execution attempt with the worker that ran it, duration, and result, enabling debugging and SLA tracking. `dead_letter_queue` stores tasks that have exhausted all retry attempts along with their full error history for manual inspection.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/scheduler"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    worker_id: str = "worker-1"
    partition_count: int = 64
    poll_interval_ms: int = 100
    max_retry_attempts: int = 5
    retry_base_delay: int = 30
    retry_max_delay: int = 3600
    dlq_max_age_hours: int = 168
    leader_ttl_seconds: int = 30
    leader_renew_seconds: int = 10
    cron_eval_interval_seconds: int = 1
    webhook_timeout_seconds: int = 10
    task_timeout_seconds: int = 300

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
    BigInteger, Boolean, Column, DateTime, Enum, ForeignKey,
    Index, Integer, String, Text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import relationship

from app.database import Base


class Priority(str, enum.Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class ExecutionStatus(str, enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class Task(Base):
    __tablename__ = "tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    handler = Column(String(255), nullable=False)
    payload = Column(JSONB, nullable=True)
    priority = Column(Enum(Priority), nullable=False, default=Priority.MEDIUM)
    status = Column(Enum(TaskStatus), nullable=False, default=TaskStatus.QUEUED)
    partition_key = Column(Integer, nullable=False)
    result = Column(JSONB, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    timeout_secs = Column(Integer, default=300)
    scheduled_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    failed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    callback_url = Column(String(2048), nullable=True)
    idempotency_key = Column(String(255), unique=True, nullable=True)
    schedule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("schedules.id"),
        nullable=True,
    )
    depends_on = Column(ARRAY(UUID(as_uuid=True)), nullable=True)

    schedule = relationship("Schedule", back_populates="tasks")
    executions = relationship("TaskExecution", back_populates="task")

    __table_args__ = (
        Index("idx_status", "status"),
        Index("idx_partition", "partition_key"),
        Index("idx_scheduled", "scheduled_at"),
        Index("idx_idempotency", "idempotency_key"),
        Index("idx_schedule", "schedule_id"),
    )


class Schedule(Base):
    __tablename__ = "schedules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    handler = Column(String(255), nullable=False)
    cron_expr = Column(String(100), nullable=False)
    timezone = Column(String(50), default="UTC")
    payload = Column(JSONB, nullable=True)
    priority = Column(Enum(Priority), default=Priority.MEDIUM)
    max_retries = Column(Integer, default=3)
    timeout_secs = Column(Integer, default=300)
    is_active = Column(Boolean, default=True)
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)
    callback_url = Column(String(2048), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    tasks = relationship("Task", back_populates="schedule")

    __table_args__ = (
        Index("idx_sched_active", "is_active"),
        Index("idx_sched_next", "next_run_at"),
    )


class TaskExecution(Base):
    __tablename__ = "task_executions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tasks.id"),
        nullable=False,
    )
    attempt_number = Column(Integer, nullable=False)
    worker_id = Column(String(100), nullable=False)
    status = Column(Enum(ExecutionStatus), nullable=False)
    error_code = Column(String(50), nullable=True)
    error_message = Column(Text, nullable=True)
    result = Column(JSONB, nullable=True)
    duration_ms = Column(Integer, nullable=True)
    started_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    completed_at = Column(DateTime(timezone=True), nullable=True)

    task = relationship("Task", back_populates="executions")

    __table_args__ = (
        Index("idx_exec_task", "task_id"),
        Index("idx_exec_status", "status"),
    )


class DeadLetterEntry(Base):
    __tablename__ = "dead_letter_queue"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tasks.id"),
        nullable=False,
    )
    original_payload = Column(JSONB, nullable=False)
    error_history = Column(JSONB, nullable=False)
    exhausted_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    requeued_at = Column(DateTime(timezone=True), nullable=True)
    requeue_count = Column(Integer, default=0)

    task = relationship("Task")

    __table_args__ = (
        Index("idx_dlq_exhausted", "exhausted_at"),
    )
```

`UUID(as_uuid=True)` stores UUIDs natively in PostgreSQL, which is more compact than strings and prevents enumeration attacks. `TaskStatus` tracks the full lifecycle: `PENDING → QUEUED → RUNNING → COMPLETED` (or `FAILED → RETRYING → COMPLETED/FAILED`). `PENDING` is used for tasks with unresolved dependencies. `partition_key` is computed when the task is created using `hash(task_id) % PARTITION_COUNT` and determines which worker picks it up. `depends_on` is a PostgreSQL `UUID[]` array; tasks with dependencies remain `PENDING` until a dependency resolver promotes them to `QUEUED`. `Schedule.cron_expr` stores standard 5-field cron expressions (minute, hour, day-of-month, month, day-of-week). `next_run_at` is precomputed using `croniter` so the scheduler service can efficiently query `WHERE next_run_at <= now AND is_active = true`. `TaskExecution` logs every attempt, including which worker ran it and how long it took, enabling SLA monitoring and debugging flaky tasks. `DeadLetterEntry.error_history` stores a JSON array of all error messages from each retry attempt.

### `app/schemas.py`

```python
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class TaskCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    handler: str = Field(min_length=1, max_length=255)
    payload: dict | None = None
    priority: str = Field("medium", pattern="^(critical|high|medium|low)$")
    scheduled_at: datetime | None = None
    delay_seconds: int | None = Field(None, ge=0, le=86400 * 30)
    max_retries: int = Field(3, ge=0, le=20)
    timeout_secs: int = Field(300, ge=1, le=86400)
    callback_url: str | None = None
    idempotency_key: str | None = None
    depends_on: list[uuid.UUID] | None = None


class TaskResponse(BaseModel):
    id: uuid.UUID
    name: str
    handler: str
    payload: dict | None
    priority: str
    status: str
    partition_key: int
    result: dict | None
    error_message: str | None
    retry_count: int
    max_retries: int
    scheduled_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    created_at: datetime

    class Config:
        from_attributes = True


class TaskCancel(BaseModel):
    reason: str | None = None


class ScheduleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    handler: str = Field(min_length=1, max_length=255)
    cron_expr: str = Field(min_length=9, max_length=100)
    timezone: str = Field("UTC", max_length=50)
    payload: dict | None = None
    priority: str = Field("medium", pattern="^(critical|high|medium|low)$")
    max_retries: int = Field(3, ge=0, le=20)
    timeout_secs: int = Field(300, ge=1, le=86400)
    callback_url: str | None = None


class ScheduleResponse(BaseModel):
    id: uuid.UUID
    name: str
    handler: str
    cron_expr: str
    timezone: str
    priority: str
    is_active: bool
    last_run_at: datetime | None
    next_run_at: datetime | None
    created_at: datetime

    class Config:
        from_attributes = True


class ScheduleUpdate(BaseModel):
    cron_expr: str | None = None
    payload: dict | None = None
    priority: str | None = Field(None, pattern="^(critical|high|medium|low)$")
    is_active: bool | None = None
    max_retries: int | None = Field(None, ge=0, le=20)
    timeout_secs: int | None = Field(None, ge=1, le=86400)


class ExecutionResponse(BaseModel):
    id: int
    task_id: uuid.UUID
    attempt_number: int
    worker_id: str
    status: str
    error_message: str | None
    result: dict | None
    duration_ms: int | None
    started_at: datetime
    completed_at: datetime | None

    class Config:
        from_attributes = True


class DLQEntry(BaseModel):
    id: int
    task_id: uuid.UUID
    original_payload: dict
    error_history: list[dict]
    exhausted_at: datetime
    requeued_at: datetime | None
    requeue_count: int

    class Config:
        from_attributes = True


class QueueStats(BaseModel):
    critical: int
    high: int
    medium: int
    low: int
    processing: int
    dlq: int
    total_workers: int
    partitions: int
```

`TaskCreate` accepts either `scheduled_at` (absolute time) or `delay_seconds` (relative delay); the API converts `delay_seconds` to an absolute timestamp. `delay_seconds` is capped at 30 days. `depends_on` is an optional list of task UUIDs that must complete before this task is eligible for execution. `ScheduleCreate.cron_expr` accepts standard 5-field cron expressions (e.g., `*/5 * * * *` for every 5 minutes, `0 9 * * 1-5` for weekdays at 9 AM). `ScheduleUpdate` uses optional fields so clients can patch individual properties without resending the entire schedule.

---

## Task Definitions & Serialization

### `handlers/base.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TaskResult:
    """Result of a task execution."""
    success: bool
    data: dict | None = None
    error_code: str | None = None
    error_message: str | None = None


class BaseTaskHandler(ABC):
    """Abstract base class for task handlers."""

    @abstractmethod
    def execute(self, payload: dict) -> TaskResult:
        """
        Execute the task with the given payload.

        Must be idempotent: running the same payload twice
        should produce the same result without side effects.
        """
        pass

    @abstractmethod
    def get_handler_name(self) -> str:
        """Return the registered handler name."""
        pass

    def validate_payload(self, payload: dict) -> list[str]:
        """
        Validate the payload before execution.
        Returns a list of errors (empty if valid).
        Override in subclasses for custom validation.
        """
        return []
```

The abstract base class defines a contract that all task handlers must implement. `TaskResult` captures success/failure, returned data, and error details. This uniform interface lets the executor run any handler without knowing its internals. Handlers must be **idempotent**: if a task is retried due to a worker crash, running it again must not produce duplicate side effects (e.g., sending two emails, charging a credit card twice). The `validate_payload` hook lets handlers reject invalid payloads before execution.

### `handlers/email_report.py`

```python
import httpx

from handlers.base import BaseTaskHandler, TaskResult


class EmailReportHandler(BaseTaskHandler):
    """
    Generate and send a scheduled email report.

    Expected payload:
        {
            "report_type": "daily_summary",
            "recipient": "user@example.com",
            "date_range": {"start": "2025-01-01", "end": "2025-01-31"}
        }
    """

    def execute(self, payload: dict) -> TaskResult:
        report_type = payload.get("report_type")
        recipient = payload.get("recipient")
        date_range = payload.get("date_range", {})

        # 1. Generate the report (simulated)
        report_data = self._generate_report(report_type, date_range)

        # 2. Send the email (simulated via HTTP)
        try:
            response = httpx.post(
                "https://api.sendgrid.com/v3/mail/send",
                json={
                    "to": recipient,
                    "subject": f"{report_type} Report",
                    "body": report_data,
                },
                timeout=30,
            )

            if response.status_code in (200, 201, 202):
                return TaskResult(
                    success=True,
                    data={"recipient": recipient, "report_type": report_type},
                )
            else:
                return TaskResult(
                    success=False,
                    error_code=str(response.status_code),
                    error_message=response.text,
                )

        except httpx.TimeoutException:
            return TaskResult(
                success=False,
                error_code="TIMEOUT",
                error_message="Email API request timed out",
            )

        except Exception as e:
            return TaskResult(
                success=False,
                error_code="EXCEPTION",
                error_message=str(e),
            )

    def _generate_report(self, report_type: str, date_range: dict) -> str:
        """Generate a report. In production, query the database."""
        return f"Report: {report_type} for {date_range.get('start')} to {date_range.get('end')}"

    def get_handler_name(self) -> str:
        return "email_report"

    def validate_payload(self, payload: dict) -> list[str]:
        errors = []
        if not payload.get("report_type"):
            errors.append("Missing required field: report_type")
        if not payload.get("recipient"):
            errors.append("Missing required field: recipient")
        return errors
```

### `handlers/cleanup.py`

```python
from datetime import datetime, timezone, timedelta

from handlers.base import BaseTaskHandler, TaskResult


class CleanupHandler(BaseTaskHandler):
    """
    Database cleanup task. Removes old records beyond retention.

    Expected payload:
        {
            "table": "task_executions",
            "retention_days": 30
        }
    """

    def execute(self, payload: dict) -> TaskResult:
        table = payload.get("table", "task_executions")
        retention_days = payload.get("retention_days", 30)

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        # In production, this would execute a DELETE query
        # DELETE FROM {table} WHERE created_at < cutoff
        deleted_count = self._cleanup(table, cutoff)

        return TaskResult(
            success=True,
            data={
                "table": table,
                "cutoff": cutoff.isoformat(),
                "deleted_count": deleted_count,
            },
        )

    def _cleanup(self, table: str, cutoff: datetime) -> int:
        """Simulate cleanup. In production, run the actual DELETE."""
        return 0

    def get_handler_name(self) -> str:
        return "cleanup"
```

### Handler Registry

```python
# handlers/__init__.py

from handlers.email_report import EmailReportHandler
from handlers.cleanup import CleanupHandler


# ─── Handler Registry ────────────────────────────────────────────
# Maps handler names to handler classes.
# Workers look up handlers by name when executing tasks.

HANDLER_REGISTRY: dict[str, type] = {
    "email_report": EmailReportHandler,
    "cleanup": CleanupHandler,
}


def get_handler(handler_name: str):
    """
    Look up a handler by name and return an instance.
    Raises KeyError if the handler is not registered.
    """
    handler_class = HANDLER_REGISTRY.get(handler_name)
    if not handler_class:
        raise KeyError(f"Unknown handler: '{handler_name}'. Registered: {list(HANDLER_REGISTRY.keys())}")
    return handler_class()
```

The handler registry is a simple dictionary that maps handler names (strings stored in the database) to handler classes. When a worker dequeues a task, it looks up the handler by name, creates an instance, validates the payload, and calls `execute()`. This decouples task definition from execution: the API accepts any handler name as a string, and workers resolve it at runtime. To add a new task type, you only need to write a handler class and register it in `HANDLER_REGISTRY` — no API or schema changes required.

---

## Priority Queue & Delayed Jobs

### `app/queue.py`

```python
import json
import time
from datetime import datetime, timezone

import redis as redis_lib

from app.config import settings

redis_client = redis_lib.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)


# ─── Queue Keys ──────────────────────────────────────────────────
# Each priority level has a sorted set per partition.
# Key format: queue:{priority}:{partition}

PRIORITIES = ["critical", "high", "medium", "low"]
PROCESSING_PREFIX = "processing"
DLQ_KEY = "queue:dlq"


def _queue_key(priority: str, partition: int) -> str:
    return f"queue:{priority}:{partition}"


def _processing_key(partition: int) -> str:
    return f"queue:{PROCESSING_PREFIX}:{partition}"


# ─── Enqueue ──────────────────────────────────────────────────────

def enqueue(
    task_id: str,
    handler: str,
    payload: dict | None,
    priority: str,
    partition: int,
    scheduled_at: datetime | None = None,
):
    """
    Add a task to the priority queue for a specific partition.

    Tasks are stored in a Redis sorted set, scored by their
    scheduled execution time. This enables both immediate tasks
    (scored at now) and delayed tasks (scored at a future time).
    Workers only dequeue tasks with score <= now.
    """
    score = scheduled_at.timestamp() if scheduled_at else time.time()

    job = json.dumps({
        "task_id": task_id,
        "handler": handler,
        "payload": payload,
        "priority": priority,
        "partition": partition,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    })

    queue_key = _queue_key(priority, partition)
    redis_client.zadd(queue_key, {job: score})


# ─── Dequeue ──────────────────────────────────────────────────────

def dequeue(partition: int, count: int = 10) -> list[dict]:
    """
    Dequeue up to `count` ready tasks from a partition.

    Polls priority levels in order: critical → high → medium → low.
    Only picks tasks whose score <= current time (handles delayed).
    Uses a pipeline for atomicity: ZRANGEBYSCORE + ZREM + SADD
    ensures no two workers process the same task.
    """
    now = time.time()
    results = []

    for priority in PRIORITIES:
        if len(results) >= count:
            break

        queue_key = _queue_key(priority, partition)
        remaining = count - len(results)

        # Get tasks with score <= now (ready to execute)
        jobs = redis_client.zrangebyscore(
            queue_key, "-inf", now, start=0, num=remaining,
        )

        if not jobs:
            continue

        pipe = redis_client.pipeline()
        for job in jobs:
            pipe.zrem(queue_key, job)
            pipe.sadd(_processing_key(partition), job)
        pipe.execute()

        results.extend([json.loads(j) for j in jobs])

    return results


def complete_job(job_json: str, partition: int):
    """Mark a job as completed by removing from the processing set."""
    redis_client.srem(_processing_key(partition), job_json)


def fail_job(job_json: str, partition: int):
    """Move a failed job to the dead letter queue."""
    pipe = redis_client.pipeline()
    pipe.srem(_processing_key(partition), job_json)
    pipe.zadd(DLQ_KEY, {job_json: time.time()})
    pipe.execute()


# ─── Requeue (for retries) ───────────────────────────────────────

def requeue(
    task_id: str,
    handler: str,
    payload: dict | None,
    priority: str,
    partition: int,
    delay_seconds: int,
):
    """
    Requeue a task with a delay for retry.
    The task will not be picked up until now + delay_seconds.
    """
    scheduled_at = datetime.fromtimestamp(
        time.time() + delay_seconds,
        tz=timezone.utc,
    )
    enqueue(task_id, handler, payload, priority, partition, scheduled_at)


# ─── Queue Stats ──────────────────────────────────────────────────

def get_queue_depths() -> dict:
    """Get the current depth of each priority queue across all partitions."""
    depths = {p: 0 for p in PRIORITIES}
    depths["processing"] = 0
    depths["dlq"] = redis_client.zcard(DLQ_KEY)

    for partition in range(settings.partition_count):
        for priority in PRIORITIES:
            depths[priority] += redis_client.zcard(
                _queue_key(priority, partition)
            )
        depths["processing"] += redis_client.scard(
            _processing_key(partition)
        )

    return depths


def get_dlq_jobs(count: int = 50) -> list[dict]:
    """Get jobs from the dead letter queue for inspection."""
    jobs = redis_client.zrange(DLQ_KEY, 0, count - 1)
    return [json.loads(j) for j in jobs]


def requeue_dlq_job(job_json: str, priority: str, partition: int):
    """Move a job from the DLQ back to a priority queue for retry."""
    pipe = redis_client.pipeline()
    pipe.zrem(DLQ_KEY, job_json)
    pipe.zadd(_queue_key(priority, partition), {job_json: time.time()})
    pipe.execute()
```

The priority queue uses **partitioned Redis sorted sets** — one per priority level per partition (e.g., `queue:critical:0`, `queue:high:0`, ..., `queue:low:63`). Partitioning prevents hot spots: worker 1 only polls partitions 0–15, worker 2 polls 16–31, etc. Each task is scored by its execution timestamp, making delayed tasks invisible to `ZRANGEBYSCORE` until their scheduled time arrives. Workers poll in strict priority order (critical → high → medium → low), ensuring critical tasks are always processed first even under heavy load. `ZRANGEBYSCORE` + `ZREM` is executed atomically in a pipeline to prevent two workers from dequeuing the same task. The `processing` set tracks in-flight tasks; if a worker crashes, a supervisor can detect orphaned tasks and requeue them. The `requeue` function inserts a task with a future score, implementing the delay portion of exponential backoff.

---

## Cron & Recurring Schedules

### `app/cron_engine.py`

```python
import uuid
from datetime import datetime, timezone

from croniter import croniter
from sqlalchemy.orm import Session

from app.models import Schedule, Task, TaskStatus, Priority
from app.queue import enqueue
from app.config import settings


# ─── Cron Expression Utilities ────────────────────────────────────

def validate_cron(cron_expr: str) -> bool:
    """Validate a cron expression. Returns True if valid."""
    try:
        croniter(cron_expr)
        return True
    except (ValueError, KeyError):
        return False


def next_run_time(cron_expr: str, after: datetime | None = None, tz: str = "UTC") -> datetime:
    """
    Calculate the next execution time for a cron expression.

    Args:
        cron_expr: Standard 5-field cron expression
        after: Calculate next run after this time (default: now)
        tz: Timezone name (e.g., "US/Eastern", "UTC")
    """
    import pytz

    base = after or datetime.now(timezone.utc)
    local_tz = pytz.timezone(tz)
    local_time = base.astimezone(local_tz)

    cron = croniter(cron_expr, local_time)
    next_time = cron.get_next(datetime)

    # Convert back to UTC for storage
    return next_time.astimezone(timezone.utc)


# ─── Cron Evaluator (runs every second by the leader) ────────────

def evaluate_due_schedules(db: Session) -> int:
    """
    Find all active schedules whose next_run_at <= now,
    create a task instance for each, enqueue it, and
    update next_run_at.

    Returns the number of tasks enqueued.
    """
    now = datetime.now(timezone.utc)

    due_schedules = (
        db.query(Schedule)
        .filter(
            Schedule.is_active.is_(True),
            Schedule.next_run_at <= now,
        )
        .limit(1000)          # Process in batches to avoid long transactions
        .all()
    )

    enqueued = 0

    for schedule in due_schedules:
        task_id = uuid.uuid4()
        partition = hash(str(task_id)) % settings.partition_count

        # Create the task instance in the database
        task = Task(
            id=task_id,
            name=f"{schedule.name} [{now.strftime('%Y-%m-%d %H:%M')}]",
            handler=schedule.handler,
            payload=schedule.payload,
            priority=Priority(schedule.priority.value),
            status=TaskStatus.QUEUED,
            partition_key=partition,
            max_retries=schedule.max_retries,
            timeout_secs=schedule.timeout_secs,
            scheduled_at=now,
            callback_url=schedule.callback_url,
            schedule_id=schedule.id,
        )
        db.add(task)

        # Enqueue for worker pickup
        enqueue(
            task_id=str(task_id),
            handler=schedule.handler,
            payload=schedule.payload,
            priority=schedule.priority.value,
            partition=partition,
            scheduled_at=now,
        )

        # Advance next_run_at
        schedule.last_run_at = now
        schedule.next_run_at = next_run_time(
            schedule.cron_expr,
            after=now,
            tz=schedule.timezone,
        )

        enqueued += 1

    db.commit()
    return enqueued


# ─── Schedule Lifecycle ──────────────────────────────────────────

def create_schedule(
    name: str,
    handler: str,
    cron_expr: str,
    tz: str,
    payload: dict | None,
    priority: str,
    max_retries: int,
    timeout_secs: int,
    callback_url: str | None,
    db: Session,
) -> Schedule:
    """
    Create a new recurring schedule.
    Computes the first next_run_at based on the cron expression.
    """
    if not validate_cron(cron_expr):
        raise ValueError(f"Invalid cron expression: '{cron_expr}'")

    first_run = next_run_time(cron_expr, tz=tz)

    schedule = Schedule(
        name=name,
        handler=handler,
        cron_expr=cron_expr,
        timezone=tz,
        payload=payload,
        priority=Priority(priority),
        max_retries=max_retries,
        timeout_secs=timeout_secs,
        is_active=True,
        next_run_at=first_run,
        callback_url=callback_url,
    )
    db.add(schedule)
    db.commit()
    db.refresh(schedule)

    return schedule


def pause_schedule(schedule_id: str, db: Session) -> Schedule:
    """Pause a schedule, preventing future task creation."""
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if not schedule:
        raise ValueError(f"Schedule '{schedule_id}' not found")
    schedule.is_active = False
    db.commit()
    db.refresh(schedule)
    return schedule


def resume_schedule(schedule_id: str, db: Session) -> Schedule:
    """Resume a paused schedule, recomputing next_run_at."""
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if not schedule:
        raise ValueError(f"Schedule '{schedule_id}' not found")
    schedule.is_active = True
    schedule.next_run_at = next_run_time(
        schedule.cron_expr,
        tz=schedule.timezone,
    )
    db.commit()
    db.refresh(schedule)
    return schedule
```

The cron engine has two responsibilities: **validating and computing** cron schedules, and **evaluating due schedules**. `croniter` is the de-facto Python library for cron parsing, supporting standard 5-field expressions and extensions like `@daily`, `@hourly`. The evaluator runs every second on the leader-elected scheduler service. It queries `WHERE next_run_at <= now AND is_active = true`, creates a task instance for each due schedule, enqueues it, and advances `next_run_at` to the next occurrence. Processing is batched at 1000 schedules per tick to avoid long database transactions. Timezone-aware scheduling converts the cron expression to the user's timezone for evaluation, then converts back to UTC for storage. This ensures a schedule set to "9 AM US/Eastern" fires at the correct time regardless of DST transitions.

---

## Worker Pool & Execution Engine

### `app/executor.py`

```python
import json
import time
import signal
import threading
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import Task, TaskExecution, TaskStatus, ExecutionStatus
from app.queue import dequeue, complete_job, fail_job, requeue
from app.retry import calculate_backoff, should_retry
from app.callbacks import deliver_callback
from handlers import get_handler


class TaskExecutor:
    """
    Executes tasks dequeued from Redis priority queues.

    Each executor is assigned a set of partitions. It polls
    those partitions in a loop, dequeues ready tasks, runs
    the handler, records the result, and handles retries.
    """

    def __init__(self, partitions: list[int]):
        self.partitions = partitions
        self.worker_id = settings.worker_id
        self.running = True
        self.current_task_id = None

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        """Handle graceful shutdown."""
        print(f"[{self.worker_id}] Received shutdown signal, finishing current task...")
        self.running = False

    def run(self):
        """Main loop: poll partitions and execute tasks."""
        print(f"[{self.worker_id}] Starting executor for partitions {self.partitions}")
        poll_interval = settings.poll_interval_ms / 1000

        while self.running:
            executed = False

            for partition in self.partitions:
                if not self.running:
                    break

                jobs = dequeue(partition, count=5)

                for job in jobs:
                    self._execute_job(job, partition)
                    executed = True

            # If no tasks were found, sleep before next poll
            if not executed:
                time.sleep(poll_interval)

        print(f"[{self.worker_id}] Executor stopped.")

    def _execute_job(self, job: dict, partition: int):
        """Execute a single task job."""
        task_id = job["task_id"]
        handler_name = job["handler"]
        payload = job.get("payload") or {}
        priority = job["priority"]

        self.current_task_id = task_id
        job_json = json.dumps(job)

        db = SessionLocal()

        try:
            # 1. Update task status to RUNNING
            task = db.query(Task).filter(Task.id == task_id).first()
            if not task:
                complete_job(job_json, partition)
                return

            if task.status == TaskStatus.CANCELLED:
                complete_job(job_json, partition)
                return

            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(timezone.utc)
            db.commit()

            # 2. Look up and validate the handler
            try:
                handler = get_handler(handler_name)
            except KeyError as e:
                self._record_failure(
                    task, db, job_json, partition, priority,
                    "UNKNOWN_HANDLER", str(e),
                )
                return

            errors = handler.validate_payload(payload)
            if errors:
                self._record_failure(
                    task, db, job_json, partition, priority,
                    "INVALID_PAYLOAD", "; ".join(errors),
                )
                return

            # 3. Execute with timeout
            start_time = time.time()
            result = self._execute_with_timeout(
                handler, payload, task.timeout_secs,
            )
            duration_ms = int((time.time() - start_time) * 1000)

            # 4. Record execution attempt
            attempt = TaskExecution(
                task_id=task_id,
                attempt_number=task.retry_count + 1,
                worker_id=self.worker_id,
                status=(
                    ExecutionStatus.SUCCESS if result.success
                    else ExecutionStatus.FAILED
                ),
                error_code=result.error_code,
                error_message=result.error_message,
                result=result.data,
                duration_ms=duration_ms,
                started_at=task.started_at,
                completed_at=datetime.now(timezone.utc),
            )
            db.add(attempt)

            # 5. Handle success or failure
            if result.success:
                task.status = TaskStatus.COMPLETED
                task.result = result.data
                task.completed_at = datetime.now(timezone.utc)
                complete_job(job_json, partition)
                db.commit()

                # Deliver callback if configured
                if task.callback_url:
                    deliver_callback(task.callback_url, task_id, "completed", result.data)

                # Check if any dependent tasks can now be unblocked
                self._unblock_dependents(task_id, db)

            else:
                self._record_failure(
                    task, db, job_json, partition, priority,
                    result.error_code, result.error_message,
                )

        except Exception as e:
            # Unexpected error — treat as failure
            task = db.query(Task).filter(Task.id == task_id).first()
            if task:
                self._record_failure(
                    task, db, job_json, partition, priority,
                    "UNHANDLED_EXCEPTION", str(e),
                )
            else:
                fail_job(job_json, partition)

        finally:
            db.close()
            self.current_task_id = None

    def _execute_with_timeout(self, handler, payload: dict, timeout_secs: int):
        """
        Run the handler with a timeout.
        Uses a thread so we can enforce the timeout without killing the process.
        """
        from handlers.base import TaskResult

        result_container = [None]
        exception_container = [None]

        def target():
            try:
                result_container[0] = handler.execute(payload)
            except Exception as e:
                exception_container[0] = e

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=timeout_secs)

        if thread.is_alive():
            # Task timed out
            return TaskResult(
                success=False,
                error_code="TIMEOUT",
                error_message=f"Task exceeded {timeout_secs}s timeout",
            )

        if exception_container[0]:
            return TaskResult(
                success=False,
                error_code="EXCEPTION",
                error_message=str(exception_container[0]),
            )

        return result_container[0]

    def _record_failure(
        self,
        task: Task,
        db: Session,
        job_json: str,
        partition: int,
        priority: str,
        error_code: str,
        error_message: str,
    ):
        """Handle a task failure: retry or move to DLQ."""
        task.error_message = error_message

        # Record the failed attempt
        attempt = TaskExecution(
            task_id=task.id,
            attempt_number=task.retry_count + 1,
            worker_id=self.worker_id,
            status=ExecutionStatus.FAILED,
            error_code=error_code,
            error_message=error_message,
            started_at=task.started_at or datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
        db.add(attempt)

        if should_retry(task.retry_count, task.max_retries, error_code):
            # Retry with exponential backoff
            delay = calculate_backoff(task.retry_count)
            task.retry_count += 1
            task.status = TaskStatus.RETRYING
            db.commit()

            # Remove from processing and requeue with delay
            from app.queue import redis_client
            redis_client.srem(f"queue:processing:{partition}", job_json)

            requeue(
                task_id=str(task.id),
                handler=task.handler,
                payload=task.payload,
                priority=priority,
                partition=partition,
                delay_seconds=delay,
            )
        else:
            # Exhausted retries — move to DLQ
            task.status = TaskStatus.FAILED
            task.failed_at = datetime.now(timezone.utc)
            fail_job(job_json, partition)
            db.commit()

            if task.callback_url:
                deliver_callback(
                    task.callback_url, str(task.id),
                    "failed", {"error": error_message},
                )

    def _unblock_dependents(self, completed_task_id: str, db: Session):
        """
        Check if any tasks depend on the completed task.
        If all dependencies are satisfied, promote them to QUEUED.
        """
        dependents = (
            db.query(Task)
            .filter(
                Task.depends_on.any(completed_task_id),
                Task.status == TaskStatus.PENDING,
            )
            .all()
        )

        for dep_task in dependents:
            # Check if ALL dependencies are now completed
            all_deps = dep_task.depends_on or []
            completed_deps = (
                db.query(Task)
                .filter(
                    Task.id.in_(all_deps),
                    Task.status == TaskStatus.COMPLETED,
                )
                .count()
            )

            if completed_deps == len(all_deps):
                dep_task.status = TaskStatus.QUEUED
                dep_task.scheduled_at = datetime.now(timezone.utc)
                db.commit()

                enqueue(
                    task_id=str(dep_task.id),
                    handler=dep_task.handler,
                    payload=dep_task.payload,
                    priority=dep_task.priority.value,
                    partition=dep_task.partition_key,
                )
```

The `TaskExecutor` is the heart of the worker process. Each instance is assigned a set of partitions (e.g., partitions 0–15) and polls them in a loop. The execution pipeline: (1) dequeue a batch of ready tasks, (2) update status to `RUNNING`, (3) look up the handler by name, (4) validate the payload, (5) execute with a timeout using a daemon thread, (6) record the result as a `TaskExecution`, (7) on success, mark as `COMPLETED` and unblock dependents; on failure, either retry with backoff or move to the DLQ. Timeout enforcement uses `threading.Thread.join(timeout=...)`: if the handler exceeds the timeout, the thread is abandoned (daemon threads are killed when the process exits) and a `TIMEOUT` result is returned. Graceful shutdown via `SIGTERM` sets `self.running = False`, allowing the current task to finish before the worker exits — critical for preventing orphaned tasks during deployments.

### `worker.py`

```python
"""
Worker process entry point.

Each worker claims a set of partitions via the partitioner
and runs a TaskExecutor on those partitions.
"""

from app.config import settings
from app.partitioner import get_assigned_partitions
from app.executor import TaskExecutor


def main():
    worker_id = settings.worker_id
    partition_count = settings.partition_count

    # Get partitions assigned to this worker
    partitions = get_assigned_partitions(worker_id, partition_count)
    print(f"[{worker_id}] Assigned partitions: {partitions}")

    if not partitions:
        print(f"[{worker_id}] No partitions assigned. Exiting.")
        return

    # Start the executor
    executor = TaskExecutor(partitions)
    executor.run()


if __name__ == "__main__":
    main()
```

The worker process is lightweight: it resolves its partition assignments and starts the executor. In production, each worker instance has a unique `WORKER_ID` set via an environment variable (e.g., the Kubernetes pod name). The partitioner determines which partitions this worker is responsible for.

---

## Retry & Dead Letter Queue

### `app/retry.py`

```python
import math

from app.config import settings


# ─── Error Classification ────────────────────────────────────────

# Errors that should NOT be retried (permanent failures)
NON_RETRYABLE_ERRORS = {
    "INVALID_PAYLOAD",
    "UNKNOWN_HANDLER",
    "AUTHORIZATION_FAILED",
    "RESOURCE_NOT_FOUND",
}

# Errors that SHOULD be retried (transient failures)
RETRYABLE_ERRORS = {
    "TIMEOUT",
    "CONNECTION_ERROR",
    "RATE_LIMITED",
    "SERVICE_UNAVAILABLE",
    "EXCEPTION",
}


def should_retry(
    current_retries: int,
    max_retries: int,
    error_code: str | None,
) -> bool:
    """
    Determine if a failed task should be retried.

    Rules:
    1. Non-retryable errors are never retried
    2. Must not exceed max_retries
    3. Unknown error codes default to retryable
    """
    if error_code in NON_RETRYABLE_ERRORS:
        return False

    if current_retries >= max_retries:
        return False

    return True


def calculate_backoff(retry_count: int) -> int:
    """
    Calculate delay in seconds using exponential backoff with jitter.

    Formula: min(base * 2^retry + jitter, max_delay)

    Retry 0: ~30s
    Retry 1: ~60s
    Retry 2: ~120s
    Retry 3: ~240s
    Retry 4: ~480s (capped at max_delay)
    """
    import random

    base = settings.retry_base_delay
    max_delay = settings.retry_max_delay

    delay = base * (2 ** retry_count)

    # Add jitter (±25%) to prevent thundering herd
    jitter = delay * 0.25 * (random.random() * 2 - 1)
    delay = int(delay + jitter)

    return min(delay, max_delay)


# ─── DLQ Management ──────────────────────────────────────────────

def build_error_history(
    existing_history: list[dict] | None,
    error_code: str,
    error_message: str,
    attempt_number: int,
) -> list[dict]:
    """Build the error history JSON array for the DLQ entry."""
    history = existing_history or []
    history.append({
        "attempt": attempt_number,
        "error_code": error_code,
        "error_message": error_message,
    })
    return history
```

The retry module classifies errors into **retryable** (transient) and **non-retryable** (permanent). Transient errors like timeouts and rate limits are retried with exponential backoff; permanent errors like invalid payloads go straight to the DLQ. Backoff uses `base × 2^retry` with ±25% jitter. Jitter is critical in distributed systems: without it, all retries for a batch of failed tasks would hit the downstream service at exactly the same time (thundering herd), potentially causing another failure. The jitter spreads retries across a time window. The backoff is capped at `max_delay` (1 hour by default) to prevent absurdly long delays. `build_error_history` accumulates a full audit trail of every failure for the DLQ entry, so operators can see the progression of errors when investigating.

---

## Leader Election & Partition Assignment

### `app/leader.py`

```python
import time
import threading

from app.queue import redis_client
from app.config import settings


LEADER_KEY = "scheduler:leader"


class LeaderElection:
    """
    Distributed leader election using a Redis lock.

    Only the leader runs the cron evaluator. If the leader
    crashes, its lock expires after LEADER_TTL_SECONDS and
    another instance takes over.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.is_leader = False
        self.running = True
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        """
        Attempt to become the leader.

        Uses SET NX EX (set if not exists, with expiry) for
        atomic lock acquisition. Only one node succeeds.
        """
        acquired = redis_client.set(
            LEADER_KEY,
            self.node_id,
            nx=True,                          # Only set if key doesn't exist
            ex=settings.leader_ttl_seconds,   # Auto-expire if leader crashes
        )

        with self._lock:
            self.is_leader = bool(acquired)

        return self.is_leader

    def renew(self) -> bool:
        """
        Renew the leader lock.

        Only succeeds if this node is the current leader.
        Uses a Lua script for atomic check-and-extend.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = redis_client.eval(
            lua_script,
            1,
            LEADER_KEY,
            self.node_id,
            settings.leader_ttl_seconds,
        )

        with self._lock:
            self.is_leader = bool(result)

        return self.is_leader

    def release(self):
        """
        Release leadership voluntarily (during graceful shutdown).
        Only deletes the key if this node owns it.
        """
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        redis_client.eval(lua_script, 1, LEADER_KEY, self.node_id)

        with self._lock:
            self.is_leader = False

    def run_election_loop(self, on_leader_callback):
        """
        Continuously attempt to acquire or renew leadership.
        Calls on_leader_callback when this node is the leader.
        """
        while self.running:
            if self.is_leader:
                if self.renew():
                    on_leader_callback()
                else:
                    print(f"[{self.node_id}] Lost leadership.")
                    self.is_leader = False
            else:
                if self.try_acquire():
                    print(f"[{self.node_id}] Acquired leadership!")
                    on_leader_callback()

            time.sleep(settings.leader_renew_seconds)

    def stop(self):
        """Stop the election loop and release leadership."""
        self.running = False
        if self.is_leader:
            self.release()
```

Leader election uses Redis `SET NX EX` (set-if-not-exists with expiry) — the simplest correct distributed lock. Only one node can set the key, making it the leader. The key has a TTL (`LEADER_TTL_SECONDS`), so if the leader crashes without releasing the lock, it auto-expires and another node takes over. Renewal uses a Lua script for atomicity: it checks that the lock is still owned by this node before extending the TTL. Without the Lua script, there is a race condition: the lock could expire between the `GET` and `EXPIRE` calls, and another node could acquire it. Release also uses a Lua script to prevent accidentally deleting a lock owned by a different node. The election loop runs in the scheduler service, and `on_leader_callback` is the function that evaluates due cron schedules.

### `app/partitioner.py`

```python
import hashlib

from app.queue import redis_client
from app.config import settings


# ─── Worker Registry ──────────────────────────────────────────────

WORKERS_KEY = "scheduler:workers"
WORKER_TTL = 60  # Seconds before a worker is considered dead


def register_worker(worker_id: str):
    """Register this worker in the shared worker registry."""
    redis_client.hset(WORKERS_KEY, worker_id, "active")
    redis_client.expire(WORKERS_KEY, WORKER_TTL * 2)


def heartbeat(worker_id: str):
    """Update the worker's last heartbeat timestamp."""
    import time
    redis_client.hset(WORKERS_KEY, worker_id, str(time.time()))


def get_active_workers() -> list[str]:
    """Get a sorted list of all active worker IDs."""
    import time
    workers = redis_client.hgetall(WORKERS_KEY)
    now = time.time()

    active = []
    for wid, last_seen in workers.items():
        try:
            if now - float(last_seen) < WORKER_TTL:
                active.append(wid)
        except ValueError:
            active.append(wid)  # "active" string (initial registration)

    return sorted(active)


# ─── Partition Assignment ─────────────────────────────────────────

def get_assigned_partitions(
    worker_id: str,
    partition_count: int | None = None,
) -> list[int]:
    """
    Determine which partitions are assigned to this worker.

    Uses consistent hashing: partitions are distributed evenly
    across active workers. When a worker joins or leaves, only
    its partitions are redistributed (not all of them).
    """
    if partition_count is None:
        partition_count = settings.partition_count

    register_worker(worker_id)
    workers = get_active_workers()

    if not workers or worker_id not in workers:
        return []

    # Simple modulo assignment: partition P is assigned to
    # workers[P % len(workers)]
    assigned = []
    for p in range(partition_count):
        if workers[p % len(workers)] == worker_id:
            assigned.append(p)

    return assigned


def compute_partition_key(task_id: str, partition_count: int | None = None) -> int:
    """
    Compute the partition key for a task.

    Uses MD5 hash for uniform distribution across partitions.
    """
    if partition_count is None:
        partition_count = settings.partition_count

    hash_digest = hashlib.md5(task_id.encode()).hexdigest()
    return int(hash_digest, 16) % partition_count
```

The partitioner assigns partitions to workers using modulo distribution. With 64 partitions and 4 workers, each worker gets 16 partitions. When a worker joins (worker 5 appears), partitions are rebalanced: each worker now gets ~13 partitions. When a worker crashes, its heartbeat expires after `WORKER_TTL` seconds, and remaining workers absorb its partitions on the next assignment check. `compute_partition_key` uses MD5 hashing for uniform distribution: even if task IDs are sequential (UUIDs are not, but if they were), the hash distributes them evenly across partitions. Workers register themselves in a Redis hash and send heartbeats to prove they are alive. In production, workers would re-check their partition assignments periodically (e.g., every 30 seconds) to handle rebalancing.

---

## Core API – Tasks & Schedules

### `app/main.py`

```python
from fastapi import FastAPI

from app.routers import tasks, schedules, admin, live

app = FastAPI(
    title="Distributed Task Scheduler",
    description="Priority queues, delayed jobs, cron schedules, retries",
    version="1.0.0",
)

app.include_router(tasks.router, prefix="/tasks", tags=["Tasks"])
app.include_router(schedules.router, prefix="/schedules", tags=["Schedules"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(live.router, tags=["WebSocket"])


@app.get("/health")
def health_check():
    """Health check endpoint for load balancers and monitoring."""
    return {"status": "healthy"}
```

### `app/routers/tasks.py`

```python
import uuid
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Task, TaskStatus, Priority
from app.schemas import TaskCreate, TaskResponse, TaskCancel, ExecutionResponse
from app.queue import enqueue
from app.partitioner import compute_partition_key
from app.config import settings

router = APIRouter()


@router.post("", response_model=TaskResponse, status_code=201)
def create_task(req: TaskCreate, db: Session = Depends(get_db)):
    """
    Submit a new task for execution.

    The task is stored in PostgreSQL and enqueued into the
    appropriate Redis priority queue for worker pickup.
    """
    # Idempotency check
    if req.idempotency_key:
        existing = (
            db.query(Task)
            .filter(Task.idempotency_key == req.idempotency_key)
            .first()
        )
        if existing:
            return existing

    # Compute execution time
    if req.delay_seconds:
        scheduled_at = datetime.now(timezone.utc) + timedelta(
            seconds=req.delay_seconds,
        )
    elif req.scheduled_at:
        scheduled_at = req.scheduled_at
    else:
        scheduled_at = datetime.now(timezone.utc)

    # Compute partition
    task_id = uuid.uuid4()
    partition = compute_partition_key(str(task_id))

    # Determine initial status
    has_dependencies = bool(req.depends_on)
    initial_status = TaskStatus.PENDING if has_dependencies else TaskStatus.QUEUED

    task = Task(
        id=task_id,
        name=req.name,
        handler=req.handler,
        payload=req.payload,
        priority=Priority(req.priority),
        status=initial_status,
        partition_key=partition,
        max_retries=req.max_retries,
        timeout_secs=req.timeout_secs,
        scheduled_at=scheduled_at,
        callback_url=req.callback_url,
        idempotency_key=req.idempotency_key,
        depends_on=req.depends_on,
    )
    db.add(task)
    db.commit()
    db.refresh(task)

    # Only enqueue if no dependencies
    if not has_dependencies:
        enqueue(
            task_id=str(task.id),
            handler=req.handler,
            payload=req.payload,
            priority=req.priority,
            partition=partition,
            scheduled_at=scheduled_at,
        )

    return task


@router.get("/{task_id}", response_model=TaskResponse)
def get_task(task_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get a task by ID."""
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.delete("/{task_id}")
def cancel_task(
    task_id: uuid.UUID,
    req: TaskCancel,
    db: Session = Depends(get_db),
):
    """
    Cancel a pending or queued task.

    Running tasks cannot be cancelled (they must time out or complete).
    """
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel a task with status '{task.status.value}'",
        )

    if task.status == TaskStatus.RUNNING:
        raise HTTPException(
            status_code=409,
            detail="Cannot cancel a running task. Wait for it to complete or time out.",
        )

    task.status = TaskStatus.CANCELLED
    task.error_message = req.reason or "Cancelled by user"
    db.commit()

    return {"message": "Task cancelled", "task_id": str(task_id)}


@router.get("/{task_id}/executions", response_model=list[ExecutionResponse])
def get_task_executions(
    task_id: uuid.UUID,
    db: Session = Depends(get_db),
):
    """Get all execution attempts for a task."""
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task.executions


@router.get("", response_model=list[TaskResponse])
def list_tasks(
    status: str | None = Query(None),
    handler: str | None = Query(None),
    priority: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List tasks with optional filters."""
    query = db.query(Task)

    if status:
        query = query.filter(Task.status == TaskStatus(status))
    if handler:
        query = query.filter(Task.handler == handler)
    if priority:
        query = query.filter(Task.priority == Priority(priority))

    query = query.order_by(Task.created_at.desc())
    return query.offset(offset).limit(limit).all()
```

The tasks router provides CRUD endpoints for task management. `POST /tasks` handles three scheduling modes: immediate (no `scheduled_at` or `delay_seconds`), delayed (`delay_seconds` converts to an absolute timestamp), and absolute (`scheduled_at`). Tasks with dependencies start as `PENDING` and are not enqueued until all dependencies complete. The `DELETE` endpoint only cancels tasks in `PENDING` or `QUEUED` status — running tasks must complete or time out naturally to prevent data corruption from interrupted handlers. List endpoints support filtering by status, handler, and priority for operational dashboards.

### `app/routers/schedules.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Schedule
from app.schemas import ScheduleCreate, ScheduleResponse, ScheduleUpdate
from app.cron_engine import create_schedule, pause_schedule, resume_schedule, validate_cron

router = APIRouter()


@router.post("", response_model=ScheduleResponse, status_code=201)
def create_recurring_schedule(
    req: ScheduleCreate,
    db: Session = Depends(get_db),
):
    """
    Create a new recurring schedule.

    The cron expression defines when tasks are created.
    Examples:
        "*/5 * * * *"   → Every 5 minutes
        "0 9 * * 1-5"   → Weekdays at 9:00 AM
        "0 0 1 * *"     → First day of every month at midnight
        "30 */2 * * *"  → Every 2 hours at :30
    """
    if not validate_cron(req.cron_expr):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid cron expression: '{req.cron_expr}'",
        )

    schedule = create_schedule(
        name=req.name,
        handler=req.handler,
        cron_expr=req.cron_expr,
        tz=req.timezone,
        payload=req.payload,
        priority=req.priority,
        max_retries=req.max_retries,
        timeout_secs=req.timeout_secs,
        callback_url=req.callback_url,
        db=db,
    )

    return schedule


@router.get("/{schedule_id}", response_model=ScheduleResponse)
def get_schedule(schedule_id: uuid.UUID, db: Session = Depends(get_db)):
    """Get a schedule by ID."""
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return schedule


@router.patch("/{schedule_id}", response_model=ScheduleResponse)
def update_schedule(
    schedule_id: uuid.UUID,
    req: ScheduleUpdate,
    db: Session = Depends(get_db),
):
    """Update a schedule's configuration."""
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    if req.cron_expr is not None:
        if not validate_cron(req.cron_expr):
            raise HTTPException(
                status_code=422,
                detail=f"Invalid cron expression: '{req.cron_expr}'",
            )
        schedule.cron_expr = req.cron_expr
        # Recompute next_run_at with new expression
        from app.cron_engine import next_run_time
        schedule.next_run_at = next_run_time(
            req.cron_expr,
            tz=schedule.timezone,
        )

    if req.payload is not None:
        schedule.payload = req.payload
    if req.priority is not None:
        from app.models import Priority
        schedule.priority = Priority(req.priority)
    if req.is_active is not None:
        schedule.is_active = req.is_active
    if req.max_retries is not None:
        schedule.max_retries = req.max_retries
    if req.timeout_secs is not None:
        schedule.timeout_secs = req.timeout_secs

    db.commit()
    db.refresh(schedule)
    return schedule


@router.post("/{schedule_id}/pause", response_model=ScheduleResponse)
def pause(schedule_id: uuid.UUID, db: Session = Depends(get_db)):
    """Pause a schedule. No new tasks will be created."""
    try:
        return pause_schedule(str(schedule_id), db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{schedule_id}/resume", response_model=ScheduleResponse)
def resume(schedule_id: uuid.UUID, db: Session = Depends(get_db)):
    """Resume a paused schedule. Recomputes next_run_at."""
    try:
        return resume_schedule(str(schedule_id), db)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/{schedule_id}")
def delete_schedule(schedule_id: uuid.UUID, db: Session = Depends(get_db)):
    """Delete a schedule permanently."""
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    db.delete(schedule)
    db.commit()
    return {"message": "Schedule deleted", "schedule_id": str(schedule_id)}


@router.get("", response_model=list[ScheduleResponse])
def list_schedules(
    is_active: bool | None = Query(None),
    handler: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List schedules with optional filters."""
    query = db.query(Schedule)

    if is_active is not None:
        query = query.filter(Schedule.is_active == is_active)
    if handler:
        query = query.filter(Schedule.handler == handler)

    query = query.order_by(Schedule.created_at.desc())
    return query.offset(offset).limit(limit).all()
```

The schedules router provides full CRUD for recurring tasks. The `PATCH` endpoint lets operators change a schedule's cron expression, payload, or priority without deleting and recreating it. When the cron expression changes, `next_run_at` is recomputed immediately so the scheduler service picks up the new timing. Pause/resume endpoints control whether the scheduler creates task instances for a schedule. Deleting a schedule does not cancel already-created task instances — they continue to execute.

### `app/routers/admin.py`

```python
import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DeadLetterEntry
from app.schemas import QueueStats, DLQEntry
from app.queue import get_queue_depths, get_dlq_jobs, requeue_dlq_job
from app.partitioner import get_active_workers
from app.config import settings

router = APIRouter()


@router.get("/queue/stats", response_model=QueueStats)
def queue_stats():
    """Get current queue depths across all partitions."""
    depths = get_queue_depths()
    workers = get_active_workers()

    return QueueStats(
        critical=depths.get("critical", 0),
        high=depths.get("high", 0),
        medium=depths.get("medium", 0),
        low=depths.get("low", 0),
        processing=depths.get("processing", 0),
        dlq=depths.get("dlq", 0),
        total_workers=len(workers),
        partitions=settings.partition_count,
    )


@router.get("/dlq", response_model=list[DLQEntry])
def list_dlq(db: Session = Depends(get_db)):
    """List entries in the dead letter queue."""
    entries = (
        db.query(DeadLetterEntry)
        .order_by(DeadLetterEntry.exhausted_at.desc())
        .limit(100)
        .all()
    )
    return entries


@router.post("/dlq/{entry_id}/requeue")
def requeue_entry(
    entry_id: int,
    priority: str = "medium",
    db: Session = Depends(get_db),
):
    """
    Requeue a DLQ entry back to the priority queue.
    Resets the task's retry count and status.
    """
    entry = (
        db.query(DeadLetterEntry)
        .filter(DeadLetterEntry.id == entry_id)
        .first()
    )
    if not entry:
        raise HTTPException(status_code=404, detail="DLQ entry not found")

    from app.models import Task, TaskStatus
    from app.queue import enqueue
    from app.partitioner import compute_partition_key

    task = db.query(Task).filter(Task.id == entry.task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Original task not found")

    # Reset task for retry
    task.status = TaskStatus.QUEUED
    task.retry_count = 0
    task.error_message = None
    task.failed_at = None

    entry.requeued_at = __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc
    )
    entry.requeue_count += 1

    partition = compute_partition_key(str(task.id))
    db.commit()

    enqueue(
        task_id=str(task.id),
        handler=task.handler,
        payload=task.payload,
        priority=priority,
        partition=partition,
    )

    return {
        "message": "Task requeued from DLQ",
        "task_id": str(task.id),
        "partition": partition,
    }


@router.get("/workers")
def list_workers():
    """List all active workers and their status."""
    workers = get_active_workers()
    return {
        "total": len(workers),
        "workers": workers,
        "partitions": settings.partition_count,
    }
```

The admin router provides operational endpoints for monitoring and managing the scheduler. Queue stats show depths across all priority levels and partitions, helping operators identify backlogs. The DLQ inspection endpoint lets operators view failed tasks and their full error history. The requeue endpoint resets a failed task's state and re-enqueues it, useful when the root cause has been fixed (e.g., a downstream service was restored). The workers endpoint shows all active workers, useful for verifying that partition assignment is balanced.

---

## Real-Time Status (WebSockets)

### `app/websocket_manager.py`

```python
import json
import asyncio
from collections import defaultdict

from fastapi import WebSocket


class WebSocketManager:
    """
    Manages WebSocket connections for real-time task status updates.

    Clients connect per task_id and receive status updates
    as the task transitions through its lifecycle.
    """

    def __init__(self):
        # task_id → set of connected WebSockets
        self.connections: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, task_id: str, websocket: WebSocket):
        """Accept a WebSocket connection for a task."""
        await websocket.accept()
        self.connections[task_id].add(websocket)

    async def disconnect(self, task_id: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.connections[task_id].discard(websocket)
        if not self.connections[task_id]:
            del self.connections[task_id]

    async def broadcast_status(
        self,
        task_id: str,
        status: str,
        data: dict | None = None,
    ):
        """
        Broadcast a status update to all clients watching a task.

        Called by the executor when a task's status changes.
        """
        message = json.dumps({
            "task_id": task_id,
            "status": status,
            "data": data,
        })

        dead_connections = set()

        for ws in self.connections.get(task_id, set()):
            try:
                await ws.send_text(message)
            except Exception:
                dead_connections.add(ws)

        # Clean up dead connections
        for ws in dead_connections:
            self.connections[task_id].discard(ws)


manager = WebSocketManager()
```

### `app/routers/live.py`

```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/status/{task_id}")
async def task_status_stream(task_id: str, websocket: WebSocket):
    """
    WebSocket endpoint for real-time task status updates.

    Connect: ws://host/ws/status/{task_id}

    Messages are JSON: {"task_id": "...", "status": "running", "data": {...}}

    Status transitions:
        queued → running → completed
        queued → running → failed → retrying → running → completed
        pending → queued → running → completed (with dependencies)
    """
    await manager.connect(task_id, websocket)

    try:
        while True:
            # Keep connection alive; client can send heartbeats
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(task_id, websocket)
```

The WebSocket manager allows clients to subscribe to real-time status updates for a specific task. When the executor changes a task's status (QUEUED → RUNNING → COMPLETED), it calls `manager.broadcast_status()` to push the update to all connected clients. This is useful for UI dashboards and CLI tools that want to watch a task's progress without polling the API. Dead connections are cleaned up automatically when a send fails.

---

## Callbacks & Webhooks

### `app/callbacks.py`

```python
import httpx

from app.config import settings


def deliver_callback(
    callback_url: str,
    task_id: str,
    status: str,
    data: dict | None = None,
):
    """
    Deliver a webhook callback when a task completes or fails.

    Sends a POST request to the callback URL with the task
    result. Fire-and-forget: failures are logged but do not
    affect the task's final status.
    """
    payload = {
        "task_id": task_id,
        "status": status,
        "data": data,
    }

    try:
        response = httpx.post(
            callback_url,
            json=payload,
            timeout=settings.webhook_timeout_seconds,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "TaskScheduler/1.0",
            },
        )

        if response.status_code >= 400:
            print(
                f"[Callback] Failed for task {task_id}: "
                f"HTTP {response.status_code} from {callback_url}"
            )

    except httpx.TimeoutException:
        print(f"[Callback] Timeout for task {task_id}: {callback_url}")

    except Exception as e:
        print(f"[Callback] Error for task {task_id}: {e}")
```

Callbacks are fire-and-forget HTTP POST requests sent to a client-specified URL when a task completes or fails. The payload includes the task ID, final status, and result data. Callback failures are logged but do not affect the task's status — the task is still marked as completed even if the webhook delivery fails. In production, you would add retry logic for webhook delivery (with its own backoff) and store delivery status in a `webhook_deliveries` table.

---

## Scheduler Service

### `scheduler_service.py`

```python
"""
Scheduler service entry point.

Runs the cron evaluator on the leader-elected node.
Only one instance evaluates cron schedules at a time.
"""

import time
import signal
import threading

from app.config import settings
from app.database import SessionLocal
from app.leader import LeaderElection
from app.cron_engine import evaluate_due_schedules


class SchedulerService:
    """
    The scheduler service:
    1. Participates in leader election
    2. If elected, evaluates due cron schedules every second
    3. Creates task instances and enqueues them
    """

    def __init__(self):
        self.node_id = f"scheduler-{settings.worker_id}"
        self.leader = LeaderElection(self.node_id)
        self.running = True

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        print(f"[{self.node_id}] Shutting down...")
        self.running = False
        self.leader.stop()

    def run(self):
        """Main loop: participate in election and evaluate cron."""
        print(f"[{self.node_id}] Starting scheduler service...")

        # Start election loop in a background thread
        election_thread = threading.Thread(
            target=self.leader.run_election_loop,
            args=(self._on_leader_tick,),
            daemon=True,
        )
        election_thread.start()

        # Main thread: evaluate cron if leader
        while self.running:
            if self.leader.is_leader:
                self._evaluate_cron()
            time.sleep(settings.cron_eval_interval_seconds)

        print(f"[{self.node_id}] Scheduler service stopped.")

    def _on_leader_tick(self):
        """Called when this node is the leader (from election loop)."""
        pass  # Evaluation happens in the main loop

    def _evaluate_cron(self):
        """Evaluate due schedules and create task instances."""
        db = SessionLocal()
        try:
            count = evaluate_due_schedules(db)
            if count > 0:
                print(f"[{self.node_id}] Enqueued {count} scheduled tasks")
        except Exception as e:
            print(f"[{self.node_id}] Cron evaluation error: {e}")
        finally:
            db.close()


def main():
    service = SchedulerService()
    service.run()


if __name__ == "__main__":
    main()
```

The scheduler service is a separate process from the workers. It participates in leader election and, if elected, evaluates due cron schedules every second. Only one instance runs the evaluator at a time to prevent duplicate task creation. If the leader crashes, another instance takes over after the leader lock expires (`LEADER_TTL_SECONDS`). The evaluation loop queries `WHERE next_run_at <= now AND is_active = true`, creates task instances, enqueues them, and advances `next_run_at`. The main thread and election thread run independently: the election thread manages lock renewal, while the main thread does the actual cron evaluation.

---

## Docker & Docker Compose

### `Dockerfile`

```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["fastapi", "run", "app/main.py", "--port", "8000"]
```

### `Dockerfile.worker`

```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "worker.py"]
```

### `Dockerfile.scheduler`

```dockerfile
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "scheduler_service.py"]
```

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

# Tests
tests/
```

### `compose.yaml`

```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler
      - REDIS_URL=redis://redis:6379/0
      - BASE_URL=http://localhost:8000
    depends_on:
      - db
      - redis
    volumes:
      - .:/app

  worker-1:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler
      - REDIS_URL=redis://redis:6379/0
      - WORKER_ID=worker-1
      - PARTITION_COUNT=64
      - POLL_INTERVAL_MS=100
      - MAX_RETRY_ATTEMPTS=5
      - RETRY_BASE_DELAY=30
      - TASK_TIMEOUT_SECONDS=300
    depends_on:
      - db
      - redis

  worker-2:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler
      - REDIS_URL=redis://redis:6379/0
      - WORKER_ID=worker-2
      - PARTITION_COUNT=64
      - POLL_INTERVAL_MS=100
      - MAX_RETRY_ATTEMPTS=5
      - RETRY_BASE_DELAY=30
      - TASK_TIMEOUT_SECONDS=300
    depends_on:
      - db
      - redis

  scheduler:
    build:
      context: .
      dockerfile: Dockerfile.scheduler
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/scheduler
      - REDIS_URL=redis://redis:6379/0
      - WORKER_ID=scheduler-1
      - LEADER_TTL_SECONDS=30
      - LEADER_RENEW_SECONDS=10
      - CRON_EVAL_INTERVAL_SECONDS=1
    depends_on:
      - db
      - redis

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=scheduler
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

volumes:
  db-data:
  redis-data:
```

`services` defines six containers: the API server, two workers, the scheduler service, PostgreSQL, and Redis. Each worker has a unique `WORKER_ID` for partition assignment. The scheduler service participates in leader election; if you run multiple scheduler instances (for high availability), only one evaluates cron schedules at a time. `depends_on` ensures PostgreSQL and Redis start before the application containers. `redis:7-alpine` is a minimal Redis image with persistence enabled via the volume mount. Workers share the same `PARTITION_COUNT` so partition assignment is consistent.

### Docker Compose Commands

```bash
docker compose up                     # Start all services (foreground)
docker compose up -d                  # Start in background (detached)
docker compose up --build             # Rebuild images before starting
docker compose down                   # Stop and remove containers + network
docker compose down -v                # Also remove volumes
docker compose ps                     # List running services
docker compose logs                   # View logs from all services
docker compose logs -f worker-1       # Follow logs for worker-1
docker compose exec api bash          # Open shell in the API container
docker compose scale worker-1=3       # Scale to 3 worker instances
```

---

## Testing

### `tests/conftest.py`

```python
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from app.database import Base, get_db
from app.main import app


TEST_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestSession = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@pytest.fixture(autouse=True)
def db():
    """Create a fresh database for each test."""
    Base.metadata.create_all(bind=engine)
    session = TestSession()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client(db):
    """Create a test client with database override."""
    def override_get_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
```

`autouse=True` ensures every test gets a fresh database without explicitly requesting the fixture. `SQLite` is used for testing to avoid needing a running PostgreSQL instance. `check_same_thread=False` allows SQLite to be used across threads. `Base.metadata.create_all` creates all tables before each test, and `drop_all` removes them after, ensuring test isolation.

### `tests/test_queue.py`

```python
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

from app.queue import enqueue, dequeue, complete_job, fail_job, requeue


def test_enqueue_immediate_task():
    """Test that immediate tasks are enqueued with current timestamp."""
    with patch("app.queue.redis_client") as mock_redis:
        enqueue(
            task_id="task-1",
            handler="cleanup",
            payload={"table": "logs"},
            priority="medium",
            partition=5,
        )

        mock_redis.zadd.assert_called_once()
        call_args = mock_redis.zadd.call_args
        assert "queue:medium:5" == call_args[0][0]


def test_enqueue_delayed_task():
    """Test that delayed tasks use a future timestamp as score."""
    with patch("app.queue.redis_client") as mock_redis:
        future = datetime.now(timezone.utc) + timedelta(hours=1)

        enqueue(
            task_id="task-2",
            handler="email_report",
            payload={},
            priority="high",
            partition=10,
            scheduled_at=future,
        )

        mock_redis.zadd.assert_called_once()
        call_args = mock_redis.zadd.call_args
        assert "queue:high:10" == call_args[0][0]


def test_dequeue_respects_priority_order():
    """Test that critical tasks are dequeued before low tasks."""
    with patch("app.queue.redis_client") as mock_redis:
        import json

        critical_job = json.dumps({"task_id": "c1", "handler": "h", "priority": "critical"})
        mock_redis.zrangebyscore.side_effect = [
            [critical_job],  # critical queue returns a job
            [],              # high queue empty
            [],              # medium queue empty
            [],              # low queue empty
        ]
        mock_redis.pipeline.return_value = MagicMock()

        results = dequeue(partition=0, count=5)
        assert len(results) == 1
        assert results[0]["priority"] == "critical"


def test_complete_job_removes_from_processing():
    """Test that completing a job removes it from the processing set."""
    with patch("app.queue.redis_client") as mock_redis:
        complete_job('{"task_id": "t1"}', partition=3)
        mock_redis.srem.assert_called_once_with("queue:processing:3", '{"task_id": "t1"}')


def test_fail_job_moves_to_dlq():
    """Test that failing a job moves it to the dead letter queue."""
    with patch("app.queue.redis_client") as mock_redis:
        mock_pipe = mock_redis.pipeline.return_value
        fail_job('{"task_id": "t2"}', partition=7)
        mock_pipe.srem.assert_called_once()
        mock_pipe.zadd.assert_called_once()
```

### `tests/test_cron.py`

```python
from datetime import datetime, timezone

from app.cron_engine import validate_cron, next_run_time


def test_validate_valid_cron():
    """Test that valid cron expressions pass validation."""
    assert validate_cron("*/5 * * * *") is True
    assert validate_cron("0 9 * * 1-5") is True
    assert validate_cron("0 0 1 * *") is True
    assert validate_cron("30 */2 * * *") is True


def test_validate_invalid_cron():
    """Test that invalid cron expressions fail validation."""
    assert validate_cron("not a cron") is False
    assert validate_cron("* * *") is False
    assert validate_cron("") is False


def test_next_run_time_every_5_minutes():
    """Test next run time for a 5-minute interval."""
    base = datetime(2025, 6, 15, 10, 3, 0, tzinfo=timezone.utc)
    next_time = next_run_time("*/5 * * * *", after=base)

    # Next 5-minute mark after 10:03 is 10:05
    assert next_time.minute == 5
    assert next_time.hour == 10


def test_next_run_time_daily_at_9am():
    """Test next run time for a daily-at-9am schedule."""
    base = datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    next_time = next_run_time("0 9 * * *", after=base)

    # Next 9 AM after 10 AM today is 9 AM tomorrow
    assert next_time.hour == 9
    assert next_time.day == 16


def test_next_run_time_weekdays_only():
    """Test that weekday schedules skip weekends."""
    # June 14, 2025 is a Saturday
    base = datetime(2025, 6, 14, 10, 0, 0, tzinfo=timezone.utc)
    next_time = next_run_time("0 9 * * 1-5", after=base)

    # Next weekday (Monday) is June 16
    assert next_time.weekday() == 0  # Monday
```

### `tests/test_retry.py`

```python
from app.retry import should_retry, calculate_backoff


def test_retry_on_timeout():
    """Test that timeout errors are retried."""
    assert should_retry(0, 3, "TIMEOUT") is True


def test_retry_on_connection_error():
    """Test that connection errors are retried."""
    assert should_retry(1, 3, "CONNECTION_ERROR") is True


def test_no_retry_on_invalid_payload():
    """Test that invalid payload errors are NOT retried."""
    assert should_retry(0, 3, "INVALID_PAYLOAD") is False


def test_no_retry_on_unknown_handler():
    """Test that unknown handler errors are NOT retried."""
    assert should_retry(0, 3, "UNKNOWN_HANDLER") is False


def test_no_retry_when_max_reached():
    """Test that retries stop when max is reached."""
    assert should_retry(3, 3, "TIMEOUT") is False


def test_backoff_increases_exponentially():
    """Test that backoff delay increases with each retry."""
    delays = [calculate_backoff(i) for i in range(5)]

    # Each delay should be roughly double the previous (±jitter)
    for i in range(1, len(delays)):
        # Allow for jitter: delay[i] should be > delay[i-1] * 1.2
        # (conservative check given ±25% jitter)
        assert delays[i] >= delays[i - 1] * 0.5


def test_backoff_respects_max_delay():
    """Test that backoff is capped at max_delay."""
    from app.config import settings
    delay = calculate_backoff(20)  # Very high retry count
    assert delay <= settings.retry_max_delay
```

### `tests/test_tasks.py`

```python
import uuid
from unittest.mock import patch

from app.models import Task, TaskStatus, Priority


def test_create_immediate_task(client, db):
    """Test creating a task for immediate execution."""
    with patch("app.routers.tasks.enqueue"):
        response = client.post("/tasks", json={
            "name": "Test Task",
            "handler": "cleanup",
            "payload": {"table": "logs"},
            "priority": "medium",
        })

    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Test Task"
    assert data["handler"] == "cleanup"
    assert data["status"] == "queued"


def test_create_delayed_task(client, db):
    """Test creating a task with a delay."""
    with patch("app.routers.tasks.enqueue"):
        response = client.post("/tasks", json={
            "name": "Delayed Task",
            "handler": "email_report",
            "payload": {"report_type": "daily"},
            "delay_seconds": 3600,
        })

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "queued"


def test_create_task_with_dependencies(client, db):
    """Test that tasks with dependencies start as PENDING."""
    dep_id = str(uuid.uuid4())

    with patch("app.routers.tasks.enqueue"):
        response = client.post("/tasks", json={
            "name": "Dependent Task",
            "handler": "cleanup",
            "depends_on": [dep_id],
        })

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "pending"


def test_idempotency_returns_existing(client, db):
    """Test that duplicate idempotency keys return the existing task."""
    with patch("app.routers.tasks.enqueue"):
        response1 = client.post("/tasks", json={
            "name": "Idempotent Task",
            "handler": "cleanup",
            "idempotency_key": "unique-key-123",
        })
        response2 = client.post("/tasks", json={
            "name": "Idempotent Task Retry",
            "handler": "cleanup",
            "idempotency_key": "unique-key-123",
        })

    assert response1.json()["id"] == response2.json()["id"]


def test_get_task(client, db):
    """Test retrieving a task by ID."""
    task = Task(
        id=uuid.uuid4(),
        name="Lookup Test",
        handler="cleanup",
        priority=Priority.MEDIUM,
        status=TaskStatus.COMPLETED,
        partition_key=0,
        scheduled_at=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
    )
    db.add(task)
    db.commit()

    response = client.get(f"/tasks/{task.id}")
    assert response.status_code == 200
    assert response.json()["status"] == "completed"


def test_cancel_queued_task(client, db):
    """Test cancelling a queued task."""
    task = Task(
        id=uuid.uuid4(),
        name="Cancel Test",
        handler="cleanup",
        priority=Priority.LOW,
        status=TaskStatus.QUEUED,
        partition_key=0,
        scheduled_at=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
    )
    db.add(task)
    db.commit()

    response = client.delete(
        f"/tasks/{task.id}",
        json={"reason": "No longer needed"},
    )
    assert response.status_code == 200


def test_cancel_completed_task_fails(client, db):
    """Test that completed tasks cannot be cancelled."""
    task = Task(
        id=uuid.uuid4(),
        name="Completed Task",
        handler="cleanup",
        priority=Priority.MEDIUM,
        status=TaskStatus.COMPLETED,
        partition_key=0,
        scheduled_at=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
    )
    db.add(task)
    db.commit()

    response = client.delete(
        f"/tasks/{task.id}",
        json={"reason": "Too late"},
    )
    assert response.status_code == 409


def test_list_tasks_with_filter(client, db):
    """Test listing tasks filtered by status."""
    for i in range(3):
        task = Task(
            name=f"Task {i}",
            handler="cleanup",
            priority=Priority.MEDIUM,
            status=TaskStatus.QUEUED,
            partition_key=i,
            scheduled_at=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc
            ),
        )
        db.add(task)
    db.commit()

    response = client.get("/tasks?status=queued")
    assert response.status_code == 200
    assert len(response.json()) == 3
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_retry"                        # Run only retry tests
pytest tests/test_queue.py                    # Run a specific test file
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
gcloud sql instances create scheduler-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-4-15360 \
  --region=us-central1 \
  --storage-size=500GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create scheduler \
  --instance=scheduler-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=scheduler-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-4-15360` provisions 4 vCPUs and 15 GB RAM, suitable for the write-heavy task logging workload (recording every execution attempt). `--availability-type=REGIONAL` enables automatic failover. `--storage-size=500GB` accommodates high write volume; with 30-day retention and 1 TB/day, aggressive partitioning by `created_at` month is required.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create scheduler-cache \
  --size=5 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe scheduler-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=5` provisions 5 GB of memory for priority queues, processing sets, distributed locks, and leader election keys. `--tier=STANDARD` enables replication with automatic failover, critical because losing Redis means losing queued tasks and leader state. Memorystore is accessible only from within the same VPC network.

### Build & Push Docker Images

```bash
# Build and push API image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/scheduler/api \
  --file=Dockerfile

# Build and push worker image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/scheduler/worker \
  --file=Dockerfile.worker

# Build and push scheduler image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/scheduler/scheduler \
  --file=Dockerfile.scheduler
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build. Three separate images are built: one for the API server, one for the workers, and one for the scheduler service.

### Deploy API to Cloud Run

```bash
gcloud run deploy scheduler-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/scheduler/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 20 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:scheduler-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/scheduler?host=/cloudsql/YOUR_PROJECT_ID:us-central1:scheduler-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
BASE_URL=https://your-service-url.run.app,\
PARTITION_COUNT=64"
```

`--min-instances 2` keeps at least two instances warm to avoid cold starts. `--max-instances 20` allows scaling for burst API traffic. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy, which provides secure access to the database without exposing it to the internet.

### Deploy Workers to GKE

```bash
# Workers need to run continuously, so GKE is preferred over Cloud Run
# Cloud Run has request timeouts; workers are long-running daemons

# Create a GKE cluster
gcloud container clusters create scheduler-cluster \
  --num-nodes=3 \
  --machine-type=e2-standard-4 \
  --region=us-central1 \
  --enable-autoscaling \
  --min-nodes=2 \
  --max-nodes=10

# Deploy workers as a Kubernetes Deployment
kubectl apply -f k8s/worker-deployment.yaml
```

Cloud Run is suitable for the stateless API server. For the always-on workers that continuously poll Redis, **GKE (Kubernetes Engine)** is preferred because workers are long-running daemons, not request-driven services. Each worker pod gets a unique `WORKER_ID` derived from its pod name, ensuring unique partition assignments.

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis)
gcloud compute networks vpc-access connectors create scheduler-vpc-connector \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update scheduler-api \
  --vpc-connector=scheduler-vpc-connector \
  --region=us-central1
```

Cloud Run services run in Google's serverless infrastructure outside your VPC by default. A VPC connector creates a bridge so Cloud Run can reach private resources like Memorystore. `--range=10.8.0.0/28` allocates a small IP range for the connector.

### Store Secrets Securely

```bash
# Store database credentials in Secret Manager
gcloud secrets create scheduler-db-password \
  --data-file=- <<< "YOUR_SECURE_PASSWORD"

# Grant Cloud Run access to secrets
gcloud secrets add-iam-policy-binding scheduler-db-password \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Reference secrets in Cloud Run
gcloud run services update scheduler-api \
  --update-secrets="DB_PASSWORD=scheduler-db-password:latest"
```

Database credentials and API keys should NEVER be in plain-text environment variables. Secret Manager encrypts them at rest and provides audit logging. `--update-secrets` mounts secrets as environment variables that are fetched from Secret Manager at container start.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Table Partitioning    | `tasks` exceeds 1 B rows                  | Partition by `created_at` month (range partition)  |
| Read Replicas         | Heavy dashboard/analytics queries          | Cloud SQL read replicas for task history queries    |
| Connection Pooling    | Many concurrent workers + API connections  | PgBouncer sidecar or Cloud SQL Auth Proxy pooling  |
| Archival              | Retention policy (30 days)                 | Move old records to BigQuery or Cold Storage       |
| CQRS                  | Write-heavy with separate read patterns    | Separate read model for task status dashboards     |

### Queue & Worker Scaling

| Strategy                   | When to Use                             | How                                               |
| -------------------------- | --------------------------------------- | ------------------------------------------------- |
| Increase Partitions        | Workers exceed 50 per partition         | Double `PARTITION_COUNT` (requires rebalancing)    |
| Auto-Scale Workers         | Queue depth fluctuates with traffic     | HPA on GKE based on Redis queue depth metrics      |
| Cloud Pub/Sub              | Redis can't handle 100 K RPS           | Replace Redis with Pub/Sub for durability + scale  |
| Priority Isolation         | Critical queue must never be starved    | Dedicated critical-only workers with reserved CPU  |
| Multi-Region Workers       | Workloads are geo-distributed           | Regional worker pools consuming partition subsets  |

### Reliability Patterns

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Exactly-Once Delivery | Task handlers are not idempotent           | Redis distributed lock per task_id during execution|
| Dead Letter Alerting  | DLQ depth indicates systematic failures    | Alert on DLQ depth > 100 via Cloud Monitoring      |
| Circuit Breaker       | Downstream dependency is failing           | Stop enqueuing tasks for a handler after N failures|
| Graceful Shutdown     | Worker pods are terminated during deploys  | Handle SIGTERM, finish current task, then exit     |
| Poison Pill Detection | A single bad task crashes workers          | Skip tasks that fail > N times in < M minutes     |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ scheduler.xyz  │
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
           │  │ Locks +  │ │              │  │
           │  │ Queues   │ │              │  │
           │  └────┬─────┘ └──────────────┘  │
           │       │                          │
           │  ┌────▼──────────────────────┐  │
           │  │    Cloud Pub/Sub          │  │
           │  │  ┌──────────────────────┐ │  │
           │  │  │ critical-tasks       │ │  │
           │  │  │ high-tasks           │ │  │
           │  │  │ medium-tasks         │ │  │
           │  │  │ low-tasks            │ │  │
           │  │  │ status-events        │ │  │
           │  │  └──────────────────────┘ │  │
           │  └────┬──────────────────────┘  │
           │       │                          │
           │  ┌────▼──────────────────────┐  │
           │  │    GKE Worker Pools       │  │
           │  │  ┌────────┐ ┌──────────┐ │  │
           │  │  │Critical│ │  High    │ │  │
           │  │  │Workers │ │ Workers  │ │  │
           │  │  │(reserv)│ │(auto-scl)│ │  │
           │  │  └────────┘ └──────────┘ │  │
           │  │  ┌────────┐ ┌──────────┐ │  │
           │  │  │ Medium │ │Scheduler │ │  │
           │  │  │Workers │ │ Service  │ │  │
           │  │  │(auto)  │ │(leader)  │ │  │
           │  │  └────────┘ └──────────┘ │  │
           │  └───────────────────────────┘  │
           │                                  │
           │  ┌───────────────────────────┐  │
           │  │  Secret Manager           │  │
           │  │  (DB creds, API keys)     │  │
           │  └───────────────────────────┘  │
           └──────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud Pub/Sub** replaces Redis as the primary queue for durability (Pub/Sub guarantees at-least-once delivery with persistent storage) and scale (handles millions of messages per second natively). Redis is retained for distributed locks, leader election, and partition tracking only.
- **GKE Worker Pools** run dedicated, always-on worker deployments per priority level. Critical workers have reserved CPU and memory (no autoscaling) to guarantee capacity. High and medium workers autoscale based on queue depth via Kubernetes HPA.
- **Priority Isolation** ensures critical tasks are never starved by a flood of low-priority tasks. Dedicated critical workers process only the critical queue, with guaranteed resources.
- **Scheduler Service HA** runs 2–3 replicas on GKE. Leader election ensures only one evaluates cron schedules; the others are hot standbys that take over within `LEADER_TTL_SECONDS` if the leader crashes.
- **Table Partitioning** on `tasks` by `created_at` month keeps query performance stable as the table grows. Partitions older than 30 days are dropped (or archived to BigQuery for analytics).
- **Secret Manager** stores database credentials and any API keys encrypted at rest with audit logging, replacing plain-text environment variables.

### Security Checklist

```
✓ Store all credentials in Secret Manager (never in env vars or code)
✓ Validate handler names against a whitelist (HANDLER_REGISTRY)
✓ Sanitize task payloads (no code injection via handler arguments)
✓ Rate limit the task submission API to prevent queue flooding
✓ Authenticate webhook callbacks with HMAC signatures
✓ Encrypt data in transit (TLS) and at rest (Cloud SQL encryption)
✓ Restrict worker network access (VPC firewall rules)
✓ Enable Cloud Audit Logs for compliance
✓ Set task payload size limits (e.g., 256 KB max)
✓ Rotate Redis AUTH passwords periodically
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create scheduler-health \
  --display-name="Scheduler API Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Critical queue depth > 50 (tasks are being delayed)
# - Total queue depth > 100 000 (processing backlog)
# - Task failure rate > 5% over 5 minutes
# - DLQ depth > 100 (systematic failures)
# - Worker crash/restart rate > 2 in 10 minutes
# - Cron evaluation lag > 5 seconds (missed schedules)
# - Leader election flapping > 3 transitions in 1 minute
# - Task execution p99 latency > 30 seconds
# - Cloud SQL connections > 90% of max
# - Redis memory usage > 85%
# - Worker partition imbalance > 20% deviation
```

Cloud Run and GKE automatically export metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing across the API → Redis → Worker → Handler chain. The most critical alert is **cron evaluation lag**: if the scheduler service falls behind, recurring tasks won't fire on time. The second most critical is **leader election flapping**: rapid leader transitions indicate Redis instability or network partitions, and each transition risks a few seconds of missed evaluations.
