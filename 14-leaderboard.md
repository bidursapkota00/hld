# Design Leaderboard / Distributed Counters

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [Redis Sorted Sets & Real-Time Ranking](#redis-sorted-sets--real-time-ranking)
6. [Distributed Counters (Sharded Writes)](#distributed-counters-sharded-writes)
7. [Leaderboard Engine (Top-K, Rank, Percentile)](#leaderboard-engine-top-k-rank-percentile)
8. [Time-Windowed Leaderboards (Daily, Weekly, All-Time)](#time-windowed-leaderboards-daily-weekly-all-time)
9. [Score Ingestion Pipeline](#score-ingestion-pipeline)
10. [Core API – Scores & Leaderboards](#core-api--scores--leaderboards)
11. [Real-Time Updates (WebSockets)](#real-time-updates-websockets)
12. [Docker & Docker Compose](#docker--docker-compose)
13. [Testing](#testing)
14. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
15. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement              | Description                                                              |
| ------------------------ | ------------------------------------------------------------------------ |
| Score Submission         | Accept score updates from millions of concurrent users                   |
| Real-Time Ranking        | Return a user's rank among all participants in < 50 ms                   |
| Top-K Leaderboard        | Fetch the top N users by score (e.g., top 100) in < 20 ms               |
| Surrounding Ranks        | Show users ranked around a given user (±N neighbors)                     |
| Time Windows             | Maintain separate leaderboards for daily, weekly, monthly, and all-time  |
| Distributed Counters     | High-throughput atomic score increments without contention               |
| Percentile Queries       | Return a user's percentile relative to all participants                  |
| Multi-Leaderboard        | Support multiple independent leaderboards (games, contests, features)    |
| Score Aggregation        | Sum, max, or replace strategies for score updates                        |
| Anti-Cheat Validation    | Reject impossible score jumps and flag suspicious patterns               |
| Historical Snapshots     | Periodic snapshots of leaderboard state for analytics                    |
| Real-Time Push           | Stream rank changes to connected clients via WebSockets                  |

### Non-Functional Requirements

| Requirement      | Target                                                     |
| ---------------- | ---------------------------------------------------------- |
| Availability     | 99.99% uptime (leaderboard downtime ruins user engagement) |
| Read Latency     | Top-K and rank queries in < 50 ms (p99)                    |
| Write Latency    | Score submissions acknowledged in < 100 ms                 |
| Throughput       | 500 K score updates/second at peak                         |
| Consistency      | Eventual consistency acceptable (< 2 s propagation delay)  |
| Scalability      | Handle 1 B total users, 100 M active per leaderboard       |

### Back-of-the-Envelope Estimations

```
Active users per leaderboard       ≈ 100 M
Total leaderboards                 ≈ 1 000 (games, contests, features)
Score updates per second           ≈ 500 K RPS (peak), 50 K RPS (avg)
Score updates per day              ≈ 50 K × 86 400 ≈ 4.3 B/day
Read queries per second            ≈ 2 M RPS (top-K, rank, surrounding)
Score record size                  ≈ 100 B (user_id + score + metadata)
Redis sorted set memory (100 M)   ≈ 100 M × 100 B = 10 GB per leaderboard
Total Redis memory (active)        ≈ 10 GB × 10 hot leaderboards = 100 GB
PostgreSQL storage per day         ≈ 4.3 B × 200 B = 860 GB/day (score events)
PostgreSQL retention (7 days)      ≈ 6 TB (with aggressive pruning)
Counter shards per leaderboard     ≈ 16–64 (tuned by write throughput)
Snapshot frequency                 ≈ Every 1 hour (for analytics)
```

500 K score updates per second requires sharded writes to avoid hot-key contention in Redis. A single Redis `ZADD` handles ~300 K ops/sec on a single key, but multiple leaderboards and time windows multiply the load. The architecture uses **distributed counter shards** to buffer writes: each shard accumulates increments locally and flushes to the canonical Redis sorted set periodically. Reads go directly to Redis sorted sets, which provide O(log N) rank lookups and O(log N + K) top-K queries natively. PostgreSQL stores the durable score event log and user profiles. Periodic snapshots persist the Redis leaderboard state to PostgreSQL for analytics and disaster recovery.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │(App / Web)   │
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
                │  POST /scores              │
                │  GET  /leaderboard/{id}/top│
                │  GET  /leaderboard/{id}/rank│
                │  WS   /ws/live/{board_id}  │
                └──┬──────────┬───────────┬──┘
                   │          │           │
            ┌──────▼───┐ ┌───▼────┐ ┌────▼──────┐
            │  Redis   │ │Postgre-│ │  Pub/Sub  │
            │ (Sorted  │ │  SQL   │ │ (Score    │
            │  Sets +  │ │(Events │ │  Events + │
            │ Counters)│ │& Users)│ │  Fan-Out) │
            └────┬─────┘ └────────┘ └─────┬─────┘
                 │                        │
        ┌────────▼────────────────────────▼────────┐
        │           Background Workers             │
        │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
        │  │ Counter   │ │ Snapshot │ │ Window   │ │
        │  │ Flusher   │ │ Worker   │ │ Rotator  │ │
        │  │ (shards→  │ │ (Redis→  │ │ (daily,  │ │
        │  │  sorted   │ │  PgSQL)  │ │  weekly  │ │
        │  │  sets)    │ │          │ │  reset)  │ │
        │  └──────────┘ └──────────┘ └──────────┘ │
        └──────────────────────────────────────────┘
```

The API server handles two distinct workloads: **writes** (score submissions) and **reads** (leaderboard queries). Score submissions are buffered into sharded Redis counters to avoid contention, then flushed to the canonical sorted set by a background counter flusher. Reads go directly to Redis sorted sets, which support O(log N) rank lookups and O(log N + K) range queries natively. PostgreSQL stores the durable score event log (for audit, analytics, and replay), user profiles, and leaderboard configuration. Pub/Sub broadcasts score events for WebSocket fan-out to connected clients. The snapshot worker periodically persists the full Redis leaderboard state to PostgreSQL for disaster recovery. The window rotator manages time-windowed leaderboards (daily, weekly), creating new sorted sets and archiving expired ones.

---

## Project Setup

### Project Structure

```
leaderboard/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── redis_client.py       # Redis connection and sorted set ops
│   ├── counters.py           # Distributed sharded counters
│   ├── ranking.py            # Leaderboard engine (top-K, rank, percentile)
│   ├── windows.py            # Time-windowed leaderboard management
│   ├── ingestion.py          # Score validation and ingestion pipeline
│   ├── snapshots.py          # Periodic leaderboard snapshots
│   ├── websocket_manager.py  # WebSocket connection manager
│   └── routers/
│       ├── __init__.py
│       ├── scores.py         # Score submission endpoints
│       ├── leaderboards.py   # Leaderboard query endpoints
│       ├── admin.py          # Admin and stats endpoints
│       └── live.py           # WebSocket real-time updates
├── workers/
│   ├── __init__.py
│   ├── counter_flusher.py    # Flush sharded counters to sorted sets
│   ├── snapshot_worker.py    # Persist leaderboard to PostgreSQL
│   └── window_rotator.py     # Rotate time-windowed leaderboards
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_counters.py      # Tests for distributed counters
│   ├── test_ranking.py       # Tests for ranking engine
│   ├── test_windows.py       # Tests for time windows
│   ├── test_ingestion.py     # Tests for score validation
│   └── test_api.py           # Tests for API endpoints
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
mkdir leaderboard && cd leaderboard
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
websockets==13.1
croniter==3.0.3
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for sorted sets, sharded counters, and Pub/Sub. `httpx` is an async HTTP client for webhook callbacks. `croniter` parses cron expressions for the window rotator schedule. `websockets` provides WebSocket support for real-time rank change notifications.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/leaderboard
REDIS_URL=redis://localhost:6379/0
BASE_URL=http://localhost:8000
COUNTER_SHARDS=16
COUNTER_FLUSH_INTERVAL_MS=500
MAX_SCORE_PER_UPDATE=10000
ANTI_CHEAT_MAX_DELTA=5000
ANTI_CHEAT_WINDOW_SECONDS=60
SNAPSHOT_INTERVAL_MINUTES=60
SNAPSHOT_TOP_N=10000
WEBSOCKET_BROADCAST_INTERVAL_MS=200
DEFAULT_LEADERBOARD_SIZE=100
MAX_LEADERBOARD_SIZE=1000
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `COUNTER_SHARDS` controls the number of shards per leaderboard for write distribution; 16 is a good default for up to ~200 K writes/sec per leaderboard. `COUNTER_FLUSH_INTERVAL_MS` controls how often shards are flushed to the canonical sorted set (500 ms = eventual consistency within ~1 second). `MAX_SCORE_PER_UPDATE` rejects score updates exceeding this value. `ANTI_CHEAT_MAX_DELTA` flags users whose score increases by more than this amount within `ANTI_CHEAT_WINDOW_SECONDS`. `SNAPSHOT_INTERVAL_MINUTES` controls how often the full leaderboard is persisted to PostgreSQL. `SNAPSHOT_TOP_N` limits snapshots to the top 10 000 users. `WEBSOCKET_BROADCAST_INTERVAL_MS` batches WebSocket broadcasts to reduce message volume.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│           leaderboards               │     │           score_events               │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            UUID       PRIMARY KEY │     │ id            BIGSERIAL  PRIMARY KEY │
│ name          VARCHAR    NOT NULL    │     │ leaderboard_id UUID     FK → boards  │
│ slug          VARCHAR    UNIQUE      │     │ user_id       UUID       NOT NULL    │
│ description   TEXT                   │     │ score_delta   BIGINT     NOT NULL    │
│ score_type    ENUM       NOT NULL    │     │ score_after   BIGINT     NOT NULL    │
│ sort_order    ENUM       DEFAULT DESC│     │ source        VARCHAR    NOT NULL    │
│ max_entries   INTEGER    DEFAULT 0   │     │ metadata      JSONB                  │
│ is_active     BOOLEAN    DEFAULT T   │     │ ip_address    VARCHAR               │
│ reset_cron    VARCHAR               │     │ flagged       BOOLEAN    DEFAULT F   │
│ metadata      JSONB                  │     │ created_at    TIMESTAMPTZ            │
│ created_at    TIMESTAMPTZ            │     ├──────────────────────────────────────┤
│ updated_at    TIMESTAMPTZ            │     │ INDEX idx_event_board (leaderboard)  │
├──────────────────────────────────────┤     │ INDEX idx_event_user (user_id)       │
│ INDEX idx_board_slug (slug)          │     │ INDEX idx_event_created (created_at) │
│ INDEX idx_board_active (is_active)   │     │ INDEX idx_event_flagged (flagged)    │
└──────────────────────────────────────┘     └──────────────────────────────────────┘

┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│          user_scores                 │     │       leaderboard_snapshots          │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY │     │ id            BIGSERIAL  PRIMARY KEY │
│ leaderboard_id UUID     FK → boards  │     │ leaderboard_id UUID     FK → boards  │
│ user_id       UUID       NOT NULL    │     │ window_key    VARCHAR    NOT NULL    │
│ display_name  VARCHAR    NOT NULL    │     │ snapshot_data JSONB      NOT NULL    │
│ avatar_url    VARCHAR               │     │ top_n         INTEGER    NOT NULL    │
│ score         BIGINT     DEFAULT 0   │     │ total_users   INTEGER    NOT NULL    │
│ highest_score BIGINT     DEFAULT 0   │     │ captured_at   TIMESTAMPTZ            │
│ update_count  INTEGER    DEFAULT 0   │     ├──────────────────────────────────────┤
│ last_score_at TIMESTAMPTZ           │     │ INDEX idx_snap_board (leaderboard)   │
│ created_at    TIMESTAMPTZ            │     │ INDEX idx_snap_window (window_key)   │
│ updated_at    TIMESTAMPTZ            │     │ INDEX idx_snap_captured (captured_at)│
├──────────────────────────────────────┤     └──────────────────────────────────────┘
│ UNIQUE (leaderboard_id, user_id)     │
│ INDEX idx_uscore_board (leaderboard) │
│ INDEX idx_uscore_user (user_id)      │
│ INDEX idx_uscore_score (score DESC)  │
└──────────────────────────────────────┘
```

The `leaderboards` table stores configuration for each leaderboard: `score_type` determines how updates are applied (SUM adds to the existing score, MAX keeps the highest, REPLACE overwrites), and `sort_order` controls ranking direction (DESC for highest-score-wins, ASC for lowest-time-wins). `reset_cron` is an optional cron expression for automatic periodic resets (e.g., daily tournaments). `user_scores` is the durable source of truth for each user's score per leaderboard, with `highest_score` tracking the all-time peak regardless of resets. `score_events` logs every individual score update for audit, anti-cheat analysis, and replay; `flagged` marks suspicious events. `leaderboard_snapshots` stores periodic JSON snapshots of the top N users, used for analytics dashboards and disaster recovery (rebuilding Redis from PostgreSQL).

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/leaderboard"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    counter_shards: int = 16
    counter_flush_interval_ms: int = 500
    max_score_per_update: int = 10000
    anti_cheat_max_delta: int = 5000
    anti_cheat_window_seconds: int = 60
    snapshot_interval_minutes: int = 60
    snapshot_top_n: int = 10000
    websocket_broadcast_interval_ms: int = 200
    default_leaderboard_size: int = 100
    max_leaderboard_size: int = 1000

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
    Index, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database import Base


class ScoreType(str, enum.Enum):
    SUM = "sum"
    MAX = "max"
    REPLACE = "replace"


class SortOrder(str, enum.Enum):
    DESC = "desc"
    ASC = "asc"


class Leaderboard(Base):
    __tablename__ = "leaderboards"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    slug = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    score_type = Column(Enum(ScoreType), nullable=False, default=ScoreType.SUM)
    sort_order = Column(Enum(SortOrder), nullable=False, default=SortOrder.DESC)
    max_entries = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    reset_cron = Column(String(100), nullable=True)
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

    user_scores = relationship("UserScore", back_populates="leaderboard")
    score_events = relationship("ScoreEvent", back_populates="leaderboard")
    snapshots = relationship("LeaderboardSnapshot", back_populates="leaderboard")

    __table_args__ = (
        Index("idx_board_slug", "slug"),
        Index("idx_board_active", "is_active"),
    )


class UserScore(Base):
    __tablename__ = "user_scores"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    leaderboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("leaderboards.id"),
        nullable=False,
    )
    user_id = Column(UUID(as_uuid=True), nullable=False)
    display_name = Column(String(100), nullable=False)
    avatar_url = Column(String(2048), nullable=True)
    score = Column(BigInteger, default=0)
    highest_score = Column(BigInteger, default=0)
    update_count = Column(Integer, default=0)
    last_score_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    leaderboard = relationship("Leaderboard", back_populates="user_scores")

    __table_args__ = (
        UniqueConstraint("leaderboard_id", "user_id", name="uq_board_user"),
        Index("idx_uscore_board", "leaderboard_id"),
        Index("idx_uscore_user", "user_id"),
        Index("idx_uscore_score", "score"),
    )


class ScoreEvent(Base):
    __tablename__ = "score_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    leaderboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("leaderboards.id"),
        nullable=False,
    )
    user_id = Column(UUID(as_uuid=True), nullable=False)
    score_delta = Column(BigInteger, nullable=False)
    score_after = Column(BigInteger, nullable=False)
    source = Column(String(100), nullable=False)
    metadata_ = Column("metadata", JSONB, nullable=True)
    ip_address = Column(String(45), nullable=True)
    flagged = Column(Boolean, default=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    leaderboard = relationship("Leaderboard", back_populates="score_events")

    __table_args__ = (
        Index("idx_event_board", "leaderboard_id"),
        Index("idx_event_user", "user_id"),
        Index("idx_event_created", "created_at"),
        Index("idx_event_flagged", "flagged"),
    )


class LeaderboardSnapshot(Base):
    __tablename__ = "leaderboard_snapshots"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    leaderboard_id = Column(
        UUID(as_uuid=True),
        ForeignKey("leaderboards.id"),
        nullable=False,
    )
    window_key = Column(String(50), nullable=False)
    snapshot_data = Column(JSONB, nullable=False)
    top_n = Column(Integer, nullable=False)
    total_users = Column(Integer, nullable=False)
    captured_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    leaderboard = relationship("Leaderboard", back_populates="snapshots")

    __table_args__ = (
        Index("idx_snap_board", "leaderboard_id"),
        Index("idx_snap_window", "window_key"),
        Index("idx_snap_captured", "captured_at"),
    )
```

`UUID(as_uuid=True)` stores UUIDs natively in PostgreSQL, which is more compact than strings and prevents enumeration attacks. `ScoreType` determines how score updates are applied: `SUM` adds the delta to the current score (points games), `MAX` keeps only the highest score (speedrun), `REPLACE` overwrites entirely (rating systems). `SortOrder.DESC` means the highest score wins (most games); `ASC` means the lowest score wins (golf, racing). `UserScore` uses a composite unique constraint on `(leaderboard_id, user_id)` to ensure one score row per user per leaderboard. `highest_score` tracks the all-time peak even if the leaderboard resets. `ScoreEvent` logs every individual score change for anti-cheat analysis and replay; `flagged` marks events that triggered anti-cheat rules. `LeaderboardSnapshot.snapshot_data` stores a JSON array of `{rank, user_id, display_name, score}` for the top N users at capture time; `window_key` identifies the time window (e.g., `"2025-W24"` for ISO week 24).

### `app/schemas.py`

```python
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class ScoreSubmit(BaseModel):
    leaderboard_slug: str = Field(min_length=1, max_length=255)
    user_id: uuid.UUID
    display_name: str = Field(min_length=1, max_length=100)
    avatar_url: str | None = None
    score: int = Field(ge=0)
    source: str = Field("api", min_length=1, max_length=100)
    metadata: dict | None = None


class ScoreBatch(BaseModel):
    leaderboard_slug: str = Field(min_length=1, max_length=255)
    scores: list[ScoreSubmit] = Field(min_length=1, max_length=500)


class ScoreResponse(BaseModel):
    user_id: uuid.UUID
    display_name: str
    score: int
    rank: int
    previous_rank: int | None = None
    delta: int = 0
    percentile: float | None = None


class LeaderboardEntry(BaseModel):
    rank: int
    user_id: uuid.UUID
    display_name: str
    avatar_url: str | None
    score: int


class LeaderboardPage(BaseModel):
    leaderboard_id: uuid.UUID
    leaderboard_name: str
    window: str
    total_users: int
    entries: list[LeaderboardEntry]
    has_more: bool


class LeaderboardCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    slug: str = Field(min_length=1, max_length=255)
    description: str | None = None
    score_type: str = Field("sum", pattern="^(sum|max|replace)$")
    sort_order: str = Field("desc", pattern="^(desc|asc)$")
    max_entries: int = Field(0, ge=0)
    reset_cron: str | None = None
    metadata: dict | None = None


class LeaderboardResponse(BaseModel):
    id: uuid.UUID
    name: str
    slug: str
    description: str | None
    score_type: str
    sort_order: str
    is_active: bool
    total_users: int = 0
    created_at: datetime

    class Config:
        from_attributes = True


class RankResponse(BaseModel):
    user_id: uuid.UUID
    display_name: str
    score: int
    rank: int
    total_users: int
    percentile: float
    surrounding: list[LeaderboardEntry]


class UserScoreHistory(BaseModel):
    user_id: uuid.UUID
    leaderboard_slug: str
    current_score: int
    highest_score: int
    update_count: int
    events: list[dict]


class CounterStats(BaseModel):
    leaderboard_slug: str
    total_shards: int
    pending_flushes: int
    flush_rate_per_sec: float
    last_flush_at: datetime | None
```

`ScoreSubmit` accepts a score value (not a delta) for `MAX` and `REPLACE` modes, or a points value for `SUM` mode. The ingestion pipeline interprets the value based on the leaderboard's `score_type`. `ScoreBatch` accepts up to 500 scores in a single API call for bulk ingestion. `LeaderboardPage` includes `has_more` for cursor-based pagination of large leaderboards. `RankResponse` includes `percentile` (e.g., 95.2 means top 4.8%) and `surrounding` entries for context. `CounterStats` exposes shard health for operational monitoring.

---

## Redis Sorted Sets & Real-Time Ranking

### `app/redis_client.py`

```python
import json
from datetime import datetime, timezone

import redis as redis_lib

from app.config import settings

redis_client = redis_lib.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)


# ─── Key Schema ──────────────────────────────────────────────────
# Sorted sets:   lb:{slug}:{window}          (e.g., lb:weekly-challenge:all-time)
# User meta:     lb:{slug}:meta:{user_id}    (display_name, avatar_url)
# Counter shards: counter:{slug}:{shard_id}  (hash of user_id → pending delta)
# Pub/Sub:       events:{slug}               (score change events)

def leaderboard_key(slug: str, window: str = "all-time") -> str:
    return f"lb:{slug}:{window}"


def user_meta_key(slug: str, user_id: str) -> str:
    return f"lb:{slug}:meta:{user_id}"


def counter_shard_key(slug: str, shard_id: int) -> str:
    return f"counter:{slug}:{shard_id}"


def event_channel(slug: str) -> str:
    return f"events:{slug}"


# ─── Sorted Set Operations ───────────────────────────────────────

def set_score(slug: str, user_id: str, score: int, window: str = "all-time"):
    """
    Set a user's score in the leaderboard sorted set.

    For DESC leaderboards, scores are stored as-is (ZREVRANK).
    For ASC leaderboards, scores are stored as-is (ZRANK).
    """
    key = leaderboard_key(slug, window)
    redis_client.zadd(key, {user_id: score})


def increment_score(slug: str, user_id: str, delta: int, window: str = "all-time") -> float:
    """
    Atomically increment a user's score.
    Returns the new score after increment.
    """
    key = leaderboard_key(slug, window)
    return redis_client.zincrby(key, delta, user_id)


def get_score(slug: str, user_id: str, window: str = "all-time") -> int | None:
    """Get a user's current score. Returns None if not on the board."""
    key = leaderboard_key(slug, window)
    score = redis_client.zscore(key, user_id)
    return int(score) if score is not None else None


def get_rank_desc(slug: str, user_id: str, window: str = "all-time") -> int | None:
    """
    Get a user's rank in a descending leaderboard (highest score = rank 1).
    Uses ZREVRANK which returns 0-indexed rank; we add 1 for 1-indexed.
    Returns None if the user is not on the board.
    """
    key = leaderboard_key(slug, window)
    rank = redis_client.zrevrank(key, user_id)
    return rank + 1 if rank is not None else None


def get_rank_asc(slug: str, user_id: str, window: str = "all-time") -> int | None:
    """
    Get a user's rank in an ascending leaderboard (lowest score = rank 1).
    Uses ZRANK which returns 0-indexed rank; we add 1.
    """
    key = leaderboard_key(slug, window)
    rank = redis_client.zrank(key, user_id)
    return rank + 1 if rank is not None else None


def get_top_k_desc(slug: str, k: int, offset: int = 0, window: str = "all-time") -> list[tuple]:
    """
    Get top K entries from a descending leaderboard.

    Returns [(user_id, score), ...] ordered by score descending.
    ZREVRANGE returns members from highest to lowest score.
    """
    key = leaderboard_key(slug, window)
    return redis_client.zrevrange(key, offset, offset + k - 1, withscores=True)


def get_top_k_asc(slug: str, k: int, offset: int = 0, window: str = "all-time") -> list[tuple]:
    """
    Get top K entries from an ascending leaderboard.
    ZRANGE returns members from lowest to highest score.
    """
    key = leaderboard_key(slug, window)
    return redis_client.zrange(key, offset, offset + k - 1, withscores=True)


def get_surrounding(
    slug: str,
    user_id: str,
    count: int = 5,
    window: str = "all-time",
    sort_order: str = "desc",
) -> list[tuple]:
    """
    Get entries surrounding a user (±count neighbors).

    First finds the user's rank, then fetches a range around it.
    """
    key = leaderboard_key(slug, window)

    if sort_order == "desc":
        rank = redis_client.zrevrank(key, user_id)
        if rank is None:
            return []
        start = max(0, rank - count)
        end = rank + count
        return redis_client.zrevrange(key, start, end, withscores=True)
    else:
        rank = redis_client.zrank(key, user_id)
        if rank is None:
            return []
        start = max(0, rank - count)
        end = rank + count
        return redis_client.zrange(key, start, end, withscores=True)


def get_total_users(slug: str, window: str = "all-time") -> int:
    """Get the total number of users on a leaderboard."""
    key = leaderboard_key(slug, window)
    return redis_client.zcard(key)


def remove_user(slug: str, user_id: str, window: str = "all-time"):
    """Remove a user from a leaderboard."""
    key = leaderboard_key(slug, window)
    redis_client.zrem(key, user_id)


def delete_leaderboard(slug: str, window: str = "all-time"):
    """Delete an entire leaderboard sorted set."""
    key = leaderboard_key(slug, window)
    redis_client.delete(key)


# ─── User Metadata ───────────────────────────────────────────────

def set_user_meta(slug: str, user_id: str, display_name: str, avatar_url: str | None):
    """Store user display metadata alongside the leaderboard."""
    key = user_meta_key(slug, user_id)
    data = {"display_name": display_name}
    if avatar_url:
        data["avatar_url"] = avatar_url
    redis_client.hset(key, mapping=data)
    redis_client.expire(key, 86400 * 30)     # 30-day TTL


def get_user_meta(slug: str, user_id: str) -> dict | None:
    """Retrieve user display metadata."""
    key = user_meta_key(slug, user_id)
    data = redis_client.hgetall(key)
    return data if data else None


# ─── Pub/Sub ──────────────────────────────────────────────────────

def publish_score_event(slug: str, event: dict):
    """Publish a score change event for WebSocket fan-out."""
    redis_client.publish(event_channel(slug), json.dumps(event))
```

Redis sorted sets are the backbone of the leaderboard. Each sorted set maps `user_id → score`, and Redis maintains the set in sorted order internally using a skip list. This gives O(log N) for `ZADD`, `ZSCORE`, `ZRANK`, and `ZREVRANK`, and O(log N + K) for `ZRANGE`/`ZREVRANGE` where K is the number of returned elements. For a 100 M user leaderboard, `ZREVRANK` takes ~27 operations (log₂ 100M), completing in microseconds. `ZINCRBY` atomically increments a score without read-modify-write races. User metadata (display names, avatars) is stored in separate Redis hashes keyed by user ID, with a 30-day TTL to prevent stale data from consuming memory. The Pub/Sub channel broadcasts score events for WebSocket clients to receive real-time rank changes. The key schema uses `lb:{slug}:{window}` so each time window (daily, weekly, all-time) has its own independent sorted set.

---

## Distributed Counters (Sharded Writes)

### `app/counters.py`

```python
import hashlib
import json
import time
from datetime import datetime, timezone

from app.redis_client import (
    redis_client, counter_shard_key, increment_score,
    set_score, get_score, publish_score_event,
)
from app.config import settings


# ─── Shard Assignment ─────────────────────────────────────────────

def _shard_for_user(user_id: str, total_shards: int) -> int:
    """
    Determine which shard a user's writes go to.

    Uses MD5 hash for uniform distribution. The same user
    always maps to the same shard, ensuring ordering of
    that user's increments.
    """
    hash_digest = hashlib.md5(user_id.encode()).hexdigest()
    return int(hash_digest, 16) % total_shards


# ─── Write to Shard ──────────────────────────────────────────────

def buffer_score_increment(
    slug: str,
    user_id: str,
    delta: int,
    total_shards: int | None = None,
):
    """
    Buffer a score increment in a counter shard.

    Instead of writing directly to the sorted set (which
    can cause contention at 500 K writes/sec), we buffer
    increments in per-shard Redis hashes. A background
    flusher periodically drains each shard into the
    canonical sorted set.

    Shard structure (Redis hash):
        counter:{slug}:{shard_id} → {user_id: accumulated_delta}
    """
    if total_shards is None:
        total_shards = settings.counter_shards

    shard_id = _shard_for_user(user_id, total_shards)
    key = counter_shard_key(slug, shard_id)

    # HINCRBY is atomic — no read-modify-write race
    redis_client.hincrby(key, user_id, delta)


def buffer_score_set(
    slug: str,
    user_id: str,
    score: int,
    total_shards: int | None = None,
):
    """
    Buffer an absolute score set in a counter shard.

    For MAX and REPLACE modes, store the target score
    with a special prefix so the flusher knows to SET
    instead of INCREMENT.
    """
    if total_shards is None:
        total_shards = settings.counter_shards

    shard_id = _shard_for_user(user_id, total_shards)
    key = counter_shard_key(slug, shard_id)

    # Use a marker prefix to distinguish SET from INCR
    redis_client.hset(key, f"__SET__{user_id}", str(score))


# ─── Flush Shards ─────────────────────────────────────────────────

def flush_shard(slug: str, shard_id: int, windows: list[str] | None = None):
    """
    Flush a single shard's accumulated deltas into the
    canonical sorted set(s).

    Uses HGETALL + DEL atomically via a Lua script to
    prevent lost updates between read and delete.

    Args:
        slug: Leaderboard slug
        shard_id: Shard to flush
        windows: Time windows to update (default: ["all-time"])
    """
    if windows is None:
        windows = ["all-time"]

    key = counter_shard_key(slug, shard_id)

    # Atomic read-and-clear using Lua
    lua_script = """
    local data = redis.call("HGETALL", KEYS[1])
    if #data > 0 then
        redis.call("DEL", KEYS[1])
    end
    return data
    """
    raw = redis_client.eval(lua_script, 1, key)

    if not raw:
        return 0

    # Parse the flat list into key-value pairs
    entries = {}
    for i in range(0, len(raw), 2):
        entries[raw[i]] = raw[i + 1]

    flushed = 0

    for entry_key, value in entries.items():
        if entry_key.startswith("__SET__"):
            # Absolute set operation
            user_id = entry_key[7:]     # Strip "__SET__" prefix
            score = int(value)
            for window in windows:
                set_score(slug, user_id, score, window)
        else:
            # Increment operation
            user_id = entry_key
            delta = int(value)
            for window in windows:
                new_score = increment_score(slug, user_id, delta, window)

            # Publish event for WebSocket
            publish_score_event(slug, {
                "user_id": user_id,
                "delta": delta,
                "score": int(new_score) if 'new_score' in dir() else delta,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        flushed += 1

    return flushed


def flush_all_shards(slug: str, windows: list[str] | None = None) -> int:
    """Flush all shards for a leaderboard. Returns total entries flushed."""
    total = 0
    for shard_id in range(settings.counter_shards):
        total += flush_shard(slug, shard_id, windows)
    return total


# ─── Shard Stats ──────────────────────────────────────────────────

def get_shard_depths(slug: str) -> dict:
    """Get the number of pending entries in each shard."""
    depths = {}
    for shard_id in range(settings.counter_shards):
        key = counter_shard_key(slug, shard_id)
        depths[shard_id] = redis_client.hlen(key)
    return depths


def get_total_pending(slug: str) -> int:
    """Get total pending entries across all shards."""
    total = 0
    for shard_id in range(settings.counter_shards):
        key = counter_shard_key(slug, shard_id)
        total += redis_client.hlen(key)
    return total
```

Distributed counters solve the **hot-key write contention** problem. At 500 K writes/sec, all score updates targeting a single Redis sorted set would create a bottleneck. Instead, writes are distributed across `COUNTER_SHARDS` Redis hashes: `counter:{slug}:0` through `counter:{slug}:15`. Each user consistently maps to one shard via MD5 hashing, preserving ordering of that user's updates. `HINCRBY` is atomic within Redis, so no locking is needed. A background flusher periodically drains each shard using a **Lua script** that atomically reads all entries and deletes the hash — this prevents the race condition where new writes arrive between `HGETALL` and `DEL`. The flusher applies the accumulated deltas to the canonical sorted set using `ZINCRBY`. The tradeoff is **eventual consistency**: scores in the sorted set lag behind by up to `COUNTER_FLUSH_INTERVAL_MS` (500 ms by default). For absolute score modes (MAX, REPLACE), a `__SET__` prefix marker distinguishes set operations from increments.

---

## Leaderboard Engine (Top-K, Rank, Percentile)

### `app/ranking.py`

```python
from app.redis_client import (
    get_top_k_desc, get_top_k_asc,
    get_rank_desc, get_rank_asc,
    get_score, get_total_users,
    get_surrounding, get_user_meta,
)
from app.config import settings


# ─── Top-K Queries ────────────────────────────────────────────────

def get_leaderboard_page(
    slug: str,
    sort_order: str = "desc",
    page: int = 1,
    page_size: int | None = None,
    window: str = "all-time",
) -> dict:
    """
    Fetch a page of the leaderboard.

    Args:
        slug: Leaderboard slug
        sort_order: "desc" (highest first) or "asc" (lowest first)
        page: Page number (1-indexed)
        page_size: Entries per page (default from settings)
        window: Time window key

    Returns:
        {
            "entries": [{"rank": 1, "user_id": "...", "display_name": "...", "score": 100}, ...],
            "total_users": 1000000,
            "has_more": True,
        }
    """
    if page_size is None:
        page_size = settings.default_leaderboard_size

    page_size = min(page_size, settings.max_leaderboard_size)
    offset = (page - 1) * page_size

    if sort_order == "desc":
        raw = get_top_k_desc(slug, page_size, offset, window)
    else:
        raw = get_top_k_asc(slug, page_size, offset, window)

    total = get_total_users(slug, window)

    entries = []
    for i, (user_id, score) in enumerate(raw):
        meta = get_user_meta(slug, user_id) or {}
        entries.append({
            "rank": offset + i + 1,
            "user_id": user_id,
            "display_name": meta.get("display_name", f"User-{user_id[:8]}"),
            "avatar_url": meta.get("avatar_url"),
            "score": int(score),
        })

    has_more = (offset + page_size) < total

    return {
        "entries": entries,
        "total_users": total,
        "has_more": has_more,
    }


# ─── Rank Query ───────────────────────────────────────────────────

def get_user_rank(
    slug: str,
    user_id: str,
    sort_order: str = "desc",
    window: str = "all-time",
    surrounding_count: int = 5,
) -> dict | None:
    """
    Get a user's rank, score, percentile, and surrounding entries.

    Returns None if the user is not on the leaderboard.
    """
    score = get_score(slug, user_id, window)
    if score is None:
        return None

    if sort_order == "desc":
        rank = get_rank_desc(slug, user_id, window)
    else:
        rank = get_rank_asc(slug, user_id, window)

    if rank is None:
        return None

    total = get_total_users(slug, window)
    percentile = calculate_percentile(rank, total)

    # Get surrounding entries
    raw_surrounding = get_surrounding(
        slug, user_id, surrounding_count, window, sort_order,
    )

    meta = get_user_meta(slug, user_id) or {}
    surrounding = []
    for i, (uid, s) in enumerate(raw_surrounding):
        uid_meta = get_user_meta(slug, uid) or {}

        if sort_order == "desc":
            entry_rank = get_rank_desc(slug, uid, window)
        else:
            entry_rank = get_rank_asc(slug, uid, window)

        surrounding.append({
            "rank": entry_rank,
            "user_id": uid,
            "display_name": uid_meta.get("display_name", f"User-{uid[:8]}"),
            "avatar_url": uid_meta.get("avatar_url"),
            "score": int(s),
        })

    return {
        "user_id": user_id,
        "display_name": meta.get("display_name", f"User-{user_id[:8]}"),
        "score": int(score),
        "rank": rank,
        "total_users": total,
        "percentile": percentile,
        "surrounding": surrounding,
    }


# ─── Percentile Calculation ──────────────────────────────────────

def calculate_percentile(rank: int, total: int) -> float:
    """
    Calculate the percentile for a given rank.

    Percentile = ((total - rank) / total) × 100

    Rank 1 out of 1000 → 99.9th percentile
    Rank 500 out of 1000 → 50.0th percentile
    Rank 1000 out of 1000 → 0.0th percentile
    """
    if total == 0:
        return 0.0
    return round(((total - rank) / total) * 100, 2)


# ─── Score at Percentile ─────────────────────────────────────────

def get_score_at_percentile(
    slug: str,
    percentile: float,
    sort_order: str = "desc",
    window: str = "all-time",
) -> int | None:
    """
    Get the score at a specific percentile.

    Used for tier boundaries (e.g., top 10% = diamond tier).
    """
    total = get_total_users(slug, window)
    if total == 0:
        return None

    # Convert percentile to rank
    rank = max(1, int(total * (1 - percentile / 100)))

    # Fetch the entry at that rank
    offset = rank - 1
    if sort_order == "desc":
        entries = get_top_k_desc(slug, 1, offset, window)
    else:
        entries = get_top_k_asc(slug, 1, offset, window)

    if entries:
        return int(entries[0][1])
    return None


# ─── Tier Calculation ─────────────────────────────────────────────

TIERS = [
    {"name": "Grandmaster", "min_percentile": 99.0, "color": "#FF4500"},
    {"name": "Master", "min_percentile": 95.0, "color": "#FFD700"},
    {"name": "Diamond", "min_percentile": 90.0, "color": "#00BFFF"},
    {"name": "Platinum", "min_percentile": 75.0, "color": "#E5E4E2"},
    {"name": "Gold", "min_percentile": 50.0, "color": "#DAA520"},
    {"name": "Silver", "min_percentile": 25.0, "color": "#C0C0C0"},
    {"name": "Bronze", "min_percentile": 0.0, "color": "#CD7F32"},
]


def get_user_tier(percentile: float) -> dict:
    """
    Determine a user's tier based on their percentile.
    Returns the tier name and color.
    """
    for tier in TIERS:
        if percentile >= tier["min_percentile"]:
            return {"name": tier["name"], "color": tier["color"]}
    return {"name": "Bronze", "color": "#CD7F32"}
```

The ranking engine wraps Redis sorted set operations with business logic. **Top-K queries** use `ZREVRANGE` (descending) or `ZRANGE` (ascending) with offset-based pagination. For a 100 M user leaderboard, fetching the top 100 takes ~27 skip-list traversals plus 100 sequential reads — completing in under 1 ms. **Rank queries** use `ZREVRANK`/`ZRANK`, which are O(log N). The `get_surrounding` function provides context: instead of just showing rank #4523, it shows ranks #4518–#4528, making the leaderboard feel alive. **Percentile** is calculated as `(total − rank) / total × 100`: rank 1 out of 1 M is the 99.9999th percentile. The **tier system** maps percentiles to named tiers (Grandmaster, Diamond, Gold, etc.) with colors, commonly used in competitive games. `get_score_at_percentile` is the inverse — given a percentile, find the score threshold, useful for defining tier boundaries.

---

## Time-Windowed Leaderboards (Daily, Weekly, All-Time)

### `app/windows.py`

```python
from datetime import datetime, timezone, timedelta

from app.redis_client import (
    redis_client, leaderboard_key,
    get_top_k_desc, get_total_users,
    delete_leaderboard, get_user_meta,
)
from app.config import settings


# ─── Window Key Generation ────────────────────────────────────────

def current_window_key(window_type: str) -> str:
    """
    Generate the current window key for a given window type.

    Examples:
        "daily"    → "2025-06-15"
        "weekly"   → "2025-W24"
        "monthly"  → "2025-06"
        "all-time" → "all-time"
    """
    now = datetime.now(timezone.utc)

    if window_type == "daily":
        return now.strftime("%Y-%m-%d")
    elif window_type == "weekly":
        iso_year, iso_week, _ = now.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    elif window_type == "monthly":
        return now.strftime("%Y-%m")
    else:
        return "all-time"


def previous_window_key(window_type: str) -> str:
    """Generate the previous window key for archival/comparison."""
    now = datetime.now(timezone.utc)

    if window_type == "daily":
        prev = now - timedelta(days=1)
        return prev.strftime("%Y-%m-%d")
    elif window_type == "weekly":
        prev = now - timedelta(weeks=1)
        iso_year, iso_week, _ = prev.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    elif window_type == "monthly":
        if now.month == 1:
            return f"{now.year - 1}-12"
        return f"{now.year}-{now.month - 1:02d}"
    else:
        return "all-time"


# ─── Active Windows ──────────────────────────────────────────────

def get_active_windows() -> list[str]:
    """Return all currently active window keys."""
    return [
        "all-time",
        current_window_key("daily"),
        current_window_key("weekly"),
        current_window_key("monthly"),
    ]


# ─── Window Rotation ─────────────────────────────────────────────

def rotate_window(slug: str, window_type: str) -> dict:
    """
    Rotate a time window: archive the current window and start fresh.

    Steps:
    1. Snapshot the current window's top N to PostgreSQL
    2. Delete the expired sorted set from Redis
    3. The new window's sorted set starts empty and accumulates
       scores from new submissions

    Returns rotation summary.
    """
    current = current_window_key(window_type)
    previous = previous_window_key(window_type)

    # Get summary before deletion
    total = get_total_users(slug, previous)
    top_entries = []

    if total > 0:
        raw = get_top_k_desc(slug, min(total, settings.snapshot_top_n), window=previous)
        for i, (user_id, score) in enumerate(raw):
            meta = get_user_meta(slug, user_id) or {}
            top_entries.append({
                "rank": i + 1,
                "user_id": user_id,
                "display_name": meta.get("display_name", f"User-{user_id[:8]}"),
                "score": int(score),
            })

    # Delete the previous window's sorted set
    delete_leaderboard(slug, previous)

    return {
        "slug": slug,
        "window_type": window_type,
        "rotated_window": previous,
        "new_window": current,
        "archived_users": total,
        "top_entries": top_entries,
    }


# ─── Cross-Window Queries ────────────────────────────────────────

def get_user_across_windows(slug: str, user_id: str, sort_order: str = "desc") -> dict:
    """
    Get a user's rank and score across all active time windows.
    Useful for profile pages showing daily/weekly/all-time standings.
    """
    from app.redis_client import get_score, get_rank_desc, get_rank_asc

    results = {}
    for window_type in ["all-time", "daily", "weekly", "monthly"]:
        window_key = current_window_key(window_type) if window_type != "all-time" else "all-time"
        score = get_score(slug, user_id, window_key)

        if score is not None:
            if sort_order == "desc":
                rank = get_rank_desc(slug, user_id, window_key)
            else:
                rank = get_rank_asc(slug, user_id, window_key)

            total = get_total_users(slug, window_key)

            results[window_type] = {
                "score": int(score),
                "rank": rank,
                "total_users": total,
            }
        else:
            results[window_type] = None

    return results
```

Time-windowed leaderboards maintain separate Redis sorted sets per window. When a score is submitted, the ingestion pipeline writes to **all active windows** simultaneously: all-time, daily (`2025-06-15`), weekly (`2025-W24`), and monthly (`2025-06`). Each window has its own sorted set key (e.g., `lb:weekly-challenge:2025-W24`), so queries against the daily leaderboard are independent of the all-time leaderboard. The window rotator runs on a schedule: at midnight UTC, it archives yesterday's daily leaderboard (snapshots the top N to PostgreSQL) and deletes the Redis key. The new day's sorted set starts empty and accumulates scores naturally. `get_user_across_windows` powers profile pages that show a user's rank in all windows side by side. The previous window key is calculated for archival: when today is June 15, the previous daily window is `2025-06-14`.

---

## Score Ingestion Pipeline

### `app/ingestion.py`

```python
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models import (
    Leaderboard, UserScore, ScoreEvent, ScoreType, SortOrder,
)
from app.counters import buffer_score_increment, buffer_score_set
from app.redis_client import (
    set_user_meta, get_score, publish_score_event,
)
from app.windows import get_active_windows
from app.config import settings


# ─── Score Submission ─────────────────────────────────────────────

def submit_score(
    slug: str,
    user_id: str,
    display_name: str,
    avatar_url: str | None,
    score: int,
    source: str,
    metadata: dict | None,
    ip_address: str | None,
    db: Session,
) -> dict:
    """
    Process a score submission through the full ingestion pipeline.

    Pipeline:
    1. Validate the leaderboard exists and is active
    2. Anti-cheat validation
    3. Apply score based on score_type (SUM, MAX, REPLACE)
    4. Buffer the write in a counter shard
    5. Update PostgreSQL (user_scores + score_events)
    6. Store user metadata in Redis
    7. Return the new score and rank estimate
    """
    # 1. Look up leaderboard
    board = (
        db.query(Leaderboard)
        .filter(Leaderboard.slug == slug, Leaderboard.is_active.is_(True))
        .first()
    )
    if not board:
        raise ValueError(f"Leaderboard '{slug}' not found or inactive.")

    # 2. Validate score
    if score > settings.max_score_per_update:
        raise ValueError(
            f"Score {score} exceeds max allowed ({settings.max_score_per_update})."
        )

    if score < 0:
        raise ValueError("Score must be non-negative.")

    # 3. Anti-cheat check
    flagged = _check_anti_cheat(slug, user_id, score, board.score_type, db)

    # 4. Compute new score based on score_type
    current_score = get_score(slug, str(user_id)) or 0

    if board.score_type == ScoreType.SUM:
        new_score = current_score + score
        delta = score
    elif board.score_type == ScoreType.MAX:
        new_score = max(current_score, score)
        delta = new_score - current_score
    elif board.score_type == ScoreType.REPLACE:
        new_score = score
        delta = score - current_score
    else:
        new_score = current_score + score
        delta = score

    # 5. Buffer the write to counter shards (all active windows)
    windows = get_active_windows()

    if board.score_type == ScoreType.SUM:
        for window in windows:
            buffer_score_increment(
                f"{slug}:{window}", str(user_id), delta,
            )
    else:
        for window in windows:
            buffer_score_set(
                f"{slug}:{window}", str(user_id), new_score,
            )

    # 6. Update PostgreSQL
    user_score = (
        db.query(UserScore)
        .filter(
            UserScore.leaderboard_id == board.id,
            UserScore.user_id == user_id,
        )
        .first()
    )

    if user_score:
        user_score.score = new_score
        user_score.highest_score = max(user_score.highest_score, new_score)
        user_score.update_count += 1
        user_score.last_score_at = datetime.now(timezone.utc)
        if display_name:
            user_score.display_name = display_name
        if avatar_url:
            user_score.avatar_url = avatar_url
    else:
        user_score = UserScore(
            leaderboard_id=board.id,
            user_id=user_id,
            display_name=display_name,
            avatar_url=avatar_url,
            score=new_score,
            highest_score=new_score,
            update_count=1,
            last_score_at=datetime.now(timezone.utc),
        )
        db.add(user_score)

    # Log the event
    event = ScoreEvent(
        leaderboard_id=board.id,
        user_id=user_id,
        score_delta=delta,
        score_after=new_score,
        source=source,
        metadata_=metadata,
        ip_address=ip_address,
        flagged=flagged,
    )
    db.add(event)
    db.commit()

    # 7. Store user meta in Redis
    set_user_meta(slug, str(user_id), display_name, avatar_url)

    return {
        "user_id": str(user_id),
        "display_name": display_name,
        "score": new_score,
        "delta": delta,
        "flagged": flagged,
    }


# ─── Anti-Cheat Validation ───────────────────────────────────────

def _check_anti_cheat(
    slug: str,
    user_id: str,
    score: int,
    score_type: ScoreType,
    db: Session,
) -> bool:
    """
    Validate a score submission against anti-cheat rules.

    Returns True if the submission is flagged (suspicious).

    Rules:
    1. Score delta exceeds ANTI_CHEAT_MAX_DELTA in a single update
    2. More than 10 updates within ANTI_CHEAT_WINDOW_SECONDS
    3. Score jumps that are physically impossible (game-specific)
    """
    flagged = False

    # Rule 1: Excessive single score
    if score > settings.anti_cheat_max_delta:
        flagged = True

    # Rule 2: Rapid-fire submissions
    from datetime import timedelta

    window_start = datetime.now(timezone.utc) - timedelta(
        seconds=settings.anti_cheat_window_seconds,
    )

    recent_count = (
        db.query(ScoreEvent)
        .filter(
            ScoreEvent.user_id == user_id,
            ScoreEvent.created_at >= window_start,
        )
        .count()
    )

    if recent_count > 10:
        flagged = True

    return flagged


# ─── Batch Ingestion ──────────────────────────────────────────────

def submit_batch(
    slug: str,
    scores: list[dict],
    db: Session,
) -> dict:
    """
    Process a batch of score submissions.
    Returns summary with success/failure counts.
    """
    success = 0
    failed = 0
    errors = []

    for entry in scores:
        try:
            submit_score(
                slug=slug,
                user_id=entry["user_id"],
                display_name=entry["display_name"],
                avatar_url=entry.get("avatar_url"),
                score=entry["score"],
                source=entry.get("source", "batch"),
                metadata=entry.get("metadata"),
                ip_address=None,
                db=db,
            )
            success += 1
        except Exception as e:
            failed += 1
            errors.append({
                "user_id": str(entry.get("user_id")),
                "error": str(e),
            })

    return {
        "total": len(scores),
        "success": success,
        "failed": failed,
        "errors": errors[:10],
    }
```

The ingestion pipeline is the write path for all score updates. It follows a strict sequence: validate → anti-cheat → compute → buffer → persist → respond. The `score_type` determines how the new score is calculated: `SUM` adds the submitted value to the current score (ideal for points-based games), `MAX` keeps only the highest (ideal for high-score games), and `REPLACE` overwrites entirely (ideal for rating systems like ELO). Writes go through the **counter shard** layer rather than directly to the sorted set, preventing hot-key contention. The pipeline writes to **all active time windows** simultaneously (daily, weekly, monthly, all-time), so a single score submission updates 4 sorted sets. Anti-cheat validation runs synchronously and flags suspicious submissions: single updates exceeding `ANTI_CHEAT_MAX_DELTA` or more than 10 updates within 60 seconds. Flagged submissions are still accepted (to avoid false-positive blocking) but are marked in the `score_events` table for manual review. PostgreSQL stores the durable record; Redis is the performance layer.

---

## Core API – Scores & Leaderboards

### `app/main.py`

```python
from fastapi import FastAPI

from app.routers import scores, leaderboards, admin, live

app = FastAPI(
    title="Leaderboard & Distributed Counters",
    description="Real-time ranking at scale with priority tiers and time windows",
    version="1.0.0",
)

app.include_router(scores.router, prefix="/scores", tags=["Scores"])
app.include_router(leaderboards.router, prefix="/leaderboards", tags=["Leaderboards"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(live.router, tags=["WebSocket"])


@app.get("/health")
def health_check():
    """Health check endpoint for load balancers and monitoring."""
    return {"status": "healthy"}
```

### `app/routers/scores.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import ScoreSubmit, ScoreBatch, ScoreResponse
from app.ingestion import submit_score, submit_batch

router = APIRouter()


@router.post("", response_model=ScoreResponse, status_code=201)
def submit(
    req: ScoreSubmit,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Submit a score update.

    The score is processed according to the leaderboard's score_type:
    - SUM: score is added to the current total
    - MAX: score replaces the current if higher
    - REPLACE: score overwrites the current value
    """
    try:
        result = submit_score(
            slug=req.leaderboard_slug,
            user_id=str(req.user_id),
            display_name=req.display_name,
            avatar_url=req.avatar_url,
            score=req.score,
            source=req.source,
            metadata=req.metadata,
            ip_address=request.client.host if request.client else None,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return ScoreResponse(
        user_id=req.user_id,
        display_name=result["display_name"],
        score=result["score"],
        rank=0,              # Rank is eventually consistent; use /rank for live rank
        delta=result["delta"],
    )


@router.post("/batch")
def submit_scores_batch(
    req: ScoreBatch,
    db: Session = Depends(get_db),
):
    """
    Submit a batch of score updates (up to 500).
    Returns a summary with success/failure counts.
    """
    scores = [s.model_dump() for s in req.scores]

    try:
        result = submit_batch(
            slug=req.leaderboard_slug,
            scores=scores,
            db=db,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return result
```

### `app/routers/leaderboards.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Leaderboard, ScoreType, SortOrder
from app.schemas import (
    LeaderboardCreate, LeaderboardResponse, LeaderboardPage,
    RankResponse, LeaderboardEntry,
)
from app.ranking import (
    get_leaderboard_page, get_user_rank,
    get_user_tier, get_score_at_percentile,
)
from app.windows import current_window_key, get_user_across_windows
from app.redis_client import get_total_users

router = APIRouter()


@router.post("", response_model=LeaderboardResponse, status_code=201)
def create_leaderboard(
    req: LeaderboardCreate,
    db: Session = Depends(get_db),
):
    """Create a new leaderboard."""
    existing = (
        db.query(Leaderboard)
        .filter(Leaderboard.slug == req.slug)
        .first()
    )
    if existing:
        raise HTTPException(
            status_code=409, detail=f"Leaderboard slug '{req.slug}' already exists.",
        )

    board = Leaderboard(
        name=req.name,
        slug=req.slug,
        description=req.description,
        score_type=ScoreType(req.score_type),
        sort_order=SortOrder(req.sort_order),
        max_entries=req.max_entries,
        reset_cron=req.reset_cron,
        metadata_=req.metadata,
    )
    db.add(board)
    db.commit()
    db.refresh(board)

    return board


@router.get("/{slug}/top", response_model=LeaderboardPage)
def get_top(
    slug: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    window: str = Query("all-time"),
    db: Session = Depends(get_db),
):
    """
    Get the top entries of a leaderboard.

    Supports pagination and time windows:
        /leaderboards/weekly-challenge/top?window=daily&page=1&page_size=50
    """
    board = (
        db.query(Leaderboard)
        .filter(Leaderboard.slug == slug)
        .first()
    )
    if not board:
        raise HTTPException(status_code=404, detail="Leaderboard not found")

    # Resolve window key
    if window in ("daily", "weekly", "monthly"):
        window_key = current_window_key(window)
    else:
        window_key = window

    result = get_leaderboard_page(
        slug=slug,
        sort_order=board.sort_order.value,
        page=page,
        page_size=page_size,
        window=window_key,
    )

    entries = [LeaderboardEntry(**e) for e in result["entries"]]

    return LeaderboardPage(
        leaderboard_id=board.id,
        leaderboard_name=board.name,
        window=window_key,
        total_users=result["total_users"],
        entries=entries,
        has_more=result["has_more"],
    )


@router.get("/{slug}/rank/{user_id}", response_model=RankResponse)
def get_rank(
    slug: str,
    user_id: uuid.UUID,
    window: str = Query("all-time"),
    surrounding: int = Query(5, ge=0, le=25),
    db: Session = Depends(get_db),
):
    """
    Get a user's rank, score, percentile, tier, and surrounding entries.
    """
    board = (
        db.query(Leaderboard)
        .filter(Leaderboard.slug == slug)
        .first()
    )
    if not board:
        raise HTTPException(status_code=404, detail="Leaderboard not found")

    if window in ("daily", "weekly", "monthly"):
        window_key = current_window_key(window)
    else:
        window_key = window

    result = get_user_rank(
        slug=slug,
        user_id=str(user_id),
        sort_order=board.sort_order.value,
        window=window_key,
        surrounding_count=surrounding,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"User '{user_id}' not found on leaderboard '{slug}'",
        )

    surrounding_entries = [
        LeaderboardEntry(**e) for e in result["surrounding"]
    ]

    return RankResponse(
        user_id=user_id,
        display_name=result["display_name"],
        score=result["score"],
        rank=result["rank"],
        total_users=result["total_users"],
        percentile=result["percentile"],
        surrounding=surrounding_entries,
    )


@router.get("/{slug}/user/{user_id}/windows")
def get_user_windows(
    slug: str,
    user_id: uuid.UUID,
    db: Session = Depends(get_db),
):
    """
    Get a user's standings across all time windows.
    Useful for profile pages showing daily/weekly/all-time.
    """
    board = (
        db.query(Leaderboard)
        .filter(Leaderboard.slug == slug)
        .first()
    )
    if not board:
        raise HTTPException(status_code=404, detail="Leaderboard not found")

    return get_user_across_windows(slug, str(user_id), board.sort_order.value)


@router.get("/{slug}/tiers")
def get_tier_boundaries(
    slug: str,
    db: Session = Depends(get_db),
):
    """
    Get the score boundaries for each tier.
    Useful for showing "you need X more points for Diamond".
    """
    board = (
        db.query(Leaderboard)
        .filter(Leaderboard.slug == slug)
        .first()
    )
    if not board:
        raise HTTPException(status_code=404, detail="Leaderboard not found")

    from app.ranking import TIERS

    boundaries = []
    for tier in TIERS:
        score = get_score_at_percentile(
            slug, tier["min_percentile"], board.sort_order.value,
        )
        boundaries.append({
            "tier": tier["name"],
            "color": tier["color"],
            "min_percentile": tier["min_percentile"],
            "score_threshold": score,
        })

    return {"leaderboard": slug, "tiers": boundaries}


@router.get("", response_model=list[LeaderboardResponse])
def list_leaderboards(
    is_active: bool | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List all leaderboards with optional filters."""
    query = db.query(Leaderboard)

    if is_active is not None:
        query = query.filter(Leaderboard.is_active == is_active)

    query = query.order_by(Leaderboard.created_at.desc())
    return query.offset(offset).limit(limit).all()
```

The leaderboard API provides five key query patterns. `GET /leaderboards/{slug}/top` is the most common: fetch the top N entries with pagination and time window selection. Clients specify `?window=daily` or `?window=weekly` to see the current period's standings. `GET /leaderboards/{slug}/rank/{user_id}` returns a user's exact rank, percentile, and surrounding entries — this powers the "You are ranked #4523 — top 5.4%" UI. `GET /leaderboards/{slug}/user/{user_id}/windows` returns a user's standings across all time windows in a single call, powering profile pages. `GET /leaderboards/{slug}/tiers` returns the score thresholds for each competitive tier, enabling "you need 200 more points for Diamond" progress bars. All read operations go directly to Redis — no PostgreSQL queries on the read path.

### `app/routers/admin.py`

```python
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import CounterStats
from app.counters import get_shard_depths, get_total_pending, flush_all_shards
from app.redis_client import get_total_users
from app.config import settings

router = APIRouter()


@router.get("/counters/{slug}/stats", response_model=CounterStats)
def counter_stats(slug: str):
    """Get counter shard statistics for a leaderboard."""
    depths = get_shard_depths(slug)
    pending = get_total_pending(slug)

    return CounterStats(
        leaderboard_slug=slug,
        total_shards=settings.counter_shards,
        pending_flushes=pending,
        flush_rate_per_sec=1000 / settings.counter_flush_interval_ms,
        last_flush_at=None,
    )


@router.post("/counters/{slug}/flush")
def force_flush(slug: str):
    """Force-flush all counter shards for a leaderboard."""
    flushed = flush_all_shards(slug)
    return {"slug": slug, "flushed_entries": flushed}


@router.get("/stats/{slug}")
def leaderboard_stats(
    slug: str,
    window: str = Query("all-time"),
):
    """Get overall statistics for a leaderboard."""
    total = get_total_users(slug, window)
    pending = get_total_pending(slug)

    return {
        "slug": slug,
        "window": window,
        "total_users": total,
        "pending_counter_entries": pending,
        "counter_shards": settings.counter_shards,
    }


@router.get("/flagged")
def list_flagged_events(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """List score events flagged by anti-cheat."""
    from app.models import ScoreEvent

    events = (
        db.query(ScoreEvent)
        .filter(ScoreEvent.flagged.is_(True))
        .order_by(ScoreEvent.created_at.desc())
        .limit(limit)
        .all()
    )

    return [
        {
            "id": e.id,
            "user_id": str(e.user_id),
            "score_delta": e.score_delta,
            "score_after": e.score_after,
            "source": e.source,
            "ip_address": e.ip_address,
            "created_at": e.created_at.isoformat(),
        }
        for e in events
    ]
```

The admin router provides operational endpoints for monitoring the leaderboard infrastructure. Counter stats show the number of pending entries in each shard, helping operators identify write imbalances. The force-flush endpoint is useful during maintenance or before snapshotting, ensuring all buffered writes reach the sorted set. Flagged events expose anti-cheat detections for manual review.

---

## Real-Time Updates (WebSockets)

### `app/websocket_manager.py`

```python
import json
import asyncio
from collections import defaultdict

from fastapi import WebSocket


class WebSocketManager:
    """
    Manages WebSocket connections for real-time leaderboard updates.

    Clients subscribe to a leaderboard slug and receive score
    change events as they are flushed from counter shards.
    """

    def __init__(self):
        # board_slug → set of connected WebSockets
        self.connections: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, board_slug: str, websocket: WebSocket):
        """Accept a WebSocket connection for a leaderboard."""
        await websocket.accept()
        self.connections[board_slug].add(websocket)

    async def disconnect(self, board_slug: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.connections[board_slug].discard(websocket)
        if not self.connections[board_slug]:
            del self.connections[board_slug]

    async def broadcast(self, board_slug: str, event: dict):
        """
        Broadcast an event to all clients watching a leaderboard.

        Events include score changes, rank updates, and tier promotions.
        """
        message = json.dumps(event)
        dead = set()

        for ws in self.connections.get(board_slug, set()):
            try:
                await ws.send_text(message)
            except Exception:
                dead.add(ws)

        for ws in dead:
            self.connections[board_slug].discard(ws)


manager = WebSocketManager()
```

### `app/routers/live.py`

```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/live/{board_slug}")
async def leaderboard_live(board_slug: str, websocket: WebSocket):
    """
    WebSocket endpoint for real-time leaderboard updates.

    Connect: ws://host/ws/live/{board_slug}

    Messages are JSON events:
        {"type": "score_update", "user_id": "...", "score": 1500, "delta": 100}
        {"type": "rank_change", "user_id": "...", "old_rank": 5, "new_rank": 3}
        {"type": "new_leader", "user_id": "...", "score": 9999}
    """
    await manager.connect(board_slug, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(board_slug, websocket)
```

The WebSocket manager broadcasts score change events to all clients watching a leaderboard. Events are published by the counter flusher after scores are applied to the sorted set. Clients receive updates including score deltas, rank changes, and new leader announcements. This powers live leaderboard UIs where entries animate in real time as scores change. Dead connections are cleaned up automatically on send failure.

---

## Background Workers

### `workers/counter_flusher.py`

```python
"""
Counter flusher worker.

Runs in a loop, flushing sharded counters into the
canonical Redis sorted sets. This is the bridge between
high-throughput buffered writes and the readable leaderboard.
"""

import time
import signal

from app.config import settings
from app.counters import flush_all_shards
from app.windows import get_active_windows
from app.redis_client import redis_client


class CounterFlusher:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        print("[Flusher] Shutting down...")
        self.running = False

    def run(self):
        """Main loop: flush all shards at the configured interval."""
        interval = settings.counter_flush_interval_ms / 1000
        print(f"[Flusher] Starting with {interval}s flush interval")

        while self.running:
            try:
                # Get all active leaderboard slugs
                slugs = self._get_active_slugs()

                for slug in slugs:
                    windows = get_active_windows()
                    flushed = flush_all_shards(slug, windows)
                    if flushed > 0:
                        print(f"[Flusher] Flushed {flushed} entries for '{slug}'")

            except Exception as e:
                print(f"[Flusher] Error: {e}")

            time.sleep(interval)

        # Final flush on shutdown
        print("[Flusher] Final flush before exit...")
        for slug in self._get_active_slugs():
            flush_all_shards(slug)

        print("[Flusher] Stopped.")

    def _get_active_slugs(self) -> list[str]:
        """
        Discover active leaderboards by scanning Redis keys.
        In production, read from PostgreSQL or a config cache.
        """
        cursor = 0
        slugs = set()

        while True:
            cursor, keys = redis_client.scan(cursor, match="counter:*:0")
            for key in keys:
                # key format: counter:{slug}:0
                parts = key.split(":")
                if len(parts) >= 3:
                    slugs.add(parts[1])
            if cursor == 0:
                break

        return list(slugs)


def main():
    flusher = CounterFlusher()
    flusher.run()


if __name__ == "__main__":
    main()
```

### `workers/snapshot_worker.py`

```python
"""
Snapshot worker.

Periodically persists the current leaderboard state from Redis
to PostgreSQL for disaster recovery and analytics.
"""

import time
import signal
from datetime import datetime, timezone

from app.config import settings
from app.database import SessionLocal
from app.models import LeaderboardSnapshot, Leaderboard
from app.redis_client import get_top_k_desc, get_total_users, get_user_meta
from app.windows import current_window_key


class SnapshotWorker:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        print("[Snapshot] Shutting down...")
        self.running = False

    def run(self):
        """Main loop: snapshot leaderboards at the configured interval."""
        interval = settings.snapshot_interval_minutes * 60
        print(f"[Snapshot] Starting with {settings.snapshot_interval_minutes}m interval")

        while self.running:
            try:
                self._snapshot_all()
            except Exception as e:
                print(f"[Snapshot] Error: {e}")

            # Sleep in small increments to respect shutdown
            for _ in range(int(interval)):
                if not self.running:
                    break
                time.sleep(1)

        print("[Snapshot] Stopped.")

    def _snapshot_all(self):
        """Snapshot all active leaderboards."""
        db = SessionLocal()

        try:
            boards = (
                db.query(Leaderboard)
                .filter(Leaderboard.is_active.is_(True))
                .all()
            )

            for board in boards:
                for window_type in ["all-time", "daily", "weekly", "monthly"]:
                    if window_type == "all-time":
                        window_key = "all-time"
                    else:
                        window_key = current_window_key(window_type)

                    self._snapshot_board(board, window_key, db)

            db.commit()
            print(f"[Snapshot] Completed {len(boards)} boards")

        finally:
            db.close()

    def _snapshot_board(self, board, window_key: str, db):
        """Snapshot a single leaderboard window."""
        slug = board.slug
        total = get_total_users(slug, window_key)

        if total == 0:
            return

        top_n = min(total, settings.snapshot_top_n)
        raw = get_top_k_desc(slug, top_n, window=window_key)

        snapshot_data = []
        for i, (user_id, score) in enumerate(raw):
            meta = get_user_meta(slug, user_id) or {}
            snapshot_data.append({
                "rank": i + 1,
                "user_id": user_id,
                "display_name": meta.get("display_name", f"User-{user_id[:8]}"),
                "score": int(score),
            })

        snapshot = LeaderboardSnapshot(
            leaderboard_id=board.id,
            window_key=window_key,
            snapshot_data=snapshot_data,
            top_n=top_n,
            total_users=total,
        )
        db.add(snapshot)


def main():
    worker = SnapshotWorker()
    worker.run()


if __name__ == "__main__":
    main()
```

### `workers/window_rotator.py`

```python
"""
Window rotator worker.

Runs at scheduled intervals to rotate time-windowed leaderboards.
Archives expired windows and ensures new windows are ready.
"""

import time
import signal
from datetime import datetime, timezone

from app.windows import rotate_window, current_window_key
from app.database import SessionLocal
from app.models import Leaderboard, LeaderboardSnapshot


class WindowRotator:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        print("[Rotator] Shutting down...")
        self.running = False

    def run(self):
        """Main loop: check for expired windows every minute."""
        print("[Rotator] Starting window rotator")

        last_daily = None
        last_weekly = None
        last_monthly = None

        while self.running:
            now = datetime.now(timezone.utc)
            current_daily = current_window_key("daily")
            current_weekly = current_window_key("weekly")
            current_monthly = current_window_key("monthly")

            db = SessionLocal()

            try:
                boards = (
                    db.query(Leaderboard)
                    .filter(Leaderboard.is_active.is_(True))
                    .all()
                )

                # Daily rotation
                if last_daily and last_daily != current_daily:
                    for board in boards:
                        result = rotate_window(board.slug, "daily")
                        self._save_snapshot(board, result, db)
                        print(f"[Rotator] Daily rotation: {board.slug}")

                # Weekly rotation
                if last_weekly and last_weekly != current_weekly:
                    for board in boards:
                        result = rotate_window(board.slug, "weekly")
                        self._save_snapshot(board, result, db)
                        print(f"[Rotator] Weekly rotation: {board.slug}")

                # Monthly rotation
                if last_monthly and last_monthly != current_monthly:
                    for board in boards:
                        result = rotate_window(board.slug, "monthly")
                        self._save_snapshot(board, result, db)
                        print(f"[Rotator] Monthly rotation: {board.slug}")

                db.commit()

            except Exception as e:
                print(f"[Rotator] Error: {e}")

            finally:
                db.close()

            last_daily = current_daily
            last_weekly = current_weekly
            last_monthly = current_monthly

            time.sleep(60)

        print("[Rotator] Stopped.")

    def _save_snapshot(self, board, result: dict, db):
        """Save the rotation result as a snapshot."""
        if result["archived_users"] > 0:
            snapshot = LeaderboardSnapshot(
                leaderboard_id=board.id,
                window_key=result["rotated_window"],
                snapshot_data=result["top_entries"],
                top_n=len(result["top_entries"]),
                total_users=result["archived_users"],
            )
            db.add(snapshot)


def main():
    rotator = WindowRotator()
    rotator.run()


if __name__ == "__main__":
    main()
```

Three background workers keep the leaderboard healthy. The **counter flusher** runs every 500 ms, draining sharded counter hashes into canonical sorted sets. It performs a final flush on shutdown to prevent data loss. The **snapshot worker** runs every hour, persisting the top N entries from each leaderboard and time window to PostgreSQL. These snapshots serve as disaster recovery (if Redis fails, rebuild from the latest snapshot) and as analytics data (export to BigQuery for trend analysis). The **window rotator** checks every minute whether a time window has crossed a boundary (midnight for daily, Monday for weekly, 1st for monthly). When it detects a boundary crossing, it archives the previous window's leaderboard (saves a snapshot to PostgreSQL) and deletes the Redis sorted set. The new window's sorted set starts empty and accumulates scores from new submissions.

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

# Default to counter flusher; override CMD for other workers
CMD ["python", "-m", "workers.counter_flusher"]
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
      - DATABASE_URL=postgresql://user:pass@db:5432/leaderboard
      - REDIS_URL=redis://redis:6379/0
      - BASE_URL=http://localhost:8000
      - COUNTER_SHARDS=16
    depends_on:
      - db
      - redis
    volumes:
      - .:/app

  counter-flusher:
    build:
      context: .
      dockerfile: Dockerfile.worker
    command: ["python", "-m", "workers.counter_flusher"]
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/leaderboard
      - REDIS_URL=redis://redis:6379/0
      - COUNTER_SHARDS=16
      - COUNTER_FLUSH_INTERVAL_MS=500
    depends_on:
      - db
      - redis

  snapshot-worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    command: ["python", "-m", "workers.snapshot_worker"]
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/leaderboard
      - REDIS_URL=redis://redis:6379/0
      - SNAPSHOT_INTERVAL_MINUTES=60
      - SNAPSHOT_TOP_N=10000
    depends_on:
      - db
      - redis

  window-rotator:
    build:
      context: .
      dockerfile: Dockerfile.worker
    command: ["python", "-m", "workers.window_rotator"]
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/leaderboard
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - db
      - redis

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=leaderboard
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
    command: redis-server --maxmemory 512mb --maxmemory-policy noeviction

volumes:
  db-data:
  redis-data:
```

`services` defines six containers: the API server, three specialized workers, PostgreSQL, and Redis. The counter flusher has a fast flush interval (500 ms) for near-real-time leaderboard updates. The snapshot worker runs hourly for disaster recovery. The window rotator checks for time boundary crossings every minute. Redis is configured with `--maxmemory-policy noeviction` because evicting sorted set members would corrupt leaderboard data — memory pressure should be handled by increasing Redis capacity, not by evicting entries. `depends_on` ensures PostgreSQL and Redis start before the application containers.

### Docker Compose Commands

```bash
docker compose up                     # Start all services (foreground)
docker compose up -d                  # Start in background (detached)
docker compose up --build             # Rebuild images before starting
docker compose down                   # Stop and remove containers + network
docker compose down -v                # Also remove volumes
docker compose ps                     # List running services
docker compose logs                   # View logs from all services
docker compose logs -f counter-flusher # Follow logs for the flusher
docker compose exec api bash          # Open shell in the API container
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

### `tests/test_counters.py`

```python
from unittest.mock import patch, MagicMock

from app.counters import (
    buffer_score_increment,
    buffer_score_set,
    flush_shard,
    _shard_for_user,
    get_total_pending,
)


def test_shard_assignment_is_deterministic():
    """Test that the same user always maps to the same shard."""
    shard1 = _shard_for_user("user-abc", 16)
    shard2 = _shard_for_user("user-abc", 16)
    assert shard1 == shard2


def test_shard_assignment_distributes_evenly():
    """Test that users are distributed across shards."""
    shards = set()
    for i in range(1000):
        shards.add(_shard_for_user(f"user-{i}", 16))
    # With 1000 users and 16 shards, all shards should be hit
    assert len(shards) == 16


def test_buffer_increment_calls_hincrby():
    """Test that buffer_score_increment uses HINCRBY."""
    with patch("app.counters.redis_client") as mock_redis:
        buffer_score_increment("game-1", "user-abc", 100, total_shards=4)
        mock_redis.hincrby.assert_called_once()


def test_buffer_set_calls_hset():
    """Test that buffer_score_set uses HSET with SET prefix."""
    with patch("app.counters.redis_client") as mock_redis:
        buffer_score_set("game-1", "user-abc", 5000, total_shards=4)
        mock_redis.hset.assert_called_once()
        call_args = mock_redis.hset.call_args
        # Key should have __SET__ prefix
        assert "__SET__user-abc" in str(call_args)


def test_flush_shard_applies_increments():
    """Test that flushing a shard applies buffered increments."""
    with patch("app.counters.redis_client") as mock_redis:
        # Simulate a shard with 2 users
        mock_redis.eval.return_value = [
            "user-1", "100",
            "user-2", "250",
        ]

        with patch("app.counters.increment_score") as mock_incr:
            with patch("app.counters.publish_score_event"):
                flushed = flush_shard("game-1", 0)

        assert flushed == 2
        assert mock_incr.call_count == 2
```

### `tests/test_ranking.py`

```python
from unittest.mock import patch

from app.ranking import (
    calculate_percentile,
    get_user_tier,
    get_leaderboard_page,
)


def test_percentile_rank_1():
    """Test that rank 1 out of 1000 is ~99.9th percentile."""
    p = calculate_percentile(1, 1000)
    assert p == 99.9


def test_percentile_rank_500():
    """Test that rank 500 out of 1000 is 50th percentile."""
    p = calculate_percentile(500, 1000)
    assert p == 50.0


def test_percentile_rank_last():
    """Test that last place is 0th percentile."""
    p = calculate_percentile(1000, 1000)
    assert p == 0.0


def test_percentile_zero_total():
    """Test that zero total returns 0 percentile."""
    p = calculate_percentile(1, 0)
    assert p == 0.0


def test_tier_grandmaster():
    """Test that 99th percentile maps to Grandmaster."""
    tier = get_user_tier(99.5)
    assert tier["name"] == "Grandmaster"


def test_tier_diamond():
    """Test that 92nd percentile maps to Diamond."""
    tier = get_user_tier(92.0)
    assert tier["name"] == "Diamond"


def test_tier_gold():
    """Test that 55th percentile maps to Gold."""
    tier = get_user_tier(55.0)
    assert tier["name"] == "Gold"


def test_tier_bronze():
    """Test that 10th percentile maps to Bronze."""
    tier = get_user_tier(10.0)
    assert tier["name"] == "Bronze"


def test_leaderboard_page_structure():
    """Test that leaderboard page returns correct structure."""
    with patch("app.ranking.get_top_k_desc") as mock_top:
        with patch("app.ranking.get_total_users", return_value=1000):
            with patch("app.ranking.get_user_meta", return_value={"display_name": "Alice"}):
                mock_top.return_value = [("user-1", 5000.0), ("user-2", 4500.0)]

                result = get_leaderboard_page("game-1", page=1, page_size=10)

    assert result["total_users"] == 1000
    assert len(result["entries"]) == 2
    assert result["entries"][0]["rank"] == 1
    assert result["entries"][0]["score"] == 5000
    assert result["has_more"] is True
```

### `tests/test_windows.py`

```python
from app.windows import current_window_key, previous_window_key, get_active_windows


def test_current_daily_key_format():
    """Test that daily keys have YYYY-MM-DD format."""
    key = current_window_key("daily")
    parts = key.split("-")
    assert len(parts) == 3
    assert len(parts[0]) == 4     # Year


def test_current_weekly_key_format():
    """Test that weekly keys have YYYY-WNN format."""
    key = current_window_key("weekly")
    assert "-W" in key


def test_current_monthly_key_format():
    """Test that monthly keys have YYYY-MM format."""
    key = current_window_key("monthly")
    parts = key.split("-")
    assert len(parts) == 2


def test_alltime_key():
    """Test that all-time returns 'all-time'."""
    key = current_window_key("all-time")
    assert key == "all-time"


def test_active_windows_includes_all():
    """Test that active windows includes all 4 types."""
    windows = get_active_windows()
    assert len(windows) == 4
    assert "all-time" in windows


def test_previous_daily_is_different():
    """Test that previous daily key differs from current."""
    current = current_window_key("daily")
    previous = previous_window_key("daily")
    assert current != previous
```

### `tests/test_api.py`

```python
import uuid
from unittest.mock import patch


def test_create_leaderboard(client, db):
    """Test creating a new leaderboard."""
    response = client.post("/leaderboards", json={
        "name": "Weekly Challenge",
        "slug": "weekly-challenge",
        "score_type": "sum",
        "sort_order": "desc",
    })

    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "Weekly Challenge"
    assert data["slug"] == "weekly-challenge"
    assert data["score_type"] == "sum"


def test_duplicate_slug_rejected(client, db):
    """Test that duplicate slugs are rejected."""
    client.post("/leaderboards", json={
        "name": "Game 1",
        "slug": "game-1",
    })
    response = client.post("/leaderboards", json={
        "name": "Game 1 Copy",
        "slug": "game-1",
    })

    assert response.status_code == 409


def test_submit_score(client, db):
    """Test submitting a score."""
    # Create leaderboard first
    client.post("/leaderboards", json={
        "name": "Test Board",
        "slug": "test-board",
    })

    with patch("app.routers.scores.submit_score") as mock_submit:
        mock_submit.return_value = {
            "user_id": str(uuid.uuid4()),
            "display_name": "Alice",
            "score": 1500,
            "delta": 1500,
            "flagged": False,
        }

        response = client.post("/scores", json={
            "leaderboard_slug": "test-board",
            "user_id": str(uuid.uuid4()),
            "display_name": "Alice",
            "score": 1500,
        })

    assert response.status_code == 201


def test_get_top_leaderboard(client, db):
    """Test fetching the top entries."""
    from app.models import Leaderboard, ScoreType, SortOrder

    board = Leaderboard(
        name="Test",
        slug="test",
        score_type=ScoreType.SUM,
        sort_order=SortOrder.DESC,
    )
    db.add(board)
    db.commit()

    with patch("app.routers.leaderboards.get_leaderboard_page") as mock_page:
        mock_page.return_value = {
            "entries": [
                {"rank": 1, "user_id": str(uuid.uuid4()), "display_name": "Alice", "avatar_url": None, "score": 5000},
                {"rank": 2, "user_id": str(uuid.uuid4()), "display_name": "Bob", "avatar_url": None, "score": 4500},
            ],
            "total_users": 1000,
            "has_more": True,
        }

        response = client.get("/leaderboards/test/top?page_size=2")

    assert response.status_code == 200
    data = response.json()
    assert len(data["entries"]) == 2
    assert data["entries"][0]["rank"] == 1


def test_list_leaderboards(client, db):
    """Test listing all leaderboards."""
    from app.models import Leaderboard, ScoreType, SortOrder

    for i in range(3):
        board = Leaderboard(
            name=f"Board {i}",
            slug=f"board-{i}",
            score_type=ScoreType.SUM,
            sort_order=SortOrder.DESC,
        )
        db.add(board)
    db.commit()

    response = client.get("/leaderboards")
    assert response.status_code == 200
    assert len(response.json()) == 3
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_percentile"                   # Run only percentile tests
pytest tests/test_ranking.py                  # Run a specific test file
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
gcloud sql instances create leaderboard-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-4-15360 \
  --region=us-central1 \
  --storage-size=500GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create leaderboard \
  --instance=leaderboard-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=leaderboard-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-4-15360` provisions 4 vCPUs and 15 GB RAM, suitable for the write-heavy score event logging workload. `--availability-type=REGIONAL` enables automatic failover. `--storage-size=500GB` accommodates high write volume; with 7-day retention and 860 GB/day of score events, aggressive partitioning by `created_at` is required.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create leaderboard-cache \
  --size=10 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe leaderboard-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=10` provisions 10 GB of memory for sorted sets (100 M users × 100 B ≈ 10 GB per leaderboard), counter shards, and user metadata hashes. `--tier=STANDARD` enables replication with automatic failover, critical because Redis is the primary data store for real-time rankings. Memorystore is accessible only from within the same VPC network.

### Build & Push Docker Images

```bash
# Build and push API image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/leaderboard/api \
  --file=Dockerfile

# Build and push worker image
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/leaderboard/worker \
  --file=Dockerfile.worker
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build. Two images are built: one for the API server and one shared by all three workers (each overrides the CMD to run its specific worker module).

### Deploy API to Cloud Run

```bash
gcloud run deploy leaderboard-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/leaderboard/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 2Gi \
  --cpu 4 \
  --min-instances 3 \
  --max-instances 50 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:leaderboard-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/leaderboard?host=/cloudsql/YOUR_PROJECT_ID:us-central1:leaderboard-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
BASE_URL=https://your-service-url.run.app,\
COUNTER_SHARDS=16,\
MAX_LEADERBOARD_SIZE=1000"
```

`--min-instances 3` keeps three instances warm for consistent low-latency responses. `--max-instances 50` allows scaling for burst read traffic (2 M RPS). `--cpu 4` and `--memory 2Gi` provide headroom for sorting and serializing large leaderboard pages. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy.

### Deploy Workers to GKE

```bash
# Workers run continuously — GKE is preferred over Cloud Run
gcloud container clusters create leaderboard-cluster \
  --num-nodes=3 \
  --machine-type=e2-standard-4 \
  --region=us-central1 \
  --enable-autoscaling \
  --min-nodes=2 \
  --max-nodes=10

# Deploy workers as Kubernetes Deployments
kubectl apply -f k8s/counter-flusher.yaml
kubectl apply -f k8s/snapshot-worker.yaml
kubectl apply -f k8s/window-rotator.yaml
```

Cloud Run is ideal for the stateless API server. For the always-on workers (counter flusher, snapshot worker, window rotator), **GKE** is preferred because they are long-running daemons that need continuous access to Redis and PostgreSQL. The counter flusher is the most critical — if it stops, scores accumulate in shards without reaching the sorted set.

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis)
gcloud compute networks vpc-access connectors create leaderboard-vpc \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update leaderboard-api \
  --vpc-connector=leaderboard-vpc \
  --region=us-central1
```

Cloud Run services run outside your VPC by default. A VPC connector bridges Cloud Run to private resources like Memorystore. `--range=10.8.0.0/28` allocates a small IP range for the connector.

### Store Secrets Securely

```bash
# Store database credentials in Secret Manager
gcloud secrets create leaderboard-db-password \
  --data-file=- <<< "YOUR_SECURE_PASSWORD"

# Grant Cloud Run access
gcloud secrets add-iam-policy-binding leaderboard-db-password \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Reference secrets in Cloud Run
gcloud run services update leaderboard-api \
  --update-secrets="DB_PASSWORD=leaderboard-db-password:latest"
```

Database credentials should NEVER be in plain-text environment variables. Secret Manager encrypts them at rest and provides audit logging. `--update-secrets` mounts secrets as environment variables fetched from Secret Manager at container start.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Table Partitioning    | `score_events` exceeds 1 B rows           | Partition by `created_at` day (range partition)    |
| Read Replicas         | Heavy analytics/dashboard queries          | Cloud SQL read replicas for historical queries     |
| Connection Pooling    | Many concurrent workers + API connections  | PgBouncer sidecar or Cloud SQL Auth Proxy pooling  |
| Archival              | Retention policy (7 days for events)       | Move old events to BigQuery or Cold Storage        |
| Materialized Views    | Aggregation queries for analytics          | Pre-computed daily/weekly stats views              |

### Redis Scaling

| Strategy                 | When to Use                              | How                                               |
| ------------------------ | ---------------------------------------- | ------------------------------------------------- |
| Increase Shard Count     | Write throughput exceeds 300 K/sec       | Double `COUNTER_SHARDS` (16 → 32 → 64)            |
| Redis Cluster            | Single node memory exceeds 100 GB       | Shard sorted sets across Redis Cluster nodes       |
| Flush Interval Tuning    | Latency vs consistency tradeoff          | Reduce `COUNTER_FLUSH_INTERVAL_MS` for fresher data|
| Memory Optimization      | Redis memory pressure                    | Use `OBJECT ENCODING` to verify ziplist encoding   |
| Separate Read/Write      | Read contention on sorted sets           | Read replicas for top-K queries                    |

### Write Path Scaling

| Strategy                 | When to Use                              | How                                               |
| ------------------------ | ---------------------------------------- | ------------------------------------------------- |
| Cloud Pub/Sub Ingest     | API can't handle 500 K writes/sec       | API publishes to Pub/Sub; workers consume + buffer |
| Batch Writes             | Many small score updates per user        | Client-side batching (accumulate 5s, send once)    |
| Write-Behind Cache       | PostgreSQL can't keep up with events     | Batch-insert score events every 5 seconds          |
| Rate Limit Submissions   | Prevent abusive write patterns           | Token bucket per user (max 10 updates/sec)         |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ leaderboard.xyz│
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │  Cloud CDN +   │
                     │  Load Balancer │
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
           │  ┌──────────────────────────┐   │
           │  │ Memorystore Redis Cluster│   │
           │  │  ┌─────────┐ ┌────────┐ │   │
           │  │  │ Primary │ │Replica │ │   │
           │  │  │ (Writes)│ │(Reads) │ │   │
           │  │  └─────────┘ └────────┘ │   │
           │  └───────┬──────────────────┘   │
           │          │                       │
           │  ┌───────▼──────────────────┐   │
           │  │  Cloud Pub/Sub           │   │
           │  │  ┌────────────────────┐  │   │
           │  │  │ score-events       │  │   │
           │  │  │ rank-changes       │  │   │
           │  │  │ window-rotations   │  │   │
           │  │  └────────────────────┘  │   │
           │  └───────┬──────────────────┘   │
           │          │                       │
           │  ┌───────▼──────────────────┐   │
           │  │  GKE Worker Pools        │   │
           │  │  ┌──────────┐ ┌────────┐ │   │
           │  │  │ Counter  │ │Snapshot│ │   │
           │  │  │ Flusher  │ │Worker  │ │   │
           │  │  │ (500ms)  │ │(hourly)│ │   │
           │  │  └──────────┘ └────────┘ │   │
           │  │  ┌──────────┐ ┌────────┐ │   │
           │  │  │ Window   │ │ Anti-  │ │   │
           │  │  │ Rotator  │ │ Cheat  │ │   │
           │  │  │          │ │Analyzer│ │   │
           │  │  └──────────┘ └────────┘ │   │
           │  └──────────────────────────┘   │
           │                                  │
           │  ┌──────────────────────────┐   │
           │  │  Cloud SQL (Partitioned) │   │
           │  │  + Read Replicas         │   │
           │  └──────────────────────────┘   │
           │                                  │
           │  ┌──────────────────────────┐   │
           │  │  Secret Manager          │   │
           │  │  (DB creds)              │   │
           │  └──────────────────────────┘   │
           └──────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud CDN** caches leaderboard page responses for extremely popular leaderboards. Top-100 queries that don't change every second can be cached with a 5-second TTL, reducing Redis load by 90%.
- **Redis Cluster** with read replicas separates the write path (counter flushes to primary) from the read path (top-K queries from replicas). This prevents write-heavy flushes from affecting read latency.
- **Cloud Pub/Sub** as the ingestion buffer replaces direct API-to-Redis writes. The API publishes score events to Pub/Sub, and dedicated ingestion workers consume and buffer them. This decouples write throughput from API latency.
- **Anti-Cheat Analyzer** is a dedicated worker that runs statistical analysis on `score_events`: detecting bots (consistent superhuman scores), collusion (coordinated scoring patterns), and exploits (impossible score deltas). Flagged users are investigated before being banned.
- **Table Partitioning** on `score_events` by day keeps query performance stable as the table grows. Daily partitions older than 7 days are dropped or archived to BigQuery for trend analysis.
- **Secret Manager** stores database credentials encrypted at rest with audit logging.

### Security Checklist

```
✓ Store all credentials in Secret Manager (never in env vars or code)
✓ Validate score values (non-negative, within max bounds)
✓ Rate limit score submissions per user (max 10/sec)
✓ Log IP addresses for anti-cheat forensics
✓ Flag and investigate suspicious score patterns
✓ Encrypt data in transit (TLS) and at rest (Cloud SQL encryption)
✓ Sanitize display names (prevent XSS in leaderboard UIs)
✓ Restrict admin endpoints with authentication
✓ Enable Cloud Audit Logs for compliance
✓ Set Redis maxmemory-policy to noeviction (never silently drop data)
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create leaderboard-health \
  --display-name="Leaderboard API Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Counter shard depth > 10 000 (flusher is falling behind)
# - Flush latency p99 > 2 seconds (sorted set contention)
# - Top-K query latency p99 > 100 ms (Redis under pressure)
# - Score submission latency p99 > 500 ms (ingestion bottleneck)
# - Anti-cheat flag rate > 5% (possible exploit in the wild)
# - Redis memory usage > 85% (approaching maxmemory)
# - Redis connected clients > 90% of max (connection leak)
# - Sorted set cardinality drift > 1% (counter/sorted set mismatch)
# - Snapshot age > 2 hours (snapshot worker stalled)
# - Window rotation missed (rotator did not fire at boundary)
# - Cloud SQL connections > 90% of max (pool exhaustion)
# - Score event table size > 500 GB (archival overdue)
```

Cloud Run and GKE automatically export metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. The most critical alert is **counter shard depth**: if shards accumulate faster than the flusher drains them, the leaderboard becomes stale. The second most critical is **Redis memory**: sorted sets with 100 M members consume significant memory, and with `noeviction` policy, Redis will reject writes when full rather than silently dropping data. Use Cloud Trace for distributed tracing across the API → Redis → Worker chain. Track business metrics like active users per leaderboard, score submission rate, and rank query volume in a dashboard.
