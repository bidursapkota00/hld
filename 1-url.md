# Design URL Shortener

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [URL Shortening Algorithm](#url-shortening-algorithm)
6. [Core API – Shorten & Redirect](#core-api--shorten--redirect)
7. [Caching with Redis](#caching-with-redis)
8. [Rate Limiting](#rate-limiting)
9. [Analytics & Click Tracking](#analytics--click-tracking)
10. [Custom Aliases & Expiration](#custom-aliases--expiration)
11. [Docker & Docker Compose](#docker--docker-compose)
12. [Testing](#testing)
13. [Simulating CDN, Load Balancer & Millions of Users Locally](#simulating-cdn-load-balancer--millions-of-users-locally)
14. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
15. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement  | Description                                                      |
| ------------ | ---------------------------------------------------------------- |
| Shorten URL  | Given a long URL, generate a unique short link                   |
| Redirect     | Given a short link, redirect to the original URL (HTTP 301/302)  |
| Custom Alias | Optionally let users choose their own short code                 |
| Expiration   | URLs can have a TTL after which they stop working                |
| Analytics    | Track click count, timestamp, referrer, and geolocation per link |

### Non-Functional Requirements

| Requirement  | Target                                                 |
| ------------ | ------------------------------------------------------ |
| Availability | 99.9% uptime                                           |
| Latency      | Redirect in < 10 ms (cache hit) / < 50 ms (cache miss) |
| Durability   | No shortened link should ever be lost                  |
| Scalability  | Handle 1 000 shortens/sec, 10 000 redirects/sec        |

### Back-of-the-Envelope Estimations

```
Read : Write ratio          ≈ 100 : 1
New URLs per month          ≈ 100 M
Reads per month             ≈ 10 B
Storage per record          ≈ 500 bytes (URL + metadata)
Storage for 5 years         ≈ 100 M × 12 × 5 × 500 B ≈ 3 TB
Short code length           ≈ 7 chars  (62^7 ≈ 3.5 trillion unique codes)
Cache (20% hot URLs)        ≈ 100 M × 0.2 × 500 B ≈ 10 GB (fits in Redis)
```

A 7-character code using `[a-zA-Z0-9]` gives 62^7 ≈ 3.5 trillion combinations, which is more than enough for the estimated 6 billion total URLs over 5 years. Caching the top 20% of hot URLs in Redis keeps redirect latency under 10 ms.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │ (Browser/App)│
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │       API Servers           │
                │  (FastAPI on Cloud Run)      │
                │                              │
                │  POST /shorten → short code  │
                │  GET /{code}  → 301 redirect │
                └──────┬──────────────┬────────┘
                       │              │
                ┌──────▼──────┐ ┌─────▼──────┐
                │    Redis    │ │ PostgreSQL  │
                │   (Cache)   │ │  (Storage)  │
                │ Cloud       │ │ Cloud SQL   │
                │ Memorystore │ │             │
                └─────────────┘ └─────────────┘
```

The client sends a request to the load balancer, which forwards it to one of the FastAPI instances running on Cloud Run. For redirects, the API checks Redis first (cache hit returns in < 10 ms). On a cache miss, it queries PostgreSQL, caches the result, and redirects. For new URLs, the API generates a short code, stores it in PostgreSQL, and writes through to Redis.

---

## Project Setup

### Project Structure

```
url-shortener/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── cache.py              # Redis cache layer
│   ├── shortener.py          # URL shortening algorithm
│   ├── rate_limiter.py       # Rate limiting middleware
│   └── routers/
│       ├── __init__.py
│       ├── urls.py           # Shorten & redirect endpoints
│       └── analytics.py     # Analytics endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_shorten.py       # Tests for shortening logic
│   └── test_redirect.py     # Tests for redirection
├── alembic/                  # Database migrations
│   ├── versions/
│   └── env.py
├── alembic.ini
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir url-shortener && cd url-shortener
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
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for Redis. `python-dotenv` loads `.env` files. `httpx` is used for async HTTP requests and testing. `pytest` and `pytest-asyncio` are the testing framework and its async plugin.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/urlshortener
REDIS_URL=redis://localhost:6379/0
BASE_URL=http://localhost:8000
SHORT_CODE_LENGTH=7
DEFAULT_TTL_DAYS=365
RATE_LIMIT_PER_MINUTE=60
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `BASE_URL` is the public-facing domain used to construct full short URLs. `SHORT_CODE_LENGTH` controls how many characters the generated code has. `DEFAULT_TTL_DAYS` sets the default expiration for new links. `RATE_LIMIT_PER_MINUTE` caps how many requests a single IP can make.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐
│              urls                     │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ short_code    VARCHAR(10) UNIQUE      │
│ original_url  TEXT        NOT NULL    │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
│ expires_at    TIMESTAMPTZ NULL        │
│ click_count   BIGINT     DEFAULT 0    │
│ is_active     BOOLEAN    DEFAULT TRUE │
│ user_ip       VARCHAR(45)             │
├──────────────────────────────────────┤
│ INDEX idx_short_code (short_code)     │
│ INDEX idx_expires_at (expires_at)     │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│            click_events               │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ url_id        BIGINT     FK → urls.id │
│ clicked_at    TIMESTAMPTZ DEFAULT NOW │
│ referrer      TEXT                     │
│ user_agent    TEXT                     │
│ ip_address    VARCHAR(45)             │
│ country       VARCHAR(2)              │
├──────────────────────────────────────┤
│ INDEX idx_url_id (url_id)             │
│ INDEX idx_clicked_at (clicked_at)     │
└──────────────────────────────────────┘
```

The `urls` table stores the mapping between short codes and original URLs. `short_code` has a unique index for fast lookups during redirects. `click_count` is a denormalized counter for quick reads without joining. The `click_events` table stores granular per-click data for analytics. Separating events from the URL record avoids locking the URL row on every redirect and lets you run analytics queries independently.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    short_code_length: int = 7
    default_ttl_days: int = 365
    rate_limit_per_minute: int = 60

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
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey, Index, String, Text
)
from sqlalchemy.orm import relationship

from app.database import Base


class URL(Base):
    __tablename__ = "urls"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    short_code = Column(String(10), unique=True, nullable=False, index=True)
    original_url = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)
    click_count = Column(BigInteger, default=0)
    is_active = Column(Boolean, default=True)
    user_ip = Column(String(45), nullable=True)

    clicks = relationship("ClickEvent", back_populates="url")

    __table_args__ = (
        Index("idx_expires_at", "expires_at"),
    )


class ClickEvent(Base):
    __tablename__ = "click_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    url_id = Column(BigInteger, ForeignKey("urls.id"), nullable=False)
    clicked_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    referrer = Column(Text, nullable=True)
    user_agent = Column(Text, nullable=True)
    ip_address = Column(String(45), nullable=True)
    country = Column(String(2), nullable=True)

    url = relationship("URL", back_populates="clicks")

    __table_args__ = (
        Index("idx_url_id", "url_id"),
        Index("idx_clicked_at", "clicked_at"),
    )
```

`BigInteger` for `id` supports billions of rows. `String(45)` for IP addresses accommodates both IPv4 and IPv6. `DateTime(timezone=True)` stores timestamps with timezone information, which is critical for analytics across regions. `relationship()` sets up the ORM-level link between `URL` and `ClickEvent` so you can access `url.clicks` or `click.url` without writing joins manually. `__table_args__` adds composite or additional indexes beyond the ones declared inline on columns.

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, HttpUrl


class URLCreate(BaseModel):
    url: HttpUrl                              # Validates that input is a valid URL
    custom_alias: str | None = None           # Optional custom short code
    expires_in_days: int | None = None        # Optional TTL in days


class URLResponse(BaseModel):
    short_code: str
    short_url: str                            # Full shortened URL
    original_url: str
    created_at: datetime
    expires_at: datetime | None

    class Config:
        from_attributes = True                # Allow ORM model → Pydantic conversion


class URLStats(BaseModel):
    short_code: str
    original_url: str
    click_count: int
    created_at: datetime
    expires_at: datetime | None
    is_active: bool

    class Config:
        from_attributes = True


class ClickEventResponse(BaseModel):
    clicked_at: datetime
    referrer: str | None
    user_agent: str | None
    ip_address: str | None
    country: str | None

    class Config:
        from_attributes = True
```

`HttpUrl` from Pydantic validates that the input string is a properly formatted URL, rejecting malformed inputs at the schema layer before any business logic runs. `from_attributes = True` (formerly `orm_mode`) tells Pydantic to read data from ORM model attributes instead of expecting a dictionary, enabling `URLResponse.model_validate(url_object)`.

---

## URL Shortening Algorithm

The core challenge is generating a unique, short, and collision-resistant code for each URL. There are several approaches:

### Approach Comparison

| Approach           | Pros                           | Cons                                          |
| ------------------ | ------------------------------ | --------------------------------------------- |
| Random + Retry     | Simple, no coordination needed | Collision risk under high load                |
| Counter + Base62   | Zero collisions, predictable   | Requires distributed counter, codes guessable |
| MD5/SHA256 + Trunc | Deterministic, same URL → same | Collision risk with truncation                |
| Snowflake ID       | Globally unique, time-sortable | Requires Snowflake service, long codes        |
| Pre-generated Pool | Fast, no runtime computation   | Requires key generation service               |

For this project, we use **Base62 encoding of an auto-incrementing counter** combined with **random padding** to prevent sequential guessing.

### `app/shortener.py`

```python
import hashlib
import string
import random

BASE62 = string.ascii_letters + string.digits   # a-zA-Z0-9 (62 chars)


def encode_base62(num: int) -> str:
    """Convert a positive integer to a Base62 string."""
    if num == 0:
        return BASE62[0]

    result = []
    while num > 0:
        result.append(BASE62[num % 62])
        num //= 62

    return "".join(reversed(result))


def generate_short_code(url_id: int, length: int = 7) -> str:
    """
    Generate a short code by Base62-encoding the database ID
    and padding with random characters to reach the desired length.
    """
    base = encode_base62(url_id)

    if len(base) >= length:
        return base[:length]

    # Pad with random Base62 characters to desired length
    padding = "".join(random.choices(BASE62, k=length - len(base)))
    return base + padding


def generate_from_hash(original_url: str, length: int = 7) -> str:
    """
    Fallback: generate a short code from the URL's MD5 hash.
    Useful when you want the same URL to always produce the same code.
    """
    hash_digest = hashlib.md5(original_url.encode()).hexdigest()
    num = int(hash_digest[:12], 16)           # Use first 12 hex chars
    code = encode_base62(num)
    return code[:length]


def is_valid_alias(alias: str) -> bool:
    """Check that a custom alias contains only Base62 characters."""
    return all(c in BASE62 for c in alias) and 3 <= len(alias) <= 20
```

`encode_base62` converts an integer to a Base62 string by repeatedly dividing by 62 and mapping remainders to `[a-zA-Z0-9]`. For example, ID `12345` becomes `"dnh"`. `generate_short_code` takes the database-assigned auto-increment ID, encodes it, and pads with random characters so the code is always exactly `length` characters. The random padding prevents users from guessing the next short code by incrementing. `generate_from_hash` is a fallback that hashes the URL itself, giving a deterministic code for the same input URL. `is_valid_alias` validates custom aliases to allow only alphanumeric characters between 3 and 20 characters long. **For Production use Snowflake Id with Base62 encoding without truncation or random padding.**

---

## Core API – Shorten & Redirect

### `app/routers/urls.py`

```python
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_short_code, is_valid_alias

router = APIRouter()


@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
    """Create a shortened URL."""
    original_url = str(payload.url)

    # Handle custom alias
    if payload.custom_alias:
        if not is_valid_alias(payload.custom_alias):
            raise HTTPException(
                status_code=400,
                detail="Alias must be 3-20 alphanumeric characters.",
            )

        existing = db.query(URL).filter(URL.short_code == payload.custom_alias).first()
        if existing:
            raise HTTPException(status_code=409, detail="Alias already taken.")

        short_code = payload.custom_alias
    else:
        short_code = None                     # Will be generated after insert

    # Calculate expiration
    ttl_days = payload.expires_in_days or settings.default_ttl_days
    expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

    # Create the URL record
    url_record = URL(
        short_code=short_code or "placeholder",  # Temporary, replaced below
        original_url=original_url,
        expires_at=expires_at,
        user_ip=request.client.host,
    )

    db.add(url_record)
    db.flush()                                # Get the auto-generated ID

    # Generate short code from ID if no custom alias
    if not payload.custom_alias:
        url_record.short_code = generate_short_code(url_record.id)

    db.commit()
    db.refresh(url_record)

    return URLResponse(
        short_code=url_record.short_code,
        short_url=f"{settings.base_url}/{url_record.short_code}",
        original_url=url_record.original_url,
        created_at=url_record.created_at,
        expires_at=url_record.expires_at,
    )


@router.get("/{short_code}")
def redirect_to_url(short_code: str, request: Request, db: Session = Depends(get_db)):
    """Redirect a short code to its original URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()

    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    if not url_record.is_active:
        raise HTTPException(status_code=410, detail="This link has been deactivated.")

    if url_record.expires_at and url_record.expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=410, detail="This link has expired.")

    # Increment click count
    url_record.click_count += 1
    db.commit()

    return RedirectResponse(
        url=url_record.original_url,
        status_code=301,                      # Permanent redirect (cacheable)
    )
```

`db.flush()` sends the INSERT to the database and populates `url_record.id` without committing the transaction. This lets us use the auto-generated ID to create the short code, then commit everything atomically. `RedirectResponse` with status `301` tells browsers and CDNs to cache the redirect, reducing repeat traffic to the server. Use `302` instead if you need to track every click (browsers won't cache `302` redirects). `request.client.host` captures the caller's IP address for analytics and rate limiting.

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import urls, analytics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(urls.router)
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`lifespan` is an async context manager that runs setup code before the app starts accepting requests (`Base.metadata.create_all` creates missing tables) and teardown code after shutdown. `include_router` mounts the URL and analytics routers. The health check endpoint at `/health` is used by load balancers and container orchestrators to verify the service is running.

---

## Caching with Redis

Every redirect triggers a database query. At 10 000 reads/sec, PostgreSQL would buckle. Redis acts as a read-through cache, serving hot URLs from memory in < 1 ms.

### `app/cache.py`

```python
import json
import redis

from app.config import settings

redis_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,                    # Return strings instead of bytes
    socket_connect_timeout=5,                 # Timeout for initial connection
    retry_on_timeout=True,                    # Auto-retry on timeout
)

CACHE_TTL = 3600                              # Cache entries expire after 1 hour


def get_cached_url(short_code: str) -> dict | None:
    """Retrieve a URL mapping from Redis cache."""
    data = redis_client.get(f"url:{short_code}")
    if data:
        return json.loads(data)
    return None


def set_cached_url(short_code: str, original_url: str, expires_at: str | None):
    """Store a URL mapping in Redis cache."""
    data = json.dumps({
        "original_url": original_url,
        "expires_at": expires_at,
    })
    redis_client.setex(f"url:{short_code}", CACHE_TTL, data)


def delete_cached_url(short_code: str):
    """Remove a URL from cache (used when link is deactivated/expired)."""
    redis_client.delete(f"url:{short_code}")


def increment_click_count(short_code: str) -> int:
    """Atomically increment click count in Redis."""
    return redis_client.incr(f"clicks:{short_code}")
```

`redis.from_url` parses the connection string and creates a connection pool automatically. `decode_responses=True` makes Redis return Python strings instead of bytes, avoiding `.decode()` calls everywhere. `setex` sets a key with an expiration time in seconds; after `CACHE_TTL` seconds, Redis automatically deletes the key. The `url:` prefix namespaces cache keys to avoid collisions with other data stored in the same Redis instance. `incr` is atomic, meaning concurrent requests will not lose counts, even without locks.

### Update Redirect to Use Cache

Update the redirect function in `app/routers/urls.py`:

```python
from app.cache import get_cached_url, set_cached_url, increment_click_count


@router.get("/{short_code}")
def redirect_to_url(short_code: str, request: Request, db: Session = Depends(get_db)):
    """Redirect with Redis cache layer."""

    # 1. Check cache first
    cached = get_cached_url(short_code)
    if cached:
        increment_click_count(short_code)
        return RedirectResponse(url=cached["original_url"], status_code=301)

    # 2. Cache miss → query database
    url_record = db.query(URL).filter(URL.short_code == short_code).first()

    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    if not url_record.is_active:
        raise HTTPException(status_code=410, detail="This link has been deactivated.")

    if url_record.expires_at and url_record.expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=410, detail="This link has expired.")

    # 3. Populate cache
    set_cached_url(
        short_code,
        url_record.original_url,
        url_record.expires_at.isoformat() if url_record.expires_at else None,
    )

    # 4. Update count and redirect
    url_record.click_count += 1
    db.commit()

    return RedirectResponse(url=url_record.original_url, status_code=301)
```

The cache-aside pattern works as follows: check Redis first (step 1). If the URL is cached, increment the counter in Redis atomically and redirect immediately without touching PostgreSQL. On a cache miss (step 2), query the database and populate the cache (step 3) so subsequent requests hit Redis. In production, you would run a periodic batch job to sync `clicks:{short_code}` counters from Redis back to PostgreSQL's `click_count` column, combining Redis speed with PostgreSQL durability.

---

## Rate Limiting

Without rate limiting, a single client can overwhelm the service with requests. We implement a **sliding window** rate limiter using Redis.

### `app/rate_limiter.py`

```python
import time

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.cache import redis_client
from app.config import settings


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """Sliding window rate limiter using Redis sorted sets."""

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health checks
        if request.url.path == "/health":
            return await call_next(request)

        client_ip = request.client.host
        key = f"rate:{client_ip}"
        now = time.time()
        window = 60                           # 60-second sliding window

        pipe = redis_client.pipeline()
        pipe.zremrangebyscore(key, 0, now - window)   # Remove expired entries
        pipe.zadd(key, {str(now): now})               # Add current request
        pipe.zcard(key)                               # Count requests in window
        pipe.expire(key, window)                      # Auto-cleanup key
        results = pipe.execute()

        request_count = results[2]

        if request_count > settings.rate_limit_per_minute:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Max {settings.rate_limit_per_minute} "
                       f"requests per minute.",
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(
            max(0, settings.rate_limit_per_minute - request_count)
        )
        return response
```

A **sliding window** with Redis sorted sets is more accurate than fixed-window counters. Each request timestamp is stored as both the member and score in a sorted set keyed by client IP. `zremrangebyscore` removes entries older than 60 seconds, `zadd` adds the current request, and `zcard` counts the remaining entries. `pipeline()` sends all four commands in a single round trip, reducing latency. The `X-RateLimit-*` response headers inform the client of their remaining quota. `expire` ensures the key is automatically cleaned up even if the client disappears.

### Register the Middleware

Add to `app/main.py`:

```python
from app.rate_limiter import RateLimiterMiddleware

app.add_middleware(RateLimiterMiddleware)
```

---

## Analytics & Click Tracking

### `app/routers/analytics.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import URL, ClickEvent
from app.schemas import URLStats, ClickEventResponse

router = APIRouter()


@router.get("/{short_code}", response_model=URLStats)
def get_url_stats(short_code: str, db: Session = Depends(get_db)):
    """Get statistics for a shortened URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    return url_record


@router.get("/{short_code}/clicks", response_model=list[ClickEventResponse])
def get_click_events(
    short_code: str,
    skip: int = Query(0, ge=0),               # Pagination offset
    limit: int = Query(50, ge=1, le=200),      # Page size (max 200)
    db: Session = Depends(get_db),
):
    """Get individual click events for a shortened URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    clicks = (
        db.query(ClickEvent)
        .filter(ClickEvent.url_id == url_record.id)
        .order_by(ClickEvent.clicked_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return clicks


@router.get("/{short_code}/summary")
def get_click_summary(short_code: str, db: Session = Depends(get_db)):
    """Get aggregated click analytics."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    # Clicks per day for the last 30 days
    daily_clicks = (
        db.query(
            func.date(ClickEvent.clicked_at).label("date"),
            func.count().label("count"),
        )
        .filter(ClickEvent.url_id == url_record.id)
        .group_by(func.date(ClickEvent.clicked_at))
        .order_by(func.date(ClickEvent.clicked_at).desc())
        .limit(30)
        .all()
    )

    # Top referrers
    top_referrers = (
        db.query(
            ClickEvent.referrer,
            func.count().label("count"),
        )
        .filter(ClickEvent.url_id == url_record.id)
        .filter(ClickEvent.referrer.isnot(None))
        .group_by(ClickEvent.referrer)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )

    # Top countries
    top_countries = (
        db.query(
            ClickEvent.country,
            func.count().label("count"),
        )
        .filter(ClickEvent.url_id == url_record.id)
        .filter(ClickEvent.country.isnot(None))
        .group_by(ClickEvent.country)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )

    return {
        "short_code": short_code,
        "total_clicks": url_record.click_count,
        "daily_clicks": [{"date": str(d.date), "count": d.count} for d in daily_clicks],
        "top_referrers": [{"referrer": r.referrer, "count": r.count} for r in top_referrers],
        "top_countries": [{"country": c.country, "count": c.count} for c in top_countries],
    }
```

`Query(0, ge=0)` uses FastAPI's `Query` with validation: `ge=0` ensures the offset is not negative. `func.date()` extracts the date part from a timestamp for grouping clicks by day. `group_by` + `func.count()` aggregates rows in SQL, which is far more efficient than fetching all rows and counting in Python. `order_by(...desc())` returns the most recent results first. `.limit(10)` on referrers and countries returns only the top 10 to keep the response size manageable.

### Update Redirect to Record Click Events

Update the redirect function to log detailed click events:

```python
from app.models import ClickEvent


# Inside redirect_to_url, before db.commit():
click_event = ClickEvent(
    url_id=url_record.id,
    referrer=request.headers.get("referer"),      # Note: HTTP header is "referer"
    user_agent=request.headers.get("user-agent"),
    ip_address=request.client.host,
    country=None,                                  # Populated by GeoIP service
)
db.add(click_event)
url_record.click_count += 1
db.commit()
```

The HTTP `Referer` header (note the deliberate misspelling in the HTTP spec) tells you which page the user clicked the link from. In production, you would send click events to a message queue (Pub/Sub, Kafka) and process them asynchronously to avoid slowing down the redirect response.

---

## Custom Aliases & Expiration

Custom aliases and TTL are already handled in the schemas and the shorten endpoint. Here is how expiration cleanup works:

### Expiration Cleanup Job

```python
# app/cleanup.py
from datetime import datetime, timezone

from sqlalchemy import update

from app.database import SessionLocal
from app.models import URL
from app.cache import delete_cached_url


def deactivate_expired_urls():
    """Mark expired URLs as inactive and clear their cache entries."""
    db = SessionLocal()
    try:
        expired = (
            db.query(URL)
            .filter(URL.expires_at < datetime.now(timezone.utc))
            .filter(URL.is_active == True)
            .all()
        )

        for url in expired:
            url.is_active = False
            delete_cached_url(url.short_code)

        db.commit()
        print(f"Deactivated {len(expired)} expired URLs.")
    finally:
        db.close()


if __name__ == "__main__":
    deactivate_expired_urls()
```

This script is designed to run as a **cron job** or Cloud Scheduler task (e.g., every hour). It queries for active URLs past their expiration, marks them inactive, and evicts them from Redis. Running cleanup as a separate process avoids adding latency to the redirect path. In production, you could schedule this with `Cloud Scheduler → Cloud Run Job` or a Kubernetes CronJob.

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
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
      - REDIS_URL=redis://cache:6379/0
      - BASE_URL=http://localhost:8000
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    restart: unless-stopped

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=urlshortener
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d urlshortener"]
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

`depends_on` with `condition: service_healthy` ensures the API container waits until PostgreSQL and Redis pass their health checks before starting. `pg_isready` is a PostgreSQL utility that checks if the server is accepting connections. `--appendonly yes` enables Redis AOF (Append-Only File) persistence, writing every operation to disk so data survives container restarts. `redis:7-alpine` is a minimal Redis image based on Alpine Linux, significantly smaller than the default image. `restart: unless-stopped` automatically restarts the container unless you explicitly stop it.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Shorten a URL
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/path"}'

# Redirect (follow redirects with -L)
curl -L http://localhost:8000/abc1234

# Get analytics
curl http://localhost:8000/api/analytics/abc1234/summary
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

### `tests/test_shorten.py`

```python
def test_shorten_url(client):
    """Test creating a shortened URL."""
    response = client.post("/shorten", json={"url": "https://www.example.com"})
    assert response.status_code == 201
    data = response.json()
    assert "short_code" in data
    assert "short_url" in data
    assert data["original_url"] == "https://www.example.com/"


def test_shorten_with_custom_alias(client):
    """Test creating a URL with a custom alias."""
    response = client.post(
        "/shorten",
        json={"url": "https://www.example.com", "custom_alias": "mylink"},
    )
    assert response.status_code == 201
    assert response.json()["short_code"] == "mylink"


def test_duplicate_alias_rejected(client):
    """Test that duplicate custom aliases are rejected."""
    client.post(
        "/shorten",
        json={"url": "https://www.example.com", "custom_alias": "taken"},
    )
    response = client.post(
        "/shorten",
        json={"url": "https://www.other.com", "custom_alias": "taken"},
    )
    assert response.status_code == 409


def test_invalid_alias_rejected(client):
    """Test that invalid aliases are rejected."""
    response = client.post(
        "/shorten",
        json={"url": "https://www.example.com", "custom_alias": "a"},
    )
    assert response.status_code == 400


def test_invalid_url_rejected(client):
    """Test that invalid URLs are rejected."""
    response = client.post("/shorten", json={"url": "not-a-url"})
    assert response.status_code == 422
```

### `tests/test_redirect.py`

```python
def test_redirect(client):
    """Test that short codes redirect to the original URL."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == "https://www.example.com/"


def test_redirect_not_found(client):
    """Test 404 for non-existent short codes."""
    response = client.get("/nonexistent", follow_redirects=False)
    assert response.status_code == 404


def test_click_count_increments(client):
    """Test that clicks are tracked."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    client.get(f"/{short_code}", follow_redirects=False)
    client.get(f"/{short_code}", follow_redirects=False)

    stats = client.get(f"/api/analytics/{short_code}")
    assert stats.json()["click_count"] == 2
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_shorten"                      # Run only shorten tests
```

---

## Simulating CDN, Load Balancer & Millions of Users Locally

In production, your URL shortener sits behind a CDN and a load balancer, and handles millions of concurrent users. You can simulate all three locally using **Nginx** (as both CDN cache and load balancer), **multiple API replicas** via Docker Compose, and **Locust** for load testing. This setup lets you observe caching behavior, load distribution, and failure modes before deploying to the cloud.

### Architecture Overview

```
                    ┌──────────────────────────────────────┐
                    │          Locust (Load Generator)     │
                    │     Simulates millions of users      │
                    │     http://localhost:8089 (Web UI)    │
                    └──────────────┬───────────────────────┘
                                   │
                        10,000+ requests/sec
                                   │
                    ┌──────────────▼───────────────────────┐
                    │          Nginx (port 80)             │
                    │                                      │
                    │  ┌────────────────────────────────┐  │
                    │  │  CDN Simulation Layer          │  │
                    │  │  • proxy_cache for 301s        │  │
                    │  │  • Cache-Control headers       │  │
                    │  │  • Cache HIT/MISS via X-Cache  │  │
                    │  └──────────────┬─────────────────┘  │
                    │                 │ (cache MISS)        │
                    │  ┌──────────────▼─────────────────┐  │
                    │  │  Load Balancer Layer           │  │
                    │  │  • Round-robin / least_conn    │  │
                    │  │  • Health checks               │  │
                    │  │  • X-Upstream-Instance header  │  │
                    │  └──────────────┬─────────────────┘  │
                    └─────────────────┼────────────────────┘
                                      │
                 ┌────────────────────┼────────────────────┐
                 │                    │                    │
          ┌──────▼──────┐     ┌──────▼──────┐     ┌──────▼──────┐
          │   API - 1   │     │   API - 2   │     │   API - 3   │
          │  :8000      │     │  :8000      │     │  :8000      │
          └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
                 │                    │                    │
                 └────────────────────┼────────────────────┘
                                      │
                        ┌─────────────┼──────────────┐
                        │                            │
                 ┌──────▼──────┐              ┌──────▼──────┐
                 │   Redis     │              │ PostgreSQL  │
                 │   :6379     │              │   :5432     │
                 └─────────────┘              └─────────────┘
```

### Nginx Configuration

Create `nginx/nginx.conf` to act as both a CDN cache and a load balancer:

```nginx
# nginx/nginx.conf

# ── Upstream: defines the pool of API servers for load balancing ──
upstream url_shortener_api {
    # least_conn sends new requests to the server with the fewest
    # active connections. Better than round-robin when requests
    # have varying processing times.
    least_conn;

    server api-1:8000;
    server api-2:8000;
    server api-3:8000;
}

# ── CDN Cache: stores responses on disk to avoid hitting the API ──
proxy_cache_path /var/cache/nginx/cdn
    levels=1:2                   # Two-level directory hash (e.g., /a/1b/...)
    keys_zone=cdn_cache:10m      # 10 MB shared memory for cache keys (~80k keys)
    max_size=1g                  # Max 1 GB of cached responses on disk
    inactive=60m                 # Evict entries not accessed for 60 minutes
    use_temp_path=off;           # Write directly to cache dir (avoids copy)

server {
    listen 80;
    server_name localhost;

    # ── Logging: track cache behavior ──
    log_format cdn_log '$remote_addr - $request - '
                       'Cache:$upstream_cache_status - '
                       'Upstream:$upstream_addr - '
                       'Response:$status - ${request_time}s';
    access_log /var/log/nginx/access.log cdn_log;

    # ── Health check endpoint (not cached) ──
    location /health {
        proxy_pass http://url_shortener_api;
        proxy_cache off;
    }

    # ── POST /shorten (never cached, needs load balancing only) ──
    location /shorten {
        proxy_pass http://url_shortener_api;
        proxy_cache off;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Tell the client which backend served the request
        add_header X-Upstream-Instance $upstream_addr always;
    }

    # ── GET /{short_code} (cached like a CDN) ──
    location / {
        proxy_pass http://url_shortener_api;

        # CDN cache settings
        proxy_cache cdn_cache;
        proxy_cache_valid 301 302 10m;       # Cache redirects for 10 minutes
        proxy_cache_valid 404 1m;            # Cache 404s briefly to prevent hammering
        proxy_cache_valid any 0;             # Don't cache other status codes
        proxy_cache_key "$scheme$request_method$host$request_uri";
        proxy_cache_use_stale error timeout updating http_500 http_502 http_503;

        # Expose cache status to the client (HIT, MISS, EXPIRED, STALE)
        add_header X-Cache-Status $upstream_cache_status always;
        add_header X-Upstream-Instance $upstream_addr always;

        # Forward real client IP to the API
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # ── Analytics endpoints (not cached) ──
    location /api/analytics {
        proxy_pass http://url_shortener_api;
        proxy_cache off;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # ── Cache status page (for debugging) ──
    location /nginx-status {
        stub_status on;
        allow 127.0.0.1;
        allow 172.0.0.0/8;                  # Docker network
        deny all;
    }
}
```

`proxy_cache_path` creates an on-disk cache that mimics what a CDN does at edge locations. `levels=1:2` creates a two-level directory hierarchy to avoid putting too many files in a single directory. `keys_zone=cdn_cache:10m` allocates 10 MB of shared memory for storing cache keys and metadata (the actual cached responses are on disk). `proxy_cache_valid 301 302 10m` caches redirect responses for 10 minutes, the same way a real CDN like Cloudflare or Cloud CDN would cache `301` responses. `proxy_cache_use_stale` serves stale cached content if the upstream is down, providing resilience similar to CDN origin shielding. `$upstream_cache_status` returns `HIT`, `MISS`, `EXPIRED`, or `STALE`, letting you verify caching is working. `least_conn` distributes requests to the backend with the fewest active connections, which handles uneven response times better than simple round-robin.

### Updated Docker Compose with CDN & Load Balancer

Replace the original `compose.yaml` with this extended version that runs multiple API replicas behind Nginx:

```yaml
# compose.prod-sim.yaml

services:
  # ── Nginx: CDN + Load Balancer ──
  nginx:
    image: nginx:1.27-alpine
    ports:
      - "80:80"                              # Public entry point
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/conf.d/default.conf:ro
      - nginx-cache:/var/cache/nginx/cdn
    depends_on:
      api-1:
        condition: service_healthy
      api-2:
        condition: service_healthy
      api-3:
        condition: service_healthy
    restart: unless-stopped

  # ── API Replicas (simulates Cloud Run instances) ──
  api-1:
    build: .
    environment: &api-env
      DATABASE_URL: postgresql://user:pass@db:5432/urlshortener
      REDIS_URL: redis://cache:6379/0
      BASE_URL: http://localhost
      INSTANCE_ID: api-1
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped

  api-2:
    build: .
    environment:
      <<: *api-env
      INSTANCE_ID: api-2
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped

  api-3:
    build: .
    environment:
      <<: *api-env
      INSTANCE_ID: api-3
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 3
    restart: unless-stopped

  # ── Database ──
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
      POSTGRES_DB: urlshortener
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d urlshortener"]
      interval: 5s
      timeout: 5s
      retries: 5

  # ── Redis Cache ──
  cache:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes --maxmemory 256mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  # ── Locust: Load Testing (simulates millions of users) ──
  locust:
    build:
      context: ./loadtest
      dockerfile: Dockerfile
    ports:
      - "8089:8089"                          # Locust web UI
    environment:
      - LOCUST_HOST=http://nginx
      - LOCUST_USERS=10000                   # Simulated concurrent users
      - LOCUST_SPAWN_RATE=500                # Users spawned per second
    depends_on:
      - nginx
    volumes:
      - ./loadtest:/mnt/locust
    restart: unless-stopped

volumes:
  db-data:
  redis-data:
  nginx-cache:
```

The YAML anchor `&api-env` and merge key `<<: *api-env` avoid duplicating environment variables across replicas. `INSTANCE_ID` lets each replica identify itself in logs and responses. `--maxmemory 256mb --maxmemory-policy allkeys-lru` configures Redis to cap memory at 256 MB and evict least-recently-used keys when full, simulating production memory pressure. `nginx-cache` volume persists the CDN cache across container restarts. Each API replica has its own health check so Nginx only routes to healthy instances.

### Locust Load Test Configuration

Locust simulates user behavior at scale. Create the load test files:

#### `loadtest/Dockerfile`

```dockerfile
FROM locustio/locust:2.31.5

COPY locustfile.py /mnt/locust/locustfile.py
```

#### `loadtest/locustfile.py`

```python
"""
Load test simulating millions of users hitting the URL shortener.
Mimics real-world traffic: 95% redirects (reads), 5% shortens (writes).

Run via Docker Compose or standalone:
    locust -f locustfile.py --host=http://localhost
"""

import random
import string

from locust import HttpUser, task, between, events
from locust.runners import MasterRunner


# ── Shared state: pool of short codes created during the test ──
created_codes: list[str] = []


class URLShortenerUser(HttpUser):
    """Simulates a single user interacting with the URL shortener."""

    # Wait 0.1–1 second between tasks (simulates think time)
    wait_time = between(0.1, 1)

    # Prebuilt list of URLs to shorten (avoids generating on every request)
    SAMPLE_URLS = [
        f"https://www.example.com/article/{i}" for i in range(10000)
    ]

    def on_start(self):
        """Called when a simulated user starts. Create an initial short URL."""
        self._create_short_url()

    @task(95)
    def redirect_to_url(self):
        """
        Simulate a redirect (read). Weighted 95 — this runs 95x more often
        than the shorten task, matching the 100:1 read/write ratio.
        """
        if not created_codes:
            self._create_short_url()
            return

        code = random.choice(created_codes)
        with self.client.get(
            f"/{code}",
            allow_redirects=False,         # Don't follow the 301
            name="GET /{short_code}",      # Group all redirects under one name
            catch_response=True,
        ) as response:
            if response.status_code in (301, 302):
                response.success()
            elif response.status_code == 429:
                response.failure("Rate limited")
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @task(5)
    def shorten_url(self):
        """Simulate creating a new short URL (write). Weighted 5."""
        self._create_short_url()

    def _create_short_url(self):
        """Helper to create a short URL and store the code."""
        url = random.choice(self.SAMPLE_URLS)
        with self.client.post(
            "/shorten",
            json={"url": url},
            name="POST /shorten",
            catch_response=True,
        ) as response:
            if response.status_code == 201:
                data = response.json()
                created_codes.append(data["short_code"])
                # Cap the pool to avoid unbounded memory growth
                if len(created_codes) > 100000:
                    created_codes.pop(0)
                response.success()
            elif response.status_code == 429:
                response.failure("Rate limited")
            else:
                response.failure(f"Status: {response.status_code}")


class HighVolumeUser(HttpUser):
    """
    Aggressive user that only does redirects with no think time.
    Simulates CDN cache warming and bot traffic.
    """

    wait_time = between(0.01, 0.05)
    weight = 1                                # 1 HighVolumeUser per 10 normal users

    @task
    def rapid_redirect(self):
        """Hit the same popular URLs repeatedly to test CDN caching."""
        if not created_codes:
            return

        # Pick from the top 10 "hottest" URLs (simulates Zipf distribution)
        hot_codes = created_codes[:10] if len(created_codes) >= 10 else created_codes
        code = random.choice(hot_codes)

        with self.client.get(
            f"/{code}",
            allow_redirects=False,
            name="GET /{short_code} [hot]",
            catch_response=True,
        ) as response:
            if response.status_code in (301, 302):
                response.success()
            elif response.status_code == 429:
                response.failure("Rate limited")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Seed some URLs before the load test begins."""
    if isinstance(environment.runner, MasterRunner):
        return  # Only seed on workers, not the master

    print("Seeding initial URLs for load test...")
    import requests

    host = environment.host
    for i in range(100):
        try:
            requests.post(
                f"{host}/shorten",
                json={"url": f"https://www.example.com/seed/{i}"},
                timeout=5,
            )
        except Exception:
            pass

    print(f"Seeded URLs. Pool size: {len(created_codes)}")
```

`@task(95)` and `@task(5)` create a 95:5 ratio between reads and writes, closely matching the estimated 100:1 read/write ratio. `between(0.1, 1)` adds random think time between requests to simulate real user behavior rather than hammering the server unrealistically. `name="GET /{short_code}"` groups all redirect requests under a single metric name instead of creating separate entries for each short code. `catch_response=True` lets you manually mark responses as success or failure based on custom logic. `HighVolumeUser` with `weight=1` spawns fewer instances but with minimal wait time, simulating bot traffic or CDN cache-warming behavior. The `on_test_start` event seeds 100 URLs before the test begins so redirect requests have codes to work with immediately.

### Running the Simulation

#### Start Everything

```bash
# Build and launch all services
docker compose -f compose.prod-sim.yaml up --build -d

# Watch logs from all services
docker compose -f compose.prod-sim.yaml logs -f

# Watch only Nginx access logs (see cache HIT/MISS)
docker compose -f compose.prod-sim.yaml logs -f nginx
```

#### Open the Locust Web UI

Navigate to `http://localhost:8089` in your browser. Configure the test:

| Setting          | Value                | Purpose                                      |
| ---------------- | -------------------- | -------------------------------------------- |
| Number of users  | 10,000               | Simulates 10k concurrent users               |
| Spawn rate       | 500                  | Ramp up 500 users per second                 |
| Host             | http://nginx         | Target the Nginx CDN/LB (auto-filled)        |

Click **Start swarming** to begin. Locust shows real-time charts for:
- **Requests per second** — aim for 10,000+ RPS
- **Response time** (median, p95, p99) — cache HITs should be < 5 ms
- **Failure rate** — watch for 429 (rate limiting) or 5xx errors

#### Verify CDN Caching

```bash
# First request → cache MISS (hits the API)
curl -I http://localhost/abc1234
# X-Cache-Status: MISS
# X-Upstream-Instance: 172.18.0.5:8000

# Second request → cache HIT (served from Nginx cache)
curl -I http://localhost/abc1234
# X-Cache-Status: HIT
# X-Upstream-Instance: (empty — no upstream contacted)
```

#### Verify Load Balancing

```bash
# Multiple requests should hit different backends
for i in $(seq 1 6); do
  curl -s -o /dev/null -w "Instance: %header{X-Upstream-Instance}\n" \
    -X POST http://localhost/shorten \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"https://example.com/$i\"}"
done

# Output (with least_conn):
# Instance: 172.18.0.3:8000
# Instance: 172.18.0.4:8000
# Instance: 172.18.0.5:8000
# Instance: 172.18.0.3:8000
# Instance: 172.18.0.5:8000
# Instance: 172.18.0.4:8000
```

#### Simulate Server Failure

```bash
# Kill one API replica
docker compose -f compose.prod-sim.yaml stop api-2

# Nginx automatically stops sending traffic to api-2
# Existing cached redirects still serve from CDN cache (X-Cache-Status: HIT)

# Bring it back
docker compose -f compose.prod-sim.yaml start api-2
```

### Scaling to Millions of Users

A single machine can't truly open millions of TCP connections, but Locust can approximate it:

```bash
# Run Locust in distributed mode for higher throughput
docker compose -f compose.prod-sim.yaml up --build --scale locust-worker=4 -d
```

For distributed Locust, add worker services to `compose.prod-sim.yaml`:

```yaml
  locust-master:
    build:
      context: ./loadtest
    ports:
      - "8089:8089"
    command: >
      locust -f /mnt/locust/locustfile.py
      --master
      --expect-workers=4
    environment:
      - LOCUST_HOST=http://nginx
    volumes:
      - ./loadtest:/mnt/locust
    depends_on:
      - nginx

  locust-worker:
    build:
      context: ./loadtest
    command: >
      locust -f /mnt/locust/locustfile.py
      --worker
      --master-host=locust-master
    environment:
      - LOCUST_HOST=http://nginx
    volumes:
      - ./loadtest:/mnt/locust
    depends_on:
      - locust-master
    deploy:
      replicas: 4                            # 4 workers, each handles ~2500 users
```

Each Locust worker runs in its own process and can generate ~2,500 concurrent users. With 4 workers, you simulate 10,000 users generating 15,000–30,000 requests/sec depending on response times. To simulate millions of users, run Locust workers across multiple machines or use Locust's `--headless` mode with `--users=1000000` and `--spawn-rate=10000`.

### What to Observe

| Metric               | Where to Check                        | What It Tells You                                               |
| -------------------- | ------------------------------------- | --------------------------------------------------------------- |
| Cache hit ratio      | Nginx logs (`X-Cache-Status`)         | How effective the CDN cache is (aim for > 80% HITs)             |
| RPS per instance     | Locust dashboard                      | Whether load is evenly distributed across API replicas          |
| p99 latency          | Locust dashboard                      | Tail latency — cache MISSes should be < 50 ms                  |
| Redis memory         | `docker exec cache redis-cli INFO memory` | Whether the LRU eviction policy is working under pressure  |
| PostgreSQL connections | `docker exec db psql -c 'SELECT count(*) FROM pg_stat_activity'` | Connection pool pressure across replicas |
| Error rate           | Locust failures tab                   | 429s indicate rate limiter kicking in; 5xx means backend issues |
| Nginx active connections | `curl http://localhost/nginx-status` | How many concurrent connections Nginx is handling              |

This local simulation gives you confidence that the architecture handles production-like traffic patterns, that the CDN cache dramatically reduces backend load, and that the load balancer distributes traffic evenly across healthy replicas, all before spending a dollar on cloud infrastructure.

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
gcloud sql instances create urlshortener-db \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1 \
  --storage-size=10GB \
  --storage-auto-increase

# Create the database
gcloud sql databases create urlshortener \
  --instance=urlshortener-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=urlshortener-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-f1-micro` is the smallest (cheapest) instance for development. `--storage-auto-increase` automatically adds storage when the disk fills up. In production, use `--tier=db-custom-2-7680` or higher with `--availability-type=REGIONAL` for high availability with automatic failover.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create urlshortener-cache \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default

# Get the Redis host IP
gcloud redis instances describe urlshortener-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=1` provisions 1 GB of memory. Memorystore is a fully managed Redis service; you don't need to handle patching, backups, or failover. The Redis instance is accessible only from within the same VPC network, which secures it without additional firewall rules.

### Build & Push the Docker Image

```bash
# Build and push to Google Container Registry
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/url-shortener

# Or use Artifact Registry (recommended)
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/url-shortener/api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build, Google's managed CI/CD service. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy to Cloud Run

```bash
gcloud run deploy url-shortener \
  --image gcr.io/YOUR_PROJECT_ID/url-shortener \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 1 \
  --max-instances 10 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:urlshortener-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/urlshortener?host=/cloudsql/YOUR_PROJECT_ID:us-central1:urlshortener-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
BASE_URL=https://your-service-url.run.app,\
SHORT_CODE_LENGTH=7,\
DEFAULT_TTL_DAYS=365,\
RATE_LIMIT_PER_MINUTE=60"
```

`--min-instances 1` keeps at least one instance warm to avoid cold starts. `--max-instances 10` caps the number of instances to control costs. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy, which creates a Unix socket at `/cloudsql/...` for secure, IAM-based database connections without SSL certificates. `--allow-unauthenticated` makes the service publicly accessible. The `?host=/cloudsql/...` in the DATABASE_URL tells SQLAlchemy to connect via the Unix socket created by the proxy.

### Set Up a Custom Domain

```bash
# Map a custom domain
gcloud run domain-mappings create \
  --service url-shortener \
  --domain short.yourdomain.com \
  --region us-central1
```

After running this command, GCP provides DNS records to add at your domain registrar. Once DNS propagates, GCP automatically provisions and renews SSL certificates via Let's Encrypt.

### Schedule the Cleanup Job

```bash
# Create a Cloud Scheduler job to clean up expired URLs every hour
gcloud scheduler jobs create http cleanup-expired-urls \
  --schedule="0 * * * *" \
  --uri="https://your-service-url.run.app/api/cleanup" \
  --http-method=POST \
  --oidc-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

`--schedule="0 * * * *"` runs the job at the start of every hour. `--oidc-service-account-email` authenticates the request using a service account, ensuring only Cloud Scheduler can trigger the cleanup endpoint.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy           | When to Use                               | How                                               |
| ------------------ | ----------------------------------------- | ------------------------------------------------- |
| Read Replicas      | Read-heavy workload (100:1 read/write)    | Cloud SQL read replicas + SQLAlchemy `bind` param |
| Connection Pooling | Many concurrent connections               | PgBouncer sidecar or Cloud SQL Auth Proxy pooling |
| Table Partitioning | `click_events` table grows past 1B rows   | Partition by `clicked_at` month using range parts |
| Sharding           | Single database can't handle write volume | Shard by `short_code` hash across multiple DBs    |

### Cache Scaling

| Strategy             | When to Use                        | How                                              |
| -------------------- | ---------------------------------- | ------------------------------------------------ |
| Redis Cluster        | Dataset exceeds single-node memory | Memorystore cluster mode (auto-shards data)      |
| Cache Warming        | Predictable traffic patterns       | Pre-load popular URLs at startup                 |
| Multi-tier Cache     | Reduce Redis load further          | Local in-memory cache (LRU) → Redis → PostgreSQL |
| Write-behind Counter | Counter writes slow down redirects | Batch-flush click counts from Redis to DB        |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ short.xyz      │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud CDN      │
                     │ (301 caching)  │
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
       │ Instance 1  │ │Run #2  │ │ Instance N  │
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │            VPC Network              │
         │                                      │
         │  ┌─────────┐    ┌────────────────┐  │
         │  │ Memory- │    │   Cloud SQL    │  │
         │  │ store   │    │ (Primary +     │  │
         │  │ (Redis) │    │  Read Replica) │  │
         │  └─────────┘    └────────────────┘  │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Pub/Sub                │   │
         │  │  (Async click event queue)    │   │
         │  └──────────┬────────────────────┘   │
         │             │                        │
         │  ┌──────────▼────────────────────┐   │
         │  │  Cloud Run Job                │   │
         │  │  (Click event processor)      │   │
         │  └───────────────────────────────┘   │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud CDN** caches `301` redirects at edge locations worldwide, meaning repeat visits to popular short URLs never even reach your API servers.
- **Cloud Pub/Sub** decouples click event recording from the redirect path. The API publishes a message per click and returns the redirect immediately. A separate consumer processes events in batches and writes them to PostgreSQL.
- **Read Replicas** handle analytics queries without impacting redirect latency on the primary database.
- **Cloud Run auto-scaling** adjusts from 1 to N instances based on traffic, scaling to zero during idle periods to save costs (set `--min-instances 0` for cost optimization, `1` for low latency).

### Security Checklist

```
✓ Validate and sanitize all input URLs (reject javascript:, data: schemes)
✓ Rate limit by IP and API key
✓ Set CORS headers to restrict which domains can call the API
✓ Use HTTPS everywhere (Cloud Run provides this by default)
✓ Store secrets in Secret Manager, not environment variables
✓ Enable Cloud Audit Logs for compliance
✓ Implement URL blocklisting (phishing, malware domains)
✓ Add CAPTCHA for anonymous URL creation at high volume
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create url-shortener-health \
  --display-name="URL Shortener Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# View Cloud Run metrics in the console
# → CPU utilization, request count, latency (p50, p95, p99), error rate, instance count

# Key metrics to alert on:
# - Error rate > 1% over 5 minutes
# - p99 latency > 500 ms
# - Instance count near max-instances
# - Cloud SQL CPU > 80%
# - Redis memory usage > 80%
```

Cloud Run automatically exports metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing to identify bottlenecks across the API → Redis → PostgreSQL chain.
