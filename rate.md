# Design Rate Limiter

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Rate Limiting Algorithms](#rate-limiting-algorithms)
5. [Token Bucket Implementation](#token-bucket-implementation)
6. [Sliding Window Log Implementation](#sliding-window-log-implementation)
7. [Sliding Window Counter Implementation](#sliding-window-counter-implementation)
8. [Fixed Window Counter Implementation](#fixed-window-counter-implementation)
9. [Leaky Bucket Implementation](#leaky-bucket-implementation)
10. [Distributed Rate Limiter with Redis](#distributed-rate-limiter-with-redis)
11. [Core API & Middleware](#core-api--middleware)
12. [Rule Engine & Configuration](#rule-engine--configuration)
13. [Monitoring & Analytics](#monitoring--analytics)
14. [Docker & Docker Compose](#docker--docker-compose)
15. [Testing](#testing)
16. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
17. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement            | Description                                                             |
| ---------------------- | ----------------------------------------------------------------------- |
| Limit by Key           | Rate limit requests by IP, user ID, API key, or any custom identifier   |
| Multiple Algorithms    | Support Token Bucket, Sliding Window, Fixed Window, and Leaky Bucket    |
| Configurable Rules     | Define per-route, per-user, and global limits via configuration         |
| Distributed Limiting   | Enforce limits consistently across multiple API server instances        |
| Response Headers       | Return `X-RateLimit-*` headers so clients know their remaining quota    |
| Graceful Rejection     | Return HTTP 429 (Too Many Requests) with `Retry-After` header          |
| Tiered Limits          | Different limits for free, pro, and enterprise tiers                    |
| Burst Allowance        | Allow short bursts above the steady-state rate                          |

### Non-Functional Requirements

| Requirement      | Target                                                    |
| ---------------- | --------------------------------------------------------- |
| Latency          | Rate check in < 1 ms (local) / < 5 ms (distributed)      |
| Availability     | 99.99% uptime (rate limiter failure should not block traffic) |
| Consistency      | Slight over-allowance acceptable, never hard-block legitimate traffic |
| Scalability      | Handle 500 K rate check decisions per second               |
| Fault Tolerance  | Fall open (allow traffic) if Redis is unreachable          |

### Back-of-the-Envelope Estimations

```
API requests per second        ≈ 500 K
Unique clients (IPs/keys)      ≈ 10 M
Rate check per request         ≈ 1 (single Redis round trip)
Redis operations per second    ≈ 500 K (one per request)
Memory per client bucket       ≈ 100 bytes (key + counter + timestamp)
Total Redis memory             ≈ 10 M × 100 B = 1 GB
Sliding window log (worst)     ≈ 10 M × 1 000 timestamps × 8 B = 80 GB (too large)
Sliding window counter         ≈ 10 M × 200 B = 2 GB (manageable)
```

Token Bucket and Fixed Window Counter are the most memory-efficient at ~100 bytes per client. Sliding Window Log stores every request timestamp, which explodes to 80 GB at scale — impractical for high-throughput systems. Sliding Window Counter is a hybrid that approximates accuracy while keeping memory under 2 GB. Redis handles 500 K operations per second on a single node with pipelining.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │(Browser/App) │
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │        API Servers          │
                │   (FastAPI on Cloud Run)     │
                │                             │
                │   ┌───────────────────────┐ │
                │   │  Rate Limit Middleware │ │
                │   │  (checks every req)   │ │
                │   └──────────┬────────────┘ │
                │              │              │
                │   ┌──────────▼────────────┐ │
                │   │  Business Logic       │ │
                │   │  (only if allowed)    │ │
                │   └───────────────────────┘ │
                └──────┬──────────────┬───────┘
                       │              │
                ┌──────▼──────┐ ┌─────▼──────┐
                │    Redis    │ │ PostgreSQL  │
                │  (Counters  │ │ (Rules +   │
                │   + State)  │ │  Analytics) │
                │ Memorystore │ │ Cloud SQL   │
                └─────────────┘ └─────────────┘
```

Every incoming request passes through the Rate Limit Middleware before reaching business logic. The middleware extracts the client identifier (IP, API key, user ID), looks up the applicable rule (limits per route, per tier), and queries Redis to check and update the client's counter. If the request is within limits, it proceeds to business logic. If not, the middleware returns HTTP 429 immediately without touching the backend. Redis stores counters and timestamps in memory for sub-millisecond checks. PostgreSQL stores rate limit rules and analytics (blocked request counts, top offenders).

---

## Project Setup

### Project Structure

```
rate-limiter/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── redis_client.py       # Redis connection and helpers
│   ├── algorithms/
│   │   ├── __init__.py
│   │   ├── base.py           # Abstract base class for all algorithms
│   │   ├── token_bucket.py   # Token Bucket algorithm
│   │   ├── sliding_window_log.py    # Sliding Window Log
│   │   ├── sliding_window_counter.py # Sliding Window Counter
│   │   ├── fixed_window.py   # Fixed Window Counter
│   │   └── leaky_bucket.py   # Leaky Bucket algorithm
│   ├── middleware.py         # FastAPI rate limit middleware
│   ├── rules.py              # Rule engine and configuration
│   └── routers/
│       ├── __init__.py
│       ├── limiter.py        # Rate limiter management endpoints
│       └── analytics.py      # Monitoring and analytics endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_token_bucket.py  # Tests for Token Bucket
│   ├── test_sliding_window.py # Tests for Sliding Window
│   ├── test_fixed_window.py  # Tests for Fixed Window
│   └── test_middleware.py    # Tests for middleware integration
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir rate-limiter && cd rate-limiter
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
DATABASE_URL=postgresql://user:pass@localhost:5432/ratelimiter
REDIS_URL=redis://localhost:6379/0
DEFAULT_ALGORITHM=token_bucket
DEFAULT_RATE=100
DEFAULT_WINDOW_SECONDS=60
DEFAULT_BURST_SIZE=20
FAIL_OPEN=true
ENABLE_ANALYTICS=true
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `DEFAULT_ALGORITHM` sets which algorithm to use when no specific rule matches. `DEFAULT_RATE` is the number of allowed requests per window. `DEFAULT_WINDOW_SECONDS` is the time window in seconds. `DEFAULT_BURST_SIZE` controls how many extra requests above the steady rate are allowed in Token Bucket. `FAIL_OPEN=true` means requests are allowed when Redis is unreachable, preventing the rate limiter from becoming a single point of failure.

---

## Rate Limiting Algorithms

Before implementing, understand the tradeoffs between each algorithm:

### Algorithm Comparison

| Algorithm               | Memory per Client | Accuracy   | Burst Handling      | Distributed Complexity |
| ----------------------- | ----------------- | ---------- | ------------------- | ---------------------- |
| Token Bucket            | O(1) – 2 values   | High       | Allows controlled bursts | Low (2 Redis ops)   |
| Sliding Window Log      | O(N) – N timestamps | Exact    | No bursts allowed   | High (sorted set ops)  |
| Sliding Window Counter  | O(1) – 3 values   | ~99.7%     | Smooth, no bursts   | Low (3 Redis ops)      |
| Fixed Window Counter    | O(1) – 1 value    | Low (boundary spike) | Allows 2x at boundary | Lowest (1 Redis op) |
| Leaky Bucket            | O(1) – 2 values   | High       | Smooths output rate | Low (2 Redis ops)      |

**Token Bucket** is the industry standard (used by AWS, Stripe, Cloudflare) because it allows controlled bursts while enforcing a long-term average rate. **Sliding Window Counter** is a close second, offering near-exact accuracy with minimal memory. **Fixed Window** is simplest but allows double the rate at window boundaries. **Leaky Bucket** is ideal when you need a perfectly smooth output rate (e.g., sending emails).

### How Each Algorithm Works

```
TOKEN BUCKET                      SLIDING WINDOW LOG
┌─────────────┐                   ┌─────────────────────────────┐
│ Bucket: 10  │ ← refills at     │  [t1, t2, t3, ... , tN]     │
│ tokens      │   r tokens/sec   │  timestamps in window       │
│             │                   │  count ≤ limit → allow      │
│ Request →   │                   │  remove expired timestamps  │
│ take 1      │                   └─────────────────────────────┘
│ token       │
│ empty →     │                   FIXED WINDOW
│ reject      │                   ┌────────┬────────┬────────┐
└─────────────┘                   │ 10:00  │ 10:01  │ 10:02  │
                                  │ cnt: 8 │ cnt: 3 │ cnt: 0 │
LEAKY BUCKET                      │ max:10 │ max:10 │ max:10 │
┌─────────────┐                   └────────┴────────┴────────┘
│ Queue: FIFO │
│ [r1,r2,r3]  │ → processes      SLIDING WINDOW COUNTER
│             │   at fixed rate   ┌──────────────────────────┐
│ Queue full  │                   │ prev_count × overlap %   │
│ → reject    │                   │ + curr_count             │
└─────────────┘                   │ ≤ limit → allow          │
                                  └──────────────────────────┘
```

---

## Token Bucket Implementation

### `app/algorithms/base.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool                             # Whether the request is allowed
    limit: int                                # Maximum requests in the window
    remaining: int                            # Requests remaining in the window
    reset_after: float                        # Seconds until the window resets
    retry_after: float | None                 # Seconds to wait before retrying (if rejected)


class RateLimiterAlgorithm(ABC):
    """Abstract base class for all rate limiting algorithms."""

    @abstractmethod
    def check(self, key: str, limit: int, window: int, **kwargs) -> RateLimitResult:
        """
        Check if a request should be allowed.

        Args:
            key: Unique identifier for the client (IP, API key, user ID)
            limit: Maximum number of requests allowed in the window
            window: Time window in seconds

        Returns:
            RateLimitResult with the decision and metadata
        """
        pass

    @abstractmethod
    def reset(self, key: str):
        """Reset the rate limit state for a key."""
        pass

    @abstractmethod
    def get_state(self, key: str) -> dict:
        """Get the current state of a rate limit key (for debugging)."""
        pass
```

`RateLimitResult` is a dataclass that encapsulates the rate limit decision along with metadata needed for response headers. `allowed` is the binary decision. `remaining` tells the client how many more requests they can make. `reset_after` tells the client when the window resets. `retry_after` is only set when the request is rejected, telling the client exactly how long to wait. The abstract base class ensures all algorithms implement the same interface, making them interchangeable via configuration.

### `app/algorithms/token_bucket.py`

```python
import time

from app.algorithms.base import RateLimiterAlgorithm, RateLimitResult
from app.redis_client import redis_client


class TokenBucket(RateLimiterAlgorithm):
    """
    Token Bucket Algorithm.

    A bucket holds up to `burst` tokens. Tokens are added at a rate of
    `limit / window` tokens per second. Each request consumes one token.
    If the bucket is empty, the request is rejected.

    This allows short bursts up to `burst` while enforcing `limit` requests
    per `window` seconds on average.
    """

    def check(
        self, key: str, limit: int, window: int, burst: int = 0, **kwargs,
    ) -> RateLimitResult:
        now = time.time()
        bucket_key = f"tb:{key}"
        max_tokens = limit + burst            # Burst allows extra tokens

        # Lua script for atomic token bucket check
        lua_script = """
        local key = KEYS[1]
        local max_tokens = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local window = tonumber(ARGV[4])

        -- Get current state
        local data = redis.call('HMGET', key, 'tokens', 'last_refill')
        local tokens = tonumber(data[1])
        local last_refill = tonumber(data[2])

        -- Initialize if first request
        if tokens == nil then
            tokens = max_tokens
            last_refill = now
        end

        -- Calculate tokens to add since last refill
        local elapsed = now - last_refill
        local new_tokens = elapsed * refill_rate
        tokens = math.min(max_tokens, tokens + new_tokens)

        -- Try to consume a token
        local allowed = 0
        local remaining = 0

        if tokens >= 1 then
            tokens = tokens - 1
            allowed = 1
            remaining = math.floor(tokens)
        else
            remaining = 0
        end

        -- Save state
        redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
        redis.call('EXPIRE', key, window * 2)

        -- Calculate reset time (when bucket will be full again)
        local deficit = max_tokens - tokens
        local reset_after = deficit / refill_rate

        return {allowed, remaining, tostring(reset_after)}
        """

        refill_rate = limit / window          # Tokens per second

        result = redis_client.eval(
            lua_script,
            1,                                # Number of KEYS
            bucket_key,                       # KEYS[1]
            max_tokens,                       # ARGV[1]
            refill_rate,                      # ARGV[2]
            now,                              # ARGV[3]
            window,                           # ARGV[4]
        )

        allowed = bool(result[0])
        remaining = int(result[1])
        reset_after = float(result[2])

        retry_after = None
        if not allowed:
            retry_after = 1.0 / refill_rate   # Time to get 1 token

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after=reset_after,
            retry_after=retry_after,
        )

    def reset(self, key: str):
        redis_client.delete(f"tb:{key}")

    def get_state(self, key: str) -> dict:
        data = redis_client.hgetall(f"tb:{key}")
        return {
            "tokens": float(data.get("tokens", 0)),
            "last_refill": float(data.get("last_refill", 0)),
        }
```

The Token Bucket algorithm uses a **Lua script** executed atomically on Redis. This is critical for correctness in a distributed setting: if two API servers check the same client's bucket simultaneously, the Lua script ensures they don't both see the same token count. Without atomicity, both servers might see 1 token remaining and both allow their request, exceeding the limit. The bucket starts full at `max_tokens = limit + burst`. Tokens are refilled at `limit / window` tokens per second. For example, 100 requests per 60 seconds refills at 1.67 tokens/sec. The `burst` parameter allows a short spike above the steady rate — if burst is 20 and limit is 100, the bucket holds 120 tokens initially, allowing 120 requests in a quick burst before settling to the 100/minute average. `EXPIRE` with `window * 2` auto-cleans the key if the client disappears, preventing memory leaks. The `reset_after` tells the client how long until the bucket is completely full again.

---

## Sliding Window Log Implementation

### `app/algorithms/sliding_window_log.py`

```python
import time

from app.algorithms.base import RateLimiterAlgorithm, RateLimitResult
from app.redis_client import redis_client


class SlidingWindowLog(RateLimiterAlgorithm):
    """
    Sliding Window Log Algorithm.

    Stores the timestamp of every request in a Redis sorted set.
    For each new request, removes timestamps older than the window,
    then counts remaining entries. If count < limit, the request is allowed.

    Pros: Most accurate, no boundary issues.
    Cons: O(N) memory per client where N is the rate limit.
    """

    def check(self, key: str, limit: int, window: int, **kwargs) -> RateLimitResult:
        now = time.time()
        window_start = now - window
        log_key = f"swl:{key}"

        # Lua script for atomic sliding window log check
        lua_script = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window_start = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local window = tonumber(ARGV[4])

        -- Remove expired entries (timestamps before window start)
        redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

        -- Count current entries in the window
        local count = redis.call('ZCARD', key)

        local allowed = 0
        local remaining = 0

        if count < limit then
            -- Add current timestamp (member = timestamp to ensure uniqueness)
            redis.call('ZADD', key, now, tostring(now) .. ':' .. tostring(math.random(1000000)))
            allowed = 1
            remaining = limit - count - 1
        else
            remaining = 0
        end

        -- Set TTL to auto-cleanup
        redis.call('EXPIRE', key, window + 1)

        -- Get the oldest entry to calculate reset time
        local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
        local reset_after = window
        if #oldest > 0 then
            reset_after = tonumber(oldest[2]) + window - now
        end

        return {allowed, remaining, tostring(reset_after)}
        """

        result = redis_client.eval(
            lua_script,
            1,
            log_key,
            now,
            window_start,
            limit,
            window,
        )

        allowed = bool(result[0])
        remaining = int(result[1])
        reset_after = float(result[2])

        retry_after = None
        if not allowed:
            # Time until the oldest entry exits the window
            retry_after = max(0.1, reset_after)

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after=reset_after,
            retry_after=retry_after,
        )

    def reset(self, key: str):
        redis_client.delete(f"swl:{key}")

    def get_state(self, key: str) -> dict:
        entries = redis_client.zrangebyscore(
            f"swl:{key}", "-inf", "+inf", withscores=True
        )
        return {
            "count": len(entries),
            "timestamps": [{"member": m, "score": s} for m, s in entries[-10:]],
        }
```

`ZREMRANGEBYSCORE` removes all sorted set members with scores (timestamps) before the window start, cleaning up expired entries. `ZADD` adds the current timestamp as both the score and part of the member name. The member includes a random suffix to handle sub-millisecond burst requests that might share the same timestamp. `ZCARD` counts the remaining entries in O(1). The `retry_after` is calculated from the oldest entry in the window: when that entry expires, one slot opens up. This algorithm is the most accurate because it considers the exact timestamp of every request, but at scale (e.g., limit=1000), it stores 1000 entries per client, consuming significantly more memory than counter-based approaches.

---

## Sliding Window Counter Implementation

### `app/algorithms/sliding_window_counter.py`

```python
import time

from app.algorithms.base import RateLimiterAlgorithm, RateLimitResult
from app.redis_client import redis_client


class SlidingWindowCounter(RateLimiterAlgorithm):
    """
    Sliding Window Counter Algorithm.

    Combines the accuracy of Sliding Window Log with the efficiency of
    Fixed Window Counter. Uses two fixed windows (previous and current)
    and weights the previous window's count by the overlap percentage.

    Formula:
        weighted_count = prev_count × (1 - elapsed_fraction) + curr_count

    Example (limit=100, window=60s, 45 seconds into current window):
        prev_count = 80, curr_count = 30
        overlap = 1 - 45/60 = 0.25  (25% of prev window still relevant)
        weighted = 80 × 0.25 + 30 = 50  →  allowed (50 < 100)

    Accuracy: ~99.7% (CloudFlare measured 0.003% false positive rate).
    Memory: O(1) per client (3 values: prev_count, curr_count, window_start).
    """

    def check(self, key: str, limit: int, window: int, **kwargs) -> RateLimitResult:
        now = time.time()
        swc_key = f"swc:{key}"

        lua_script = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])

        -- Determine current and previous window boundaries
        local current_window = math.floor(now / window) * window
        local previous_window = current_window - window

        local curr_key = key .. ':' .. tostring(current_window)
        local prev_key = key .. ':' .. tostring(previous_window)

        -- Get counts for both windows
        local curr_count = tonumber(redis.call('GET', curr_key) or '0')
        local prev_count = tonumber(redis.call('GET', prev_key) or '0')

        -- Calculate elapsed fraction of current window
        local elapsed = now - current_window
        local fraction = elapsed / window

        -- Weighted count: previous window's remaining contribution + current
        local weighted = prev_count * (1 - fraction) + curr_count

        local allowed = 0
        local remaining = 0

        if weighted < limit then
            -- Increment current window counter
            redis.call('INCR', curr_key)
            redis.call('EXPIRE', curr_key, window * 2)
            allowed = 1
            remaining = math.floor(limit - weighted - 1)
            if remaining < 0 then remaining = 0 end
        else
            remaining = 0
        end

        -- Reset time is when the current window ends
        local reset_after = window - elapsed

        return {allowed, remaining, tostring(reset_after), tostring(weighted)}
        """

        result = redis_client.eval(
            lua_script,
            1,
            swc_key,
            now,
            window,
            limit,
        )

        allowed = bool(result[0])
        remaining = int(result[1])
        reset_after = float(result[2])
        weighted_count = float(result[3])

        retry_after = None
        if not allowed:
            retry_after = reset_after

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after=reset_after,
            retry_after=retry_after,
        )

    def reset(self, key: str):
        # Delete both current and previous window keys
        now = time.time()
        for offset in [0, 1]:
            pattern = f"swc:{key}:*"
            keys = redis_client.keys(pattern)
            if keys:
                redis_client.delete(*keys)

    def get_state(self, key: str) -> dict:
        pattern = f"swc:{key}:*"
        keys = redis_client.keys(pattern)
        state = {}
        for k in keys:
            state[k] = int(redis_client.get(k) or 0)
        return state
```

The Sliding Window Counter is the sweet spot between accuracy and efficiency. It uses two fixed-window counters (previous and current) and weights their contribution based on how far into the current window we are. At 45 seconds into a 60-second window, the previous window contributes 25% of its count (because only 25% of the previous window still overlaps with our sliding window). CloudFlare measured this algorithm at 99.97% accuracy compared to the exact Sliding Window Log, making the <0.03% inaccuracy acceptable for virtually all use cases. Memory usage is O(1) per client: just two counter keys in Redis. The keys auto-expire after `window * 2` seconds, which is enough time for the next window to use them as the "previous" window before they become irrelevant.

---

## Fixed Window Counter Implementation

### `app/algorithms/fixed_window.py`

```python
import time

from app.algorithms.base import RateLimiterAlgorithm, RateLimitResult
from app.redis_client import redis_client


class FixedWindowCounter(RateLimiterAlgorithm):
    """
    Fixed Window Counter Algorithm.

    Divides time into fixed windows (e.g., 10:00–10:01, 10:01–10:02).
    Each window has a counter that increments with each request.
    When the counter exceeds the limit, requests are rejected.

    Pros: Simplest algorithm, lowest memory and CPU, single Redis op.
    Cons: Allows up to 2× the rate at window boundaries.

    Example of boundary spike (limit=100, window=60s):
        10:00:59 → 100 requests (fills window 10:00)
        10:01:00 → 100 requests (fills window 10:01)
        Result: 200 requests in 2 seconds (both at the boundary)
    """

    def check(self, key: str, limit: int, window: int, **kwargs) -> RateLimitResult:
        now = time.time()
        fw_key = f"fw:{key}"

        lua_script = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])

        -- Calculate the current window key
        local current_window = math.floor(now / window) * window
        local window_key = key .. ':' .. tostring(current_window)

        -- Get current count
        local count = tonumber(redis.call('GET', window_key) or '0')

        local allowed = 0
        local remaining = 0

        if count < limit then
            redis.call('INCR', window_key)
            redis.call('EXPIRE', window_key, window + 1)
            allowed = 1
            remaining = limit - count - 1
        else
            remaining = 0
        end

        -- Reset when current window ends
        local elapsed = now - current_window
        local reset_after = window - elapsed

        return {allowed, remaining, tostring(reset_after)}
        """

        result = redis_client.eval(
            lua_script,
            1,
            fw_key,
            now,
            window,
            limit,
        )

        allowed = bool(result[0])
        remaining = int(result[1])
        reset_after = float(result[2])

        retry_after = None
        if not allowed:
            retry_after = reset_after

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after=reset_after,
            retry_after=retry_after,
        )

    def reset(self, key: str):
        pattern = f"fw:{key}:*"
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)

    def get_state(self, key: str) -> dict:
        pattern = f"fw:{key}:*"
        keys = redis_client.keys(pattern)
        state = {}
        for k in keys:
            state[k] = int(redis_client.get(k) or 0)
        return state
```

The Fixed Window Counter is the simplest algorithm. `math.floor(now / window) * window` snaps the current time to the start of the window, creating a deterministic key like `fw:192.168.1.1:1697644800` (the epoch timestamp of the window start). `INCR` is atomic in Redis, so concurrent requests from the same client will correctly increment without race conditions. The key auto-expires 1 second after the window ends. The boundary spike problem is the main drawback: if a client sends all their requests at the end of one window and the start of the next, they effectively get 2× the limit. This is acceptable for non-critical rate limiting but dangerous for protecting expensive backends.

---

## Leaky Bucket Implementation

### `app/algorithms/leaky_bucket.py`

```python
import time

from app.algorithms.base import RateLimiterAlgorithm, RateLimitResult
from app.redis_client import redis_client


class LeakyBucket(RateLimiterAlgorithm):
    """
    Leaky Bucket Algorithm.

    Requests enter a FIFO queue of fixed capacity. The queue "leaks"
    (processes requests) at a constant rate. If the queue is full when
    a new request arrives, it is rejected.

    Unlike Token Bucket, this enforces a strictly smooth output rate.
    Token Bucket allows bursts (all tokens at once); Leaky Bucket
    spaces requests evenly regardless of arrival pattern.

    Implementation uses a virtual queue: instead of storing actual
    requests, we track the queue size and the time of the last leak.
    """

    def check(self, key: str, limit: int, window: int, **kwargs) -> RateLimitResult:
        now = time.time()
        lb_key = f"lb:{key}"
        queue_capacity = limit                # Max requests that can be queued
        leak_rate = limit / window            # Requests leaked (processed) per second

        lua_script = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local leak_rate = tonumber(ARGV[3])
        local window = tonumber(ARGV[4])

        -- Get current state
        local data = redis.call('HMGET', key, 'queue_size', 'last_leak')
        local queue_size = tonumber(data[1]) or 0
        local last_leak = tonumber(data[2]) or now

        -- Leak: calculate how many requests have been processed since last check
        local elapsed = now - last_leak
        local leaked = elapsed * leak_rate
        queue_size = math.max(0, queue_size - leaked)

        local allowed = 0
        local remaining = 0

        if queue_size < capacity then
            queue_size = queue_size + 1
            allowed = 1
            remaining = math.floor(capacity - queue_size)
        else
            remaining = 0
        end

        -- Save state
        redis.call('HMSET', key, 'queue_size', queue_size, 'last_leak', now)
        redis.call('EXPIRE', key, window * 2)

        -- Time until one slot opens up
        local retry_after = 0
        if allowed == 0 then
            retry_after = 1.0 / leak_rate
        end

        -- Time until queue is fully empty
        local reset_after = queue_size / leak_rate

        return {allowed, remaining, tostring(reset_after), tostring(retry_after)}
        """

        result = redis_client.eval(
            lua_script,
            1,
            lb_key,
            now,
            queue_capacity,
            leak_rate,
            window,
        )

        allowed = bool(result[0])
        remaining = int(result[1])
        reset_after = float(result[2])
        retry_after_val = float(result[3])

        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after=reset_after,
            retry_after=retry_after_val if not allowed else None,
        )

    def reset(self, key: str):
        redis_client.delete(f"lb:{key}")

    def get_state(self, key: str) -> dict:
        data = redis_client.hgetall(f"lb:{key}")
        return {
            "queue_size": float(data.get("queue_size", 0)),
            "last_leak": float(data.get("last_leak", 0)),
        }
```

The Leaky Bucket uses a virtual queue instead of storing actual requests. `queue_size` tracks how many "slots" are occupied. `leaked` calculates how many slots have been freed since the last check based on the constant `leak_rate`. If a burst of 50 requests arrives at once, the queue fills by 50 and then leaks at a constant rate (e.g., 1.67 per second for 100/minute). Once the queue is full, new requests are rejected until slots leak. The key difference from Token Bucket: Token Bucket allows all accumulated tokens to be spent instantly (bursty output), while Leaky Bucket enforces a perfectly smooth output regardless of input burstiness. This makes Leaky Bucket ideal for rate-limiting outbound operations like sending emails, making API calls, or writing to databases where the downstream system needs a consistent request rate.

---

## Distributed Rate Limiter with Redis

### `app/redis_client.py`

```python
import redis

from app.config import settings

redis_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,                    # Return strings instead of bytes
    socket_connect_timeout=5,                 # Timeout for initial connection
    socket_timeout=1,                         # Timeout per operation (1 second max)
    retry_on_timeout=True,                    # Auto-retry on timeout
    health_check_interval=30,                 # Ping Redis every 30s to detect failures
)


def is_redis_healthy() -> bool:
    """Check if Redis is reachable."""
    try:
        return redis_client.ping()
    except (redis.ConnectionError, redis.TimeoutError):
        return False
```

`socket_timeout=1` ensures a single rate limit check never takes more than 1 second. If Redis is slow, the operation times out and the middleware falls back to allowing the request (fail-open). `health_check_interval=30` makes the Redis client automatically ping the server every 30 seconds to detect dead connections early. `is_redis_healthy` is used by the middleware to quickly determine if Redis is reachable before attempting a rate limit check.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/ratelimiter"
    redis_url: str = "redis://localhost:6379/0"
    default_algorithm: str = "token_bucket"
    default_rate: int = 100
    default_window_seconds: int = 60
    default_burst_size: int = 20
    fail_open: bool = True
    enable_analytics: bool = True

    class Config:
        env_file = ".env"


settings = Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads values from environment variables and `.env` files. Field names are case-insensitive and map to the corresponding `ENV_VAR` names. The `Config` inner class tells Pydantic to also load from the `.env` file.

---

## Core API & Middleware

### `app/middleware.py`

```python
import time

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.algorithms.base import RateLimitResult
from app.algorithms.token_bucket import TokenBucket
from app.algorithms.sliding_window_counter import SlidingWindowCounter
from app.algorithms.sliding_window_log import SlidingWindowLog
from app.algorithms.fixed_window import FixedWindowCounter
from app.algorithms.leaky_bucket import LeakyBucket
from app.config import settings
from app.redis_client import is_redis_healthy
from app.rules import get_rule_for_request


# Algorithm registry
ALGORITHMS = {
    "token_bucket": TokenBucket(),
    "sliding_window_log": SlidingWindowLog(),
    "sliding_window_counter": SlidingWindowCounter(),
    "fixed_window": FixedWindowCounter(),
    "leaky_bucket": LeakyBucket(),
}


def extract_client_key(request: Request) -> str:
    """
    Extract the rate limit key from the request.
    Priority: API key header → authenticated user ID → IP address.
    """
    # Check for API key
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return f"apikey:{api_key}"

    # Check for authenticated user
    user_id = request.headers.get("X-User-ID")
    if user_id:
        return f"user:{user_id}"

    # Fall back to IP address
    client_ip = request.client.host
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()

    return f"ip:{client_ip}"


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that rate limits every incoming request.

    Behavior:
    - Extracts client key (API key, user ID, or IP)
    - Looks up the applicable rate limit rule
    - Checks the rate limiter (Redis-backed)
    - Adds X-RateLimit-* headers to every response
    - Returns 429 if rate limit exceeded
    - Falls open (allows) if Redis is down and FAIL_OPEN=true
    """

    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for health checks and internal endpoints
        if request.url.path in ("/health", "/docs", "/openapi.json", "/redoc"):
            return await call_next(request)

        # Extract client identifier
        client_key = extract_client_key(request)

        # Get the applicable rule for this request
        rule = get_rule_for_request(request, client_key)

        # Build the full rate limit key (includes route for per-route limits)
        rate_key = f"{client_key}:{rule.route_pattern}"

        # Check Redis health
        if not is_redis_healthy():
            if settings.fail_open:
                # Allow the request but log the failure
                response = await call_next(request)
                response.headers["X-RateLimit-Status"] = "fail-open"
                return response
            else:
                raise HTTPException(
                    status_code=503,
                    detail="Rate limiter temporarily unavailable.",
                )

        # Get the algorithm
        algorithm = ALGORITHMS.get(rule.algorithm, ALGORITHMS["token_bucket"])

        # Check rate limit
        try:
            result: RateLimitResult = algorithm.check(
                key=rate_key,
                limit=rule.limit,
                window=rule.window_seconds,
                burst=rule.burst_size,
            )
        except Exception:
            # Redis operation failed
            if settings.fail_open:
                response = await call_next(request)
                response.headers["X-RateLimit-Status"] = "fail-open"
                return response
            raise HTTPException(status_code=503, detail="Rate limiter error.")

        if not result.allowed:
            response = Response(
                content=f'{{"detail":"Rate limit exceeded. Retry after {result.retry_after:.1f} seconds."}}',
                status_code=429,
                media_type="application/json",
            )
            response.headers["Retry-After"] = str(int(result.retry_after or 1))
            response.headers["X-RateLimit-Limit"] = str(result.limit)
            response.headers["X-RateLimit-Remaining"] = "0"
            response.headers["X-RateLimit-Reset"] = str(int(result.reset_after))
            return response

        # Request allowed → proceed to business logic
        response = await call_next(request)

        # Add rate limit headers to successful responses
        response.headers["X-RateLimit-Limit"] = str(result.limit)
        response.headers["X-RateLimit-Remaining"] = str(result.remaining)
        response.headers["X-RateLimit-Reset"] = str(int(result.reset_after))

        return response
```

The middleware follows a strict order: skip known paths (health, docs) → extract the client key → look up the rule → check Redis health → run the algorithm → set headers. `extract_client_key` uses a priority chain: API key is checked first because it is the most specific identifier, followed by user ID, and finally IP address. `X-Forwarded-For` is checked for the real IP when behind a load balancer (Cloud Run sets this automatically). The **fail-open** pattern is critical: if Redis goes down, the middleware allows all requests through rather than rejecting them. This prevents the rate limiter from becoming a single point of failure that takes down the entire API. The `X-RateLimit-Status: fail-open` header signals to monitoring that rate limiting is degraded. `Retry-After` is a standard HTTP header that well-behaved clients use to back off automatically.

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

---

## Rule Engine & Configuration

### `app/models.py`

```python
import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Enum, Float,
    Integer, String, Text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database import Base


class Tier(str, enum.Enum):
    FREE = "free"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class Algorithm(str, enum.Enum):
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW_LOG = "sliding_window_log"
    SLIDING_WINDOW_COUNTER = "sliding_window_counter"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


class RateLimitRule(Base):
    __tablename__ = "rate_limit_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), unique=True, nullable=False)
    route_pattern = Column(String(200), nullable=False, default="*")
    tier = Column(Enum(Tier), nullable=False, default=Tier.FREE)
    algorithm = Column(Enum(Algorithm), nullable=False, default=Algorithm.TOKEN_BUCKET)
    limit = Column(Integer, nullable=False)
    window_seconds = Column(Integer, nullable=False)
    burst_size = Column(Integer, default=0)
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


class RateLimitEvent(Base):
    __tablename__ = "rate_limit_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    client_key = Column(String(255), nullable=False)
    route = Column(String(200), nullable=False)
    algorithm = Column(String(50), nullable=False)
    allowed = Column(Boolean, nullable=False)
    remaining = Column(Integer, nullable=False)
    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    metadata_ = Column("metadata", JSONB, nullable=True)
```

`RateLimitRule` stores configurable rate limit policies. `route_pattern` can be a specific path like `/api/users` or a wildcard `*` for global rules. `tier` allows different limits for free, pro, and enterprise users. `burst_size` is only relevant for Token Bucket and controls how many extra requests above the steady rate are allowed. `RateLimitEvent` logs every rate limit decision for analytics, tracking which clients are hitting limits and which routes are most heavily rate-limited. `JSONB` for metadata allows storing additional context like the client's IP, user agent, or the specific rule that was applied.

### `app/rules.py`

```python
from dataclasses import dataclass

from fastapi import Request

from app.config import settings


@dataclass
class AppliedRule:
    """The resolved rate limit rule for a specific request."""
    route_pattern: str
    algorithm: str
    limit: int
    window_seconds: int
    burst_size: int
    tier: str


# In-memory rule cache (loaded from DB or config at startup)
# In production, this would be refreshed periodically from PostgreSQL
RULES: list[dict] = [
    {
        "route_pattern": "/api/*",
        "tier": "free",
        "algorithm": "token_bucket",
        "limit": 100,
        "window_seconds": 60,
        "burst_size": 20,
    },
    {
        "route_pattern": "/api/*",
        "tier": "pro",
        "algorithm": "token_bucket",
        "limit": 1000,
        "window_seconds": 60,
        "burst_size": 100,
    },
    {
        "route_pattern": "/api/*",
        "tier": "enterprise",
        "algorithm": "sliding_window_counter",
        "limit": 10000,
        "window_seconds": 60,
        "burst_size": 0,
    },
    {
        "route_pattern": "/api/heavy-endpoint",
        "tier": "free",
        "algorithm": "leaky_bucket",
        "limit": 10,
        "window_seconds": 60,
        "burst_size": 0,
    },
]


def _get_client_tier(client_key: str) -> str:
    """
    Determine the client's tier from their key.
    In production, this would look up the tier from a database or cache.
    """
    if client_key.startswith("apikey:ent_"):
        return "enterprise"
    elif client_key.startswith("apikey:pro_"):
        return "pro"
    return "free"


def _match_route(pattern: str, path: str) -> bool:
    """Check if a route path matches a pattern (simple wildcard matching)."""
    if pattern == "*":
        return True
    if pattern.endswith("/*"):
        prefix = pattern[:-2]
        return path.startswith(prefix)
    return pattern == path


def get_rule_for_request(request: Request, client_key: str) -> AppliedRule:
    """
    Find the most specific rate limit rule for a request.

    Priority:
    1. Exact route match + client tier
    2. Wildcard route match + client tier
    3. Global default
    """
    tier = _get_client_tier(client_key)
    path = request.url.path

    # Find matching rules, most specific first
    best_match = None
    for rule in RULES:
        if rule["tier"] != tier:
            continue
        if not _match_route(rule["route_pattern"], path):
            continue

        # Prefer exact matches over wildcards
        if best_match is None or (
            "*" not in rule["route_pattern"] and "*" in best_match["route_pattern"]
        ):
            best_match = rule

    if best_match:
        return AppliedRule(
            route_pattern=best_match["route_pattern"],
            algorithm=best_match["algorithm"],
            limit=best_match["limit"],
            window_seconds=best_match["window_seconds"],
            burst_size=best_match["burst_size"],
            tier=tier,
        )

    # Fall back to global default
    return AppliedRule(
        route_pattern="*",
        algorithm=settings.default_algorithm,
        limit=settings.default_rate,
        window_seconds=settings.default_window_seconds,
        burst_size=settings.default_burst_size,
        tier=tier,
    )
```

The rule engine resolves which rate limit policy applies to each request. Rules are stored in memory for speed (sub-microsecond lookup) and loaded from PostgreSQL at startup, then refreshed periodically (e.g., every 5 minutes). The matching priority ensures specific rules override general ones: `/api/heavy-endpoint` at 10 req/min overrides the general `/api/*` at 100 req/min. The `_get_client_tier` function uses API key prefixes (`ent_`, `pro_`) as a simple tier detection mechanism. In production, this would query a user database or a cached tier mapping. The fallback to global defaults ensures every request is rate-limited even if no specific rule matches.

### `app/schemas.py`

```python
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class RuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    route_pattern: str = Field(default="*", max_length=200)
    tier: str = "free"
    algorithm: str = "token_bucket"
    limit: int = Field(gt=0)
    window_seconds: int = Field(gt=0)
    burst_size: int = Field(ge=0, default=0)


class RuleResponse(BaseModel):
    id: uuid.UUID
    name: str
    route_pattern: str
    tier: str
    algorithm: str
    limit: int
    window_seconds: int
    burst_size: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class RateLimitStatus(BaseModel):
    client_key: str
    rule_applied: str
    algorithm: str
    limit: int
    remaining: int
    reset_after: float
    tier: str


class AnalyticsSummary(BaseModel):
    total_requests: int
    allowed_requests: int
    rejected_requests: int
    rejection_rate: float
    top_rejected_clients: list[dict]
    top_rejected_routes: list[dict]


class BulkCheckRequest(BaseModel):
    keys: list[str] = Field(min_length=1, max_length=100)
    route: str = "*"
```

`Field(gt=0)` ensures `limit` and `window_seconds` are positive integers. `Field(ge=0)` allows `burst_size` to be zero (no burst allowed). `RateLimitStatus` is returned when clients want to check their current quota without consuming it. `AnalyticsSummary` provides aggregate metrics for monitoring dashboards. `BulkCheckRequest` allows checking multiple keys at once, useful for batch operations.

### `app/routers/limiter.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.algorithms.token_bucket import TokenBucket
from app.algorithms.sliding_window_counter import SlidingWindowCounter
from app.algorithms.sliding_window_log import SlidingWindowLog
from app.algorithms.fixed_window import FixedWindowCounter
from app.algorithms.leaky_bucket import LeakyBucket
from app.config import settings
from app.database import get_db
from app.middleware import ALGORITHMS
from app.models import RateLimitRule
from app.rules import get_rule_for_request, _get_client_tier, AppliedRule
from app.schemas import RuleCreate, RuleResponse, RateLimitStatus

router = APIRouter()


@router.post("/rules", response_model=RuleResponse, status_code=201)
def create_rule(payload: RuleCreate, db: Session = Depends(get_db)):
    """Create a new rate limit rule."""
    existing = (
        db.query(RateLimitRule)
        .filter(RateLimitRule.name == payload.name)
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="Rule name already exists.")

    rule = RateLimitRule(
        name=payload.name,
        route_pattern=payload.route_pattern,
        tier=payload.tier,
        algorithm=payload.algorithm,
        limit=payload.limit,
        window_seconds=payload.window_seconds,
        burst_size=payload.burst_size,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)

    return rule


@router.get("/rules", response_model=list[RuleResponse])
def list_rules(db: Session = Depends(get_db)):
    """List all rate limit rules."""
    rules = db.query(RateLimitRule).filter(RateLimitRule.is_active == True).all()
    return rules


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: str, db: Session = Depends(get_db)):
    """Deactivate a rate limit rule."""
    rule = db.query(RateLimitRule).filter(RateLimitRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found.")

    rule.is_active = False
    db.commit()

    return {"status": "deactivated", "rule_id": str(rule.id)}


@router.get("/status/{client_key}", response_model=RateLimitStatus)
def check_status(client_key: str):
    """
    Check the current rate limit status for a client key
    without consuming a request.
    """
    tier = _get_client_tier(client_key)

    # Use defaults since we don't have a request context
    rule = AppliedRule(
        route_pattern="*",
        algorithm=settings.default_algorithm,
        limit=settings.default_rate,
        window_seconds=settings.default_window_seconds,
        burst_size=settings.default_burst_size,
        tier=tier,
    )

    algorithm = ALGORITHMS.get(rule.algorithm, ALGORITHMS["token_bucket"])
    state = algorithm.get_state(f"{client_key}:*")

    # Calculate remaining without consuming
    result = algorithm.check(
        key=f"status_check:{client_key}",     # Separate key so it doesn't count
        limit=rule.limit,
        window=rule.window_seconds,
        burst=rule.burst_size,
    )

    return RateLimitStatus(
        client_key=client_key,
        rule_applied=rule.route_pattern,
        algorithm=rule.algorithm,
        limit=rule.limit,
        remaining=result.remaining,
        reset_after=result.reset_after,
        tier=tier,
    )


@router.post("/reset/{client_key}")
def reset_client(client_key: str):
    """Reset all rate limit counters for a client."""
    for algo in ALGORITHMS.values():
        algo.reset(client_key)

    return {"status": "reset", "client_key": client_key}
```

The CRUD endpoints let operators manage rate limit rules dynamically without redeploying the service. `create_rule` adds a new rule to PostgreSQL and the in-memory cache. `delete_rule` soft-deletes (deactivates) the rule so historical analytics still reference it. `check_status` lets clients query their current quota without consuming a request, using a separate key (`status_check:`) to avoid affecting the actual counter. `reset_client` clears all counters across all algorithms for a given client, useful for customer support when a legitimate user is mistakenly rate-limited.

---

## Monitoring & Analytics

### `app/routers/analytics.py`

```python
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import RateLimitEvent
from app.redis_client import redis_client
from app.schemas import AnalyticsSummary

router = APIRouter()

# Redis keys for real-time analytics
TOTAL_KEY = "analytics:total"
ALLOWED_KEY = "analytics:allowed"
REJECTED_KEY = "analytics:rejected"
REJECTED_CLIENTS_KEY = "analytics:rejected_clients"
REJECTED_ROUTES_KEY = "analytics:rejected_routes"


def record_decision(
    client_key: str,
    route: str,
    allowed: bool,
    algorithm: str,
):
    """
    Record a rate limit decision for analytics.
    Uses Redis for real-time counters and periodic flush to PostgreSQL.
    """
    pipe = redis_client.pipeline()

    pipe.incr(TOTAL_KEY)
    if allowed:
        pipe.incr(ALLOWED_KEY)
    else:
        pipe.incr(REJECTED_KEY)
        # Track top rejected clients and routes using sorted sets
        pipe.zincrby(REJECTED_CLIENTS_KEY, 1, client_key)
        pipe.zincrby(REJECTED_ROUTES_KEY, 1, route)

    # Auto-expire counters after 24 hours for a rolling window
    pipe.expire(TOTAL_KEY, 86400)
    pipe.expire(ALLOWED_KEY, 86400)
    pipe.expire(REJECTED_KEY, 86400)
    pipe.expire(REJECTED_CLIENTS_KEY, 86400)
    pipe.expire(REJECTED_ROUTES_KEY, 86400)

    pipe.execute()


@router.get("/summary", response_model=AnalyticsSummary)
def get_analytics_summary():
    """Get real-time rate limiting analytics."""
    total = int(redis_client.get(TOTAL_KEY) or 0)
    allowed = int(redis_client.get(ALLOWED_KEY) or 0)
    rejected = int(redis_client.get(REJECTED_KEY) or 0)

    rejection_rate = (rejected / total * 100) if total > 0 else 0

    # Top 10 rejected clients
    top_clients = redis_client.zrevrange(
        REJECTED_CLIENTS_KEY, 0, 9, withscores=True
    )

    # Top 10 rejected routes
    top_routes = redis_client.zrevrange(
        REJECTED_ROUTES_KEY, 0, 9, withscores=True
    )

    return AnalyticsSummary(
        total_requests=total,
        allowed_requests=allowed,
        rejected_requests=rejected,
        rejection_rate=round(rejection_rate, 2),
        top_rejected_clients=[
            {"client": c, "count": int(s)} for c, s in top_clients
        ],
        top_rejected_routes=[
            {"route": r, "count": int(s)} for r, s in top_routes
        ],
    )


@router.get("/history")
def get_event_history(
    client_key: str | None = None,
    route: str | None = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Get historical rate limit events from PostgreSQL."""
    query = db.query(RateLimitEvent)

    if client_key:
        query = query.filter(RateLimitEvent.client_key == client_key)
    if route:
        query = query.filter(RateLimitEvent.route == route)

    events = (
        query
        .order_by(RateLimitEvent.timestamp.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return [
        {
            "id": e.id,
            "client_key": e.client_key,
            "route": e.route,
            "algorithm": e.algorithm,
            "allowed": e.allowed,
            "remaining": e.remaining,
            "timestamp": e.timestamp.isoformat(),
        }
        for e in events
    ]


@router.get("/stats/by-route")
def get_stats_by_route(db: Session = Depends(get_db)):
    """Get aggregate stats grouped by route."""
    stats = (
        db.query(
            RateLimitEvent.route,
            func.count().label("total"),
            func.sum(
                func.cast(RateLimitEvent.allowed == False, Integer)
            ).label("rejected"),
        )
        .group_by(RateLimitEvent.route)
        .order_by(func.count().desc())
        .limit(20)
        .all()
    )

    return [
        {
            "route": s.route,
            "total_requests": s.total,
            "rejected_requests": int(s.rejected or 0),
            "rejection_rate": round(
                (int(s.rejected or 0) / s.total * 100) if s.total > 0 else 0, 2
            ),
        }
        for s in stats
    ]
```

Analytics uses a two-tier storage approach. Real-time counters live in Redis for instant dashboard updates: `INCR` is atomic and O(1), and `ZINCRBY` maintains sorted sets of top offenders. Redis counters auto-expire after 24 hours, providing a rolling window. Historical data is periodically flushed to PostgreSQL for long-term analysis and compliance. `ZREVRANGE` returns sorted set members in descending score order, so the most frequently rejected clients and routes appear first. The `record_decision` function is called from the middleware after every rate limit check, using `pipeline()` to batch all 5-7 Redis commands into a single round trip.

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
      - DATABASE_URL=postgresql://user:pass@db:5432/ratelimiter
      - REDIS_URL=redis://cache:6379/0
      - DEFAULT_ALGORITHM=token_bucket
      - DEFAULT_RATE=100
      - DEFAULT_WINDOW_SECONDS=60
      - FAIL_OPEN=true
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
      - POSTGRES_DB=ratelimiter
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d ratelimiter"]
      interval: 5s
      timeout: 5s
      retries: 5

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

volumes:
  db-data:
  redis-data:
```

`depends_on` with `condition: service_healthy` ensures the API container waits until PostgreSQL and Redis pass their health checks before starting. `pg_isready` is a PostgreSQL utility that checks if the server is accepting connections. `--appendonly yes` enables Redis AOF (Append-Only File) persistence. `--maxmemory 256mb` caps Redis memory usage. `--maxmemory-policy allkeys-lru` evicts the least recently used keys when memory is full, which is safe for rate limiting because evicting a key is equivalent to resetting a client's counter (they temporarily get a fresh window). `redis:7-alpine` is a minimal Redis image. `restart: unless-stopped` automatically restarts the container unless you explicitly stop it.

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.middleware import RateLimiterMiddleware
from app.routers import limiter, analytics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Distributed Rate Limiter API",
    description="A configurable, multi-algorithm distributed rate limiter",
    version="1.0.0",
    lifespan=lifespan,
)

# Register the rate limiter middleware
app.add_middleware(RateLimiterMiddleware)

app.include_router(limiter.router, prefix="/api/limiter", tags=["Rate Limiter"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])


@app.get("/health")
def health_check():
    from app.redis_client import is_redis_healthy
    return {
        "status": "healthy",
        "redis": "connected" if is_redis_healthy() else "disconnected",
    }


@app.get("/api/echo")
def echo():
    """A sample endpoint to test rate limiting against."""
    return {"message": "Hello! This endpoint is rate-limited."}


@app.post("/api/heavy-endpoint")
def heavy_endpoint():
    """A sample expensive endpoint with stricter rate limits."""
    return {"message": "This is an expensive operation."}
```

`lifespan` is an async context manager that runs setup code before the app starts accepting requests (`Base.metadata.create_all` creates missing tables) and teardown code after shutdown. `app.add_middleware(RateLimiterMiddleware)` registers the rate limiter so every request passes through it. The health check includes Redis connectivity status so monitoring can detect when rate limiting is operating in fail-open mode. `echo` and `heavy-endpoint` are sample endpoints to test rate limiting.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the rate limiter:

```bash
# Test basic rate limiting (send 5 rapid requests)
for i in {1..5}; do
  curl -s -o /dev/null -w "Request $i: HTTP %{http_code}\n" http://localhost:8000/api/echo
done

# Check response headers
curl -v http://localhost:8000/api/echo 2>&1 | grep -i "x-ratelimit"

# Check rate limit status
curl http://localhost:8000/api/limiter/status/ip:127.0.0.1

# Get analytics summary
curl http://localhost:8000/api/analytics/summary

# Create a custom rule
curl -X POST http://localhost:8000/api/limiter/rules \
  -H "Content-Type: application/json" \
  -d '{
    "name": "strict-api-limit",
    "route_pattern": "/api/heavy-endpoint",
    "tier": "free",
    "algorithm": "leaky_bucket",
    "limit": 5,
    "window_seconds": 60,
    "burst_size": 0
  }'

# Reset a client's counters
curl -X POST http://localhost:8000/api/limiter/reset/ip:192.168.1.1
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

### `tests/test_token_bucket.py`

```python
import time
from unittest.mock import patch, MagicMock

from app.algorithms.token_bucket import TokenBucket
from app.algorithms.base import RateLimitResult


def make_mock_eval(tokens_remaining, allowed=True):
    """Create a mock Redis eval that returns a TokenBucket result."""
    def mock_eval(*args, **kwargs):
        return [int(allowed), tokens_remaining, "10.0"]
    return mock_eval


def test_allows_request_within_limit():
    """Test that requests within the rate limit are allowed."""
    bucket = TokenBucket()

    with patch("app.algorithms.token_bucket.redis_client") as mock_redis:
        mock_redis.eval = make_mock_eval(tokens_remaining=99, allowed=True)

        result = bucket.check(key="test:user1", limit=100, window=60)

        assert result.allowed is True
        assert result.remaining == 99
        assert result.retry_after is None


def test_rejects_request_over_limit():
    """Test that requests exceeding the rate limit are rejected."""
    bucket = TokenBucket()

    with patch("app.algorithms.token_bucket.redis_client") as mock_redis:
        mock_redis.eval = make_mock_eval(tokens_remaining=0, allowed=False)

        result = bucket.check(key="test:user1", limit=100, window=60)

        assert result.allowed is False
        assert result.remaining == 0
        assert result.retry_after is not None
        assert result.retry_after > 0


def test_burst_allows_extra_requests():
    """Test that burst parameter allows extra tokens."""
    bucket = TokenBucket()

    with patch("app.algorithms.token_bucket.redis_client") as mock_redis:
        mock_redis.eval = make_mock_eval(tokens_remaining=119, allowed=True)

        result = bucket.check(key="test:user1", limit=100, window=60, burst=20)

        assert result.allowed is True
        assert result.remaining == 119        # 100 + 20 - 1


def test_reset_clears_state():
    """Test that reset removes the client's bucket."""
    bucket = TokenBucket()

    with patch("app.algorithms.token_bucket.redis_client") as mock_redis:
        bucket.reset("test:user1")
        mock_redis.delete.assert_called_once_with("tb:test:user1")
```

### `tests/test_sliding_window.py`

```python
from unittest.mock import patch

from app.algorithms.sliding_window_counter import SlidingWindowCounter


def test_allows_request_within_window():
    """Test sliding window counter allows requests within limit."""
    swc = SlidingWindowCounter()

    with patch("app.algorithms.sliding_window_counter.redis_client") as mock_redis:
        # weighted count = 30 (below limit of 100)
        mock_redis.eval.return_value = [1, 69, "45.0", "30.0"]

        result = swc.check(key="test:user1", limit=100, window=60)

        assert result.allowed is True
        assert result.remaining == 69


def test_rejects_when_weighted_count_exceeds_limit():
    """Test sliding window counter rejects when weighted count exceeds limit."""
    swc = SlidingWindowCounter()

    with patch("app.algorithms.sliding_window_counter.redis_client") as mock_redis:
        # weighted count = 105 (above limit of 100)
        mock_redis.eval.return_value = [0, 0, "15.0", "105.0"]

        result = swc.check(key="test:user1", limit=100, window=60)

        assert result.allowed is False
        assert result.remaining == 0
        assert result.retry_after == 15.0


def test_sliding_window_accuracy():
    """
    Test that sliding window counter correctly weights previous window.

    Scenario: 60-second window, 45 seconds into current window
    prev_count = 80, curr_count = 30
    overlap = 1 - 45/60 = 0.25
    weighted = 80 × 0.25 + 30 = 50 → allowed (50 < 100)
    """
    swc = SlidingWindowCounter()

    with patch("app.algorithms.sliding_window_counter.redis_client") as mock_redis:
        # Simulate: weighted = 50, remaining = 49
        mock_redis.eval.return_value = [1, 49, "15.0", "50.0"]

        result = swc.check(key="test:user1", limit=100, window=60)

        assert result.allowed is True
        assert result.remaining == 49
```

### `tests/test_fixed_window.py`

```python
from unittest.mock import patch

from app.algorithms.fixed_window import FixedWindowCounter


def test_allows_first_request():
    """Test that the first request in a window is always allowed."""
    fw = FixedWindowCounter()

    with patch("app.algorithms.fixed_window.redis_client") as mock_redis:
        mock_redis.eval.return_value = [1, 99, "60.0"]

        result = fw.check(key="test:user1", limit=100, window=60)

        assert result.allowed is True
        assert result.remaining == 99


def test_rejects_at_limit():
    """Test that requests at the limit boundary are rejected."""
    fw = FixedWindowCounter()

    with patch("app.algorithms.fixed_window.redis_client") as mock_redis:
        mock_redis.eval.return_value = [0, 0, "30.0"]

        result = fw.check(key="test:user1", limit=100, window=60)

        assert result.allowed is False
        assert result.remaining == 0
        assert result.retry_after == 30.0     # 30 seconds until window resets
```

### `tests/test_middleware.py`

```python
from unittest.mock import patch

from app.algorithms.base import RateLimitResult


def test_rate_limit_headers_present(client):
    """Test that rate limit headers are in every response."""
    with patch("app.middleware.is_redis_healthy", return_value=True), \
         patch("app.middleware.ALGORITHMS") as mock_algos:

        mock_algo = mock_algos.__getitem__.return_value
        mock_algo.check.return_value = RateLimitResult(
            allowed=True, limit=100, remaining=99,
            reset_after=60.0, retry_after=None,
        )

        response = client.get("/api/echo")

        assert "X-RateLimit-Limit" in response.headers
        assert "X-RateLimit-Remaining" in response.headers
        assert "X-RateLimit-Reset" in response.headers


def test_returns_429_when_limited(client):
    """Test that HTTP 429 is returned when rate limited."""
    with patch("app.middleware.is_redis_healthy", return_value=True), \
         patch("app.middleware.ALGORITHMS") as mock_algos:

        mock_algo = mock_algos.__getitem__.return_value
        mock_algo.check.return_value = RateLimitResult(
            allowed=False, limit=100, remaining=0,
            reset_after=30.0, retry_after=5.0,
        )

        response = client.get("/api/echo")

        assert response.status_code == 429
        assert "Retry-After" in response.headers
        assert response.headers["Retry-After"] == "5"


def test_fail_open_when_redis_down(client):
    """Test that requests are allowed when Redis is unreachable."""
    with patch("app.middleware.is_redis_healthy", return_value=False):
        response = client.get("/api/echo")

        assert response.status_code == 200
        assert response.headers.get("X-RateLimit-Status") == "fail-open"


def test_health_check_bypasses_rate_limit(client):
    """Test that /health is not rate limited."""
    response = client.get("/health")
    assert response.status_code == 200
    assert "X-RateLimit-Limit" not in response.headers
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_token"                        # Run only token bucket tests
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
gcloud sql instances create ratelimiter-db \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1 \
  --storage-size=10GB \
  --storage-auto-increase

# Create the database
gcloud sql databases create ratelimiter \
  --instance=ratelimiter-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=ratelimiter-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-f1-micro` is the smallest (cheapest) instance for development. In production, use `--tier=db-custom-2-7680` or higher. `--storage-auto-increase` automatically adds storage when the disk fills up. The PostgreSQL instance stores rate limit rules and analytics events, not the hot-path counters (those live in Redis).

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create ratelimiter-cache \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe ratelimiter-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=1` provisions 1 GB of memory, enough for 10 M client rate limit keys at ~100 bytes each. `--tier=STANDARD` enables replication with automatic failover, critical because a Redis outage causes the rate limiter to fail open (allowing all traffic). Memorystore is accessible only from within the same VPC network, which secures it without additional firewall rules.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry (recommended)
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/ratelimiter/api

# Or use Container Registry
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/ratelimiter
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build, Google's managed CI/CD service. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy to Cloud Run

```bash
gcloud run deploy ratelimiter \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/ratelimiter/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 1 \
  --max-instances 20 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:ratelimiter-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/ratelimiter?host=/cloudsql/YOUR_PROJECT_ID:us-central1:ratelimiter-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
DEFAULT_ALGORITHM=token_bucket,\
DEFAULT_RATE=100,\
DEFAULT_WINDOW_SECONDS=60,\
DEFAULT_BURST_SIZE=20,\
FAIL_OPEN=true,\
ENABLE_ANALYTICS=true"
```

`--min-instances 1` keeps at least one instance warm to avoid cold starts. `--max-instances 20` caps the number of instances to control costs. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy, which creates a Unix socket at `/cloudsql/...` for secure, IAM-based database connections without SSL certificates. `--allow-unauthenticated` makes the service publicly accessible.

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis)
gcloud compute networks vpc-access connectors create rl-vpc-connector \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update ratelimiter \
  --vpc-connector=rl-vpc-connector \
  --region=us-central1
```

Cloud Run services run in Google's serverless infrastructure, which is outside your VPC by default. A VPC connector creates a bridge so Cloud Run can reach private resources like Memorystore.

---

## Scaling & Production Considerations

### Redis Scaling

| Strategy               | When to Use                                | How                                              |
| ---------------------- | ------------------------------------------ | ------------------------------------------------ |
| Redis Cluster          | Single node can't handle 500 K ops/sec     | Memorystore cluster mode (auto-shards by key)    |
| Read Replicas          | Read-heavy analytics on Redis counters     | Memorystore Standard tier (built-in replica)     |
| Local Cache Layer      | Reduce Redis round trips per request       | In-memory LRU cache with short TTL (1-5 sec)    |
| Pipelining             | Analytics writes add latency               | Batch analytics writes every 100 ms              |
| Key Sharding           | Hot keys (popular API users) cause hotspots| Prefix keys with shard ID based on hash          |

### Accuracy vs Performance Tradeoffs

| Approach                  | Accuracy | Latency    | Use Case                          |
| ------------------------- | -------- | ---------- | --------------------------------- |
| Single Redis (Lua atomic) | Exact    | 1-5 ms     | Default for most services         |
| Local + Redis sync        | ~95%     | 0.1 ms     | Ultra-low latency APIs            |
| Local only (no Redis)     | Per-node | 0.01 ms    | Stateless CDN edge limiting       |
| Multi-region Redis        | Eventual | 5-50 ms    | Global API with regional limits   |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ api.xyz        │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud Armor    │
                     │ (L7 DDoS +    │
                     │  WAF + basic  │
                     │  rate limit)  │
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
       │ Instance 1  │ │Run #2  │ │ Instance N  │
       │             │ │        │ │             │
       │ ┌─────────┐ │ │        │ │ ┌─────────┐ │
       │ │ Local   │ │ │        │ │ │ Local   │ │
       │ │ LRU     │ │ │        │ │ │ LRU     │ │
       │ │ Cache   │ │ │        │ │ │ Cache   │ │
       │ └────┬────┘ │ │        │ │ └────┬────┘ │
       └──────┼──────┘ └───┬────┘ └──────┼──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │            VPC Network              │
         │                                      │
         │  ┌──────────────────────────────┐   │
         │  │  Memorystore Redis Cluster   │   │
         │  │  (Primary + Replica)         │   │
         │  │                              │   │
         │  │  Shard 1    Shard 2    ...   │   │
         │  │  [a-m keys] [n-z keys]       │   │
         │  └──────────────────────────────┘   │
         │                                      │
         │  ┌──────────┐    ┌────────────────┐ │
         │  │ Cloud    │    │  Cloud SQL     │ │
         │  │ Pub/Sub  │    │  (Rules +      │ │
         │  │ (async   │    │   Analytics)   │ │
         │  │  events) │    │               │ │
         │  └──────────┘    └────────────────┘ │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud Armor** sits in front of the load balancer and provides L7 DDoS protection, web application firewall (WAF), and basic IP-based rate limiting as the first line of defense. This catches obvious abuse before it reaches your API servers.
- **Local LRU Cache** on each Cloud Run instance caches rate limit decisions for 1-5 seconds. For a client sending 100 requests/second, only 1 request/second actually hits Redis. The trade-off is ~5% reduced accuracy, which is acceptable for most use cases.
- **Redis Cluster** auto-shards rate limit keys across multiple nodes. Keys are distributed by hash slot, so `ip:192.168.1.1` and `ip:10.0.0.1` may land on different shards, preventing hotspots.
- **Cloud Pub/Sub** decouples analytics event recording from the hot path. Instead of writing to Redis and PostgreSQL synchronously, the middleware publishes an event and moves on. A separate consumer batch-writes to storage.

### Security Checklist

```
✓ Validate all client identifiers (reject malformed API keys, IPs)
✓ Use X-Forwarded-For with caution (can be spoofed by clients)
✓ Implement rate limiting on the rate limiter management API itself
✓ Store API keys hashed (SHA-256), never in plaintext
✓ Use HTTPS everywhere (Cloud Run provides this by default)
✓ Store secrets in Secret Manager, not environment variables
✓ Enable Cloud Audit Logs for compliance
✓ Monitor for rate limit bypass attempts (distributed IPs, key rotation)
✓ Implement graduated penalties (warn → soft limit → hard block)
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create ratelimiter-health \
  --display-name="Rate Limiter Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Redis disconnection (fail-open mode active)
# - Rejection rate > 20% globally (possible misconfiguration or attack)
# - p99 rate check latency > 10 ms (Redis overloaded)
# - Single client rejection rate > 95% (legitimate user blocked)
# - Redis memory usage > 80%
# - Cloud SQL CPU > 80% (analytics writes too heavy)
# - Cloud Run instance count near max-instances
# - Error rate > 1% over 5 minutes
```

Cloud Run automatically exports metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. The most critical alert is Redis disconnection, because it means all rate limiting is disabled (fail-open). Use Cloud Trace for distributed tracing to measure the actual latency overhead of rate limit checks on every request. A well-tuned rate limiter should add less than 2 ms to the p99 latency of your API.
