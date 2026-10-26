# Design Distributed Cache

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Cache Engine & Eviction Policies](#cache-engine--eviction-policies)
5. [Core API – Get, Put, Delete, TTL](#core-api--get-put-delete-ttl)
6. [Consistent Hashing & Partitioning](#consistent-hashing--partitioning)
7. [Cache Invalidation Strategies](#cache-invalidation-strategies)
8. [Replication & Failover](#replication--failover)
9. [Cache Warming & Preloading](#cache-warming--preloading)
10. [Monitoring & Cache Metrics](#monitoring--cache-metrics)
11. [Docker & Docker Compose](#docker--docker-compose)
12. [Testing](#testing)
13. [Deploying to GCP (Cloud Run + Memorystore)](#deploying-to-gcp-cloud-run--memorystore)
14. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement       | Description                                                          |
| ----------------- | -------------------------------------------------------------------- |
| Get(key)          | Retrieve a cached value by key, return miss if not present           |
| Put(key, value)   | Store a key-value pair with an optional TTL                          |
| Delete(key)       | Explicitly remove a key from the cache                               |
| TTL               | Keys auto-expire after a configurable time-to-live                   |
| Eviction          | When memory is full, evict keys using a configurable policy (LRU, LFU, FIFO) |
| Batch Ops         | Support multi-key get and put in a single request                    |
| Cache Invalidation| Invalidate keys across all nodes when the origin data changes        |

### Non-Functional Requirements

| Requirement        | Target                                                        |
| ------------------ | ------------------------------------------------------------- |
| Latency            | < 1 ms (local), < 5 ms (remote), < 50 ms (cache miss + origin fetch) |
| Availability       | 99.99% uptime (cache unavailability degrades but does not break the system) |
| Scalability        | Handle 500K reads/sec and 100K writes/sec across the cluster  |
| Memory Efficiency  | Maximize hit ratio with bounded memory (no unbounded growth)  |
| Consistency        | Eventual consistency across cache nodes (stale reads are tolerable) |
| Partition Tolerance| Continue serving cached data when nodes fail                  |

### Back-of-the-Envelope Estimations

```
Average key size               ≈ 128 bytes
Average value size             ≈ 2 KB
Average record size            ≈ 2.2 KB (key + value + metadata)
Hot dataset (actively cached)  ≈ 50 million keys
Memory per node                ≈ 50 M × 2.2 KB ÷ 5 nodes = ~22 GB per node
Total cluster memory           ≈ 110 GB (5 nodes × 22 GB)
QPS reads                      ≈ 500 000 / sec
QPS writes                     ≈ 100 000 / sec
Cache hit ratio target         ≈ 95% (industry standard for warm cache)
Evictions per second           ≈ ~5 000 (under steady state)
Network bandwidth per node     ≈ ~2 Gbps (at peak read QPS)
```

A 5-node cluster with 22 GB per node can hold roughly 50 million cached records. With a 95% hit ratio, 95% of reads are served directly from memory without touching the origin database. Network bandwidth at peak load is around 2 Gbps per node, well within modern NIC capacity. Evictions at steady state should be a small fraction of total QPS, indicating the working set fits in memory.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │ (App Servers)│
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │       Cache Gateway         │
                │     (FastAPI Coordinator)    │
                │                              │
                │  GET /cache/{key}  → lookup  │
                │  PUT /cache/{key}  → store   │
                │  DELETE /cache/{key} → evict │
                └──────┬──────────────┬────────┘
                       │              │
          ┌────────────▼──┐    ┌──────▼────────┐
          │  Hash Ring     │    │  Origin DB    │
          │ (Consistent    │    │ (PostgreSQL)  │
          │  Hashing)      │    │               │
          └───┬────┬───┬───┘    └───────────────┘
              │    │   │
        ┌─────▼┐ ┌▼───▼┐ ┌──────┐
        │Shard │ │Shard │ │Shard │
        │  A   │ │  B   │ │  C   │
        │(LRU  │ │(LRU  │ │(LRU  │
        │Cache)│ │Cache)│ │Cache)│
        └──────┘ └──────┘ └──────┘
```

The client sends a request to the cache gateway (FastAPI coordinator). The coordinator hashes the key to determine which cache shard owns it, then routes the request to that node. On a cache hit, the shard returns the value directly from memory in under 1 ms. On a cache miss, the gateway fetches the value from the origin database (PostgreSQL), stores it in the appropriate shard, and returns it to the client. This is the **cache-aside** pattern, also known as lazy loading. Each shard runs an LRU (or configurable) eviction policy to stay within its memory budget.

---

## Project Setup

### Project Structure

```
distributed-cache/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models (origin data)
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # PostgreSQL connection and session
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── engine.py         # Core cache engine with eviction
│   │   ├── lru.py            # LRU eviction policy
│   │   ├── lfu.py            # LFU eviction policy
│   │   ├── ttl.py            # TTL expiration manager
│   │   └── metrics.py        # Cache hit/miss/eviction counters
│   ├── partitioning/
│   │   ├── __init__.py
│   │   ├── consistent_hash.py # Consistent hashing ring
│   │   └── replication.py    # Replication and failover
│   ├── invalidation/
│   │   ├── __init__.py
│   │   ├── pubsub.py         # Pub/Sub based invalidation
│   │   └── strategies.py     # Write-through, write-behind, write-around
│   └── routers/
│       ├── __init__.py
│       ├── cache_routes.py   # Cache CRUD endpoints
│       ├── admin.py          # Cluster admin endpoints
│       └── warmup.py         # Cache warming endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_lru.py           # Tests for LRU eviction
│   ├── test_lfu.py           # Tests for LFU eviction
│   ├── test_api.py           # Tests for API endpoints
│   └── test_hashing.py       # Tests for consistent hashing
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir distributed-cache && cd distributed-cache
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
xxhash==3.5.0
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for the origin PostgreSQL database that the cache sits in front of. `psycopg2-binary` is the PostgreSQL driver. `redis` is used as a Pub/Sub transport for cross-node cache invalidation. `xxhash` is a fast non-cryptographic hash used for consistent hashing and key partitioning. `pydantic-settings` handles environment variable loading.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/appdb
REDIS_URL=redis://localhost:6379/0
NODE_ID=cache-1
MAX_MEMORY_MB=512
EVICTION_POLICY=lru
DEFAULT_TTL_SECONDS=300
MAX_KEY_SIZE=256
MAX_VALUE_SIZE=65536
PORT=8000
```

`DATABASE_URL` points to the origin database that the cache sits in front of. `REDIS_URL` is used for Pub/Sub invalidation messages between cache nodes, not for actual caching (the cache is our own implementation). `NODE_ID` uniquely identifies this cache node in the cluster. `MAX_MEMORY_MB=512` caps this node's in-memory cache at 512 MB; once reached, eviction kicks in. `EVICTION_POLICY=lru` selects Least Recently Used as the default policy. `DEFAULT_TTL_SECONDS=300` sets keys to expire after 5 minutes if no explicit TTL is provided.

---

## Cache Engine & Eviction Policies

The heart of a distributed cache is the eviction policy. When memory is full and a new entry arrives, the cache must decide which existing entry to remove. Different policies optimize for different access patterns.

### Policy Comparison

| Policy | How It Works                                  | Best For                                | Weakness                               |
| ------ | --------------------------------------------- | --------------------------------------- | -------------------------------------- |
| LRU    | Evict the least recently used entry           | Temporal locality (recent = likely hot) | Scan pollution (one-time access floods)|
| LFU    | Evict the least frequently used entry         | Frequency-based access patterns         | Slow to adapt to changing popularity   |
| FIFO   | Evict the oldest entry regardless of access   | Simple workloads, append-heavy          | Ignores access patterns entirely       |
| TTL    | Evict entries that have expired               | Time-sensitive data (sessions, tokens)  | Does not free memory on demand         |
| Random | Evict a random entry                          | Uniform access patterns                 | Unpredictable behavior                 |

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/appdb"
    redis_url: str = "redis://localhost:6379/0"
    node_id: str = "cache-1"
    max_memory_mb: int = 512
    eviction_policy: str = "lru"                  # lru, lfu, fifo
    default_ttl_seconds: int = 300
    max_key_size: int = 256
    max_value_size: int = 65536
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

`pool_size=20` keeps 20 connections open and reusable. The origin database is queried only on cache misses, so even high QPS translates to relatively few database queries (5% miss rate at 500K QPS = 25K DB queries/sec). `pool_pre_ping=True` detects stale connections from cloud databases that close idle connections.

### `app/cache/lru.py`

```python
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field


@dataclass
class CacheEntry:
    """A single cache entry with metadata."""
    key: str
    value: str
    size_bytes: int
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: int | None = None

    @property
    def expires_at(self) -> float | None:
        if self.ttl_seconds is not None:
            return self.created_at + self.ttl_seconds
        return None

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


class LRUCache:
    """
    Least Recently Used cache using OrderedDict.

    OrderedDict maintains insertion order. On every access, the entry
    is moved to the end (most recently used). On eviction, the entry
    at the front (least recently used) is removed.

    Thread-safe via a reentrant lock.
    """

    def __init__(self, max_memory_bytes: int):
        self._data: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_memory = max_memory_bytes
        self._current_memory = 0
        self._lock = threading.RLock()

        # Metrics
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._total_writes = 0

    def get(self, key: str) -> CacheEntry | None:
        """
        Retrieve an entry by key. On hit, moves the entry to the end
        of the OrderedDict (marking it as most recently used).
        Returns None on miss or if the entry has expired.
        """
        with self._lock:
            if key not in self._data:
                self._misses += 1
                return None

            entry = self._data[key]

            # Check expiration
            if entry.is_expired:
                self._remove(key)
                self._misses += 1
                return None

            # Move to end (most recently used)
            self._data.move_to_end(key)
            entry.last_accessed = time.time()
            entry.access_count += 1
            self._hits += 1
            return entry

    def put(self, key: str, value: str, ttl_seconds: int | None = None) -> CacheEntry:
        """
        Store a key-value pair. If the key already exists, update it.
        If memory is full, evict the least recently used entries until
        there is enough room.
        """
        with self._lock:
            entry_size = len(key.encode()) + len(value.encode()) + 128  # metadata

            # Remove existing entry first (to reclaim its memory)
            if key in self._data:
                self._remove(key)

            # Evict until there is room
            while (
                self._current_memory + entry_size > self._max_memory
                and self._data
            ):
                self._evict_one()

            entry = CacheEntry(
                key=key,
                value=value,
                size_bytes=entry_size,
                ttl_seconds=ttl_seconds,
            )

            self._data[key] = entry
            self._data.move_to_end(key)           # Mark as most recently used
            self._current_memory += entry_size
            self._total_writes += 1
            return entry

    def delete(self, key: str) -> bool:
        """Explicitly remove a key from the cache."""
        with self._lock:
            if key not in self._data:
                return False
            self._remove(key)
            return True

    def _remove(self, key: str):
        """Internal: remove a key and reclaim its memory."""
        if key in self._data:
            entry = self._data.pop(key)
            self._current_memory -= entry.size_bytes

    def _evict_one(self):
        """Evict the least recently used entry (front of OrderedDict)."""
        if not self._data:
            return

        # popitem(last=False) removes the first (oldest/least recent) item
        key, entry = self._data.popitem(last=False)
        self._current_memory -= entry.size_bytes
        self._evictions += 1

    def clear(self):
        """Remove all entries from the cache."""
        with self._lock:
            self._data.clear()
            self._current_memory = 0

    def keys(self) -> list[str]:
        """Return all keys in the cache."""
        with self._lock:
            return list(self._data.keys())

    @property
    def stats(self) -> dict:
        """Return cache statistics."""
        total_requests = self._hits + self._misses
        hit_ratio = (self._hits / total_requests * 100) if total_requests > 0 else 0.0

        return {
            "entries": len(self._data),
            "memory_used_bytes": self._current_memory,
            "memory_max_bytes": self._max_memory,
            "memory_usage_pct": round(
                self._current_memory / self._max_memory * 100, 2
            ),
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio_pct": round(hit_ratio, 2),
            "evictions": self._evictions,
            "total_writes": self._total_writes,
        }
```

`OrderedDict` from Python's `collections` module maintains entries in insertion/access order. `move_to_end(key)` moves the accessed key to the tail (most recently used). `popitem(last=False)` removes the head (least recently used), which is the eviction target. This gives O(1) for all three operations: get, put, and evict. The 128-byte overhead per entry accounts for Python object headers, dictionary pointers, and the `CacheEntry` fields. `threading.RLock()` is a reentrant lock, meaning the same thread can acquire it multiple times without deadlocking (important because `put` calls `_remove` and `_evict_one` while already holding the lock). Memory tracking is approximate but sufficient: the key and value byte lengths plus fixed overhead gives a close-enough estimate without the complexity of `sys.getsizeof` recursion.

### `app/cache/lfu.py`

```python
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field

from app.cache.lru import CacheEntry


class LFUCache:
    """
    Least Frequently Used cache.

    Tracks the access frequency of each key. On eviction, the key
    with the lowest frequency is removed. Ties are broken by removing
    the least recently used key among the lowest-frequency entries.

    Implementation uses a frequency → keys mapping and a min-frequency
    tracker for O(1) eviction.
    """

    def __init__(self, max_memory_bytes: int):
        self._data: dict[str, CacheEntry] = {}
        self._freq: dict[str, int] = {}           # key → access frequency
        self._freq_to_keys: dict[int, dict] = defaultdict(dict)  # freq → {key: True}
        self._min_freq = 0
        self._max_memory = max_memory_bytes
        self._current_memory = 0
        self._lock = threading.RLock()

        # Metrics
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._total_writes = 0

    def _increment_freq(self, key: str):
        """Move a key to the next frequency bucket."""
        freq = self._freq[key]
        self._freq_to_keys[freq].pop(key, None)

        # If we emptied the min-frequency bucket, bump min_freq
        if not self._freq_to_keys[freq] and freq == self._min_freq:
            self._min_freq += 1

        self._freq[key] = freq + 1
        self._freq_to_keys[freq + 1][key] = True

    def get(self, key: str) -> CacheEntry | None:
        """
        Retrieve an entry. On hit, increment the key's frequency counter
        and move it to the next frequency bucket.
        """
        with self._lock:
            if key not in self._data:
                self._misses += 1
                return None

            entry = self._data[key]

            if entry.is_expired:
                self._remove(key)
                self._misses += 1
                return None

            self._increment_freq(key)
            entry.last_accessed = time.time()
            entry.access_count += 1
            self._hits += 1
            return entry

    def put(self, key: str, value: str, ttl_seconds: int | None = None) -> CacheEntry:
        """
        Store a key-value pair. New entries start at frequency 1.
        On memory pressure, evict the least frequently used key.
        """
        with self._lock:
            entry_size = len(key.encode()) + len(value.encode()) + 128

            if key in self._data:
                self._remove(key)

            while (
                self._current_memory + entry_size > self._max_memory
                and self._data
            ):
                self._evict_one()

            entry = CacheEntry(
                key=key,
                value=value,
                size_bytes=entry_size,
                ttl_seconds=ttl_seconds,
            )

            self._data[key] = entry
            self._freq[key] = 1
            self._freq_to_keys[1][key] = True
            self._min_freq = 1                    # New entry always has freq 1
            self._current_memory += entry_size
            self._total_writes += 1
            return entry

    def delete(self, key: str) -> bool:
        """Explicitly remove a key from the cache."""
        with self._lock:
            if key not in self._data:
                return False
            self._remove(key)
            return True

    def _remove(self, key: str):
        """Internal: remove a key from all data structures."""
        if key in self._data:
            entry = self._data.pop(key)
            freq = self._freq.pop(key, 0)
            self._freq_to_keys[freq].pop(key, None)
            self._current_memory -= entry.size_bytes

    def _evict_one(self):
        """Evict the key with the lowest frequency."""
        if not self._data:
            return

        # Get a key from the min-frequency bucket
        min_bucket = self._freq_to_keys[self._min_freq]
        if not min_bucket:
            return

        # Pop the first key (oldest in the bucket, breaking ties by insertion order)
        evict_key = next(iter(min_bucket))
        self._remove(evict_key)
        self._evictions += 1

    def clear(self):
        """Remove all entries from the cache."""
        with self._lock:
            self._data.clear()
            self._freq.clear()
            self._freq_to_keys.clear()
            self._current_memory = 0
            self._min_freq = 0

    def keys(self) -> list[str]:
        """Return all keys in the cache."""
        with self._lock:
            return list(self._data.keys())

    @property
    def stats(self) -> dict:
        """Return cache statistics."""
        total_requests = self._hits + self._misses
        hit_ratio = (self._hits / total_requests * 100) if total_requests > 0 else 0.0

        return {
            "entries": len(self._data),
            "memory_used_bytes": self._current_memory,
            "memory_max_bytes": self._max_memory,
            "memory_usage_pct": round(
                self._current_memory / self._max_memory * 100, 2
            ),
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio_pct": round(hit_ratio, 2),
            "evictions": self._evictions,
            "total_writes": self._total_writes,
        }
```

LFU maintains a frequency counter per key and a reverse mapping from frequency → set of keys. `_min_freq` tracks the lowest frequency in the cache, enabling O(1) eviction. When a key is accessed, `_increment_freq` moves it from frequency `f` to `f+1`. If the old frequency bucket becomes empty and it was the minimum, `_min_freq` is incremented. New entries always start at frequency 1, so `_min_freq` is reset to 1 on every insert. Ties within the same frequency bucket are broken by insertion order (first-in is evicted first), making this an LFU-with-LRU-tiebreaker variant. This is the same approach Redis uses internally for its `allkeys-lfu` eviction policy.

### `app/cache/engine.py`

```python
from app.cache.lru import LRUCache, CacheEntry
from app.cache.lfu import LFUCache
from app.config import settings


class CacheEngine:
    """
    Cache engine that delegates to the configured eviction policy.
    Acts as a factory and unified interface for LRU, LFU, and FIFO caches.
    """

    def __init__(self):
        max_memory = settings.max_memory_mb * 1024 * 1024
        policy = settings.eviction_policy.lower()

        if policy == "lru":
            self._cache = LRUCache(max_memory_bytes=max_memory)
        elif policy == "lfu":
            self._cache = LFUCache(max_memory_bytes=max_memory)
        else:
            # Default to LRU
            self._cache = LRUCache(max_memory_bytes=max_memory)

        self._policy_name = policy

    def get(self, key: str) -> CacheEntry | None:
        """Retrieve a cached entry."""
        return self._cache.get(key)

    def put(self, key: str, value: str, ttl_seconds: int | None = None) -> CacheEntry:
        """Store a key-value pair in the cache."""
        ttl = ttl_seconds if ttl_seconds is not None else settings.default_ttl_seconds
        return self._cache.put(key, value, ttl_seconds=ttl)

    def delete(self, key: str) -> bool:
        """Remove a key from the cache."""
        return self._cache.delete(key)

    def clear(self) -> None:
        """Flush the entire cache."""
        self._cache.clear()

    def keys(self) -> list[str]:
        """Return all cached keys."""
        return self._cache.keys()

    def batch_get(self, keys: list[str]) -> dict[str, CacheEntry | None]:
        """Retrieve multiple keys in a single call."""
        return {key: self._cache.get(key) for key in keys}

    def batch_put(self, items: list[dict]) -> list[CacheEntry]:
        """Store multiple key-value pairs in a single call."""
        results = []
        for item in items:
            entry = self.put(
                key=item["key"],
                value=item["value"],
                ttl_seconds=item.get("ttl_seconds"),
            )
            results.append(entry)
        return results

    @property
    def stats(self) -> dict:
        """Return cache statistics including policy name."""
        base_stats = self._cache.stats
        base_stats["eviction_policy"] = self._policy_name
        base_stats["node_id"] = settings.node_id
        return base_stats


# Global cache engine instance
cache_engine = CacheEngine()
```

`CacheEngine` is a facade that selects the underlying eviction policy based on the `EVICTION_POLICY` environment variable. This decouples the API layer from the eviction implementation, letting you switch policies by changing a single config value without touching any route code. `batch_get` and `batch_put` reduce network round trips when clients need to operate on multiple keys. The default TTL is applied when no explicit TTL is provided, ensuring every cached entry eventually expires and does not go stale indefinitely.

### `app/cache/ttl.py`

```python
import threading
import time

from app.cache.engine import cache_engine


class TTLManager:
    """
    Background daemon that periodically scans for and removes expired entries.

    While expired entries are also caught lazily on read (passive expiration),
    active expiration prevents memory waste from entries that are written
    but never read again.
    """

    def __init__(self, scan_interval_seconds: int = 30, sample_size: int = 100):
        self._interval = scan_interval_seconds
        self._sample_size = sample_size
        self._running = False
        self._thread: threading.Thread | None = None
        self._total_expired = 0

    def start(self):
        """Start the background TTL expiration thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background TTL expiration thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        """Periodically scan for expired entries."""
        while self._running:
            try:
                expired = self._scan_and_expire()
                if expired > 0:
                    self._total_expired += expired
            except Exception as e:
                print(f"TTL scan error: {e}")
            time.sleep(self._interval)

    def _scan_and_expire(self) -> int:
        """
        Sample random keys and expire any that have passed their TTL.
        Uses probabilistic sampling (like Redis) instead of full scans
        to keep the operation lightweight.
        """
        keys = cache_engine.keys()
        if not keys:
            return 0

        import random
        sample = random.sample(keys, min(self._sample_size, len(keys)))
        expired_count = 0

        for key in sample:
            entry = cache_engine._cache._data.get(key)
            if entry and entry.is_expired:
                cache_engine.delete(key)
                expired_count += 1

        # If more than 25% of the sample was expired, run again immediately
        # (same heuristic Redis uses)
        if expired_count > len(sample) * 0.25:
            return expired_count + self._scan_and_expire()

        return expired_count

    @property
    def stats(self) -> dict:
        return {"total_expired": self._total_expired}


ttl_manager = TTLManager(scan_interval_seconds=30, sample_size=100)
```

Active expiration uses **probabilistic sampling**, the same approach Redis uses. Instead of scanning all keys (which would be O(n) and block the cache), it samples 100 random keys every 30 seconds and removes any that are expired. If more than 25% of the sample is expired, it runs another sample immediately, continuing until the expired fraction drops below 25%. This adaptive behavior ensures that a burst of expirations is handled quickly while keeping steady-state CPU usage minimal. The daemon thread is marked `daemon=True` so it does not prevent the process from exiting.

### `app/cache/metrics.py`

```python
import time
import threading
from dataclasses import dataclass, field


@dataclass
class CacheMetrics:
    """Centralized cache metrics collector."""
    node_id: str = ""
    start_time: float = field(default_factory=time.time)

    # Request counters
    total_requests: int = 0
    total_hits: int = 0
    total_misses: int = 0
    total_writes: int = 0
    total_deletes: int = 0
    total_evictions: int = 0

    # Latency tracking (in milliseconds)
    _read_latencies: list = field(default_factory=list)
    _write_latencies: list = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_hit(self, latency_ms: float):
        """Record a cache hit with its latency."""
        self.total_requests += 1
        self.total_hits += 1
        with self._lock:
            self._read_latencies.append(latency_ms)
            # Keep only the last 10000 latencies to bound memory
            if len(self._read_latencies) > 10000:
                self._read_latencies = self._read_latencies[-5000:]

    def record_miss(self, latency_ms: float):
        """Record a cache miss with its latency."""
        self.total_requests += 1
        self.total_misses += 1
        with self._lock:
            self._read_latencies.append(latency_ms)
            if len(self._read_latencies) > 10000:
                self._read_latencies = self._read_latencies[-5000:]

    def record_write(self, latency_ms: float):
        """Record a cache write with its latency."""
        self.total_writes += 1
        with self._lock:
            self._write_latencies.append(latency_ms)
            if len(self._write_latencies) > 10000:
                self._write_latencies = self._write_latencies[-5000:]

    def _percentile(self, data: list, p: float) -> float:
        """Calculate the p-th percentile of a sorted list."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * p / 100)
        idx = min(idx, len(sorted_data) - 1)
        return round(sorted_data[idx], 3)

    @property
    def summary(self) -> dict:
        """Return a summary of all metrics."""
        uptime = time.time() - self.start_time
        hit_ratio = (
            self.total_hits / self.total_requests * 100
            if self.total_requests > 0 else 0.0
        )
        qps = self.total_requests / uptime if uptime > 0 else 0

        with self._lock:
            read_lats = list(self._read_latencies)
            write_lats = list(self._write_latencies)

        return {
            "node_id": self.node_id,
            "uptime_seconds": round(uptime, 1),
            "total_requests": self.total_requests,
            "total_hits": self.total_hits,
            "total_misses": self.total_misses,
            "hit_ratio_pct": round(hit_ratio, 2),
            "qps": round(qps, 1),
            "read_latency_p50_ms": self._percentile(read_lats, 50),
            "read_latency_p95_ms": self._percentile(read_lats, 95),
            "read_latency_p99_ms": self._percentile(read_lats, 99),
            "write_latency_p50_ms": self._percentile(write_lats, 50),
            "write_latency_p95_ms": self._percentile(write_lats, 95),
            "write_latency_p99_ms": self._percentile(write_lats, 99),
            "total_writes": self.total_writes,
            "total_deletes": self.total_deletes,
        }


metrics = CacheMetrics()
```

`CacheMetrics` tracks hits, misses, writes, deletes, and evictions along with read/write latency percentiles. Latency lists are capped at 10,000 entries and halved when full to bound memory usage while preserving recent data. Percentile calculations sort the data and index into it, giving p50 (median), p95, and p99 latencies. QPS is calculated as total requests divided by uptime. In production, you would export these metrics to Cloud Monitoring or Prometheus instead of computing them in-process.

---

## Core API – Get, Put, Delete, TTL

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, Field


class CachePut(BaseModel):
    value: str                                    # Value to cache
    ttl_seconds: int | None = None                # Optional TTL in seconds


class CacheResponse(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None
    created_at: datetime
    expires_at: datetime | None = None
    source: str = "cache"                         # "cache" or "origin"


class CacheDeleteResponse(BaseModel):
    key: str
    deleted: bool
    message: str


class BatchPutRequest(BaseModel):
    items: list[dict] = Field(
        ...,
        description="List of {key, value, ttl_seconds} objects",
        max_length=100,
    )


class BatchGetRequest(BaseModel):
    keys: list[str] = Field(
        ...,
        description="List of keys to retrieve",
        max_length=100,
    )


class BatchGetResponse(BaseModel):
    results: dict[str, CacheResponse | None]
    hits: int
    misses: int


class CacheStats(BaseModel):
    entries: int
    memory_used_bytes: int
    memory_max_bytes: int
    memory_usage_pct: float
    hits: int
    misses: int
    hit_ratio_pct: float
    evictions: int
    total_writes: int
    eviction_policy: str
    node_id: str


class InvalidateRequest(BaseModel):
    keys: list[str] = Field(
        ...,
        description="Keys to invalidate across all cache nodes",
        max_length=500,
    )
    reason: str = "manual"                        # manual, db_update, deploy
```

`CacheResponse` includes a `source` field (`"cache"` or `"origin"`) so the caller knows whether the response came from the cache or required a database lookup. `InvalidateRequest` accepts up to 500 keys per request and a `reason` field for audit logging. The `BatchGetResponse` separates `hits` and `misses` counts so clients can monitor cache effectiveness per-batch.

### `app/models.py`

```python
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Column, DateTime, String, Text
)

from app.database import Base


class OriginData(Base):
    """
    Simulated origin database table.
    In a real system, this would be your actual application tables
    (users, products, sessions, etc.) that the cache sits in front of.
    """
    __tablename__ = "origin_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    key = Column(String(256), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
```

`OriginData` simulates the backing database that the cache sits in front of. In a real system, this would be your user profiles, product catalog, session data, or any table with read-heavy access patterns. The `updated_at` column is used to detect stale cache entries.

### `app/routers/cache_routes.py`

```python
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.cache.engine import cache_engine
from app.cache.metrics import metrics
from app.config import settings
from app.database import get_db
from app.models import OriginData
from app.schemas import (
    CachePut, CacheResponse, CacheDeleteResponse,
    BatchPutRequest, BatchGetRequest, BatchGetResponse,
)

router = APIRouter()


@router.get("/{key}", response_model=CacheResponse)
def get_cached(key: str, db: Session = Depends(get_db)):
    """
    Retrieve a value by key. Implements cache-aside pattern:
    1. Check cache → return on hit
    2. On miss → query origin DB → populate cache → return
    """
    start = time.monotonic()

    # 1. Check cache
    entry = cache_engine.get(key)
    if entry is not None:
        latency = (time.monotonic() - start) * 1000
        metrics.record_hit(latency)

        return CacheResponse(
            key=entry.key,
            value=entry.value,
            ttl_seconds=entry.ttl_seconds,
            created_at=datetime.fromtimestamp(entry.created_at, tz=timezone.utc),
            expires_at=(
                datetime.fromtimestamp(entry.expires_at, tz=timezone.utc)
                if entry.expires_at else None
            ),
            source="cache",
        )

    # 2. Cache miss → fetch from origin
    origin = db.query(OriginData).filter(OriginData.key == key).first()

    if origin is None:
        latency = (time.monotonic() - start) * 1000
        metrics.record_miss(latency)
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found.")

    # 3. Populate cache
    cached = cache_engine.put(key, origin.value)
    latency = (time.monotonic() - start) * 1000
    metrics.record_miss(latency)

    return CacheResponse(
        key=key,
        value=origin.value,
        ttl_seconds=cached.ttl_seconds,
        created_at=datetime.fromtimestamp(cached.created_at, tz=timezone.utc),
        expires_at=(
            datetime.fromtimestamp(cached.expires_at, tz=timezone.utc)
            if cached.expires_at else None
        ),
        source="origin",
    )


@router.put("/{key}", response_model=CacheResponse, status_code=200)
def put_cached(key: str, payload: CachePut, db: Session = Depends(get_db)):
    """
    Store a value in the cache and optionally write through to the origin DB.
    Implements write-through pattern: write to cache AND origin atomically.
    """
    start = time.monotonic()

    if len(key) > settings.max_key_size:
        raise HTTPException(
            status_code=400,
            detail=f"Key exceeds {settings.max_key_size} character limit.",
        )

    if len(payload.value) > settings.max_value_size:
        raise HTTPException(
            status_code=400,
            detail=f"Value exceeds {settings.max_value_size // 1024} KB limit.",
        )

    # Write to cache
    entry = cache_engine.put(key, payload.value, ttl_seconds=payload.ttl_seconds)

    # Write-through to origin DB
    origin = db.query(OriginData).filter(OriginData.key == key).first()
    if origin:
        origin.value = payload.value
    else:
        origin = OriginData(key=key, value=payload.value)
        db.add(origin)
    db.commit()

    latency = (time.monotonic() - start) * 1000
    metrics.record_write(latency)

    return CacheResponse(
        key=entry.key,
        value=entry.value,
        ttl_seconds=entry.ttl_seconds,
        created_at=datetime.fromtimestamp(entry.created_at, tz=timezone.utc),
        expires_at=(
            datetime.fromtimestamp(entry.expires_at, tz=timezone.utc)
            if entry.expires_at else None
        ),
        source="cache",
    )


@router.delete("/{key}", response_model=CacheDeleteResponse)
def delete_cached(key: str, db: Session = Depends(get_db)):
    """Delete a key from both the cache and the origin DB."""
    start = time.monotonic()

    cache_deleted = cache_engine.delete(key)

    origin = db.query(OriginData).filter(OriginData.key == key).first()
    if origin:
        db.delete(origin)
        db.commit()

    if not cache_deleted and origin is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found.")

    latency = (time.monotonic() - start) * 1000
    metrics.total_deletes += 1

    return CacheDeleteResponse(
        key=key,
        deleted=True,
        message=f"Key '{key}' deleted from cache and origin.",
    )


@router.post("/batch/get", response_model=BatchGetResponse)
def batch_get(payload: BatchGetRequest, db: Session = Depends(get_db)):
    """Retrieve multiple keys. Fetches misses from origin and populates cache."""
    results = {}
    hits = 0
    misses = 0

    for key in payload.keys:
        entry = cache_engine.get(key)
        if entry is not None:
            results[key] = CacheResponse(
                key=entry.key,
                value=entry.value,
                ttl_seconds=entry.ttl_seconds,
                created_at=datetime.fromtimestamp(entry.created_at, tz=timezone.utc),
                expires_at=(
                    datetime.fromtimestamp(entry.expires_at, tz=timezone.utc)
                    if entry.expires_at else None
                ),
                source="cache",
            )
            hits += 1
        else:
            # Fetch from origin
            origin = db.query(OriginData).filter(OriginData.key == key).first()
            if origin:
                cached = cache_engine.put(key, origin.value)
                results[key] = CacheResponse(
                    key=key,
                    value=origin.value,
                    ttl_seconds=cached.ttl_seconds,
                    created_at=datetime.fromtimestamp(
                        cached.created_at, tz=timezone.utc
                    ),
                    expires_at=(
                        datetime.fromtimestamp(cached.expires_at, tz=timezone.utc)
                        if cached.expires_at else None
                    ),
                    source="origin",
                )
            else:
                results[key] = None
            misses += 1

    return BatchGetResponse(results=results, hits=hits, misses=misses)


@router.post("/batch/put", response_model=list[CacheResponse])
def batch_put(payload: BatchPutRequest):
    """Store multiple key-value pairs in the cache."""
    entries = cache_engine.batch_put(payload.items)

    return [
        CacheResponse(
            key=e.key,
            value=e.value,
            ttl_seconds=e.ttl_seconds,
            created_at=datetime.fromtimestamp(e.created_at, tz=timezone.utc),
            expires_at=(
                datetime.fromtimestamp(e.expires_at, tz=timezone.utc)
                if e.expires_at else None
            ),
            source="cache",
        )
        for e in entries
    ]
```

`GET /{key}` implements the **cache-aside** (lazy loading) pattern: check the cache first, and on a miss, fetch from the origin database, populate the cache, and return. `time.monotonic()` is used for latency measurement because unlike `time.time()`, it is not affected by system clock adjustments (NTP, manual changes). `PUT /{key}` implements **write-through**: the value is written to both the cache and the origin database synchronously. This ensures the origin always has the latest data, at the cost of higher write latency. `batch/get` is efficient because it fetches all misses from the origin in the same request, reducing the number of client-server round trips compared to individual get calls.

---

## Consistent Hashing & Partitioning

In a distributed cache, data must be split across nodes. Consistent hashing ensures that when a cache node joins or leaves, only a minimal fraction of keys are remapped, minimizing cache stampedes.

### `app/partitioning/consistent_hash.py`

```python
import bisect
import hashlib
from dataclasses import dataclass


@dataclass
class VirtualNode:
    """A virtual node on the hash ring."""
    physical_node: str
    vnode_index: int
    hash_value: int


class ConsistentHashRing:
    """
    Consistent hash ring with virtual nodes for even key distribution.
    Uses MD5 hashing to place keys and nodes on a ring of [0, 2^32).
    """

    def __init__(self, num_vnodes: int = 256):
        self._num_vnodes = num_vnodes
        self._ring: list[int] = []                # Sorted hash positions
        self._hash_to_vnode: dict[int, VirtualNode] = {}
        self._nodes: dict[str, list[VirtualNode]] = {}

    def _hash(self, key: str) -> int:
        """Hash a key to a 32-bit integer position on the ring."""
        digest = hashlib.md5(key.encode()).hexdigest()
        return int(digest[:8], 16)

    def add_node(self, node_id: str):
        """Add a physical node with virtual nodes to the ring."""
        if node_id in self._nodes:
            return

        vnodes = []
        for i in range(self._num_vnodes):
            vnode_key = f"{node_id}:vnode-{i}"
            hash_value = self._hash(vnode_key)

            vnode = VirtualNode(
                physical_node=node_id,
                vnode_index=i,
                hash_value=hash_value,
            )

            pos = bisect.bisect_left(self._ring, hash_value)
            self._ring.insert(pos, hash_value)
            self._hash_to_vnode[hash_value] = vnode
            vnodes.append(vnode)

        self._nodes[node_id] = vnodes

    def remove_node(self, node_id: str):
        """Remove a physical node and all its virtual nodes from the ring."""
        if node_id not in self._nodes:
            return

        for vnode in self._nodes[node_id]:
            pos = bisect.bisect_left(self._ring, vnode.hash_value)
            if pos < len(self._ring) and self._ring[pos] == vnode.hash_value:
                self._ring.pop(pos)
            del self._hash_to_vnode[vnode.hash_value]

        del self._nodes[node_id]

    def get_node(self, key: str) -> str | None:
        """
        Get the physical node responsible for a given key.
        Walks clockwise from the key's hash to the next vnode.
        """
        if not self._ring:
            return None

        hash_value = self._hash(key)
        pos = bisect.bisect_right(self._ring, hash_value)

        if pos >= len(self._ring):
            pos = 0

        vnode = self._hash_to_vnode[self._ring[pos]]
        return vnode.physical_node

    def get_replica_nodes(self, key: str, n: int = 3) -> list[str]:
        """
        Get n distinct physical nodes for replication.
        Walks clockwise, skipping vnodes belonging to already-selected nodes.
        """
        if not self._ring:
            return []

        hash_value = self._hash(key)
        pos = bisect.bisect_right(self._ring, hash_value)

        replicas = []
        seen = set()

        for i in range(len(self._ring)):
            idx = (pos + i) % len(self._ring)
            vnode = self._hash_to_vnode[self._ring[idx]]

            if vnode.physical_node not in seen:
                replicas.append(vnode.physical_node)
                seen.add(vnode.physical_node)

            if len(replicas) >= n:
                break

        return replicas

    def get_all_nodes(self) -> list[str]:
        """Return all physical node IDs."""
        return list(self._nodes.keys())

    @property
    def node_count(self) -> int:
        return len(self._nodes)


# Global hash ring instance
hash_ring = ConsistentHashRing(num_vnodes=256)
```

`bisect_right` finds the next position clockwise from the key's hash on the ring. The wrap-around (`if pos >= len(self._ring): pos = 0`) handles keys hashing past the last vnode. With 256 virtual nodes per physical node, the standard deviation of key distribution drops below 5%, ensuring no single node becomes a hotspot. `get_replica_nodes` walks clockwise collecting distinct physical nodes, ensuring replicas land on different machines for true redundancy.

---

## Cache Invalidation Strategies

Cache invalidation is famously one of the two hard problems in computer science. When the origin data changes, all cache nodes holding that data must be notified. We use Redis Pub/Sub as a lightweight message bus for cross-node invalidation.

### Strategy Comparison

| Strategy       | How It Works                                           | Pros                                | Cons                                       |
| -------------- | ------------------------------------------------------ | ----------------------------------- | ------------------------------------------ |
| Write-Through  | Write to cache AND origin on every write               | Cache is always fresh               | Higher write latency                       |
| Write-Behind   | Write to cache, async flush to origin in batches       | Fast writes, reduced DB load        | Data loss risk if cache node crashes       |
| Write-Around   | Write only to origin, cache populated on read          | No cache pollution from cold writes | Higher read latency on first access        |
| TTL-Based      | Let entries expire naturally, re-fetch on miss         | Simplest to implement               | Stale reads within TTL window              |
| Event-Driven   | Origin publishes invalidation events, cache subscribes | Near real-time freshness            | Requires message infrastructure (Pub/Sub)  |

### `app/invalidation/pubsub.py`

```python
import json
import threading
import redis

from app.cache.engine import cache_engine
from app.config import settings


INVALIDATION_CHANNEL = "cache:invalidation"


class InvalidationPublisher:
    """
    Publishes cache invalidation messages to Redis Pub/Sub.
    When a key is updated or deleted in the origin database,
    all cache nodes receive the invalidation message and remove
    the stale entry.
    """

    def __init__(self):
        self._redis = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )

    def invalidate(self, keys: list[str], reason: str = "manual"):
        """
        Publish an invalidation message for the given keys.
        All subscribed cache nodes will remove these keys.
        """
        message = json.dumps({
            "keys": keys,
            "source_node": settings.node_id,
            "reason": reason,
        })
        self._redis.publish(INVALIDATION_CHANNEL, message)

    def invalidate_pattern(self, pattern: str, reason: str = "manual"):
        """
        Invalidate all keys matching a glob pattern (e.g., "user:*").
        Scans local cache keys and publishes those that match.
        """
        import fnmatch
        all_keys = cache_engine.keys()
        matching = [k for k in all_keys if fnmatch.fnmatch(k, pattern)]
        if matching:
            self.invalidate(matching, reason)


class InvalidationSubscriber:
    """
    Subscribes to Redis Pub/Sub for invalidation messages.
    Runs in a background thread, removing invalidated keys
    from the local cache when notified by other nodes.
    """

    def __init__(self):
        self._redis = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        self._pubsub = self._redis.pubsub()
        self._running = False
        self._thread: threading.Thread | None = None
        self._messages_processed = 0

    def start(self):
        """Start listening for invalidation messages."""
        self._pubsub.subscribe(INVALIDATION_CHANNEL)
        self._running = True
        self._thread = threading.Thread(target=self._listen, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop listening and clean up."""
        self._running = False
        self._pubsub.unsubscribe(INVALIDATION_CHANNEL)
        if self._thread:
            self._thread.join(timeout=5)

    def _listen(self):
        """Listen for invalidation messages and delete stale keys."""
        while self._running:
            try:
                message = self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message and message["type"] == "message":
                    self._handle_message(message["data"])
            except Exception as e:
                print(f"Invalidation listener error: {e}")

    def _handle_message(self, data: str):
        """Process an invalidation message."""
        try:
            payload = json.loads(data)
            keys = payload.get("keys", [])
            source = payload.get("source_node", "unknown")

            # Skip messages from ourselves (we already invalidated locally)
            if source == settings.node_id:
                return

            for key in keys:
                cache_engine.delete(key)

            self._messages_processed += 1
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Invalid invalidation message: {e}")

    @property
    def stats(self) -> dict:
        return {"messages_processed": self._messages_processed}


publisher = InvalidationPublisher()
subscriber = InvalidationSubscriber()
```

`InvalidationPublisher` sends invalidation messages to the `cache:invalidation` Redis Pub/Sub channel. All subscribed cache nodes receive the message simultaneously. `InvalidationSubscriber` runs in a background thread, listening for messages and deleting stale keys from the local cache. The `source_node` field prevents a node from processing its own invalidation messages (it already deleted the key locally before publishing). `get_message(timeout=1.0)` polls with a 1-second timeout, balancing responsiveness with CPU usage. `invalidate_pattern` uses `fnmatch` for glob-style pattern matching (e.g., `user:*` invalidates all user-related keys), useful for bulk invalidation after schema changes or deployments.

### `app/invalidation/strategies.py`

```python
from sqlalchemy.orm import Session

from app.cache.engine import cache_engine
from app.invalidation.pubsub import publisher
from app.models import OriginData


def write_through(key: str, value: str, db: Session, ttl_seconds: int | None = None):
    """
    Write-Through: write to both cache and origin synchronously.
    Guarantees the cache is always consistent with the origin.
    """
    # 1. Write to origin DB
    origin = db.query(OriginData).filter(OriginData.key == key).first()
    if origin:
        origin.value = value
    else:
        origin = OriginData(key=key, value=value)
        db.add(origin)
    db.commit()

    # 2. Write to cache
    cache_engine.put(key, value, ttl_seconds=ttl_seconds)

    # 3. Invalidate on other nodes so they refetch
    publisher.invalidate([key], reason="write_through")


def write_behind(key: str, value: str, ttl_seconds: int | None = None):
    """
    Write-Behind (Write-Back): write to cache immediately,
    flush to origin asynchronously in batches.
    The write buffer is drained periodically.
    """
    # Write to cache immediately (fast)
    cache_engine.put(key, value, ttl_seconds=ttl_seconds)

    # Add to write buffer for async flush
    _write_buffer.append({"key": key, "value": value})


def write_around(key: str, value: str, db: Session):
    """
    Write-Around: write only to origin, do NOT populate cache.
    The cache is populated on the next read (cache-aside).
    Avoids polluting the cache with data that may never be read.
    """
    # 1. Write only to origin
    origin = db.query(OriginData).filter(OriginData.key == key).first()
    if origin:
        origin.value = value
    else:
        origin = OriginData(key=key, value=value)
        db.add(origin)
    db.commit()

    # 2. Invalidate cache (force re-fetch on next read)
    cache_engine.delete(key)
    publisher.invalidate([key], reason="write_around")


# Write buffer for write-behind strategy
_write_buffer: list[dict] = []


def flush_write_buffer(db: Session):
    """
    Flush the write-behind buffer to the origin database.
    Called periodically by a background task.
    """
    global _write_buffer

    if not _write_buffer:
        return

    buffer = _write_buffer.copy()
    _write_buffer.clear()

    for item in buffer:
        origin = db.query(OriginData).filter(OriginData.key == item["key"]).first()
        if origin:
            origin.value = item["value"]
        else:
            origin = OriginData(key=item["key"], value=item["value"])
            db.add(origin)

    db.commit()
    print(f"Flushed {len(buffer)} entries from write buffer to origin.")
```

**Write-through** writes to both the origin and cache in the same request, guaranteeing consistency at the cost of higher write latency. **Write-behind** writes only to the cache and buffers the origin write for later, achieving very low write latency but risking data loss if the cache node crashes before the buffer is flushed. **Write-around** writes only to the origin and invalidates the cache, which is optimal for write-heavy keys that are rarely read (avoids polluting the cache). The choice depends on the access pattern: write-through for frequently read data that must be fresh, write-behind for high write throughput, and write-around for infrequently read data.

---

## Replication & Failover

Cache replication ensures that if one node goes down, the cached data is still available on another node. Unlike database replication, cache replication prioritizes speed over strong consistency.

### `app/partitioning/replication.py`

```python
import httpx

from app.cache.engine import cache_engine, CacheEntry
from app.config import settings
from app.partitioning.consistent_hash import hash_ring


class CacheReplicationManager:
    """
    Manages cache replication across nodes.
    Uses async replication: the primary node responds immediately
    and replicates to secondaries in the background.
    """

    def __init__(self, replication_factor: int = 2):
        self._replication_factor = replication_factor

    def get_target_nodes(self, key: str) -> list[str]:
        """Get the list of nodes that should cache this key."""
        return hash_ring.get_replica_nodes(key, self._replication_factor)

    async def replicate_put(
        self, key: str, value: str, ttl_seconds: int | None = None
    ):
        """
        Replicate a cache write to secondary nodes.
        Called asynchronously after the primary write succeeds.
        Fire-and-forget: failures on secondaries are logged but do not
        block the primary response.
        """
        target_nodes = self.get_target_nodes(key)

        for node_id in target_nodes:
            if node_id == settings.node_id:
                continue                          # Skip self

            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    await client.put(
                        f"http://{node_id}:8000/cache/{key}",
                        json={"value": value, "ttl_seconds": ttl_seconds},
                    )
            except Exception as e:
                print(f"Replication to {node_id} failed for key '{key}': {e}")

    async def replicate_delete(self, key: str):
        """Replicate a cache delete to secondary nodes."""
        target_nodes = self.get_target_nodes(key)

        for node_id in target_nodes:
            if node_id == settings.node_id:
                continue

            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    await client.delete(f"http://{node_id}:8000/cache/{key}")
            except Exception as e:
                print(f"Delete replication to {node_id} failed for '{key}': {e}")

    async def handle_failover(self, failed_node: str):
        """
        When a node is detected as down, redistribute its keys
        to the remaining nodes on the hash ring.
        """
        print(f"Handling failover for node: {failed_node}")
        hash_ring.remove_node(failed_node)

        # In a production system, you would:
        # 1. Identify keys owned by the failed node
        # 2. Re-hash them to find new owners
        # 3. Warm the new owners from the origin database


replication_manager = CacheReplicationManager(replication_factor=2)
```

Cache replication uses a **fire-and-forget** pattern: the primary node responds to the client immediately and replicates to secondaries asynchronously. This keeps read and write latency low at the cost of eventual consistency between replicas. The `timeout=2.0` ensures a slow secondary does not block replication to other nodes. On failover, the failed node is removed from the hash ring, causing its keys to be redistributed to remaining nodes. The next read for those keys will be a cache miss, triggering a fetch from the origin database (self-healing).

---

## Cache Warming & Preloading

After a deployment, restart, or node addition, the cache is cold and every request hits the origin database. Cache warming pre-populates the cache with frequently accessed keys.

### `app/routers/warmup.py`

```python
import time
from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.cache.engine import cache_engine
from app.database import get_db
from app.models import OriginData

router = APIRouter()


def _warm_top_keys(db: Session, limit: int):
    """
    Fetch the most recently updated keys from the origin database
    and pre-populate the cache. In a real system, you would use
    access logs or a popularity ranking to determine which keys to warm.
    """
    records = (
        db.query(OriginData)
        .order_by(OriginData.updated_at.desc())
        .limit(limit)
        .all()
    )

    warmed = 0
    for record in records:
        cache_engine.put(record.key, record.value)
        warmed += 1

    print(f"Cache warming complete: loaded {warmed} keys.")
    return warmed


@router.post("/warm")
def warm_cache(
    limit: int = 1000,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
):
    """
    Trigger cache warming. Loads the most recently updated keys
    from the origin database into the cache.

    Runs synchronously for small limits, in the background for large ones.
    """
    if limit <= 100:
        warmed = _warm_top_keys(db, limit)
        return {"status": "complete", "keys_warmed": warmed}

    # Large warming jobs run in the background
    background_tasks.add_task(_warm_top_keys, db, limit)
    return {"status": "warming_started", "target_keys": limit}


@router.post("/warm/keys")
def warm_specific_keys(
    keys: list[str],
    db: Session = Depends(get_db),
):
    """Pre-populate specific keys into the cache."""
    warmed = 0
    missed = 0

    for key in keys:
        entry = cache_engine.get(key)
        if entry is not None:
            continue                              # Already cached

        origin = db.query(OriginData).filter(OriginData.key == key).first()
        if origin:
            cache_engine.put(origin.key, origin.value)
            warmed += 1
        else:
            missed += 1

    return {"warmed": warmed, "not_found": missed, "already_cached": len(keys) - warmed - missed}


@router.get("/warm/status")
def warming_status():
    """Return current cache fill level."""
    stats = cache_engine.stats
    return {
        "entries": stats["entries"],
        "memory_usage_pct": stats["memory_usage_pct"],
        "hit_ratio_pct": stats["hit_ratio_pct"],
        "is_warm": stats["hit_ratio_pct"] > 90,
    }
```

`warm_cache` fetches the most recently updated keys from the origin database and pre-populates the cache. For small warming jobs (≤100 keys), the operation is synchronous and returns immediately. For large jobs, `BackgroundTasks` runs the warming asynchronously so the API remains responsive. `warm_specific_keys` lets operators pre-load specific high-traffic keys (e.g., the homepage product list, trending topics). `warming_status` reports whether the cache is "warm" (hit ratio above 90%). In production, warming should be triggered automatically on deployment and node scale-up events.

---

## Monitoring & Cache Metrics

### `app/routers/admin.py`

```python
from fastapi import APIRouter

from app.cache.engine import cache_engine
from app.cache.metrics import metrics
from app.cache.ttl import ttl_manager
from app.partitioning.consistent_hash import hash_ring
from app.invalidation.pubsub import subscriber

router = APIRouter()


@router.get("/stats")
def get_cache_stats():
    """Return comprehensive cache statistics for this node."""
    return cache_engine.stats


@router.get("/metrics")
def get_metrics():
    """Return detailed metrics including latency percentiles."""
    return metrics.summary


@router.get("/cluster")
def get_cluster_info():
    """Return cluster topology and per-node status."""
    return {
        "total_nodes": hash_ring.node_count,
        "nodes": hash_ring.get_all_nodes(),
        "local_node": cache_engine.stats["node_id"],
        "invalidation_messages": subscriber.stats,
        "ttl_expirations": ttl_manager.stats,
    }


@router.post("/flush")
def flush_cache():
    """Flush the entire cache on this node. Use with caution."""
    cache_engine.clear()
    return {"status": "flushed", "message": "All cached entries removed."}


@router.get("/keys")
def list_keys(limit: int = 100):
    """List cached keys (for debugging, not production use)."""
    keys = cache_engine.keys()
    return {
        "total_keys": len(keys),
        "keys": keys[:limit],
        "truncated": len(keys) > limit,
    }
```

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.cache.ttl import ttl_manager
from app.invalidation.pubsub import subscriber
from app.routers import cache_routes, admin, warmup


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background daemons on startup, stop on shutdown."""
    Base.metadata.create_all(bind=engine)
    ttl_manager.start()
    subscriber.start()
    yield
    subscriber.stop()
    ttl_manager.stop()


app = FastAPI(
    title="Distributed Cache API",
    description="A Redis/Memcached-inspired distributed cache with eviction, "
                "partitioning, and invalidation",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(cache_routes.router, prefix="/cache", tags=["Cache"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(warmup.router, prefix="/warmup", tags=["Warming"])


@app.get("/health")
def health_check():
    """Basic health check for load balancers."""
    return {"status": "healthy"}
```

`lifespan` starts the TTL expiration daemon and invalidation subscriber before the app begins serving requests, and stops them on shutdown. `include_router` mounts the cache CRUD routes under `/cache`, admin/monitoring under `/admin`, and warming under `/warmup`. The health check at `/health` is used by load balancers and container orchestrators to verify the service is alive.

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

This uses a multi-stage build. The `builder` stage installs dependencies into `/install`. The second stage copies only the installed packages, keeping the final image small. `--host 0.0.0.0` binds to all interfaces so the container is reachable from outside.

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
  cache1:
    build: .
    ports:
      - "8001:8000"
    environment:
      - NODE_ID=cache-1
      - DATABASE_URL=postgresql://user:pass@db:5432/appdb
      - REDIS_URL=redis://redis:6379/0
      - MAX_MEMORY_MB=256
      - EVICTION_POLICY=lru
      - DEFAULT_TTL_SECONDS=300
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: unless-stopped

  cache2:
    build: .
    ports:
      - "8002:8000"
    environment:
      - NODE_ID=cache-2
      - DATABASE_URL=postgresql://user:pass@db:5432/appdb
      - REDIS_URL=redis://redis:6379/0
      - MAX_MEMORY_MB=256
      - EVICTION_POLICY=lru
      - DEFAULT_TTL_SECONDS=300
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: unless-stopped

  cache3:
    build: .
    ports:
      - "8003:8000"
    environment:
      - NODE_ID=cache-3
      - DATABASE_URL=postgresql://user:pass@db:5432/appdb
      - REDIS_URL=redis://redis:6379/0
      - MAX_MEMORY_MB=256
      - EVICTION_POLICY=lfu
      - DEFAULT_TTL_SECONDS=300
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_healthy
    restart: unless-stopped

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=appdb
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d appdb"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  db-data:
```

The compose file runs a 3-node distributed cache cluster. Each node (`cache1`, `cache2`, `cache3`) runs the same image with a different `NODE_ID` and is exposed on a different host port. `cache3` is configured with `EVICTION_POLICY=lfu` to demonstrate per-node policy configuration. Redis is used solely as a Pub/Sub transport for cache invalidation messages, not for caching. PostgreSQL is the origin database that the cache sits in front of. `depends_on` with `condition: service_healthy` ensures cache nodes wait until the origin database and Redis pass their health checks. Note that cache nodes do not have persistent volumes because cache data is ephemeral by design. Lost cache data is simply re-fetched from the origin database on the next miss.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f cache1                 # Follow Cache Node 1 logs
```

Visit `http://localhost:8001/docs` for Node 1's interactive API documentation. Test the API:

```bash
# Store a value in the cache
curl -X PUT http://localhost:8001/cache/user:42 \
  -H "Content-Type: application/json" \
  -d '{"value": "{\"name\": \"John\", \"email\": \"john@example.com\"}", "ttl_seconds": 600}'

# Retrieve from cache (hit)
curl http://localhost:8001/cache/user:42

# Delete from cache
curl -X DELETE http://localhost:8001/cache/user:42

# Batch get
curl -X POST http://localhost:8001/cache/batch/get \
  -H "Content-Type: application/json" \
  -d '{"keys": ["user:42", "user:43", "user:44"]}'

# Check cache stats
curl http://localhost:8001/admin/stats

# Check latency metrics
curl http://localhost:8001/admin/metrics

# Warm the cache with top 500 keys
curl -X POST http://localhost:8001/warmup/warm?limit=500

# Flush the cache (use with caution)
curl -X POST http://localhost:8001/admin/flush
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

Tests use SQLite instead of PostgreSQL for speed and isolation. `dependency_overrides` swaps out the production database dependency during testing. `TestClient` from FastAPI wraps `httpx` and provides a synchronous interface for making requests without running a server.

### `tests/test_lru.py`

```python
import time
from app.cache.lru import LRUCache


def test_basic_put_and_get():
    """Test basic cache put and get."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("key1", "value1")
    entry = cache.get("key1")
    assert entry is not None
    assert entry.value == "value1"


def test_cache_miss():
    """Test that missing keys return None."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    assert cache.get("nonexistent") is None


def test_overwrite():
    """Test that putting the same key overwrites the value."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("key1", "value1")
    cache.put("key1", "value2")
    assert cache.get("key1").value == "value2"


def test_eviction_order():
    """Test that LRU evicts the least recently used entry."""
    cache = LRUCache(max_memory_bytes=1024)       # Very small cache

    cache.put("a", "x" * 200)
    cache.put("b", "x" * 200)
    cache.put("c", "x" * 200)

    # Access "a" to make it recently used
    cache.get("a")

    # This should evict "b" (least recently used), not "a"
    cache.put("d", "x" * 200)

    assert cache.get("a") is not None             # Still present (recently accessed)
    assert cache.get("b") is None                 # Evicted (LRU)
    assert cache.get("d") is not None             # Just added


def test_ttl_expiration():
    """Test that entries expire after TTL."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("temp", "data", ttl_seconds=1)
    assert cache.get("temp") is not None
    time.sleep(1.5)
    assert cache.get("temp") is None


def test_delete():
    """Test explicit deletion."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("key1", "value1")
    assert cache.delete("key1") is True
    assert cache.get("key1") is None
    assert cache.delete("nonexistent") is False


def test_memory_tracking():
    """Test that memory usage is tracked correctly."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("key1", "value1")
    assert cache.stats["memory_used_bytes"] > 0
    cache.delete("key1")
    assert cache.stats["memory_used_bytes"] == 0


def test_hit_miss_metrics():
    """Test that hit/miss counters work."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("key1", "value1")

    cache.get("key1")                             # Hit
    cache.get("key1")                             # Hit
    cache.get("missing")                          # Miss

    stats = cache.stats
    assert stats["hits"] == 2
    assert stats["misses"] == 1
    assert stats["hit_ratio_pct"] == round(2 / 3 * 100, 2)


def test_clear():
    """Test that clear removes all entries."""
    cache = LRUCache(max_memory_bytes=1024 * 1024)
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    cache.clear()
    assert cache.get("k1") is None
    assert cache.stats["entries"] == 0
```

### `tests/test_lfu.py`

```python
from app.cache.lfu import LFUCache


def test_basic_lfu():
    """Test basic LFU put and get."""
    cache = LFUCache(max_memory_bytes=1024 * 1024)
    cache.put("key1", "value1")
    assert cache.get("key1").value == "value1"


def test_frequency_eviction():
    """Test that LFU evicts the least frequently used entry."""
    cache = LFUCache(max_memory_bytes=1200)

    cache.put("popular", "x" * 200)
    cache.put("rare", "x" * 200)
    cache.put("medium", "x" * 200)

    # Access "popular" many times, "medium" once
    for _ in range(10):
        cache.get("popular")
    cache.get("medium")

    # This should evict "rare" (freq=0 after put), not "popular" or "medium"
    cache.put("new", "x" * 200)

    assert cache.get("popular") is not None       # Highest frequency
    assert cache.get("new") is not None           # Just added
    # "rare" should have been evicted (lowest frequency)


def test_tie_breaking():
    """Test that ties in frequency are broken by insertion order."""
    cache = LFUCache(max_memory_bytes=1200)

    cache.put("first", "x" * 200)
    cache.put("second", "x" * 200)
    cache.put("third", "x" * 200)

    # All have frequency 0 (no gets), so the first inserted should be evicted
    cache.put("trigger_eviction", "x" * 200)

    assert cache.get("first") is None             # Evicted (oldest at min freq)
```

### `tests/test_api.py`

```python
def test_put_and_get(client):
    """Test storing and retrieving via the API."""
    client.put("/cache/testkey", json={"value": "testvalue"})
    response = client.get("/cache/testkey")
    assert response.status_code == 200
    assert response.json()["value"] == "testvalue"
    assert response.json()["source"] in ("cache", "origin")


def test_get_missing_key(client):
    """Test 404 for keys not in cache or origin."""
    response = client.get("/cache/nonexistent")
    assert response.status_code == 404


def test_delete_key(client):
    """Test deleting a key via the API."""
    client.put("/cache/testkey", json={"value": "testvalue"})
    response = client.delete("/cache/testkey")
    assert response.status_code == 200
    assert response.json()["deleted"] is True


def test_key_size_limit(client):
    """Test that keys exceeding the max size are rejected."""
    long_key = "a" * 257
    response = client.put(f"/cache/{long_key}", json={"value": "v"})
    assert response.status_code == 400


def test_value_size_limit(client):
    """Test that values exceeding the max size are rejected."""
    large_value = "x" * 65537
    response = client.put("/cache/bigkey", json={"value": large_value})
    assert response.status_code == 400


def test_cache_stats(client):
    """Test that stats endpoint returns valid data."""
    client.put("/cache/k1", json={"value": "v1"})
    response = client.get("/admin/stats")
    assert response.status_code == 200
    stats = response.json()
    assert stats["entries"] >= 1
    assert "hit_ratio_pct" in stats


def test_health_check(client):
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"
```

### `tests/test_hashing.py`

```python
from app.partitioning.consistent_hash import ConsistentHashRing


def test_single_node_routing():
    """All keys route to the only node."""
    ring = ConsistentHashRing(num_vnodes=64)
    ring.add_node("cache-1")

    for i in range(100):
        assert ring.get_node(f"key-{i}") == "cache-1"


def test_even_distribution():
    """Keys should be roughly evenly distributed."""
    ring = ConsistentHashRing(num_vnodes=256)
    ring.add_node("cache-1")
    ring.add_node("cache-2")
    ring.add_node("cache-3")

    counts = {"cache-1": 0, "cache-2": 0, "cache-3": 0}

    for i in range(10000):
        node = ring.get_node(f"key-{i}")
        counts[node] += 1

    for node, count in counts.items():
        ratio = count / 10000
        assert 0.13 < ratio < 0.53, f"{node}: {ratio:.2%}"


def test_minimal_redistribution():
    """Adding a node should move roughly 1/N of keys."""
    ring = ConsistentHashRing(num_vnodes=256)
    ring.add_node("cache-1")
    ring.add_node("cache-2")
    ring.add_node("cache-3")

    original = {}
    for i in range(10000):
        key = f"key-{i}"
        original[key] = ring.get_node(key)

    ring.add_node("cache-4")

    moved = sum(
        1 for i in range(10000)
        if ring.get_node(f"key-{i}") != original[f"key-{i}"]
    )

    assert moved / 10000 < 0.50


def test_node_removal():
    """Removing a node redistributes its keys."""
    ring = ConsistentHashRing(num_vnodes=64)
    ring.add_node("cache-1")
    ring.add_node("cache-2")
    ring.add_node("cache-3")

    ring.remove_node("cache-2")

    for i in range(100):
        assert ring.get_node(f"key-{i}") in ("cache-1", "cache-3")


def test_replicas_are_distinct():
    """Replica nodes must be on different physical nodes."""
    ring = ConsistentHashRing(num_vnodes=64)
    ring.add_node("cache-1")
    ring.add_node("cache-2")
    ring.add_node("cache-3")

    replicas = ring.get_replica_nodes("test-key", n=3)
    assert len(set(replicas)) == 3
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_lru"                          # Run only LRU tests
pytest -k "test_lfu"                          # Run only LFU tests
```

---

## Deploying to GCP (Cloud Run + Memorystore)

### Prerequisites

```bash
# Install Google Cloud SDK
brew install google-cloud-sdk                 # macOS
# Or download from https://cloud.google.com/sdk/docs/install

gcloud init                                   # Authenticate and set project
gcloud auth configure-docker                  # Configure Docker for GCR
```

### Set Up Cloud SQL (PostgreSQL) – Origin Database

```bash
# Create a Cloud SQL instance for the origin database
gcloud sql instances create cache-origin-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-2-7680 \
  --region=us-central1 \
  --storage-size=50GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create appdb \
  --instance=cache-origin-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=cache-origin-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-2-7680` provisions 2 vCPUs and 7.5 GB RAM, appropriate for a production origin database. `--availability-type=REGIONAL` enables automatic failover for high availability. The origin database is the source of truth; the cache reduces the read load on it.

### Set Up Memorystore (Redis) – Pub/Sub for Invalidation

```bash
# Create a Redis instance for Pub/Sub invalidation messages
gcloud redis instances create cache-pubsub \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default

# Get the Redis host IP
gcloud redis instances describe cache-pubsub \
  --region=us-central1 \
  --format="value(host)"
```

`--size=1` provisions 1 GB of memory, which is more than enough for Pub/Sub message transport. Memorystore is fully managed and accessible only within the VPC.

### Build & Push the Docker Image

```bash
# Create Artifact Registry repository
gcloud artifacts repositories create distributed-cache \
  --repository-format=docker \
  --location=us-central1

# Build and push
gcloud builds submit \
  --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/distributed-cache/api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build, pushing directly to Artifact Registry without needing Docker installed locally.

### Deploy to Cloud Run (Multiple Instances)

```bash
# Deploy Cache Node 1
gcloud run deploy cache-node-1 \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/distributed-cache/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 4Gi \
  --cpu 2 \
  --min-instances 1 \
  --max-instances 5 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:cache-origin-db \
  --set-env-vars "\
NODE_ID=cache-1,\
DATABASE_URL=postgresql://postgres:PASS@/appdb?host=/cloudsql/YOUR_PROJECT_ID:us-central1:cache-origin-db,\
REDIS_URL=redis://REDIS_IP:6379/0,\
MAX_MEMORY_MB=2048,\
EVICTION_POLICY=lru,\
DEFAULT_TTL_SECONDS=300"

# Deploy Cache Node 2 (repeat with NODE_ID=cache-2)
# Deploy Cache Node 3 (repeat with NODE_ID=cache-3)
```

`--memory 4Gi` gives the container 4 GB, of which 2 GB is reserved for the cache (`MAX_MEMORY_MB=2048`) and the rest for the application, Python runtime, and OS overhead. `--min-instances 1` keeps at least one instance warm to avoid cold starts. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy for secure database connections.

### Set Up Load Balancer

```bash
# Create a serverless NEG for each Cloud Run service
gcloud compute network-endpoint-groups create cache-neg-1 \
  --region=us-central1 \
  --network-endpoint-type=serverless \
  --cloud-run-service=cache-node-1

# Create backend service
gcloud compute backend-services create cache-backend \
  --global \
  --load-balancing-scheme=EXTERNAL_MANAGED

# Add NEGs to backend
gcloud compute backend-services add-backend cache-backend \
  --global \
  --network-endpoint-group=cache-neg-1 \
  --network-endpoint-group-region=us-central1

# Create URL map and forwarding rule
gcloud compute url-maps create cache-lb \
  --default-service=cache-backend

gcloud compute target-https-proxies create cache-proxy \
  --url-map=cache-lb \
  --ssl-certificates=your-cert

gcloud compute forwarding-rules create cache-forwarding \
  --global \
  --target-https-proxy=cache-proxy \
  --ports=443
```

### Custom Domain

```bash
gcloud run domain-mappings create \
  --service cache-node-1 \
  --domain cache.yourdomain.com \
  --region us-central1
```

After running this command, GCP provides DNS records to add at your domain registrar. GCP automatically provisions and renews SSL certificates.

---

## Scaling & Production Considerations

### Memory & Eviction Scaling

| Strategy              | When to Use                                | How                                                |
| --------------------- | ------------------------------------------ | -------------------------------------------------- |
| Add Cache Nodes       | Working set exceeds single-node memory     | Add node to hash ring, keys redistribute naturally |
| Two-Tier Cache        | Reduce inter-node latency                  | L1 in-process (per-app) → L2 distributed cache    |
| Adaptive TTL          | Different keys have different freshness needs | Set TTL based on key pattern or access frequency |
| Bloom Filters         | Too many negative lookups hit the origin   | Filter definitely-not-cached keys before DB query  |
| Admission Control     | Scan pollution degrades hit ratio          | Only cache keys accessed more than once (TinyLFU)  |

### Consistency & Invalidation Scaling

| Strategy              | When to Use                                | How                                                |
| --------------------- | ------------------------------------------ | -------------------------------------------------- |
| Lease-Based Inv.      | Thunder herd on popular key expiration     | Only one client can populate a key; others wait    |
| Version Stamps        | Detect stale reads during cache-aside      | Compare cached version with origin version on read |
| Change Data Capture   | Origin DB has frequent updates             | Stream binlog/WAL changes → invalidation events    |
| Delayed Invalidation  | Reduce invalidation storm impact           | Stagger invalidations over a short time window     |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │cache.example.com│
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud CDN      │
                     │ (Edge Cache)   │
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
       │ App Server  │ │App     │ │ App Server  │
       │ + L1 Cache  │ │Server  │ │ + L1 Cache  │
       │ (in-process)│ │+ L1    │ │ (in-process)│
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │       L2 Distributed Cache          │
         │       (Our Implementation)           │
         │                                      │
         │  ┌──────────┐ ┌──────────┐ ┌──────┐ │
         │  │ Cache    │ │ Cache    │ │Cache │ │
         │  │ Node 1   │ │ Node 2   │ │Node N│ │
         │  │ (LRU     │ │ (LRU     │ │(LRU  │ │
         │  │  2GB)    │ │  2GB)    │ │ 2GB) │ │
         │  └────┬─────┘ └────┬─────┘ └──┬───┘ │
         │       │             │           │     │
         │  ┌────▼─────────────▼───────────▼──┐ │
         │  │    Redis Pub/Sub                │ │
         │  │    (Invalidation Bus)           │ │
         │  └─────────────────────────────────┘ │
         └──────────────────┬───────────────────┘
                            │
                     ┌──────▼─────────┐
                     │  Cloud SQL     │
                     │ (Origin DB)    │
                     │ Primary +      │
                     │ Read Replicas  │
                     └────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud CDN (Edge Cache)** serves as L0 cache at edge locations worldwide, caching responses for static or slowly-changing keys. This reduces load on the distributed cache for highly popular keys.
- **L1 In-Process Cache** is a small (e.g., 64 MB) per-application-server LRU cache. It avoids network round trips for the hottest keys. TTL should be very short (5–30 seconds) to limit staleness.
- **L2 Distributed Cache** is our implementation, shared across all app servers. It handles the bulk of caching and invalidation.
- **Redis Pub/Sub** provides near-instant cross-node invalidation when the origin data changes.
- **Read Replicas** on Cloud SQL handle the 5% of requests that miss the cache, preventing the primary database from being overwhelmed.
- **Change Data Capture (CDC)** streams database changes directly to the invalidation bus, ensuring the cache is invalidated within milliseconds of an origin update without application-level code changes.

### Cache Stampede Prevention

```
Normal flow:
  Key expires → 1000 concurrent requests → all hit origin DB → thundering herd

With stampede prevention:
  Key expires → first request acquires lock → fetches from origin
  → 999 requests wait or receive stale value → lock released → all get fresh value
```

Implement a distributed lock using Redis `SET key NX EX` (set if not exists, with expiry). The first client to acquire the lock fetches from the origin and repopulates the cache. Other clients either wait (blocking) or receive a slightly stale cached value (non-blocking). This is critical for high-traffic keys where a stampede could overload the origin database.

### Security Checklist

```
✓ Authenticate cache API with API keys or service account tokens
✓ Encrypt data in transit (TLS between clients and cache nodes)
✓ Encrypt sensitive cached values at rest (AES-256)
✓ Validate key and value sizes to prevent memory abuse
✓ Rate limit writes per client to prevent hot-key storms
✓ Use VPC-native networking (no public IPs on cache nodes)
✓ Store database credentials in Secret Manager, not env vars
✓ Enable Cloud Audit Logs for cache access patterns
✓ Sanitize cached data to prevent injection attacks
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create cache-health \
  --display-name="Distributed Cache Health" \
  --uri="https://cache.example.com/health" \
  --period=60

# Key metrics to monitor and alert on:
# - Cache hit ratio (alert if <85% for 5 minutes → cache is cold)
# - Memory usage per node (alert at 90% → eviction pressure)
# - Eviction rate (alert if >1000/sec → cache is undersized)
# - Read latency p99 (alert if >10ms → performance degradation)
# - Origin DB QPS (alert if rising → cache effectiveness dropping)
# - Invalidation message lag (alert if >1s → stale reads increasing)
# - Node heartbeat misses (alert after 3 → failover needed)
# - Cache warming status (alert if hit ratio <50% after deploy)
```

Cloud Run automatically exports metrics to Cloud Monitoring. Custom metrics for hit ratio, eviction rate, and memory usage should be exported via the `/admin/metrics` endpoint and scraped by a monitoring agent. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. The most critical alert is the **hit ratio drop**, which indicates the cache is either too small, keys are expiring too fast, or an invalidation storm has flushed the cache.
