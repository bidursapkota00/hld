# Design Search Autocomplete

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Data Collection & Storage](#data-collection--storage)
5. [Trie Data Structure](#trie-data-structure)
6. [Core API – Suggestions & Recording](#core-api--suggestions--recording)
7. [Ranking & Relevance](#ranking--relevance)
8. [Caching with Redis](#caching-with-redis)
9. [Trie Persistence & Rebuild](#trie-persistence--rebuild)
10. [Rate Limiting & Abuse Prevention](#rate-limiting--abuse-prevention)
11. [Filtering & Content Moderation](#filtering--content-moderation)
12. [Docker & Docker Compose](#docker--docker-compose)
13. [Testing](#testing)
14. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
15. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement        | Description                                                                |
| ------------------ | -------------------------------------------------------------------------- |
| Autocomplete       | Given a prefix, return top-K most relevant search suggestions              |
| Record Query       | Log every completed search to update suggestion frequency                  |
| Real-time Update   | New trending queries appear in suggestions within minutes                  |
| Personalization    | Optionally boost suggestions based on user's search history                |
| Filtering          | Exclude offensive, NSFW, or legally prohibited terms from results          |
| Multi-language     | Support Unicode prefixes for non-Latin scripts                             |
| Delete Suggestion  | Allow users to remove specific suggestions from their personal history     |

### Non-Functional Requirements

| Requirement        | Target                                                             |
| ------------------ | ------------------------------------------------------------------ |
| Latency            | < 10 ms per suggestion request (p99) — typeahead must feel instant |
| Availability       | 99.99% uptime (degradation loses suggestions, not core search)     |
| Scalability        | Handle 100K queries per second at peak                             |
| Freshness          | Trending queries reflected within 5 minutes                        |
| Fault Tolerance    | Stale suggestions served if backend is down (never empty results)  |

### Back-of-the-Envelope Estimations

```
Total unique queries             ≈ 500 million
Average query length             ≈ 20 characters (UTF-8)
Average node size in trie        ≈ 100 bytes (char + children pointers + metadata)
Average nodes per query          ≈ 20 (one per character)
Trie memory (naive)              ≈ 500 M × 20 × 100 B = ~1 TB (too large, needs pruning)
Pruned trie (top 10M queries)    ≈ 10 M × 20 × 100 B = ~20 GB (fits in memory)
Suggestion QPS (peak)            ≈ 100 000 / sec (each keystroke triggers a request)
Search recording QPS             ≈ 10 000 / sec (only completed searches)
Cache hit ratio target           ≈ 90% (popular prefixes are highly repetitive)
Redis cache memory               ≈ 10 M prefixes × 500 B avg = ~5 GB
Keystrokes per query             ≈ 10 (average word before selection)
Read : Write ratio               ≈ 100 : 1 (reads dominate heavily)
```

Only the top 10 million most frequent queries are loaded into the in-memory trie, keeping memory usage around 20 GB per node. A 90% cache hit ratio means only 10% of requests actually traverse the trie, reducing CPU load. The trie is rebuilt periodically from aggregated query logs rather than updated on every keystroke.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │ (Browser/App)│
                        └──────┬───────┘
                               │ (keystroke → prefix)
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │      Suggestion Service     │
                │    (FastAPI on Cloud Run)    │
                │                              │
                │  GET /suggest?q=prefix → []  │
                │  POST /queries   → record   │
                └──────┬──────────────┬────────┘
                       │              │
                ┌──────▼──────┐ ┌─────▼──────────┐
                │    Redis    │ │  In-Memory Trie │
                │   (Cache)   │ │ (Per-Instance)  │
                │ Memorystore │ │                 │
                └─────────────┘ └────────┬────────┘
                                         │ (rebuild periodically)
                              ┌──────────▼──────────┐
                              │    PostgreSQL        │
                              │  (Query Logs &       │
                              │   Aggregated Freq)   │
                              │    Cloud SQL         │
                              └──────────────────────┘
```

The client sends the current prefix on each keystroke (debounced at 100-200 ms) to the suggestion service. The service first checks Redis for a cached result for that exact prefix. On a cache miss, it traverses the in-memory trie to find the top-K suggestions ranked by frequency, caches the result in Redis, and returns it. Completed searches are recorded asynchronously to PostgreSQL via a background worker. A periodic aggregation job rebuilds the trie from the latest frequency data every 5-15 minutes to reflect trending queries.

---

## Project Setup

### Project Structure

```
search-autocomplete/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # PostgreSQL connection and session
│   ├── cache.py              # Redis cache layer
│   ├── trie/
│   │   ├── __init__.py
│   │   ├── node.py           # TrieNode data structure
│   │   ├── trie.py           # Trie with insert, search, top-K
│   │   └── builder.py        # Trie builder from database
│   ├── ranking/
│   │   ├── __init__.py
│   │   └── scorer.py         # Scoring and ranking logic
│   ├── moderation/
│   │   ├── __init__.py
│   │   └── filter.py         # Offensive content filter
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── aggregator.py     # Periodic frequency aggregation
│   │   └── collector.py      # Async query recording
│   └── routers/
│       ├── __init__.py
│       ├── suggest.py        # Suggestion endpoints
│       └── admin.py          # Admin/health endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_trie.py          # Tests for trie operations
│   ├── test_suggest.py       # Tests for suggestion API
│   └── test_ranking.py       # Tests for scoring logic
├── data/
│   └── blocklist.txt         # Blocked terms for moderation
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir search-autocomplete && cd search-autocomplete
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

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL where query logs and aggregated frequencies are stored. `psycopg2-binary` is the PostgreSQL driver. `redis` is the Python client for Redis, used for caching prefix → suggestions mappings. `pydantic-settings` handles environment variable loading. `httpx` is used for async HTTP requests and testing.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/autocomplete
REDIS_URL=redis://localhost:6379/0
TOP_K=10
TRIE_REBUILD_INTERVAL_MINUTES=15
MAX_QUERY_LENGTH=100
MIN_QUERY_FREQUENCY=5
CACHE_TTL_SECONDS=300
RATE_LIMIT_PER_MINUTE=300
PORT=8000
```

`DATABASE_URL` points to the PostgreSQL instance storing query logs and frequency data. `REDIS_URL` is the Redis instance used for caching suggestion results. `TOP_K=10` returns at most 10 suggestions per prefix. `TRIE_REBUILD_INTERVAL_MINUTES=15` rebuilds the in-memory trie every 15 minutes from the latest frequency data. `MIN_QUERY_FREQUENCY=5` filters out queries searched fewer than 5 times, keeping the trie lean. `CACHE_TTL_SECONDS=300` caches suggestions for 5 minutes.

---

## Data Collection & Storage

Every search autocomplete system relies on query frequency data. When a user completes a search, the query is logged. A periodic job aggregates raw logs into frequency counts, which feed the trie.

### Data Flow

```
User types "how to cook" → keystroke events → suggestions returned
User presses Enter       → POST /queries    → raw log inserted
                                             ↓
                         Aggregator (cron) → frequency table updated
                                             ↓
                         Trie builder      → new trie loaded into memory
```

Raw query logs are append-only and high-volume. The aggregation step reduces millions of raw logs into a compact frequency table (query → count), which is what the trie actually reads from. This separation lets the write path (logging) and read path (suggestions) scale independently.

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐
│           query_logs                  │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ query_text    VARCHAR(200) NOT NULL   │
│ user_id       VARCHAR(64)  NULL       │
│ searched_at   TIMESTAMPTZ DEFAULT NOW │
│ ip_address    VARCHAR(45)             │
│ country       VARCHAR(2)              │
├──────────────────────────────────────┤
│ INDEX idx_query_text (query_text)     │
│ INDEX idx_searched_at (searched_at)   │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│         query_frequencies             │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ query_text    VARCHAR(200) UNIQUE     │
│ frequency     BIGINT     DEFAULT 0    │
│ last_updated  TIMESTAMPTZ DEFAULT NOW │
│ is_blocked    BOOLEAN    DEFAULT FALSE│
├──────────────────────────────────────┤
│ INDEX idx_freq_text (query_text)      │
│ INDEX idx_frequency (frequency DESC)  │
└──────────────────────────────────────┘
```

The `query_logs` table is the raw event store — every completed search is appended here. `query_frequencies` is the aggregated table that the trie reads from. Separating raw logs from frequencies avoids running expensive `GROUP BY` queries on the hot write table. `is_blocked` marks queries flagged by the moderation system so the trie builder can skip them.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/autocomplete"
    redis_url: str = "redis://localhost:6379/0"
    top_k: int = 10
    trie_rebuild_interval_minutes: int = 15
    max_query_length: int = 100
    min_query_frequency: int = 5
    cache_ttl_seconds: int = 300
    rate_limit_per_minute: int = 300
    port: int = 8000

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

`pool_size=20` keeps 20 connections open and reusable. The database is queried primarily during trie rebuilds and query logging, not during suggestion lookups (those hit Redis or the in-memory trie). `pool_pre_ping=True` detects stale connections from cloud databases that close idle connections.

### `app/models.py`

```python
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Index, String, Text
)

from app.database import Base


class QueryLog(Base):
    __tablename__ = "query_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    query_text = Column(String(200), nullable=False, index=True)
    user_id = Column(String(64), nullable=True)
    searched_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    ip_address = Column(String(45), nullable=True)
    country = Column(String(2), nullable=True)

    __table_args__ = (
        Index("idx_searched_at", "searched_at"),
    )


class QueryFrequency(Base):
    __tablename__ = "query_frequencies"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    query_text = Column(String(200), unique=True, nullable=False, index=True)
    frequency = Column(BigInteger, default=0)
    last_updated = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    is_blocked = Column(Boolean, default=False)

    __table_args__ = (
        Index("idx_frequency", "frequency"),
    )
```

`BigInteger` for `id` supports billions of rows. `String(200)` limits query text to 200 characters to prevent abuse. `String(45)` for IP addresses accommodates both IPv4 and IPv6. `onupdate` on `last_updated` automatically refreshes the timestamp whenever the row is modified. `is_blocked` is checked during trie builds to exclude moderated content.

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, Field


class SuggestionRequest(BaseModel):
    prefix: str = Field(..., min_length=1, max_length=100)
    user_id: str | None = None                    # Optional for personalization


class SuggestionResponse(BaseModel):
    prefix: str
    suggestions: list[str]
    cached: bool = False                          # Whether result came from cache


class QueryRecord(BaseModel):
    query: str = Field(..., min_length=1, max_length=200)
    user_id: str | None = None


class QueryRecordResponse(BaseModel):
    status: str
    query: str


class TrieStats(BaseModel):
    total_nodes: int
    total_words: int
    memory_estimate_mb: float
    last_rebuilt: datetime | None

    class Config:
        from_attributes = True
```

`Field(..., min_length=1, max_length=100)` validates that the prefix is between 1 and 100 characters, rejecting empty strings and overly long inputs at the schema layer. `cached` tells the client whether the response was served from Redis or computed from the trie, useful for debugging and monitoring. `TrieStats` provides observability into the trie's state.

---

## Trie Data Structure

A trie (prefix tree) is the ideal data structure for autocomplete. Every path from the root to a node represents a prefix. To find suggestions for "ho", traverse root → h → o and collect all words reachable from that node, ranked by frequency.

### Why Trie Over Alternatives

| Approach            | Lookup Time      | Memory         | Pros                                     | Cons                                        |
| ------------------- | ---------------- | -------------- | ---------------------------------------- | ------------------------------------------- |
| Trie                | O(prefix length) | Moderate       | Fast prefix matching, natural for autocomplete | Higher memory than flat structures          |
| Sorted Array + BSearch | O(log N)      | Low            | Compact, cache-friendly                  | Full rebuild on updates, no prefix semantics |
| Hash Map (prefix → results) | O(1)   | High           | Fastest lookup                           | Exponential key space, expensive to build    |
| Inverted Index      | O(log N)         | High           | Great for full-text search               | Overkill for prefix matching                 |
| SQL LIKE 'prefix%'  | O(N) worst case  | N/A (on disk)  | No extra data structures                 | Far too slow for real-time typeahead         |

The trie gives O(L) lookup time where L is the prefix length (typically 1-20 characters), which is effectively constant time. Combined with storing top-K results at each node (precomputation), suggestion lookup is O(L + K) regardless of the dataset size.

### `app/trie/node.py`

```python
from dataclasses import dataclass, field


@dataclass
class TrieNode:
    """A single node in the trie."""
    children: dict[str, "TrieNode"] = field(default_factory=dict)
    is_end_of_word: bool = False
    frequency: int = 0
    top_suggestions: list[tuple[str, int]] = field(default_factory=list)
```

`children` maps each character to the next node. `is_end_of_word` marks whether this node completes a valid query. `frequency` stores how often this query was searched (only meaningful at word-ending nodes). `top_suggestions` is a precomputed list of the top-K most frequent queries reachable from this node, stored as `(query_text, frequency)` tuples sorted by frequency descending. This precomputation is the key optimization — without it, every suggestion request would need a DFS traversal of all descendants.

### `app/trie/trie.py`

```python
import threading
import time
from dataclasses import dataclass, field

from app.config import settings
from app.trie.node import TrieNode


@dataclass
class Trie:
    """Thread-safe trie with top-K suggestion precomputation."""
    root: TrieNode = field(default_factory=TrieNode)
    total_nodes: int = 0
    total_words: int = 0
    last_rebuilt: float | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def insert(self, word: str, frequency: int) -> None:
        """Insert a word with its frequency into the trie."""
        if not word:
            return

        word = word.lower().strip()

        with self._lock:
            node = self.root
            for char in word:
                if char not in node.children:
                    node.children[char] = TrieNode()
                    self.total_nodes += 1
                node = node.children[char]

            if not node.is_end_of_word:
                self.total_words += 1

            node.is_end_of_word = True
            node.frequency = frequency

    def search_prefix(self, prefix: str) -> TrieNode | None:
        """Traverse to the node representing the given prefix."""
        prefix = prefix.lower().strip()
        node = self.root

        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]

        return node

    def get_suggestions(self, prefix: str, k: int | None = None) -> list[tuple[str, int]]:
        """
        Return top-K suggestions for a prefix.
        Uses precomputed top_suggestions if available,
        otherwise falls back to DFS collection.
        """
        k = k or settings.top_k
        prefix = prefix.lower().strip()

        node = self.search_prefix(prefix)
        if node is None:
            return []

        # Use precomputed results if available
        if node.top_suggestions:
            return node.top_suggestions[:k]

        # Fallback: DFS to collect all words under this prefix
        results = []
        self._dfs_collect(node, prefix, results)
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:k]

    def _dfs_collect(
        self,
        node: TrieNode,
        current_prefix: str,
        results: list[tuple[str, int]],
    ) -> None:
        """Depth-first search to collect all words under a node."""
        if node.is_end_of_word:
            results.append((current_prefix, node.frequency))

        for char, child_node in node.children.items():
            self._dfs_collect(child_node, current_prefix + char, results)

    def precompute_top_k(self, k: int | None = None) -> None:
        """
        Precompute top-K suggestions at every node in the trie.
        This is called once after building the trie, making subsequent
        lookups O(prefix_length) instead of O(prefix_length + subtree_size).
        """
        k = k or settings.top_k

        with self._lock:
            self._precompute_node(self.root, "", k)
            self.last_rebuilt = time.time()

    def _precompute_node(
        self,
        node: TrieNode,
        current_prefix: str,
        k: int,
    ) -> list[tuple[str, int]]:
        """
        Recursively compute top-K for each node by merging
        results from all children using a bottom-up approach.
        """
        results = []

        # Include this node's word if it terminates here
        if node.is_end_of_word:
            results.append((current_prefix, node.frequency))

        # Merge results from all children
        for char, child_node in node.children.items():
            child_results = self._precompute_node(
                child_node, current_prefix + char, k
            )
            results.extend(child_results)

        # Sort by frequency descending, keep top K
        results.sort(key=lambda x: x[1], reverse=True)
        node.top_suggestions = results[:k]

        return results[:k]

    def get_stats(self) -> dict:
        """Return trie statistics for monitoring."""
        return {
            "total_nodes": self.total_nodes,
            "total_words": self.total_words,
            "memory_estimate_mb": round(self.total_nodes * 100 / (1024 * 1024), 2),
            "last_rebuilt": self.last_rebuilt,
        }


# Global trie instance shared across the application
trie = Trie()
```

`threading.RLock` makes the trie thread-safe. `RLock` (reentrant lock) is used instead of `Lock` because the precompute method calls itself recursively and needs to re-enter the lock. `insert` normalizes queries to lowercase and strips whitespace for case-insensitive matching. `search_prefix` traverses the trie character by character, returning `None` if the prefix doesn't exist.

The critical optimization is `precompute_top_k`. After the trie is fully built, this method walks every node bottom-up, collecting all words reachable from each node, sorting by frequency, and storing the top-K at that node. Subsequent `get_suggestions` calls then just read the precomputed list, making them O(L) where L is the prefix length. Without precomputation, each suggestion request would require a DFS of the entire subtree, which could be O(millions) for short prefixes like "a" or "the".

### `app/trie/builder.py`

```python
import logging
import time

from sqlalchemy.orm import Session

from app.config import settings
from app.models import QueryFrequency
from app.trie.trie import Trie, trie

logger = logging.getLogger(__name__)


def build_trie_from_db(db: Session) -> Trie:
    """
    Build a new trie from the query_frequencies table.
    Only includes queries that meet the minimum frequency threshold
    and are not blocked.
    """
    start = time.time()
    new_trie = Trie()

    # Fetch top queries ordered by frequency
    queries = (
        db.query(QueryFrequency)
        .filter(QueryFrequency.is_blocked == False)
        .filter(QueryFrequency.frequency >= settings.min_query_frequency)
        .order_by(QueryFrequency.frequency.desc())
        .limit(10_000_000)                        # Cap at 10M queries
        .all()
    )

    for q in queries:
        new_trie.insert(q.query_text, q.frequency)

    # Precompute top-K at every node
    new_trie.precompute_top_k(settings.top_k)

    elapsed = time.time() - start
    logger.info(
        f"Trie built: {new_trie.total_words} words, "
        f"{new_trie.total_nodes} nodes, {elapsed:.2f}s"
    )

    return new_trie


def rebuild_global_trie(db: Session) -> None:
    """
    Rebuild the global trie instance.
    Uses a swap strategy: build a new trie, then atomically
    replace the global reference. This avoids locking the
    old trie during the build.
    """
    global trie

    new_trie = build_trie_from_db(db)

    # Atomic swap — Python's GIL ensures reference assignment is atomic
    from app.trie import trie as trie_module
    import app.trie.trie as trie_mod
    trie_mod.trie = new_trie

    logger.info("Global trie swapped successfully.")
```

The builder queries PostgreSQL for all non-blocked queries above the minimum frequency threshold, inserts them into a fresh trie, and precomputes top-K suggestions. The `.limit(10_000_000)` cap prevents the trie from consuming unbounded memory. The rebuild uses a **swap strategy**: a completely new trie is built in the background, and only when it's fully ready is the global reference swapped. This means the old trie continues serving requests during the build with zero downtime. Python's GIL (Global Interpreter Lock) guarantees that the reference assignment is atomic — no request will see a half-built trie.

---

## Core API – Suggestions & Recording

### `app/routers/suggest.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.cache import get_cached_suggestions, set_cached_suggestions
from app.config import settings
from app.database import get_db
from app.models import QueryLog
from app.schemas import (
    QueryRecord,
    QueryRecordResponse,
    SuggestionResponse,
)
from app.trie.trie import trie

router = APIRouter()


@router.get("/suggest", response_model=SuggestionResponse)
def get_suggestions(
    q: str = Query(..., min_length=1, max_length=100, description="Search prefix"),
    k: int = Query(None, ge=1, le=25, description="Number of suggestions"),
):
    """
    Return top-K autocomplete suggestions for the given prefix.
    Checks Redis cache first, falls back to in-memory trie.
    """
    prefix = q.lower().strip()
    num_results = k or settings.top_k

    # 1. Check cache first
    cached = get_cached_suggestions(prefix)
    if cached is not None:
        return SuggestionResponse(
            prefix=prefix,
            suggestions=cached[:num_results],
            cached=True,
        )

    # 2. Cache miss → query the trie
    results = trie.get_suggestions(prefix, num_results)
    suggestions = [text for text, freq in results]

    # 3. Populate cache
    if suggestions:
        set_cached_suggestions(prefix, suggestions)

    return SuggestionResponse(
        prefix=prefix,
        suggestions=suggestions,
        cached=False,
    )


@router.post("/queries", response_model=QueryRecordResponse, status_code=201)
def record_query(
    payload: QueryRecord,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Record a completed search query for frequency tracking.
    Called when the user submits a search (presses Enter or clicks a suggestion).
    """
    query_text = payload.query.lower().strip()

    if len(query_text) > settings.max_query_length:
        raise HTTPException(
            status_code=400,
            detail=f"Query exceeds max length of {settings.max_query_length}.",
        )

    log_entry = QueryLog(
        query_text=query_text,
        user_id=payload.user_id,
        ip_address=request.client.host,
    )

    db.add(log_entry)
    db.commit()

    return QueryRecordResponse(status="recorded", query=query_text)
```

`Query(..., min_length=1, max_length=100)` uses FastAPI's `Query` with validation — the `...` makes it required, and the length constraints prevent empty or excessively long prefixes. The suggestion endpoint follows the cache-aside pattern: check Redis first, fall back to the trie on a cache miss, then populate the cache. The query recording endpoint is a simple append to `query_logs`. In production, you would send this to a message queue (Pub/Sub, Kafka) instead of writing directly to PostgreSQL to avoid adding latency to the search flow.

### `app/routers/admin.py`

```python
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import TrieStats
from app.trie.builder import rebuild_global_trie
from app.trie.trie import trie

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
def health_check():
    """Health check for load balancers and container orchestrators."""
    return {
        "status": "healthy",
        "trie_loaded": trie.total_words > 0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/trie/stats", response_model=TrieStats)
def get_trie_stats():
    """Return current trie statistics."""
    stats = trie.get_stats()
    return TrieStats(
        total_nodes=stats["total_nodes"],
        total_words=stats["total_words"],
        memory_estimate_mb=stats["memory_estimate_mb"],
        last_rebuilt=datetime.fromtimestamp(stats["last_rebuilt"], tz=timezone.utc)
        if stats["last_rebuilt"]
        else None,
    )


@router.post("/trie/rebuild")
def trigger_rebuild(db: Session = Depends(get_db)):
    """Manually trigger a trie rebuild (admin endpoint)."""
    rebuild_global_trie(db)
    stats = trie.get_stats()
    return {
        "status": "rebuilt",
        "total_words": stats["total_words"],
        "total_nodes": stats["total_nodes"],
    }
```

The `/health` endpoint includes whether the trie is loaded, so the load balancer can avoid routing traffic to instances that haven't finished building their trie yet. `/trie/stats` provides observability into memory usage and freshness. `/trie/rebuild` is an admin endpoint that triggers an immediate rebuild — useful after importing new data or blocking offensive terms.

### `app/main.py`

```python
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine, SessionLocal
from app.routers import suggest, admin
from app.trie.builder import rebuild_global_trie
from app.workers.aggregator import start_aggregator_thread

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: create tables, build trie, start background workers."""
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)

    # Build the initial trie from database
    db = SessionLocal()
    try:
        logger.info("Building initial trie...")
        rebuild_global_trie(db)
        logger.info("Trie built successfully.")
    finally:
        db.close()

    # Start the periodic aggregator/rebuild thread
    start_aggregator_thread()

    yield

    logger.info("Shutting down...")


app = FastAPI(
    title="Search Autocomplete API",
    description="A typeahead suggestion service with trie-based prefix matching",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(suggest.router, tags=["Suggestions"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
```

`lifespan` runs startup code before the app accepts requests. It creates database tables, builds the initial trie from existing frequency data, and starts the background aggregator thread. The trie build happens synchronously at startup to ensure the service has suggestions ready before the load balancer starts routing traffic. `include_router` mounts the suggestion and admin routers with appropriate prefixes and tags for the OpenAPI docs.

---

## Ranking & Relevance

Raw frequency alone produces poor suggestions. A query searched 1 million times five years ago is not more relevant than one searched 10,000 times today. A good ranking system combines multiple signals.

### Scoring Signals

| Signal              | Weight | Purpose                                              |
| ------------------- | ------ | ---------------------------------------------------- |
| Frequency           | 0.4    | All-time search count (popularity)                   |
| Recency             | 0.3    | Time-decayed score (trending queries rank higher)    |
| Prefix Match Length | 0.2    | Longer prefix matches are more relevant              |
| Personalization     | 0.1    | Boost queries from the user's own search history     |

### `app/ranking/scorer.py`

```python
import math
import time


def compute_score(
    frequency: int,
    last_updated_ts: float,
    prefix_length: int,
    query_length: int,
    is_personal: bool = False,
) -> float:
    """
    Compute a composite relevance score for a suggestion.

    Combines frequency, time-decay (recency), prefix match ratio,
    and an optional personalization boost.
    """
    # 1. Frequency score: log-scaled to dampen very popular queries
    freq_score = math.log1p(frequency)

    # 2. Recency score: exponential decay with half-life of 7 days
    age_seconds = time.time() - last_updated_ts
    half_life = 7 * 24 * 3600                     # 7 days in seconds
    recency_score = math.pow(0.5, age_seconds / half_life)

    # 3. Prefix match ratio: longer match = higher relevance
    match_ratio = prefix_length / max(query_length, 1)

    # 4. Personalization boost
    personal_boost = 1.5 if is_personal else 1.0

    # Weighted combination
    score = (
        0.4 * freq_score
        + 0.3 * recency_score
        + 0.2 * match_ratio
        + 0.1 * personal_boost
    )

    return round(score, 6)


def time_decay_frequency(frequency: int, last_updated_ts: float) -> int:
    """
    Apply time decay to a raw frequency count.
    Used during trie building to pre-rank queries.
    Queries that haven't been searched recently get
    their effective frequency reduced.
    """
    age_seconds = time.time() - last_updated_ts
    half_life = 14 * 24 * 3600                    # 14-day half-life for builds
    decay_factor = math.pow(0.5, age_seconds / half_life)
    return max(1, int(frequency * decay_factor))
```

`math.log1p(frequency)` applies a logarithmic scale so that the difference between 1M and 2M searches matters much less than the difference between 10 and 100. This prevents mega-popular queries from permanently dominating suggestions. The recency score uses exponential decay with a 7-day half-life: a query's recency score halves every 7 days. This means a query searched 1000 times a week ago scores similarly to one searched 2000 times two weeks ago, naturally surfacing trending queries. The `match_ratio` rewards queries where the typed prefix covers a larger fraction of the total query — typing "machine le" should rank "machine learning" higher than "machine learning algorithms for advanced natural language processing". `time_decay_frequency` is used during trie builds to pre-adjust frequencies before insertion.

---

## Caching with Redis

At 100K QPS, even an O(L) trie lookup adds up. Since popular prefixes like "how", "what", "whe" are typed by thousands of users, caching prefix → suggestions in Redis avoids redundant trie traversals.

### `app/cache.py`

```python
import json
import logging

import redis

from app.config import settings

logger = logging.getLogger(__name__)

redis_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,                        # Return strings instead of bytes
    socket_connect_timeout=5,                     # Timeout for initial connection
    retry_on_timeout=True,                        # Auto-retry on timeout
)

CACHE_PREFIX = "suggest:"


def get_cached_suggestions(prefix: str) -> list[str] | None:
    """Retrieve cached suggestions for a prefix from Redis."""
    try:
        data = redis_client.get(f"{CACHE_PREFIX}{prefix}")
        if data:
            return json.loads(data)
    except redis.RedisError as e:
        logger.warning(f"Redis read error: {e}")
    return None


def set_cached_suggestions(prefix: str, suggestions: list[str]) -> None:
    """Cache suggestions for a prefix in Redis with TTL."""
    try:
        data = json.dumps(suggestions)
        redis_client.setex(
            f"{CACHE_PREFIX}{prefix}",
            settings.cache_ttl_seconds,
            data,
        )
    except redis.RedisError as e:
        logger.warning(f"Redis write error: {e}")


def invalidate_prefix_cache(prefix: str) -> None:
    """
    Invalidate all cached entries that start with the given prefix.
    Used after trie rebuilds to ensure fresh suggestions.
    """
    try:
        cursor = 0
        pattern = f"{CACHE_PREFIX}{prefix}*"
        while True:
            cursor, keys = redis_client.scan(cursor, match=pattern, count=1000)
            if keys:
                redis_client.delete(*keys)
            if cursor == 0:
                break
    except redis.RedisError as e:
        logger.warning(f"Redis invalidation error: {e}")


def invalidate_all_suggestions() -> None:
    """Flush all suggestion caches. Called after trie rebuild."""
    try:
        cursor = 0
        pattern = f"{CACHE_PREFIX}*"
        while True:
            cursor, keys = redis_client.scan(cursor, match=pattern, count=1000)
            if keys:
                redis_client.delete(*keys)
            if cursor == 0:
                break
    except redis.RedisError as e:
        logger.warning(f"Redis flush error: {e}")
```

`redis.from_url` parses the connection string and creates a connection pool automatically. `decode_responses=True` makes Redis return Python strings instead of bytes. `setex` sets a key with an expiration time in seconds. The `suggest:` prefix namespaces cache keys to avoid collisions. All Redis operations are wrapped in try/except blocks — if Redis is down, the service degrades gracefully by falling back to the trie instead of crashing. `invalidate_all_suggestions` uses `SCAN` instead of `KEYS` to avoid blocking Redis. `SCAN` iterates incrementally with a cursor, processing 1000 keys per batch, which is safe for production use.

---

## Trie Persistence & Rebuild

The trie lives in memory and is rebuilt periodically from the database. This section covers the background workers that aggregate raw query logs into frequencies and rebuild the trie.

### `app/workers/aggregator.py`

```python
import logging
import threading
import time

from sqlalchemy import func, text

from app.cache import invalidate_all_suggestions
from app.config import settings
from app.database import SessionLocal
from app.models import QueryFrequency, QueryLog
from app.trie.builder import rebuild_global_trie

logger = logging.getLogger(__name__)


def aggregate_query_frequencies() -> None:
    """
    Aggregate raw query logs into the frequency table.
    Uses an UPSERT (INSERT ON CONFLICT UPDATE) to atomically
    increment frequency counts.
    """
    db = SessionLocal()
    try:
        # Count queries logged since the last aggregation
        # Uses raw SQL for the UPSERT which is PostgreSQL-specific
        upsert_sql = text("""
            INSERT INTO query_frequencies (query_text, frequency, last_updated)
            SELECT
                query_text,
                COUNT(*) as cnt,
                MAX(searched_at) as latest
            FROM query_logs
            WHERE searched_at > (
                SELECT COALESCE(MAX(last_updated), '1970-01-01'::timestamptz)
                FROM query_frequencies
            )
            GROUP BY query_text
            ON CONFLICT (query_text)
            DO UPDATE SET
                frequency = query_frequencies.frequency + EXCLUDED.frequency,
                last_updated = EXCLUDED.last_updated
        """)

        result = db.execute(upsert_sql)
        db.commit()
        logger.info(f"Aggregation complete. Rows affected: {result.rowcount}")

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        db.rollback()
    finally:
        db.close()


def periodic_rebuild() -> None:
    """
    Background thread that periodically aggregates logs and rebuilds the trie.
    Runs every TRIE_REBUILD_INTERVAL_MINUTES.
    """
    interval = settings.trie_rebuild_interval_minutes * 60

    while True:
        time.sleep(interval)

        try:
            logger.info("Starting periodic aggregation + rebuild...")

            # Step 1: Aggregate new query logs into frequencies
            aggregate_query_frequencies()

            # Step 2: Rebuild the trie from updated frequencies
            db = SessionLocal()
            try:
                rebuild_global_trie(db)
            finally:
                db.close()

            # Step 3: Invalidate stale cache entries
            invalidate_all_suggestions()

            logger.info("Periodic rebuild complete.")

        except Exception as e:
            logger.error(f"Periodic rebuild failed: {e}")


def start_aggregator_thread() -> None:
    """Start the aggregator as a daemon thread."""
    thread = threading.Thread(target=periodic_rebuild, daemon=True)
    thread.start()
    logger.info(
        f"Aggregator thread started. "
        f"Rebuild interval: {settings.trie_rebuild_interval_minutes} minutes."
    )
```

The aggregation query uses PostgreSQL's `INSERT ... ON CONFLICT ... DO UPDATE` (UPSERT) to atomically merge new query counts into the frequency table. It only processes logs newer than the last aggregation timestamp, avoiding reprocessing. The periodic rebuild runs as a daemon thread that sleeps for the configured interval, then: (1) aggregates new logs, (2) rebuilds the trie, (3) invalidates the Redis cache so stale suggestions are not served. The thread is marked `daemon=True` so it stops automatically when the main process exits. This three-step cycle ensures suggestions stay fresh within the configured interval (default 15 minutes).

### `app/workers/collector.py`

```python
import logging
import queue
import threading

from app.database import SessionLocal
from app.models import QueryLog

logger = logging.getLogger(__name__)

# In-memory buffer for async query recording
_query_buffer: queue.Queue = queue.Queue(maxsize=10_000)


def enqueue_query(query_text: str, user_id: str | None, ip_address: str) -> None:
    """
    Add a query to the in-memory buffer for async batch insertion.
    If the buffer is full, the query is dropped (non-blocking).
    """
    try:
        _query_buffer.put_nowait({
            "query_text": query_text,
            "user_id": user_id,
            "ip_address": ip_address,
        })
    except queue.Full:
        logger.warning("Query buffer full, dropping query.")


def flush_buffer() -> None:
    """Flush the query buffer to PostgreSQL in a batch insert."""
    entries = []
    while not _query_buffer.empty():
        try:
            entries.append(_query_buffer.get_nowait())
        except queue.Empty:
            break

    if not entries:
        return

    db = SessionLocal()
    try:
        db.bulk_insert_mappings(QueryLog, entries)
        db.commit()
        logger.info(f"Flushed {len(entries)} queries to database.")
    except Exception as e:
        logger.error(f"Flush failed: {e}")
        db.rollback()
    finally:
        db.close()


def start_collector_thread(flush_interval: int = 10) -> None:
    """
    Start a background thread that flushes the query buffer
    to PostgreSQL every `flush_interval` seconds.
    """
    def _run():
        while True:
            import time
            time.sleep(flush_interval)
            flush_buffer()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    logger.info(f"Collector thread started. Flush interval: {flush_interval}s.")
```

The collector uses an in-memory queue as a write buffer. Instead of inserting each query individually (one INSERT per request), queries are buffered and flushed in batches every 10 seconds using `bulk_insert_mappings`. This reduces PostgreSQL load from 10K individual INSERTs/sec to a few bulk inserts per minute. `put_nowait` is non-blocking — if the buffer is full (10K entries), the query is dropped silently rather than slowing down the API response. This is an acceptable trade-off because losing a small percentage of query logs does not significantly affect suggestion quality.

---

## Rate Limiting & Abuse Prevention

Autocomplete endpoints are especially vulnerable to abuse because they are called on every keystroke. Without rate limiting, a bot could hammer the service with millions of prefix queries.

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
        # Skip rate limiting for health checks and admin
        if request.url.path.startswith("/admin"):
            return await call_next(request)

        client_ip = request.client.host
        key = f"rate:{client_ip}"
        now = time.time()
        window = 60                               # 60-second sliding window

        pipe = redis_client.pipeline()
        pipe.zremrangebyscore(key, 0, now - window)
        pipe.zadd(key, {str(now): now})
        pipe.zcard(key)
        pipe.expire(key, window)
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

A **sliding window** with Redis sorted sets is more accurate than fixed-window counters. Each request timestamp is stored as both the member and score in a sorted set keyed by client IP. `zremrangebyscore` removes entries older than 60 seconds, `zadd` adds the current request, and `zcard` counts the remaining entries. `pipeline()` sends all four commands in a single round trip, reducing latency. The `X-RateLimit-*` response headers inform the client of their remaining quota. The rate limit is set to 300 requests per minute per IP, which is generous enough for normal typing (10 keystrokes × 5 queries/minute = 50 requests) but blocks automated scrapers.

### Register the Middleware

Add to `app/main.py`:

```python
from app.rate_limiter import RateLimiterMiddleware

app.add_middleware(RateLimiterMiddleware)
```

---

## Filtering & Content Moderation

An autocomplete system that suggests offensive, NSFW, or illegal content is a liability. Filtering happens at two levels: during trie builds (block known bad queries) and at query time (catch anything that slipped through).

### `app/moderation/filter.py`

```python
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Load blocklist from file
_blocklist: set[str] = set()
_blocklist_patterns: list[re.Pattern] = []


def load_blocklist(filepath: str = "data/blocklist.txt") -> None:
    """
    Load blocked terms from a file.
    Each line is either an exact term or a regex pattern (prefixed with 'regex:').
    """
    global _blocklist, _blocklist_patterns

    path = Path(filepath)
    if not path.exists():
        logger.warning(f"Blocklist file not found: {filepath}")
        return

    exact_terms = set()
    patterns = []

    with open(path) as f:
        for line in f:
            term = line.strip().lower()
            if not term or term.startswith("#"):
                continue

            if term.startswith("regex:"):
                pattern = term[6:].strip()
                try:
                    patterns.append(re.compile(pattern, re.IGNORECASE))
                except re.error as e:
                    logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            else:
                exact_terms.add(term)

    _blocklist = exact_terms
    _blocklist_patterns = patterns
    logger.info(
        f"Loaded blocklist: {len(exact_terms)} exact terms, "
        f"{len(patterns)} regex patterns."
    )


def is_blocked(query: str) -> bool:
    """Check if a query matches any blocked term or pattern."""
    query_lower = query.lower().strip()

    # Exact match
    if query_lower in _blocklist:
        return True

    # Check each word in the query
    words = query_lower.split()
    for word in words:
        if word in _blocklist:
            return True

    # Regex pattern match
    for pattern in _blocklist_patterns:
        if pattern.search(query_lower):
            return True

    return False


def filter_suggestions(suggestions: list[str]) -> list[str]:
    """Remove blocked suggestions from a list."""
    return [s for s in suggestions if not is_blocked(s)]
```

The blocklist supports both exact terms and regex patterns for flexibility. Exact matches use a `set` for O(1) lookup. Regex patterns handle variations (e.g., `regex:h[o0]w\s*t[o0]\s*hack` catches leetspeak substitutions). The blocklist file uses `#` for comments and `regex:` prefix for patterns. `filter_suggestions` is called after trie lookups to catch any queries that weren't blocked during the build phase. In production, you would also integrate with a machine learning content classifier for more nuanced moderation.

### `data/blocklist.txt`

```text
# Offensive terms (examples only — use a comprehensive list in production)
# Lines starting with # are comments
# Lines starting with regex: are treated as regex patterns

# Exact match terms
offensive_term_1
offensive_term_2

# Regex patterns for variations
regex:how\s+to\s+(hack|crack)
regex:buy\s+(drugs|weapons)
```

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
      - DATABASE_URL=postgresql://user:pass@db:5432/autocomplete
      - REDIS_URL=redis://cache:6379/0
      - TOP_K=10
      - TRIE_REBUILD_INTERVAL_MINUTES=15
      - MIN_QUERY_FREQUENCY=5
      - CACHE_TTL_SECONDS=300
      - RATE_LIMIT_PER_MINUTE=300
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    volumes:
      - ./data:/app/data
    restart: unless-stopped

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=autocomplete
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d autocomplete"]
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

`depends_on` with `condition: service_healthy` ensures the API container waits until PostgreSQL and Redis pass their health checks before starting. `pg_isready` is a PostgreSQL utility that checks if the server is accepting connections. `--appendonly yes` enables Redis AOF (Append-Only File) persistence. `--maxmemory 256mb` caps Redis memory usage and `--maxmemory-policy allkeys-lru` evicts least recently used keys when the limit is reached. `redis:7-alpine` is a minimal Redis image based on Alpine Linux. The `./data:/app/data` bind mount makes the blocklist file available to the container. `restart: unless-stopped` automatically restarts the container unless you explicitly stop it.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Record some queries to seed the system
curl -X POST http://localhost:8000/queries \
  -H "Content-Type: application/json" \
  -d '{"query": "how to learn python"}'

curl -X POST http://localhost:8000/queries \
  -H "Content-Type: application/json" \
  -d '{"query": "how to learn javascript"}'

curl -X POST http://localhost:8000/queries \
  -H "Content-Type: application/json" \
  -d '{"query": "how to cook pasta"}'

# Trigger a trie rebuild (after recording enough queries)
curl -X POST http://localhost:8000/admin/trie/rebuild

# Get suggestions
curl "http://localhost:8000/suggest?q=how+to"

# Check trie stats
curl http://localhost:8000/admin/trie/stats
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
from app.trie.trie import Trie, trie
import app.trie.trie as trie_mod

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
def sample_trie():
    """Create a trie pre-loaded with test data."""
    t = Trie()
    test_data = [
        ("how to learn python", 10000),
        ("how to learn javascript", 8000),
        ("how to cook pasta", 6000),
        ("how to cook rice", 5000),
        ("how are you", 15000),
        ("hello world", 20000),
        ("help me", 3000),
        ("machine learning", 12000),
        ("machine learning tutorial", 9000),
        ("machine learning python", 7000),
    ]
    for word, freq in test_data:
        t.insert(word, freq)
    t.precompute_top_k(5)
    return t
```

Tests use SQLite instead of PostgreSQL for speed and isolation. `dependency_overrides` is a FastAPI feature that lets you swap out dependencies during testing. Every test gets a fresh database because `setup_db` creates tables before and drops them after each test. The `sample_trie` fixture provides a pre-built trie for unit testing trie operations without hitting the database.

### `tests/test_trie.py`

```python
def test_insert_and_search(sample_trie):
    """Test that inserted words can be found by prefix."""
    node = sample_trie.search_prefix("how")
    assert node is not None

    suggestions = sample_trie.get_suggestions("how", k=5)
    assert len(suggestions) > 0
    assert all(s[0].startswith("how") for s in suggestions)


def test_suggestions_ordered_by_frequency(sample_trie):
    """Test that suggestions are returned in descending frequency order."""
    suggestions = sample_trie.get_suggestions("how", k=5)
    frequencies = [freq for _, freq in suggestions]
    assert frequencies == sorted(frequencies, reverse=True)


def test_prefix_not_found(sample_trie):
    """Test empty result for non-existent prefix."""
    suggestions = sample_trie.get_suggestions("xyz")
    assert suggestions == []


def test_exact_match_included(sample_trie):
    """Test that an exact match is included in suggestions."""
    suggestions = sample_trie.get_suggestions("hello world")
    texts = [text for text, _ in suggestions]
    assert "hello world" in texts


def test_top_k_limit(sample_trie):
    """Test that at most K suggestions are returned."""
    suggestions = sample_trie.get_suggestions("how", k=2)
    assert len(suggestions) <= 2


def test_case_insensitive(sample_trie):
    """Test that search is case-insensitive."""
    suggestions = sample_trie.get_suggestions("HOW")
    assert len(suggestions) > 0


def test_precomputed_suggestions(sample_trie):
    """Test that precomputed suggestions exist at internal nodes."""
    node = sample_trie.search_prefix("mac")
    assert node is not None
    assert len(node.top_suggestions) > 0


def test_empty_prefix():
    """Test behavior with empty trie."""
    empty_trie = Trie()
    suggestions = empty_trie.get_suggestions("anything")
    assert suggestions == []


def test_trie_stats(sample_trie):
    """Test trie statistics."""
    stats = sample_trie.get_stats()
    assert stats["total_words"] == 10
    assert stats["total_nodes"] > 0
    assert stats["memory_estimate_mb"] > 0


# Import at top of file
from app.trie.trie import Trie
```

### `tests/test_suggest.py`

```python
def test_suggest_endpoint(client):
    """Test the suggestion endpoint returns valid response."""
    response = client.get("/suggest?q=test")
    assert response.status_code == 200
    data = response.json()
    assert "prefix" in data
    assert "suggestions" in data
    assert isinstance(data["suggestions"], list)


def test_suggest_empty_prefix(client):
    """Test that empty prefix is rejected."""
    response = client.get("/suggest?q=")
    assert response.status_code == 422


def test_suggest_long_prefix(client):
    """Test that overly long prefix is rejected."""
    long_prefix = "a" * 200
    response = client.get(f"/suggest?q={long_prefix}")
    assert response.status_code == 422


def test_record_query(client):
    """Test recording a completed search query."""
    response = client.post(
        "/queries",
        json={"query": "test query"},
    )
    assert response.status_code == 201
    assert response.json()["status"] == "recorded"


def test_record_empty_query(client):
    """Test that empty query is rejected."""
    response = client.post(
        "/queries",
        json={"query": ""},
    )
    assert response.status_code == 422


def test_health_check(client):
    """Test health endpoint."""
    response = client.get("/admin/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_trie_stats_endpoint(client):
    """Test trie stats endpoint."""
    response = client.get("/admin/trie/stats")
    assert response.status_code == 200
    data = response.json()
    assert "total_nodes" in data
    assert "total_words" in data
```

### `tests/test_ranking.py`

```python
import time

from app.ranking.scorer import compute_score, time_decay_frequency


def test_higher_frequency_scores_higher():
    """Test that higher frequency produces higher score."""
    now = time.time()
    score_high = compute_score(10000, now, 3, 10)
    score_low = compute_score(100, now, 3, 10)
    assert score_high > score_low


def test_recent_scores_higher():
    """Test that more recent queries score higher."""
    now = time.time()
    week_ago = now - 7 * 24 * 3600
    score_recent = compute_score(1000, now, 3, 10)
    score_old = compute_score(1000, week_ago, 3, 10)
    assert score_recent > score_old


def test_longer_match_scores_higher():
    """Test that longer prefix match ratios score higher."""
    now = time.time()
    score_long = compute_score(1000, now, 8, 10)      # 80% match
    score_short = compute_score(1000, now, 2, 10)     # 20% match
    assert score_long > score_short


def test_personalization_boost():
    """Test that personalized results get a boost."""
    now = time.time()
    score_personal = compute_score(1000, now, 3, 10, is_personal=True)
    score_generic = compute_score(1000, now, 3, 10, is_personal=False)
    assert score_personal > score_generic


def test_time_decay_frequency_reduces():
    """Test that time decay reduces effective frequency."""
    now = time.time()
    month_ago = now - 30 * 24 * 3600
    decayed = time_decay_frequency(10000, month_ago)
    assert decayed < 10000


def test_time_decay_frequency_minimum():
    """Test that decayed frequency never goes below 1."""
    very_old = time.time() - 365 * 24 * 3600
    decayed = time_decay_frequency(1, very_old)
    assert decayed >= 1
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_trie"                         # Run only trie tests
pytest tests/test_ranking.py                  # Run ranking tests only
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
gcloud sql instances create autocomplete-db \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1 \
  --storage-size=10GB \
  --storage-auto-increase

# Create the database
gcloud sql databases create autocomplete \
  --instance=autocomplete-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=autocomplete-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-f1-micro` is the smallest (cheapest) instance for development. `--storage-auto-increase` automatically adds storage when the disk fills up. In production, use `--tier=db-custom-2-7680` or higher with `--availability-type=REGIONAL` for high availability with automatic failover.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create autocomplete-cache \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default

# Get the Redis host IP
gcloud redis instances describe autocomplete-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=1` provisions 1 GB of memory. Memorystore is a fully managed Redis service; you don't need to handle patching, backups, or failover. The Redis instance is accessible only from within the same VPC network, which secures it without additional firewall rules.

### Build & Push the Docker Image

```bash
# Build and push to Google Container Registry
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/search-autocomplete

# Or use Artifact Registry (recommended)
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/search-autocomplete/api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build, Google's managed CI/CD service. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy to Cloud Run

```bash
gcloud run deploy search-autocomplete \
  --image gcr.io/YOUR_PROJECT_ID/search-autocomplete \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 4Gi \
  --cpu 2 \
  --min-instances 1 \
  --max-instances 10 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:autocomplete-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/autocomplete?host=/cloudsql/YOUR_PROJECT_ID:us-central1:autocomplete-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
TOP_K=10,\
TRIE_REBUILD_INTERVAL_MINUTES=15,\
MIN_QUERY_FREQUENCY=5,\
CACHE_TTL_SECONDS=300,\
RATE_LIMIT_PER_MINUTE=300"
```

`--memory 4Gi` is higher than typical services because the trie lives in memory (20 GB for the full dataset, but starts smaller). `--cpu 2` provides two vCPUs for the trie precomputation step which is CPU-intensive. `--min-instances 1` keeps at least one instance warm to avoid cold starts — cold starts for this service are especially painful because the trie must be rebuilt from scratch. `--max-instances 10` caps the number of instances to control costs. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy for secure IAM-based database connections. The `?host=/cloudsql/...` in the DATABASE_URL tells SQLAlchemy to connect via the Unix socket created by the proxy.

### Schedule the Aggregation Job

```bash
# Create a Cloud Scheduler job to trigger aggregation every 15 minutes
gcloud scheduler jobs create http autocomplete-aggregate \
  --schedule="*/15 * * * *" \
  --uri="https://your-service-url.run.app/admin/trie/rebuild" \
  --http-method=POST \
  --oidc-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

`--schedule="*/15 * * * *"` runs every 15 minutes. While the service has its own internal rebuild thread, Cloud Scheduler provides an external trigger as a safety net in case the internal thread fails. `--oidc-service-account-email` authenticates the request using a service account, ensuring only Cloud Scheduler can trigger the rebuild endpoint.

### Set Up a Custom Domain

```bash
# Map a custom domain
gcloud run domain-mappings create \
  --service search-autocomplete \
  --domain suggest.yourdomain.com \
  --region us-central1
```

After running this command, GCP provides DNS records to add at your domain registrar. Once DNS propagates, GCP automatically provisions and renews SSL certificates via Let's Encrypt.

---

## Scaling & Production Considerations

### Data Layer Scaling

| Strategy              | When to Use                                   | How                                                 |
| --------------------- | --------------------------------------------- | --------------------------------------------------- |
| Read Replicas         | Analytics queries slow down the primary        | Cloud SQL read replicas for aggregation reads        |
| Table Partitioning    | `query_logs` grows past 1B rows               | Partition by `searched_at` month using range parts   |
| Log Rotation          | Old raw logs consume too much storage          | Archive logs older than 90 days to Cloud Storage     |
| Sharding              | Frequency table exceeds single-node capacity   | Shard by first character of `query_text`             |

### Trie Scaling

| Strategy              | When to Use                                   | How                                                 |
| --------------------- | --------------------------------------------- | --------------------------------------------------- |
| Trie Sharding         | Single trie exceeds instance memory            | Shard by prefix range (a-m on shard 1, n-z on shard 2) |
| Serialized Trie       | Rebuild time is too slow from DB               | Serialize trie to Cloud Storage, load on startup     |
| External Trie Service | Need trie shared across instances              | Dedicated trie service with gRPC, instances query it |
| Ternary Search Tree   | Memory too high with standard trie             | TST uses ~60% less memory than hash-map trie         |

### Cache Scaling

| Strategy              | When to Use                                   | How                                                 |
| --------------------- | --------------------------------------------- | --------------------------------------------------- |
| Redis Cluster         | Cache dataset exceeds single-node memory       | Memorystore cluster mode (auto-shards data)          |
| Local Cache + Redis   | Reduce Redis round trips further               | In-process LRU cache → Redis → Trie                 |
| Prefix Aggregation    | Too many unique prefixes to cache              | Cache only prefixes up to length 5, compute the rest |
| CDN Caching           | Global users, latency-sensitive                | Cache popular prefix responses at CDN edge           |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ suggest.xyz    │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud CDN      │
                     │ (prefix cache) │
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
       │ (Trie A-M)  │ │(A-M)   │ │ (Trie N-Z)  │
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
         │  │  (Async query log ingestion)  │   │
         │  └──────────┬────────────────────┘   │
         │             │                        │
         │  ┌──────────▼────────────────────┐   │
         │  │  Cloud Run Job                │   │
         │  │  (Aggregator + Trie Builder)  │   │
         │  └───────────────────────────────┘   │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Storage                │   │
         │  │  (Serialized Trie Snapshots)  │   │
         │  └───────────────────────────────┘   │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud CDN** caches popular prefix suggestions at edge locations worldwide. Since "how to" is typed millions of times, CDN-cached responses eliminate the need for the request to reach your API at all.
- **Cloud Pub/Sub** decouples query recording from the API path. The API publishes a message per completed search and returns immediately. A separate consumer batches and writes logs to PostgreSQL.
- **Cloud Run Job** runs the aggregation and trie build as a dedicated job, avoiding CPU contention with the serving instances. The built trie is serialized to Cloud Storage.
- **Cloud Storage** stores serialized trie snapshots. New Cloud Run instances load the latest snapshot on startup instead of rebuilding from the database, reducing cold start time from minutes to seconds.
- **Trie Sharding** splits the prefix space across instance groups (A-M, N-Z) so each instance needs only half the memory. The load balancer routes requests based on the first character of the prefix.
- **Read Replicas** handle aggregation queries without impacting the write path (query logging) on the primary database.

### Security Checklist

```
✓ Validate and sanitize all prefix inputs (reject control characters, scripts)
✓ Rate limit by IP and API key (300 req/min for anonymous, higher for authenticated)
✓ Implement content moderation (blocklist + ML classifier)
✓ Set CORS headers to restrict which domains can call the API
✓ Use HTTPS everywhere (Cloud Run provides this by default)
✓ Store secrets in Secret Manager, not environment variables
✓ Enable Cloud Audit Logs for compliance
✓ Log and alert on unusual query patterns (injection attempts, scraping)
✓ Sanitize prefix before trie lookup (prevent path traversal in any file-based trie)
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create autocomplete-health \
  --display-name="Autocomplete Health" \
  --uri="https://your-service-url.run.app/admin/health" \
  --period=60

# View Cloud Run metrics in the console
# → CPU utilization, request count, latency (p50, p95, p99), error rate, instance count

# Key metrics to alert on:
# - p99 suggestion latency > 50 ms
# - Error rate > 1% over 5 minutes
# - Trie age > 30 minutes (rebuild may be failing)
# - Cache hit ratio < 80% (trie is too cold or cache is misconfigured)
# - Instance count near max-instances
# - Cloud SQL CPU > 80%
# - Redis memory usage > 80%
# - Query recording drop rate > 5% (buffer overflow)
```

Cloud Run automatically exports metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing to identify bottlenecks across the API → Redis → Trie → PostgreSQL chain. Custom metrics for trie age, cache hit ratio, and buffer drop rate should be exported using the Cloud Monitoring client library.
