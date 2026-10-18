# Design Key-Value Store

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Storage Engine](#storage-engine)
5. [Core API – Put, Get, Delete](#core-api--put-get-delete)
6. [TTL & Expiration](#ttl--expiration)
7. [Consistent Hashing & Partitioning](#consistent-hashing--partitioning)
8. [Replication & Consistency](#replication--consistency)
9. [Write-Ahead Log (WAL)](#write-ahead-log-wal)
10. [Compaction & SSTable Merge](#compaction--sstable-merge)
11. [Docker & Docker Compose](#docker--docker-compose)
12. [Testing](#testing)
13. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
14. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement     | Description                                                           |
| --------------- | --------------------------------------------------------------------- |
| Put(key, value) | Store a key-value pair, overwrite if the key already exists           |
| Get(key)        | Retrieve the value for a given key                                    |
| Delete(key)     | Remove a key-value pair                                               |
| TTL             | Optionally set a time-to-live after which the key auto-expires        |
| Batch Ops       | Support multi-key get and put in a single request                     |
| Versioning      | Keep a version counter per key for conflict detection                 |

### Non-Functional Requirements

| Requirement      | Target                                                          |
| ---------------- | --------------------------------------------------------------- |
| Availability     | 99.99% uptime (AP system favoring availability over consistency)|
| Latency          | Read < 5 ms (cache hit) / < 20 ms (cache miss)                 |
| Durability       | No data loss after acknowledgment (WAL + replication)           |
| Scalability      | Handle 100K reads/sec, 50K writes/sec across the cluster        |
| Partition Tolerance | Continue serving reads/writes when nodes fail                |

### Back-of-the-Envelope Estimations

```
Key size                    ≈ 256 bytes (max)
Value size                  ≈ 64 KB (max, configurable)
Average record size         ≈ 1 KB (key + value + metadata)
Records stored              ≈ 1 billion
Total storage               ≈ 1 B × 1 KB = 1 TB
Replication factor          ≈ 3 → 3 TB total across cluster
Partitions (vnodes)         ≈ 256 per physical node
QPS reads                   ≈ 100 000 / sec
QPS writes                  ≈ 50 000 / sec
Memory for in-memory index  ≈ 1 B × 256 B = 256 GB (distributed)
```

A 256-byte key limit keeps the in-memory hash index compact. With a replication factor of 3, every write is stored on three nodes for durability. The consistent hash ring with 256 virtual nodes per physical node ensures even data distribution when nodes join or leave.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │ (SDK / HTTP) │
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │      Coordinator Node       │
                │     (FastAPI Gateway)        │
                │                              │
                │  PUT /kv/{key}  → store      │
                │  GET /kv/{key}  → retrieve   │
                │  DELETE /kv/{key} → remove   │
                └──────┬──────────────┬────────┘
                       │              │
          ┌────────────▼──┐    ┌──────▼────────┐
          │  Hash Ring     │    │   Metadata    │
          │ (Consistent    │    │   Store       │
          │  Hashing)      │    │ (PostgreSQL)  │
          └───┬────┬───┬───┘    └───────────────┘
              │    │   │
        ┌─────▼┐ ┌▼───▼┐ ┌──────┐
        │Node A│ │Node B│ │Node C│
        │(Store│ │(Store│ │(Store│
        │ shard│ │ shard│ │ shard│
        │  +   │ │  +   │ │  +   │
        │ WAL) │ │ WAL) │ │ WAL) │
        └──────┘ └──────┘ └──────┘
```

The client sends a request to the coordinator (FastAPI gateway). The coordinator hashes the key to determine which partition owns it, then routes the request to the responsible storage node. For writes, the coordinator sends the value to the primary node plus N-1 replicas. For reads, it can query the primary or any replica depending on the consistency level requested. PostgreSQL stores cluster metadata (node membership, partition map). Each storage node maintains a Write-Ahead Log (WAL) for crash recovery and uses an in-memory hash table backed by an on-disk append-only log.

---

## Project Setup

### Project Structure

```
kv-store/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models (metadata)
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # PostgreSQL connection and session
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── engine.py         # In-memory + disk storage engine
│   │   ├── wal.py            # Write-Ahead Log implementation
│   │   ├── compaction.py     # Background compaction / SSTable merge
│   │   └── memtable.py       # In-memory sorted buffer (MemTable)
│   ├── partitioning/
│   │   ├── __init__.py
│   │   ├── consistent_hash.py # Consistent hashing ring
│   │   └── replication.py    # Replication manager
│   └── routers/
│       ├── __init__.py
│       ├── kv.py             # Key-value CRUD endpoints
│       ├── admin.py          # Cluster admin endpoints
│       └── health.py         # Health check endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_storage.py       # Tests for storage engine
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
mkdir kv-store && cd kv-store
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
sortedcontainers==2.4.0
xxhash==3.5.0
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL metadata. `psycopg2-binary` is the PostgreSQL driver. `redis` is used for caching hot keys. `sortedcontainers` provides a sorted list for the MemTable, maintaining keys in order for efficient range scans and SSTable flushes. `xxhash` is a fast non-cryptographic hash used for consistent hashing and key partitioning, chosen over MD5/SHA for its speed at hashing short keys. `pydantic-settings` handles environment variable loading.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/kvstore
REDIS_URL=redis://localhost:6379/0
NODE_ID=node-1
DATA_DIR=/data/kv
WAL_DIR=/data/wal
REPLICATION_FACTOR=3
READ_QUORUM=2
WRITE_QUORUM=2
MAX_MEMTABLE_SIZE_MB=64
COMPACTION_THRESHOLD=4
PORT=8000
```

`DATABASE_URL` stores cluster metadata (node registry, partition map), not the actual key-value data. `NODE_ID` uniquely identifies this node in the cluster. `DATA_DIR` is where SSTables (sorted on-disk files) are stored. `WAL_DIR` is the directory for Write-Ahead Log files. `REPLICATION_FACTOR=3` means each key is stored on 3 nodes. `READ_QUORUM=2` and `WRITE_QUORUM=2` implement quorum-based consistency: a write succeeds when 2 of 3 replicas acknowledge, and a read returns once 2 of 3 replicas respond with the same value. `MAX_MEMTABLE_SIZE_MB=64` triggers a flush to disk when the in-memory buffer reaches 64 MB. `COMPACTION_THRESHOLD=4` triggers a merge when 4 or more SSTables accumulate.

---

## Storage Engine

A key-value storage engine needs to handle reads and writes efficiently. We implement a **Log-Structured Merge-Tree (LSM-Tree)** inspired architecture, similar to what LevelDB, RocksDB, and DynamoDB use internally.

### How It Works

```
Write Path:                              Read Path:

Client PUT(k, v)                         Client GET(k)
       │                                        │
       ▼                                        ▼
┌─────────────┐                          ┌─────────────┐
│  Write to   │                          │ Check       │
│  WAL (disk) │                          │ MemTable    │──→ found? return
│  (append)   │                          └──────┬──────┘
└──────┬──────┘                                 │ miss
       │                                        ▼
       ▼                                 ┌─────────────┐
┌─────────────┐                          │ Check       │
│  Write to   │                          │ SSTables    │──→ found? return
│  MemTable   │                          │ (newest     │
│  (in-memory)│                          │  first)     │
└──────┬──────┘                          └──────┬──────┘
       │                                        │ miss
       ▼ (when full)                            ▼
┌─────────────┐                          Return NOT_FOUND
│  Flush to   │
│  SSTable    │
│  (sorted    │
│   on disk)  │
└─────────────┘
```

Writes are always sequential appends (WAL then MemTable), making them very fast. Reads check the in-memory MemTable first, then scan SSTables from newest to oldest. Background compaction merges SSTables to keep read performance from degrading.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/kvstore"
    redis_url: str = "redis://localhost:6379/0"
    node_id: str = "node-1"
    data_dir: str = "/data/kv"
    wal_dir: str = "/data/wal"
    replication_factor: int = 3
    read_quorum: int = 2
    write_quorum: int = 2
    max_memtable_size_mb: int = 64
    compaction_threshold: int = 4
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
    pool_size=10,                             # Max persistent connections
    max_overflow=5,                           # Extra connections when pool is full
    pool_pre_ping=True,                       # Test connections before using them
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

`pool_size=10` keeps 10 connections open and reusable. PostgreSQL here stores only cluster metadata (node membership, partition map), not the actual key-value data. The key-value data lives in the LSM-Tree engine on each storage node. `pool_pre_ping=True` sends a lightweight query before each checkout to detect stale connections, important for cloud databases that close idle connections.

### `app/models.py`

```python
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Column, DateTime, Float, Integer, String, Text, Boolean
)

from app.database import Base


class ClusterNode(Base):
    """Registry of all nodes in the KV store cluster."""
    __tablename__ = "cluster_nodes"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    node_id = Column(String(64), unique=True, nullable=False, index=True)
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False)
    status = Column(String(20), default="active")       # active, draining, down
    virtual_nodes = Column(Integer, default=256)
    joined_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    last_heartbeat = Column(DateTime(timezone=True), nullable=True)


class PartitionMap(Base):
    """Mapping of hash ranges to nodes for consistent hashing metadata."""
    __tablename__ = "partition_map"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    hash_start = Column(BigInteger, nullable=False)
    hash_end = Column(BigInteger, nullable=False)
    node_id = Column(String(64), nullable=False, index=True)
    is_primary = Column(Boolean, default=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


class KeyMetadata(Base):
    """Metadata about stored keys for administrative purposes."""
    __tablename__ = "key_metadata"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    key = Column(String(256), nullable=False, index=True)
    version = Column(BigInteger, default=1)
    size_bytes = Column(BigInteger, default=0)
    node_id = Column(String(64), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)
    is_deleted = Column(Boolean, default=False)
```

`ClusterNode` tracks every node in the cluster with its status and virtual node count. `PartitionMap` records which hash ranges are owned by which nodes, enabling the coordinator to route requests correctly. `KeyMetadata` is an optional index for administrative queries like listing all keys, checking key ownership, and tracking versions. The actual key-value data is stored in the LSM-Tree engine, not in PostgreSQL.

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, Field


class KVPut(BaseModel):
    value: str                                    # Value to store (base64 for binary)
    ttl_seconds: int | None = None                # Optional TTL in seconds
    version: int | None = None                    # Optional expected version for CAS


class KVResponse(BaseModel):
    key: str
    value: str
    version: int
    created_at: datetime
    expires_at: datetime | None = None


class KVDeleteResponse(BaseModel):
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


class BatchResponse(BaseModel):
    results: dict[str, KVResponse | None]
    found: int
    not_found: int


class NodeInfo(BaseModel):
    node_id: str
    host: str
    port: int
    status: str
    virtual_nodes: int

    class Config:
        from_attributes = True


class ClusterStatus(BaseModel):
    total_nodes: int
    active_nodes: int
    total_keys: int
    nodes: list[NodeInfo]
```

`KVPut` accepts a string value (base64-encoded for binary data) and an optional TTL. The `version` field enables **Compare-And-Swap (CAS)**: the write succeeds only if the current version matches the expected version, preventing lost updates from concurrent writes. `BatchPutRequest` and `BatchGetRequest` limit to 100 items per batch to prevent oversized requests. `Field(..., max_length=100)` validates the list length at the schema layer.

---

## Core API – Put, Get, Delete

### `app/storage/engine.py`

```python
import json
import os
import time
import threading
from dataclasses import dataclass, field
from pathlib import Path

from sortedcontainers import SortedDict

from app.config import settings


@dataclass
class KVEntry:
    """A single key-value entry with metadata."""
    key: str
    value: str
    version: int = 1
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: int | None = None
    is_deleted: bool = False                      # Tombstone marker

    @property
    def expires_at(self) -> float | None:
        if self.ttl_seconds is not None:
            return self.timestamp + self.ttl_seconds
        return None

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "value": self.value,
            "version": self.version,
            "timestamp": self.timestamp,
            "ttl_seconds": self.ttl_seconds,
            "is_deleted": self.is_deleted,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "KVEntry":
        return cls(**data)


class MemTable:
    """
    In-memory sorted buffer that accumulates writes before flushing to disk.
    Uses a SortedDict for O(log n) inserts and ordered iteration.
    """

    def __init__(self, max_size_bytes: int = 64 * 1024 * 1024):
        self._data: SortedDict[str, KVEntry] = SortedDict()
        self._size_bytes = 0
        self._max_size_bytes = max_size_bytes
        self._lock = threading.RLock()

    def put(self, entry: KVEntry) -> bool:
        """
        Insert or update an entry. Returns True if the MemTable
        should be flushed (size exceeded).
        """
        with self._lock:
            entry_size = len(entry.key) + len(entry.value) + 64  # metadata overhead

            # Subtract old entry size if key exists
            if entry.key in self._data:
                old = self._data[entry.key]
                self._size_bytes -= len(old.key) + len(old.value) + 64

            self._data[entry.key] = entry
            self._size_bytes += entry_size

            return self._size_bytes >= self._max_size_bytes

    def get(self, key: str) -> KVEntry | None:
        """Retrieve an entry by key. Returns None if not found."""
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str, version: int) -> None:
        """Write a tombstone marker for the key."""
        with self._lock:
            tombstone = KVEntry(
                key=key,
                value="",
                version=version,
                is_deleted=True,
            )
            self._data[key] = tombstone

    def entries(self) -> list[KVEntry]:
        """Return all entries in sorted key order."""
        with self._lock:
            return list(self._data.values())

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @property
    def count(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        """Reset the MemTable."""
        with self._lock:
            self._data.clear()
            self._size_bytes = 0


class SSTable:
    """
    Sorted String Table – an immutable, sorted file of key-value entries.
    Each SSTable is written once and never modified. Compaction merges
    multiple SSTables into a new one.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._index: dict[str, int] = {}          # key → byte offset
        self._load_index()

    def _load_index(self):
        """Build an in-memory index of key → byte offset for fast lookups."""
        if not os.path.exists(self.filepath):
            return

        with open(self.filepath, "r") as f:
            offset = 0
            for line in f:
                entry = json.loads(line)
                self._index[entry["key"]] = offset
                offset = f.tell()

    def get(self, key: str) -> KVEntry | None:
        """Look up a key using the in-memory index, then seek to its position."""
        if key not in self._index:
            return None

        with open(self.filepath, "r") as f:
            f.seek(self._index[key])
            line = f.readline()
            if line:
                data = json.loads(line)
                entry = KVEntry.from_dict(data)
                if entry.is_deleted or entry.is_expired:
                    return None
                return entry
        return None

    def entries(self) -> list[KVEntry]:
        """Read all entries in sorted order."""
        result = []
        if not os.path.exists(self.filepath):
            return result

        with open(self.filepath, "r") as f:
            for line in f:
                entry = KVEntry.from_dict(json.loads(line))
                result.append(entry)
        return result

    @staticmethod
    def write(filepath: str, entries: list[KVEntry]):
        """Write sorted entries to a new SSTable file."""
        sorted_entries = sorted(entries, key=lambda e: e.key)
        with open(filepath, "w") as f:
            for entry in sorted_entries:
                f.write(json.dumps(entry.to_dict()) + "\n")

    @property
    def size_bytes(self) -> int:
        if os.path.exists(self.filepath):
            return os.path.getsize(self.filepath)
        return 0


class StorageEngine:
    """
    LSM-Tree inspired storage engine.
    Writes go to WAL → MemTable → flush to SSTable when full.
    Reads check MemTable → SSTables (newest first).
    """

    def __init__(self, data_dir: str = None, wal_dir: str = None):
        self.data_dir = data_dir or settings.data_dir
        self.wal_dir = wal_dir or settings.wal_dir
        self._memtable = MemTable(
            max_size_bytes=settings.max_memtable_size_mb * 1024 * 1024
        )
        self._sstables: list[SSTable] = []
        self._wal_path = os.path.join(self.wal_dir, "wal.log")
        self._sstable_counter = 0
        self._lock = threading.RLock()

        # Create directories
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.wal_dir).mkdir(parents=True, exist_ok=True)

        # Recovery: replay WAL and load existing SSTables
        self._load_sstables()
        self._replay_wal()

    def _load_sstables(self):
        """Load existing SSTables from disk, sorted by creation time (newest first)."""
        sstable_files = sorted(
            Path(self.data_dir).glob("sstable_*.jsonl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,                         # Newest first for read priority
        )
        for filepath in sstable_files:
            self._sstables.append(SSTable(str(filepath)))
            # Track counter to avoid filename collisions
            num = int(filepath.stem.split("_")[1])
            self._sstable_counter = max(self._sstable_counter, num + 1)

    def _replay_wal(self):
        """Replay the WAL to recover uncommitted writes after a crash."""
        if not os.path.exists(self._wal_path):
            return

        with open(self._wal_path, "r") as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    entry = KVEntry.from_dict(data)
                    self._memtable.put(entry)
                except (json.JSONDecodeError, KeyError):
                    continue                      # Skip corrupted WAL entries

    def _append_wal(self, entry: KVEntry):
        """Append an entry to the Write-Ahead Log."""
        with open(self._wal_path, "a") as f:
            f.write(json.dumps(entry.to_dict()) + "\n")
            f.flush()
            os.fsync(f.fileno())                  # Force write to disk

    def _flush_memtable(self):
        """Flush the current MemTable to a new SSTable on disk."""
        if self._memtable.count == 0:
            return

        entries = self._memtable.entries()
        filepath = os.path.join(
            self.data_dir, f"sstable_{self._sstable_counter:06d}.jsonl"
        )
        SSTable.write(filepath, entries)

        new_sstable = SSTable(filepath)
        self._sstables.insert(0, new_sstable)     # Newest first
        self._sstable_counter += 1

        # Clear the MemTable and WAL
        self._memtable.clear()
        if os.path.exists(self._wal_path):
            open(self._wal_path, "w").close()      # Truncate WAL

    def put(self, key: str, value: str, ttl_seconds: int | None = None,
            expected_version: int | None = None) -> KVEntry:
        """
        Store a key-value pair.
        If expected_version is provided, performs Compare-And-Swap (CAS).
        """
        with self._lock:
            # Get current version for CAS and version increment
            current = self.get(key)

            if expected_version is not None and current is not None:
                if current.version != expected_version:
                    raise ValueError(
                        f"Version conflict: expected {expected_version}, "
                        f"current is {current.version}"
                    )

            new_version = (current.version + 1) if current else 1

            entry = KVEntry(
                key=key,
                value=value,
                version=new_version,
                ttl_seconds=ttl_seconds,
            )

            # 1. Write to WAL (durability)
            self._append_wal(entry)

            # 2. Write to MemTable (performance)
            should_flush = self._memtable.put(entry)

            # 3. Flush if MemTable is full
            if should_flush:
                self._flush_memtable()

            return entry

    def get(self, key: str) -> KVEntry | None:
        """
        Retrieve a key. Checks MemTable first, then SSTables newest-to-oldest.
        Returns None if the key does not exist or has been deleted/expired.
        """
        # 1. Check MemTable (fastest)
        entry = self._memtable.get(key)
        if entry is not None:
            if entry.is_deleted or entry.is_expired:
                return None
            return entry

        # 2. Check SSTables (newest first)
        for sstable in self._sstables:
            entry = sstable.get(key)
            if entry is not None:
                return entry

        return None

    def delete(self, key: str) -> bool:
        """
        Delete a key by writing a tombstone.
        The actual data is removed during compaction.
        """
        with self._lock:
            current = self.get(key)
            if current is None:
                return False

            new_version = current.version + 1
            tombstone = KVEntry(
                key=key,
                value="",
                version=new_version,
                is_deleted=True,
            )

            self._append_wal(tombstone)
            self._memtable.delete(key, new_version)
            return True

    def batch_put(self, items: list[dict]) -> list[KVEntry]:
        """Store multiple key-value pairs atomically."""
        results = []
        with self._lock:
            for item in items:
                entry = self.put(
                    key=item["key"],
                    value=item["value"],
                    ttl_seconds=item.get("ttl_seconds"),
                )
                results.append(entry)
        return results

    def batch_get(self, keys: list[str]) -> dict[str, KVEntry | None]:
        """Retrieve multiple keys in a single call."""
        results = {}
        for key in keys:
            results[key] = self.get(key)
        return results

    def stats(self) -> dict:
        """Return storage engine statistics."""
        return {
            "memtable_entries": self._memtable.count,
            "memtable_size_bytes": self._memtable.size_bytes,
            "sstable_count": len(self._sstables),
            "sstable_total_bytes": sum(s.size_bytes for s in self._sstables),
            "data_dir": self.data_dir,
        }


# Global storage engine instance
storage = StorageEngine()
```

`KVEntry` is a dataclass representing a single record with key, value, version, timestamp, TTL, and a tombstone flag. Tombstones are markers that indicate a key has been deleted; the actual data is physically removed during compaction. `MemTable` uses `SortedDict` from `sortedcontainers` which maintains keys in sorted order with O(log n) inserts, enabling efficient ordered flushes to SSTables. `SSTable` is an immutable, sorted file where each line is a JSON-encoded entry. An in-memory index maps keys to byte offsets for O(1) lookups via `seek()`. `StorageEngine` orchestrates the write path (WAL → MemTable → SSTable) and read path (MemTable → SSTables newest-first). `os.fsync(f.fileno())` forces the operating system to write the WAL entry to the physical disk, guaranteeing durability even if the process crashes immediately after.

### `app/routers/kv.py`

```python
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query

from app.schemas import (
    KVPut, KVResponse, KVDeleteResponse,
    BatchPutRequest, BatchGetRequest, BatchResponse,
)
from app.storage.engine import storage

router = APIRouter()


@router.put("/{key}", response_model=KVResponse, status_code=200)
def put_key(key: str, payload: KVPut):
    """Store a key-value pair. Overwrites if the key already exists."""
    if len(key) > 256:
        raise HTTPException(status_code=400, detail="Key exceeds 256 character limit.")

    if len(payload.value) > 65536:
        raise HTTPException(
            status_code=400,
            detail="Value exceeds 64 KB limit.",
        )

    try:
        entry = storage.put(
            key=key,
            value=payload.value,
            ttl_seconds=payload.ttl_seconds,
            expected_version=payload.version,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return KVResponse(
        key=entry.key,
        value=entry.value,
        version=entry.version,
        created_at=datetime.fromtimestamp(entry.timestamp, tz=timezone.utc),
        expires_at=(
            datetime.fromtimestamp(entry.expires_at, tz=timezone.utc)
            if entry.expires_at else None
        ),
    )


@router.get("/{key}", response_model=KVResponse)
def get_key(key: str):
    """Retrieve the value for a given key."""
    entry = storage.get(key)

    if entry is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found.")

    return KVResponse(
        key=entry.key,
        value=entry.value,
        version=entry.version,
        created_at=datetime.fromtimestamp(entry.timestamp, tz=timezone.utc),
        expires_at=(
            datetime.fromtimestamp(entry.expires_at, tz=timezone.utc)
            if entry.expires_at else None
        ),
    )


@router.delete("/{key}", response_model=KVDeleteResponse)
def delete_key(key: str):
    """Delete a key-value pair (writes a tombstone)."""
    deleted = storage.delete(key)

    if not deleted:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found.")

    return KVDeleteResponse(
        key=key,
        deleted=True,
        message=f"Key '{key}' deleted successfully.",
    )


@router.post("/batch/put", response_model=list[KVResponse])
def batch_put(payload: BatchPutRequest):
    """Store multiple key-value pairs in a single request."""
    entries = storage.batch_put(payload.items)

    return [
        KVResponse(
            key=e.key,
            value=e.value,
            version=e.version,
            created_at=datetime.fromtimestamp(e.timestamp, tz=timezone.utc),
            expires_at=(
                datetime.fromtimestamp(e.expires_at, tz=timezone.utc)
                if e.expires_at else None
            ),
        )
        for e in entries
    ]


@router.post("/batch/get", response_model=BatchResponse)
def batch_get(payload: BatchGetRequest):
    """Retrieve multiple keys in a single request."""
    raw_results = storage.batch_get(payload.keys)
    results = {}
    found = 0
    not_found = 0

    for key, entry in raw_results.items():
        if entry is not None:
            results[key] = KVResponse(
                key=entry.key,
                value=entry.value,
                version=entry.version,
                created_at=datetime.fromtimestamp(entry.timestamp, tz=timezone.utc),
                expires_at=(
                    datetime.fromtimestamp(entry.expires_at, tz=timezone.utc)
                    if entry.expires_at else None
                ),
            )
            found += 1
        else:
            results[key] = None
            not_found += 1

    return BatchResponse(results=results, found=found, not_found=not_found)
```

`PUT /{key}` validates that the key is under 256 characters and the value under 64 KB before storing. If a `version` is provided in the request body, the engine performs Compare-And-Swap, returning `409 Conflict` if the stored version does not match. `DELETE /{key}` does not physically remove data; it writes a tombstone entry that signals the key is logically deleted. Tombstones are cleaned up during compaction. `batch/put` and `batch/get` handle up to 100 items per request to reduce round trips.

### `app/routers/admin.py`

```python
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import ClusterNode
from app.schemas import ClusterStatus, NodeInfo
from app.storage.engine import storage

router = APIRouter()


@router.get("/stats")
def get_stats():
    """Return storage engine statistics for this node."""
    return storage.stats()


@router.get("/cluster", response_model=ClusterStatus)
def get_cluster_status(db: Session = Depends(get_db)):
    """Return the status of all nodes in the cluster."""
    nodes = db.query(ClusterNode).all()
    active = [n for n in nodes if n.status == "active"]

    return ClusterStatus(
        total_nodes=len(nodes),
        active_nodes=len(active),
        total_keys=storage.stats()["memtable_entries"],
        nodes=[
            NodeInfo(
                node_id=n.node_id,
                host=n.host,
                port=n.port,
                status=n.status,
                virtual_nodes=n.virtual_nodes,
            )
            for n in nodes
        ],
    )
```

### `app/routers/health.py`

```python
from fastapi import APIRouter

from app.storage.engine import storage

router = APIRouter()


@router.get("/health")
def health_check():
    """Basic health check for load balancers."""
    return {"status": "healthy", "node": "node-1"}


@router.get("/health/detailed")
def detailed_health():
    """Detailed health check including storage engine stats."""
    stats = storage.stats()
    return {
        "status": "healthy",
        "storage": stats,
    }
```

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import kv, admin, health


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create metadata tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Distributed Key-Value Store API",
    description="A DynamoDB/Redis-inspired key-value store with LSM-Tree storage",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(kv.router, prefix="/kv", tags=["Key-Value"])
app.include_router(admin.router, prefix="/admin", tags=["Admin"])
app.include_router(health.router, tags=["Health"])
```

`lifespan` is an async context manager that runs setup code before the app starts accepting requests (`Base.metadata.create_all` creates missing metadata tables). `include_router` mounts the KV, admin, and health routers under their respective prefixes. All key-value operations are under `/kv`, admin/monitoring under `/admin`, and health checks at the root level for easy load balancer configuration.

---

## TTL & Expiration

Keys with a TTL are automatically expired. The in-memory check is instant (comparing timestamps), but expired data still exists on disk until compaction cleans it up.

### Passive Expiration (Lazy)

Passive expiration is already implemented in the `get()` path. When a key is read, the engine checks `is_expired` and returns `None` if the TTL has passed. This costs nothing extra because the check happens at read time.

### Active Expiration (Background Cleanup)

```python
# app/storage/expiration.py
import threading
import time

from app.storage.engine import storage


class ExpirationDaemon:
    """
    Background thread that periodically scans the MemTable for expired keys
    and writes tombstones, ensuring they are eventually cleaned up during
    compaction.
    """

    def __init__(self, interval_seconds: int = 60):
        self._interval = interval_seconds
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self):
        """Start the background expiration thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background expiration thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        """Periodically scan for expired keys."""
        while self._running:
            try:
                self._cleanup_expired()
            except Exception as e:
                print(f"Expiration cleanup error: {e}")
            time.sleep(self._interval)

    def _cleanup_expired(self):
        """Scan MemTable entries and mark expired keys as deleted."""
        expired_count = 0
        entries = storage._memtable.entries()

        for entry in entries:
            if entry.is_expired and not entry.is_deleted:
                storage.delete(entry.key)
                expired_count += 1

        if expired_count > 0:
            print(f"Expired {expired_count} keys during cleanup.")


expiration_daemon = ExpirationDaemon(interval_seconds=60)
```

The `ExpirationDaemon` runs as a daemon thread (does not block program exit) every 60 seconds. It scans the MemTable for entries past their TTL and writes tombstones for them. This ensures that expired keys do not consume MemTable memory indefinitely. The actual disk data for expired keys is cleaned up during compaction, which merges SSTables and discards tombstones older than a retention period.

### Register the Daemon in `app/main.py`

Add to the lifespan:

```python
from app.storage.expiration import expiration_daemon


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create metadata tables on startup, start background daemons."""
    Base.metadata.create_all(bind=engine)
    expiration_daemon.start()
    yield
    expiration_daemon.stop()
```

`expiration_daemon.start()` launches the background thread before the app starts serving requests. `expiration_daemon.stop()` gracefully shuts it down when the app is terminating. The `yield` separates startup logic (before) from shutdown logic (after).

---

## Consistent Hashing & Partitioning

In a distributed key-value store, data must be split across nodes. **Consistent hashing** maps keys to nodes using a hash ring, ensuring that when nodes join or leave, only a minimal fraction of keys need to be remapped.

### How Consistent Hashing Works

```
                    0 (2^32)
                    │
           N1-v3 ──┼── N2-v1
                   ╱│╲
          N3-v2 ──╱ │ ╲── N1-v1
                 ╱  │  ╲
        N2-v3 ──╱   │   ╲── N3-v1
               ╱    │    ╲
      N1-v2 ──╱     │     ╲── N2-v2
                    │
              N3-v3 ┘

    Key "user:42" → hash → lands between N2-v2 and N3-v3
    → Assigned to N3-v3 → Physical Node 3

    Adding Node 4: only keys between N4's vnodes and their
    predecessors need to move. ~1/N of total keys.
```

Each physical node places multiple **virtual nodes (vnodes)** on the hash ring. When a key is hashed, it is assigned to the next vnode clockwise on the ring, which maps to a physical node. Virtual nodes solve the problem of uneven distribution: with only physical nodes, some nodes might end up with far more keys than others. With 256 vnodes per node, the standard deviation of load drops to under 5%.

### `app/partitioning/consistent_hash.py`

```python
import bisect
import hashlib
from dataclasses import dataclass


@dataclass
class VirtualNode:
    """A virtual node on the hash ring."""
    physical_node: str                            # Physical node ID
    vnode_index: int                              # Virtual node index
    hash_value: int                               # Position on the ring


class ConsistentHashRing:
    """
    Consistent hash ring with virtual nodes.
    Uses MD5 hashing to distribute keys evenly across nodes.
    """

    def __init__(self, num_vnodes: int = 256):
        self._num_vnodes = num_vnodes
        self._ring: list[int] = []                # Sorted hash positions
        self._hash_to_vnode: dict[int, VirtualNode] = {}
        self._nodes: dict[str, list[VirtualNode]] = {}  # node_id → vnodes

    def _hash(self, key: str) -> int:
        """Hash a key to a 32-bit integer position on the ring."""
        digest = hashlib.md5(key.encode()).hexdigest()
        return int(digest[:8], 16)                # Use first 8 hex chars (32 bits)

    def add_node(self, node_id: str) -> list[tuple[int, int]]:
        """
        Add a physical node with virtual nodes to the ring.
        Returns a list of (start, end) hash ranges this node is now responsible for.
        """
        if node_id in self._nodes:
            return []

        vnodes = []
        affected_ranges = []

        for i in range(self._num_vnodes):
            vnode_key = f"{node_id}:vnode-{i}"
            hash_value = self._hash(vnode_key)

            vnode = VirtualNode(
                physical_node=node_id,
                vnode_index=i,
                hash_value=hash_value,
            )

            # Insert into sorted ring
            pos = bisect.bisect_left(self._ring, hash_value)
            self._ring.insert(pos, hash_value)
            self._hash_to_vnode[hash_value] = vnode
            vnodes.append(vnode)

        self._nodes[node_id] = vnodes
        return affected_ranges

    def remove_node(self, node_id: str) -> list[str]:
        """
        Remove a physical node and all its virtual nodes from the ring.
        Returns the list of keys that need to be reassigned.
        """
        if node_id not in self._nodes:
            return []

        for vnode in self._nodes[node_id]:
            pos = bisect.bisect_left(self._ring, vnode.hash_value)
            if pos < len(self._ring) and self._ring[pos] == vnode.hash_value:
                self._ring.pop(pos)
            del self._hash_to_vnode[vnode.hash_value]

        del self._nodes[node_id]
        return []

    def get_node(self, key: str) -> str | None:
        """
        Get the physical node responsible for a given key.
        Finds the first vnode clockwise from the key's hash position.
        """
        if not self._ring:
            return None

        hash_value = self._hash(key)
        pos = bisect.bisect_right(self._ring, hash_value)

        # Wrap around to the beginning of the ring
        if pos >= len(self._ring):
            pos = 0

        vnode = self._hash_to_vnode[self._ring[pos]]
        return vnode.physical_node

    def get_replica_nodes(self, key: str, n: int = 3) -> list[str]:
        """
        Get n distinct physical nodes for replication.
        Walks clockwise from the key's position, skipping vnodes
        that belong to an already-selected physical node.
        """
        if not self._ring:
            return []

        hash_value = self._hash(key)
        pos = bisect.bisect_right(self._ring, hash_value)

        replicas = []
        seen_nodes = set()

        for i in range(len(self._ring)):
            idx = (pos + i) % len(self._ring)
            vnode = self._hash_to_vnode[self._ring[idx]]

            if vnode.physical_node not in seen_nodes:
                replicas.append(vnode.physical_node)
                seen_nodes.add(vnode.physical_node)

            if len(replicas) >= n:
                break

        return replicas

    def get_node_count(self) -> int:
        """Return the number of physical nodes on the ring."""
        return len(self._nodes)

    def get_all_nodes(self) -> list[str]:
        """Return all physical node IDs."""
        return list(self._nodes.keys())


# Global hash ring instance
hash_ring = ConsistentHashRing(num_vnodes=256)
```

`_hash` uses MD5 truncated to 32 bits, producing values in `[0, 2^32)`. MD5 is chosen here not for security but for its uniform distribution. `bisect_right` finds the insertion point in the sorted ring, effectively finding the next vnode clockwise. The wrap-around (`if pos >= len(self._ring): pos = 0`) handles keys that hash past the last vnode by wrapping to the beginning of the ring. `get_replica_nodes` walks clockwise collecting distinct physical nodes, ensuring replicas land on different machines. This is critical: without the `seen_nodes` check, two replicas could land on virtual nodes of the same physical node, providing no redundancy.

### `app/partitioning/replication.py`

```python
import httpx

from app.config import settings
from app.partitioning.consistent_hash import hash_ring
from app.storage.engine import KVEntry


class ReplicationManager:
    """
    Manages replication of writes to multiple nodes.
    Implements quorum-based consistency (W + R > N).
    """

    def __init__(self):
        self._replication_factor = settings.replication_factor
        self._write_quorum = settings.write_quorum
        self._read_quorum = settings.read_quorum

    def get_target_nodes(self, key: str) -> list[str]:
        """Get the list of nodes that should store this key."""
        return hash_ring.get_replica_nodes(key, self._replication_factor)

    async def replicate_write(
        self, key: str, value: str, ttl_seconds: int | None = None
    ) -> dict:
        """
        Write to multiple replica nodes and wait for quorum.
        Returns success if at least write_quorum nodes acknowledge.
        """
        target_nodes = self.get_target_nodes(key)
        successes = 0
        failures = 0
        results = []

        for node_id in target_nodes:
            if node_id == settings.node_id:
                # Local write – skip network call
                from app.storage.engine import storage
                try:
                    entry = storage.put(key, value, ttl_seconds)
                    successes += 1
                    results.append({"node": node_id, "status": "ok"})
                except Exception as e:
                    failures += 1
                    results.append({"node": node_id, "status": "error", "error": str(e)})
            else:
                # Remote write
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        resp = await client.put(
                            f"http://{node_id}:8000/kv/{key}",
                            json={"value": value, "ttl_seconds": ttl_seconds},
                        )
                        if resp.status_code in (200, 201):
                            successes += 1
                            results.append({"node": node_id, "status": "ok"})
                        else:
                            failures += 1
                            results.append({"node": node_id, "status": "error"})
                except Exception as e:
                    failures += 1
                    results.append({"node": node_id, "status": "error", "error": str(e)})

        quorum_met = successes >= self._write_quorum

        return {
            "quorum_met": quorum_met,
            "successes": successes,
            "failures": failures,
            "results": results,
        }

    async def replicate_read(self, key: str) -> KVEntry | None:
        """
        Read from multiple replica nodes and return the most recent version.
        Waits for read_quorum responses.
        """
        target_nodes = self.get_target_nodes(key)
        responses: list[KVEntry | None] = []

        for node_id in target_nodes:
            if node_id == settings.node_id:
                from app.storage.engine import storage
                entry = storage.get(key)
                responses.append(entry)
            else:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        resp = await client.get(f"http://{node_id}:8000/kv/{key}")
                        if resp.status_code == 200:
                            data = resp.json()
                            responses.append(KVEntry(
                                key=data["key"],
                                value=data["value"],
                                version=data["version"],
                            ))
                        else:
                            responses.append(None)
                except Exception:
                    responses.append(None)

            # Check if we have enough responses
            valid = [r for r in responses if r is not None]
            if len(valid) >= self._read_quorum:
                # Return the entry with the highest version
                return max(valid, key=lambda e: e.version)

        # Not enough responses for quorum
        valid = [r for r in responses if r is not None]
        if valid:
            return max(valid, key=lambda e: e.version)
        return None


replication_manager = ReplicationManager()
```

`replicate_write` sends the write to all replica nodes and returns success once `write_quorum` nodes acknowledge. With `W=2` and `N=3`, one node can be down and writes still succeed. The `if node_id == settings.node_id` check avoids a network call when the coordinator is also a replica. `replicate_read` queries replica nodes and returns the entry with the highest version number, implementing **last-writer-wins** conflict resolution. With `R=2` and `W=2`, the quorum intersection (`R + W > N` → `2 + 2 > 3`) guarantees at least one replica in the read set has the latest write. `httpx.AsyncClient(timeout=5.0)` sets a 5-second timeout to prevent a slow replica from blocking the entire request.

---

## Write-Ahead Log (WAL)

The Write-Ahead Log ensures durability: every write is appended to an on-disk log before being applied to the in-memory MemTable. If the process crashes, the WAL is replayed on startup to recover all acknowledged writes.

### `app/storage/wal.py`

```python
import json
import os
import time
import threading
from pathlib import Path

from app.storage.engine import KVEntry


class WriteAheadLog:
    """
    Append-only log file for crash recovery.
    Every mutation (put, delete) is written here before being applied
    to the MemTable. On crash recovery, the WAL is replayed to
    reconstruct the MemTable.
    """

    def __init__(self, wal_dir: str, segment_size_mb: int = 64):
        self.wal_dir = wal_dir
        self._segment_size = segment_size_mb * 1024 * 1024
        self._current_segment: str | None = None
        self._current_size = 0
        self._segment_counter = 0
        self._lock = threading.Lock()

        Path(wal_dir).mkdir(parents=True, exist_ok=True)
        self._init_segment()

    def _init_segment(self):
        """Create or resume the current WAL segment."""
        existing = sorted(Path(self.wal_dir).glob("wal_*.log"))
        if existing:
            self._current_segment = str(existing[-1])
            self._current_size = existing[-1].stat().st_size
            num = int(existing[-1].stem.split("_")[1])
            self._segment_counter = num
        else:
            self._rotate_segment()

    def _rotate_segment(self):
        """Create a new WAL segment file."""
        self._segment_counter += 1
        self._current_segment = os.path.join(
            self.wal_dir, f"wal_{self._segment_counter:06d}.log"
        )
        self._current_size = 0
        # Create the file
        open(self._current_segment, "a").close()

    def append(self, entry: KVEntry) -> int:
        """
        Append an entry to the WAL. Returns the byte offset of the entry.
        Uses fsync to guarantee the write reaches disk.
        """
        with self._lock:
            record = json.dumps({
                "timestamp": time.time(),
                "operation": "delete" if entry.is_deleted else "put",
                "entry": entry.to_dict(),
                "checksum": self._checksum(entry),
            })

            line = record + "\n"
            line_bytes = line.encode("utf-8")

            # Rotate if current segment exceeds size limit
            if self._current_size + len(line_bytes) > self._segment_size:
                self._rotate_segment()

            with open(self._current_segment, "a") as f:
                offset = f.tell()
                f.write(line)
                f.flush()
                os.fsync(f.fileno())              # Guarantee durability

            self._current_size += len(line_bytes)
            return offset

    def replay(self) -> list[KVEntry]:
        """
        Replay all WAL segments in order for crash recovery.
        Returns the list of entries that should be applied to the MemTable.
        Skips corrupted entries.
        """
        entries = []
        segments = sorted(Path(self.wal_dir).glob("wal_*.log"))

        for segment in segments:
            with open(segment, "r") as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        entry = KVEntry.from_dict(record["entry"])

                        # Verify checksum
                        if self._checksum(entry) != record.get("checksum"):
                            print(
                                f"WAL checksum mismatch at {segment}:{line_num}, "
                                f"skipping."
                            )
                            continue

                        entries.append(entry)
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"WAL corruption at {segment}:{line_num}: {e}")
                        continue

        return entries

    def truncate(self):
        """
        Remove all WAL segments. Called after a successful MemTable flush
        since those entries are now safely in an SSTable.
        """
        with self._lock:
            for segment in Path(self.wal_dir).glob("wal_*.log"):
                os.remove(segment)
            self._rotate_segment()

    def _checksum(self, entry: KVEntry) -> str:
        """Compute a simple checksum for data integrity verification."""
        import hashlib
        data = f"{entry.key}:{entry.value}:{entry.version}:{entry.timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    @property
    def size_bytes(self) -> int:
        """Total size of all WAL segments."""
        total = 0
        for segment in Path(self.wal_dir).glob("wal_*.log"):
            total += segment.stat().st_size
        return total

    @property
    def segment_count(self) -> int:
        """Number of WAL segments."""
        return len(list(Path(self.wal_dir).glob("wal_*.log")))
```

The WAL is split into **segments** of configurable size (default 64 MB). When a segment fills up, a new one is created. This prevents a single WAL file from growing indefinitely and makes cleanup easier. `append` serializes the entry to JSON, appends it to the current segment, and calls `fsync` to flush the OS buffer to disk. `replay` reads all segments in order and reconstructs entries, skipping any that fail checksum verification. The `_checksum` uses SHA-256 truncated to 16 hex characters to detect corruption without adding significant overhead. `truncate` is called after a successful MemTable → SSTable flush, since those entries are now durable on disk in the SSTable.

---

## Compaction & SSTable Merge

As the system runs, SSTables accumulate on disk. Without compaction, read performance degrades because the engine must check more files. Compaction merges multiple SSTables into one, discarding tombstones and expired entries.

### `app/storage/compaction.py`

```python
import os
import time
import threading
from pathlib import Path

from app.config import settings
from app.storage.engine import SSTable, KVEntry


class CompactionManager:
    """
    Background compaction that merges multiple SSTables into a new, smaller one.
    Discards tombstones and expired entries during the merge.

    Implements size-tiered compaction: when the number of SSTables at a level
    exceeds the threshold, they are merged into a single larger SSTable.
    """

    def __init__(self, data_dir: str = None, threshold: int = None):
        self.data_dir = data_dir or settings.data_dir
        self._threshold = threshold or settings.compaction_threshold
        self._running = False
        self._thread: threading.Thread | None = None
        self._compaction_count = 0

    def start(self):
        """Start the background compaction thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background compaction thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)

    def _run(self):
        """Periodically check if compaction is needed."""
        while self._running:
            try:
                self._maybe_compact()
            except Exception as e:
                print(f"Compaction error: {e}")
            time.sleep(30)                        # Check every 30 seconds

    def _maybe_compact(self):
        """Trigger compaction if the number of SSTables exceeds the threshold."""
        sstable_files = sorted(
            Path(self.data_dir).glob("sstable_*.jsonl"),
            key=lambda p: p.stat().st_mtime,
        )

        if len(sstable_files) < self._threshold:
            return

        print(
            f"Compaction triggered: {len(sstable_files)} SSTables "
            f"(threshold: {self._threshold})"
        )

        # Merge all SSTables into one
        self._compact(sstable_files)

    def _compact(self, sstable_files: list[Path]):
        """
        Merge multiple SSTables into a single new SSTable.
        During the merge:
        - For duplicate keys, keep only the newest version
        - Discard tombstones (is_deleted=True)
        - Discard expired entries
        """
        # Read all entries from all SSTables
        all_entries: dict[str, KVEntry] = {}

        for filepath in sstable_files:
            sstable = SSTable(str(filepath))
            for entry in sstable.entries():
                existing = all_entries.get(entry.key)
                # Keep the entry with the highest version (newest wins)
                if existing is None or entry.version > existing.version:
                    all_entries[entry.key] = entry

        # Filter out tombstones and expired entries
        live_entries = [
            entry for entry in all_entries.values()
            if not entry.is_deleted and not entry.is_expired
        ]

        # Write the merged SSTable
        timestamp = int(time.time() * 1000)
        merged_path = os.path.join(
            self.data_dir, f"sstable_merged_{timestamp}.jsonl"
        )
        SSTable.write(merged_path, live_entries)

        # Remove old SSTables
        for filepath in sstable_files:
            os.remove(filepath)

        self._compaction_count += 1
        print(
            f"Compaction complete: merged {len(sstable_files)} SSTables "
            f"into 1 ({len(live_entries)} live entries)."
        )

    @property
    def stats(self) -> dict:
        """Return compaction statistics."""
        return {
            "compaction_count": self._compaction_count,
            "threshold": self._threshold,
        }


compaction_manager = CompactionManager()
```

Size-tiered compaction is the simplest strategy: when the number of SSTables reaches the threshold, merge them all into one. This is what Apache Cassandra uses at lower levels. The `_compact` method reads all entries from all SSTables, keeps only the newest version of each key (highest version number wins), discards tombstones and expired entries, and writes the result to a new merged SSTable. After writing, the old SSTables are deleted. The 30-second check interval balances between responsiveness and CPU usage. In production systems like LevelDB, compaction is more sophisticated with multiple levels, but size-tiered is sufficient for understanding the core concept.

### Register in `app/main.py`

```python
from app.storage.compaction import compaction_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create metadata tables on startup, start background daemons."""
    Base.metadata.create_all(bind=engine)
    expiration_daemon.start()
    compaction_manager.start()
    yield
    compaction_manager.stop()
    expiration_daemon.stop()
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

# Create data directories
RUN mkdir -p /data/kv /data/wal

EXPOSE 8000

CMD ["fastapi", "run", "app/main.py", "--host", "0.0.0.0", "--port", "8000"]
```

This uses a multi-stage build. The `builder` stage installs dependencies into `/install`. The second stage copies only the installed packages, keeping the final image small by excluding pip caches and build artifacts. `RUN mkdir -p /data/kv /data/wal` creates the data directories for SSTables and WAL files. `--host 0.0.0.0` binds to all interfaces so the container is reachable from outside.

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
  node1:
    build: .
    ports:
      - "8001:8000"
    environment:
      - NODE_ID=node-1
      - DATABASE_URL=postgresql://user:pass@db:5432/kvstore
      - REDIS_URL=redis://cache:6379/0
      - DATA_DIR=/data/kv
      - WAL_DIR=/data/wal
      - REPLICATION_FACTOR=3
      - READ_QUORUM=2
      - WRITE_QUORUM=2
    volumes:
      - node1-data:/data
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    restart: unless-stopped

  node2:
    build: .
    ports:
      - "8002:8000"
    environment:
      - NODE_ID=node-2
      - DATABASE_URL=postgresql://user:pass@db:5432/kvstore
      - REDIS_URL=redis://cache:6379/0
      - DATA_DIR=/data/kv
      - WAL_DIR=/data/wal
      - REPLICATION_FACTOR=3
      - READ_QUORUM=2
      - WRITE_QUORUM=2
    volumes:
      - node2-data:/data
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    restart: unless-stopped

  node3:
    build: .
    ports:
      - "8003:8000"
    environment:
      - NODE_ID=node-3
      - DATABASE_URL=postgresql://user:pass@db:5432/kvstore
      - REDIS_URL=redis://cache:6379/0
      - DATA_DIR=/data/kv
      - WAL_DIR=/data/wal
      - REPLICATION_FACTOR=3
      - READ_QUORUM=2
      - WRITE_QUORUM=2
    volumes:
      - node3-data:/data
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
      - POSTGRES_DB=kvstore
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d kvstore"]
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
  node1-data:
  node2-data:
  node3-data:
  db-data:
  redis-data:
```

The compose file runs a 3-node KV store cluster. Each node (`node1`, `node2`, `node3`) runs the same Docker image with a different `NODE_ID` and is exposed on a different host port (8001, 8002, 8003). Each node gets its own named volume (`node1-data`, `node2-data`, `node3-data`) for persistent storage, ensuring data survives container restarts. `depends_on` with `condition: service_healthy` ensures nodes wait until PostgreSQL and Redis pass their health checks. `--appendonly yes` enables Redis AOF persistence. `redis:7-alpine` is a minimal Redis image based on Alpine Linux. `restart: unless-stopped` automatically restarts containers unless explicitly stopped.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f node1                  # Follow Node 1 logs
```

Visit `http://localhost:8001/docs` for Node 1's interactive API documentation. Test the API:

```bash
# Store a key-value pair
curl -X PUT http://localhost:8001/kv/user:42 \
  -H "Content-Type: application/json" \
  -d '{"value": "John Doe", "ttl_seconds": 3600}'

# Retrieve the value
curl http://localhost:8001/kv/user:42

# Delete the key
curl -X DELETE http://localhost:8001/kv/user:42

# Batch put
curl -X POST http://localhost:8001/kv/batch/put \
  -H "Content-Type: application/json" \
  -d '{"items": [{"key": "k1", "value": "v1"}, {"key": "k2", "value": "v2"}]}'

# Batch get
curl -X POST http://localhost:8001/kv/batch/get \
  -H "Content-Type: application/json" \
  -d '{"keys": ["k1", "k2", "k3"]}'

# Check storage stats
curl http://localhost:8001/admin/stats
```

---

## Testing

### `tests/conftest.py`

```python
import os
import shutil
import tempfile

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.main import app
from app.storage.engine import StorageEngine

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
def temp_dirs():
    """Create temporary directories for test data and WAL."""
    data_dir = tempfile.mkdtemp()
    wal_dir = tempfile.mkdtemp()
    yield data_dir, wal_dir
    shutil.rmtree(data_dir, ignore_errors=True)
    shutil.rmtree(wal_dir, ignore_errors=True)


@pytest.fixture
def test_engine(temp_dirs):
    """Create a fresh StorageEngine with temporary directories."""
    data_dir, wal_dir = temp_dirs
    return StorageEngine(data_dir=data_dir, wal_dir=wal_dir)


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

Tests use SQLite for metadata instead of PostgreSQL for speed and isolation. `temp_dirs` creates temporary directories for each test's data and WAL files, cleaned up automatically afterward. `test_engine` provides a fresh `StorageEngine` instance with isolated storage. `dependency_overrides` swaps out the production database dependency during testing.

### `tests/test_storage.py`

```python
import time


def test_put_and_get(test_engine):
    """Test basic put and get operations."""
    test_engine.put("key1", "value1")
    entry = test_engine.get("key1")
    assert entry is not None
    assert entry.value == "value1"
    assert entry.version == 1


def test_put_overwrites(test_engine):
    """Test that putting the same key updates the value and version."""
    test_engine.put("key1", "value1")
    test_engine.put("key1", "value2")
    entry = test_engine.get("key1")
    assert entry.value == "value2"
    assert entry.version == 2


def test_delete(test_engine):
    """Test that deleting a key makes it unreadable."""
    test_engine.put("key1", "value1")
    assert test_engine.delete("key1") is True
    assert test_engine.get("key1") is None


def test_delete_nonexistent(test_engine):
    """Test that deleting a non-existent key returns False."""
    assert test_engine.delete("nonexistent") is False


def test_ttl_expiration(test_engine):
    """Test that keys expire after their TTL."""
    test_engine.put("temp", "data", ttl_seconds=1)
    assert test_engine.get("temp") is not None
    time.sleep(1.5)
    assert test_engine.get("temp") is None


def test_cas_success(test_engine):
    """Test Compare-And-Swap with matching version."""
    test_engine.put("key1", "v1")
    entry = test_engine.put("key1", "v2", expected_version=1)
    assert entry.version == 2
    assert entry.value == "v2"


def test_cas_failure(test_engine):
    """Test Compare-And-Swap with version mismatch."""
    test_engine.put("key1", "v1")
    test_engine.put("key1", "v2")                 # Version is now 2

    try:
        test_engine.put("key1", "v3", expected_version=1)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Version conflict" in str(e)


def test_batch_put(test_engine):
    """Test batch put stores multiple keys."""
    items = [
        {"key": "k1", "value": "v1"},
        {"key": "k2", "value": "v2"},
        {"key": "k3", "value": "v3"},
    ]
    results = test_engine.batch_put(items)
    assert len(results) == 3
    assert test_engine.get("k2").value == "v2"


def test_batch_get(test_engine):
    """Test batch get retrieves multiple keys."""
    test_engine.put("k1", "v1")
    test_engine.put("k2", "v2")
    results = test_engine.batch_get(["k1", "k2", "k3"])
    assert results["k1"].value == "v1"
    assert results["k2"].value == "v2"
    assert results["k3"] is None


def test_wal_recovery(temp_dirs):
    """Test that data survives crash recovery via WAL replay."""
    data_dir, wal_dir = temp_dirs

    # Write data with first engine instance
    engine1 = StorageEngine(data_dir=data_dir, wal_dir=wal_dir)
    engine1.put("recover_me", "important_data")

    # Simulate crash: create a new engine from the same directories
    engine2 = StorageEngine(data_dir=data_dir, wal_dir=wal_dir)
    entry = engine2.get("recover_me")
    assert entry is not None
    assert entry.value == "important_data"
```

### `tests/test_api.py`

```python
def test_put_key(client):
    """Test storing a key via the API."""
    response = client.put("/kv/testkey", json={"value": "testvalue"})
    assert response.status_code == 200
    data = response.json()
    assert data["key"] == "testkey"
    assert data["value"] == "testvalue"
    assert data["version"] == 1


def test_get_key(client):
    """Test retrieving a key via the API."""
    client.put("/kv/testkey", json={"value": "testvalue"})
    response = client.get("/kv/testkey")
    assert response.status_code == 200
    assert response.json()["value"] == "testvalue"


def test_get_missing_key(client):
    """Test 404 for non-existent keys."""
    response = client.get("/kv/nonexistent")
    assert response.status_code == 404


def test_delete_key(client):
    """Test deleting a key via the API."""
    client.put("/kv/testkey", json={"value": "testvalue"})
    response = client.delete("/kv/testkey")
    assert response.status_code == 200
    assert response.json()["deleted"] is True

    # Verify it's gone
    response = client.get("/kv/testkey")
    assert response.status_code == 404


def test_key_too_long(client):
    """Test that keys exceeding 256 characters are rejected."""
    long_key = "a" * 257
    response = client.put(f"/kv/{long_key}", json={"value": "v"})
    assert response.status_code == 400


def test_value_too_large(client):
    """Test that values exceeding 64 KB are rejected."""
    large_value = "x" * 65537
    response = client.put("/kv/bigkey", json={"value": large_value})
    assert response.status_code == 400


def test_cas_conflict(client):
    """Test Compare-And-Swap conflict via the API."""
    client.put("/kv/caskey", json={"value": "v1"})
    client.put("/kv/caskey", json={"value": "v2"})

    response = client.put("/kv/caskey", json={"value": "v3", "version": 1})
    assert response.status_code == 409
```

### `tests/test_hashing.py`

```python
from app.partitioning.consistent_hash import ConsistentHashRing


def test_single_node():
    """All keys should map to the only node."""
    ring = ConsistentHashRing(num_vnodes=64)
    ring.add_node("node-1")

    for i in range(100):
        assert ring.get_node(f"key-{i}") == "node-1"


def test_distribution():
    """Keys should be roughly evenly distributed across nodes."""
    ring = ConsistentHashRing(num_vnodes=256)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    counts = {"node-1": 0, "node-2": 0, "node-3": 0}
    total = 10000

    for i in range(total):
        node = ring.get_node(f"key-{i}")
        counts[node] += 1

    # Each node should get roughly 33% of keys (within 20% tolerance)
    for node, count in counts.items():
        ratio = count / total
        assert 0.13 < ratio < 0.53, f"{node} got {ratio:.2%} of keys"


def test_minimal_redistribution():
    """Adding a node should only move ~1/N of existing keys."""
    ring = ConsistentHashRing(num_vnodes=256)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    # Record initial assignment
    total = 10000
    original = {}
    for i in range(total):
        key = f"key-{i}"
        original[key] = ring.get_node(key)

    # Add a new node
    ring.add_node("node-4")

    # Count how many keys moved
    moved = 0
    for i in range(total):
        key = f"key-{i}"
        if ring.get_node(key) != original[key]:
            moved += 1

    # Should move roughly 1/4 of keys (within tolerance)
    ratio = moved / total
    assert ratio < 0.50, f"Moved {ratio:.2%} of keys (expected ~25%)"


def test_replica_nodes():
    """Replica nodes should be on distinct physical nodes."""
    ring = ConsistentHashRing(num_vnodes=64)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    replicas = ring.get_replica_nodes("test-key", n=3)
    assert len(replicas) == 3
    assert len(set(replicas)) == 3                # All distinct


def test_remove_node():
    """Removing a node should redistribute its keys to remaining nodes."""
    ring = ConsistentHashRing(num_vnodes=64)
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    ring.remove_node("node-2")

    for i in range(100):
        node = ring.get_node(f"key-{i}")
        assert node in ("node-1", "node-3")
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_storage"                      # Run only storage tests
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

### Set Up Cloud SQL (PostgreSQL) for Metadata

```bash
# Create a Cloud SQL instance
gcloud sql instances create kvstore-metadata \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1 \
  --storage-size=10GB \
  --storage-auto-increase

# Create the database
gcloud sql databases create kvstore \
  --instance=kvstore-metadata

# Set the user password
gcloud sql users set-password postgres \
  --instance=kvstore-metadata \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-f1-micro` is the smallest (cheapest) instance for development. Cloud SQL stores only cluster metadata (node registry, partition map), not the actual key-value data. In production, use `--tier=db-custom-2-7680` or higher with `--availability-type=REGIONAL` for high availability.

### Set Up Memorystore (Redis) for Hot Key Cache

```bash
# Create a Redis instance
gcloud redis instances create kvstore-cache \
  --size=2 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default

# Get the Redis host IP
gcloud redis instances describe kvstore-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=2` provisions 2 GB of memory for caching hot keys. Memorystore is a fully managed Redis service accessible only within the same VPC network.

### Set Up Persistent Disks for Data

```bash
# Create persistent disks for each storage node
gcloud compute disks create kvstore-node1-data \
  --size=100GB \
  --type=pd-ssd \
  --zone=us-central1-a

gcloud compute disks create kvstore-node2-data \
  --size=100GB \
  --type=pd-ssd \
  --zone=us-central1-b

gcloud compute disks create kvstore-node3-data \
  --size=100GB \
  --type=pd-ssd \
  --zone=us-central1-c
```

`pd-ssd` (SSD persistent disk) provides consistent low-latency I/O for WAL writes and SSTable reads. Placing each disk in a different zone ensures availability if one zone goes down.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry
gcloud artifacts repositories create kvstore \
  --repository-format=docker \
  --location=us-central1

gcloud builds submit \
  --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/kvstore/api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build. The image is pushed directly to Artifact Registry without needing Docker installed locally.

### Deploy to GKE (Google Kubernetes Engine)

For a distributed key-value store, GKE with StatefulSets is more appropriate than Cloud Run because each node needs persistent storage and a stable network identity.

```bash
# Create a GKE cluster
gcloud container clusters create kvstore-cluster \
  --num-nodes=3 \
  --machine-type=e2-standard-4 \
  --zone=us-central1-a \
  --enable-ip-alias

# Get credentials
gcloud container clusters get-credentials kvstore-cluster \
  --zone=us-central1-a
```

### Kubernetes StatefulSet

```yaml
# k8s/statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kvstore
spec:
  serviceName: kvstore
  replicas: 3
  selector:
    matchLabels:
      app: kvstore
  template:
    metadata:
      labels:
        app: kvstore
    spec:
      containers:
        - name: kvstore
          image: us-central1-docker.pkg.dev/YOUR_PROJECT_ID/kvstore/api
          ports:
            - containerPort: 8000
          env:
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kvstore-secrets
                  key: database-url
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: kvstore-secrets
                  key: redis-url
            - name: DATA_DIR
              value: /data/kv
            - name: WAL_DIR
              value: /data/wal
            - name: REPLICATION_FACTOR
              value: "3"
            - name: READ_QUORUM
              value: "2"
            - name: WRITE_QUORUM
              value: "2"
          volumeMounts:
            - name: data
              mountPath: /data
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: premium-rwo
        resources:
          requests:
            storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: kvstore
spec:
  clusterIP: None
  selector:
    app: kvstore
  ports:
    - port: 8000
      targetPort: 8000
---
apiVersion: v1
kind: Service
metadata:
  name: kvstore-lb
spec:
  type: LoadBalancer
  selector:
    app: kvstore
  ports:
    - port: 80
      targetPort: 8000
```

`StatefulSet` gives each pod a stable hostname (`kvstore-0`, `kvstore-1`, `kvstore-2`) that persists across restarts, which is used as the `NODE_ID`. `volumeClaimTemplates` automatically provisions a persistent disk per pod, ensuring data survives pod rescheduling. `clusterIP: None` creates a headless service so pods can discover each other by DNS (`kvstore-0.kvstore`, `kvstore-1.kvstore`). `livenessProbe` and `readinessProbe` tell Kubernetes to restart unhealthy pods and stop routing traffic to pods not yet ready. `premium-rwo` is GKE's SSD-backed storage class for low-latency I/O.

### Deploy

```bash
# Create secrets
kubectl create secret generic kvstore-secrets \
  --from-literal=database-url="postgresql://postgres:PASS@CLOUD_SQL_IP:5432/kvstore" \
  --from-literal=redis-url="redis://REDIS_IP:6379/0"

# Apply the StatefulSet
kubectl apply -f k8s/statefulset.yaml

# Verify pods are running
kubectl get pods -l app=kvstore

# View logs
kubectl logs kvstore-0 -f
```

---

## Scaling & Production Considerations

### Data Scaling

| Strategy              | When to Use                              | How                                                   |
| --------------------- | ---------------------------------------- | ----------------------------------------------------- |
| Add Nodes             | Total data exceeds single-node storage   | Add node to hash ring, trigger data migration          |
| Increase vnodes       | Load imbalance across nodes              | More vnodes per node → smoother distribution           |
| Tiered Storage        | SSTables dominate disk usage             | Move cold SSTables to GCS (object storage)             |
| Bloom Filters         | Too many SSTable lookups on read miss    | Per-SSTable bloom filter eliminates unnecessary reads  |
| Block Cache           | Frequently read SSTables                 | Cache SSTable blocks in memory (LRU)                   |

### Consistency & Availability

| Strategy              | When to Use                              | How                                                   |
| --------------------- | ---------------------------------------- | ----------------------------------------------------- |
| Tunable Consistency   | Different use cases need different levels | Configure R, W per request (R=1 for fast, R=3 for strong) |
| Hinted Handoff        | Node temporarily down during write       | Store hint on coordinator, replay when node recovers   |
| Anti-Entropy (Merkle) | Detect replica drift over time           | Compare Merkle trees to find divergent key ranges      |
| Read Repair           | Stale replica detected during read       | Update stale replica with the latest version inline    |
| Vector Clocks         | Need causal ordering across replicas     | Replace version counter with vector clock per key      |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ kv.example.com │
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
       │ Coordinator │ │Coord   │ │ Coordinator │
       │ Node 1      │ │Node 2  │ │ Node N      │
       │ (Stateless) │ │        │ │             │
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │         Consistent Hash Ring        │
         │                                      │
         │  ┌──────────┐  ┌──────────┐         │
         │  │ Storage  │  │ Storage  │  ...    │
         │  │ Node A   │  │ Node B   │         │
         │  │(GKE Pod) │  │(GKE Pod) │         │
         │  │+SSD Disk │  │+SSD Disk │         │
         │  │+WAL+LSM  │  │+WAL+LSM  │         │
         │  └────┬─────┘  └────┬─────┘         │
         │       │              │               │
         │  ┌────▼──────────────▼────────┐      │
         │  │   Replication via gRPC     │      │
         │  │   (Quorum W=2, R=2, N=3)  │      │
         │  └───────────────────────────┘      │
         │                                      │
         │  ┌─────────┐    ┌────────────────┐  │
         │  │ Memory- │    │  Cloud SQL     │  │
         │  │ store   │    │ (Metadata DB)  │  │
         │  │ (Redis) │    │                │  │
         │  │ Hot Key │    │ Node Registry  │  │
         │  │ Cache   │    │ Partition Map  │  │
         │  └─────────┘    └────────────────┘  │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Monitoring + Alerting  │   │
         │  │  (Latency, Error Rate, Disk)  │   │
         │  └───────────────────────────────┘   │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Stateless Coordinators** run on Cloud Run or GKE Deployments and auto-scale independently of storage nodes. They only route requests, so they need no persistent disk.
- **gRPC Replication** replaces HTTP for inter-node communication, reducing serialization overhead and supporting bidirectional streaming for bulk data transfers during rebalancing.
- **Bloom Filters** are attached to each SSTable. Before reading an SSTable from disk, the bloom filter quickly determines if the key is definitely not in that file, eliminating most unnecessary disk reads.
- **Hinted Handoff** ensures writes succeed even when a replica is temporarily down. The coordinator stores a "hint" (the write) locally and replays it to the recovering node once it comes back online.
- **Merkle Trees** are built per partition and compared between replicas periodically. If a difference is found, only the divergent key ranges are synced, making anti-entropy repair efficient.

### Security Checklist

```
✓ Authenticate all API requests (API key or IAM tokens)
✓ Encrypt data at rest (GKE disk encryption, SSTable encryption)
✓ Encrypt data in transit (TLS for client connections, mTLS between nodes)
✓ Validate key and value sizes to prevent memory abuse
✓ Rate limit writes per client to prevent hot-key storms
✓ Use VPC-native networking (no public IPs on storage nodes)
✓ Store secrets in Secret Manager, not environment variables
✓ Enable Cloud Audit Logs for all data access
✓ Implement key-level access control for multi-tenant use cases
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create kvstore-health \
  --display-name="KV Store Health" \
  --uri="https://kv.example.com/health" \
  --period=60

# Key metrics to monitor and alert on:
# - Read latency p50, p95, p99 (target: <5ms, <20ms, <50ms)
# - Write latency p50, p95, p99 (target: <10ms, <30ms, <100ms)
# - Disk usage per node (alert at 80%)
# - MemTable size (alert if not flushing)
# - SSTable count (alert if compaction is falling behind)
# - WAL size (alert if growing unbounded)
# - Replication lag (seconds behind primary)
# - Quorum failure rate (alert at >0.1%)
# - Node heartbeat misses (alert after 3 consecutive misses)
```

Cloud Monitoring automatically collects GKE metrics (CPU, memory, disk I/O). Custom metrics for MemTable size, SSTable count, and WAL size should be exported via the `/admin/stats` endpoint and scraped by a monitoring agent. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached.
