# Design URL Shortener

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

![Design URL Shortener: Backend & System Design Guide by Bidur Sapkota](images/url-1200.webp "Design URL Shortener: Backend & System Design Guide - Blog by Bidur Sapkota")

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Health Check FastAPI in Docker Compose](#health-check-fastapi-in-docker-compose)
4. [Connect to PostgreSQL](#connect-to-postgresql)
5. [URL Model + POST API + Tests](#url-model--post-api--tests)
6. [Redirect Endpoint + Tests](#redirect-endpoint--tests)
7. [Custom Aliases](#custom-aliases)
8. [Hash Algorithm](#hash-algorithm)
9. [Snowflake Algorithm](#snowflake-algorithm)
10. [Expiration](#expiration)
11. [Caching with Redis](#caching-with-redis)
12. [Rate Limiting](#rate-limiting)
13. [Click Events Model + Analytics](#click-events-model--analytics)
14. [Docker & Docker Compose (Final)](#docker--docker-compose-final)
15. [Testing (Final)](#testing-final)
16. [Project Setup (Final)](#project-setup-final)
17. [Load Balancing with Autoscaling on Kubernetes](#load-balancing-with-autoscaling-on-kubernetes)
18. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)

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

## Health Check FastAPI in Docker Compose

In this first step we create the bare minimum: a FastAPI app with a single `/health` endpoint, a Dockerfile, and a Docker Compose file with only the `api` service. No database, no Redis.

**Project structure so far:**

```
url-shortener/
├── app/
│   ├── __init__.py
│   └── main.py
├── Dockerfile
├── compose.yaml
└── requirements.txt
```

#### **NEW** `requirements.txt`

```text
fastapi[standard]
```

`fastapi[standard]` includes `uvicorn` and `pydantic`.

#### **NEW** `app/__init__.py`

```python
# empty – marks this directory as a Python package
```

#### **NEW** `app/main.py`

```python
from fastapi import FastAPI

app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

#### **NEW** `Dockerfile`

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

#### **NEW** `compose.yaml`

```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    restart: unless-stopped
```

### Build, Run & Verify

```bash
docker compose up --build
```

In another terminal:

```bash
curl http://localhost:8000/health
```

**Expected output:**

```json
{ "status": "healthy" }
```

Visit `http://localhost:8000/docs` for the interactive API documentation.

Stop the stack with `Ctrl+C` or `docker compose down`.

---

## Connect to PostgreSQL

We now add a PostgreSQL database, wire it into the FastAPI app, and verify the connection.

**Project structure so far:**

```text
url-shortener/
├── app/
│   ├── __init__.py
│   ├── main.py          # UPDATED
│   ├── config.py        # NEW
│   └── database.py      # NEW
├── Dockerfile
├── compose.yaml         # UPDATED
├── requirements.txt     # UPDATED
└── .env                 # NEW
```

#### **UPDATED** `requirements.txt`

```text
fastapi[standard]
sqlalchemy
psycopg[binary]
python-dotenv
pydantic-settings
```

`sqlalchemy` is the ORM for PostgreSQL. `psycopg[binary]` is the PostgreSQL driver. `python-dotenv` loads `.env` files. `pydantic-settings` provides `BaseSettings` for typed configuration.

#### **NEW** `.env`

```bash
DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
BASE_URL=http://localhost:8000
```

#### **NEW** `app/config.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    base_url: str = "http://localhost:8000"


settings = Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads values from environment variables and `.env` files. Field names are case-insensitive and map to the corresponding `ENV_VAR` names. `model_config = SettingsConfigDict(env_file=".env")` tells Pydantic to also load from the `.env` file.

#### **NEW** `app/database.py`

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

#### **UPDATED** `app/main.py`

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.database import Base, engine

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

@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`lifespan` is an async context manager that runs setup code before the app starts accepting requests (`Base.metadata.create_all` creates missing tables) and teardown code after shutdown. The health check endpoint at `/health` is used by load balancers and container orchestrators to verify the service is running.

#### **UPDATED** `compose.yaml`

```yaml
services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
      - BASE_URL=http://localhost:8000
    depends_on:
      db:
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

volumes:
  db-data:
```

`depends_on` with `condition: service_healthy` ensures the API container waits until PostgreSQL passes its health check before starting. `pg_isready` is a PostgreSQL utility that checks if the server is accepting connections.

### Build, Run & Verify

```bash
docker compose up --build
```

Watch the logs. You should see the API start successfully after PostgreSQL is ready:

```
db-1   | ... database system is ready to accept connections
api-1  | INFO:     Application startup complete.
```

In another terminal:

```bash
curl http://localhost:8000/health
```

**Expected output:**

```json
{ "status": "healthy" }
```

---

## URL Model + POST API + Tests

We create the initial `URL` model (minimal — no click tracking or expiration yet), Pydantic schemas, the shortening algorithm, and the `POST /shorten` endpoint. We also add the first tests.

**Project structure so far:**

```text
url-shortener/
├── app/
│   ├── __init__.py
│   ├── main.py          # UPDATED
│   ├── config.py
│   ├── database.py
│   ├── models.py        # NEW
│   ├── schemas.py       # NEW
│   ├── shortener.py     # NEW
│   └── routers/
│       ├── __init__.py  # NEW
│       └── urls.py      # NEW
├── tests/
│   ├── __init__.py      # NEW
│   ├── conftest.py      # NEW
│   └── test_shorten.py  # NEW
├── Dockerfile
├── compose.yaml
├── requirements.txt     # UPDATED
└── .env
```

#### **UPDATED** `requirements.txt`

```text
fastapi[standard]
sqlalchemy
psycopg[binary]
python-dotenv
pydantic-settings
httpx
pytest
```

`httpx` is used for async HTTP requests and testing. `pytest` is the testing framework.

#### **NEW** `app/models.py`

```python
from datetime import datetime, timezone
from sqlalchemy import BigInteger, Column, DateTime, String, Text
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
```

We start with only the essential fields: `id`, `short_code`, `original_url`, and `created_at`. More fields will be added later. `BigInteger` for `id` supports billions of rows.

#### **NEW** `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, ConfigDict, HttpUrl


class URLCreate(BaseModel):
    url: HttpUrl                              # Validates that input is a valid URL


class URLResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)   # Allow ORM model to Pydantic conversion

    short_code: str
    short_url: str                            # Full shortened URL
    original_url: str
    created_at: datetime
```

`HttpUrl` from Pydantic validates that the input string is a properly formatted URL, rejecting malformed inputs at the schema layer before any business logic runs. `from_attributes = True` (formerly `orm_mode`) tells Pydantic to read data from ORM model attributes instead of expecting a dictionary, enabling `URLResponse.model_validate(url_object)`.

#### **NEW** `app/shortener.py`

```python
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
```

`encode_base62` converts an integer to a Base62 string by repeatedly dividing by 62 and mapping remainders to `[a-zA-Z0-9]`. For example, ID `12345` becomes `"dnh"`. `generate_short_code` takes the database-assigned auto-increment ID, encodes it, and pads with random characters so the code is always exactly `length` characters. The random padding prevents users from guessing the next short code by incrementing.

#### **NEW** `app/routers/__init__.py`

```python
# empty – marks this directory as a Python package
```

#### **NEW** `app/routers/urls.py`

```python
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_short_code

router = APIRouter()


@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
    """Create a shortened URL."""
    original_url = str(payload.url)

    # Create the URL record
    url_record = URL(
        short_code=uuid.uuid4().hex[:8],      # Temporary unique code, replaced below
        original_url=original_url,
    )

    db.add(url_record)
    db.flush()                                # Get the auto-generated ID

    # Generate short code from ID
    url_record.short_code = generate_short_code(url_record.id)

    db.commit()
    db.refresh(url_record)

    return URLResponse(
        short_code=url_record.short_code,
        short_url=f"{settings.base_url}/{url_record.short_code}",
        original_url=url_record.original_url,
        created_at=url_record.created_at,
    )
```

`db.flush()` sends the INSERT to the database and populates `url_record.id` without committing the transaction. This lets us use the auto-generated ID to create the short code, then commit everything atomically.

#### **UPDATED** `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import urls


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


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`include_router` mounts the URL router so the `POST /shorten` endpoint is accessible.

#### **NEW** `pytest.ini`

```ini
[pytest]
pythonpath = .
```

#### **NEW** `tests/__init__.py`

```python
# empty
```

#### **NEW** `tests/conftest.py`

```python
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BigInteger


@compiles(BigInteger, "sqlite")
def compile_biginteger_sqlite(type_, compiler, **kw):
    """Compile BigInteger as INTEGER in SQLite so autoincrement ROWID works."""
    return "INTEGER"


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

#### **NEW** `tests/test_shorten.py`

```python
def test_shorten_url(client):
    """Test creating a shortened URL."""
    response = client.post("/shorten", json={"url": "https://www.example.com"})
    assert response.status_code == 201
    data = response.json()
    assert "short_code" in data
    assert "short_url" in data
    assert data["original_url"] == "https://www.example.com/"


def test_invalid_url_rejected(client):
    """Test that invalid URLs are rejected."""
    response = client.post("/shorten", json={"url": "not-a-url"})
    assert response.status_code == 422
```

### Build, Run & Verify

```bash
docker compose up --build
```

In another terminal, shorten a URL:

```bash
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/path"}'
```

**Expected output (short_code will vary):**

```json
{
  "short_code": "bXkRzPq",
  "short_url": "http://localhost:8000/bXkRzPq",
  "original_url": "https://www.example.com/very/long/path",
  "created_at": "2024-01-15T10:30:00Z"
}
```

Run the tests locally (install deps in a venv first):

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pytest -v tests/test_shorten.py
```

**Expected output:**

```
tests/test_shorten.py::test_shorten_url PASSED
tests/test_shorten.py::test_invalid_url_rejected PASSED
```

---

## Redirect Endpoint + Tests

We add the `GET /{short_code}` redirect endpoint (simple version — no cache, no click tracking, no expiration check yet) and its tests.

**Changed files:**

```
app/routers/urls.py      # UPDATED
tests/test_redirect.py   # NEW
```

#### **UPDATED** `app/routers/urls.py`

```python
from fastapi.responses import RedirectResponse

@router.get("/{short_code}")
def redirect_to_url(short_code: str, request: Request, db: Session = Depends(get_db)):
    """Redirect a short code to its original URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()

    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    return RedirectResponse(
        url=url_record.original_url,
        status_code=301,                      # Permanent redirect (cacheable)
    )
```

`RedirectResponse` with status `301` tells browsers and CDNs to cache the redirect, reducing repeat traffic to the server. Use `302` instead if you need to track every click (browsers won't cache `302` redirects).

#### **NEW** `tests/test_redirect.py`

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
```

### Build, Run & Verify

```bash
docker compose up --build
```

Create a URL and follow the redirect:

```bash
# Shorten
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/path"}'

# Redirect (follow redirects with -L)
curl -L http://localhost:8000/<short_code>
```

Run the tests:

```bash
pytest -v tests/test_redirect.py
```

**Expected output:**

```
tests/test_redirect.py::test_redirect PASSED
tests/test_redirect.py::test_redirect_not_found PASSED
```

---

## Custom Aliases

We now enhance the system with multiple shortening algorithms, custom alias support, URL expiration, and click counting.

### Approach Comparison

The core challenge is generating a unique, short, and collision-resistant code for each URL. There are several approaches:

| Approach           | Pros                           | Cons                                          |
| ------------------ | ------------------------------ | --------------------------------------------- |
| Random + Retry     | Simple, no coordination needed | Collision risk under high load                |
| Counter + Base62   | Zero collisions, predictable   | Requires distributed counter, codes guessable |
| MD5/SHA256 + Trunc | Deterministic, same URL → same | Collision risk with truncation                |
| Snowflake ID       | Globally unique, time-sortable | Requires Snowflake service, long codes        |
| Pre-generated Pool | Fast, no runtime computation   | Requires key generation service               |

For this project, we use **Base62 encoding of an auto-incrementing counter** combined with **random padding** to prevent sequential guessing.

#### **UPDATED** `app/schemas.py`

Add custom_alias attribute.

```py
class URLCreate(BaseModel):
    url: HttpUrl                              # Validates that input is a valid URL
    custom_alias: str | None = None           # Optional custom short code
```

#### **UPDATED** `app/shortener.py`

Allow users to create own short url but with validation.

```python
def is_valid_alias(alias: str) -> bool:
    """Check that a custom alias contains only Base62 characters."""
    return all(c in BASE62 for c in alias) and 3 <= len(alias) <= 20
```

`is_valid_alias` validates custom aliases to allow only alphanumeric characters between 3 and 20 characters long.

#### **UPDATED** `app/urls/shortener.py`

```py
from app.shortener import is_valid_alias

# ...
    db.flush()                                # Get the auto-generated ID

    # Handle custom alias or generate code in-memory
    if payload.custom_alias:
        if not is_valid_alias(payload.custom_alias):
            raise HTTPException(
                status_code=400,
                detail="Alias must be 3-20 alphanumeric characters.",
            )

        existing = db.query(URL).filter(URL.short_code == payload.custom_alias).first()
        if existing:
            raise HTTPException(status_code=409, detail="Alias already taken.")

        url_record.short_code = payload.custom_alias
    else:
        # Generate short code from ID
        url_record.short_code = generate_short_code(url_record.id)


    db.commit()
# ...
```

### Build, Run & Verify

```bash
docker compose up --build
```

Create a URL:

```bash
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/path", "custom_alias": "ab123"}'
```

**Output:**

```text
{"short_code":"ab123","short_url":"http://localhost:8000/ab123","original_url":"https://www.example.com/very/long/path","created_at":"2026-11-21T07:59:27.672985Z"}
```

---

## Hash Algorithm

#### **UPDATED** `app/shortener.py`

New algorithm using hash of original url.

```py
import hashlib
# ...

def generate_from_hash(original_url: str, length: int = 7) -> str:
    """
    Fallback: generate a short code from the URL's MD5 hash.
    Useful when you want the same URL to always produce the same code.
    """
    hash_digest = hashlib.md5(original_url.encode()).hexdigest()
    num = int(hash_digest[:12], 16)           # Use first 12 hex chars
    code = encode_base62(num)
    return code[:length]
```

`generate_from_hash` is a fallback that hashes the URL itself, giving a deterministic code for the same input URL.

#### **UPDATED** `app/routers/urls.py`

```py
from app.shortener import generate_from_hash, is_valid_alias

@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
    """Create a shortened URL using Snowflake ID or custom alias."""
    original_url = str(payload.url)

    # Handle custom alias or generate code in-memory
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
        short_code = generate_from_hash(original_url)

    # Direct single-step insert (no temporary UUID or db.flush required)
    url_record = URL(
        short_code=short_code,
        original_url=original_url,
    )
    db.add(url_record)
    db.commit()
    db.refresh(url_record)

    return URLResponse(
        short_code=url_record.short_code,
        short_url=f"{settings.base_url}/{url_record.short_code}",
        original_url=url_record.original_url,
        created_at=url_record.created_at,
    )
```

### Build, Run & Verify

```bash
docker compose up --build
```

Create a URL:

```bash
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/path"}'

# Redirect (follow redirects with -L)
curl -L http://localhost:8000/<short_code>
```

**Output:**

```text
{"short_code":"brm8LUk","short_url":"http://localhost:8000/brm8LUk","original_url":"https://www.example.com/very/long/path","created_at":"2026-11-21T12:12:50.145356Z"}
```

---

## Snowflake Algorithm

For production systems with distributed nodes, we can replace the database auto-increment counter with Twitter's Snowflake ID generator. This removes the need for temporary UUIDs and two-step database flushes (`db.flush()`).

**Changed files:**

```
app/config.py            # UPDATED
app/models.py            # UPDATED
app/shortener.py         # UPDATED
app/routers/urls.py      # UPDATED
```

#### **UPDATED** `app/config.py`

Add `machine_id` to `Settings` to identify worker instances (0–1023):

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    base_url: str = "http://localhost:8000"
    machine_id: int = 0                       # Worker node ID (0-1023)


settings = Settings()
```

#### **UPDATED** `app/models.py`

Expand `short_code` column length to `VARCHAR(16)` because 64-bit Snowflake IDs encoded in Base62 produce up to 11 characters ($62^{11} \approx 5.2 \times 10^{19} > 2^{64}$):

```python
# In app/models.py:
class URL(Base):
    __tablename__ = "urls"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    short_code = Column(String(16), unique=True, nullable=False, index=True)
    original_url = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
```

#### **UPDATED** `app/shortener.py`

Add the `Snowflake` class and `generate_snowflake_code()` helper function:

```python
import hashlib
import string
import threading
import time

from app.config import settings

BASE62 = string.ascii_letters + string.digits   # a-zA-Z0-9 (62 chars)
EPOCH = 1_704_067_200_000                       # 2024-01-01 UTC in ms


class Snowflake:
    """
    64-bit ID: 41-bit timestamp | 10-bit machine | 12-bit sequence
    → 4096 IDs/ms/machine, no collisions, no DB coordination.
    """

    def __init__(self, machine_id: int = 0):
        assert 0 <= machine_id < 1024, "machine_id must be between 0 and 1023"
        self.machine_id = machine_id
        self._seq = 0
        self._last_ts = -1
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            ts = int(time.time() * 1000) - EPOCH
            if ts == self._last_ts:
                self._seq = (self._seq + 1) & 0xFFF
                if self._seq == 0:              # 4096 exhausted, wait 1 ms
                    while ts <= self._last_ts:
                        ts = int(time.time() * 1000) - EPOCH
            else:
                self._seq = 0
            self._last_ts = ts
            return (ts << 22) | (self.machine_id << 12) | self._seq


# Singleton instance per worker process
snowflake = Snowflake(machine_id=settings.machine_id)


def generate_snowflake_code() -> str:
    """Generate an ~11-character unique, time-sortable short code in memory."""
    return encode_base62(snowflake.next_id())
```

Each call to `next_id()` assembles a 64-bit int from a millisecond timestamp (41 bits), a machine identifier (10 bits, supporting 1,024 workers), and a sequence counter (12 bits, 4,096 IDs per millisecond). Base62-encoding this directly produces an **~11-character** code that is unique, time-sortable, and non-sequential — no retries or DB lookups needed.

#### **UPDATED** `app/routers/urls.py`

```python
from app.shortener import generate_snowflake_code, is_valid_alias

# ...
    else:
        short_code = generate_snowflake_code()
# ...
```

---

## Expiration

URLs should not live in the database forever. In this topic, we add Time-to-Live (TTL) expiration support so links can expire automatically after a specified number of days (or fall back to a default TTL), and an `is_active` flag to allow soft deactivation. When a link expires or is deactivated, accessing it returns `HTTP 410 Gone`.

**Changed files:**

```
requirements.txt                                # UPDATED
compose.yaml                                    # UPDATED
alembic.ini                                     # NEW
alembic/env.py                                  # NEW
alembic/versions/0001_initial_urls.py           # NEW
alembic/versions/0002_add_expiration_to_urls.py # NEW
app/main.py                                     # UPDATED
app/config.py                                   # UPDATED
app/schemas.py                                  # UPDATED
app/models.py                                   # UPDATED
app/routers/urls.py                             # UPDATED
tests/test_shorten.py                           # UPDATED
tests/test_redirect.py                          # UPDATED
```

#### Database Migration

In production, database schemas evolve using versioned migration files rather than recreating tables with `Base.metadata.create_all()`. We use Alembic to manage and execute incremental migrations.

##### 1. Install Alembic

Add `alembic` to `requirements.txt`:

#### **UPDATED** `requirements.txt`

```text
fastapi[standard]
sqlalchemy
psycopg[binary]
alembic
python-dotenv
pydantic-settings
httpx
pytest
```

Install it into your Python environment:

```bash
pip install alembic
```

##### 2. Initialize Alembic

Run the initialization command from the project root (`url-shortener/`):

```bash
alembic init alembic
```

This generates the following files:

```
├── alembic.ini              # Global Alembic configuration
└── alembic/
    ├── env.py               # Runtime script executed on every migration command
    ├── README
    ├── script.py.mako       # Template file for creating new migration files
    └── versions/            # Directory where versioned migration files are stored
```

##### 3. Configure `alembic/env.py` (Handling Docker vs. Host Network)

In a Docker Compose setup, PostgreSQL runs inside a container with service name `db` (`postgresql://user:pass@db:5432/urlshortener`), but your host terminal (Mac/Linux) accesses it via `localhost:5432` (via mapped ports `5432:5432`).

Update `alembic/env.py` so that:

1. It dynamically reads `settings.database_url` (from `.env`).
2. It automatically falls back `@db:` to `@localhost:` when run from your host machine if `db` cannot be resolved.
3. It sets `target_metadata = Base.metadata` so Alembic can compare models against the live database.

#### **UPDATED** `alembic/env.py`

```python
import os
import socket
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.config import settings
from app.database import Base
from app.models import URL  # Ensure models are imported so Base.metadata is populated

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Dynamically set the database URL from settings (.env)
db_url = os.getenv("DATABASE_URL", settings.database_url)

# If running outside Docker (host machine) where 'db' does not resolve,
# automatically fallback to 'localhost' using mapped port 5432:5432
if "@db:" in db_url:
    try:
        socket.gethostbyname("db")
    except socket.gaierror:
        db_url = db_url.replace("@db:", "@localhost:")

config.set_main_option("sqlalchemy.url", db_url)

# Set target metadata for 'autogenerate' support
target_metadata = Base.metadata
```

#### Ensure PostgreSQL is running in Docker

```bash
docker compose up -d db
```

#### Generate the Initial Migration (`initial_urls`)

If starting from a fresh database:

```bash
alembic revision --autogenerate -m "initial_urls"
```

Alembic will detect `urls` in `Base.metadata` that doesn't yet exist in PostgreSQL and generate `op.create_table("urls", ...)` inside `alembic/versions/`.

Apply it to the database:

```bash
alembic upgrade head
```

> **Note:** If PostgreSQL already has the `urls` table created from earlier steps (e.g. from `Base.metadata.create_all()`), `--autogenerate` will compare DB with models and find 0 differences (`pass`). In that case, either reset the database volume with `docker compose down -v && docker compose up -d db`, or stamp the initial baseline with `alembic stamp head`.

#### 4. Create Incremental Migration: Expiration & Active Flag

Now update `app/models.py` (adding `expires_at` and `is_active`).

#### **UPDATED** `app/models.py`

```python
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Index, String, Text
)

from app.database import Base


class URL(Base):
    __tablename__ = "urls"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    short_code = Column(String(16), unique=True, nullable=False, index=True)
    original_url = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)

    __table_args__ = (
        Index("idx_expires_at", "expires_at"),
    )
```

We added `expires_at` and `is_active` to `URL`. `DateTime(timezone=True)` ensures UTC accuracy across different server environments.

#### **UPDATED** `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, ConfigDict, HttpUrl


class URLCreate(BaseModel):
    url: HttpUrl                              # Validates that input is a valid URL
    custom_alias: str | None = None           # Optional custom short code
    expires_in_days: int | None = None        # Optional TTL in days


class URLResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)   # Allow ORM model → Pydantic conversion

    short_code: str
    short_url: str                            # Full shortened URL
    original_url: str
    created_at: datetime
    expires_at: datetime | None
```

`expires_in_days` allows API clients to request a specific link lifespan. `expires_at` in `URLResponse` returns the calculated UTC expiration timestamp.

#### urls Table

```
┌──────────────────────────────────────┐
│              urls                     │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ short_code    VARCHAR(16) UNIQUE      │
│ original_url  TEXT        NOT NULL    │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
│ expires_at    TIMESTAMPTZ NULL        │
│ is_active     BOOLEAN    DEFAULT TRUE │
├──────────────────────────────────────┤
│ INDEX idx_short_code (short_code)     │
│ INDEX idx_expires_at (expires_at)     │
└──────────────────────────────────────┘
```

`expires_at` stores the expiration timestamp with timezone awareness (`TIMESTAMPTZ`). `is_active` provides a soft-deletion mechanism for deactivating URLs without immediately dropping database records. The `idx_expires_at` index optimizes timestamp lookups and background cleanup scans.

**Run `--autogenerate` again:**

```bash
alembic revision --autogenerate -m "add_expiration_and_is_active_to_urls"
```

Alembic compares the live `urls` table in Docker with the updated `URL` model, automatically detects the missing columns and index, and generates:

- `op.add_column("urls", sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True))`
- `op.add_column("urls", sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False))`
- `op.create_index("idx_expires_at", "urls", ["expires_at"], unique=False)`

##### 5. Apply the Migration

Run the migration against your Docker database:

```bash
alembic upgrade head
```

Verify that the database schema is at the latest revision:

```bash
alembic current
```

**Expected output:**

```
<id> (head)
```

##### 6. Automate Migrations in Docker & Clean Up `app/main.py`

Now that Alembic handles database migrations:

1. **Can `lifespan` be removed completely from FastAPI?**
   **Yes!** Previously, `lifespan` existed solely to call `Base.metadata.create_all(bind=engine)`. Because schema management is now owned by Alembic, the entire `lifespan` context manager is redundant and can be removed completely.
2. **How migrations run automatically in Docker:**
   Update `compose.yaml` so the `api` container runs `alembic upgrade head` before booting FastAPI.

#### **UPDATED** `app/main.py`

```python
from fastapi import FastAPI

from app.routers import urls

app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0",
)

app.include_router(urls.router)


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

#### **UPDATED** `compose.yaml`

```yaml
services:
  api:
    build: .
    command: sh -c "alembic upgrade head && fastapi run app/main.py --host 0.0.0.0 --port 8000"
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
      - BASE_URL=http://localhost:8000
    depends_on:
      db:
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

volumes:
  db-data:
```

#### **UPDATED** `app/config.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    base_url: str = "http://localhost:8000"
    short_code_length: int = 7
    default_ttl_days: int = 365
    machine_id: int = 0


settings = Settings()
```

Added `default_ttl_days: int = 365` as the fallback expiration period when a user does not supply `expires_in_days`.

#### **UPDATED** `app/routers/urls.py`

```python
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_snowflake_code, is_valid_alias

router = APIRouter()


@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
    """Create a shortened URL with optional TTL and custom alias."""
    original_url = str(payload.url)

    # 1. Handle custom alias or generate snowflake code
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
        short_code = generate_snowflake_code()

    # 2. Compute expiration timestamp
    ttl_days = payload.expires_in_days or settings.default_ttl_days
    expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

    # 3. Persist record
    url_record = URL(
        short_code=short_code,
        original_url=original_url,
        expires_at=expires_at,
    )
    db.add(url_record)
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
    """Redirect a short code to its original URL after checking validity."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()

    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    if not url_record.is_active:
        raise HTTPException(status_code=410, detail="This link has been deactivated.")

    if url_record.expires_at:
        expires_at = url_record.expires_at
        # SQLite in test environments strips tzinfo; ensure timezone awareness for comparison
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=410, detail="This link has expired.")

    return RedirectResponse(
        url=url_record.original_url,
        status_code=301,                      # Permanent redirect (cacheable)
    )
```

The redirect endpoint now validates link freshness before redirecting. `HTTP 410 Gone` is returned instead of `404 Not Found` when a link once existed but has expired or been disabled, conforming to HTTP standards.

#### **UPDATED** `tests/test_shorten.py`

```python
def test_shorten_url(client):
    """Test creating a shortened URL with default expiration."""
    response = client.post("/shorten", json={"url": "https://www.example.com"})
    assert response.status_code == 201
    data = response.json()
    assert "short_code" in data
    assert "short_url" in data
    assert data["original_url"] == "https://www.example.com/"
    assert data["expires_at"] is not None


def test_shorten_with_custom_ttl(client):
    """Test creating a shortened URL with custom TTL in days."""
    response = client.post(
        "/shorten",
        json={"url": "https://www.example.com", "expires_in_days": 7},
    )
    assert response.status_code == 201
    assert response.json()["expires_at"] is not None


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

#### **UPDATED** `tests/test_redirect.py`

```python
from datetime import datetime, timedelta, timezone
from app.models import URL


def test_redirect_expired_url_returns_410(client, db):
    """Test 410 Gone for expired links."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    # Manually backdate expiration in the database
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    url_record.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
    db.commit()

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 410
    assert response.json()["detail"] == "This link has expired."


def test_redirect_deactivated_url_returns_410(client, db):
    """Test 410 Gone for deactivated links."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    url_record.is_active = False
    db.commit()

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 410
    assert response.json()["detail"] == "This link has been deactivated."
```

### Build, Run & Verify

```bash
docker compose up --build
```

Shorten a URL with a custom 30-day expiration:

```bash
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com", "expires_in_days": 30}'
```

**Expected output:**

```json
{
  "short_code": "0B8x9YmZ",
  "short_url": "http://localhost:8000/0B8x9YmZ",
  "original_url": "https://www.example.com/",
  "created_at": "2026-11-22T12:00:00Z",
  "expires_at": "2026-12-22T12:00:00Z"
}
```

Run all tests:

```bash
pytest -v
```

**Expected output:**

```
tests/test_shorten.py::test_shorten_url PASSED
tests/test_shorten.py::test_shorten_with_custom_ttl PASSED
tests/test_shorten.py::test_shorten_with_custom_alias PASSED
tests/test_shorten.py::test_duplicate_alias_rejected PASSED
tests/test_shorten.py::test_invalid_alias_rejected PASSED
tests/test_shorten.py::test_invalid_url_rejected PASSED
tests/test_redirect.py::test_redirect PASSED
tests/test_redirect.py::test_redirect_not_found PASSED
tests/test_redirect.py::test_redirect_expired_url_returns_410 PASSED
tests/test_redirect.py::test_redirect_deactivated_url_returns_410 PASSED
```

---

## Caching with Redis

Every redirect triggers a database query. At 10 000 reads/sec, PostgreSQL would buckle. Redis acts as a read-through cache, serving hot URLs from memory in < 1 ms.

**Changed files:**

```
compose.yaml             # UPDATED
app/config.py            # UPDATED
app/cache.py             # NEW
app/routers/urls.py      # UPDATED
requirements.txt         # UPDATED
.env                     # UPDATED
```

#### **UPDATED** `requirements.txt`

```text
fastapi[standard]==0.115.0
sqlalchemy==2.0.35
psycopg2-binary==2.9.10
python-dotenv==1.0.1
pydantic-settings==2.5.2
httpx==0.27.2
pytest==8.3.3
redis==5.1.1
```

#### **UPDATED** `.env`

```bash
DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
BASE_URL=http://localhost:8000
REDIS_URL=redis://cache:6379/0
```

#### **UPDATED** `app/config.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    short_code_length: int = 7
    default_ttl_days: int = 365


settings = Settings()
```

Added `redis_url`.

#### **UPDATED** `compose.yaml`

```yaml
services:
  api:
    build: .
    command: sh -c "alembic upgrade head && fastapi run app/main.py --host 0.0.0.0 --port 8000"
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

`--appendonly yes` enables Redis AOF (Append-Only File) persistence, writing every operation to disk so data survives container restarts. `redis:7-alpine` is a minimal Redis image based on Alpine Linux, significantly smaller than the default image.

#### **NEW** `app/cache.py`

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

#### **UPDATED** `app/routers/urls.py`

```python
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_short_code, is_valid_alias
from app.cache import get_cached_url, set_cached_url, increment_click_count

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
        short_code=short_code or uuid.uuid4().hex[:8],  # Temporary, replaced below
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

    if url_record.expires_at:
        expires_at = url_record.expires_at
        # SQLite in test environments strips tzinfo; ensure timezone awareness for comparison
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < datetime.now(timezone.utc):
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

### Build, Run & Verify

```bash
docker compose up --build
```

Verify caching by curling a redirect twice and checking logs:

```bash
# Shorten a URL
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com"}'

# First redirect — cache miss, hits the DB and populates cache
curl -v http://localhost:8000/<short_code> 2>&1 | head -20

# Second redirect — cache hit, served from Redis (faster)
curl -v http://localhost:8000/<short_code> 2>&1 | head -20
```

---

## Rate Limiting

Without rate limiting, a single client can overwhelm the service with requests. We implement a **sliding window** rate limiter using Redis.

**Changed files:**

```
app/rate_limiter.py      ← NEW
app/main.py              ← UPDATED
app/config.py            ← UPDATED
.env                     ← UPDATED
```

#### **UPDATED** `.env`

```bash
DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
BASE_URL=http://localhost:8000
REDIS_URL=redis://cache:6379/0
RATE_LIMIT_PER_MINUTE=60
```

#### **UPDATED** `app/config.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://user:pass@localhost:5432/urlshortener"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    short_code_length: int = 7
    default_ttl_days: int = 365
    rate_limit_per_minute: int = 60


settings = Settings()
```

Added `rate_limit_per_minute`.

#### **NEW** `app/rate_limiter.py`

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

#### **UPDATED** `app/main.py`

```python
from fastapi import FastAPI

from app.routers import urls
from app.rate_limiter import RateLimiterMiddleware

app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0",
)

app.add_middleware(RateLimiterMiddleware)

app.include_router(urls.router)


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

### Build, Run & Verify

```bash
docker compose up --build
```

Demonstrate rate limiting by sending many requests rapidly:

```bash
# Send 65 rapid requests (limit is 60/min)
for i in $(seq 1 65); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/health
done
```

The first 60 requests return `200`. Requests 61+ return `429`:

**Expected output (last few lines):**

```
200
200
429
429
429
```

Check the rate limit headers:

```bash
curl -v http://localhost:8000/health 2>&1 | grep -i x-ratelimit
```

**Expected output:**

```
< X-RateLimit-Limit: 60
< X-RateLimit-Remaining: 59
```

---

## Click Events Model + Analytics

We now add granular per-click tracking with the `ClickEvent` model and a full analytics router.

**Changed files:**

```
app/models.py            ← UPDATED
app/schemas.py           ← UPDATED
app/routers/analytics.py ← NEW
app/routers/urls.py      ← UPDATED
app/main.py              ← UPDATED
tests/test_redirect.py   ← UPDATED
```

#### Entity-Relationship Diagram (Final)

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

The `click_events` table stores granular per-click data for analytics. Separating events from the URL record avoids locking the URL row on every redirect and lets you run analytics queries independently.

#### **UPDATED** `app/models.py`

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

`relationship()` sets up the ORM-level link between `URL` and `ClickEvent` so you can access `url.clicks` or `click.url` without writing joins manually. `__table_args__` adds composite or additional indexes beyond the ones declared inline on columns.

#### **UPDATED** `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, ConfigDict, HttpUrl


class URLCreate(BaseModel):
    url: HttpUrl                              # Validates that input is a valid URL
    custom_alias: str | None = None           # Optional custom short code
    expires_in_days: int | None = None        # Optional TTL in days


class URLResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)   # Allow ORM model → Pydantic conversion

    short_code: str
    short_url: str                            # Full shortened URL
    original_url: str
    created_at: datetime
    expires_at: datetime | None


class URLStats(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    short_code: str
    original_url: str
    click_count: int
    created_at: datetime
    expires_at: datetime | None
    is_active: bool


class ClickEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    clicked_at: datetime
    referrer: str | None
    user_agent: str | None
    ip_address: str | None
    country: str | None
```

`ClickEventResponse` is the new schema for returning individual click events.

#### **NEW** `app/routers/analytics.py`

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

#### **UPDATED** `app/routers/urls.py`

```python
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL, ClickEvent
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_short_code, is_valid_alias
from app.cache import get_cached_url, set_cached_url, increment_click_count

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
        short_code=short_code or uuid.uuid4().hex[:8],  # Temporary, replaced below
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
    """Redirect with Redis cache layer and click event recording."""

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

    if url_record.expires_at:
        expires_at = url_record.expires_at
        # SQLite in test environments strips tzinfo; ensure timezone awareness for comparison
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=410, detail="This link has expired.")

    # 3. Populate cache
    set_cached_url(
        short_code,
        url_record.original_url,
        url_record.expires_at.isoformat() if url_record.expires_at else None,
    )

    # 4. Record click event
    click_event = ClickEvent(
        url_id=url_record.id,
        referrer=request.headers.get("referer"),      # Note: HTTP header is "referer"
        user_agent=request.headers.get("user-agent"),
        ip_address=request.client.host,
        country=None,                                  # Populated by GeoIP service
    )
    db.add(click_event)

    # 5. Update count and redirect
    url_record.click_count += 1
    db.commit()

    return RedirectResponse(url=url_record.original_url, status_code=301)
```

The HTTP `Referer` header (note the deliberate misspelling in the HTTP spec) tells you which page the user clicked the link from. In production, you would send click events to a message queue (Pub/Sub, Kafka) and process them asynchronously to avoid slowing down the redirect response.

#### **UPDATED** `app/main.py`

```python
from fastapi import FastAPI

from app.routers import urls, analytics
from app.rate_limiter import RateLimiterMiddleware

app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0",
)

app.add_middleware(RateLimiterMiddleware)

app.include_router(urls.router)
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

#### **UPDATED** `tests/test_redirect.py`

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

### Custom Aliases & Expiration

Custom aliases and TTL are already handled in the schemas and the shorten endpoint. Here is how expiration cleanup works:

#### Expiration Cleanup Job

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

#### GCP Deployment: Cloud Scheduler → Cloud Run Job

**Step 1 — Containerize the cleanup script**

Use the same `Dockerfile` but override the entrypoint for the job:

```dockerfile
# Dockerfile.cleanup
FROM python:3.14-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.14-slim
WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .

CMD ["python", "-m", "app.cleanup"]
```

**Step 2 — Push the image to Artifact Registry**

```bash
# Create a repository (one-time)
gcloud artifacts repositories create url-shortener \
  --repository-format=docker \
  --location=us-central1

# Build and push
gcloud builds submit \
  --tag us-central1-docker.pkg.dev/PROJECT_ID/url-shortener/cleanup:latest \
  -f Dockerfile.cleanup .
```

**Step 3 — Create a Cloud Run Job**

```bash
gcloud run jobs create url-cleanup \
  --image us-central1-docker.pkg.dev/PROJECT_ID/url-shortener/cleanup:latest \
  --region us-central1 \
  --set-env-vars DATABASE_URL=postgresql://user:pass@DB_IP:5432/urlshortener,REDIS_URL=redis://REDIS_IP:6379/0 \
  --vpc-connector my-connector \
  --max-retries 2 \
  --task-timeout 300s
```

`--vpc-connector` lets the job reach Cloud SQL and Memorystore (Redis) over private IPs. `--max-retries 2` retries on failure. `--task-timeout 300s` kills it if cleanup hangs beyond 5 minutes.

**Step 4 — Schedule it with Cloud Scheduler**

```bash
# Create a service account for the scheduler
gcloud iam service-accounts create cleanup-scheduler

# Grant it permission to invoke the Cloud Run job
gcloud run jobs add-iam-policy-binding url-cleanup \
  --region us-central1 \
  --member serviceAccount:cleanup-scheduler@PROJECT_ID.iam.gserviceaccount.com \
  --role roles/run.invoker

# Create the hourly schedule
gcloud scheduler jobs create http url-cleanup-schedule \
  --location us-central1 \
  --schedule "0 * * * *" \
  --time-zone "UTC" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT_ID/jobs/url-cleanup:run" \
  --http-method POST \
  --oauth-service-account-email cleanup-scheduler@PROJECT_ID.iam.gserviceaccount.com
```

`--schedule "0 * * * *"` triggers every hour at minute 0. Cloud Scheduler makes an authenticated HTTP POST to the Cloud Run Jobs API, which spins up a container, runs the cleanup, and shuts down — you pay only for execution time.

### Build, Run & Verify

```bash
docker compose up --build
```

Demonstrate the full flow:

```bash
# 1. Shorten a URL
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.example.com/very/long/path"}'

# 2. Click it a few times
curl -L http://localhost:8000/<short_code>
curl -L http://localhost:8000/<short_code>
curl -L http://localhost:8000/<short_code>

# 3. Check analytics
curl http://localhost:8000/api/analytics/<short_code>

# 4. Get detailed click summary
curl http://localhost:8000/api/analytics/<short_code>/summary
```

**Expected analytics output:**

```json
{
  "short_code": "bXkRzPq",
  "original_url": "https://www.example.com/very/long/path",
  "click_count": 3,
  "created_at": "2024-01-15T10:30:00Z",
  "expires_at": "2025-01-14T10:30:00Z",
  "is_active": true
}
```

---

## Docker & Docker Compose (Final)

This section recaps the final, complete Docker configuration that was built up incrementally through the steps above.

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
    command: sh -c "alembic upgrade head && fastapi run app/main.py --host 0.0.0.0 --port 8000"
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

## Testing (Final)

This section recaps the final, complete test files.

### `tests/conftest.py`

```python
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BigInteger


@compiles(BigInteger, "sqlite")
def compile_biginteger_sqlite(type_, compiler, **kw):
    """Compile BigInteger as INTEGER in SQLite so autoincrement ROWID works."""
    return "INTEGER"


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

## Project Setup (Final)

This section recaps the final, complete project structure and configuration.

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
│   ├── cleanup.py            # Expiration cleanup job
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

## Load Balancing with Autoscaling on Kubernetes

Kubernetes orchestrates your containers across multiple nodes, automatically distributing traffic and scaling replicas based on demand. This section sets up a local cluster with Minikube, deploys PostgreSQL and Redis as StatefulSets, runs the API as a Deployment behind a Service, and configures a Horizontal Pod Autoscaler (HPA) to scale based on CPU utilization.

### Prerequisites

```bash
# Install Minikube and kubectl
brew install minikube kubectl

# Start a local cluster
minikube start --cpus=4 --memory=4096

# Enable the metrics-server addon (required for HPA)
minikube addons enable metrics-server

# Enable the ingress addon (optional, for local domain routing)
minikube addons enable ingress

# Verify the cluster is running
kubectl cluster-info
kubectl get nodes
```

`minikube start` creates a single-node Kubernetes cluster inside a VM or container on your machine. `metrics-server` collects CPU and memory usage from each pod, which the HPA reads to decide when to scale. Without it, `kubectl top pods` and autoscaling won't work.

### Namespace

```bash
kubectl create namespace url-shortener
```

Namespaces isolate resources logically within a cluster. All manifests below use `namespace: url-shortener` so the entire application can be managed or torn down as a unit with `kubectl delete namespace url-shortener`.

### ConfigMap & Secrets

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: api-config
  namespace: url-shortener
data:
  BASE_URL: "http://localhost:8000"
  SHORT_CODE_LENGTH: "7"
  DEFAULT_TTL_DAYS: "365"
  RATE_LIMIT_PER_MINUTE: "60"
```

```yaml
# k8s/secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: db-secret
  namespace: url-shortener
type: Opaque
stringData:
  POSTGRES_USER: user
  POSTGRES_PASSWORD: pass
  POSTGRES_DB: urlshortener
  DATABASE_URL: "postgresql://user:pass@postgres:5432/urlshortener"
  REDIS_URL: "redis://redis:6379/0"
```

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
```

ConfigMaps hold non-sensitive configuration; Secrets hold credentials. Both are injected into pods as environment variables. In production, use an external secret manager (GCP Secret Manager, HashiCorp Vault) and reference secrets via the `ExternalSecret` CRD instead of storing them in YAML files.

### PostgreSQL StatefulSet

```yaml
# k8s/postgres.yaml
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: url-shortener
spec:
  clusterIP: None # Headless service for stable DNS
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: url-shortener
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:16
          ports:
            - containerPort: 5432
          envFrom:
            - secretRef:
                name: db-secret
          volumeMounts:
            - name: postgres-data
              mountPath: /var/lib/postgresql/data
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "user", "-d", "urlshortener"]
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
  volumeClaimTemplates:
    - metadata:
        name: postgres-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

A StatefulSet (not a Deployment) is used because databases need stable network identities and persistent storage. The headless Service (`clusterIP: None`) gives each pod a stable DNS name (`postgres-0.postgres.url-shortener.svc.cluster.local`). `volumeClaimTemplates` creates a PersistentVolumeClaim for each replica, so data survives pod restarts. The `readinessProbe` uses `pg_isready` — the same utility from the Docker Compose health check — to ensure traffic is only routed to the pod after PostgreSQL is ready.

### Redis StatefulSet

```yaml
# k8s/redis.yaml
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: url-shortener
spec:
  clusterIP: None
  selector:
    app: redis
  ports:
    - port: 6379
      targetPort: 6379
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
  namespace: url-shortener
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          command: ["redis-server", "--appendonly", "yes"]
          ports:
            - containerPort: 6379
          volumeMounts:
            - name: redis-data
              mountPath: /data
          readinessProbe:
            exec:
              command: ["redis-cli", "ping"]
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 250m
              memory: 256Mi
  volumeClaimTemplates:
    - metadata:
        name: redis-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 500Mi
```

Same pattern as PostgreSQL: a headless Service for stable DNS and a PersistentVolumeClaim for AOF persistence. The `redis-cli ping` readiness probe mirrors the Docker Compose health check.

### API Deployment

```yaml
# k8s/api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: url-shortener
spec:
  replicas: 2 # Start with 2 replicas
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: url-shortener:latest
          imagePullPolicy: IfNotPresent # Use local Minikube image
          ports:
            - containerPort: 8000
          envFrom:
            - configMapRef:
                name: api-config
            - secretRef:
                name: db-secret
          readinessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 3
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
```

Unlike the databases, the API is stateless and uses a **Deployment**, which supports rolling updates and horizontal scaling. The `readinessProbe` checks `/health` before routing traffic to a new pod — this prevents sending requests to pods that haven't connected to PostgreSQL or Redis yet. The `livenessProbe` restarts a pod if it becomes unresponsive (3 consecutive failures). `imagePullPolicy: IfNotPresent` tells Kubernetes to use the image from Minikube's local Docker daemon instead of pulling from a remote registry.

### API Service

```yaml
# k8s/api-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: api
  namespace: url-shortener
spec:
  type: LoadBalancer # NodePort on Minikube
  selector:
    app: api
  ports:
    - port: 80
      targetPort: 8000
      protocol: TCP
```

The Service is the stable network endpoint for the API pods. `type: LoadBalancer` provisions an external IP on cloud providers; on Minikube, it falls back to a NodePort. The Service distributes traffic across all pods matching `app: api` using round-robin by default. Kubernetes automatically updates the endpoint list as pods scale up/down or restart.

### Horizontal Pod Autoscaler (HPA)

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-hpa
  namespace: url-shortener
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 30 # React quickly to spikes
      policies:
        - type: Pods
          value: 2
          periodSeconds: 60 # Add up to 2 pods per minute
    scaleDown:
      stabilizationWindowSeconds: 300 # Wait 5 min before scaling down
      policies:
        - type: Pods
          value: 1
          periodSeconds: 60 # Remove 1 pod per minute
```

The HPA watches the metrics-server and adjusts the number of API replicas. When average CPU across all pods exceeds 60%, it adds pods (up to 10). When load drops, it removes pods (down to 2). The `behavior` section prevents flapping: scale-up has a short 30-second window to react quickly to traffic spikes, while scale-down waits 5 minutes to avoid premature removal during intermittent load dips.

### Build & Deploy

```bash
# Point your shell to Minikube's Docker daemon (so builds are available to the cluster)
eval $(minikube docker-env)

# Build the image inside Minikube's Docker
docker build -t url-shortener:latest .

# Apply all manifests
kubectl apply -f k8s/

# Verify everything is running
kubectl get all -n url-shortener
```

`eval $(minikube docker-env)` redirects `docker build` to Minikube's internal Docker daemon. This avoids pushing images to a registry — Kubernetes pods can pull `url-shortener:latest` directly from the local daemon.

### Access the API

```bash
# Get the external URL (Minikube tunnels LoadBalancer services)
minikube service api -n url-shortener --url

# Or use port-forwarding for quick local access
kubectl port-forward svc/api 8000:80 -n url-shortener
```

`minikube service` creates a tunnel and prints the accessible URL. `port-forward` maps `localhost:8000` directly to the Service, useful for development without exposing a node port.

### Monitor Autoscaling

```bash
# Watch HPA status (shows current/target CPU and replica count)
kubectl get hpa -n url-shortener --watch

# View pod CPU and memory usage
kubectl top pods -n url-shortener

# Check pod events and scaling decisions
kubectl describe hpa api-hpa -n url-shortener
```

### Load Test to Trigger Autoscaling

```bash
# Install hey (HTTP load generator)
brew install hey

# Send 1000 requests with 50 concurrent workers
hey -n 1000 -c 50 http://$(minikube service api -n url-shortener --url | head -1)/health

# Watch pods scale up in another terminal
kubectl get pods -n url-shortener --watch
```

Under load, you'll see the HPA increase the replica count as CPU crosses the 60% threshold. After the load stops, pods scale back down after the 5-minute stabilization window.

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
