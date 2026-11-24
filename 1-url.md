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
12. [Rate Limiting with Nginx](#rate-limiting-with-nginx)
13. [Click Events Model + Analytics](#click-events-model--analytics)
14. [Load Balancing & Production Deployment on GCP Compute Engine](#load-balancing--production-deployment-on-gcp-compute-engine)

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

#### **NEW** `.dockerignore`

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

docker compose logs -f api                    # Follow API logs
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

@app.get("/health")
def health_check():
    return {"status": "healthy"}

app.include_router(urls.router)
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


@app.get("/health")
def health_check():
    return {"status": "healthy"}

app.include_router(urls.router)
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
fastapi[standard]
sqlalchemy
psycopg[binary]
python-dotenv
pydantic-settings
httpx
pytest
alembic
redis
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


# def increment_click_count(short_code: str) -> int:
#     """Atomically increment click count in Redis."""
#     return redis_client.incr(f"clicks:{short_code}")
```

`redis.from_url` parses the connection string and creates a connection pool automatically. `decode_responses=True` makes Redis return Python strings instead of bytes, avoiding `.decode()` calls everywhere. `setex` sets a key with an expiration time in seconds; after `CACHE_TTL` seconds, Redis automatically deletes the key. The `url:` prefix namespaces cache keys to avoid collisions with other data stored in the same Redis instance. `incr` is atomic, meaning concurrent requests will not lose counts, even without locks.

#### **UPDATED** `app/routers/urls.py`

```python
from app.cache import get_cached_url, set_cached_url, increment_click_count

@router.get("/{short_code}")
def redirect_to_url(short_code: str, request: Request, db: Session = Depends(get_db)):
    """Redirect with Redis cache layer."""

    # 1. Check cache first
    cached = get_cached_url(short_code)
    if cached:
        print(f"CACHE HIT: {short_code}")
        return RedirectResponse(url=cached["original_url"], status_code=301)

    # 2. Cache miss then query database
    print(f"CACHE MISS: {short_code}")
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

    db.commit()

    return RedirectResponse(url=url_record.original_url, status_code=301)
```

The cache-aside pattern works as follows: check Redis first (step 1). If the URL is cached, Redis atomically and redirect immediately without touching PostgreSQL. On a cache miss (step 2), query the database and populate the cache (step 3) so subsequent requests hit Redis.

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

# First redirect: cache miss, hits the DB and populates cache
curl http://localhost:8000/<short_code>

# Second redirect: cache hit, served from Redis (faster)
curl http://localhost:8000/<short_code>
```

---

## Rate Limiting with Nginx

Handling rate limiting inside application code (e.g., Python middleware with Redis) means every abusive or volumetric request still consumes application process time, event loop cycles, and network I/O.

Moving rate limiting to **Nginx** at the reverse proxy layer uses the **leaky bucket algorithm** implemented in C. Malicious or excessive requests are dropped at the network edge with an immediate `429 Too Many Requests` response before they ever reach FastAPI or touch Redis.

**Changed files:**

```
nginx/nginx.conf         # NEW
compose.yaml             # UPDATED
```

#### **NEW** `nginx/nginx.conf`

```nginx
events {
    worker_connections 1024;
}

http {
    # 10MB memory zone tracks ~160,000 IPs at a rate limit of 10 requests/second per IP
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

    upstream fastapi_app {
        server api:8000;
    }

    server {
        listen 80;
        server_name localhost;

        # Allow bursts of up to 20 requests without delay; excess is dropped with 429
        limit_req zone=api_limit burst=20 nodelay;
        limit_req_status 429;

        location / {
            proxy_pass http://fastapi_app;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
```

`limit_req_zone` uses `$binary_remote_addr` (only 4 bytes for IPv4 or 16 bytes for IPv6) to minimize memory overhead. The `10m` zone stores approximately 160,000 unique client IP states. `burst=20 nodelay` allows sudden, legitimate bursts while still throttling continuous floods without adding artificial delay to responsive requests. `limit_req_status 429` ensures standard HTTP 429 status is returned instead of Nginx's default 503.

#### **UPDATED** `compose.yaml`

```yaml
services:
  nginx:
    image: nginx:alpine
    restart: always
    ports:
      - "80:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - api

  api:
    build: .
    restart: unless-stopped
    command: sh -c "alembic upgrade head && fastapi run app/main.py --host 0.0.0.0 --port 8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
      - REDIS_URL=redis://cache:6379/0
      - BASE_URL=http://localhost
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy

  db:
    image: postgres:16
    restart: always
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=urlshortener
    volumes:
      - db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d urlshortener"]
      interval: 5s
      timeout: 5s
      retries: 5

  cache:
    image: redis:7-alpine
    restart: always
    command: redis-server --appendonly yes
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  db-data:
  redis-data:
```

The API service port `8000` is removed from external host mapping and stays isolated inside the Docker network. Only Nginx binds to host port `80`.

### Build, Run & Verify

```bash
docker compose up --build -d
```

Test rate limiting by sending 35 rapid requests (rate limit is 10 r/s with a burst allowance of 20):

```bash
for i in $(seq 1 35); do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost/health
done
```

The first 20 requests return `200`. Once the burst allowance is exhausted, requests return `429`:

**Expected output (last few lines):**

```
200
200
429
429
429
```

---

## Click Events Model + Analytics

We now add granular per-click tracking with the `ClickEvent` model and a full analytics router.

**Changed files:**

```
app/models.py            # UPDATED
app/schemas.py           # UPDATED
app/routers/analytics.py # NEW
app/routers/urls.py      # UPDATED
app/main.py              # UPDATED
tests/test_redirect.py   # UPDATED
```

#### Table Schemas (Final)

```
┌──────────────────────────────────────┐
│              urls                     │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ short_code    VARCHAR(16) UNIQUE      │
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
    short_code = Column(String(16), unique=True, nullable=False, index=True)
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

**Run make migrations:**

```bash
alembic revision --autogenerate -m "add_click_event"
```

**Apply migration:**

```bash
alembic upgrade head
```

#### **UPDATED** `app/schemas.py`

```python
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
from app.models import URL, ClickEvent
from app.cache import get_cached_url, set_cached_url, increment_click_count

@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
  # ...
    url_record = URL(
      # ...
        user_ip=request.client.host,
    )


@router.get("/{short_code}")
def redirect_to_url(short_code: str, request: Request, db: Session = Depends(get_db)):
    """Redirect with Redis cache layer."""

    # 1. Check cache first
    cached = get_cached_url(short_code)
    if cached:
        increment_click_count(short_code)
        print(f"CACHE HIT: {short_code}")
        return RedirectResponse(url=cached["original_url"], status_code=301)

    # 2. Cache miss then query database
    print(f"CACHE MISS: {short_code}")
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

The HTTP `Referer` header (note the deliberate misspelling in the HTTP spec) tells you which page the user clicked the link from. In production, you would send click events to a message queue (Pub/Sub, Kafka) and process them asynchronously to avoid slowing down the redirect response. In production, you would run a periodic batch job to sync `clicks:{short_code}` counters from Redis back to PostgreSQL's `click_count` column, combining Redis speed with PostgreSQL durability.

> **Note:** Use 302 to test clicks on GET /short-code, otherwise browser caches and click count does not change.

#### **NEW** `app/flush_clicks.py`

```py
from app.database import SessionLocal
from app.models import URL
from app.cache import redis_client


def flush_click_counts():
    """Sync Redis click counters back to PostgreSQL's click_count column.

    Redis key pattern: clicks:<short_code>  (set by increment_click_count)
    """
    db = SessionLocal()
    try:
        # 1. Discover all pending counters
        keys = redis_client.keys("clicks:*")
        if not keys:
            print("No pending click counters to flush.")
            return

        flushed = 0
        for key in keys:
            short_code = key.removeprefix("clicks:")

            # 2. Atomically read-and-delete so we never double-count
            delta = redis_client.getdel(key)
            if not delta:
                continue  # another process beat us to it
            delta = int(delta)

            # 3. Apply delta to Postgres
            updated = (
                db.query(URL)
                .filter(URL.short_code == short_code)
                .update({"click_count": URL.click_count + delta})
            )
            if updated:
                flushed += 1
                print(f"  {short_code}: +{delta} clicks written to DB")
            else:
                # short_code was deleted from DB but key lingered in Redis — ignore
                print(f"  {short_code}: skipped (URL not found in DB)")

        db.commit()
        print(f"Flushed click counts for {flushed} URL(s).")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    flush_click_counts()
```

#### **UPDATED** `app/main.py`

```python
from app.routers import urls, analytics

app.include_router(urls.router)
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])

```

#### **UPDATED** `tests/test_redirect.py`

```python
def test_click_count_increments(client):
    """Test that clicks are tracked."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    client.get(f"/{short_code}", follow_redirects=False)
    client.get(f"/{short_code}", follow_redirects=False)

    stats = client.get(f"/api/analytics/{short_code}")
    assert stats.json()["click_count"] == 2
```

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

This script is designed to run as a **cron job** or Cloud Scheduler task (e.g., every hour). It queries for active URLs past their expiration, marks them inactive, and evicts them from Redis. Running cleanup as a separate process avoids adding latency to the redirect path. In production, you could schedule this with `Cloud Scheduler -> Cloud Run Job` or a Kubernetes CronJob.

**Mimic Cron:**

```bash
# Create a URL with negative expiration
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com/expired-test", "expires_in_days": -1}'


cd url
docker compose exec -w /app api python -m app.cleanup
```

```
Deactivated 1 expired URLs.
```

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

# 3. Flush Clicks from Redis to Postgres
cd url
docker compose exec -w /app api python -m app.flush_clicks

# 4. Check analytics
curl http://localhost:8000/api/analytics/<short_code>

# 5. Get detailed click summary
curl http://localhost:8000/api/analytics/<short_code>/summary
```

**Expected analytics output:**

```json
{
  "short_code": "CsWrlrArNQ",
  "original_url": "https://www.example.com/very/long/path",
  "click_count": 3,
  "created_at": "2024-01-15T10:30:00Z",
  "expires_at": "2025-01-14T10:30:00Z",
  "is_active": true
}
```

---

## Load Balancing & Production Deployment on GCP Compute Engine

Instead of managing the cost and operational overhead of a Kubernetes cluster (GKE) and managed cloud databases (Cloud SQL, Memorystore) for this single-service workload, we deploy a production-ready, self-hosted stack on a single **GCP Compute Engine (GCE) VM** using Docker Compose and Nginx.

### Cost & Operational Comparison

| Component | Kubernetes + Managed Cloud (GKE + Cloud SQL + Memorystore) | Single GCP VM (Compute Engine + Docker Compose) |
| :--- | :--- | :--- |
| **Cluster Management Fee** | ~$74/mo (GKE control plane) | $0 |
| **Compute / Nodes** | ~$50 - $100/mo (node pool VMs) | ~$25/mo (`e2-medium`, 2 vCPU, 4GB RAM) |
| **Database** | ~$35 - $100+/mo (Cloud SQL PostgreSQL) | $0 (Self-hosted in Docker container with persistent SSD) |
| **Cache** | ~$35+/mo (Cloud Memorystore Redis) | $0 (Self-hosted in Docker container) |
| **Load Balancer** | ~$18+/mo (GCP HTTPS Load Balancer) | $0 (Nginx reverse proxy on port 80/443) |
| **Total Estimated Cost** | **~$210 - $320+/month** | **~$25/month** |

### CPU & Memory Utilization: Avoiding Waste

When running on a 2 vCPU / 4 GB RAM VM (such as GCP `e2-medium`):

1. **Python GIL & Multi-Instance API:**
   * Python's Global Interpreter Lock (GIL) limits a single Uvicorn/FastAPI process to **1 CPU core** (100% of 1 vCPU).
   * A single API instance on a 2-vCPU VM leaves 50% of total VM CPU idle during traffic spikes.
   * Running **2 API instances** (`api-1` and `api-2`) allows FastAPI to saturate both vCPUs during burst periods while providing fault tolerance.
2. **Leaving Headroom for Postgres, Redis, and Nginx:**
   * Running 2 API instances (rather than 4 or 6) ensures that background services (PostgreSQL writing WAL logs, Redis persisting snapshots, and Nginx routing packets) are never starved of CPU cycles.
3. **Memory Breakdown & Linux Page Cache:**
   * **FastAPI (2 instances):** ~200 – 300 MB total.
   * **Nginx:** ~20 MB.
   * **Redis:** ~50 – 200 MB (100k cached short codes ≈ 50 MB).
   * **PostgreSQL:** ~300 – 800 MB.
   * **Total active containers:** ~1.2 – 1.8 GB RAM.
   * **Unused RAM is NOT wasted:** The Linux kernel automatically claims all remaining free RAM as **Page Cache**, caching PostgreSQL table data and indexes directly in memory. Disk reads from persistent storage drop to near zero.

### The Database Migration Challenge

Running multiple API instances in Docker Compose introduces a potential race condition: if both `api-1` and `api-2` boot simultaneously and execute `alembic upgrade head`, they can collide on database locks.

To solve this, we extract database migrations into a lightweight, one-off **`migration` service** that runs `alembic upgrade head` once before any API instance starts.

### Architecture

```
                 Internet (HTTP/HTTPS: 80, 443)
                               │
                               ▼
                ┌──────────────────────────────┐
                │    GCP Compute Engine VM     │
                │                              │
                │  ┌────────────────────────┐  │
                │  │         Nginx          │  │ (Rate limit, SSL, Least-Conn LB)
                │  └───────┬──────────┬─────┘  │
                │          │          │        │
                │    ┌─────▼────┐ ┌───▼──────┐ │
                │    │  api-1   │ │  api-2   │ │ (FastAPI Uvicorn instances)
                │    └─────┬────┘ └───┬──────┘ │
                │          │          │        │
                │  ┌───────▼──────────▼─────┐  │
                │  │  Redis     PostgreSQL  │  │ (Containers with persistent volumes)
                │  └────────────────────────┘  │
                └──────────────────────────────┘
```

**Changed files:**

```
nginx/nginx.conf         # UPDATED
compose.yaml             # UPDATED
```

#### **UPDATED** `nginx/nginx.conf`

```nginx
events {
    worker_connections 1024;
}

http {
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

    upstream fastapi_cluster {
        least_conn; # Distribute requests to the instance with fewest active connections
        server api-1:8000 max_fails=3 fail_timeout=10s;
        server api-2:8000 max_fails=3 fail_timeout=10s;
    }

    server {
        listen 80;
        server_name _;

        limit_req zone=api_limit burst=20 nodelay;
        limit_req_status 429;

        location / {
            proxy_pass http://fastapi_cluster;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Automatically retry next instance if one fails or times out
            proxy_next_upstream error timeout http_500 http_502 http_503;
        }
    }
}
```

* `least_conn`: Forwards incoming connections to the backend instance with the smallest number of active requests, preventing one container from being overloaded by slow requests.
* `proxy_next_upstream`: Automatically re-routes requests to the other replica if an API instance throws a 500, 502, 503, or times out.

#### **UPDATED** `compose.yaml`

```yaml
services:
  nginx:
    image: nginx:alpine
    restart: always
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - api-1
      - api-2

  # One-off migration runner to prevent Alembic lock contention
  migration:
    build: .
    command: alembic upgrade head
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
    depends_on:
      db:
        condition: service_healthy

  api-1:
    build: .
    restart: unless-stopped
    command: fastapi run app/main.py --host 0.0.0.0 --port 8000
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
      - REDIS_URL=redis://cache:6379/0
      - BASE_URL=http://localhost
    depends_on:
      migration:
        condition: service_completed_successfully
      cache:
        condition: service_healthy

  api-2:
    build: .
    restart: unless-stopped
    command: fastapi run app/main.py --host 0.0.0.0 --port 8000
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/urlshortener
      - REDIS_URL=redis://cache:6379/0
      - BASE_URL=http://localhost
    depends_on:
      migration:
        condition: service_completed_successfully
      cache:
        condition: service_healthy

  db:
    image: postgres:16
    restart: always
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=urlshortener
    volumes:
      - db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d urlshortener"]
      interval: 5s
      timeout: 5s
      retries: 5

  cache:
    image: redis:7-alpine
    restart: always
    command: redis-server --appendonly yes
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  db-data:
  redis-data:
```

### Deploying to GCP Compute Engine

#### 1. Provision the VM

Use `gcloud` or the GCP Console to create an `e2-medium` instance:

```bash
gcloud compute instances create url-shortener-vm \
    --zone=us-central1-a \
    --machine-type=e2-medium \
    --boot-disk-size=30GB \
    --boot-disk-type=pd-balanced \
    --tags=http-server,https-server \
    --image-family=ubuntu-2404-lts-amd64 \
    --image-project=ubuntu-os-cloud
```

Ensure the default VPC firewall allows inbound traffic on port 80 and 443:

```bash
gcloud compute firewall-rules create allow-http-https \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:80,tcp:443 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=http-server,https-server
```

#### 2. Install Docker & Docker Compose on the VM

SSH into the newly created VM:

```bash
gcloud compute ssh url-shortener-vm --zone=us-central1-a
```

Install Docker and the Compose plugin:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
```

Log out and back in for group permissions to take effect.

#### 3. Clone Repository and Start Services

```bash
git clone <your-repository-url>
cd <your-repository-url>/url

# Start all services detached
docker compose up -d --build
```

Verify that all containers are healthy and running:

```bash
docker compose ps
```

**Expected output:**

```
NAME                    IMAGE               COMMAND                  SERVICE      STATUS                    PORTS
url-cache-1             redis:7-alpine      "docker-entrypoint.s…"   cache        Up (healthy)              6379/tcp
url-db-1                postgres:16         "docker-entrypoint.s…"   db           Up (healthy)              5432/tcp
url-migration-1         url-migration       "alembic upgrade head"   migration    Exited (0)
url-api-1-1             url-api-1           "fastapi run app/mai…"   api-1        Up                        8000/tcp
url-api-2-1             url-api-2           "fastapi run app/mai…"   api-2        Up                        8000/tcp
url-nginx-1             nginx:alpine        "/docker-entrypoint.…"   nginx        Up                        0.0.0.0:80->80/tcp, 0.0.0.0:443->443/tcp
```

### Zero-Downtime Rolling Restarts

When deploying new code updates on the VM, you can restart one API container at a time without dropping requests:

```bash
# Pull new changes and rebuild images
git pull
docker compose build api-1 api-2

# Restart instance 1 while instance 2 serves traffic
docker compose restart api-1

# Wait a few seconds for instance 1 to be ready, then restart instance 2
sleep 5
docker compose restart api-2
```

Nginx automatically routes active traffic to the remaining healthy container during each step.

### Load Testing Load Distribution

```bash
# Install hey (HTTP load generator)
brew install hey

# Send 2,000 requests with 50 concurrent workers against the VM external IP
hey -n 2000 -c 50 http://<VM_EXTERNAL_IP>/health
```

In another terminal, tail the logs for both API containers:

```bash
docker compose logs -f api-1 api-2
```

You will see Nginx distributing incoming HTTP requests evenly across both `api-1` and `api-2`, saturating both CPU cores while maintaining low latency.
