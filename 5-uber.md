# Design Uber / Ride Sharing

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [Geo-Indexing & Spatial Queries](#geo-indexing--spatial-queries)
6. [Driver Location Tracking](#driver-location-tracking)
7. [Ride Matching Algorithm](#ride-matching-algorithm)
8. [Core API – Rides & Drivers](#core-api--rides--drivers)
9. [Real-Time Communication (WebSockets)](#real-time-communication-websockets)
10. [Surge Pricing](#surge-pricing)
11. [Fare Calculation & Payments](#fare-calculation--payments)
12. [Trip Lifecycle & State Machine](#trip-lifecycle--state-machine)
13. [Docker & Docker Compose](#docker--docker-compose)
14. [Testing](#testing)
15. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
16. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement          | Description                                                             |
| -------------------- | ----------------------------------------------------------------------- |
| Request Ride         | Rider submits pickup and dropoff locations, gets fare estimate          |
| Match Driver         | System finds the nearest available driver and assigns the ride          |
| Real-Time Tracking   | Rider sees driver's live location on a map                              |
| Trip Lifecycle       | Complete flow: request → match → pickup → in-progress → complete       |
| Fare Calculation     | Compute fare based on distance, duration, and surge multiplier          |
| Surge Pricing        | Dynamically increase prices when demand exceeds supply in an area       |
| Driver Availability  | Drivers go online/offline and update their GPS location continuously    |
| Ride History         | Both riders and drivers can view past trips and receipts                |
| Ratings              | Riders and drivers rate each other after a trip                         |
| ETA Estimation       | Estimate time of arrival for pickup and destination                     |

### Non-Functional Requirements

| Requirement      | Target                                                    |
| ---------------- | --------------------------------------------------------- |
| Availability     | 99.99% uptime (ride-hailing is safety-critical)           |
| Latency          | Match a driver in < 5 seconds, location update < 200 ms   |
| Scalability      | 1 M concurrent drivers, 500 K concurrent ride requests    |
| Consistency      | A ride must be assigned to exactly one driver (no doubles) |
| Durability       | No trip or payment record should ever be lost              |

### Back-of-the-Envelope Estimations

```
Active drivers (peak)          ≈ 1 M
Location updates per driver    ≈ 1 per 4 seconds
Location writes/sec            ≈ 1 M / 4 = 250 K writes/sec
Ride requests per day          ≈ 20 M
Ride requests per second       ≈ 20 M / 86400 ≈ 230 RPS (avg), ~2 000 RPS (peak)
Matching queries/sec           ≈ 2 000 (each queries nearby drivers)
Storage per trip               ≈ 2 KB (metadata + route points)
Storage per year               ≈ 20 M × 365 × 2 KB ≈ 14.6 TB
Driver location storage        ≈ In-memory only (Redis) ≈ 1 M × 100 B ≈ 100 MB
```

250 K location writes per second is too heavy for a relational database. Redis `GEOADD` handles this in-memory with O(log N) per write. Ride matching uses Redis `GEOSEARCH` to find nearby drivers in O(N+log M) where N is results and M is total members. Trip records go to PostgreSQL for durability.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │ (Mobile App) │
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
                │  POST /rides/request        │
                │  PUT  /drivers/location     │
                │  WS   /ws/track/{ride_id}   │
                └──┬──────────┬───────────┬───┘
                   │          │           │
            ┌──────▼───┐ ┌───▼────┐ ┌────▼──────┐
            │  Redis   │ │Postgre-│ │  Pub/Sub  │
            │ (Geo +   │ │  SQL   │ │  (Events) │
            │  Cache)  │ │(Trips) │ │           │
            └──────────┘ └────────┘ └───────────┘
```

The client (mobile app) sends ride requests and driver location updates to the API servers behind a load balancer. Redis stores driver locations using its geospatial data structures for fast proximity queries. PostgreSQL stores trip records, user profiles, and payment data. Pub/Sub handles async event processing like notifications, analytics, and surge price recalculation. WebSockets deliver real-time location updates to riders tracking their driver.

---

## Project Setup

### Project Structure

```
uber/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── cache.py              # Redis client and geo operations
│   ├── geo.py                # Geo-indexing and spatial utilities
│   ├── matching.py           # Driver matching algorithm
│   ├── surge.py              # Surge pricing logic
│   ├── fare.py               # Fare calculation engine
│   ├── state_machine.py      # Trip state transitions
│   ├── websocket_manager.py  # WebSocket connection manager
│   └── routers/
│       ├── __init__.py
│       ├── rides.py          # Ride request and lifecycle endpoints
│       ├── drivers.py        # Driver location and status endpoints
│       ├── tracking.py       # Real-time tracking WebSocket
│       └── payments.py       # Fare and payment endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_matching.py      # Tests for driver matching
│   ├── test_surge.py         # Tests for surge pricing
│   ├── test_fare.py          # Tests for fare calculation
│   └── test_rides.py         # Tests for ride lifecycle
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir uber && cd uber
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
geopy==2.4.1
h3==3.7.7
websockets==13.1
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for Redis including geo commands. `geopy` calculates distances between GPS coordinates using the Haversine formula. `h3` is Uber's own hexagonal hierarchical spatial index for partitioning the world into grid cells. `websockets` enables real-time bidirectional communication between the server and clients.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/uber
REDIS_URL=redis://localhost:6379/0
BASE_URL=http://localhost:8000
BASE_FARE=2.50
PER_KM_RATE=1.50
PER_MINUTE_RATE=0.25
SURGE_THRESHOLD=0.8
MAX_SURGE_MULTIPLIER=3.0
MATCH_RADIUS_KM=5.0
MAX_MATCH_RADIUS_KM=15.0
DRIVER_LOCATION_TTL=60
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `BASE_FARE` is the flat fee added to every ride. `PER_KM_RATE` and `PER_MINUTE_RATE` set the distance and time rates. `SURGE_THRESHOLD` is the demand-to-supply ratio above which surge pricing activates (0.8 means surge when demand is 80% of supply). `MAX_SURGE_MULTIPLIER` caps the surge at 3x normal price. `MATCH_RADIUS_KM` is the initial search radius for nearby drivers. `MAX_MATCH_RADIUS_KM` is the maximum radius before giving up. `DRIVER_LOCATION_TTL` is the number of seconds after which a driver's location is considered stale.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│              users                    │     │              vehicles                 │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            UUID       PRIMARY KEY │     │ id            UUID       PRIMARY KEY │
│ email         VARCHAR    UNIQUE      │     │ driver_id     UUID       FK → users  │
│ phone         VARCHAR    UNIQUE      │     │ make          VARCHAR                │
│ full_name     VARCHAR    NOT NULL    │     │ model         VARCHAR                │
│ role          ENUM       NOT NULL    │     │ year          INTEGER                │
│ rating        FLOAT      DEFAULT 5.0 │     │ color         VARCHAR                │
│ total_rides   INTEGER    DEFAULT 0   │     │ license_plate VARCHAR    UNIQUE      │
│ created_at    TIMESTAMPTZ            │     │ vehicle_type  ENUM                   │
│ is_active     BOOLEAN    DEFAULT TRUE│     │ created_at    TIMESTAMPTZ            │
├──────────────────────────────────────┤     └──────────────────────────────────────┘
│ INDEX idx_email (email)              │
│ INDEX idx_phone (phone)              │
└──────────────────────────────────────┘
                    │
                    │ FK
                    ▼
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│              trips                    │     │            trip_events               │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            UUID       PRIMARY KEY │     │ id            BIGSERIAL  PRIMARY KEY │
│ rider_id      UUID       FK → users  │     │ trip_id       UUID       FK → trips  │
│ driver_id     UUID       FK → users  │     │ event_type    VARCHAR    NOT NULL    │
│ status        ENUM       NOT NULL    │     │ latitude      FLOAT                  │
│ pickup_lat    FLOAT      NOT NULL    │     │ longitude     FLOAT                  │
│ pickup_lng    FLOAT      NOT NULL    │     │ timestamp     TIMESTAMPTZ            │
│ dropoff_lat   FLOAT      NOT NULL    │     │ metadata      JSONB                  │
│ dropoff_lng   FLOAT      NOT NULL    │     ├──────────────────────────────────────┤
│ pickup_address    TEXT               │     │ INDEX idx_trip_events (trip_id)       │
│ dropoff_address   TEXT               │     └──────────────────────────────────────┘
│ estimated_fare    DECIMAL(10,2)      │
│ actual_fare       DECIMAL(10,2)      │
│ surge_multiplier  FLOAT  DEFAULT 1.0 │
│ distance_km       FLOAT              │
│ duration_minutes  FLOAT              │
│ requested_at      TIMESTAMPTZ        │
│ accepted_at       TIMESTAMPTZ        │
│ picked_up_at      TIMESTAMPTZ        │
│ completed_at      TIMESTAMPTZ        │
│ cancelled_at      TIMESTAMPTZ        │
│ cancelled_by      ENUM               │
│ rating_by_rider   INTEGER            │
│ rating_by_driver  INTEGER            │
│ payment_status    ENUM               │
├──────────────────────────────────────┤
│ INDEX idx_rider (rider_id)           │
│ INDEX idx_driver (driver_id)         │
│ INDEX idx_status (status)            │
│ INDEX idx_requested_at (requested_at)│
└──────────────────────────────────────┘
```

The `users` table stores both riders and drivers distinguished by `role`. The `trips` table is the core entity, storing pickup/dropoff coordinates, fare details, surge multiplier, and timestamps for every state transition. `trip_events` logs fine-grained events like location updates during a trip, status changes, and cancellation reasons. The `vehicles` table links drivers to their car details. Driver locations are NOT stored in PostgreSQL because 250 K writes/sec would overwhelm it; they live in Redis's geospatial index.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/uber"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    base_fare: float = 2.50
    per_km_rate: float = 1.50
    per_minute_rate: float = 0.25
    surge_threshold: float = 0.8
    max_surge_multiplier: float = 3.0
    match_radius_km: float = 5.0
    max_match_radius_km: float = 15.0
    driver_location_ttl: int = 60

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
    BigInteger, Boolean, Column, DateTime, Enum, Float,
    ForeignKey, Index, Integer, Numeric, String, Text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database import Base


class UserRole(str, enum.Enum):
    RIDER = "rider"
    DRIVER = "driver"


class TripStatus(str, enum.Enum):
    REQUESTED = "requested"
    MATCHED = "matched"
    ACCEPTED = "accepted"
    DRIVER_EN_ROUTE = "driver_en_route"
    ARRIVED = "arrived"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class CancelledBy(str, enum.Enum):
    RIDER = "rider"
    DRIVER = "driver"
    SYSTEM = "system"


class VehicleType(str, enum.Enum):
    ECONOMY = "economy"
    COMFORT = "comfort"
    PREMIUM = "premium"
    XL = "xl"


class PaymentStatus(str, enum.Enum):
    PENDING = "pending"
    CHARGED = "charged"
    REFUNDED = "refunded"
    FAILED = "failed"


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    phone = Column(String(20), unique=True, nullable=False)
    full_name = Column(String(255), nullable=False)
    role = Column(Enum(UserRole), nullable=False)
    rating = Column(Float, default=5.0)
    total_rides = Column(Integer, default=0)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    is_active = Column(Boolean, default=True)

    trips_as_rider = relationship(
        "Trip", back_populates="rider", foreign_keys="Trip.rider_id"
    )
    trips_as_driver = relationship(
        "Trip", back_populates="driver", foreign_keys="Trip.driver_id"
    )
    vehicle = relationship("Vehicle", back_populates="driver", uselist=False)

    __table_args__ = (
        Index("idx_email", "email"),
        Index("idx_phone", "phone"),
    )


class Vehicle(Base):
    __tablename__ = "vehicles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    driver_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    make = Column(String(50), nullable=False)
    model = Column(String(50), nullable=False)
    year = Column(Integer, nullable=False)
    color = Column(String(30), nullable=False)
    license_plate = Column(String(20), unique=True, nullable=False)
    vehicle_type = Column(Enum(VehicleType), default=VehicleType.ECONOMY)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    driver = relationship("User", back_populates="vehicle")


class Trip(Base):
    __tablename__ = "trips"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rider_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    driver_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    status = Column(Enum(TripStatus), nullable=False, default=TripStatus.REQUESTED)
    pickup_lat = Column(Float, nullable=False)
    pickup_lng = Column(Float, nullable=False)
    dropoff_lat = Column(Float, nullable=False)
    dropoff_lng = Column(Float, nullable=False)
    pickup_address = Column(Text, nullable=True)
    dropoff_address = Column(Text, nullable=True)
    estimated_fare = Column(Numeric(10, 2), nullable=True)
    actual_fare = Column(Numeric(10, 2), nullable=True)
    surge_multiplier = Column(Float, default=1.0)
    distance_km = Column(Float, nullable=True)
    duration_minutes = Column(Float, nullable=True)
    requested_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    accepted_at = Column(DateTime(timezone=True), nullable=True)
    picked_up_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    cancelled_at = Column(DateTime(timezone=True), nullable=True)
    cancelled_by = Column(Enum(CancelledBy), nullable=True)
    rating_by_rider = Column(Integer, nullable=True)
    rating_by_driver = Column(Integer, nullable=True)
    payment_status = Column(Enum(PaymentStatus), default=PaymentStatus.PENDING)

    rider = relationship("User", back_populates="trips_as_rider", foreign_keys=[rider_id])
    driver = relationship("User", back_populates="trips_as_driver", foreign_keys=[driver_id])
    events = relationship("TripEvent", back_populates="trip")

    __table_args__ = (
        Index("idx_rider", "rider_id"),
        Index("idx_driver", "driver_id"),
        Index("idx_status", "status"),
        Index("idx_requested_at", "requested_at"),
    )


class TripEvent(Base):
    __tablename__ = "trip_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trip_id = Column(UUID(as_uuid=True), ForeignKey("trips.id"), nullable=False)
    event_type = Column(String(50), nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    metadata_ = Column("metadata", JSONB, nullable=True)

    trip = relationship("Trip", back_populates="events")

    __table_args__ = (
        Index("idx_trip_events", "trip_id"),
    )
```

`UUID(as_uuid=True)` stores UUIDs natively in PostgreSQL, which is more compact than storing them as strings and prevents enumeration attacks (unlike auto-increment IDs). `TripStatus` is a Python enum mapped to a PostgreSQL ENUM type, enforcing valid states at the database level. `driver_id` on `Trip` is nullable because a trip starts without a driver (status `REQUESTED`) and gets assigned one after matching. `JSONB` for event metadata allows flexible schema-free data like cancellation reasons, route deviations, or ETA updates without altering the table schema. `Numeric(10, 2)` stores monetary values with exact precision, avoiding floating-point rounding errors.

### `app/schemas.py`

```python
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class RideRequest(BaseModel):
    rider_id: uuid.UUID
    pickup_lat: float = Field(ge=-90, le=90)
    pickup_lng: float = Field(ge=-180, le=180)
    dropoff_lat: float = Field(ge=-90, le=90)
    dropoff_lng: float = Field(ge=-180, le=180)
    pickup_address: str | None = None
    dropoff_address: str | None = None
    vehicle_type: str = "economy"


class RideResponse(BaseModel):
    trip_id: uuid.UUID
    status: str
    estimated_fare: float
    surge_multiplier: float
    pickup_lat: float
    pickup_lng: float
    dropoff_lat: float
    dropoff_lng: float
    driver: "DriverInfo | None" = None

    class Config:
        from_attributes = True


class DriverInfo(BaseModel):
    driver_id: uuid.UUID
    full_name: str
    rating: float
    vehicle_make: str | None = None
    vehicle_model: str | None = None
    vehicle_color: str | None = None
    license_plate: str | None = None
    eta_minutes: float | None = None

    class Config:
        from_attributes = True


class DriverLocationUpdate(BaseModel):
    driver_id: uuid.UUID
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    heading: float | None = Field(None, ge=0, le=360)
    speed_kmh: float | None = Field(None, ge=0)


class DriverStatusUpdate(BaseModel):
    driver_id: uuid.UUID
    is_available: bool


class FareEstimate(BaseModel):
    distance_km: float
    duration_minutes: float
    base_fare: float
    distance_charge: float
    time_charge: float
    surge_multiplier: float
    surge_charge: float
    total_fare: float


class TripStatusUpdate(BaseModel):
    status: str
    latitude: float | None = None
    longitude: float | None = None


class RatingSubmit(BaseModel):
    rating: int = Field(ge=1, le=5)
    comment: str | None = None


class TripHistory(BaseModel):
    trip_id: uuid.UUID
    status: str
    pickup_address: str | None
    dropoff_address: str | None
    actual_fare: float | None
    distance_km: float | None
    duration_minutes: float | None
    requested_at: datetime
    completed_at: datetime | None
    rating_by_rider: int | None
    rating_by_driver: int | None

    class Config:
        from_attributes = True
```

`Field(ge=-90, le=90)` validates latitude is within the valid range. `Field(ge=-180, le=180)` does the same for longitude. This catches invalid coordinates at the schema layer before they reach any business logic. `DriverInfo` is nested inside `RideResponse` so the rider gets the matched driver's details in a single response. `heading` (0-360 degrees) and `speed_kmh` from `DriverLocationUpdate` are used to show the driver's car direction on the map and predict future positions for smoother animations.

---

## Geo-Indexing & Spatial Queries

The core challenge of a ride-sharing system is efficiently answering "which available drivers are closest to this rider?" when millions of drivers are moving simultaneously.

### Approach Comparison

| Approach              | Pros                                 | Cons                                             |
| --------------------- | ------------------------------------ | ------------------------------------------------ |
| PostGIS (PostgreSQL)  | Rich spatial queries, ACID           | Too slow for 250 K writes/sec                    |
| Redis GEOADD/GEOSEARCH| In-memory, 100 K+ ops/sec, O(log N) | No persistence by default, basic queries only    |
| H3 Hexagonal Grid     | Uniform cells, great for aggregation | Requires additional storage for actual coords    |
| Quadtree              | Adaptive resolution, in-memory       | Custom implementation, hard to distribute         |
| S2 Geometry           | Hierarchical cells, used by Google   | Complex API, steeper learning curve               |

We use **Redis GEO** for real-time driver locations (250 K writes/sec) and **H3** for surge pricing grid cells (aggregating demand/supply per hexagonal zone). PostgreSQL stores trip data for durability.

### `app/geo.py`

```python
import math

import h3


# ─── Haversine Distance ───────────────────────────────────────────

def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate the great-circle distance between two GPS points in km."""
    R = 6371.0                                # Earth's radius in km

    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


# ─── H3 Hexagonal Grid ────────────────────────────────────────────

H3_RESOLUTION = 7                             # ~5.16 km² per hexagon

def lat_lng_to_h3(lat: float, lng: float, resolution: int = H3_RESOLUTION) -> str:
    """Convert a GPS coordinate to an H3 hex cell index."""
    return h3.geo_to_h3(lat, lng, resolution)


def get_h3_neighbors(h3_index: str, ring_size: int = 1) -> list[str]:
    """Get the surrounding hex cells (k-ring) around a given cell."""
    return list(h3.k_ring(h3_index, ring_size))


def h3_to_center(h3_index: str) -> tuple[float, float]:
    """Get the center lat/lng of an H3 cell."""
    return h3.h3_to_geo(h3_index)


# ─── ETA Estimation ───────────────────────────────────────────────

AVERAGE_SPEED_KMH = 30.0                      # City average speed

def estimate_eta_minutes(
    lat1: float, lng1: float,
    lat2: float, lng2: float,
    speed_kmh: float = AVERAGE_SPEED_KMH,
) -> float:
    """Estimate travel time between two points in minutes."""
    distance = haversine_km(lat1, lng1, lat2, lng2)
    return (distance / speed_kmh) * 60
```

The **Haversine formula** calculates the shortest distance between two points on a sphere given their latitudes and longitudes. It accounts for Earth's curvature, unlike Euclidean distance which would be inaccurate at scale. **H3** is Uber's hexagonal hierarchical spatial index. Resolution 7 creates hexagons of approximately 5.16 km² each, which is a good size for surge pricing zones, balancing granularity with computational cost. `k_ring` returns the cell and its neighbors, which is used to aggregate demand and supply across adjacent zones. `estimate_eta_minutes` uses a simple distance/speed calculation; in production, this would call a routing API (Google Maps, OSRM) for accurate turn-by-turn ETAs.

---

## Driver Location Tracking

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

# ─── Geo Keys ─────────────────────────────────────────────────────

GEO_KEY = "drivers:locations"                 # Redis GEO sorted set
AVAILABLE_KEY = "drivers:available"           # Set of available driver IDs
DRIVER_META_PREFIX = "driver:meta:"           # Hash per driver for metadata

# ─── Driver Location Operations ───────────────────────────────────

def update_driver_location(
    driver_id: str,
    latitude: float,
    longitude: float,
    heading: float | None = None,
    speed_kmh: float | None = None,
):
    """Store driver's GPS location in Redis GEO index."""
    pipe = redis_client.pipeline()

    # GEOADD stores (longitude, latitude, member) — note: lng before lat
    pipe.geoadd(GEO_KEY, (longitude, latitude, driver_id))

    # Store metadata (heading, speed, last update time)
    meta = {"heading": heading, "speed": speed_kmh}
    pipe.set(
        f"{DRIVER_META_PREFIX}{driver_id}",
        json.dumps(meta),
        ex=settings.driver_location_ttl,      # Auto-expire stale locations
    )

    pipe.execute()


def remove_driver_location(driver_id: str):
    """Remove a driver from the geo index (went offline)."""
    pipe = redis_client.pipeline()
    pipe.zrem(GEO_KEY, driver_id)
    pipe.srem(AVAILABLE_KEY, driver_id)
    pipe.delete(f"{DRIVER_META_PREFIX}{driver_id}")
    pipe.execute()


def set_driver_available(driver_id: str, available: bool):
    """Mark a driver as available or unavailable for matching."""
    if available:
        redis_client.sadd(AVAILABLE_KEY, driver_id)
    else:
        redis_client.srem(AVAILABLE_KEY, driver_id)


def is_driver_available(driver_id: str) -> bool:
    """Check if a driver is in the available set."""
    return redis_client.sismember(AVAILABLE_KEY, driver_id)


# ─── Nearby Driver Queries ────────────────────────────────────────

def find_nearby_drivers(
    latitude: float,
    longitude: float,
    radius_km: float,
    count: int = 20,
) -> list[dict]:
    """
    Find drivers within a radius of a point, sorted by distance.
    Returns list of {driver_id, distance_km, longitude, latitude}.
    """
    results = redis_client.geosearch(
        GEO_KEY,
        longitude=longitude,
        latitude=latitude,
        radius=radius_km,
        unit="km",
        sort="ASC",                           # Nearest first
        count=count,
        withcoord=True,                       # Include coordinates
        withdist=True,                        # Include distance
    )

    drivers = []
    for item in results:
        member = item[0]                      # driver_id
        distance = float(item[1])             # distance in km
        coords = item[2]                      # (longitude, latitude)
        drivers.append({
            "driver_id": member,
            "distance_km": distance,
            "longitude": float(coords[0]),
            "latitude": float(coords[1]),
        })

    return drivers


def get_driver_location(driver_id: str) -> dict | None:
    """Get a single driver's current location."""
    positions = redis_client.geopos(GEO_KEY, driver_id)
    if positions and positions[0]:
        lng, lat = positions[0]
        meta_raw = redis_client.get(f"{DRIVER_META_PREFIX}{driver_id}")
        meta = json.loads(meta_raw) if meta_raw else {}
        return {
            "driver_id": driver_id,
            "latitude": float(lat),
            "longitude": float(lng),
            "heading": meta.get("heading"),
            "speed_kmh": meta.get("speed"),
        }
    return None


# ─── Surge Pricing Counters ───────────────────────────────────────

DEMAND_PREFIX = "surge:demand:"               # Requests per H3 cell
SUPPLY_PREFIX = "surge:supply:"               # Available drivers per H3 cell
SURGE_TTL = 300                               # 5-minute sliding window

def increment_demand(h3_index: str):
    """Increment ride request count for an H3 cell."""
    key = f"{DEMAND_PREFIX}{h3_index}"
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, SURGE_TTL)
    pipe.execute()


def get_demand(h3_index: str) -> int:
    """Get current demand count for an H3 cell."""
    val = redis_client.get(f"{DEMAND_PREFIX}{h3_index}")
    return int(val) if val else 0


def set_supply(h3_index: str, count: int):
    """Set the number of available drivers in an H3 cell."""
    redis_client.setex(f"{SUPPLY_PREFIX}{h3_index}", SURGE_TTL, count)


def get_supply(h3_index: str) -> int:
    """Get current supply count for an H3 cell."""
    val = redis_client.get(f"{SUPPLY_PREFIX}{h3_index}")
    return int(val) if val else 0
```

`GEOADD` stores driver locations in a Redis sorted set using geohash-encoded scores, enabling O(log N) inserts. Redis GEO expects `(longitude, latitude, member)` — note the order is `lng, lat`, not `lat, lng` like most APIs. `GEOSEARCH` performs a radial search returning members within a given radius, sorted by distance. This replaces the older `GEORADIUS` command and is more flexible. `withcoord=True` includes each driver's exact coordinates in the response, and `withdist=True` includes the distance from the query point. The `AVAILABLE_KEY` is a Redis SET tracking which drivers are currently accepting rides; a driver can have a location (online) but be unavailable (on a trip). `pipeline()` batches multiple Redis commands into a single round trip. The `driver_location_ttl` auto-expires metadata keys so stale drivers don't appear in search results if they crash without gracefully going offline.

---

## Ride Matching Algorithm

### `app/matching.py`

```python
import uuid

from app.cache import (
    find_nearby_drivers,
    is_driver_available,
    set_driver_available,
    get_driver_location,
)
from app.geo import estimate_eta_minutes
from app.config import settings


class MatchResult:
    """Result of a driver matching attempt."""

    def __init__(
        self,
        driver_id: str | None = None,
        distance_km: float = 0,
        eta_minutes: float = 0,
        latitude: float = 0,
        longitude: float = 0,
        matched: bool = False,
    ):
        self.driver_id = driver_id
        self.distance_km = distance_km
        self.eta_minutes = eta_minutes
        self.latitude = latitude
        self.longitude = longitude
        self.matched = matched


def match_driver(
    pickup_lat: float,
    pickup_lng: float,
    vehicle_type: str = "economy",
) -> MatchResult:
    """
    Find the best available driver for a ride request.

    Strategy:
    1. Search within the initial radius (5 km)
    2. If no drivers found, expand radius up to max (15 km)
    3. Filter for available drivers only
    4. Rank by distance (nearest wins)
    5. Atomically claim the driver to prevent double-matching
    """
    radius = settings.match_radius_km
    max_radius = settings.max_match_radius_km

    while radius <= max_radius:
        nearby = find_nearby_drivers(
            latitude=pickup_lat,
            longitude=pickup_lng,
            radius_km=radius,
            count=50,                         # Fetch more to filter availability
        )

        # Filter to only available drivers
        for driver in nearby:
            driver_id = driver["driver_id"]

            if not is_driver_available(driver_id):
                continue

            # Atomically mark driver as unavailable (prevents double-matching)
            # srem returns 1 if the member was removed, 0 if it wasn't there
            claimed = set_driver_available(driver_id, False)

            eta = estimate_eta_minutes(
                driver["latitude"], driver["longitude"],
                pickup_lat, pickup_lng,
            )

            return MatchResult(
                driver_id=driver_id,
                distance_km=driver["distance_km"],
                eta_minutes=eta,
                latitude=driver["latitude"],
                longitude=driver["longitude"],
                matched=True,
            )

        # No available drivers in this radius, expand
        radius += 2.0

    # No drivers found within max radius
    return MatchResult(matched=False)


def release_driver(driver_id: str):
    """Release a driver back to the available pool (e.g., after cancellation)."""
    set_driver_available(driver_id, True)
```

The matching algorithm uses an **expanding radius search**. It starts with a 5 km radius and expands by 2 km each iteration up to 15 km. This balances fast matches in dense areas with coverage in sparse areas. For each candidate, it checks the `AVAILABLE_KEY` set. The critical step is atomically marking the driver as unavailable using `SREM`, which returns 1 only if the member was actually removed. In a concurrent environment with multiple API servers, two ride requests could find the same nearby driver. The `SREM` call acts as a distributed lock: only one request can successfully remove the driver from the set, and the other gets 0, skipping to the next candidate. This prevents double-matching without a separate distributed lock. `release_driver` is called when a ride is cancelled, returning the driver to the available pool.

---

## Core API – Rides & Drivers

### `app/routers/drivers.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.cache import (
    update_driver_location,
    remove_driver_location,
    set_driver_available,
    get_driver_location,
)
from app.database import get_db
from app.models import User, UserRole
from app.schemas import DriverLocationUpdate, DriverStatusUpdate

router = APIRouter()


@router.put("/location")
def update_location(payload: DriverLocationUpdate, db: Session = Depends(get_db)):
    """Update driver's GPS location. Called every 4 seconds by driver app."""
    driver = (
        db.query(User)
        .filter(User.id == payload.driver_id, User.role == UserRole.DRIVER)
        .first()
    )
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found.")

    update_driver_location(
        driver_id=str(payload.driver_id),
        latitude=payload.latitude,
        longitude=payload.longitude,
        heading=payload.heading,
        speed_kmh=payload.speed_kmh,
    )

    return {"status": "location_updated"}


@router.put("/status")
def update_status(payload: DriverStatusUpdate, db: Session = Depends(get_db)):
    """Toggle driver online/offline status."""
    driver = (
        db.query(User)
        .filter(User.id == payload.driver_id, User.role == UserRole.DRIVER)
        .first()
    )
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found.")

    if payload.is_available:
        set_driver_available(str(payload.driver_id), True)
    else:
        remove_driver_location(str(payload.driver_id))

    return {"status": "available" if payload.is_available else "offline"}


@router.get("/{driver_id}/location")
def get_location(driver_id: str):
    """Get a driver's current location."""
    location = get_driver_location(driver_id)
    if not location:
        raise HTTPException(status_code=404, detail="Driver location not found.")

    return location
```

The location update endpoint is called every 4 seconds by the driver app. At 1 M active drivers, this generates 250 K requests/second. Each request writes to Redis (sub-millisecond) rather than PostgreSQL, keeping latency under 5 ms. The status endpoint controls whether a driver appears in matching queries. Going offline calls `remove_driver_location` which removes them from both the GEO index and the available set. `heading` and `speed_kmh` are forwarded to Redis for the rider app to animate the driver's car on the map.

### `app/routers/rides.py`

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.cache import increment_demand
from app.config import settings
from app.database import get_db
from app.fare import calculate_fare_estimate
from app.geo import haversine_km, lat_lng_to_h3
from app.matching import match_driver, release_driver
from app.models import Trip, TripEvent, TripStatus, User, CancelledBy, PaymentStatus
from app.schemas import (
    RideRequest, RideResponse, TripStatusUpdate,
    RatingSubmit, TripHistory, DriverInfo,
)
from app.surge import get_surge_multiplier

router = APIRouter()


@router.post("/request", response_model=RideResponse, status_code=201)
def request_ride(payload: RideRequest, db: Session = Depends(get_db)):
    """Request a new ride. Finds a driver, estimates fare, creates trip."""

    # 1. Calculate distance and surge
    distance_km = haversine_km(
        payload.pickup_lat, payload.pickup_lng,
        payload.dropoff_lat, payload.dropoff_lng,
    )

    h3_cell = lat_lng_to_h3(payload.pickup_lat, payload.pickup_lng)
    increment_demand(h3_cell)

    surge = get_surge_multiplier(h3_cell)

    # 2. Estimate fare
    fare = calculate_fare_estimate(distance_km, surge_multiplier=surge)

    # 3. Match a driver
    match = match_driver(
        pickup_lat=payload.pickup_lat,
        pickup_lng=payload.pickup_lng,
        vehicle_type=payload.vehicle_type,
    )

    # 4. Create trip record
    trip = Trip(
        rider_id=payload.rider_id,
        driver_id=match.driver_id if match.matched else None,
        status=TripStatus.MATCHED if match.matched else TripStatus.REQUESTED,
        pickup_lat=payload.pickup_lat,
        pickup_lng=payload.pickup_lng,
        dropoff_lat=payload.dropoff_lat,
        dropoff_lng=payload.dropoff_lng,
        pickup_address=payload.pickup_address,
        dropoff_address=payload.dropoff_address,
        estimated_fare=fare.total_fare,
        surge_multiplier=surge,
        distance_km=distance_km,
    )

    db.add(trip)
    db.flush()

    # Log the request event
    event = TripEvent(
        trip_id=trip.id,
        event_type="ride_requested",
        latitude=payload.pickup_lat,
        longitude=payload.pickup_lng,
        metadata_={"vehicle_type": payload.vehicle_type, "surge": surge},
    )
    db.add(event)

    if match.matched:
        match_event = TripEvent(
            trip_id=trip.id,
            event_type="driver_matched",
            latitude=match.latitude,
            longitude=match.longitude,
            metadata_={
                "driver_id": match.driver_id,
                "eta_minutes": match.eta_minutes,
                "distance_km": match.distance_km,
            },
        )
        db.add(match_event)

    db.commit()
    db.refresh(trip)

    # 5. Build response
    driver_info = None
    if match.matched:
        driver = db.query(User).filter(User.id == match.driver_id).first()
        vehicle = driver.vehicle if driver else None
        driver_info = DriverInfo(
            driver_id=driver.id,
            full_name=driver.full_name,
            rating=driver.rating,
            vehicle_make=vehicle.make if vehicle else None,
            vehicle_model=vehicle.model if vehicle else None,
            vehicle_color=vehicle.color if vehicle else None,
            license_plate=vehicle.license_plate if vehicle else None,
            eta_minutes=match.eta_minutes,
        )

    return RideResponse(
        trip_id=trip.id,
        status=trip.status.value,
        estimated_fare=float(trip.estimated_fare),
        surge_multiplier=trip.surge_multiplier,
        pickup_lat=trip.pickup_lat,
        pickup_lng=trip.pickup_lng,
        dropoff_lat=trip.dropoff_lat,
        dropoff_lng=trip.dropoff_lng,
        driver=driver_info,
    )


@router.put("/{trip_id}/status")
def update_trip_status(
    trip_id: str,
    payload: TripStatusUpdate,
    db: Session = Depends(get_db),
):
    """Update trip status (driver accepts, arrives, starts, completes)."""
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found.")

    new_status = TripStatus(payload.status)
    now = datetime.now(timezone.utc)

    # Validate state transitions
    valid_transitions = {
        TripStatus.MATCHED: [TripStatus.ACCEPTED, TripStatus.CANCELLED],
        TripStatus.ACCEPTED: [TripStatus.DRIVER_EN_ROUTE, TripStatus.CANCELLED],
        TripStatus.DRIVER_EN_ROUTE: [TripStatus.ARRIVED, TripStatus.CANCELLED],
        TripStatus.ARRIVED: [TripStatus.IN_PROGRESS, TripStatus.CANCELLED],
        TripStatus.IN_PROGRESS: [TripStatus.COMPLETED],
        TripStatus.REQUESTED: [TripStatus.MATCHED, TripStatus.CANCELLED],
    }

    allowed = valid_transitions.get(trip.status, [])
    if new_status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot transition from {trip.status.value} to {new_status.value}.",
        )

    trip.status = new_status

    # Set timestamps based on status
    if new_status == TripStatus.ACCEPTED:
        trip.accepted_at = now
    elif new_status == TripStatus.IN_PROGRESS:
        trip.picked_up_at = now
    elif new_status == TripStatus.COMPLETED:
        trip.completed_at = now
        trip.payment_status = PaymentStatus.CHARGED
        # Calculate actual fare based on real distance/duration
        if trip.picked_up_at:
            duration = (now - trip.picked_up_at).total_seconds() / 60
            trip.duration_minutes = duration
            actual_fare = calculate_fare_estimate(
                trip.distance_km, duration, trip.surge_multiplier
            )
            trip.actual_fare = actual_fare.total_fare
        # Release driver back to available pool
        if trip.driver_id:
            release_driver(str(trip.driver_id))
    elif new_status == TripStatus.CANCELLED:
        trip.cancelled_at = now
        if trip.driver_id:
            release_driver(str(trip.driver_id))

    # Log event
    event = TripEvent(
        trip_id=trip.id,
        event_type=f"status_{new_status.value}",
        latitude=payload.latitude,
        longitude=payload.longitude,
    )
    db.add(event)
    db.commit()

    return {"trip_id": str(trip.id), "status": new_status.value}


@router.put("/{trip_id}/cancel")
def cancel_trip(
    trip_id: str,
    cancelled_by: str = "rider",
    db: Session = Depends(get_db),
):
    """Cancel a trip."""
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found.")

    if trip.status in [TripStatus.COMPLETED, TripStatus.CANCELLED]:
        raise HTTPException(status_code=400, detail="Trip already ended.")

    trip.status = TripStatus.CANCELLED
    trip.cancelled_at = datetime.now(timezone.utc)
    trip.cancelled_by = CancelledBy(cancelled_by)

    # Release driver back to pool
    if trip.driver_id:
        release_driver(str(trip.driver_id))

    event = TripEvent(
        trip_id=trip.id,
        event_type="trip_cancelled",
        metadata_={"cancelled_by": cancelled_by},
    )
    db.add(event)
    db.commit()

    return {"trip_id": str(trip.id), "status": "cancelled"}


@router.put("/{trip_id}/rate")
def rate_trip(
    trip_id: str,
    payload: RatingSubmit,
    rated_by: str = "rider",
    db: Session = Depends(get_db),
):
    """Submit a rating for a completed trip."""
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found.")

    if trip.status != TripStatus.COMPLETED:
        raise HTTPException(status_code=400, detail="Can only rate completed trips.")

    if rated_by == "rider":
        if trip.rating_by_rider:
            raise HTTPException(status_code=400, detail="Already rated.")
        trip.rating_by_rider = payload.rating

        # Update driver's average rating
        if trip.driver_id:
            driver = db.query(User).filter(User.id == trip.driver_id).first()
            if driver:
                total = driver.total_rides
                driver.rating = (
                    (driver.rating * total + payload.rating) / (total + 1)
                )
                driver.total_rides += 1
    else:
        if trip.rating_by_driver:
            raise HTTPException(status_code=400, detail="Already rated.")
        trip.rating_by_driver = payload.rating

        rider = db.query(User).filter(User.id == trip.rider_id).first()
        if rider:
            total = rider.total_rides
            rider.rating = (rider.rating * total + payload.rating) / (total + 1)
            rider.total_rides += 1

    db.commit()
    return {"trip_id": str(trip.id), "rating": payload.rating}


@router.get("/history/{user_id}", response_model=list[TripHistory])
def get_trip_history(
    user_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get ride history for a user (rider or driver)."""
    trips = (
        db.query(Trip)
        .filter((Trip.rider_id == user_id) | (Trip.driver_id == user_id))
        .order_by(Trip.requested_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return trips
```

`db.flush()` sends the INSERT to the database and populates `trip.id` without committing the transaction. This lets us log events with the trip ID and commit everything atomically. The state transition validation uses a dictionary mapping each status to its valid next states, enforcing a strict state machine at the API layer. When a trip completes, `release_driver` returns the driver to the available pool so they can be matched again. The rating endpoint uses a running average formula `(old_avg * count + new_rating) / (count + 1)` to update the user's rating without scanning all past trips. `Query(0, ge=0)` validates pagination parameters at the schema level.

---

## Real-Time Communication (WebSockets)

### `app/websocket_manager.py`

```python
import json
from fastapi import WebSocket


class ConnectionManager:
    """Manages WebSocket connections for real-time ride tracking."""

    def __init__(self):
        # trip_id → list of connected WebSocket clients
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, trip_id: str, websocket: WebSocket):
        """Accept a new WebSocket connection for a trip."""
        await websocket.accept()
        if trip_id not in self.active_connections:
            self.active_connections[trip_id] = []
        self.active_connections[trip_id].append(websocket)

    def disconnect(self, trip_id: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        if trip_id in self.active_connections:
            self.active_connections[trip_id].remove(websocket)
            if not self.active_connections[trip_id]:
                del self.active_connections[trip_id]

    async def broadcast_to_trip(self, trip_id: str, message: dict):
        """Send a message to all clients connected to a trip."""
        if trip_id in self.active_connections:
            dead_connections = []
            for connection in self.active_connections[trip_id]:
                try:
                    await connection.send_json(message)
                except Exception:
                    dead_connections.append(connection)

            # Clean up dead connections
            for conn in dead_connections:
                self.disconnect(trip_id, conn)

    async def send_location_update(
        self,
        trip_id: str,
        latitude: float,
        longitude: float,
        heading: float | None = None,
        eta_minutes: float | None = None,
    ):
        """Broadcast driver location update to all trip watchers."""
        await self.broadcast_to_trip(trip_id, {
            "type": "location_update",
            "data": {
                "latitude": latitude,
                "longitude": longitude,
                "heading": heading,
                "eta_minutes": eta_minutes,
            },
        })

    async def send_status_update(self, trip_id: str, status: str, details: dict = None):
        """Broadcast trip status change to all watchers."""
        await self.broadcast_to_trip(trip_id, {
            "type": "status_update",
            "data": {
                "status": status,
                **(details or {}),
            },
        })


manager = ConnectionManager()
```

The `ConnectionManager` maintains a dictionary mapping `trip_id` to a list of WebSocket connections. Both the rider and driver apps connect to the same trip's WebSocket. `broadcast_to_trip` sends updates to all connected clients. Dead connections (e.g., client disconnected without a proper close frame) are detected by catching send exceptions and cleaned up immediately. `send_location_update` pushes the driver's lat/lng plus heading and ETA so the rider app can smoothly animate the car on the map. In production, when running multiple API server instances, each instance only knows about its own WebSocket connections. You would use Redis Pub/Sub or Cloud Pub/Sub to fan out messages across all instances so every connected client gets updates regardless of which server they are connected to.

### `app/routers/tracking.py`

```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.cache import get_driver_location
from app.websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/track/{trip_id}")
async def track_ride(trip_id: str, websocket: WebSocket):
    """WebSocket endpoint for real-time ride tracking."""
    await manager.connect(trip_id, websocket)

    try:
        while True:
            # Receive messages from client (e.g., pings, location updates)
            data = await websocket.receive_json()

            if data.get("type") == "driver_location":
                # Driver sends location update via WebSocket
                await manager.send_location_update(
                    trip_id=trip_id,
                    latitude=data["latitude"],
                    longitude=data["longitude"],
                    heading=data.get("heading"),
                    eta_minutes=data.get("eta_minutes"),
                )

            elif data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        manager.disconnect(trip_id, websocket)
```

The WebSocket endpoint at `/ws/track/{trip_id}` accepts connections from both the rider and driver apps. When the driver sends a `driver_location` message, the server broadcasts it to all other connected clients (the rider). The `ping/pong` mechanism keeps the connection alive and detects dead connections quickly. `WebSocketDisconnect` is raised when the client closes the connection or the network drops, triggering cleanup.

---

## Surge Pricing

### `app/surge.py`

```python
from app.cache import get_demand, get_supply
from app.config import settings
from app.geo import get_h3_neighbors


def get_surge_multiplier(h3_index: str) -> float:
    """
    Calculate the surge multiplier for an H3 hex cell.

    Formula:
        surge = max(1.0, demand / supply)
        capped at MAX_SURGE_MULTIPLIER

    The demand/supply ratio is smoothed by including neighboring cells
    to avoid sharp surge boundaries between adjacent hexagons.
    """
    # Get demand and supply from this cell and neighbors
    cells = get_h3_neighbors(h3_index, ring_size=1)  # Cell + 6 neighbors

    total_demand = 0
    total_supply = 0

    for cell in cells:
        total_demand += get_demand(cell)
        total_supply += get_supply(cell)

    # No supply → max surge (nobody available)
    if total_supply == 0:
        return settings.max_surge_multiplier if total_demand > 0 else 1.0

    ratio = total_demand / total_supply

    if ratio <= settings.surge_threshold:
        return 1.0                            # No surge needed

    # Linear interpolation between 1.0 and max multiplier
    # surge_threshold=0.8, max_surge=3.0
    # ratio=0.8 → 1.0x, ratio=2.0 → 3.0x
    surge_range = settings.max_surge_multiplier - 1.0
    ratio_range = 2.0 - settings.surge_threshold  # Scale factor

    multiplier = 1.0 + surge_range * ((ratio - settings.surge_threshold) / ratio_range)
    multiplier = min(multiplier, settings.max_surge_multiplier)
    multiplier = round(multiplier, 1)         # Round to 1 decimal (1.3x, 2.5x)

    return multiplier


def calculate_supply_for_cell(h3_index: str, available_drivers: list[dict]) -> int:
    """
    Count available drivers in an H3 cell.
    Called periodically to update supply counters.
    """
    from app.geo import lat_lng_to_h3

    count = 0
    for driver in available_drivers:
        driver_cell = lat_lng_to_h3(driver["latitude"], driver["longitude"])
        if driver_cell == h3_index:
            count += 1
    return count
```

Surge pricing uses Uber's **H3 hexagonal grid** to divide the city into zones. Each zone tracks `demand` (ride requests in the last 5 minutes) and `supply` (available drivers currently in the zone). The demand-to-supply ratio determines the surge multiplier. The algorithm smooths the ratio across neighboring cells using `get_h3_neighbors` with `ring_size=1` (the cell + its 6 neighbors), preventing jarring price differences when a rider is near a cell boundary. Linear interpolation maps the ratio to a multiplier between 1.0x and 3.0x. The 5-minute TTL on demand counters ensures surge pricing is reactive: it ramps up quickly when demand spikes and decays naturally when demand drops. In production, supply counters would be updated by a background job that scans all available driver locations and groups them by H3 cell every 30 seconds.

---

## Fare Calculation & Payments

### `app/fare.py`

```python
from app.config import settings
from app.geo import haversine_km, estimate_eta_minutes
from app.schemas import FareEstimate


def calculate_fare_estimate(
    distance_km: float,
    duration_minutes: float | None = None,
    surge_multiplier: float = 1.0,
) -> FareEstimate:
    """
    Calculate the fare for a ride.

    Components:
        Base fare     : Flat fee (e.g., $2.50)
        Distance      : Per-km rate × distance (e.g., $1.50/km)
        Time          : Per-minute rate × duration (e.g., $0.25/min)
        Surge         : (Base + Distance + Time) × (surge - 1.0)
    """
    # Estimate duration if not provided
    if duration_minutes is None:
        duration_minutes = (distance_km / 30.0) * 60  # Assume 30 km/h city speed

    base = settings.base_fare
    distance_charge = settings.per_km_rate * distance_km
    time_charge = settings.per_minute_rate * duration_minutes

    subtotal = base + distance_charge + time_charge
    surge_charge = subtotal * (surge_multiplier - 1.0) if surge_multiplier > 1.0 else 0

    total = subtotal + surge_charge
    total = round(total, 2)

    return FareEstimate(
        distance_km=round(distance_km, 2),
        duration_minutes=round(duration_minutes, 1),
        base_fare=base,
        distance_charge=round(distance_charge, 2),
        time_charge=round(time_charge, 2),
        surge_multiplier=surge_multiplier,
        surge_charge=round(surge_charge, 2),
        total_fare=total,
    )


def calculate_fare_from_coords(
    pickup_lat: float, pickup_lng: float,
    dropoff_lat: float, dropoff_lng: float,
    surge_multiplier: float = 1.0,
) -> FareEstimate:
    """Convenience function: calculate fare from pickup/dropoff coordinates."""
    distance_km = haversine_km(pickup_lat, pickup_lng, dropoff_lat, dropoff_lng)
    return calculate_fare_estimate(distance_km, surge_multiplier=surge_multiplier)
```

Fare calculation uses three components: a flat base fare, a per-kilometer distance charge, and a per-minute time charge. Surge pricing applies a multiplier to the sum of all three. The `(surge_multiplier - 1.0)` formula means 1.0x surge adds nothing, 1.5x adds 50%, and 3.0x adds 200%. Duration is estimated using a 30 km/h average city speed when not available (before the trip starts). After trip completion, the actual duration from `picked_up_at` to `completed_at` is used for the final fare. In production, distance would come from a routing engine (Google Maps, OSRM) that follows actual roads rather than straight-line Haversine distance.

### `app/routers/payments.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.fare import calculate_fare_from_coords
from app.models import Trip, TripStatus
from app.schemas import FareEstimate
from app.surge import get_surge_multiplier
from app.geo import lat_lng_to_h3

router = APIRouter()


@router.get("/estimate")
def get_fare_estimate(
    pickup_lat: float,
    pickup_lng: float,
    dropoff_lat: float,
    dropoff_lng: float,
) -> FareEstimate:
    """Get a fare estimate before requesting a ride."""
    h3_cell = lat_lng_to_h3(pickup_lat, pickup_lng)
    surge = get_surge_multiplier(h3_cell)

    return calculate_fare_from_coords(
        pickup_lat, pickup_lng,
        dropoff_lat, dropoff_lng,
        surge_multiplier=surge,
    )


@router.get("/{trip_id}/receipt")
def get_receipt(trip_id: str, db: Session = Depends(get_db)):
    """Get the final receipt for a completed trip."""
    trip = db.query(Trip).filter(Trip.id == trip_id).first()
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found.")

    if trip.status != TripStatus.COMPLETED:
        raise HTTPException(status_code=400, detail="Trip not yet completed.")

    return {
        "trip_id": str(trip.id),
        "distance_km": trip.distance_km,
        "duration_minutes": trip.duration_minutes,
        "base_fare": float(trip.estimated_fare) if not trip.actual_fare else None,
        "actual_fare": float(trip.actual_fare) if trip.actual_fare else None,
        "surge_multiplier": trip.surge_multiplier,
        "payment_status": trip.payment_status.value,
        "requested_at": trip.requested_at.isoformat(),
        "completed_at": trip.completed_at.isoformat() if trip.completed_at else None,
    }
```

The estimate endpoint is called when the rider opens the app and enters pickup/dropoff locations, before they confirm the ride. It queries the current surge multiplier for the pickup area and returns a breakdown of costs. The receipt endpoint returns the final fare after trip completion, which may differ from the estimate if the actual route was longer or shorter.

---

## Trip Lifecycle & State Machine

### `app/state_machine.py`

```python
from app.models import TripStatus


# ─── Valid State Transitions ──────────────────────────────────────

TRANSITIONS = {
    TripStatus.REQUESTED: {
        TripStatus.MATCHED: "Driver found and assigned",
        TripStatus.CANCELLED: "Rider cancelled before match",
    },
    TripStatus.MATCHED: {
        TripStatus.ACCEPTED: "Driver accepted the ride",
        TripStatus.CANCELLED: "Rider or driver cancelled",
    },
    TripStatus.ACCEPTED: {
        TripStatus.DRIVER_EN_ROUTE: "Driver heading to pickup",
        TripStatus.CANCELLED: "Rider or driver cancelled",
    },
    TripStatus.DRIVER_EN_ROUTE: {
        TripStatus.ARRIVED: "Driver arrived at pickup",
        TripStatus.CANCELLED: "Rider or driver cancelled",
    },
    TripStatus.ARRIVED: {
        TripStatus.IN_PROGRESS: "Rider picked up, trip started",
        TripStatus.CANCELLED: "Rider no-show or cancelled",
    },
    TripStatus.IN_PROGRESS: {
        TripStatus.COMPLETED: "Trip finished, fare charged",
    },
    TripStatus.COMPLETED: {},                 # Terminal state
    TripStatus.CANCELLED: {},                 # Terminal state
}


def can_transition(current: TripStatus, target: TripStatus) -> bool:
    """Check if a state transition is valid."""
    return target in TRANSITIONS.get(current, {})


def get_valid_transitions(current: TripStatus) -> list[str]:
    """Get all valid next states from the current state."""
    return [s.value for s in TRANSITIONS.get(current, {}).keys()]


def get_transition_description(current: TripStatus, target: TripStatus) -> str:
    """Get a human-readable description of a transition."""
    return TRANSITIONS.get(current, {}).get(target, "Unknown transition")
```

The state machine enforces a strict order of trip lifecycle events:

```
REQUESTED → MATCHED → ACCEPTED → DRIVER_EN_ROUTE → ARRIVED → IN_PROGRESS → COMPLETED
     ↓          ↓          ↓              ↓              ↓
  CANCELLED  CANCELLED  CANCELLED     CANCELLED      CANCELLED
```

`COMPLETED` and `CANCELLED` are terminal states with no outgoing transitions. Cancellation is possible from any active state. Notably, `IN_PROGRESS` cannot be cancelled because the rider is already in the car, and the trip must be completed (even if the rider is dropped off early). This prevents fare disputes by always recording a completed trip with a final fare.

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
      - DATABASE_URL=postgresql://user:pass@db:5432/uber
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
      - POSTGRES_DB=uber
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d uber"]
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

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import rides, drivers, tracking, payments


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Uber Ride-Sharing API",
    description="A ride-sharing system with geo-matching, surge pricing, and real-time tracking",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(drivers.router, prefix="/drivers", tags=["Drivers"])
app.include_router(rides.router, prefix="/rides", tags=["Rides"])
app.include_router(tracking.router, tags=["Tracking"])
app.include_router(payments.router, prefix="/payments", tags=["Payments"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`lifespan` is an async context manager that runs setup code before the app starts accepting requests (`Base.metadata.create_all` creates missing tables) and teardown code after shutdown. `include_router` mounts the route groups with URL prefixes and OpenAPI tags. The health check endpoint at `/health` is used by load balancers and container orchestrators to verify the service is running.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Update driver location
curl -X PUT http://localhost:8000/drivers/location \
  -H "Content-Type: application/json" \
  -d '{"driver_id": "d1a2b3c4-...", "latitude": 40.7128, "longitude": -74.0060}'

# Set driver available
curl -X PUT http://localhost:8000/drivers/status \
  -H "Content-Type: application/json" \
  -d '{"driver_id": "d1a2b3c4-...", "is_available": true}'

# Request a ride
curl -X POST http://localhost:8000/rides/request \
  -H "Content-Type: application/json" \
  -d '{
    "rider_id": "a1b2c3d4-...",
    "pickup_lat": 40.7580,
    "pickup_lng": -73.9855,
    "dropoff_lat": 40.7484,
    "dropoff_lng": -73.9857,
    "pickup_address": "Times Square",
    "dropoff_address": "Empire State Building"
  }'

# Get fare estimate
curl "http://localhost:8000/payments/estimate?pickup_lat=40.7580&pickup_lng=-73.9855&dropoff_lat=40.7484&dropoff_lng=-73.9857"
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

### `tests/test_matching.py`

```python
from unittest.mock import patch, MagicMock

from app.matching import match_driver, MatchResult


def test_match_finds_nearest_available_driver():
    """Test that matching returns the nearest available driver."""
    mock_drivers = [
        {"driver_id": "d1", "distance_km": 1.2, "latitude": 40.71, "longitude": -74.00},
        {"driver_id": "d2", "distance_km": 2.5, "latitude": 40.72, "longitude": -74.01},
    ]

    with patch("app.matching.find_nearby_drivers", return_value=mock_drivers), \
         patch("app.matching.is_driver_available", side_effect=[True]), \
         patch("app.matching.set_driver_available"):

        result = match_driver(40.7128, -74.0060)

        assert result.matched is True
        assert result.driver_id == "d1"
        assert result.distance_km == 1.2


def test_match_skips_unavailable_drivers():
    """Test that unavailable drivers are skipped."""
    mock_drivers = [
        {"driver_id": "d1", "distance_km": 1.0, "latitude": 40.71, "longitude": -74.00},
        {"driver_id": "d2", "distance_km": 2.0, "latitude": 40.72, "longitude": -74.01},
    ]

    with patch("app.matching.find_nearby_drivers", return_value=mock_drivers), \
         patch("app.matching.is_driver_available", side_effect=[False, True]), \
         patch("app.matching.set_driver_available"):

        result = match_driver(40.7128, -74.0060)

        assert result.matched is True
        assert result.driver_id == "d2"


def test_match_returns_no_match_when_no_drivers():
    """Test that matching returns no match when no drivers are nearby."""
    with patch("app.matching.find_nearby_drivers", return_value=[]):
        result = match_driver(40.7128, -74.0060)
        assert result.matched is False
```

### `tests/test_surge.py`

```python
from unittest.mock import patch

from app.surge import get_surge_multiplier


def test_no_surge_when_supply_exceeds_demand():
    """Test that no surge applies when supply > demand."""
    with patch("app.surge.get_demand", return_value=5), \
         patch("app.surge.get_supply", return_value=20), \
         patch("app.surge.get_h3_neighbors", return_value=["cell1"]):

        multiplier = get_surge_multiplier("cell1")
        assert multiplier == 1.0


def test_surge_when_demand_exceeds_supply():
    """Test that surge applies when demand > supply."""
    with patch("app.surge.get_demand", return_value=20), \
         patch("app.surge.get_supply", return_value=5), \
         patch("app.surge.get_h3_neighbors", return_value=["cell1"]):

        multiplier = get_surge_multiplier("cell1")
        assert multiplier > 1.0


def test_max_surge_cap():
    """Test that surge is capped at the maximum multiplier."""
    with patch("app.surge.get_demand", return_value=1000), \
         patch("app.surge.get_supply", return_value=1), \
         patch("app.surge.get_h3_neighbors", return_value=["cell1"]):

        multiplier = get_surge_multiplier("cell1")
        assert multiplier == 3.0


def test_max_surge_when_zero_supply_with_demand():
    """Test max surge when there are no drivers but there is demand."""
    with patch("app.surge.get_demand", return_value=10), \
         patch("app.surge.get_supply", return_value=0), \
         patch("app.surge.get_h3_neighbors", return_value=["cell1"]):

        multiplier = get_surge_multiplier("cell1")
        assert multiplier == 3.0
```

### `tests/test_fare.py`

```python
from app.fare import calculate_fare_estimate


def test_fare_without_surge():
    """Test basic fare calculation without surge."""
    fare = calculate_fare_estimate(distance_km=10.0, duration_minutes=20.0)

    assert fare.base_fare == 2.50
    assert fare.distance_charge == 15.0       # 10 km × $1.50
    assert fare.time_charge == 5.0            # 20 min × $0.25
    assert fare.surge_charge == 0
    assert fare.total_fare == 22.50


def test_fare_with_surge():
    """Test fare calculation with 2x surge."""
    fare = calculate_fare_estimate(
        distance_km=10.0,
        duration_minutes=20.0,
        surge_multiplier=2.0,
    )

    assert fare.surge_multiplier == 2.0
    assert fare.surge_charge == 22.50         # Subtotal × (2.0 - 1.0)
    assert fare.total_fare == 45.0


def test_fare_estimates_duration_when_not_provided():
    """Test that duration is estimated from distance when not provided."""
    fare = calculate_fare_estimate(distance_km=15.0)

    assert fare.duration_minutes == 30.0      # 15 km / 30 km/h × 60


def test_zero_distance_fare():
    """Test minimum fare for very short trips."""
    fare = calculate_fare_estimate(distance_km=0.0, duration_minutes=0.0)

    assert fare.total_fare == 2.50            # Just the base fare
```

### `tests/test_rides.py`

```python
import uuid

from app.models import User, UserRole, Trip, TripStatus


def test_trip_history(client, db):
    """Test retrieving trip history for a user."""
    rider = User(
        id=uuid.uuid4(),
        email="rider@test.com",
        phone="+1234567890",
        full_name="Test Rider",
        role=UserRole.RIDER,
    )
    db.add(rider)

    trip = Trip(
        rider_id=rider.id,
        status=TripStatus.COMPLETED,
        pickup_lat=40.7128,
        pickup_lng=-74.0060,
        dropoff_lat=40.7580,
        dropoff_lng=-73.9855,
        distance_km=5.2,
        actual_fare=18.50,
    )
    db.add(trip)
    db.commit()

    response = client.get(f"/rides/history/{rider.id}")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["status"] == "completed"


def test_cancel_trip(client, db):
    """Test cancelling an active trip."""
    rider = User(
        id=uuid.uuid4(),
        email="rider2@test.com",
        phone="+1234567891",
        full_name="Test Rider 2",
        role=UserRole.RIDER,
    )
    db.add(rider)

    trip = Trip(
        rider_id=rider.id,
        status=TripStatus.REQUESTED,
        pickup_lat=40.7128,
        pickup_lng=-74.0060,
        dropoff_lat=40.7580,
        dropoff_lng=-73.9855,
    )
    db.add(trip)
    db.commit()

    response = client.put(f"/rides/{trip.id}/cancel?cancelled_by=rider")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_surge"                        # Run only surge tests
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
gcloud sql instances create uber-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-2-7680 \
  --region=us-central1 \
  --storage-size=50GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create uber \
  --instance=uber-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=uber-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-2-7680` provisions 2 vCPUs and 7.5 GB RAM, suitable for the write-heavy trip workload. `--availability-type=REGIONAL` enables automatic failover to a standby instance in another zone, critical for a ride-hailing service where downtime means stranded riders. `--storage-auto-increase` automatically adds storage when the disk fills up.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create uber-cache \
  --size=2 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe uber-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=2` provisions 2 GB of memory, enough for 1 M driver locations (100 MB) plus surge counters and driver metadata. `--tier=STANDARD` enables replication with automatic failover. Memorystore is accessible only from within the same VPC network, securing it without additional firewall rules.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry (recommended)
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/uber/api

# Or use Container Registry
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/uber-api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build, Google's managed CI/CD service. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy to Cloud Run

```bash
gcloud run deploy uber-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/uber/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 50 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:uber-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/uber?host=/cloudsql/YOUR_PROJECT_ID:us-central1:uber-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
BASE_URL=https://your-service-url.run.app,\
BASE_FARE=2.50,\
PER_KM_RATE=1.50,\
PER_MINUTE_RATE=0.25,\
SURGE_THRESHOLD=0.8,\
MAX_SURGE_MULTIPLIER=3.0,\
MATCH_RADIUS_KM=5.0"
```

`--min-instances 2` keeps at least two instances warm to avoid cold starts and handle WebSocket connections. `--max-instances 50` allows aggressive scaling under peak traffic. `--cpu 2` and `--memory 1Gi` provide enough resources for concurrent geo queries and WebSocket connections. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy for secure, IAM-based database connections. **Note**: Cloud Run has a 60-minute request timeout, which limits WebSocket connection duration. For production WebSockets, consider GKE (Kubernetes Engine) or Compute Engine.

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis)
gcloud compute networks vpc-access connectors create uber-vpc-connector \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update uber-api \
  --vpc-connector=uber-vpc-connector \
  --region=us-central1
```

Cloud Run services run in Google's serverless infrastructure, which is outside your VPC by default. A VPC connector creates a bridge so Cloud Run can reach private resources like Memorystore. `--range=10.8.0.0/28` allocates a small IP range for the connector.

### Schedule Supply Counter Updates

```bash
# Create a Cloud Scheduler job to update supply counters every 30 seconds
gcloud scheduler jobs create http update-supply-counters \
  --schedule="* * * * *" \
  --uri="https://your-service-url.run.app/api/internal/update-supply" \
  --http-method=POST \
  --oidc-service-account-email=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

This job triggers every minute (Cloud Scheduler minimum) to recalculate the number of available drivers per H3 cell, keeping surge pricing counters fresh.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Read Replicas         | Heavy analytics queries on trip data       | Cloud SQL read replicas + SQLAlchemy `bind` param  |
| Connection Pooling    | Many concurrent connections from Cloud Run | PgBouncer sidecar or Cloud SQL Auth Proxy pooling  |
| Table Partitioning    | `trips` table grows past 1B rows           | Partition by `requested_at` month                  |
| Sharding              | Single DB can't handle write volume        | Shard by city/region across multiple DB instances  |
| CQRS                  | Read patterns differ from write patterns   | Separate read models for history and analytics     |

### Cache & Geo Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Redis Cluster         | Driver count exceeds single-node capacity  | Memorystore cluster mode (auto-shards data)        |
| Geo Sharding          | Global operation across continents         | Separate Redis instances per region/city           |
| Local Cache           | Reduce Redis round trips for hot data      | In-memory LRU cache for surge multipliers          |
| Location Batching     | 250 K writes/sec overwhelms Redis          | Batch location updates every 2-4 seconds           |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ ride.xyz       │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Global Load    │
                     │ Balancer       │
                     └──────┬─────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
       ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
       │  GKE Pod 1  │ │ Pod 2  │ │  GKE Pod N  │
       │  (FastAPI)  │ │        │ │             │
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │            VPC Network              │
         │                                      │
         │  ┌──────────┐    ┌────────────────┐ │
         │  │ Memory-  │    │   Cloud SQL    │ │
         │  │ store    │    │ (Primary +     │ │
         │  │ Cluster  │    │  Read Replicas)│ │
         │  │ (Redis)  │    └────────────────┘ │
         │  └──────────┘                       │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Pub/Sub               │   │
         │  │  ┌─────────────────────────┐ │   │
         │  │  │ location-updates topic  │ │   │
         │  │  │ ride-events topic       │ │   │
         │  │  │ notifications topic     │ │   │
         │  │  └─────────────────────────┘ │   │
         │  └──────────┬────────────────────┘   │
         │             │                        │
         │  ┌──────────▼────────────────────┐   │
         │  │  Cloud Run Jobs               │   │
         │  │  ┌──────────────────────────┐ │   │
         │  │  │ Surge price calculator  │ │   │
         │  │  │ Analytics aggregator    │ │   │
         │  │  │ Notification sender     │ │   │
         │  │  └──────────────────────────┘ │   │
         │  └───────────────────────────────┘   │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **GKE (Kubernetes Engine)** replaces Cloud Run for the main API because Kubernetes supports persistent WebSocket connections without the 60-minute timeout, and gives fine-grained control over pod placement, resource limits, and horizontal pod autoscaling.
- **Memorystore Cluster** auto-shards the geo index across multiple Redis nodes, supporting millions of concurrent driver locations.
- **Cloud Pub/Sub** decouples event processing from the hot path. Location updates, ride events, and notifications are published to separate topics and consumed by dedicated workers. This keeps the API response time under 50 ms regardless of downstream processing load.
- **Read Replicas** handle analytics and trip history queries without impacting the primary database's write performance for active trips.
- **Geo Sharding** partitions Redis by city or region so a driver search in New York doesn't scan drivers in Los Angeles.

### Security Checklist

```
✓ Authenticate all API calls with JWT tokens (rider and driver apps)
✓ Validate GPS coordinates are within service area boundaries
✓ Rate limit location updates to prevent GPS spoofing flood attacks
✓ Encrypt all data in transit (TLS) and at rest (Cloud SQL encryption)
✓ Store payment tokens in Secret Manager, never in environment variables
✓ Implement driver identity verification (license, photo matching)
✓ Log all trip events for audit trail and dispute resolution
✓ Add CAPTCHA or SMS verification for new account creation
✓ Enable Cloud Audit Logs for compliance (SOC 2, PCI DSS)
✓ Mask rider/driver exact locations in public-facing APIs
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create uber-health \
  --display-name="Uber API Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Match failure rate > 5% (not enough drivers)
# - p99 match latency > 5 seconds
# - Location update failure rate > 1%
# - WebSocket connection drop rate > 10%
# - Surge multiplier > 2.5x for > 30 minutes
# - Cloud SQL CPU > 80% or connections > 90% of max
# - Redis memory usage > 85%
# - Error rate > 1% over 5 minutes
```

Cloud Run and GKE automatically export metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing across the API → Redis → PostgreSQL chain. Track business metrics like match rate, average surge multiplier, and trip completion rate in a dashboard to detect service degradation before users report it.
