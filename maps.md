# Design Google Maps

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [Map Tiles & Rendering](#map-tiles--rendering)
6. [Geocoding & Reverse Geocoding](#geocoding--reverse-geocoding)
7. [Geo-Search (Places Nearby)](#geo-search-places-nearby)
8. [Routing & Navigation](#routing--navigation)
9. [ETA & Live Traffic](#eta--live-traffic)
10. [Core API – Maps, Places & Directions](#core-api--maps-places--directions)
11. [Real-Time Location Sharing (WebSockets)](#real-time-location-sharing-websockets)
12. [Docker & Docker Compose](#docker--docker-compose)
13. [Testing](#testing)
14. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
15. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing a single line of code, define what the system needs to do and estimate the scale it must handle.

### Functional Requirements

| Requirement             | Description                                                             |
| ----------------------- | ----------------------------------------------------------------------- |
| Display Map Tiles       | Render map images at various zoom levels (1–20) for a given viewport    |
| Search Places           | Find places by name or category (restaurants, gas stations, parks)      |
| Geocode / Reverse       | Convert addresses to lat/lng and lat/lng back to addresses              |
| Nearby Search           | Find points of interest within a radius of a location                   |
| Route Planning          | Compute shortest/fastest path between an origin and a destination       |
| Turn-by-Turn Navigation | Provide step-by-step driving directions with maneuver instructions      |
| ETA Estimation          | Estimate arrival time considering distance and simulated traffic        |
| Live Location Sharing   | Share real-time GPS position with other users over WebSockets           |
| Place Details           | View details: name, address, rating, opening hours, contact info        |
| Search History          | Store and retrieve past searches per user                               |

### Non-Functional Requirements

| Requirement      | Target                                                    |
| ---------------- | --------------------------------------------------------- |
| Availability     | 99.99% uptime (navigation is safety-critical)             |
| Latency          | Tile fetch < 100 ms, route query < 500 ms, search < 200 ms |
| Scalability      | 1 B tile requests/day, 100 M route queries/day            |
| Consistency      | Place data is eventually consistent (not financial)       |
| Durability       | No user search or saved place should be lost              |

### Back-of-the-Envelope Estimations

```
Tile requests per day           ≈ 1 B
Tile requests per second        ≈ 1 B / 86400 ≈ 11 500 RPS (avg), ~50 000 RPS (peak)
Route queries per day           ≈ 100 M
Route queries per second        ≈ 100 M / 86400 ≈ 1 150 RPS (avg), ~5 000 RPS (peak)
Place search queries/sec        ≈ 3 000 RPS (peak)
Tile storage (all zoom levels)  ≈ 50 TB (pre-rendered raster tiles worldwide)
Place records                   ≈ 200 M POIs
Place record size               ≈ 1 KB each → 200 GB total
Road graph (nodes + edges)      ≈ 1 B nodes, 2 B edges → ~60 GB in memory
User search history             ≈ 500 M users × 20 searches avg × 200 B ≈ 2 TB
```

Tiles are static pre-rendered images served from a CDN and cached aggressively (they change infrequently). Routing is CPU-intensive, requiring the full road graph in memory, handled by a dedicated routing engine like OSRM (Open Source Routing Machine). Place search uses PostGIS spatial indexes for proximity queries. Redis caches hot tiles, popular routes, and geocoding results.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │(Web/Mobile)  │
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ Cloud CDN    │
                        │ (Map Tiles)  │
                        └──────┬───────┘
                               │ cache miss
                        ┌──────▼───────┐
                        │ Load Balancer│
                        │  (GCP LB)    │
                        └──────┬───────┘
                               │
                ┌──────────────▼──────────────┐
                │        API Servers          │
                │   (FastAPI on Cloud Run)    │
                │                             │
                │  GET  /tiles/{z}/{x}/{y}    │
                │  GET  /places/search        │
                │  GET  /directions           │
                │  GET  /geocode              │
                │  WS   /ws/share/{session}   │
                └──┬──────────┬───────────┬───┘
                   │          │           │
            ┌──────▼───┐ ┌───▼────┐ ┌────▼──────┐
            │  Redis   │ │Postgre-│ │   OSRM    │
            │ (Tile +  │ │SQL +   │ │ (Routing  │
            │  Cache)  │ │PostGIS │ │  Engine)  │
            └──────────┘ └────────┘ └───────────┘
```

The client loads map tiles from Cloud CDN. On cache miss, tiles are generated or fetched from the tile server. Redis caches hot tiles, geocoding results, and popular route responses. PostgreSQL with PostGIS stores place data and supports spatial queries (nearby search, bounding-box queries). OSRM handles route computation on the full road graph, running as a separate container with the road network loaded in memory. WebSockets deliver real-time location sharing between users.

---

## Project Setup

### Project Structure

```
maps/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── cache.py              # Redis client and caching layer
│   ├── tiles.py              # Map tile serving and generation
│   ├── geocoding.py          # Geocoding and reverse geocoding
│   ├── search.py             # Place search with spatial queries
│   ├── routing.py            # Route computation via OSRM
│   ├── traffic.py            # ETA and traffic simulation
│   ├── websocket_manager.py  # WebSocket connection manager
│   └── routers/
│       ├── __init__.py
│       ├── tiles.py          # Tile serving endpoints
│       ├── places.py         # Place search and detail endpoints
│       ├── directions.py     # Routing and navigation endpoints
│       ├── geocode.py        # Geocoding endpoints
│       └── sharing.py        # Live location sharing WebSocket
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_geocoding.py     # Tests for geocoding
│   ├── test_search.py        # Tests for place search
│   ├── test_routing.py       # Tests for routing
│   └── test_tiles.py         # Tests for tile serving
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir maps && cd maps
python -m venv venv
source venv/bin/activate              # macOS/Linux
```

### `requirements.txt`

```text
fastapi[standard]==0.115.0
sqlalchemy==2.0.35
psycopg2-binary==2.9.10
alembic==1.13.3
geoalchemy2==0.15.2
redis==5.1.1
python-dotenv==1.0.1
httpx==0.27.2
geopy==2.4.1
Pillow==11.0.0
websockets==13.1
pytest==8.3.3
pytest-asyncio==0.24.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `geoalchemy2` adds PostGIS spatial types and functions to SQLAlchemy, enabling geometry columns and spatial queries like `ST_DWithin` and `ST_Distance`. `redis` is the Python client for caching tiles and geocoding results. `geopy` calculates distances between GPS coordinates using the Haversine formula. `Pillow` handles image processing for tile generation. `httpx` is the async HTTP client for calling the OSRM routing engine.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/maps
REDIS_URL=redis://localhost:6379/0
BASE_URL=http://localhost:8000
OSRM_URL=http://localhost:5000
TILE_CACHE_TTL=86400
GEOCODE_CACHE_TTL=3600
ROUTE_CACHE_TTL=300
SEARCH_RADIUS_KM=5.0
MAX_SEARCH_RADIUS_KM=50.0
MAX_SEARCH_RESULTS=50
DEFAULT_ZOOM=15
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance. `OSRM_URL` points to the OSRM routing engine running in a separate container. `TILE_CACHE_TTL` is 24 hours because map tiles rarely change. `GEOCODE_CACHE_TTL` is 1 hour for geocoding results. `ROUTE_CACHE_TTL` is 5 minutes for route responses since traffic conditions change. `SEARCH_RADIUS_KM` is the default nearby search radius. `MAX_SEARCH_RESULTS` caps results to prevent expensive queries.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│              places                   │     │          place_categories            │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            UUID       PRIMARY KEY │     │ id            SERIAL     PRIMARY KEY │
│ name          VARCHAR    NOT NULL    │     │ name          VARCHAR    UNIQUE      │
│ address       TEXT                   │     │ slug          VARCHAR    UNIQUE      │
│ latitude      FLOAT      NOT NULL   │     │ icon          VARCHAR                │
│ longitude     FLOAT      NOT NULL   │     └──────────────────────────────────────┘
│ location      GEOGRAPHY  (PostGIS)  │
│ category_id   INTEGER    FK → cats  │     ┌──────────────────────────────────────┐
│ rating        FLOAT      DEFAULT 0  │     │          search_history              │
│ review_count  INTEGER    DEFAULT 0  │     ├──────────────────────────────────────┤
│ phone         VARCHAR               │     │ id            BIGSERIAL  PRIMARY KEY │
│ website       VARCHAR               │     │ user_id       UUID       NOT NULL    │
│ opening_hours JSONB                 │     │ query         VARCHAR    NOT NULL    │
│ price_level   INTEGER               │     │ latitude      FLOAT                  │
│ is_active     BOOLEAN    DEFAULT T  │     │ longitude     FLOAT                  │
│ created_at    TIMESTAMPTZ           │     │ result_type   VARCHAR                │
│ updated_at    TIMESTAMPTZ           │     │ result_id     UUID                   │
├──────────────────────────────────────┤     │ searched_at   TIMESTAMPTZ            │
│ INDEX idx_location GIST (location)  │     ├──────────────────────────────────────┤
│ INDEX idx_category (category_id)    │     │ INDEX idx_user_search (user_id)      │
│ INDEX idx_name_trgm GIN (name)      │     │ INDEX idx_searched_at (searched_at)  │
└──────────────────────────────────────┘     └──────────────────────────────────────┘

┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│            saved_places              │     │          road_segments               │
├──────────────────────────────────────┤     ├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY │     │ id            BIGSERIAL  PRIMARY KEY │
│ user_id       UUID       NOT NULL   │     │ osm_id        BIGINT     UNIQUE      │
│ place_id      UUID       FK → places│     │ name          VARCHAR                │
│ label         VARCHAR               │     │ road_class    VARCHAR                │
│ saved_at      TIMESTAMPTZ           │     │ speed_limit   INTEGER                │
├──────────────────────────────────────┤     │ geometry      GEOGRAPHY  (LineString)│
│ UNIQUE (user_id, place_id)          │     │ traffic_speed FLOAT                  │
│ INDEX idx_user_saved (user_id)      │     │ updated_at    TIMESTAMPTZ            │
└──────────────────────────────────────┘     ├──────────────────────────────────────┤
                                             │ INDEX idx_road_geom GIST (geometry)  │
                                             │ INDEX idx_road_class (road_class)    │
                                             └──────────────────────────────────────┘
```

The `places` table stores points of interest with a PostGIS `GEOGRAPHY` column for spatial queries. The `GIST` index on `location` enables fast proximity searches using `ST_DWithin`. A trigram GIN index on `name` supports fuzzy text search (`LIKE '%cafe%'` becomes index-accelerated). `road_segments` stores road network metadata with `LineString` geometries for traffic overlay rendering. `search_history` records user queries for personalization and analytics. `saved_places` lets users bookmark locations. Actual routing is handled by OSRM with its own in-memory graph, not by querying PostgreSQL for road data.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/maps"
    redis_url: str = "redis://localhost:6379/0"
    base_url: str = "http://localhost:8000"
    osrm_url: str = "http://localhost:5000"
    tile_cache_ttl: int = 86400
    geocode_cache_ttl: int = 3600
    route_cache_ttl: int = 300
    search_radius_km: float = 5.0
    max_search_radius_km: float = 50.0
    max_search_results: int = 50
    default_zoom: int = 15

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
import uuid
from datetime import datetime, timezone

from geoalchemy2 import Geography
from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Float,
    ForeignKey, Index, Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database import Base


class PlaceCategory(Base):
    __tablename__ = "place_categories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    slug = Column(String(100), unique=True, nullable=False)
    icon = Column(String(50), nullable=True)

    places = relationship("Place", back_populates="category")


class Place(Base):
    __tablename__ = "places"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    address = Column(Text, nullable=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    location = Column(
        Geography(geometry_type="POINT", srid=4326),
        nullable=False,
    )
    category_id = Column(Integer, ForeignKey("place_categories.id"), nullable=True)
    rating = Column(Float, default=0.0)
    review_count = Column(Integer, default=0)
    phone = Column(String(20), nullable=True)
    website = Column(String(255), nullable=True)
    opening_hours = Column(JSONB, nullable=True)
    price_level = Column(Integer, nullable=True)
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

    category = relationship("PlaceCategory", back_populates="places")
    saved_by = relationship("SavedPlace", back_populates="place")

    __table_args__ = (
        Index("idx_location", "location", postgresql_using="gist"),
        Index("idx_category", "category_id"),
    )


class SavedPlace(Base):
    __tablename__ = "saved_places"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    place_id = Column(UUID(as_uuid=True), ForeignKey("places.id"), nullable=False)
    label = Column(String(100), nullable=True)
    saved_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    place = relationship("Place", back_populates="saved_by")

    __table_args__ = (
        UniqueConstraint("user_id", "place_id", name="uq_user_place"),
        Index("idx_user_saved", "user_id"),
    )


class SearchHistory(Base):
    __tablename__ = "search_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    query = Column(String(255), nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    result_type = Column(String(50), nullable=True)
    result_id = Column(UUID(as_uuid=True), nullable=True)
    searched_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("idx_user_search", "user_id"),
        Index("idx_searched_at", "searched_at"),
    )


class RoadSegment(Base):
    __tablename__ = "road_segments"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    osm_id = Column(BigInteger, unique=True, nullable=True)
    name = Column(String(255), nullable=True)
    road_class = Column(String(50), nullable=True)
    speed_limit = Column(Integer, nullable=True)
    geometry = Column(
        Geography(geometry_type="LINESTRING", srid=4326),
        nullable=False,
    )
    traffic_speed = Column(Float, nullable=True)
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        Index("idx_road_geom", "geometry", postgresql_using="gist"),
        Index("idx_road_class", "road_class"),
    )
```

`Geography(geometry_type="POINT", srid=4326)` creates a PostGIS geography column storing latitude/longitude on the WGS84 ellipsoid. Unlike the `Geometry` type, `Geography` calculates distances in meters on the Earth's surface, not on a flat plane. `srid=4326` is the standard GPS coordinate system. The `GIST` index on `location` uses a generalized search tree optimized for spatial queries like "find all points within 5 km". `JSONB` for `opening_hours` allows flexible schema-free data like `{"monday": "09:00-21:00", "tuesday": "09:00-21:00"}`. `RoadSegment` with `LINESTRING` geometry stores road shapes for traffic overlay rendering on the map.

### `app/schemas.py`

```python
import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class PlaceCreate(BaseModel):
    name: str
    address: str | None = None
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    category_slug: str | None = None
    phone: str | None = None
    website: str | None = None
    opening_hours: dict | None = None
    price_level: int | None = Field(None, ge=1, le=4)


class PlaceResponse(BaseModel):
    id: uuid.UUID
    name: str
    address: str | None
    latitude: float
    longitude: float
    category: str | None = None
    rating: float
    review_count: int
    phone: str | None
    website: str | None
    opening_hours: dict | None
    price_level: int | None
    distance_km: float | None = None

    class Config:
        from_attributes = True


class PlaceSearchQuery(BaseModel):
    query: str | None = None
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    radius_km: float = Field(5.0, gt=0, le=50)
    category: str | None = None
    limit: int = Field(20, ge=1, le=50)


class GeocodeResult(BaseModel):
    address: str
    latitude: float
    longitude: float
    place_id: uuid.UUID | None = None
    confidence: float = Field(ge=0, le=1.0)


class ReverseGeocodeResult(BaseModel):
    address: str
    latitude: float
    longitude: float
    place_name: str | None = None
    road_name: str | None = None
    distance_m: float


class RouteRequest(BaseModel):
    origin_lat: float = Field(ge=-90, le=90)
    origin_lng: float = Field(ge=-180, le=180)
    dest_lat: float = Field(ge=-90, le=90)
    dest_lng: float = Field(ge=-180, le=180)
    mode: str = Field("driving", pattern="^(driving|walking|cycling)$")
    alternatives: bool = False


class RouteStep(BaseModel):
    instruction: str
    distance_m: float
    duration_s: float
    start_lat: float
    start_lng: float
    end_lat: float
    end_lng: float
    maneuver: str | None = None


class RouteResponse(BaseModel):
    distance_km: float
    duration_minutes: float
    geometry: list[list[float]]
    steps: list[RouteStep]
    summary: str
    traffic_delay_minutes: float = 0.0


class DirectionsResponse(BaseModel):
    routes: list[RouteResponse]
    origin: str
    destination: str


class TileRequest(BaseModel):
    z: int = Field(ge=0, le=20)
    x: int = Field(ge=0)
    y: int = Field(ge=0)


class SearchHistoryEntry(BaseModel):
    query: str
    latitude: float | None
    longitude: float | None
    result_type: str | None
    searched_at: datetime

    class Config:
        from_attributes = True


class LocationUpdate(BaseModel):
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    heading: float | None = Field(None, ge=0, le=360)
    speed_kmh: float | None = Field(None, ge=0)
```

`Field(ge=-90, le=90)` validates latitude is within the valid range. `Field(ge=-180, le=180)` does the same for longitude. This catches invalid coordinates at the schema layer before they reach any business logic. `RouteStep` models a single maneuver instruction (e.g., "Turn left onto Main Street") with distances and coordinates. `RouteResponse` includes the polyline geometry as a list of `[lng, lat]` pairs for rendering on the map. The `mode` field uses a regex pattern to restrict travel modes to driving, walking, or cycling.

---

## Map Tiles & Rendering

The map is divided into a grid of square tiles at each zoom level. At zoom level 0, the entire world fits in one 256×256 pixel tile. Each zoom level doubles the number of tiles in each dimension: zoom 1 has 4 tiles, zoom 2 has 16, up to zoom 20 with over 1 trillion tiles. Tiles are addressed by `(z, x, y)` coordinates following the Slippy Map standard.

### Tile Math

```
Zoom Level    Tiles          Coverage per Tile
    0           1             Entire world
    1           4             Hemisphere
   10       1 048 576         ~city block area
   15     1 073 741 824       ~individual buildings
   20   1 099 511 627 776     ~sub-meter detail
```

### `app/tiles.py`

```python
import hashlib
import math

from app.cache import get_cached_tile, cache_tile
from app.config import settings


# ─── Tile Coordinate Math ─────────────────────────────────────────

def lng_to_tile_x(lng: float, zoom: int) -> int:
    """Convert longitude to tile X coordinate."""
    return int((lng + 180.0) / 360.0 * (1 << zoom))


def lat_to_tile_y(lat: float, zoom: int) -> int:
    """Convert latitude to tile Y coordinate."""
    lat_rad = math.radians(lat)
    n = 1 << zoom
    return int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)


def tile_to_lng(x: int, zoom: int) -> float:
    """Convert tile X coordinate back to longitude."""
    return x / (1 << zoom) * 360.0 - 180.0


def tile_to_lat(y: int, zoom: int) -> float:
    """Convert tile Y coordinate back to latitude."""
    n = math.pi - 2.0 * math.pi * y / (1 << zoom)
    return math.degrees(math.atan(math.sinh(n)))


def tile_bounds(z: int, x: int, y: int) -> dict:
    """Get the geographic bounding box of a tile."""
    return {
        "north": tile_to_lat(y, z),
        "south": tile_to_lat(y + 1, z),
        "west": tile_to_lng(x, z),
        "east": tile_to_lng(x + 1, z),
    }


# ─── Tile Validation ──────────────────────────────────────────────

def is_valid_tile(z: int, x: int, y: int) -> bool:
    """Check if tile coordinates are within valid range."""
    max_tile = (1 << z) - 1
    return 0 <= x <= max_tile and 0 <= y <= max_tile


# ─── Tile Cache Key ───────────────────────────────────────────────

def tile_cache_key(z: int, x: int, y: int, style: str = "default") -> str:
    """Generate a unique cache key for a tile."""
    return f"tile:{style}:{z}:{x}:{y}"


# ─── Tile Serving ─────────────────────────────────────────────────

def get_tile(z: int, x: int, y: int, style: str = "default") -> bytes | None:
    """
    Get a map tile, checking cache first.

    Pipeline:
    1. Check Redis cache → return if hit
    2. Generate a placeholder tile (in production: fetch from tile renderer)
    3. Cache the result with TTL
    4. Return tile bytes
    """
    if not is_valid_tile(z, x, y):
        return None

    cache_key = tile_cache_key(z, x, y, style)

    # 1. Check cache
    cached = get_cached_tile(cache_key)
    if cached:
        return cached

    # 2. Generate tile (placeholder — production uses a tile renderer)
    tile_bytes = generate_placeholder_tile(z, x, y)

    # 3. Cache the tile
    cache_tile(cache_key, tile_bytes, ttl=settings.tile_cache_ttl)

    return tile_bytes


def generate_placeholder_tile(z: int, x: int, y: int) -> bytes:
    """
    Generate a placeholder tile with coordinates.
    In production, this would call a tile renderer like Mapnik or
    serve pre-rendered tiles from cloud storage.
    """
    from PIL import Image, ImageDraw, ImageFont
    import io

    img = Image.new("RGB", (256, 256), color=(240, 240, 240))
    draw = ImageDraw.Draw(img)

    # Draw grid border
    draw.rectangle([0, 0, 255, 255], outline=(200, 200, 200), width=1)

    # Draw tile coordinates
    text = f"z={z}\nx={x}\ny={y}"
    draw.text((10, 10), text, fill=(100, 100, 100))

    # Draw bounding box info
    bounds = tile_bounds(z, x, y)
    bbox_text = (
        f"N:{bounds['north']:.4f}\n"
        f"S:{bounds['south']:.4f}\n"
        f"W:{bounds['west']:.4f}\n"
        f"E:{bounds['east']:.4f}"
    )
    draw.text((10, 100), bbox_text, fill=(150, 150, 150))

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()


# ─── Viewport to Tiles ────────────────────────────────────────────

def get_tiles_for_viewport(
    north: float,
    south: float,
    west: float,
    east: float,
    zoom: int,
) -> list[dict]:
    """
    Calculate all tile coordinates needed to cover a viewport.
    The client calls this to know which tiles to request.
    """
    x_min = lng_to_tile_x(west, zoom)
    x_max = lng_to_tile_x(east, zoom)
    y_min = lat_to_tile_y(north, zoom)   # Note: y increases southward
    y_max = lat_to_tile_y(south, zoom)

    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            if is_valid_tile(zoom, x, y):
                tiles.append({"z": zoom, "x": x, "y": y})

    return tiles
```

The **Slippy Map** tile system uses a Mercator projection. `lng_to_tile_x` is a simple linear mapping from longitude [-180, 180] to tile column [0, 2^zoom). `lat_to_tile_y` uses the Mercator formula involving `log(tan + sec)` because the Mercator projection stretches latitudes non-linearly to preserve angles. At zoom level 15, a single tile covers roughly a city block (~4.8 km²). The `get_tiles_for_viewport` function determines which tiles the client needs based on the visible map area. In production, tiles would be pre-rendered using Mapnik from OpenStreetMap data and stored in Cloud Storage, with Cloud CDN serving them globally. The placeholder generator creates simple grid tiles with coordinate labels for development.

---

## Geocoding & Reverse Geocoding

Geocoding converts a human-readable address ("1600 Amphitheatre Parkway") to GPS coordinates (37.4224, -122.0842). Reverse geocoding does the opposite: given coordinates, return the nearest address.

### `app/geocoding.py`

```python
import hashlib
import json

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.cache import redis_client
from app.config import settings
from app.models import Place, RoadSegment
from app.schemas import GeocodeResult, ReverseGeocodeResult


# ─── Geocoding (Address → Coordinates) ────────────────────────────

def geocode(query: str, db: Session) -> list[GeocodeResult]:
    """
    Convert an address or place name to coordinates.

    Strategy:
    1. Check Redis cache
    2. Search places by name (trigram fuzzy match)
    3. Search places by address (substring match)
    4. Cache and return results
    """
    cache_key = f"geocode:{hashlib.md5(query.lower().encode()).hexdigest()}"

    # 1. Check cache
    cached = redis_client.get(cache_key)
    if cached:
        return [GeocodeResult(**r) for r in json.loads(cached)]

    results = []

    # 2. Fuzzy name search using trigram similarity
    name_matches = (
        db.query(Place)
        .filter(Place.is_active.is_(True))
        .filter(func.lower(Place.name).contains(query.lower()))
        .order_by(Place.review_count.desc())
        .limit(5)
        .all()
    )

    for place in name_matches:
        similarity = 1.0 if place.name.lower() == query.lower() else 0.7
        results.append(GeocodeResult(
            address=place.address or place.name,
            latitude=place.latitude,
            longitude=place.longitude,
            place_id=place.id,
            confidence=similarity,
        ))

    # 3. Address substring search
    if not results:
        addr_matches = (
            db.query(Place)
            .filter(Place.is_active.is_(True))
            .filter(func.lower(Place.address).contains(query.lower()))
            .order_by(Place.review_count.desc())
            .limit(5)
            .all()
        )

        for place in addr_matches:
            results.append(GeocodeResult(
                address=place.address or place.name,
                latitude=place.latitude,
                longitude=place.longitude,
                place_id=place.id,
                confidence=0.5,
            ))

    # 4. Cache results
    if results:
        redis_client.setex(
            cache_key,
            settings.geocode_cache_ttl,
            json.dumps([r.model_dump(mode="json") for r in results]),
        )

    return results


# ─── Reverse Geocoding (Coordinates → Address) ────────────────────

def reverse_geocode(
    latitude: float,
    longitude: float,
    db: Session,
) -> ReverseGeocodeResult:
    """
    Convert GPS coordinates to the nearest address.

    Strategy:
    1. Check Redis cache
    2. Find the nearest place within 500 m
    3. Find the nearest road segment within 200 m
    4. Combine results for the best address
    """
    cache_key = f"revgeo:{latitude:.5f}:{longitude:.5f}"

    # 1. Check cache
    cached = redis_client.get(cache_key)
    if cached:
        return ReverseGeocodeResult(**json.loads(cached))

    point = func.ST_GeogFromText(f"POINT({longitude} {latitude})")

    # 2. Find nearest place
    nearest_place = (
        db.query(
            Place,
            func.ST_Distance(Place.location, point).label("distance"),
        )
        .filter(Place.is_active.is_(True))
        .filter(func.ST_DWithin(Place.location, point, 500))  # Within 500 m
        .order_by(text("distance"))
        .first()
    )

    # 3. Find nearest road
    nearest_road = (
        db.query(
            RoadSegment,
            func.ST_Distance(RoadSegment.geometry, point).label("distance"),
        )
        .filter(func.ST_DWithin(RoadSegment.geometry, point, 200))
        .order_by(text("distance"))
        .first()
    )

    # 4. Build result
    place_name = nearest_place[0].name if nearest_place else None
    place_address = nearest_place[0].address if nearest_place else None
    road_name = nearest_road[0].name if nearest_road else None
    distance_m = float(nearest_place[1]) if nearest_place else 0

    address = place_address or ""
    if not address and road_name:
        address = f"Near {road_name}"
    if not address and place_name:
        address = f"Near {place_name}"
    if not address:
        address = f"{latitude:.6f}, {longitude:.6f}"

    result = ReverseGeocodeResult(
        address=address,
        latitude=latitude,
        longitude=longitude,
        place_name=place_name,
        road_name=road_name,
        distance_m=distance_m,
    )

    # Cache result
    redis_client.setex(
        cache_key,
        settings.geocode_cache_ttl,
        json.dumps(result.model_dump()),
    )

    return result
```

Geocoding first checks Redis for cached results. The cache key is an MD5 hash of the normalized query to handle case differences. Fuzzy matching uses PostgreSQL's `LIKE` with `contains()` which, combined with a trigram GIN index, efficiently matches partial names. Results are ranked by `review_count` so popular places appear first. Reverse geocoding uses PostGIS `ST_DWithin` with a 500-meter radius for places and 200 meters for roads. `ST_DWithin` on geography columns uses a spatial index and calculates distance on the Earth's surface in meters, unlike `ST_Distance` on geometry which uses planar coordinates. The `POINT(longitude latitude)` format follows WKT (Well-Known Text) standard where longitude comes before latitude.

---

## Geo-Search (Places Nearby)

### `app/search.py`

```python
from sqlalchemy import func, text, and_
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Place, PlaceCategory
from app.schemas import PlaceResponse


def search_places(
    latitude: float,
    longitude: float,
    radius_km: float = 5.0,
    query: str | None = None,
    category: str | None = None,
    limit: int = 20,
    db: Session = None,
) -> list[PlaceResponse]:
    """
    Search for places near a location.

    Strategy:
    1. Use PostGIS ST_DWithin for spatial filtering (index-accelerated)
    2. Apply text and category filters
    3. Calculate distance and sort by it
    4. Return with distance field populated
    """
    radius_m = min(radius_km, settings.max_search_radius_km) * 1000
    limit = min(limit, settings.max_search_results)

    point = func.ST_GeogFromText(f"POINT({longitude} {latitude})")

    # Base query: spatial filter
    q = (
        db.query(
            Place,
            func.ST_Distance(Place.location, point).label("distance_m"),
        )
        .filter(Place.is_active.is_(True))
        .filter(func.ST_DWithin(Place.location, point, radius_m))
    )

    # Text filter (fuzzy name search)
    if query:
        q = q.filter(func.lower(Place.name).contains(query.lower()))

    # Category filter
    if category:
        q = q.join(PlaceCategory).filter(PlaceCategory.slug == category)

    # Sort by distance, then by rating for equal distances
    q = q.order_by(text("distance_m"), Place.rating.desc())
    q = q.limit(limit)

    results = q.all()

    places = []
    for place, distance_m in results:
        cat_name = place.category.name if place.category else None
        places.append(PlaceResponse(
            id=place.id,
            name=place.name,
            address=place.address,
            latitude=place.latitude,
            longitude=place.longitude,
            category=cat_name,
            rating=place.rating,
            review_count=place.review_count,
            phone=place.phone,
            website=place.website,
            opening_hours=place.opening_hours,
            price_level=place.price_level,
            distance_km=round(distance_m / 1000, 2),
        ))

    return places


def search_places_in_bounds(
    north: float,
    south: float,
    west: float,
    east: float,
    category: str | None = None,
    limit: int = 50,
    db: Session = None,
) -> list[PlaceResponse]:
    """
    Search for places within a bounding box (viewport).
    Used when the user pans/zooms the map to load visible POIs.
    """
    envelope = func.ST_MakeEnvelope(west, south, east, north, 4326)

    q = (
        db.query(Place)
        .filter(Place.is_active.is_(True))
        .filter(func.ST_Within(
            func.ST_GeomFromWKB(func.ST_AsBinary(Place.location)),
            envelope,
        ))
    )

    if category:
        q = q.join(PlaceCategory).filter(PlaceCategory.slug == category)

    q = q.order_by(Place.review_count.desc()).limit(limit)
    results = q.all()

    places = []
    for place in results:
        cat_name = place.category.name if place.category else None
        places.append(PlaceResponse(
            id=place.id,
            name=place.name,
            address=place.address,
            latitude=place.latitude,
            longitude=place.longitude,
            category=cat_name,
            rating=place.rating,
            review_count=place.review_count,
            phone=place.phone,
            website=place.website,
            opening_hours=place.opening_hours,
            price_level=place.price_level,
            distance_km=None,
        ))

    return places
```

`ST_DWithin` is the workhorse of nearby search. On geography columns, it checks whether two geometries are within a given distance in meters, and it uses the GIST index, making it O(log N) instead of scanning every row. `ST_Distance` calculates the exact distance for sorting. The query first filters spatially (fast, index-accelerated), then applies text/category filters (further narrowing), then sorts by distance. `search_places_in_bounds` uses `ST_MakeEnvelope` to create a rectangular bounding box for viewport-based queries. This is used when the user pans the map: the client sends the viewport corners and receives all visible POIs. Results are ordered by `review_count` so popular places render first if the viewport contains many POIs.

---

## Routing & Navigation

Routing computes the shortest or fastest path between two points on the road network. We delegate this to OSRM (Open Source Routing Machine), which loads the entire road graph into memory and uses the Contraction Hierarchies algorithm for sub-second queries even on continental-scale graphs.

### OSRM Overview

```
Road Network (OpenStreetMap .pbf file)
         │
         ▼
┌─────────────────────┐
│  osrm-extract       │  Parse road graph from OSM data
│  osrm-partition     │  Partition graph for multi-level Dijkstra
│  osrm-customize     │  Compute edge weights (speed, distance)
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│  osrm-routed        │  HTTP API serving route queries
│  (Container)        │  GET /route/v1/driving/{lng1},{lat1};{lng2},{lat2}
└─────────────────────┘
```

### `app/routing.py`

```python
import hashlib
import json

import httpx

from app.cache import redis_client
from app.config import settings
from app.schemas import RouteResponse, RouteStep


# ─── OSRM Client ──────────────────────────────────────────────────

async def get_route(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    mode: str = "driving",
    alternatives: bool = False,
) -> list[RouteResponse]:
    """
    Get route(s) from OSRM.

    OSRM API format:
    GET /route/v1/{profile}/{lng1},{lat1};{lng2},{lat2}
        ?overview=full
        &geometries=geojson
        &steps=true
        &alternatives=true

    Note: OSRM uses lng,lat order (not lat,lng).
    """
    # Check cache
    cache_key = _route_cache_key(
        origin_lat, origin_lng, dest_lat, dest_lng, mode
    )
    cached = redis_client.get(cache_key)
    if cached:
        return [RouteResponse(**r) for r in json.loads(cached)]

    # Build OSRM request
    profile = _mode_to_profile(mode)
    coords = f"{origin_lng},{origin_lat};{dest_lng},{dest_lat}"

    url = (
        f"{settings.osrm_url}/route/v1/{profile}/{coords}"
        f"?overview=full&geometries=geojson&steps=true"
        f"&alternatives={'true' if alternatives else 'false'}"
    )

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()

    if data.get("code") != "Ok":
        return []

    routes = []
    for osrm_route in data["routes"]:
        steps = _parse_steps(osrm_route.get("legs", []))
        geometry = osrm_route["geometry"]["coordinates"]  # [[lng, lat], ...]

        route = RouteResponse(
            distance_km=round(osrm_route["distance"] / 1000, 2),
            duration_minutes=round(osrm_route["duration"] / 60, 1),
            geometry=geometry,
            steps=steps,
            summary=_build_summary(steps),
            traffic_delay_minutes=0.0,
        )
        routes.append(route)

    # Cache routes
    if routes:
        redis_client.setex(
            cache_key,
            settings.route_cache_ttl,
            json.dumps([r.model_dump() for r in routes]),
        )

    return routes


def _parse_steps(legs: list[dict]) -> list[RouteStep]:
    """Parse OSRM leg/step data into RouteStep objects."""
    steps = []
    for leg in legs:
        for step in leg.get("steps", []):
            maneuver = step.get("maneuver", {})

            # Build instruction from OSRM maneuver data
            instruction = _build_instruction(step, maneuver)

            # Get start/end coordinates
            coords = step.get("geometry", {}).get("coordinates", [])
            start = coords[0] if coords else [0, 0]
            end = coords[-1] if coords else [0, 0]

            steps.append(RouteStep(
                instruction=instruction,
                distance_m=step.get("distance", 0),
                duration_s=step.get("duration", 0),
                start_lat=start[1],    # OSRM returns [lng, lat]
                start_lng=start[0],
                end_lat=end[1],
                end_lng=end[0],
                maneuver=maneuver.get("type"),
            ))
    return steps


def _build_instruction(step: dict, maneuver: dict) -> str:
    """Build a human-readable instruction from OSRM step data."""
    maneuver_type = maneuver.get("type", "")
    modifier = maneuver.get("modifier", "")
    road_name = step.get("name", "")

    if maneuver_type == "depart":
        return f"Head {modifier} on {road_name}" if road_name else f"Head {modifier}"
    elif maneuver_type == "arrive":
        return "You have arrived at your destination"
    elif maneuver_type == "turn":
        direction = modifier.replace("-", " ")
        if road_name:
            return f"Turn {direction} onto {road_name}"
        return f"Turn {direction}"
    elif maneuver_type == "merge":
        return f"Merge onto {road_name}" if road_name else "Merge"
    elif maneuver_type == "fork":
        return f"Take the {modifier} fork onto {road_name}" if road_name else f"Take the {modifier} fork"
    elif maneuver_type == "roundabout":
        exit_num = maneuver.get("exit", "")
        return f"Take exit {exit_num} from the roundabout onto {road_name}" if road_name else f"Take exit {exit_num}"
    elif maneuver_type in ("new name", "continue"):
        return f"Continue onto {road_name}" if road_name else "Continue straight"
    else:
        return f"{maneuver_type} {modifier} {road_name}".strip()


def _build_summary(steps: list[RouteStep]) -> str:
    """Build a route summary from the major road names."""
    road_names = []
    for step in steps:
        if step.maneuver in ("depart", "arrive"):
            continue
        name = step.instruction.split("onto ")[-1] if "onto " in step.instruction else ""
        if name and name not in road_names:
            road_names.append(name)
            if len(road_names) >= 3:
                break

    return " → ".join(road_names) if road_names else "Direct route"


def _mode_to_profile(mode: str) -> str:
    """Map our travel modes to OSRM profile names."""
    profiles = {
        "driving": "car",
        "walking": "foot",
        "cycling": "bicycle",
    }
    return profiles.get(mode, "car")


def _route_cache_key(
    olat: float, olng: float,
    dlat: float, dlng: float,
    mode: str,
) -> str:
    """Generate a cache key for a route query."""
    raw = f"{olat:.5f},{olng:.5f},{dlat:.5f},{dlng:.5f},{mode}"
    return f"route:{hashlib.md5(raw.encode()).hexdigest()}"
```

OSRM uses **Contraction Hierarchies (CH)**, a preprocessing-based algorithm. During preprocessing, it repeatedly removes ("contracts") the least important nodes from the graph and adds shortcut edges. At query time, a bidirectional Dijkstra search on the contracted graph finds optimal routes in milliseconds, even for cross-country queries on graphs with billions of edges. OSRM expects coordinates in `lng,lat` order (GeoJSON standard), not `lat,lng`. The route response includes a GeoJSON geometry (polyline of coordinates) for rendering on the map, step-by-step maneuver instructions for turn-by-turn navigation, and total distance/duration. Routes are cached in Redis for 5 minutes since traffic conditions change. The `alternatives=true` parameter asks OSRM to return up to 3 alternative routes so the user can choose.

---

## ETA & Live Traffic

### `app/traffic.py`

```python
import math
import random
from datetime import datetime, timezone

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import RoadSegment
from app.schemas import RouteResponse


# ─── Traffic Simulation ───────────────────────────────────────────

def get_traffic_factor(hour: int) -> float:
    """
    Get a simulated traffic factor based on time of day.

    Returns a multiplier (1.0 = free flow, 2.0 = double the travel time).
    In production, this would use real-time traffic data from GPS probes.
    """
    # Rush hour peaks at 8 AM and 5 PM
    morning_peak = math.exp(-0.5 * ((hour - 8) / 1.5) ** 2)
    evening_peak = math.exp(-0.5 * ((hour - 17) / 1.5) ** 2)

    # Base factor + rush hour contribution
    factor = 1.0 + 0.8 * max(morning_peak, evening_peak)

    # Add random noise (±10%) to simulate variance
    noise = 1.0 + random.uniform(-0.1, 0.1)

    return round(factor * noise, 2)


def apply_traffic_to_route(route: RouteResponse) -> RouteResponse:
    """
    Adjust a route's ETA based on current traffic conditions.

    In production, OSRM would use real-time traffic-weighted edge costs.
    Here, we apply a time-of-day multiplier as a simulation.
    """
    hour = datetime.now(timezone.utc).hour
    factor = get_traffic_factor(hour)

    original_minutes = route.duration_minutes
    adjusted_minutes = round(original_minutes * factor, 1)
    delay = round(adjusted_minutes - original_minutes, 1)

    route.duration_minutes = adjusted_minutes
    route.traffic_delay_minutes = max(delay, 0)

    # Adjust individual step durations proportionally
    for step in route.steps:
        step.duration_s = round(step.duration_s * factor, 1)

    return route


# ─── ETA Calculation ──────────────────────────────────────────────

def estimate_eta(
    distance_km: float,
    speed_kmh: float = 40.0,
) -> float:
    """
    Simple ETA estimation in minutes.
    Used when OSRM is unavailable or for quick estimates.
    """
    if speed_kmh <= 0:
        return 0
    return round((distance_km / speed_kmh) * 60, 1)


def estimate_eta_with_traffic(
    distance_km: float,
    speed_kmh: float = 40.0,
) -> float:
    """ETA estimation adjusted for current traffic conditions."""
    base_eta = estimate_eta(distance_km, speed_kmh)
    hour = datetime.now(timezone.utc).hour
    factor = get_traffic_factor(hour)
    return round(base_eta * factor, 1)


# ─── Road Segment Traffic ─────────────────────────────────────────

def update_road_traffic(
    segment_id: int,
    observed_speed: float,
    db: Session,
):
    """
    Update the traffic speed for a road segment.
    Called by GPS probe data processing pipeline.
    """
    segment = db.query(RoadSegment).filter(RoadSegment.id == segment_id).first()
    if segment:
        # Exponential moving average: new = 0.3 * observed + 0.7 * old
        if segment.traffic_speed:
            segment.traffic_speed = round(
                0.3 * observed_speed + 0.7 * segment.traffic_speed, 1
            )
        else:
            segment.traffic_speed = observed_speed
        db.commit()


def get_traffic_segments_in_bounds(
    north: float,
    south: float,
    west: float,
    east: float,
    db: Session,
) -> list[dict]:
    """
    Get road segments with traffic data within a bounding box.
    Used to render traffic overlay on the map (green/yellow/red colors).
    """
    envelope = func.ST_MakeEnvelope(west, south, east, north, 4326)

    segments = (
        db.query(RoadSegment)
        .filter(func.ST_Intersects(
            func.ST_GeomFromWKB(func.ST_AsBinary(RoadSegment.geometry)),
            envelope,
        ))
        .filter(RoadSegment.traffic_speed.isnot(None))
        .limit(500)
        .all()
    )

    results = []
    for seg in segments:
        if seg.speed_limit and seg.traffic_speed:
            ratio = seg.traffic_speed / seg.speed_limit
            if ratio > 0.75:
                color = "green"       # Free flow
            elif ratio > 0.4:
                color = "yellow"      # Moderate
            else:
                color = "red"         # Heavy traffic
        else:
            color = "gray"            # No data

        results.append({
            "id": seg.id,
            "name": seg.name,
            "road_class": seg.road_class,
            "speed_limit": seg.speed_limit,
            "traffic_speed": seg.traffic_speed,
            "color": color,
        })

    return results
```

Traffic estimation uses a **Gaussian time-of-day model** that simulates rush hour peaks at 8 AM and 5 PM. The `math.exp(-0.5 * ((hour - peak) / spread)²)` formula creates bell curves centered on rush hours. In production, Google Maps uses billions of GPS probe data points from Android phones to compute real-time traffic speeds on every road segment. The **exponential moving average** (`0.3 * new + 0.7 * old`) smooths noisy GPS observations to produce stable traffic speed estimates. The traffic overlay uses speed-to-limit ratios to color roads: green (>75% of limit), yellow (40-75%), red (<40%). `ST_Intersects` finds road segments that cross the viewport bounding box for rendering the traffic layer.

---

## Core API – Maps, Places & Directions

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

# Use a separate connection for binary data (tiles)
redis_binary = redis.from_url(
    settings.redis_url,
    decode_responses=False,                   # Return raw bytes for tiles
    socket_connect_timeout=5,
    retry_on_timeout=True,
)


# ─── Tile Cache ───────────────────────────────────────────────────

def get_cached_tile(cache_key: str) -> bytes | None:
    """Get a cached tile image (binary)."""
    return redis_binary.get(cache_key)


def cache_tile(cache_key: str, tile_bytes: bytes, ttl: int = 86400):
    """Cache a tile image with TTL."""
    redis_binary.setex(cache_key, ttl, tile_bytes)


# ─── Generic Cache ────────────────────────────────────────────────

def get_cached(key: str) -> str | None:
    """Get a cached string value."""
    return redis_client.get(key)


def set_cached(key: str, value: str, ttl: int = 3600):
    """Set a cached string value with TTL."""
    redis_client.setex(key, ttl, value)


def delete_cached(key: str):
    """Delete a cached value."""
    redis_client.delete(key)


# ─── Location Sharing ────────────────────────────────────────────

SHARE_PREFIX = "share:location:"

def update_shared_location(
    session_id: str,
    user_id: str,
    latitude: float,
    longitude: float,
    heading: float | None = None,
    speed_kmh: float | None = None,
):
    """Store a user's live location for a sharing session."""
    key = f"{SHARE_PREFIX}{session_id}:{user_id}"
    data = json.dumps({
        "latitude": latitude,
        "longitude": longitude,
        "heading": heading,
        "speed_kmh": speed_kmh,
    })
    redis_client.setex(key, 300, data)         # 5-minute TTL


def get_shared_locations(session_id: str) -> list[dict]:
    """Get all live locations in a sharing session."""
    pattern = f"{SHARE_PREFIX}{session_id}:*"
    keys = redis_client.keys(pattern)
    locations = []
    for key in keys:
        data = redis_client.get(key)
        if data:
            user_id = key.split(":")[-1]
            loc = json.loads(data)
            loc["user_id"] = user_id
            locations.append(loc)
    return locations
```

Two Redis connections are needed: one with `decode_responses=True` for JSON/string data (geocoding cache, route cache), and one with `decode_responses=False` for binary data (tile images). Tile images must be stored and retrieved as raw bytes. The tile cache TTL is 24 hours because tiles change infrequently. Location sharing uses Redis keys with a 5-minute TTL, so shared locations automatically expire if a user stops updating. The `keys()` pattern scan retrieves all participants in a sharing session.

### `app/routers/tiles.py`

```python
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from app.tiles import get_tile, get_tiles_for_viewport

router = APIRouter()


@router.get("/{z}/{x}/{y}.png")
def serve_tile(z: int, x: int, y: int, style: str = "default"):
    """
    Serve a map tile at the given zoom/x/y coordinates.
    Returns a PNG image with aggressive cache headers.
    """
    if z < 0 or z > 20:
        raise HTTPException(status_code=400, detail="Zoom must be 0-20.")

    tile_bytes = get_tile(z, x, y, style)
    if not tile_bytes:
        raise HTTPException(status_code=404, detail="Tile not found.")

    return Response(
        content=tile_bytes,
        media_type="image/png",
        headers={
            "Cache-Control": "public, max-age=86400",     # CDN caches for 24h
            "X-Tile-Zoom": str(z),
            "X-Tile-X": str(x),
            "X-Tile-Y": str(y),
        },
    )


@router.get("/viewport")
def get_viewport_tiles(
    north: float,
    south: float,
    west: float,
    east: float,
    zoom: int,
):
    """
    Get the list of tile coordinates needed for a viewport.
    The client uses this to know which tiles to fetch.
    """
    if zoom < 0 or zoom > 20:
        raise HTTPException(status_code=400, detail="Zoom must be 0-20.")

    tiles = get_tiles_for_viewport(north, south, west, east, zoom)

    return {
        "zoom": zoom,
        "tile_count": len(tiles),
        "tiles": tiles,
    }
```

The tile endpoint returns raw PNG bytes with `Cache-Control: public, max-age=86400`. Cloud CDN reads this header and caches the tile at edge locations worldwide, so subsequent requests for the same tile are served from the nearest CDN edge without hitting the origin server. The `X-Tile-*` headers help with debugging and monitoring. The viewport endpoint lets the client pre-calculate which tiles it needs, useful for prefetching tiles just outside the current viewport for smooth panning.

### `app/routers/places.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Place, PlaceCategory, SavedPlace, SearchHistory
from app.schemas import PlaceResponse, PlaceCreate, SearchHistoryEntry
from app.search import search_places, search_places_in_bounds

router = APIRouter()


@router.get("/search", response_model=list[PlaceResponse])
def search(
    latitude: float = Query(ge=-90, le=90),
    longitude: float = Query(ge=-180, le=180),
    radius_km: float = Query(5.0, gt=0, le=50),
    q: str | None = None,
    category: str | None = None,
    limit: int = Query(20, ge=1, le=50),
    user_id: str | None = None,
    db: Session = Depends(get_db),
):
    """Search for places near a location with optional text and category filters."""
    results = search_places(
        latitude=latitude,
        longitude=longitude,
        radius_km=radius_km,
        query=q,
        category=category,
        limit=limit,
        db=db,
    )

    # Log search history
    if user_id and q:
        history = SearchHistory(
            user_id=user_id,
            query=q,
            latitude=latitude,
            longitude=longitude,
            result_type="place_search",
            result_id=results[0].id if results else None,
        )
        db.add(history)
        db.commit()

    return results


@router.get("/bounds", response_model=list[PlaceResponse])
def search_in_bounds(
    north: float = Query(ge=-90, le=90),
    south: float = Query(ge=-90, le=90),
    west: float = Query(ge=-180, le=180),
    east: float = Query(ge=-180, le=180),
    category: str | None = None,
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Search for places within a map viewport bounding box."""
    return search_places_in_bounds(
        north=north, south=south,
        west=west, east=east,
        category=category,
        limit=limit,
        db=db,
    )


@router.get("/{place_id}", response_model=PlaceResponse)
def get_place(place_id: str, db: Session = Depends(get_db)):
    """Get detailed information about a specific place."""
    place = db.query(Place).filter(Place.id == place_id).first()
    if not place:
        raise HTTPException(status_code=404, detail="Place not found.")

    cat_name = place.category.name if place.category else None
    return PlaceResponse(
        id=place.id,
        name=place.name,
        address=place.address,
        latitude=place.latitude,
        longitude=place.longitude,
        category=cat_name,
        rating=place.rating,
        review_count=place.review_count,
        phone=place.phone,
        website=place.website,
        opening_hours=place.opening_hours,
        price_level=place.price_level,
        distance_km=None,
    )


@router.post("/", response_model=PlaceResponse, status_code=201)
def create_place(payload: PlaceCreate, db: Session = Depends(get_db)):
    """Create a new place (admin endpoint)."""
    category = None
    if payload.category_slug:
        category = (
            db.query(PlaceCategory)
            .filter(PlaceCategory.slug == payload.category_slug)
            .first()
        )

    place = Place(
        name=payload.name,
        address=payload.address,
        latitude=payload.latitude,
        longitude=payload.longitude,
        location=f"POINT({payload.longitude} {payload.latitude})",
        category_id=category.id if category else None,
        phone=payload.phone,
        website=payload.website,
        opening_hours=payload.opening_hours,
        price_level=payload.price_level,
    )

    db.add(place)
    db.commit()
    db.refresh(place)

    return PlaceResponse(
        id=place.id,
        name=place.name,
        address=place.address,
        latitude=place.latitude,
        longitude=place.longitude,
        category=category.name if category else None,
        rating=place.rating,
        review_count=place.review_count,
        phone=place.phone,
        website=place.website,
        opening_hours=place.opening_hours,
        price_level=place.price_level,
        distance_km=None,
    )


@router.post("/{place_id}/save")
def save_place(
    place_id: str,
    user_id: str,
    label: str | None = None,
    db: Session = Depends(get_db),
):
    """Save a place to user's favorites."""
    place = db.query(Place).filter(Place.id == place_id).first()
    if not place:
        raise HTTPException(status_code=404, detail="Place not found.")

    existing = (
        db.query(SavedPlace)
        .filter(SavedPlace.user_id == user_id, SavedPlace.place_id == place_id)
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Place already saved.")

    saved = SavedPlace(user_id=user_id, place_id=place_id, label=label)
    db.add(saved)
    db.commit()

    return {"status": "saved", "place_id": place_id}


@router.get("/history/{user_id}", response_model=list[SearchHistoryEntry])
def get_search_history(
    user_id: str,
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get a user's recent search history."""
    history = (
        db.query(SearchHistory)
        .filter(SearchHistory.user_id == user_id)
        .order_by(SearchHistory.searched_at.desc())
        .limit(limit)
        .all()
    )
    return history
```

The search endpoint combines spatial, text, and category filtering in a single query. Search history is logged asynchronously; in production, this would be sent to a message queue instead of writing directly during the request. The bounds endpoint is called on every map pan/zoom, so it must be fast; the PostGIS GIST index on the geography column makes bounding box queries efficient. The `location` column is set using WKT format `POINT(lng lat)` when creating a place, and PostGIS automatically indexes it.

### `app/routers/directions.py`

```python
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.geocoding import reverse_geocode
from app.routing import get_route
from app.traffic import apply_traffic_to_route, get_traffic_segments_in_bounds
from app.schemas import DirectionsResponse

router = APIRouter()


@router.get("/route")
async def get_directions(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
    mode: str = "driving",
    alternatives: bool = False,
    db: Session = Depends(get_db),
) -> DirectionsResponse:
    """
    Get directions between two points.
    Returns route geometry, step-by-step instructions, and ETA.
    """
    routes = await get_route(
        origin_lat=origin_lat,
        origin_lng=origin_lng,
        dest_lat=dest_lat,
        dest_lng=dest_lng,
        mode=mode,
        alternatives=alternatives,
    )

    if not routes:
        raise HTTPException(
            status_code=404,
            detail="No route found between the given points.",
        )

    # Apply traffic adjustments
    routes = [apply_traffic_to_route(r) for r in routes]

    # Reverse geocode origin and destination for display
    origin_info = reverse_geocode(origin_lat, origin_lng, db)
    dest_info = reverse_geocode(dest_lat, dest_lng, db)

    return DirectionsResponse(
        routes=routes,
        origin=origin_info.address,
        destination=dest_info.address,
    )


@router.get("/traffic")
def get_traffic_overlay(
    north: float,
    south: float,
    west: float,
    east: float,
    db: Session = Depends(get_db),
):
    """
    Get traffic data for road segments within a viewport.
    Used to render the traffic overlay layer on the map.
    """
    segments = get_traffic_segments_in_bounds(north, south, west, east, db)
    return {"segments": segments, "count": len(segments)}
```

The directions endpoint orchestrates three operations: route computation (OSRM), traffic adjustment, and reverse geocoding for human-readable origin/destination names. The `async` keyword enables non-blocking calls to OSRM via `httpx.AsyncClient`. Traffic data is applied as a post-processing step on top of OSRM's base route times. The traffic overlay endpoint returns colored road segments for the client to render as a map layer.

### `app/routers/geocode.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.geocoding import geocode, reverse_geocode
from app.schemas import GeocodeResult, ReverseGeocodeResult

router = APIRouter()


@router.get("/forward", response_model=list[GeocodeResult])
def forward_geocode(
    q: str = Query(min_length=2, max_length=255),
    db: Session = Depends(get_db),
):
    """
    Convert an address or place name to coordinates.
    Returns up to 5 matching results ranked by confidence.
    """
    results = geocode(q, db)
    if not results:
        raise HTTPException(
            status_code=404,
            detail="No results found for the given query.",
        )
    return results


@router.get("/reverse", response_model=ReverseGeocodeResult)
def reverse(
    latitude: float = Query(ge=-90, le=90),
    longitude: float = Query(ge=-180, le=180),
    db: Session = Depends(get_db),
):
    """
    Convert GPS coordinates to the nearest address.
    Used when the user taps on the map to get location details.
    """
    return reverse_geocode(latitude, longitude, db)
```

The forward geocode endpoint validates query length (2-255 characters) to prevent empty queries and excessively long strings. The reverse geocode endpoint is called when a user long-presses on the map to get the address at that point. Both endpoints leverage Redis caching with a 1-hour TTL to reduce database load.

---

## Real-Time Location Sharing (WebSockets)

### `app/websocket_manager.py`

```python
import json
from fastapi import WebSocket


class MapConnectionManager:
    """Manages WebSocket connections for real-time location sharing."""

    def __init__(self):
        # session_id → list of connected WebSocket clients
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, session_id: str, websocket: WebSocket):
        """Accept a new WebSocket connection for a sharing session."""
        await websocket.accept()
        if session_id not in self.active_connections:
            self.active_connections[session_id] = []
        self.active_connections[session_id].append(websocket)

    def disconnect(self, session_id: str, websocket: WebSocket):
        """Remove a WebSocket connection."""
        if session_id in self.active_connections:
            self.active_connections[session_id].remove(websocket)
            if not self.active_connections[session_id]:
                del self.active_connections[session_id]

    async def broadcast_to_session(self, session_id: str, message: dict):
        """Send a message to all clients in a sharing session."""
        if session_id in self.active_connections:
            dead_connections = []
            for connection in self.active_connections[session_id]:
                try:
                    await connection.send_json(message)
                except Exception:
                    dead_connections.append(connection)

            # Clean up dead connections
            for conn in dead_connections:
                self.disconnect(session_id, conn)

    async def send_location_update(
        self,
        session_id: str,
        user_id: str,
        latitude: float,
        longitude: float,
        heading: float | None = None,
        speed_kmh: float | None = None,
    ):
        """Broadcast a user's location update to all session participants."""
        await self.broadcast_to_session(session_id, {
            "type": "location_update",
            "data": {
                "user_id": user_id,
                "latitude": latitude,
                "longitude": longitude,
                "heading": heading,
                "speed_kmh": speed_kmh,
            },
        })

    async def send_eta_update(
        self,
        session_id: str,
        user_id: str,
        eta_minutes: float,
        distance_km: float,
    ):
        """Broadcast ETA update to session participants."""
        await self.broadcast_to_session(session_id, {
            "type": "eta_update",
            "data": {
                "user_id": user_id,
                "eta_minutes": eta_minutes,
                "distance_km": distance_km,
            },
        })


manager = MapConnectionManager()
```

The `MapConnectionManager` maintains a dictionary mapping `session_id` to a list of WebSocket connections. Multiple users join the same session to share locations (e.g., "Share my trip" feature). Dead connections are detected by catching send exceptions and cleaned up immediately. In production, when running multiple API server instances, each instance only knows about its own WebSocket connections. You would use Redis Pub/Sub or Cloud Pub/Sub to fan out messages across all instances.

### `app/routers/sharing.py`

```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.cache import update_shared_location, get_shared_locations
from app.websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/share/{session_id}")
async def share_location(session_id: str, websocket: WebSocket):
    """WebSocket endpoint for real-time location sharing."""
    await manager.connect(session_id, websocket)

    try:
        # Send current participants' locations on connect
        locations = get_shared_locations(session_id)
        if locations:
            await websocket.send_json({
                "type": "initial_locations",
                "data": locations,
            })

        while True:
            data = await websocket.receive_json()

            if data.get("type") == "location_update":
                user_id = data.get("user_id", "anonymous")
                lat = data["latitude"]
                lng = data["longitude"]
                heading = data.get("heading")
                speed = data.get("speed_kmh")

                # Store in Redis for persistence
                update_shared_location(
                    session_id=session_id,
                    user_id=user_id,
                    latitude=lat,
                    longitude=lng,
                    heading=heading,
                    speed_kmh=speed,
                )

                # Broadcast to all session participants
                await manager.send_location_update(
                    session_id=session_id,
                    user_id=user_id,
                    latitude=lat,
                    longitude=lng,
                    heading=heading,
                    speed_kmh=speed,
                )

            elif data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        manager.disconnect(session_id, websocket)
```

When a user connects to a sharing session, they immediately receive the current locations of all other participants from Redis. As each user sends location updates, they are both persisted to Redis (for new joiners) and broadcast to all connected clients via WebSocket. The `ping/pong` mechanism keeps the connection alive. `WebSocketDisconnect` is raised when the client closes or the network drops, triggering cleanup.

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
      - DATABASE_URL=postgresql://user:pass@db:5432/maps
      - REDIS_URL=redis://cache:6379/0
      - OSRM_URL=http://osrm:5000
      - BASE_URL=http://localhost:8000
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
      osrm:
        condition: service_started
    restart: unless-stopped

  db:
    image: postgis/postgis:16-3.4
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=maps
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d maps"]
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

  osrm:
    image: osrm/osrm-backend:latest
    ports:
      - "5000:5000"
    volumes:
      - osrm-data:/data
    command: osrm-routed --algorithm mld /data/map.osrm
    restart: unless-stopped

volumes:
  db-data:
  redis-data:
  osrm-data:
```

`postgis/postgis:16-3.4` is the official PostGIS image based on PostgreSQL 16 with spatial extensions pre-installed. Using `postgres:16` would require manually installing PostGIS. `depends_on` with `condition: service_healthy` ensures the API waits until PostgreSQL and Redis pass their health checks. `osrm/osrm-backend` runs the OSRM routing engine; `--algorithm mld` uses Multi-Level Dijkstra which supports faster preprocessing than Contraction Hierarchies and allows live traffic updates. The OSRM data volume requires a pre-processed map file. `redis:7-alpine` is a minimal Redis image. `--appendonly yes` enables AOF persistence so cached data survives container restarts.

### Preparing OSRM Data

```bash
# Download OpenStreetMap data (example: Nepal)
wget https://download.geofabrik.de/asia/nepal-latest.osm.pbf -O map.osm.pbf

# Pre-process the road network
docker run --rm -v $(pwd):/data osrm/osrm-backend osrm-extract -p /opt/car.lua /data/map.osm.pbf
docker run --rm -v $(pwd):/data osrm/osrm-backend osrm-partition /data/map.osrm
docker run --rm -v $(pwd):/data osrm/osrm-backend osrm-customize /data/map.osrm

# Copy processed files to the Docker volume
docker volume create osrm-data
docker run --rm -v $(pwd):/src -v osrm-data:/data alpine cp /src/map.osrm* /data/
```

`osrm-extract` parses the OSM PBF file and extracts the road graph, applying the car profile (`car.lua`) to determine which roads are drivable and at what speed. `osrm-partition` and `osrm-customize` pre-process the graph for the Multi-Level Dijkstra algorithm. The processed files (`.osrm`, `.osrm.edges`, `.osrm.nodes`, etc.) are copied to the Docker volume. For production, use a larger extract (country or continent) from Geofabrik.

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import tiles, places, directions, geocode, sharing


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Google Maps API",
    description="A mapping system with tiles, geocoding, routing, and place search",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(tiles.router, prefix="/tiles", tags=["Tiles"])
app.include_router(places.router, prefix="/places", tags=["Places"])
app.include_router(directions.router, prefix="/directions", tags=["Directions"])
app.include_router(geocode.router, prefix="/geocode", tags=["Geocoding"])
app.include_router(sharing.router, tags=["Location Sharing"])


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
# Create a place
curl -X POST http://localhost:8000/places/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Kathmandu Durbar Square",
    "address": "Basantapur, Kathmandu 44600",
    "latitude": 27.7046,
    "longitude": 85.3066,
    "category_slug": "landmark"
  }'

# Search nearby places
curl "http://localhost:8000/places/search?latitude=27.7172&longitude=85.3240&radius_km=5&q=temple"

# Get directions
curl "http://localhost:8000/directions/route?origin_lat=27.7172&origin_lng=85.3240&dest_lat=27.6753&dest_lng=85.3135&mode=driving"

# Forward geocode
curl "http://localhost:8000/geocode/forward?q=Kathmandu+Durbar+Square"

# Reverse geocode
curl "http://localhost:8000/geocode/reverse?latitude=27.7172&longitude=85.3240"

# Get a map tile
curl "http://localhost:8000/tiles/15/23887/14209.png" --output tile.png

# Get traffic overlay
curl "http://localhost:8000/directions/traffic?north=27.75&south=27.68&west=85.28&east=85.38"
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

### `tests/test_geocoding.py`

```python
from unittest.mock import patch, MagicMock

from app.geocoding import geocode, reverse_geocode
from app.schemas import GeocodeResult


def test_geocode_returns_results_for_known_place(db):
    """Test that geocoding finds a known place by name."""
    from app.models import Place

    place = Place(
        name="Central Park",
        address="Central Park, New York, NY",
        latitude=40.7829,
        longitude=-73.9654,
        location=f"POINT(-73.9654 40.7829)",
    )
    db.add(place)
    db.commit()

    with patch("app.geocoding.redis_client") as mock_redis:
        mock_redis.get.return_value = None
        results = geocode("Central Park", db)

    assert len(results) >= 1
    assert results[0].latitude == 40.7829
    assert results[0].confidence >= 0.7


def test_geocode_returns_empty_for_unknown(db):
    """Test that geocoding returns empty for an unknown query."""
    with patch("app.geocoding.redis_client") as mock_redis:
        mock_redis.get.return_value = None
        results = geocode("xyznonexistent123", db)

    assert results == []


def test_geocode_uses_cache():
    """Test that geocoding returns cached results."""
    import json
    cached_data = [{"address": "Cached St", "latitude": 1.0, "longitude": 2.0, "place_id": None, "confidence": 0.9}]

    with patch("app.geocoding.redis_client") as mock_redis:
        mock_redis.get.return_value = json.dumps(cached_data)
        results = geocode("test query", MagicMock())

    assert len(results) == 1
    assert results[0].address == "Cached St"
```

### `tests/test_search.py`

```python
from unittest.mock import patch, MagicMock

from app.search import search_places
from app.schemas import PlaceResponse


def test_search_returns_nearby_places(db):
    """Test that search returns places within the given radius."""
    from app.models import Place

    nearby = Place(
        name="Nearby Cafe",
        address="123 Main St",
        latitude=40.7128,
        longitude=-74.0060,
        location=f"POINT(-74.0060 40.7128)",
    )
    far = Place(
        name="Far Away Restaurant",
        address="999 Distant Ave",
        latitude=41.0000,
        longitude=-75.0000,
        location=f"POINT(-75.0000 41.0000)",
    )
    db.add_all([nearby, far])
    db.commit()

    # Note: This test requires PostGIS. With SQLite, spatial queries won't work.
    # In a real test environment, use a PostGIS-enabled test database.


def test_search_filters_by_query(db):
    """Test that text filtering narrows results."""
    from app.models import Place

    cafe = Place(
        name="Blue Bottle Coffee",
        address="1 Main St",
        latitude=40.7128,
        longitude=-74.0060,
        location=f"POINT(-74.0060 40.7128)",
    )
    restaurant = Place(
        name="Italian Restaurant",
        address="2 Main St",
        latitude=40.7130,
        longitude=-74.0062,
        location=f"POINT(-74.0062 40.7130)",
    )
    db.add_all([cafe, restaurant])
    db.commit()

    # With PostGIS, search_places with query="Coffee" should return only the cafe
```

### `tests/test_routing.py`

```python
import json
from unittest.mock import patch, AsyncMock

import pytest

from app.routing import get_route, _build_instruction, _mode_to_profile


def test_build_instruction_turn():
    """Test instruction building for turn maneuvers."""
    step = {"name": "Broadway"}
    maneuver = {"type": "turn", "modifier": "left"}

    instruction = _build_instruction(step, maneuver)
    assert instruction == "Turn left onto Broadway"


def test_build_instruction_arrive():
    """Test instruction building for arrival."""
    step = {"name": ""}
    maneuver = {"type": "arrive", "modifier": ""}

    instruction = _build_instruction(step, maneuver)
    assert instruction == "You have arrived at your destination"


def test_build_instruction_depart():
    """Test instruction building for departure."""
    step = {"name": "5th Avenue"}
    maneuver = {"type": "depart", "modifier": "north"}

    instruction = _build_instruction(step, maneuver)
    assert instruction == "Head north on 5th Avenue"


def test_mode_to_profile():
    """Test travel mode to OSRM profile mapping."""
    assert _mode_to_profile("driving") == "car"
    assert _mode_to_profile("walking") == "foot"
    assert _mode_to_profile("cycling") == "bicycle"
    assert _mode_to_profile("unknown") == "car"


@pytest.mark.asyncio
async def test_get_route_with_cache():
    """Test that cached routes are returned without calling OSRM."""
    cached = [{"distance_km": 5.0, "duration_minutes": 10.0, "geometry": [], "steps": [], "summary": "Test", "traffic_delay_minutes": 0}]

    with patch("app.routing.redis_client") as mock_redis:
        mock_redis.get.return_value = json.dumps(cached)

        routes = await get_route(40.7128, -74.0060, 40.7580, -73.9855)

    assert len(routes) == 1
    assert routes[0].distance_km == 5.0


@pytest.mark.asyncio
async def test_get_route_calls_osrm():
    """Test that OSRM is called when cache is empty."""
    osrm_response = {
        "code": "Ok",
        "routes": [{
            "distance": 5000,
            "duration": 600,
            "geometry": {"coordinates": [[-74.006, 40.713], [-73.986, 40.758]]},
            "legs": [{
                "steps": [{
                    "maneuver": {"type": "depart", "modifier": "north"},
                    "name": "Broadway",
                    "distance": 5000,
                    "duration": 600,
                    "geometry": {"coordinates": [[-74.006, 40.713], [-73.986, 40.758]]},
                }]
            }]
        }]
    }

    with patch("app.routing.redis_client") as mock_redis, \
         patch("httpx.AsyncClient") as mock_client:

        mock_redis.get.return_value = None

        mock_response = AsyncMock()
        mock_response.json.return_value = osrm_response
        mock_response.raise_for_status = lambda: None

        mock_instance = AsyncMock()
        mock_instance.get.return_value = mock_response
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        mock_client.return_value = mock_instance

        routes = await get_route(40.7128, -74.0060, 40.7580, -73.9855)

    assert len(routes) == 1
    assert routes[0].distance_km == 5.0
```

### `tests/test_tiles.py`

```python
from unittest.mock import patch

from app.tiles import (
    lng_to_tile_x, lat_to_tile_y,
    tile_to_lng, tile_to_lat,
    is_valid_tile, tile_bounds,
    get_tiles_for_viewport,
)


def test_lng_to_tile_x():
    """Test longitude to tile X conversion."""
    # At zoom 0, any longitude maps to tile 0
    assert lng_to_tile_x(0, 0) == 0
    # At zoom 1, longitude 0 maps to tile 1
    assert lng_to_tile_x(0, 1) == 1
    # Longitude -180 maps to tile 0
    assert lng_to_tile_x(-180, 1) == 0


def test_lat_to_tile_y():
    """Test latitude to tile Y conversion."""
    # At zoom 0, equator maps to tile 0
    assert lat_to_tile_y(0, 0) == 0
    # At zoom 1, equator maps to tile 1
    assert lat_to_tile_y(0, 1) == 1


def test_is_valid_tile():
    """Test tile coordinate validation."""
    assert is_valid_tile(0, 0, 0) is True
    assert is_valid_tile(1, 0, 0) is True
    assert is_valid_tile(1, 1, 1) is True
    assert is_valid_tile(1, 2, 0) is False       # x > max for zoom 1
    assert is_valid_tile(1, 0, 2) is False       # y > max for zoom 1


def test_tile_bounds():
    """Test tile bounding box calculation."""
    bounds = tile_bounds(0, 0, 0)
    assert bounds["west"] == -180.0
    assert bounds["east"] == 180.0
    assert bounds["north"] > 80                   # Mercator doesn't cover poles
    assert bounds["south"] < -80


def test_roundtrip_conversion():
    """Test that tile→coords→tile round-trips correctly."""
    z, x, y = 15, 23887, 14209
    lng = tile_to_lng(x, z)
    lat = tile_to_lat(y, z)
    assert lng_to_tile_x(lng, z) == x
    assert lat_to_tile_y(lat, z) == y


def test_get_tiles_for_viewport():
    """Test viewport tile calculation."""
    # Small viewport at zoom 15
    tiles = get_tiles_for_viewport(
        north=27.72, south=27.71,
        west=85.32, east=85.33,
        zoom=15,
    )
    assert len(tiles) > 0
    for t in tiles:
        assert t["z"] == 15
        assert is_valid_tile(t["z"], t["x"], t["y"])
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_routing"                      # Run only routing tests
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

### Set Up Cloud SQL (PostgreSQL + PostGIS)

```bash
# Create a Cloud SQL instance
gcloud sql instances create maps-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-2-7680 \
  --region=us-central1 \
  --storage-size=100GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create maps \
  --instance=maps-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=maps-db \
  --password=YOUR_SECURE_PASSWORD

# Enable PostGIS extension (connect via Cloud SQL proxy)
# Then run: CREATE EXTENSION postgis;
```

`--tier=db-custom-2-7680` provisions 2 vCPUs and 7.5 GB RAM, suitable for spatial queries on 200 M POI records. `--availability-type=REGIONAL` enables automatic failover to a standby instance in another zone. `--storage-size=100GB` accommodates the place data, road segments, and search history. PostGIS must be enabled manually with `CREATE EXTENSION postgis` after the database is created.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create maps-cache \
  --size=5 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD

# Get the Redis host IP
gcloud redis instances describe maps-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=5` provisions 5 GB of memory, enough for tile cache (millions of frequently accessed tiles), geocoding cache, and route cache. `--tier=STANDARD` enables replication with automatic failover. Memorystore is accessible only from within the same VPC network.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/maps/api

# Or use Container Registry
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/maps-api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build, Google's managed CI/CD service. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy to Cloud Run

```bash
gcloud run deploy maps-api \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/maps/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 2Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 100 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:maps-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/maps?host=/cloudsql/YOUR_PROJECT_ID:us-central1:maps-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
OSRM_URL=http://OSRM_INTERNAL_IP:5000,\
BASE_URL=https://your-service-url.run.app,\
TILE_CACHE_TTL=86400,\
GEOCODE_CACHE_TTL=3600,\
ROUTE_CACHE_TTL=300"
```

`--min-instances 2` keeps at least two instances warm to avoid cold starts. `--max-instances 100` allows aggressive scaling for high tile request volumes. `--memory 2Gi` provides enough memory for handling concurrent spatial queries and image processing. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy for secure, IAM-based database connections. **Note**: OSRM should run on a dedicated Compute Engine instance or GKE pod with sufficient memory to hold the road graph (60+ GB for a continent).

### Set Up Cloud CDN for Tiles

```bash
# Create a backend bucket pointing to Cloud Storage
gsutil mb -l us-central1 gs://YOUR_PROJECT_ID-map-tiles

# Upload pre-rendered tiles
gsutil -m rsync -r ./tiles gs://YOUR_PROJECT_ID-map-tiles/

# Create a CDN-enabled load balancer
gcloud compute backend-buckets create map-tile-backend \
  --gcs-bucket-name=YOUR_PROJECT_ID-map-tiles \
  --enable-cdn \
  --cache-mode=CACHE_ALL_STATIC

# Create URL map
gcloud compute url-maps create map-tiles-lb \
  --default-backend-bucket=map-tile-backend
```

Cloud CDN caches tiles at Google's edge locations worldwide. Tile requests from Tokyo are served from a Tokyo edge server without crossing the Pacific to your origin. `CACHE_ALL_STATIC` caches all responses with `Cache-Control: public` headers. With 1 B tile requests/day, CDN offloads >99% of requests from your origin server.

### Set Up Serverless VPC Connector

```bash
# Required for Cloud Run to access Memorystore (Redis)
gcloud compute networks vpc-access connectors create maps-vpc-connector \
  --region=us-central1 \
  --network=default \
  --range=10.8.0.0/28 \
  --min-instances=2 \
  --max-instances=10

# Update Cloud Run to use the connector
gcloud run services update maps-api \
  --vpc-connector=maps-vpc-connector \
  --region=us-central1
```

Cloud Run services run in Google's serverless infrastructure outside your VPC by default. A VPC connector creates a bridge so Cloud Run can reach private resources like Memorystore. `--range=10.8.0.0/28` allocates a small IP range for the connector.

---

## Scaling & Production Considerations

### Tile Serving at Scale

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Cloud CDN             | Always (first line of defense)             | Cache tiles at edge with 24h TTL                   |
| Pre-rendered Tiles    | Static map styles                          | Render all tiles offline, store in Cloud Storage   |
| Vector Tiles          | Dynamic styling, smaller payload           | Serve Protobuf tiles, render client-side with GL   |
| Tile Pyramids         | Multi-resolution serving                   | Generate tiles at all zoom levels during ingest    |
| Regional Tile Servers | Global operation                           | Deploy tile origin servers in each continent       |

### Database Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| Read Replicas         | Heavy search and geocoding queries         | Cloud SQL read replicas + SQLAlchemy `bind` param  |
| Spatial Partitioning  | `places` table exceeds 1 B rows            | Partition by geohash prefix or region              |
| Connection Pooling    | Many concurrent connections from Cloud Run | PgBouncer sidecar or Cloud SQL Auth Proxy pooling  |
| Materialized Views    | Popular search aggregations                | Pre-compute top places per category per region     |
| CQRS                  | Read patterns differ from write patterns   | Separate read models optimized for search          |

### Routing Engine Scaling

| Strategy              | When to Use                                | How                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------- |
| OSRM Clustering       | Route QPS exceeds single-node capacity     | Multiple OSRM instances behind a load balancer     |
| Regional Graphs       | Global operation                           | Separate road graph per continent, route locally   |
| Route Caching         | Popular origin-destination pairs           | Redis cache with 5-min TTL for repeated queries    |
| Traffic Updates       | Real-time traffic-aware routing            | OSRM MLD profile with live edge weight updates     |
| Precomputed Routes    | Very high traffic corridors                | Pre-compute top 10 K routes, serve from cache      |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ maps.xyz       │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Global Load    │
                     │ Balancer       │
                     └──┬──────────┬──┘
                        │          │
              ┌─────────▼──┐  ┌───▼──────────┐
              │ Cloud CDN  │  │  Cloud Run   │
              │ (Tiles)    │  │  (API)       │
              └─────────┬──┘  └───┬──────────┘
                        │         │
                  ┌─────▼─────┐   │
                  │  Cloud    │   │
                  │  Storage  │   │
                  │  (Tiles)  │   │
                  └───────────┘   │
           ┌──────────────────────┼──────────────┐
           │            VPC Network              │
           │                                      │
           │  ┌──────────┐    ┌────────────────┐ │
           │  │ Memory-  │    │   Cloud SQL    │ │
           │  │ store    │    │  (PostGIS +    │ │
           │  │ (Redis)  │    │ Read Replicas) │ │
           │  └──────────┘    └────────────────┘ │
           │                                      │
           │  ┌──────────┐    ┌────────────────┐ │
           │  │ GKE Pods │    │   Cloud        │ │
           │  │ (OSRM    │    │   Pub/Sub      │ │
           │  │ Cluster) │    │   (Events)     │ │
           │  └──────────┘    └────────────────┘ │
           │                                      │
           │  ┌───────────────────────────────┐   │
           │  │  Cloud Run Jobs               │   │
           │  │  ┌──────────────────────────┐ │   │
           │  │  │ Tile pre-renderer       │ │   │
           │  │  │ Traffic data processor  │ │   │
           │  │  │ Place data ingester     │ │   │
           │  │  └──────────────────────────┘ │   │
           │  └───────────────────────────────┘   │
           └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud CDN** serves >99% of tile requests from edge locations without hitting the origin, critical at 1 B requests/day.
- **Cloud Storage** holds pre-rendered tiles. A background tile renderer re-generates tiles when map data is updated.
- **GKE (Kubernetes Engine)** runs the OSRM routing cluster because OSRM needs 60+ GB RAM for continental road graphs, which exceeds Cloud Run's 32 GB limit. Multiple OSRM pods handle high routing QPS.
- **Cloud Pub/Sub** decouples event processing. GPS probes from Android phones publish location data to a topic, consumed by a traffic processing pipeline that updates road segment speeds.
- **Read Replicas** handle search and geocoding queries without impacting the primary database's write performance for place updates.
- **Spatial Partitioning** of the places table by region or geohash prefix allows queries to scan only relevant partitions.

### Security Checklist

```
✓ Rate limit tile requests per API key to prevent scraping
✓ Validate all coordinates are within valid ranges (-90/90, -180/180)
✓ Authenticate API calls with API keys or OAuth tokens
✓ Encrypt all data in transit (TLS) and at rest (Cloud SQL encryption)
✓ Restrict OSRM access to internal VPC only (no public endpoint)
✓ Implement CORS for browser-based map clients
✓ Watermark tiles to prevent unauthorized redistribution
✓ Enable Cloud Audit Logs for compliance tracking
✓ Sanitize search queries to prevent SQL injection
✓ Mask exact user locations in shared sessions (snap to road)
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create maps-health \
  --display-name="Maps API Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - Tile cache hit ratio < 95% (CDN not working properly)
# - p99 route query latency > 2 seconds
# - p99 search latency > 500 ms
# - Geocoding cache hit ratio < 80%
# - OSRM error rate > 1%
# - PostGIS query time > 200 ms
# - Redis memory usage > 85%
# - Cloud SQL connections > 90% of max
# - Error rate > 1% over 5 minutes
# - Tile rendering backlog > 1000 tiles
```

Cloud Run and GKE automatically export metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing across the API → Redis → PostGIS → OSRM chain. Track business metrics like tile request volume, popular search queries, and route computation times in a dashboard to detect service degradation before users report it.
