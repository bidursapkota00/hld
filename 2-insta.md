# Design Instagram Feed

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [User & Follow System](#user--follow-system)
6. [Post Creation & Media Upload](#post-creation--media-upload)
7. [News Feed Generation (Fan-Out)](#news-feed-generation-fan-out)
8. [Feed API & Pagination](#feed-api--pagination)
9. [Likes & Comments](#likes--comments)
10. [Caching with Redis](#caching-with-redis)
11. [Background Workers with Celery](#background-workers-with-celery)
12. [Docker & Docker Compose](#docker--docker-compose)
13. [Testing](#testing)
14. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
15. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing code, define the scope and estimate the scale.

### Functional Requirements

| Requirement      | Description                                                         |
| ---------------- | ------------------------------------------------------------------- |
| Post Creation    | Users upload photos/videos with captions and hashtags               |
| Follow/Unfollow  | Users can follow and unfollow other users                           |
| News Feed        | Each user sees a chronological feed of posts from people they follow |
| Like & Comment   | Users can like posts and leave comments                             |
| Feed Pagination  | Infinite scroll with cursor-based pagination                        |
| Media Storage    | Store and serve images/videos via CDN                               |

### Non-Functional Requirements

| Requirement      | Target                                                     |
| ---------------- | ---------------------------------------------------------- |
| Availability     | 99.9% uptime                                               |
| Latency          | Feed load in < 200 ms (cache hit) / < 500 ms (cache miss) |
| Durability       | No post or follow relationship should ever be lost         |
| Scalability      | Handle 500 M users, 100 M daily active users               |

### Back-of-the-Envelope Estimations

```
Daily active users (DAU)        ≈ 100 M
Average follows per user        ≈ 200
New posts per day               ≈ 50 M
Feed reads per day              ≈ 500 M (each DAU loads feed ~5 times)
Average post size (metadata)    ≈ 1 KB
Media per post (compressed)     ≈ 500 KB average
Storage for posts (5 years)     ≈ 50 M × 365 × 5 × 1 KB ≈ 91 TB (metadata)
Media storage (5 years)         ≈ 50 M × 365 × 5 × 500 KB ≈ 45 PB
Feed fan-out writes per day     ≈ 50 M posts × 200 followers = 10 B feed entries
Cache (hot feeds, top 20%)      ≈ 100 M × 0.2 × 50 KB ≈ 1 TB (Redis cluster)
```

At this scale, a **hybrid fan-out** strategy is essential: fan-out-on-write for regular users (pre-compute feeds) and fan-out-on-read for celebrity users (millions of followers make eager fan-out impractical). For our code guide, we implement the core patterns at a manageable scale that can be extended to production.

---

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Clients    │
                        │ (Mobile/Web) │
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
                │  POST /posts      → create   │
                │  GET  /feed       → timeline │
                │  POST /follow     → follow   │
                │  POST /like       → like     │
                └──┬──────┬──────┬─────────┬──┘
                   │      │      │         │
            ┌──────▼──┐ ┌─▼────┐ ┌▼──────┐ ┌▼───────────┐
            │  Redis  │ │Postgr│ │Cloud  │ │  Celery    │
            │ (Cache +│ │eSQL  │ │Storage│ │ (Workers)  │
            │  Feed   │ │(Data)│ │(Media)│ │ + RabbitMQ │
            │  Store) │ │      │ │ + CDN │ │            │
            └─────────┘ └──────┘ └───────┘ └────────────┘
```

When a user creates a post, the API stores metadata in PostgreSQL and uploads media to Cloud Storage. A Celery background worker then performs **fan-out-on-write**: it looks up the poster's followers and inserts the post ID into each follower's feed list in Redis. When a user loads their feed, the API reads pre-computed post IDs from Redis, fetches full post data, and returns a paginated response. For celebrity accounts (> 10K followers), fan-out is skipped at write time; instead their posts are merged at read time.

---

## Project Setup

### Project Structure

```
instagram-feed/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── cache.py              # Redis cache layer
│   ├── feed.py               # Feed generation logic (fan-out)
│   ├── media.py              # Media upload to Cloud Storage
│   ├── tasks.py              # Celery background tasks
│   └── routers/
│       ├── __init__.py
│       ├── users.py          # Follow/unfollow endpoints
│       ├── posts.py          # Post creation & retrieval
│       ├── feed.py           # Feed endpoints
│       └── interactions.py   # Like & comment endpoints
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_posts.py         # Tests for post creation
│   ├── test_feed.py          # Tests for feed generation
│   └── test_follow.py        # Tests for follow system
├── celery_worker.py          # Celery worker entry point
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir instagram-feed && cd instagram-feed
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
celery==5.4.0
python-dotenv==1.0.1
python-multipart==0.0.12
google-cloud-storage==2.18.2
httpx==0.27.2
pytest==8.3.3
pytest-asyncio==0.24.0
Pillow==10.4.0
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for Redis. `celery` is the distributed task queue for background fan-out processing. `python-multipart` is required for file upload handling in FastAPI. `google-cloud-storage` is the GCP SDK for uploading media. `Pillow` handles image processing and thumbnail generation.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/instagram
REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
GCS_BUCKET_NAME=instagram-media-bucket
CDN_BASE_URL=https://cdn.yourdomain.com
CELEBRITY_THRESHOLD=10000
FEED_PAGE_SIZE=20
FEED_CACHE_SIZE=500
RATE_LIMIT_PER_MINUTE=120
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance used for caching and feed storage. `CELERY_BROKER_URL` points to RabbitMQ for task queuing. `GCS_BUCKET_NAME` is the Cloud Storage bucket for media files. `CDN_BASE_URL` is the public CDN domain for serving media. `CELEBRITY_THRESHOLD` defines the follower count above which fan-out-on-write is skipped. `FEED_CACHE_SIZE` limits how many post IDs are stored per user's feed in Redis.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐
│              users                    │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ username      VARCHAR(30) UNIQUE      │
│ display_name  VARCHAR(100)            │
│ bio           TEXT                     │
│ avatar_url    TEXT                     │
│ follower_count  BIGINT   DEFAULT 0    │
│ following_count BIGINT   DEFAULT 0    │
│ post_count    BIGINT     DEFAULT 0    │
│ is_celebrity  BOOLEAN    DEFAULT FALSE│
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_username (username)         │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│            follows                    │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ follower_id   BIGINT     FK → users   │
│ following_id  BIGINT     FK → users   │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ UNIQUE (follower_id, following_id)    │
│ INDEX idx_follower (follower_id)      │
│ INDEX idx_following (following_id)    │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│              posts                    │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ user_id       BIGINT     FK → users   │
│ caption       TEXT                     │
│ media_url     TEXT       NOT NULL     │
│ media_type    VARCHAR(10) DEFAULT img │
│ like_count    BIGINT     DEFAULT 0    │
│ comment_count BIGINT     DEFAULT 0    │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_user_created (user_id,      │
│        created_at DESC)               │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│              likes                    │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ user_id       BIGINT     FK → users   │
│ post_id       BIGINT     FK → posts   │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ UNIQUE (user_id, post_id)             │
│ INDEX idx_post_likes (post_id)        │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│            comments                   │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ user_id       BIGINT     FK → users   │
│ post_id       BIGINT     FK → posts   │
│ text          TEXT       NOT NULL     │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_post_comments (post_id,     │
│        created_at DESC)               │
└──────────────────────────────────────┘
```

The `users` table stores profiles with denormalized counters (`follower_count`, `following_count`, `post_count`) to avoid expensive COUNT queries on every profile view. `is_celebrity` is set to `true` when `follower_count` exceeds the threshold, changing the fan-out strategy. The `follows` table has a composite unique constraint preventing duplicate follows. The `posts` table stores metadata; actual media lives in Cloud Storage. A composite index on `(user_id, created_at DESC)` speeds up loading a user's profile page. The `likes` table has a unique constraint on `(user_id, post_id)` preventing double-likes.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/instagram"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "amqp://guest:guest@localhost:5672//"
    gcs_bucket_name: str = "instagram-media-bucket"
    cdn_base_url: str = "https://cdn.yourdomain.com"
    celebrity_threshold: int = 10000
    feed_page_size: int = 20
    feed_cache_size: int = 500
    rate_limit_per_minute: int = 120

    class Config:
        env_file = ".env"


settings = Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads values from environment variables and `.env` files. Field names are case-insensitive and map to the corresponding `ENV_VAR` names. `celebrity_threshold` controls when a user is treated as a celebrity for fan-out decisions.

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

`pool_size=20` keeps 20 connections open and reusable. `max_overflow=10` allows up to 10 additional connections under burst traffic. `pool_pre_ping=True` sends a lightweight query before each checkout to detect stale connections, which is important for cloud databases that may close idle connections. `get_db()` is a FastAPI dependency that yields a session and ensures it is closed after the request.

### `app/models.py`

```python
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey,
    Index, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import relationship

from app.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    username = Column(String(30), unique=True, nullable=False, index=True)
    display_name = Column(String(100), nullable=False)
    bio = Column(Text, nullable=True)
    avatar_url = Column(Text, nullable=True)
    follower_count = Column(BigInteger, default=0)
    following_count = Column(BigInteger, default=0)
    post_count = Column(BigInteger, default=0)
    is_celebrity = Column(Boolean, default=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    posts = relationship("Post", back_populates="author")
    likes = relationship("Like", back_populates="user")
    comments = relationship("Comment", back_populates="user")


class Follow(Base):
    __tablename__ = "follows"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    follower_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    following_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    follower = relationship("User", foreign_keys=[follower_id])
    following = relationship("User", foreign_keys=[following_id])

    __table_args__ = (
        UniqueConstraint("follower_id", "following_id", name="uq_follow"),
        Index("idx_follower", "follower_id"),
        Index("idx_following", "following_id"),
    )


class Post(Base):
    __tablename__ = "posts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    caption = Column(Text, nullable=True)
    media_url = Column(Text, nullable=False)
    media_type = Column(String(10), default="image")
    like_count = Column(BigInteger, default=0)
    comment_count = Column(BigInteger, default=0)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    author = relationship("User", back_populates="posts")
    likes = relationship("Like", back_populates="post")
    comments = relationship("Comment", back_populates="post")

    __table_args__ = (
        Index("idx_user_created", "user_id", "created_at"),
    )


class Like(Base):
    __tablename__ = "likes"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    post_id = Column(BigInteger, ForeignKey("posts.id"), nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    user = relationship("User", back_populates="likes")
    post = relationship("Post", back_populates="likes")

    __table_args__ = (
        UniqueConstraint("user_id", "post_id", name="uq_user_post_like"),
        Index("idx_post_likes", "post_id"),
    )


class Comment(Base):
    __tablename__ = "comments"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    post_id = Column(BigInteger, ForeignKey("posts.id"), nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    user = relationship("User", back_populates="comments")
    post = relationship("Post", back_populates="comments")

    __table_args__ = (
        Index("idx_post_comments", "post_id", "created_at"),
    )
```

`BigInteger` for all IDs supports billions of rows. `UniqueConstraint("follower_id", "following_id")` prevents duplicate follows at the database level. `UniqueConstraint("user_id", "post_id")` on the `likes` table prevents double-likes. `relationship()` with `foreign_keys` on `Follow` explicitly tells SQLAlchemy which column to use for the join, since both `follower_id` and `following_id` reference `users.id`. Composite indexes like `(user_id, created_at)` on `posts` enable efficient range scans for a user's chronologically ordered posts.

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=30)
    display_name: str = Field(min_length=1, max_length=100)
    bio: str | None = None


class UserResponse(BaseModel):
    id: int
    username: str
    display_name: str
    bio: str | None
    avatar_url: str | None
    follower_count: int
    following_count: int
    post_count: int
    created_at: datetime

    class Config:
        from_attributes = True


class FollowRequest(BaseModel):
    target_user_id: int


class PostCreate(BaseModel):
    caption: str | None = None


class PostResponse(BaseModel):
    id: int
    user_id: int
    caption: str | None
    media_url: str
    media_type: str
    like_count: int
    comment_count: int
    created_at: datetime
    author: UserResponse | None = None

    class Config:
        from_attributes = True


class FeedPostResponse(BaseModel):
    """Post as it appears in the feed, with author info."""
    id: int
    user_id: int
    caption: str | None
    media_url: str
    media_type: str
    like_count: int
    comment_count: int
    created_at: datetime
    author_username: str
    author_display_name: str
    author_avatar_url: str | None
    is_liked_by_viewer: bool = False

    class Config:
        from_attributes = True


class CommentCreate(BaseModel):
    text: str = Field(min_length=1, max_length=2000)


class CommentResponse(BaseModel):
    id: int
    user_id: int
    post_id: int
    text: str
    created_at: datetime
    author_username: str | None = None

    class Config:
        from_attributes = True


class PaginatedFeed(BaseModel):
    posts: list[FeedPostResponse]
    next_cursor: str | None                   # Opaque cursor for next page
    has_more: bool
```

`FeedPostResponse` flattens the author's information into the post object, avoiding nested joins in the API response. `is_liked_by_viewer` tells the client whether to render a filled or outline heart icon. `PaginatedFeed` uses cursor-based pagination with an opaque `next_cursor` string instead of offset-based pagination, which is more efficient for large datasets because it avoids counting skipped rows. `Field(min_length=1, max_length=2000)` on comments prevents empty submissions and excessively long texts.

---

## User & Follow System

### `app/routers/users.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import User, Follow
from app.schemas import UserCreate, UserResponse, FollowRequest
from app.cache import invalidate_feed_cache

router = APIRouter()


@router.post("/users", response_model=UserResponse, status_code=201)
def create_user(payload: UserCreate, db: Session = Depends(get_db)):
    """Register a new user."""
    existing = db.query(User).filter(User.username == payload.username).first()
    if existing:
        raise HTTPException(status_code=409, detail="Username already taken.")

    user = User(
        username=payload.username,
        display_name=payload.display_name,
        bio=payload.bio,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.get("/users/{username}", response_model=UserResponse)
def get_user(username: str, db: Session = Depends(get_db)):
    """Get a user's profile."""
    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    return user


@router.post("/users/{user_id}/follow", status_code=201)
def follow_user(
    user_id: int,
    payload: FollowRequest,
    db: Session = Depends(get_db),
):
    """Follow another user."""
    if user_id == payload.target_user_id:
        raise HTTPException(status_code=400, detail="Cannot follow yourself.")

    follower = db.query(User).filter(User.id == user_id).first()
    target = db.query(User).filter(User.id == payload.target_user_id).first()

    if not follower or not target:
        raise HTTPException(status_code=404, detail="User not found.")

    existing = (
        db.query(Follow)
        .filter(
            Follow.follower_id == user_id,
            Follow.following_id == payload.target_user_id,
        )
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="Already following this user.")

    follow = Follow(follower_id=user_id, following_id=payload.target_user_id)
    db.add(follow)

    # Update denormalized counters
    follower.following_count += 1
    target.follower_count += 1

    # Update celebrity status
    if target.follower_count >= settings.celebrity_threshold:
        target.is_celebrity = True

    db.commit()

    # Invalidate the follower's feed cache (they now need to see the target's posts)
    invalidate_feed_cache(user_id)

    return {"message": f"Now following {target.username}."}


@router.delete("/users/{user_id}/follow/{target_id}", status_code=200)
def unfollow_user(user_id: int, target_id: int, db: Session = Depends(get_db)):
    """Unfollow a user."""
    follow = (
        db.query(Follow)
        .filter(Follow.follower_id == user_id, Follow.following_id == target_id)
        .first()
    )
    if not follow:
        raise HTTPException(status_code=404, detail="Not following this user.")

    follower = db.query(User).filter(User.id == user_id).first()
    target = db.query(User).filter(User.id == target_id).first()

    db.delete(follow)
    follower.following_count -= 1
    target.follower_count -= 1

    # Recheck celebrity status
    if target.follower_count < settings.celebrity_threshold:
        target.is_celebrity = False

    db.commit()

    # Invalidate the follower's feed cache
    invalidate_feed_cache(user_id)

    return {"message": f"Unfollowed {target.username}."}


@router.get("/users/{user_id}/followers", response_model=list[UserResponse])
def get_followers(
    user_id: int,
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """Get a user's followers."""
    followers = (
        db.query(User)
        .join(Follow, Follow.follower_id == User.id)
        .filter(Follow.following_id == user_id)
        .offset(skip)
        .limit(limit)
        .all()
    )
    return followers


@router.get("/users/{user_id}/following", response_model=list[UserResponse])
def get_following(
    user_id: int,
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """Get users that a user is following."""
    following = (
        db.query(User)
        .join(Follow, Follow.following_id == User.id)
        .filter(Follow.follower_id == user_id)
        .offset(skip)
        .limit(limit)
        .all()
    )
    return following
```

`follow_user` checks for self-follows and duplicate follows before creating the relationship. Denormalized counters (`follower_count`, `following_count`) are updated in the same transaction to maintain consistency. When a user's follower count crosses the `celebrity_threshold`, the `is_celebrity` flag is set, which changes the fan-out strategy for their future posts. `invalidate_feed_cache` clears the follower's cached feed so the next feed request reflects the new follow relationship. `get_followers` uses a JOIN through the `follows` table to retrieve actual `User` objects, not just IDs.

---

## Post Creation & Media Upload

### `app/media.py`

```python
import uuid
from io import BytesIO

from PIL import Image
from google.cloud import storage

from app.config import settings


def get_storage_client():
    """Create a GCS client (uses Application Default Credentials)."""
    return storage.Client()


def upload_media(file_content: bytes, content_type: str) -> str:
    """
    Upload media to Google Cloud Storage and return the CDN URL.
    Generates a unique filename to prevent collisions.
    """
    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)

    # Generate unique filename
    ext = content_type.split("/")[-1]             # e.g., "jpeg", "png", "mp4"
    filename = f"posts/{uuid.uuid4().hex}.{ext}"

    blob = bucket.blob(filename)
    blob.upload_from_string(file_content, content_type=content_type)
    blob.make_public()

    return f"{settings.cdn_base_url}/{filename}"


def create_thumbnail(file_content: bytes, size: tuple = (300, 300)) -> bytes:
    """Create a thumbnail from an image."""
    image = Image.open(BytesIO(file_content))
    image.thumbnail(size, Image.Resampling.LANCZOS)

    output = BytesIO()
    image.save(output, format="JPEG", quality=85)
    output.seek(0)

    return output.read()


def upload_with_thumbnail(file_content: bytes, content_type: str) -> dict:
    """Upload both the original image and a thumbnail."""
    original_url = upload_media(file_content, content_type)

    thumbnail_url = None
    if content_type.startswith("image/"):
        thumbnail_bytes = create_thumbnail(file_content)
        thumbnail_url = upload_media(thumbnail_bytes, "image/jpeg")

    return {"original_url": original_url, "thumbnail_url": thumbnail_url}
```

`uuid.uuid4().hex` generates a 32-character random hex string, ensuring globally unique filenames without coordination. The `posts/` prefix organizes media in a predictable directory structure within the bucket. `blob.make_public()` makes the file accessible via a public URL; in production, use signed URLs or IAM-based access with a CDN. `Image.thumbnail()` from Pillow resizes the image in-place while maintaining aspect ratio, using `LANCZOS` resampling for high-quality downscaling. Thumbnails are always saved as JPEG at 85% quality for a good balance of size and visual fidelity.

### `app/routers/posts.py`

```python
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import Post, User
from app.schemas import PostResponse
from app.media import upload_with_thumbnail
from app.tasks import fan_out_post

router = APIRouter()

ALLOWED_MEDIA_TYPES = {"image/jpeg", "image/png", "image/webp", "video/mp4"}
MAX_FILE_SIZE = 10 * 1024 * 1024              # 10 MB


@router.post("/posts", response_model=PostResponse, status_code=201)
async def create_post(
    user_id: int = Form(...),
    caption: str = Form(None),
    media: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """Create a new post with media upload."""
    # Validate user exists
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    # Validate media type
    if media.content_type not in ALLOWED_MEDIA_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported media type. Allowed: {ALLOWED_MEDIA_TYPES}",
        )

    # Read and validate file size
    file_content = await media.read()
    if len(file_content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Max 10 MB.")

    # Upload to Cloud Storage
    media_urls = upload_with_thumbnail(file_content, media.content_type)
    media_type = "video" if media.content_type.startswith("video/") else "image"

    # Create post record
    post = Post(
        user_id=user_id,
        caption=caption,
        media_url=media_urls["original_url"],
        media_type=media_type,
    )
    db.add(post)

    # Update user's post count
    user.post_count += 1

    db.commit()
    db.refresh(post)

    # Trigger async fan-out to followers' feeds
    fan_out_post.delay(post.id, user_id)

    return post


@router.get("/posts/{post_id}", response_model=PostResponse)
def get_post(post_id: int, db: Session = Depends(get_db)):
    """Get a single post by ID."""
    post = db.query(Post).filter(Post.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found.")
    return post


@router.get("/users/{user_id}/posts", response_model=list[PostResponse])
def get_user_posts(
    user_id: int,
    skip: int = 0,
    limit: int = 20,
    db: Session = Depends(get_db),
):
    """Get a user's posts (profile page)."""
    posts = (
        db.query(Post)
        .filter(Post.user_id == user_id)
        .order_by(Post.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    return posts


@router.delete("/posts/{post_id}", status_code=200)
def delete_post(post_id: int, user_id: int, db: Session = Depends(get_db)):
    """Delete a post (only by the author)."""
    post = db.query(Post).filter(Post.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found.")

    if post.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized to delete.")

    user = db.query(User).filter(User.id == user_id).first()
    user.post_count -= 1

    db.delete(post)
    db.commit()

    return {"message": "Post deleted."}
```

`Form(...)` and `File(...)` are used instead of JSON body because file uploads require `multipart/form-data` encoding. `await media.read()` reads the entire file into memory; for very large files, you would stream chunks instead. `fan_out_post.delay(post.id, user_id)` sends the fan-out task to Celery asynchronously, so the API returns immediately without waiting for the fan-out to complete. The post is committed to PostgreSQL first, ensuring durability even if the fan-out worker is temporarily down. `order_by(Post.created_at.desc())` with the composite index on `(user_id, created_at)` provides efficient reverse-chronological ordering for the profile page.

---

## News Feed Generation (Fan-Out)

The feed is the core of Instagram's design. There are two fundamental approaches:

### Fan-Out Strategy Comparison

| Strategy          | How It Works                                       | Pros                           | Cons                                           |
| ----------------- | -------------------------------------------------- | ------------------------------ | ---------------------------------------------- |
| Fan-out-on-write  | On post create, push to all followers' feed caches | Fast reads, simple feed query  | Slow writes for celebrities, wasted space       |
| Fan-out-on-read   | On feed load, pull posts from all followed users   | No write amplification         | Slow reads, complex merge at query time         |
| Hybrid            | Write for regular users, read for celebrities      | Balances both                  | More complex implementation                    |

We implement the **hybrid approach**: regular users (< 10K followers) use fan-out-on-write, celebrity accounts use fan-out-on-read.

### `app/feed.py`

```python
import json
import time

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.cache import redis_client
from app.config import settings
from app.models import Follow, Like, Post, User


def get_follower_ids(user_id: int, db: Session) -> list[int]:
    """Get all follower IDs for a user."""
    followers = (
        db.query(Follow.follower_id)
        .filter(Follow.following_id == user_id)
        .all()
    )
    return [f.follower_id for f in followers]


def get_celebrity_following_ids(user_id: int, db: Session) -> list[int]:
    """Get IDs of celebrities that a user follows."""
    celebrity_ids = (
        db.query(Follow.following_id)
        .join(User, User.id == Follow.following_id)
        .filter(Follow.follower_id == user_id)
        .filter(User.is_celebrity == True)
        .all()
    )
    return [c.following_id for c in celebrity_ids]


def push_to_feed(follower_id: int, post_id: int, post_timestamp: float):
    """Push a post ID into a follower's feed in Redis (sorted set)."""
    key = f"feed:{follower_id}"

    # Add post to sorted set with timestamp as score
    redis_client.zadd(key, {str(post_id): post_timestamp})

    # Trim to keep only the most recent N posts
    redis_client.zremrangebyrank(key, 0, -(settings.feed_cache_size + 1))

    # Set TTL on the feed key (7 days)
    redis_client.expire(key, 7 * 24 * 3600)


def fan_out_to_followers(post_id: int, author_id: int, db: Session):
    """
    Fan-out-on-write: push post to all followers' feeds.
    Skipped for celebrity accounts.
    """
    author = db.query(User).filter(User.id == author_id).first()

    # Skip fan-out for celebrities (handled at read time)
    if author.is_celebrity:
        return

    follower_ids = get_follower_ids(author_id, db)
    post_timestamp = time.time()

    for follower_id in follower_ids:
        push_to_feed(follower_id, post_id, post_timestamp)


def get_feed_post_ids(user_id: int, cursor: float | None, limit: int) -> list[int]:
    """
    Get post IDs from the user's pre-computed feed in Redis.
    Uses cursor-based pagination with timestamps as cursors.
    """
    key = f"feed:{user_id}"

    if cursor is None:
        cursor = "+inf"                       # Start from the most recent

    # Get post IDs with scores less than the cursor, in reverse order
    results = redis_client.zrevrangebyscore(
        key,
        max=f"({cursor}" if cursor != "+inf" else "+inf",
        min="-inf",
        start=0,
        num=limit,
        withscores=True,
    )

    return [(int(post_id), score) for post_id, score in results]


def merge_celebrity_posts(
    user_id: int,
    celebrity_ids: list[int],
    cursor: float | None,
    limit: int,
    db: Session,
) -> list[tuple[int, float]]:
    """
    Fan-out-on-read: fetch recent posts from celebrities at read time
    and merge them with the pre-computed feed.
    """
    if not celebrity_ids:
        return []

    query = (
        db.query(Post.id, Post.created_at)
        .filter(Post.user_id.in_(celebrity_ids))
        .order_by(desc(Post.created_at))
        .limit(limit)
    )

    if cursor:
        from datetime import datetime, timezone
        cursor_dt = datetime.fromtimestamp(cursor, tz=timezone.utc)
        query = query.filter(Post.created_at < cursor_dt)

    results = query.all()
    return [(post.id, post.created_at.timestamp()) for post in results]


def build_feed(user_id: int, cursor: str | None, db: Session) -> dict:
    """
    Build the complete feed by merging pre-computed entries with
    celebrity posts fetched at read time.
    """
    limit = settings.feed_page_size
    cursor_float = float(cursor) if cursor else None

    # 1. Get pre-computed feed entries (fan-out-on-write results)
    feed_entries = get_feed_post_ids(user_id, cursor_float, limit)

    # 2. Get celebrity posts (fan-out-on-read)
    celebrity_ids = get_celebrity_following_ids(user_id, db)
    celebrity_entries = merge_celebrity_posts(
        user_id, celebrity_ids, cursor_float, limit, db
    )

    # 3. Merge and sort by timestamp (descending)
    all_entries = feed_entries + celebrity_entries
    all_entries.sort(key=lambda x: x[1], reverse=True)

    # 4. Take only the top `limit` entries
    page_entries = all_entries[:limit]

    if not page_entries:
        return {"post_ids": [], "next_cursor": None, "has_more": False}

    post_ids = [entry[0] for entry in page_entries]
    last_score = page_entries[-1][1]
    has_more = len(all_entries) > limit

    # 5. Fetch full post data with author info
    posts = (
        db.query(Post)
        .filter(Post.id.in_(post_ids))
        .all()
    )

    # Maintain the sorted order
    post_map = {p.id: p for p in posts}
    ordered_posts = [post_map[pid] for pid in post_ids if pid in post_map]

    # 6. Check which posts the viewer has liked
    liked_post_ids = get_liked_post_ids(user_id, post_ids, db)

    # 7. Fetch author info
    author_ids = list({p.user_id for p in ordered_posts})
    authors = db.query(User).filter(User.id.in_(author_ids)).all()
    author_map = {a.id: a for a in authors}

    feed_posts = []
    for post in ordered_posts:
        author = author_map.get(post.user_id)
        feed_posts.append({
            "id": post.id,
            "user_id": post.user_id,
            "caption": post.caption,
            "media_url": post.media_url,
            "media_type": post.media_type,
            "like_count": post.like_count,
            "comment_count": post.comment_count,
            "created_at": post.created_at,
            "author_username": author.username if author else "unknown",
            "author_display_name": author.display_name if author else "Unknown",
            "author_avatar_url": author.avatar_url if author else None,
            "is_liked_by_viewer": post.id in liked_post_ids,
        })

    return {
        "posts": feed_posts,
        "next_cursor": str(last_score) if has_more else None,
        "has_more": has_more,
    }


def get_liked_post_ids(user_id: int, post_ids: list[int], db: Session) -> set[int]:
    """Check which posts in the list the user has liked."""
    if not post_ids:
        return set()

    liked = (
        db.query(Like.post_id)
        .filter(Like.user_id == user_id)
        .filter(Like.post_id.in_(post_ids))
        .all()
    )
    return {l.post_id for l in liked}
```

This is the heart of the system. `push_to_feed` uses a Redis sorted set where the score is the post's timestamp, enabling efficient chronological ordering and cursor-based pagination. `zremrangebyrank` trims the set to the most recent `FEED_CACHE_SIZE` entries, bounding memory usage per user. `fan_out_to_followers` iterates through all follower IDs and pushes the post; for a user with 1000 followers, this creates 1000 Redis writes, which is why it runs asynchronously via Celery. `build_feed` implements the hybrid merge: it fetches pre-computed entries from Redis (step 1), queries PostgreSQL for celebrity posts (step 2), merges them by timestamp (step 3), and enriches with author data (step 5). `get_liked_post_ids` uses an `IN` query to batch-check likes instead of querying one post at a time. The `(` prefix in `zrevrangebyscore` makes the cursor exclusive, preventing the last post from the previous page from appearing again.

---

## Feed API & Pagination

### `app/routers/feed.py`

```python
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.feed import build_feed
from app.schemas import PaginatedFeed

router = APIRouter()


@router.get("/feed/{user_id}", response_model=PaginatedFeed)
def get_feed(
    user_id: int,
    cursor: str | None = Query(None),
    db: Session = Depends(get_db),
):
    """
    Get a user's news feed with cursor-based pagination.

    The cursor is a timestamp string. Pass the `next_cursor` from the
    previous response to load the next page. Omit `cursor` for the
    first page.
    """
    result = build_feed(user_id, cursor, db)

    return PaginatedFeed(
        posts=result["posts"],
        next_cursor=result.get("next_cursor"),
        has_more=result.get("has_more", False),
    )
```

Cursor-based pagination is superior to offset-based (`?page=2&size=20`) for feeds because: (1) new posts inserted at the top do not shift items, preventing duplicates or missed posts when scrolling; (2) it is more efficient in the database since it uses index range scans instead of counting and skipping rows; (3) the cursor is opaque to the client, so the server can change the underlying implementation without breaking clients. The client simply passes the `next_cursor` from the previous response to get the next page.

---

## Likes & Comments

### `app/routers/interactions.py`

```python
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Comment, Like, Post, User
from app.schemas import CommentCreate, CommentResponse
from app.cache import invalidate_post_cache

router = APIRouter()


@router.post("/posts/{post_id}/like", status_code=201)
def like_post(post_id: int, user_id: int, db: Session = Depends(get_db)):
    """Like a post."""
    post = db.query(Post).filter(Post.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found.")

    existing = (
        db.query(Like)
        .filter(Like.user_id == user_id, Like.post_id == post_id)
        .first()
    )
    if existing:
        raise HTTPException(status_code=409, detail="Already liked.")

    like = Like(user_id=user_id, post_id=post_id)
    db.add(like)
    post.like_count += 1
    db.commit()

    invalidate_post_cache(post_id)

    return {"message": "Post liked.", "like_count": post.like_count}


@router.delete("/posts/{post_id}/like", status_code=200)
def unlike_post(post_id: int, user_id: int, db: Session = Depends(get_db)):
    """Unlike a post."""
    like = (
        db.query(Like)
        .filter(Like.user_id == user_id, Like.post_id == post_id)
        .first()
    )
    if not like:
        raise HTTPException(status_code=404, detail="Like not found.")

    post = db.query(Post).filter(Post.id == post_id).first()
    db.delete(like)
    post.like_count -= 1
    db.commit()

    invalidate_post_cache(post_id)

    return {"message": "Like removed.", "like_count": post.like_count}


@router.post("/posts/{post_id}/comments", response_model=CommentResponse, status_code=201)
def add_comment(
    post_id: int,
    user_id: int,
    payload: CommentCreate,
    db: Session = Depends(get_db),
):
    """Add a comment to a post."""
    post = db.query(Post).filter(Post.id == post_id).first()
    if not post:
        raise HTTPException(status_code=404, detail="Post not found.")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    comment = Comment(user_id=user_id, post_id=post_id, text=payload.text)
    db.add(comment)
    post.comment_count += 1
    db.commit()
    db.refresh(comment)

    return CommentResponse(
        id=comment.id,
        user_id=comment.user_id,
        post_id=comment.post_id,
        text=comment.text,
        created_at=comment.created_at,
        author_username=user.username,
    )


@router.get("/posts/{post_id}/comments", response_model=list[CommentResponse])
def get_comments(
    post_id: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get comments for a post, ordered by newest first."""
    comments = (
        db.query(Comment)
        .filter(Comment.post_id == post_id)
        .order_by(Comment.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    user_ids = list({c.user_id for c in comments})
    users = db.query(User).filter(User.id.in_(user_ids)).all()
    user_map = {u.id: u for u in users}

    return [
        CommentResponse(
            id=c.id,
            user_id=c.user_id,
            post_id=c.post_id,
            text=c.text,
            created_at=c.created_at,
            author_username=user_map.get(c.user_id, User()).username,
        )
        for c in comments
    ]
```

`like_post` uses the unique constraint on `(user_id, post_id)` to prevent double-likes, with an application-level check to return a clear error message. The denormalized `like_count` on the `Post` model is updated atomically in the same transaction. `invalidate_post_cache` clears cached post data so subsequent reads reflect the new like count. `get_comments` batch-fetches user info with `User.id.in_(user_ids)` to avoid an N+1 query problem where each comment would trigger a separate database query for its author.

---

## Caching with Redis

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

POST_CACHE_TTL = 3600                         # 1 hour
USER_CACHE_TTL = 1800                         # 30 minutes


def get_cached_post(post_id: int) -> dict | None:
    """Retrieve a post from Redis cache."""
    data = redis_client.get(f"post:{post_id}")
    if data:
        return json.loads(data)
    return None


def set_cached_post(post_id: int, post_data: dict):
    """Store a post in Redis cache."""
    redis_client.setex(
        f"post:{post_id}",
        POST_CACHE_TTL,
        json.dumps(post_data, default=str),
    )


def invalidate_post_cache(post_id: int):
    """Remove a post from cache (used when likes/comments change)."""
    redis_client.delete(f"post:{post_id}")


def get_cached_user(user_id: int) -> dict | None:
    """Retrieve user profile from cache."""
    data = redis_client.get(f"user:{user_id}")
    if data:
        return json.loads(data)
    return None


def set_cached_user(user_id: int, user_data: dict):
    """Store user profile in cache."""
    redis_client.setex(
        f"user:{user_id}",
        USER_CACHE_TTL,
        json.dumps(user_data, default=str),
    )


def invalidate_feed_cache(user_id: int):
    """
    Clear a user's pre-computed feed cache.
    Called when follow/unfollow changes occur.
    """
    redis_client.delete(f"feed:{user_id}")


def get_feed_size(user_id: int) -> int:
    """Get the number of entries in a user's feed cache."""
    return redis_client.zcard(f"feed:{user_id}")
```

`redis.from_url` parses the connection string and creates a connection pool automatically. `decode_responses=True` makes Redis return Python strings instead of bytes. `setex` sets a key with a TTL; after expiration, Redis automatically deletes the key. `json.dumps(post_data, default=str)` handles `datetime` objects by converting them to strings, avoiding serialization errors. `invalidate_feed_cache` deletes the entire feed sorted set, forcing a full rebuild on the next feed request; this is acceptable because follow/unfollow is infrequent compared to feed reads. `zcard` returns the cardinality of a sorted set in O(1) time.

---

## Background Workers with Celery

### `celery_worker.py`

```python
from celery import Celery

from app.config import settings

celery_app = Celery(
    "instagram_feed",
    broker=settings.celery_broker_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,                      # Acknowledge after task completes
    worker_prefetch_multiplier=1,             # Fetch one task at a time
    task_default_queue="feed_fanout",
)
```

`task_acks_late=True` ensures that if a worker crashes mid-task, the message is re-delivered to another worker instead of being lost. `worker_prefetch_multiplier=1` prevents a single worker from grabbing too many tasks at once, ensuring fair distribution across workers. `task_default_queue` isolates feed fan-out tasks to a dedicated queue so they do not compete with other task types.

### `app/tasks.py`

```python
import time

from celery_worker import celery_app
from app.database import SessionLocal
from app.feed import fan_out_to_followers, push_to_feed


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    name="fan_out_post",
)
def fan_out_post(self, post_id: int, author_id: int):
    """
    Async task to fan out a new post to all followers' feeds.
    Runs in a Celery worker, not in the API process.
    """
    db = SessionLocal()
    try:
        fan_out_to_followers(post_id, author_id, db)
    except Exception as exc:
        db.rollback()
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    name="backfill_feed",
)
def backfill_feed(self, user_id: int):
    """
    Rebuild a user's feed cache from scratch.
    Used when a user's feed cache has expired or after a follow.
    """
    from app.models import Follow, Post, User

    db = SessionLocal()
    try:
        # Get all non-celebrity users that this user follows
        following = (
            db.query(Follow.following_id)
            .join(User, User.id == Follow.following_id)
            .filter(Follow.follower_id == user_id)
            .filter(User.is_celebrity == False)
            .all()
        )
        following_ids = [f.following_id for f in following]

        if not following_ids:
            return

        # Fetch recent posts from followed users
        from app.config import settings
        posts = (
            db.query(Post)
            .filter(Post.user_id.in_(following_ids))
            .order_by(Post.created_at.desc())
            .limit(settings.feed_cache_size)
            .all()
        )

        # Push each post to the user's feed cache
        for post in posts:
            push_to_feed(user_id, post.id, post.created_at.timestamp())

    except Exception as exc:
        db.rollback()
        raise self.retry(exc=exc)
    finally:
        db.close()


@celery_app.task(name="sync_counters")
def sync_counters():
    """
    Periodic task to sync like/comment counts from the database.
    Ensures denormalized counters stay accurate.
    """
    from sqlalchemy import func
    from app.models import Like, Comment, Post

    db = SessionLocal()
    try:
        # Sync like counts
        like_counts = (
            db.query(Like.post_id, func.count().label("count"))
            .group_by(Like.post_id)
            .all()
        )
        for post_id, count in like_counts:
            db.query(Post).filter(Post.id == post_id).update(
                {"like_count": count}
            )

        # Sync comment counts
        comment_counts = (
            db.query(Comment.post_id, func.count().label("count"))
            .group_by(Comment.post_id)
            .all()
        )
        for post_id, count in comment_counts:
            db.query(Post).filter(Post.id == post_id).update(
                {"comment_count": count}
            )

        db.commit()
    finally:
        db.close()
```

`bind=True` gives the task access to `self`, enabling `self.retry(exc=exc)` for automatic retries with exponential backoff. `max_retries=3` caps retries to prevent infinite loops. `SessionLocal()` creates a new database session per task since Celery workers run in separate processes; the FastAPI `get_db` dependency is not available here. `backfill_feed` reconstructs a user's entire feed by querying all posts from followed non-celebrity users and pushing them to Redis, used when the feed cache expires or after a new follow. `sync_counters` is a periodic housekeeping task that recalculates `like_count` and `comment_count` from the source-of-truth tables, correcting any drift from race conditions or failed transactions.

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
      - DATABASE_URL=postgresql://user:pass@db:5432/instagram
      - REDIS_URL=redis://cache:6379/0
      - CELERY_BROKER_URL=amqp://guest:guest@broker:5672//
      - GCS_BUCKET_NAME=instagram-media-bucket
      - CDN_BASE_URL=http://localhost:8000/static
      - CELEBRITY_THRESHOLD=10000
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
      broker:
        condition: service_healthy
    restart: unless-stopped

  worker:
    build: .
    command: celery -A celery_worker worker --loglevel=info --concurrency=4 -Q feed_fanout
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/instagram
      - REDIS_URL=redis://cache:6379/0
      - CELERY_BROKER_URL=amqp://guest:guest@broker:5672//
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
      broker:
        condition: service_healthy
    restart: unless-stopped

  beat:
    build: .
    command: celery -A celery_worker beat --loglevel=info
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/instagram
      - REDIS_URL=redis://cache:6379/0
      - CELERY_BROKER_URL=amqp://guest:guest@broker:5672//
    depends_on:
      - broker
    restart: unless-stopped

  db:
    image: postgres:16
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
      - POSTGRES_DB=instagram
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d instagram"]
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

  broker:
    image: rabbitmq:3-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      - RABBITMQ_DEFAULT_USER=guest
      - RABBITMQ_DEFAULT_PASS=guest
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 10s
      retries: 5

volumes:
  db-data:
  redis-data:
```

`depends_on` with `condition: service_healthy` ensures the API and workers wait until PostgreSQL, Redis, and RabbitMQ pass their health checks before starting. The `worker` service runs Celery with `--concurrency=4`, spawning 4 worker processes to handle fan-out tasks in parallel. `-Q feed_fanout` tells the worker to consume only from the `feed_fanout` queue. The `beat` service runs Celery Beat, which schedules periodic tasks like `sync_counters`. `--maxmemory 256mb --maxmemory-policy allkeys-lru` on Redis caps memory usage and evicts the least recently used keys when the limit is reached, preventing OOM kills. `rabbitmq:3-management-alpine` includes the management UI at `http://localhost:15672` for monitoring queues.

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import users, posts, feed, interactions


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Instagram Feed API",
    description="A production-ready Instagram-like feed service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(users.router, prefix="/api", tags=["Users"])
app.include_router(posts.router, prefix="/api", tags=["Posts"])
app.include_router(feed.router, prefix="/api", tags=["Feed"])
app.include_router(interactions.router, prefix="/api", tags=["Interactions"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`lifespan` is an async context manager that creates tables on startup. `include_router` with `prefix="/api"` mounts all routes under `/api`, keeping the URL structure clean. The health check endpoint at `/health` is used by load balancers and container orchestrators to verify the service is running.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f api                    # Follow API logs
docker compose logs -f worker                 # Follow worker logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Create users
curl -X POST http://localhost:8000/api/users \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "display_name": "Alice"}'

curl -X POST http://localhost:8000/api/users \
  -H "Content-Type: application/json" \
  -d '{"username": "bob", "display_name": "Bob"}'

# Follow a user (Alice follows Bob)
curl -X POST http://localhost:8000/api/users/1/follow \
  -H "Content-Type: application/json" \
  -d '{"target_user_id": 2}'

# Create a post (Bob posts)
curl -X POST http://localhost:8000/api/posts \
  -F "user_id=2" \
  -F "caption=Hello world!" \
  -F "media=@photo.jpg"

# Get Alice's feed (should show Bob's post)
curl http://localhost:8000/api/feed/1

# Like a post
curl -X POST "http://localhost:8000/api/posts/1/like?user_id=1"

# Add a comment
curl -X POST "http://localhost:8000/api/posts/1/comments?user_id=1" \
  -H "Content-Type: application/json" \
  -d '{"text": "Great photo!"}'
```

---

## Testing

### `tests/conftest.py`

```python
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import patch, MagicMock

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

    # Mock Celery tasks so they run synchronously
    with patch("app.routers.posts.fan_out_post") as mock_fanout:
        mock_fanout.delay = MagicMock()
        yield TestClient(app)

    app.dependency_overrides.clear()
```

Tests use SQLite instead of PostgreSQL for speed and isolation. `dependency_overrides` is a FastAPI feature that lets you swap out dependencies during testing. The Celery `fan_out_post` task is mocked using `patch` so tests do not require a running RabbitMQ broker. `MagicMock()` on `.delay` makes the task a no-op during tests while allowing assertions on whether it was called.

### `tests/test_posts.py`

```python
from io import BytesIO
from unittest.mock import patch


def test_create_post(client, db):
    """Test creating a post with media."""
    # Create a user first
    client.post("/api/users", json={
        "username": "testuser",
        "display_name": "Test User",
    })

    # Mock media upload
    with patch("app.routers.posts.upload_with_thumbnail") as mock_upload:
        mock_upload.return_value = {
            "original_url": "https://cdn.example.com/test.jpg",
            "thumbnail_url": "https://cdn.example.com/test_thumb.jpg",
        }

        # Create post with a fake image
        response = client.post(
            "/api/posts",
            data={"user_id": "1", "caption": "Test post"},
            files={"media": ("test.jpg", BytesIO(b"fake-image"), "image/jpeg")},
        )

    assert response.status_code == 201
    data = response.json()
    assert data["caption"] == "Test post"
    assert data["media_url"] == "https://cdn.example.com/test.jpg"


def test_get_user_posts(client, db):
    """Test fetching a user's posts."""
    client.post("/api/users", json={
        "username": "poster",
        "display_name": "Poster",
    })

    with patch("app.routers.posts.upload_with_thumbnail") as mock_upload:
        mock_upload.return_value = {
            "original_url": "https://cdn.example.com/1.jpg",
            "thumbnail_url": None,
        }

        client.post(
            "/api/posts",
            data={"user_id": "1", "caption": "Post 1"},
            files={"media": ("1.jpg", BytesIO(b"img"), "image/jpeg")},
        )
        client.post(
            "/api/posts",
            data={"user_id": "1", "caption": "Post 2"},
            files={"media": ("2.jpg", BytesIO(b"img"), "image/jpeg")},
        )

    response = client.get("/api/users/1/posts")
    assert response.status_code == 200
    assert len(response.json()) == 2


def test_delete_post_unauthorized(client, db):
    """Test that only the author can delete a post."""
    client.post("/api/users", json={"username": "a", "display_name": "A"})
    client.post("/api/users", json={"username": "b", "display_name": "B"})

    with patch("app.routers.posts.upload_with_thumbnail") as mock_upload:
        mock_upload.return_value = {"original_url": "x", "thumbnail_url": None}
        client.post(
            "/api/posts",
            data={"user_id": "1", "caption": "Mine"},
            files={"media": ("x.jpg", BytesIO(b"img"), "image/jpeg")},
        )

    response = client.delete("/api/posts/1?user_id=2")
    assert response.status_code == 403
```

### `tests/test_follow.py`

```python
def test_follow_user(client, db):
    """Test following another user."""
    client.post("/api/users", json={"username": "alice", "display_name": "Alice"})
    client.post("/api/users", json={"username": "bob", "display_name": "Bob"})

    response = client.post(
        "/api/users/1/follow",
        json={"target_user_id": 2},
    )
    assert response.status_code == 201

    # Check follower count
    bob = client.get("/api/users/bob")
    assert bob.json()["follower_count"] == 1


def test_cannot_follow_self(client, db):
    """Test that a user cannot follow themselves."""
    client.post("/api/users", json={"username": "alice", "display_name": "Alice"})

    response = client.post(
        "/api/users/1/follow",
        json={"target_user_id": 1},
    )
    assert response.status_code == 400


def test_duplicate_follow_rejected(client, db):
    """Test that duplicate follows are rejected."""
    client.post("/api/users", json={"username": "alice", "display_name": "Alice"})
    client.post("/api/users", json={"username": "bob", "display_name": "Bob"})

    client.post("/api/users/1/follow", json={"target_user_id": 2})
    response = client.post("/api/users/1/follow", json={"target_user_id": 2})
    assert response.status_code == 409


def test_unfollow_user(client, db):
    """Test unfollowing a user."""
    client.post("/api/users", json={"username": "alice", "display_name": "Alice"})
    client.post("/api/users", json={"username": "bob", "display_name": "Bob"})

    client.post("/api/users/1/follow", json={"target_user_id": 2})
    response = client.delete("/api/users/1/follow/2")
    assert response.status_code == 200

    bob = client.get("/api/users/bob")
    assert bob.json()["follower_count"] == 0
```

### `tests/test_feed.py`

```python
from unittest.mock import patch, MagicMock
from io import BytesIO


def test_empty_feed(client, db):
    """Test that a new user has an empty feed."""
    client.post("/api/users", json={"username": "lonely", "display_name": "Lonely"})

    response = client.get("/api/feed/1")
    assert response.status_code == 200
    assert response.json()["posts"] == []
    assert response.json()["has_more"] is False


def test_feed_pagination(client, db):
    """Test cursor-based feed pagination."""
    client.post("/api/users", json={"username": "alice", "display_name": "Alice"})

    response = client.get("/api/feed/1")
    data = response.json()
    assert "next_cursor" in data
    assert "has_more" in data
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_follow"                       # Run only follow tests
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
gcloud sql instances create instagram-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-2-7680 \
  --region=us-central1 \
  --storage-size=50GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create instagram \
  --instance=instagram-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=instagram-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-2-7680` provisions 2 vCPUs and 7.5 GB RAM, suitable for moderate production traffic. `--availability-type=REGIONAL` enables high availability with automatic failover to a standby in a different zone. `--storage-auto-increase` automatically adds storage when the disk fills up.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance
gcloud redis instances create instagram-cache \
  --size=2 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD_HA

# Get the Redis host IP
gcloud redis instances describe instagram-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=2` provisions 2 GB of memory for feed caches. `--tier=STANDARD_HA` enables high availability with automatic failover. Memorystore is accessible only from within the same VPC, which secures it without additional firewall rules.

### Set Up Cloud Storage (Media)

```bash
# Create a bucket for media storage
gsutil mb -l us-central1 gs://YOUR_PROJECT_ID-instagram-media

# Enable CDN on the bucket (via Cloud CDN + Load Balancer)
# Or use signed URLs for private media access

# Set CORS for direct browser uploads
gsutil cors set cors.json gs://YOUR_PROJECT_ID-instagram-media
```

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/instagram-feed/api
```

`gcloud builds submit` uploads your source code and builds the Docker image using Cloud Build. The image is pushed directly to the registry without needing Docker installed locally.

### Deploy API to Cloud Run

```bash
gcloud run deploy instagram-feed \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/instagram-feed/api \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 20 \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:instagram-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/instagram?host=/cloudsql/YOUR_PROJECT_ID:us-central1:instagram-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
GCS_BUCKET_NAME=YOUR_PROJECT_ID-instagram-media,\
CDN_BASE_URL=https://cdn.yourdomain.com,\
CELEBRITY_THRESHOLD=10000,\
FEED_PAGE_SIZE=20,\
FEED_CACHE_SIZE=500"
```

`--min-instances 2` keeps at least two instances warm for low latency. `--max-instances 20` caps scaling to control costs. `--memory 1Gi` and `--cpu 2` provide enough resources for feed generation and media processing. `--add-cloudsql-instances` sets up the Cloud SQL Auth Proxy for secure database connections.

### Deploy Celery Workers on GCE

```bash
# Create a worker VM instance
gcloud compute instances create instagram-worker \
  --zone=us-central1-a \
  --machine-type=e2-standard-2 \
  --image-family=cos-stable \
  --image-project=cos-cloud \
  --metadata=startup-script='#!/bin/bash
docker pull us-central1-docker.pkg.dev/YOUR_PROJECT_ID/instagram-feed/api
docker run -d \
  -e DATABASE_URL="postgresql://postgres:YOUR_SECURE_PASSWORD@CLOUD_SQL_IP:5432/instagram" \
  -e REDIS_URL="redis://REDIS_HOST_IP:6379/0" \
  -e CELERY_BROKER_URL="amqp://guest:guest@RABBITMQ_IP:5672//" \
  us-central1-docker.pkg.dev/YOUR_PROJECT_ID/instagram-feed/api \
  celery -A celery_worker worker --loglevel=info --concurrency=4 -Q feed_fanout'
```

Container-Optimized OS (COS) is a minimal Linux distribution designed for running containers. The startup script pulls the same Docker image and runs it with the Celery worker command instead of the API server. In production, use a managed instance group with autoscaling based on queue depth.

### Set Up Custom Domain

```bash
gcloud run domain-mappings create \
  --service instagram-feed \
  --domain api.yourdomain.com \
  --region us-central1
```

After running this command, GCP provides DNS records to add at your domain registrar. Once DNS propagates, GCP automatically provisions and renews SSL certificates.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                | How                                              |
| --------------------- | ------------------------------------------ | ------------------------------------------------ |
| Read Replicas         | Read-heavy feed and profile queries        | Cloud SQL read replicas + SQLAlchemy `bind` param |
| Connection Pooling    | Many concurrent API + worker connections   | PgBouncer sidecar or Cloud SQL Auth Proxy pooling |
| Table Partitioning    | `posts` table grows past 1B rows           | Partition by `created_at` month using range parts |
| Sharding              | Single database can't handle write volume  | Shard by `user_id` hash across multiple DBs       |

### Feed Scaling

| Strategy              | When to Use                                | How                                              |
| --------------------- | ------------------------------------------ | ------------------------------------------------ |
| Redis Cluster         | Feed cache exceeds single-node memory      | Memorystore cluster mode (auto-shards data)      |
| Tiered Fan-Out        | Celebrity posts cause fan-out storms       | Fan-out-on-write for < 10K followers, read > 10K |
| Feed Ranking          | Chronological feed misses relevant posts   | ML ranking model scores posts before serving     |
| Multi-tier Cache      | Reduce Redis load further                  | Local in-memory cache (LRU) → Redis → PostgreSQL |

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ instagram.xyz  │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud CDN      │
                     │ (media cache)  │
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
         │  │ Cluster │    │  Read Replica) │  │
         │  │ (Redis) │    └────────────────┘  │
         │  └─────────┘                        │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Pub/Sub                │   │
         │  │  (Fan-out event queue)        │   │
         │  └──────────┬────────────────────┘   │
         │             │                        │
         │  ┌──────────▼────────────────────┐   │
         │  │  GCE Instance Group           │   │
         │  │  (Celery fan-out workers)     │   │
         │  └───────────────────────────────┘   │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Storage + CDN          │   │
         │  │  (Media: images, videos)      │   │
         │  └───────────────────────────────┘   │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Cloud CDN** caches media (images, videos) at edge locations worldwide, reducing latency and Cloud Storage egress costs. Feed API responses can also be cached at the edge for read-heavy workloads.
- **Cloud Pub/Sub** replaces RabbitMQ as the message broker for fan-out tasks. Pub/Sub is fully managed, scales automatically, and guarantees at-least-once delivery without provisioning brokers.
- **Redis Cluster** (Memorystore cluster mode) shards feed data across multiple nodes, scaling beyond single-node memory limits. Each user's feed sorted set is automatically placed on the correct shard.
- **Read Replicas** handle feed reads and analytics queries without impacting write latency on the primary database.
- **GCE Instance Group** with autoscaling adjusts the number of Celery workers based on Pub/Sub queue depth, scaling up during posting spikes and down during off-peak hours.
- **Cloud Storage + CDN** handles media delivery globally, with support for image transformations (resizing, cropping) via Cloud CDN image optimization.

### Security Checklist

```
✓ Authenticate all API requests (JWT or OAuth 2.0)
✓ Validate and sanitize captions (reject XSS payloads)
✓ Rate limit by user ID and IP
✓ Scan uploaded media for NSFW content (Cloud Vision API)
✓ Use signed URLs for private media instead of public buckets
✓ Set CORS headers to restrict API access to your domains
✓ Use HTTPS everywhere (Cloud Run provides this by default)
✓ Store secrets in Secret Manager, not environment variables
✓ Enable Cloud Audit Logs for compliance
✓ Implement content blocklisting and abuse reporting
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create instagram-health \
  --display-name="Instagram Feed Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# View Cloud Run metrics in the console
# → CPU utilization, request count, latency (p50, p95, p99), error rate

# Key metrics to alert on:
# - Error rate > 1% over 5 minutes
# - p99 latency > 1 second (feed generation)
# - Celery queue depth > 10,000 (fan-out backlog)
# - Redis memory usage > 80%
# - Cloud SQL CPU > 80%
# - Cloud Storage 4xx/5xx error rate
# - Feed cache miss rate > 30%
```

Cloud Run automatically exports metrics to Cloud Monitoring. Set up alert policies to notify your team via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing to identify bottlenecks across the API → Redis → PostgreSQL → Celery chain. Monitor Celery worker health and queue depth through the RabbitMQ management UI or Pub/Sub metrics dashboard.
