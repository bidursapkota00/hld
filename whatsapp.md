# Design WhatsApp / Messaging System

![Bidur Sapkota](https://www.bidursapkota.com.np/images/gravatar.webp "Bidur Sapkota - Developer")&nbsp;[Bidur Sapkota](https://www.bidursapkota.com.np/)

## Table of Contents

1. [Requirements & Estimations](#requirements--estimations)
2. [High-Level Architecture](#high-level-architecture)
3. [Project Setup](#project-setup)
4. [Database Design](#database-design)
5. [WebSocket Connection Manager](#websocket-connection-manager)
6. [Core Messaging API](#core-messaging-api)
7. [Delivery Receipts](#delivery-receipts)
8. [Group Messaging](#group-messaging)
9. [Online Presence & Typing Indicators](#online-presence--typing-indicators)
10. [Caching with Redis](#caching-with-redis)
11. [Message Queue with Redis Pub/Sub](#message-queue-with-redis-pubsub)
12. [Media Messages & Cloud Storage](#media-messages--cloud-storage)
13. [Docker & Docker Compose](#docker--docker-compose)
14. [Testing](#testing)
15. [Deploying to GCP (Cloud Run)](#deploying-to-gcp-cloud-run)
16. [Scaling & Production Considerations](#scaling--production-considerations)

---

## Requirements & Estimations

Before writing code, define the scope and estimate the scale.

### Functional Requirements

| Requirement          | Description                                                        |
| -------------------- | ------------------------------------------------------------------ |
| One-to-One Messaging | Real-time text messages between two users                          |
| Group Messaging      | Multi-user conversations with up to 256 members                    |
| Delivery Receipts    | Three-state tracking: sent (✓), delivered (✓✓), read (✓✓ blue)   |
| Online Presence      | Show whether a user is online, offline, or last seen at            |
| Typing Indicators    | Show "typing…" in real time                                        |
| Media Messages       | Send images, videos, and documents                                 |
| Message Persistence  | Store messages for offline users, deliver when they reconnect      |
| Push Notifications   | Notify offline users of new messages                               |

### Non-Functional Requirements

| Requirement      | Target                                                    |
| ---------------- | --------------------------------------------------------- |
| Availability     | 99.99% uptime                                             |
| Latency          | Message delivery in < 100 ms (both users online)          |
| Ordering         | Messages within a conversation arrive in send order       |
| Durability       | No message should ever be lost, even during server crashes |
| Scalability      | Handle 2 B users, 100 M daily active, 100 B messages/day |

### Back-of-the-Envelope Estimations

```
Daily active users (DAU)        ≈ 100 M
Messages per user per day       ≈ 50
Messages per day                ≈ 5 B
Messages per second (avg)       ≈ 58 K
Messages per second (peak)      ≈ 150 K
Average message size            ≈ 200 bytes (text + metadata)
Storage per day (text)          ≈ 5 B × 200 B ≈ 1 TB
Storage for 5 years (text)      ≈ 1 TB × 365 × 5 ≈ 1.8 PB
Concurrent WebSocket conns      ≈ 50 M (50% of DAU online at once)
Media messages per day          ≈ 500 M (10% of all messages)
Media storage per day           ≈ 500 M × 300 KB ≈ 150 TB
```

At 50 M concurrent WebSocket connections, a single server cannot hold them all. Each server handles ~50 K connections, so we need ~1000 WebSocket servers behind a load balancer. Messages between users on different servers require a message broker (Redis Pub/Sub or Kafka) to route in real time.

---

## High-Level Architecture

```
                      ┌───────────────┐
                      │   Clients     │
                      │(Mobile / Web) │
                      └──────┬────────┘
                             │ WebSocket + HTTPS
                      ┌──────▼────────┐
                      │ Load Balancer │
                      │  (GCP LB)     │
                      └──────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │     Chat Servers            │
              │  (FastAPI + WebSocket)       │
              │                              │
              │  WS  /ws/{user_id}           │
              │  POST /messages              │
              │  GET  /conversations         │
              └──┬──────┬──────┬─────────┬──┘
                 │      │      │         │
          ┌──────▼──┐ ┌─▼────┐ ┌▼──────┐ ┌▼───────────┐
          │  Redis  │ │Cassan│ │Cloud  │ │  Redis     │
          │ (Pub/Sub│ │dra / │ │Storage│ │ (Presence  │
          │  + Sess │ │Postgr│ │(Media)│ │  + Session │
          │  ions)  │ │eSQL  │ │ + CDN │ │  Store)    │
          └─────────┘ └──────┘ └───────┘ └────────────┘
```

Each client maintains a persistent WebSocket connection to a chat server. When User A sends a message to User B, the server publishes the message to Redis Pub/Sub on a channel named after User B. The chat server holding User B's connection subscribes to that channel and pushes the message down the WebSocket. If User B is offline, the message is stored in the database and delivered when they reconnect. Delivery receipts flow in the reverse direction: User B's server publishes an acknowledgement that User A's server picks up.

---

## Project Setup

### Project Structure

```
whatsapp/
├── app/
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── config.py             # Settings and environment variables
│   ├── models.py             # SQLAlchemy ORM models
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── database.py           # Database connection and session
│   ├── cache.py              # Redis cache and Pub/Sub layer
│   ├── connection_manager.py # WebSocket connection manager
│   ├── presence.py           # Online presence tracking
│   ├── receipts.py           # Delivery receipt logic
│   ├── media.py              # Media upload to Cloud Storage
│   └── routers/
│       ├── __init__.py
│       ├── auth.py           # User registration & login
│       ├── messages.py       # Send & retrieve messages
│       ├── conversations.py  # List & manage conversations
│       ├── groups.py         # Group creation & management
│       └── ws.py             # WebSocket endpoint
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Shared test fixtures
│   ├── test_messages.py      # Tests for messaging logic
│   ├── test_receipts.py      # Tests for delivery receipts
│   ├── test_groups.py        # Tests for group messaging
│   └── test_websocket.py    # Tests for WebSocket connections
├── Dockerfile
├── compose.yaml
├── requirements.txt
├── .env
├── .dockerignore
└── README.md
```

### Create Virtual Environment & Install Dependencies

```bash
mkdir whatsapp && cd whatsapp
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
python-multipart==0.0.12
google-cloud-storage==2.18.2
websockets==13.1
httpx==0.27.2
pytest==8.3.3
pytest-asyncio==0.24.0
python-jose==3.3.0
passlib[bcrypt]==1.7.4
```

`fastapi[standard]` includes `uvicorn` and `pydantic`. `sqlalchemy` is the ORM for PostgreSQL. `psycopg2-binary` is the PostgreSQL driver. `alembic` handles database migrations. `redis` is the Python client for Redis, used for Pub/Sub, presence, and session storage. `websockets` is the WebSocket library that `uvicorn` uses under the hood. `python-jose` handles JWT token creation and verification. `passlib[bcrypt]` provides secure password hashing. `python-multipart` is required for file upload handling.

```bash
pip install -r requirements.txt
```

### Environment Variables (`.env`)

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/whatsapp
REDIS_URL=redis://localhost:6379/0
GCS_BUCKET_NAME=whatsapp-media-bucket
CDN_BASE_URL=https://cdn.yourdomain.com
JWT_SECRET_KEY=your-secret-key-change-in-production
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=1440
MAX_GROUP_SIZE=256
MESSAGE_PAGE_SIZE=50
PRESENCE_TTL_SECONDS=120
RATE_LIMIT_PER_MINUTE=300
```

`DATABASE_URL` follows the SQLAlchemy connection string format. `REDIS_URL` points to the local Redis instance used for Pub/Sub, sessions, and presence tracking. `JWT_SECRET_KEY` signs authentication tokens; in production, use GCP Secret Manager. `MAX_GROUP_SIZE` limits group conversations to 256 members, matching WhatsApp's limit. `PRESENCE_TTL_SECONDS` controls how long a user is considered online after their last heartbeat.

---

## Database Design

### Entity-Relationship Diagram

```
┌──────────────────────────────────────┐
│              users                    │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ phone         VARCHAR(20) UNIQUE      │
│ username      VARCHAR(30) UNIQUE      │
│ display_name  VARCHAR(100)            │
│ avatar_url    TEXT                     │
│ password_hash TEXT        NOT NULL    │
│ last_seen_at  TIMESTAMPTZ             │
│ is_online     BOOLEAN    DEFAULT FALSE│
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_phone (phone)               │
│ INDEX idx_username (username)         │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│          conversations                │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ type          VARCHAR(10) NOT NULL    │  ← "direct" or "group"
│ name          VARCHAR(100)            │  ← NULL for direct, name for group
│ avatar_url    TEXT                     │
│ created_by    BIGINT     FK → users   │
│ created_at    TIMESTAMPTZ DEFAULT NOW │
│ updated_at    TIMESTAMPTZ DEFAULT NOW │  ← last message timestamp
├──────────────────────────────────────┤
│ INDEX idx_updated (updated_at DESC)   │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│          participants                 │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ conversation_id BIGINT   FK → convos  │
│ user_id       BIGINT     FK → users   │
│ role          VARCHAR(10) DEFAULT mem │  ← "admin" or "member"
│ joined_at     TIMESTAMPTZ DEFAULT NOW │
│ last_read_at  TIMESTAMPTZ             │  ← for unread count
├──────────────────────────────────────┤
│ UNIQUE (conversation_id, user_id)     │
│ INDEX idx_user_convos (user_id)       │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│            messages                   │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ conversation_id BIGINT   FK → convos  │
│ sender_id     BIGINT     FK → users   │
│ content       TEXT                     │
│ message_type  VARCHAR(10) DEFAULT txt │  ← "text","image","video","file"
│ media_url     TEXT                     │
│ status        VARCHAR(10) DEFAULT sent│  ← "sent","delivered","read"
│ reply_to_id   BIGINT     FK → messages│  ← for reply threading
│ created_at    TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ INDEX idx_convo_created               │
│   (conversation_id, created_at DESC)  │
│ INDEX idx_sender (sender_id)          │
└──────────────────────────────────────┘

┌──────────────────────────────────────┐
│        message_receipts               │
├──────────────────────────────────────┤
│ id            BIGSERIAL  PRIMARY KEY  │
│ message_id    BIGINT     FK → messages│
│ user_id       BIGINT     FK → users   │
│ status        VARCHAR(10) NOT NULL    │  ← "delivered" or "read"
│ timestamp     TIMESTAMPTZ DEFAULT NOW │
├──────────────────────────────────────┤
│ UNIQUE (message_id, user_id, status)  │
│ INDEX idx_msg_receipts (message_id)   │
└──────────────────────────────────────┘
```

The `conversations` table is a unified model for both one-to-one and group chats. A `type` column distinguishes them, and `name` is `NULL` for direct messages. `participants` is a join table linking users to conversations; `last_read_at` tracks the watermark for calculating unread message counts without scanning every message. The `messages` table has a composite index on `(conversation_id, created_at DESC)` for efficient reverse-chronological pagination within a conversation. `message_receipts` tracks per-user delivery and read status for group messages; for direct messages, the `status` column on `messages` itself suffices. `reply_to_id` is a self-referencing foreign key for threaded replies.

### `app/config.py`

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://user:pass@localhost:5432/whatsapp"
    redis_url: str = "redis://localhost:6379/0"
    gcs_bucket_name: str = "whatsapp-media-bucket"
    cdn_base_url: str = "https://cdn.yourdomain.com"
    jwt_secret_key: str = "your-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440
    max_group_size: int = 256
    message_page_size: int = 50
    presence_ttl_seconds: int = 120
    rate_limit_per_minute: int = 300

    class Config:
        env_file = ".env"


settings = Settings()
```

`BaseSettings` from `pydantic_settings` automatically reads values from environment variables and `.env` files. `access_token_expire_minutes=1440` sets JWT tokens to expire after 24 hours. `presence_ttl_seconds=120` means a user is considered offline if no heartbeat is received for 2 minutes.

### `app/database.py`

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import settings

engine = create_engine(
    settings.database_url,
    pool_size=30,                         # Higher pool for WebSocket connections
    max_overflow=20,                      # Extra connections under load
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

`pool_size=30` is higher than the typical 20 because WebSocket connections are long-lived and each may need a database session for message operations. `max_overflow=20` provides additional headroom during message bursts. `pool_pre_ping=True` detects stale connections from cloud databases that close idle sockets.

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
    phone = Column(String(20), unique=True, nullable=False, index=True)
    username = Column(String(30), unique=True, nullable=False, index=True)
    display_name = Column(String(100), nullable=False)
    avatar_url = Column(Text, nullable=True)
    password_hash = Column(Text, nullable=False)
    last_seen_at = Column(DateTime(timezone=True), nullable=True)
    is_online = Column(Boolean, default=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    sent_messages = relationship("Message", back_populates="sender")
    participations = relationship("Participant", back_populates="user")


class Conversation(Base):
    __tablename__ = "conversations"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    type = Column(String(10), nullable=False, default="direct")
    name = Column(String(100), nullable=True)
    avatar_url = Column(Text, nullable=True)
    created_by = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    participants = relationship("Participant", back_populates="conversation")
    messages = relationship("Message", back_populates="conversation")

    __table_args__ = (
        Index("idx_updated", "updated_at"),
    )


class Participant(Base):
    __tablename__ = "participants"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id = Column(
        BigInteger, ForeignKey("conversations.id"), nullable=False
    )
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    role = Column(String(10), default="member")
    joined_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    last_read_at = Column(DateTime(timezone=True), nullable=True)

    conversation = relationship("Conversation", back_populates="participants")
    user = relationship("User", back_populates="participations")

    __table_args__ = (
        UniqueConstraint("conversation_id", "user_id", name="uq_participant"),
        Index("idx_user_convos", "user_id"),
    )


class Message(Base):
    __tablename__ = "messages"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id = Column(
        BigInteger, ForeignKey("conversations.id"), nullable=False
    )
    sender_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    content = Column(Text, nullable=True)
    message_type = Column(String(10), default="text")
    media_url = Column(Text, nullable=True)
    status = Column(String(10), default="sent")
    reply_to_id = Column(BigInteger, ForeignKey("messages.id"), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    conversation = relationship("Conversation", back_populates="messages")
    sender = relationship("User", back_populates="sent_messages")
    reply_to = relationship("Message", remote_side=[id])
    receipts = relationship("MessageReceipt", back_populates="message")

    __table_args__ = (
        Index("idx_convo_created", "conversation_id", "created_at"),
        Index("idx_sender", "sender_id"),
    )


class MessageReceipt(Base):
    __tablename__ = "message_receipts"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    message_id = Column(BigInteger, ForeignKey("messages.id"), nullable=False)
    user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
    status = Column(String(10), nullable=False)
    timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )

    message = relationship("Message", back_populates="receipts")

    __table_args__ = (
        UniqueConstraint("message_id", "user_id", "status", name="uq_receipt"),
        Index("idx_msg_receipts", "message_id"),
    )
```

`BigInteger` for all IDs supports billions of rows. `onupdate` on `Conversation.updated_at` automatically sets the timestamp whenever the row is modified, keeping conversations sorted by last activity. `reply_to = relationship("Message", remote_side=[id])` is a self-referencing relationship; `remote_side` tells SQLAlchemy which side of the foreign key is the "parent." `UniqueConstraint("message_id", "user_id", "status")` on `MessageReceipt` allows the same user to have both a "delivered" and a "read" receipt for the same message, but not duplicate receipts of the same status.

### `app/schemas.py`

```python
from datetime import datetime
from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    phone: str = Field(min_length=10, max_length=20)
    username: str = Field(min_length=3, max_length=30)
    display_name: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=8)


class UserResponse(BaseModel):
    id: int
    phone: str
    username: str
    display_name: str
    avatar_url: str | None
    is_online: bool
    last_seen_at: datetime | None
    created_at: datetime

    class Config:
        from_attributes = True


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MessageCreate(BaseModel):
    conversation_id: int
    content: str | None = None
    message_type: str = "text"
    reply_to_id: int | None = None


class MessageResponse(BaseModel):
    id: int
    conversation_id: int
    sender_id: int
    content: str | None
    message_type: str
    media_url: str | None
    status: str
    reply_to_id: int | None
    created_at: datetime
    sender_username: str | None = None

    class Config:
        from_attributes = True


class ConversationCreate(BaseModel):
    participant_ids: list[int]
    name: str | None = None                   # Required for group, NULL for direct


class ConversationResponse(BaseModel):
    id: int
    type: str
    name: str | None
    avatar_url: str | None
    created_at: datetime
    updated_at: datetime
    last_message: MessageResponse | None = None
    unread_count: int = 0
    participants: list[UserResponse] = []

    class Config:
        from_attributes = True


class GroupUpdate(BaseModel):
    name: str | None = None
    add_members: list[int] = []
    remove_members: list[int] = []


class ReceiptUpdate(BaseModel):
    message_ids: list[int]
    status: str = Field(pattern="^(delivered|read)$")


class TypingEvent(BaseModel):
    conversation_id: int
    is_typing: bool


class WSMessage(BaseModel):
    """Generic WebSocket message envelope."""
    type: str                                 # "message","receipt","typing","presence"
    payload: dict


class PaginatedMessages(BaseModel):
    messages: list[MessageResponse]
    next_cursor: str | None
    has_more: bool
```

`WSMessage` is the envelope format for all WebSocket communication; `type` identifies the event kind and `payload` carries the data. This pattern avoids creating separate WebSocket endpoints for each feature. `ReceiptUpdate` uses `Field(pattern=...)` to restrict status to only "delivered" or "read" at the schema validation layer. `PaginatedMessages` uses cursor-based pagination with message timestamps as cursors, identical to feed pagination but applied to conversation history.

---

## WebSocket Connection Manager

### `app/connection_manager.py`

```python
import asyncio
import json
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages active WebSocket connections for this server instance.
    Maps user_id → WebSocket for message routing.
    """

    def __init__(self):
        self.active_connections: dict[int, WebSocket] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: int, websocket: WebSocket):
        """Accept a WebSocket connection and register the user."""
        await websocket.accept()
        async with self._lock:
            # Close existing connection if user reconnects
            old_ws = self.active_connections.get(user_id)
            if old_ws:
                try:
                    await old_ws.close(code=4001, reason="Connected from another device")
                except Exception:
                    pass
            self.active_connections[user_id] = websocket
        logger.info(f"User {user_id} connected. Total: {len(self.active_connections)}")

    async def disconnect(self, user_id: int):
        """Remove a user's WebSocket connection."""
        async with self._lock:
            self.active_connections.pop(user_id, None)
        logger.info(f"User {user_id} disconnected. Total: {len(self.active_connections)}")

    async def send_to_user(self, user_id: int, message: dict) -> bool:
        """
        Send a message to a specific user on this server.
        Returns True if the user is connected locally and the message was sent.
        """
        websocket = self.active_connections.get(user_id)
        if websocket:
            try:
                await websocket.send_json(message)
                return True
            except Exception as e:
                logger.error(f"Failed to send to user {user_id}: {e}")
                await self.disconnect(user_id)
                return False
        return False

    async def broadcast_to_users(self, user_ids: list[int], message: dict):
        """Send a message to multiple users on this server."""
        tasks = [self.send_to_user(uid, message) for uid in user_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    def is_connected(self, user_id: int) -> bool:
        """Check if a user has an active WebSocket on this server."""
        return user_id in self.active_connections

    @property
    def connection_count(self) -> int:
        return len(self.active_connections)


# Singleton instance for this server process
manager = ConnectionManager()
```

`ConnectionManager` is a per-server-process singleton that maps `user_id` to their WebSocket object. When a user connects from a new device, the old connection is closed with a custom close code `4001` so the client knows to stop reconnecting. `asyncio.Lock()` protects the dictionary from race conditions during concurrent connect/disconnect operations. `send_to_user` returns a boolean so the caller knows whether the message was delivered locally or needs to be routed through the message broker. `broadcast_to_users` uses `asyncio.gather()` to send to multiple users concurrently rather than sequentially.

### `app/routers/ws.py`

```python
import asyncio
import json
import logging

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from app.cache import (
    publish_message, subscribe_to_user_channel,
    set_user_online, set_user_offline,
)
from app.config import settings
from app.connection_manager import manager
from app.database import SessionLocal
from app.models import Message, Participant
from app.presence import broadcast_presence
from app.receipts import process_receipt_update

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    """
    Main WebSocket endpoint. Each connected user maintains a persistent
    connection through which all real-time events are sent and received.
    """
    await manager.connect(user_id, websocket)
    await set_user_online(user_id)
    await broadcast_presence(user_id, is_online=True)

    # Deliver pending messages stored while user was offline
    await deliver_pending_messages(user_id, websocket)

    # Subscribe to messages routed from other server instances via Redis
    pubsub_task = asyncio.create_task(
        listen_to_pubsub(user_id, websocket)
    )

    # Send periodic heartbeat pings
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(user_id, websocket)
    )

    try:
        while True:
            # Receive messages from the client
            data = await websocket.receive_json()
            await handle_client_message(user_id, data)

    except WebSocketDisconnect:
        logger.info(f"User {user_id} WebSocket disconnected.")
    except Exception as e:
        logger.error(f"WebSocket error for user {user_id}: {e}")
    finally:
        pubsub_task.cancel()
        heartbeat_task.cancel()
        await manager.disconnect(user_id)
        await set_user_offline(user_id)
        await broadcast_presence(user_id, is_online=False)


async def handle_client_message(sender_id: int, data: dict):
    """Route incoming WebSocket messages by type."""
    msg_type = data.get("type")

    if msg_type == "message":
        await handle_new_message(sender_id, data.get("payload", {}))
    elif msg_type == "receipt":
        await handle_receipt(sender_id, data.get("payload", {}))
    elif msg_type == "typing":
        await handle_typing(sender_id, data.get("payload", {}))
    else:
        logger.warning(f"Unknown message type from user {sender_id}: {msg_type}")


async def handle_new_message(sender_id: int, payload: dict):
    """Process a new chat message from a client."""
    db = SessionLocal()
    try:
        conversation_id = payload.get("conversation_id")
        content = payload.get("content")
        message_type = payload.get("message_type", "text")
        reply_to_id = payload.get("reply_to_id")

        # Create the message in the database
        message = Message(
            conversation_id=conversation_id,
            sender_id=sender_id,
            content=content,
            message_type=message_type,
            media_url=payload.get("media_url"),
            reply_to_id=reply_to_id,
            status="sent",
        )
        db.add(message)
        db.commit()
        db.refresh(message)

        # Get all participants in the conversation except the sender
        participants = (
            db.query(Participant.user_id)
            .filter(Participant.conversation_id == conversation_id)
            .filter(Participant.user_id != sender_id)
            .all()
        )
        recipient_ids = [p.user_id for p in participants]

        # Prepare the message payload for WebSocket delivery
        ws_message = {
            "type": "message",
            "payload": {
                "id": message.id,
                "conversation_id": message.conversation_id,
                "sender_id": message.sender_id,
                "content": message.content,
                "message_type": message.message_type,
                "media_url": message.media_url,
                "status": message.status,
                "reply_to_id": message.reply_to_id,
                "created_at": message.created_at.isoformat(),
            },
        }

        # Send confirmation back to the sender
        await manager.send_to_user(sender_id, {
            "type": "message_ack",
            "payload": {
                "message_id": message.id,
                "status": "sent",
                "created_at": message.created_at.isoformat(),
            },
        })

        # Deliver to each recipient
        for recipient_id in recipient_ids:
            # Try local delivery first
            delivered_locally = await manager.send_to_user(recipient_id, ws_message)

            if not delivered_locally:
                # Recipient is on another server or offline → publish via Redis
                await publish_message(recipient_id, ws_message)

    finally:
        db.close()


async def handle_receipt(sender_id: int, payload: dict):
    """Process a delivery/read receipt from a client."""
    db = SessionLocal()
    try:
        await process_receipt_update(
            user_id=sender_id,
            message_ids=payload.get("message_ids", []),
            status=payload.get("status", "delivered"),
            db=db,
        )
    finally:
        db.close()


async def handle_typing(sender_id: int, payload: dict):
    """Broadcast typing indicator to conversation participants."""
    db = SessionLocal()
    try:
        conversation_id = payload.get("conversation_id")
        is_typing = payload.get("is_typing", True)

        participants = (
            db.query(Participant.user_id)
            .filter(Participant.conversation_id == conversation_id)
            .filter(Participant.user_id != sender_id)
            .all()
        )

        typing_event = {
            "type": "typing",
            "payload": {
                "conversation_id": conversation_id,
                "user_id": sender_id,
                "is_typing": is_typing,
            },
        }

        for p in participants:
            delivered = await manager.send_to_user(p.user_id, typing_event)
            if not delivered:
                await publish_message(p.user_id, typing_event)
    finally:
        db.close()


async def listen_to_pubsub(user_id: int, websocket: WebSocket):
    """
    Listen for messages published to this user's Redis Pub/Sub channel.
    Messages arrive here when the sender is on a different server instance.
    """
    try:
        async for message in subscribe_to_user_channel(user_id):
            try:
                await websocket.send_json(message)
            except Exception:
                break
    except asyncio.CancelledError:
        pass


async def heartbeat_loop(user_id: int, websocket: WebSocket):
    """Send periodic pings to detect stale connections."""
    try:
        while True:
            await asyncio.sleep(30)
            try:
                await websocket.send_json({"type": "ping"})
                await set_user_online(user_id)
            except Exception:
                break
    except asyncio.CancelledError:
        pass


async def deliver_pending_messages(user_id: int, websocket: WebSocket):
    """Deliver messages that were stored while the user was offline."""
    db = SessionLocal()
    try:
        # Find conversations the user is part of
        participant_rows = (
            db.query(Participant)
            .filter(Participant.user_id == user_id)
            .all()
        )

        for p in participant_rows:
            # Get unread messages (messages after last_read_at)
            query = (
                db.query(Message)
                .filter(Message.conversation_id == p.conversation_id)
                .filter(Message.sender_id != user_id)
                .filter(Message.status.in_(["sent", "delivered"]))
                .order_by(Message.created_at.asc())
            )

            if p.last_read_at:
                query = query.filter(Message.created_at > p.last_read_at)

            pending_messages = query.limit(200).all()

            for msg in pending_messages:
                await websocket.send_json({
                    "type": "message",
                    "payload": {
                        "id": msg.id,
                        "conversation_id": msg.conversation_id,
                        "sender_id": msg.sender_id,
                        "content": msg.content,
                        "message_type": msg.message_type,
                        "media_url": msg.media_url,
                        "status": msg.status,
                        "reply_to_id": msg.reply_to_id,
                        "created_at": msg.created_at.isoformat(),
                    },
                })

                # Update message status to "delivered"
                if msg.status == "sent":
                    msg.status = "delivered"

            db.commit()
    finally:
        db.close()
```

This is the core real-time engine. `websocket_endpoint` is the entry point for every WebSocket connection. On connect, it marks the user online, delivers pending messages, starts two background coroutines (Pub/Sub listener and heartbeat), and then enters the receive loop. `handle_new_message` follows a write-then-fan-out pattern: the message is committed to PostgreSQL first (durability guarantee), then delivered to each recipient either locally or via Redis Pub/Sub. The sender receives a `message_ack` confirming the server accepted the message. `listen_to_pubsub` is an async generator that yields messages from the user's Redis channel, enabling cross-server delivery. `heartbeat_loop` sends a ping every 30 seconds; if the send fails, the loop breaks and the connection is cleaned up. `deliver_pending_messages` runs once on reconnect, delivering all messages that arrived while the user was offline, limited to 200 per conversation to avoid flooding.

---

## Core Messaging API

These REST endpoints complement the WebSocket layer, providing message history and conversation management for clients that need HTTP fallback.

### `app/routers/messages.py`

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Message, Participant
from app.schemas import MessageResponse, PaginatedMessages

router = APIRouter()


@router.get(
    "/conversations/{conversation_id}/messages",
    response_model=PaginatedMessages,
)
def get_messages(
    conversation_id: int,
    user_id: int,
    cursor: str | None = Query(None),
    limit: int = Query(50, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """
    Get message history for a conversation with cursor-based pagination.
    The cursor is an ISO timestamp. Pass `next_cursor` from the previous
    response to load older messages.
    """
    # Verify user is a participant
    participant = (
        db.query(Participant)
        .filter(
            Participant.conversation_id == conversation_id,
            Participant.user_id == user_id,
        )
        .first()
    )
    if not participant:
        raise HTTPException(status_code=403, detail="Not a participant.")

    query = (
        db.query(Message)
        .filter(Message.conversation_id == conversation_id)
        .order_by(Message.created_at.desc())
    )

    if cursor:
        cursor_dt = datetime.fromisoformat(cursor)
        query = query.filter(Message.created_at < cursor_dt)

    messages = query.limit(limit + 1).all()       # Fetch one extra to check has_more
    has_more = len(messages) > limit
    messages = messages[:limit]

    # Fetch sender usernames
    sender_ids = list({m.sender_id for m in messages})
    from app.models import User
    users = db.query(User).filter(User.id.in_(sender_ids)).all()
    user_map = {u.id: u.username for u in users}

    return PaginatedMessages(
        messages=[
            MessageResponse(
                id=m.id,
                conversation_id=m.conversation_id,
                sender_id=m.sender_id,
                content=m.content,
                message_type=m.message_type,
                media_url=m.media_url,
                status=m.status,
                reply_to_id=m.reply_to_id,
                created_at=m.created_at,
                sender_username=user_map.get(m.sender_id),
            )
            for m in messages
        ],
        next_cursor=messages[-1].created_at.isoformat() if has_more else None,
        has_more=has_more,
    )


@router.delete("/messages/{message_id}", status_code=200)
def delete_message(message_id: int, user_id: int, db: Session = Depends(get_db)):
    """Delete a message (soft delete — replace content)."""
    message = db.query(Message).filter(Message.id == message_id).first()
    if not message:
        raise HTTPException(status_code=404, detail="Message not found.")

    if message.sender_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized.")

    message.content = None
    message.message_type = "deleted"
    message.media_url = None
    db.commit()

    return {"message": "Message deleted."}
```

`limit + 1` is a standard trick for cursor-based pagination: fetching one extra row tells you whether more data exists without running a separate COUNT query. If the extra row exists, `has_more` is `True` and the extra row is discarded. Soft deletion replaces the content with `None` and sets `message_type` to `"deleted"` instead of removing the row, preserving conversation continuity and threading (other messages may reply to a deleted one).

### `app/routers/conversations.py`

```python
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Conversation, Message, Participant, User
from app.schemas import ConversationCreate, ConversationResponse, UserResponse
from app.config import settings

router = APIRouter()


@router.post("/conversations", response_model=ConversationResponse, status_code=201)
def create_conversation(
    payload: ConversationCreate,
    user_id: int,
    db: Session = Depends(get_db),
):
    """
    Create a new conversation.
    - 1 participant_id + no name → direct message
    - 2+ participant_ids + name → group
    """
    all_participant_ids = list(set(payload.participant_ids + [user_id]))

    # Direct message: check if conversation already exists
    if len(all_participant_ids) == 2 and not payload.name:
        other_id = [pid for pid in all_participant_ids if pid != user_id][0]
        existing = find_direct_conversation(user_id, other_id, db)
        if existing:
            return build_conversation_response(existing, user_id, db)

        conv_type = "direct"
    else:
        if not payload.name:
            raise HTTPException(
                status_code=400, detail="Group conversations require a name."
            )
        if len(all_participant_ids) > settings.max_group_size:
            raise HTTPException(
                status_code=400,
                detail=f"Maximum group size is {settings.max_group_size}.",
            )
        conv_type = "group"

    # Validate all participants exist
    existing_users = (
        db.query(User.id).filter(User.id.in_(all_participant_ids)).all()
    )
    if len(existing_users) != len(all_participant_ids):
        raise HTTPException(status_code=404, detail="One or more users not found.")

    conversation = Conversation(
        type=conv_type,
        name=payload.name,
        created_by=user_id,
    )
    db.add(conversation)
    db.flush()

    # Add participants
    for pid in all_participant_ids:
        role = "admin" if pid == user_id else "member"
        participant = Participant(
            conversation_id=conversation.id,
            user_id=pid,
            role=role,
        )
        db.add(participant)

    db.commit()
    db.refresh(conversation)

    return build_conversation_response(conversation, user_id, db)


def find_direct_conversation(
    user_id: int, other_id: int, db: Session
) -> Conversation | None:
    """Find an existing direct conversation between two users."""
    result = (
        db.query(Participant.conversation_id)
        .filter(Participant.user_id == user_id)
        .intersect(
            db.query(Participant.conversation_id)
            .filter(Participant.user_id == other_id)
        )
        .all()
    )

    for row in result:
        conv = db.query(Conversation).filter(
            Conversation.id == row.conversation_id,
            Conversation.type == "direct",
        ).first()
        if conv:
            return conv

    return None


@router.get("/conversations", response_model=list[ConversationResponse])
def list_conversations(
    user_id: int,
    skip: int = 0,
    limit: int = 20,
    db: Session = Depends(get_db),
):
    """List all conversations for a user, sorted by last activity."""
    conversation_ids = (
        db.query(Participant.conversation_id)
        .filter(Participant.user_id == user_id)
        .subquery()
    )

    conversations = (
        db.query(Conversation)
        .filter(Conversation.id.in_(conversation_ids))
        .order_by(desc(Conversation.updated_at))
        .offset(skip)
        .limit(limit)
        .all()
    )

    return [
        build_conversation_response(conv, user_id, db)
        for conv in conversations
    ]


def build_conversation_response(
    conv: Conversation, viewer_id: int, db: Session
) -> ConversationResponse:
    """Build a rich conversation response with last message and unread count."""
    # Last message
    last_msg = (
        db.query(Message)
        .filter(Message.conversation_id == conv.id)
        .order_by(desc(Message.created_at))
        .first()
    )

    # Unread count
    viewer_participant = (
        db.query(Participant)
        .filter(
            Participant.conversation_id == conv.id,
            Participant.user_id == viewer_id,
        )
        .first()
    )

    unread_query = (
        db.query(func.count(Message.id))
        .filter(Message.conversation_id == conv.id)
        .filter(Message.sender_id != viewer_id)
    )
    if viewer_participant and viewer_participant.last_read_at:
        unread_query = unread_query.filter(
            Message.created_at > viewer_participant.last_read_at
        )
    unread_count = unread_query.scalar() or 0

    # Participants
    participants = (
        db.query(User)
        .join(Participant, Participant.user_id == User.id)
        .filter(Participant.conversation_id == conv.id)
        .all()
    )

    last_message = None
    if last_msg:
        sender = db.query(User).filter(User.id == last_msg.sender_id).first()
        last_message = {
            "id": last_msg.id,
            "conversation_id": last_msg.conversation_id,
            "sender_id": last_msg.sender_id,
            "content": last_msg.content,
            "message_type": last_msg.message_type,
            "media_url": last_msg.media_url,
            "status": last_msg.status,
            "reply_to_id": last_msg.reply_to_id,
            "created_at": last_msg.created_at,
            "sender_username": sender.username if sender else None,
        }

    return ConversationResponse(
        id=conv.id,
        type=conv.type,
        name=conv.name,
        avatar_url=conv.avatar_url,
        created_at=conv.created_at,
        updated_at=conv.updated_at,
        last_message=last_message,
        unread_count=unread_count,
        participants=participants,
    )
```

`create_conversation` handles both direct and group creation through a single endpoint. For direct messages, it first checks for an existing conversation using set intersection on participant IDs, avoiding duplicate conversations. `find_direct_conversation` uses SQL `INTERSECT` to find conversation IDs where both users are participants, then filters for `type="direct"`. `build_conversation_response` enriches the raw conversation with the last message, unread count (computed by counting messages after the viewer's `last_read_at`), and participant profiles. `order_by(desc(Conversation.updated_at))` sorts conversations by last activity, so the conversation list always shows the most recently active at the top.

---

## Delivery Receipts

### `app/receipts.py`

```python
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.cache import publish_message
from app.connection_manager import manager
from app.models import Message, MessageReceipt, Participant


async def process_receipt_update(
    user_id: int,
    message_ids: list[int],
    status: str,
    db: Session,
):
    """
    Process delivery or read receipts from a client.

    WhatsApp-style receipt semantics:
    - "delivered" (✓✓)     → message reached the recipient's device
    - "read" (✓✓ blue)     → recipient opened the conversation

    For direct messages, we update the message's status column directly.
    For group messages, we insert per-user receipts into message_receipts.
    """
    messages = (
        db.query(Message)
        .filter(Message.id.in_(message_ids))
        .all()
    )

    for message in messages:
        # Get the conversation type
        conversation_id = message.conversation_id
        participant = (
            db.query(Participant)
            .filter(
                Participant.conversation_id == conversation_id,
                Participant.user_id == user_id,
            )
            .first()
        )

        if not participant:
            continue

        # Determine conversation type by counting participants
        participant_count = (
            db.query(Participant)
            .filter(Participant.conversation_id == conversation_id)
            .count()
        )

        is_direct = participant_count == 2

        if is_direct:
            # Direct message: update message status directly
            if should_update_status(message.status, status):
                message.status = status
        else:
            # Group message: create per-user receipt
            existing_receipt = (
                db.query(MessageReceipt)
                .filter(
                    MessageReceipt.message_id == message.id,
                    MessageReceipt.user_id == user_id,
                    MessageReceipt.status == status,
                )
                .first()
            )
            if not existing_receipt:
                receipt = MessageReceipt(
                    message_id=message.id,
                    user_id=user_id,
                    status=status,
                )
                db.add(receipt)

        # Update last_read_at for "read" receipts
        if status == "read" and participant:
            now = datetime.now(timezone.utc)
            if not participant.last_read_at or participant.last_read_at < now:
                participant.last_read_at = now

        # Notify the sender that their message was delivered/read
        receipt_event = {
            "type": "receipt",
            "payload": {
                "message_id": message.id,
                "conversation_id": conversation_id,
                "user_id": user_id,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }

        # Try sending directly, fall back to Pub/Sub
        delivered = await manager.send_to_user(message.sender_id, receipt_event)
        if not delivered:
            await publish_message(message.sender_id, receipt_event)

    db.commit()


def should_update_status(current: str, new: str) -> bool:
    """
    Only advance the status forward: sent → delivered → read.
    Never go backwards (e.g., "read" should not revert to "delivered").
    """
    status_order = {"sent": 0, "delivered": 1, "read": 2}
    return status_order.get(new, 0) > status_order.get(current, 0)


async def mark_conversation_delivered(user_id: int, conversation_id: int, db: Session):
    """
    Mark all undelivered messages in a conversation as delivered.
    Called when a user comes online and pending messages are flushed.
    """
    undelivered = (
        db.query(Message)
        .filter(
            Message.conversation_id == conversation_id,
            Message.sender_id != user_id,
            Message.status == "sent",
        )
        .all()
    )

    if undelivered:
        message_ids = [m.id for m in undelivered]
        await process_receipt_update(user_id, message_ids, "delivered", db)


async def mark_conversation_read(user_id: int, conversation_id: int, db: Session):
    """
    Mark all messages in a conversation as read.
    Called when the user opens a conversation in the UI.
    """
    unread = (
        db.query(Message)
        .filter(
            Message.conversation_id == conversation_id,
            Message.sender_id != user_id,
            Message.status.in_(["sent", "delivered"]),
        )
        .all()
    )

    if unread:
        message_ids = [m.id for m in unread]
        await process_receipt_update(user_id, message_ids, "read", db)
```

`should_update_status` enforces a strict forward-only state machine: `sent → delivered → read`. This prevents race conditions where a "delivered" receipt arriving after a "read" receipt would incorrectly downgrade the status. For direct messages, the status is stored directly on the `Message` row since there is only one recipient. For group messages, `MessageReceipt` stores per-user status so the sender can see which participants have received/read the message. `last_read_at` is updated on "read" receipts, providing the watermark for unread count calculations. Receipt notifications are sent to the original sender via WebSocket (local or Pub/Sub), updating the checkmark icons in their UI in real time.

---

## Group Messaging

### `app/routers/groups.py`

```python
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.cache import publish_message
from app.config import settings
from app.connection_manager import manager
from app.database import get_db
from app.models import Conversation, Participant, User
from app.schemas import GroupUpdate, UserResponse

router = APIRouter()


@router.put("/conversations/{conversation_id}/group", status_code=200)
def update_group(
    conversation_id: int,
    user_id: int,
    payload: GroupUpdate,
    db: Session = Depends(get_db),
):
    """Update a group conversation (admin only)."""
    conversation = (
        db.query(Conversation)
        .filter(Conversation.id == conversation_id, Conversation.type == "group")
        .first()
    )
    if not conversation:
        raise HTTPException(status_code=404, detail="Group not found.")

    # Verify the user is an admin
    admin = (
        db.query(Participant)
        .filter(
            Participant.conversation_id == conversation_id,
            Participant.user_id == user_id,
            Participant.role == "admin",
        )
        .first()
    )
    if not admin:
        raise HTTPException(status_code=403, detail="Only admins can modify the group.")

    # Update group name
    if payload.name:
        conversation.name = payload.name

    # Add new members
    if payload.add_members:
        current_count = (
            db.query(Participant)
            .filter(Participant.conversation_id == conversation_id)
            .count()
        )
        if current_count + len(payload.add_members) > settings.max_group_size:
            raise HTTPException(
                status_code=400,
                detail=f"Adding members would exceed the max group size of "
                       f"{settings.max_group_size}.",
            )

        for member_id in payload.add_members:
            user = db.query(User).filter(User.id == member_id).first()
            if not user:
                continue

            existing = (
                db.query(Participant)
                .filter(
                    Participant.conversation_id == conversation_id,
                    Participant.user_id == member_id,
                )
                .first()
            )
            if not existing:
                db.add(Participant(
                    conversation_id=conversation_id,
                    user_id=member_id,
                    role="member",
                ))

    # Remove members
    if payload.remove_members:
        for member_id in payload.remove_members:
            if member_id == user_id:
                continue                  # Admins can't remove themselves this way

            participant = (
                db.query(Participant)
                .filter(
                    Participant.conversation_id == conversation_id,
                    Participant.user_id == member_id,
                )
                .first()
            )
            if participant:
                db.delete(participant)

    db.commit()

    return {"message": "Group updated."}


@router.post("/conversations/{conversation_id}/leave", status_code=200)
def leave_group(
    conversation_id: int,
    user_id: int,
    db: Session = Depends(get_db),
):
    """Leave a group conversation."""
    participant = (
        db.query(Participant)
        .filter(
            Participant.conversation_id == conversation_id,
            Participant.user_id == user_id,
        )
        .first()
    )
    if not participant:
        raise HTTPException(status_code=404, detail="Not a member.")

    conversation = (
        db.query(Conversation).filter(Conversation.id == conversation_id).first()
    )
    if conversation.type != "group":
        raise HTTPException(status_code=400, detail="Cannot leave a direct conversation.")

    db.delete(participant)

    # If the leaving user was the only admin, promote the next member
    remaining_admins = (
        db.query(Participant)
        .filter(
            Participant.conversation_id == conversation_id,
            Participant.role == "admin",
        )
        .count()
    )

    if remaining_admins == 0:
        next_member = (
            db.query(Participant)
            .filter(Participant.conversation_id == conversation_id)
            .order_by(Participant.joined_at.asc())
            .first()
        )
        if next_member:
            next_member.role = "admin"

    db.commit()

    return {"message": "Left the group."}


@router.get(
    "/conversations/{conversation_id}/members",
    response_model=list[UserResponse],
)
def get_members(
    conversation_id: int,
    user_id: int,
    db: Session = Depends(get_db),
):
    """Get all members of a conversation."""
    is_participant = (
        db.query(Participant)
        .filter(
            Participant.conversation_id == conversation_id,
            Participant.user_id == user_id,
        )
        .first()
    )
    if not is_participant:
        raise HTTPException(status_code=403, detail="Not a participant.")

    members = (
        db.query(User)
        .join(Participant, Participant.user_id == User.id)
        .filter(Participant.conversation_id == conversation_id)
        .all()
    )
    return members
```

`update_group` enforces admin-only modification, checking the `role` column on the `Participant` row. The group size check counts current members and validates that adding new ones would not exceed `MAX_GROUP_SIZE`. `leave_group` handles admin succession: if the last admin leaves, the earliest-joined remaining member is promoted automatically to prevent orphaned groups. `get_members` requires the requester to be a participant, ensuring non-members cannot enumerate group membership.

---

## Online Presence & Typing Indicators

### `app/presence.py`

```python
import json
import time

from app.cache import redis_client, publish_message
from app.config import settings
from app.connection_manager import manager
from app.database import SessionLocal
from app.models import Participant


async def set_user_online(user_id: int):
    """Mark a user as online in Redis with a TTL-based heartbeat."""
    redis_client.setex(
        f"presence:{user_id}",
        settings.presence_ttl_seconds,
        json.dumps({
            "is_online": True,
            "last_seen": time.time(),
        }),
    )


async def set_user_offline(user_id: int):
    """Mark a user as offline and record last-seen timestamp."""
    redis_client.set(
        f"presence:{user_id}",
        json.dumps({
            "is_online": False,
            "last_seen": time.time(),
        }),
    )

    # Update the database for long-term storage
    db = SessionLocal()
    try:
        from app.models import User
        from datetime import datetime, timezone
        user = db.query(User).filter(User.id == user_id).first()
        if user:
            user.is_online = False
            user.last_seen_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()


def get_user_presence(user_id: int) -> dict:
    """Get a user's online/offline status."""
    data = redis_client.get(f"presence:{user_id}")
    if data:
        return json.loads(data)
    return {"is_online": False, "last_seen": None}


def get_bulk_presence(user_ids: list[int]) -> dict[int, dict]:
    """Get presence status for multiple users using pipeline."""
    if not user_ids:
        return {}

    pipe = redis_client.pipeline()
    for uid in user_ids:
        pipe.get(f"presence:{uid}")
    results = pipe.execute()

    presence = {}
    for uid, data in zip(user_ids, results):
        if data:
            presence[uid] = json.loads(data)
        else:
            presence[uid] = {"is_online": False, "last_seen": None}

    return presence


async def broadcast_presence(user_id: int, is_online: bool):
    """
    Notify all users who share a conversation with this user
    about their online/offline status change.
    """
    db = SessionLocal()
    try:
        # Find all unique users who share at least one conversation
        user_conversation_ids = (
            db.query(Participant.conversation_id)
            .filter(Participant.user_id == user_id)
            .subquery()
        )

        related_users = (
            db.query(Participant.user_id)
            .filter(Participant.conversation_id.in_(user_conversation_ids))
            .filter(Participant.user_id != user_id)
            .distinct()
            .all()
        )

        presence_event = {
            "type": "presence",
            "payload": {
                "user_id": user_id,
                "is_online": is_online,
                "last_seen": time.time() if not is_online else None,
            },
        }

        for row in related_users:
            delivered = await manager.send_to_user(row.user_id, presence_event)
            if not delivered:
                await publish_message(row.user_id, presence_event)
    finally:
        db.close()
```

Presence uses a TTL-based heartbeat pattern in Redis. `set_user_online` stores presence with `setex`, meaning the key automatically expires after `PRESENCE_TTL_SECONDS`. The WebSocket heartbeat loop calls `set_user_online` every 30 seconds, refreshing the TTL. If the heartbeat stops (device loses connectivity, app crashes), the key expires and the user is implicitly offline. `get_bulk_presence` uses a Redis pipeline to batch-fetch presence for an entire contact list in a single round trip instead of N individual calls. `broadcast_presence` finds all users who share a conversation with the status-changing user and notifies them; this uses a subquery to find conversation IDs and then a distinct query to find unique related user IDs, avoiding duplicate notifications.

---

## Caching with Redis

### `app/cache.py`

```python
import asyncio
import json
import redis
import redis.asyncio as aioredis

from app.config import settings

# Synchronous Redis client (for cache operations)
redis_client = redis.from_url(
    settings.redis_url,
    decode_responses=True,
    socket_connect_timeout=5,
    retry_on_timeout=True,
)

# Async Redis client (for Pub/Sub operations)
async_redis_client = aioredis.from_url(
    settings.redis_url,
    decode_responses=True,
)


async def publish_message(user_id: int, message: dict):
    """
    Publish a message to a user's Redis Pub/Sub channel.
    Used for cross-server message delivery.
    """
    channel = f"user:{user_id}"
    await async_redis_client.publish(channel, json.dumps(message, default=str))


async def subscribe_to_user_channel(user_id: int):
    """
    Async generator that yields messages from a user's Pub/Sub channel.
    Used by WebSocket listeners for cross-server delivery.
    """
    pubsub = async_redis_client.pubsub()
    channel = f"user:{user_id}"

    try:
        await pubsub.subscribe(channel)

        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            if message and message["type"] == "message":
                yield json.loads(message["data"])
            else:
                await asyncio.sleep(0.1)
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()


# ── Session Cache ──────────────────────────────────────────────

SESSION_TTL = 86400                           # 24 hours


def cache_user_session(user_id: int, session_data: dict):
    """Store user session data in Redis."""
    redis_client.setex(
        f"session:{user_id}",
        SESSION_TTL,
        json.dumps(session_data, default=str),
    )


def get_user_session(user_id: int) -> dict | None:
    """Retrieve user session from cache."""
    data = redis_client.get(f"session:{user_id}")
    if data:
        return json.loads(data)
    return None


def invalidate_user_session(user_id: int):
    """Remove user session from cache."""
    redis_client.delete(f"session:{user_id}")


# ── Conversation Cache ─────────────────────────────────────────

CONVERSATION_TTL = 3600                       # 1 hour


def cache_conversation(conversation_id: int, data: dict):
    """Cache conversation metadata."""
    redis_client.setex(
        f"convo:{conversation_id}",
        CONVERSATION_TTL,
        json.dumps(data, default=str),
    )


def get_cached_conversation(conversation_id: int) -> dict | None:
    """Retrieve cached conversation."""
    data = redis_client.get(f"convo:{conversation_id}")
    if data:
        return json.loads(data)
    return None


def invalidate_conversation(conversation_id: int):
    """Remove a conversation from cache."""
    redis_client.delete(f"convo:{conversation_id}")


# ── Unread Count Cache ─────────────────────────────────────────


def increment_unread(user_id: int, conversation_id: int):
    """Increment unread message count for a user in a conversation."""
    key = f"unread:{user_id}:{conversation_id}"
    redis_client.incr(key)
    redis_client.expire(key, 86400)


def reset_unread(user_id: int, conversation_id: int):
    """Reset unread count when user reads a conversation."""
    redis_client.delete(f"unread:{user_id}:{conversation_id}")


def get_unread_count(user_id: int, conversation_id: int) -> int:
    """Get unread message count for a user in a conversation."""
    count = redis_client.get(f"unread:{user_id}:{conversation_id}")
    return int(count) if count else 0


def get_all_unread_counts(user_id: int, conversation_ids: list[int]) -> dict:
    """Get unread counts for multiple conversations using pipeline."""
    if not conversation_ids:
        return {}

    pipe = redis_client.pipeline()
    for cid in conversation_ids:
        pipe.get(f"unread:{user_id}:{cid}")
    results = pipe.execute()

    return {
        cid: int(count) if count else 0
        for cid, count in zip(conversation_ids, results)
    }
```

This module splits into two Redis clients: a synchronous one for simple cache operations and an async one for Pub/Sub. `publish_message` sends a JSON payload to a channel named `user:{user_id}`, and any WebSocket listener subscribed to that channel will receive it immediately. `subscribe_to_user_channel` is an async generator that yields messages as they arrive on the channel, integrating cleanly with the `listen_to_pubsub` coroutine in the WebSocket handler. `increment_unread` / `reset_unread` provide a fast unread badge count without querying the database; `incr` is atomic in Redis, so concurrent message deliveries will not lose counts. `get_all_unread_counts` uses a pipeline to batch-fetch unread counts for all conversations on the chat list in a single round trip.

---

## Message Queue with Redis Pub/Sub

The Pub/Sub layer is the backbone of cross-server message routing. When User A on Server 1 sends a message to User B on Server 2:

```
Server 1                  Redis Pub/Sub               Server 2
┌─────────┐              ┌─────────────┐              ┌─────────┐
│ User A  │──send msg──▶│ channel:    │──subscribe──▶│ User B  │
│ (WS)    │              │ user:B      │              │ (WS)    │
└─────────┘              └─────────────┘              └─────────┘
```

1. Server 1 receives the message via User A's WebSocket.
2. Server 1 stores the message in PostgreSQL.
3. Server 1 tries to deliver to User B locally (`manager.send_to_user`). It fails because User B is on Server 2.
4. Server 1 publishes the message to Redis channel `user:B`.
5. Server 2's `listen_to_pubsub` coroutine receives the message from the channel.
6. Server 2 pushes it down User B's WebSocket.

If User B is completely offline (not connected to any server), the message remains in PostgreSQL with status `"sent"`. When User B reconnects, `deliver_pending_messages` flushes all stored messages.

This pattern handles the distributed nature of WebSocket connections: since any server can hold any user's connection, Redis Pub/Sub acts as the message bus connecting them. In production, replace Redis Pub/Sub with Kafka or Google Cloud Pub/Sub for durability guarantees and replay capability.

---

## Media Messages & Cloud Storage

### `app/media.py`

```python
import uuid
from io import BytesIO

from google.cloud import storage

from app.config import settings


def get_storage_client():
    """Create a GCS client (uses Application Default Credentials)."""
    return storage.Client()


def upload_media(file_content: bytes, content_type: str, prefix: str = "media") -> str:
    """
    Upload media to Google Cloud Storage and return the CDN URL.
    Generates a unique filename to prevent collisions.
    """
    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)

    ext = content_type.split("/")[-1]
    filename = f"{prefix}/{uuid.uuid4().hex}.{ext}"

    blob = bucket.blob(filename)
    blob.upload_from_string(file_content, content_type=content_type)
    blob.make_public()

    return f"{settings.cdn_base_url}/{filename}"


def generate_signed_url(blob_name: str, expiration_minutes: int = 60) -> str:
    """
    Generate a signed URL for private media access.
    The URL expires after the specified number of minutes.
    """
    import datetime

    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(minutes=expiration_minutes),
        method="GET",
    )

    return url


def generate_upload_url(content_type: str) -> dict:
    """
    Generate a pre-signed upload URL for direct client-to-GCS uploads.
    This avoids sending large files through the API server.
    """
    import datetime

    client = get_storage_client()
    bucket = client.bucket(settings.gcs_bucket_name)

    ext = content_type.split("/")[-1]
    blob_name = f"media/{uuid.uuid4().hex}.{ext}"
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(hours=1),
        method="PUT",
        content_type=content_type,
    )

    return {
        "upload_url": url,
        "media_url": f"{settings.cdn_base_url}/{blob_name}",
        "blob_name": blob_name,
    }
```

`generate_signed_url` creates a time-limited URL for private media access, avoiding making all files public. In WhatsApp's architecture, media is end-to-end encrypted, so the server only stores opaque ciphertext. `generate_upload_url` creates a pre-signed PUT URL so clients can upload files directly to Cloud Storage without proxying through the API server. This is critical for performance: a 10 MB video should not pass through the chat server. The flow is: (1) client requests a pre-signed URL, (2) client uploads directly to GCS, (3) client sends a message with the resulting `media_url`.

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

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
```

This uses a multi-stage build. The `builder` stage installs dependencies into `/install`. The second stage copies only the installed packages. `--workers 1` is used instead of multiple workers because WebSocket connections are stateful and pinned to the process holding the connection. Scaling is done horizontally by running multiple container replicas behind a load balancer with sticky sessions.

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
  chat:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/whatsapp
      - REDIS_URL=redis://cache:6379/0
      - GCS_BUCKET_NAME=whatsapp-media-bucket
      - CDN_BASE_URL=http://localhost:8000/static
      - JWT_SECRET_KEY=dev-secret-key
      - MAX_GROUP_SIZE=256
      - PRESENCE_TTL_SECONDS=120
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
    restart: unless-stopped

  chat-2:
    build: .
    ports:
      - "8001:8000"
    environment:
      - DATABASE_URL=postgresql://user:pass@db:5432/whatsapp
      - REDIS_URL=redis://cache:6379/0
      - GCS_BUCKET_NAME=whatsapp-media-bucket
      - CDN_BASE_URL=http://localhost:8000/static
      - JWT_SECRET_KEY=dev-secret-key
      - MAX_GROUP_SIZE=256
      - PRESENCE_TTL_SECONDS=120
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
      - POSTGRES_DB=whatsapp
    volumes:
      - db-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d whatsapp"]
      interval: 5s
      timeout: 5s
      retries: 5

  cache:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes --maxmemory 512mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  db-data:
  redis-data:
```

Two chat server instances (`chat` on port 8000, `chat-2` on port 8001) demonstrate multi-server architecture. Users connected to `chat` can send messages to users connected to `chat-2` via Redis Pub/Sub. In production, you would use a load balancer with WebSocket affinity (sticky sessions) in front of N replicas. `--appendonly yes` enables Redis AOF persistence. `--maxmemory 512mb` with `allkeys-lru` eviction prevents OOM kills while preserving the most recently used data.

### `app/main.py`

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.routers import auth, messages, conversations, groups, ws


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create tables on startup."""
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="WhatsApp Messaging API",
    description="A production-ready real-time messaging service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(messages.router, prefix="/api", tags=["Messages"])
app.include_router(conversations.router, prefix="/api", tags=["Conversations"])
app.include_router(groups.router, prefix="/api", tags=["Groups"])
app.include_router(ws.router, tags=["WebSocket"])


@app.get("/health")
def health_check():
    return {"status": "healthy"}
```

`lifespan` creates tables on startup. The WebSocket router is mounted without a prefix so the endpoint is at `/ws/{user_id}`. All REST routers are under `/api`.

### `app/routers/auth.py`

```python
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from jose import jwt
from passlib.context import CryptContext
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import User
from app.schemas import UserCreate, UserResponse, LoginRequest, TokenResponse

router = APIRouter()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@router.post("/register", response_model=UserResponse, status_code=201)
def register(payload: UserCreate, db: Session = Depends(get_db)):
    """Register a new user."""
    if db.query(User).filter(User.phone == payload.phone).first():
        raise HTTPException(status_code=409, detail="Phone already registered.")
    if db.query(User).filter(User.username == payload.username).first():
        raise HTTPException(status_code=409, detail="Username taken.")

    user = User(
        phone=payload.phone,
        username=payload.username,
        display_name=payload.display_name,
        password_hash=pwd_context.hash(payload.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate and get a JWT token."""
    user = db.query(User).filter(User.username == payload.username).first()
    if not user or not pwd_context.verify(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    expires = datetime.now(timezone.utc) + timedelta(
        minutes=settings.access_token_expire_minutes
    )
    token = jwt.encode(
        {"sub": str(user.id), "exp": expires},
        settings.jwt_secret_key,
        algorithm=settings.jwt_algorithm,
    )

    return TokenResponse(access_token=token)
```

`CryptContext` from `passlib` handles bcrypt hashing with automatic salt generation. `jwt.encode` creates a signed JWT token with the user's ID in the `sub` claim and an expiration timestamp. In production, add refresh tokens, token revocation via Redis, and use an identity provider like Firebase Auth or Auth0.

### Run the Full Stack

```bash
docker compose up --build                     # Build images and start all services
docker compose logs -f chat                   # Follow chat server logs
```

Visit `http://localhost:8000/docs` for the interactive API documentation. Test the API:

```bash
# Register users
curl -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"phone": "+1234567890", "username": "alice", "display_name": "Alice", "password": "password123"}'

curl -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"phone": "+0987654321", "username": "bob", "display_name": "Bob", "password": "password123"}'

# Login
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "password": "password123"}'

# Create a direct conversation
curl -X POST "http://localhost:8000/api/conversations?user_id=1" \
  -H "Content-Type: application/json" \
  -d '{"participant_ids": [2]}'

# Connect via WebSocket (use wscat or Postman)
wscat -c ws://localhost:8000/ws/1

# Send a message via WebSocket
{"type": "message", "payload": {"conversation_id": 1, "content": "Hello Bob!"}}

# Get message history via REST
curl "http://localhost:8000/api/conversations/1/messages?user_id=1"

# Create a group
curl -X POST "http://localhost:8000/api/conversations?user_id=1" \
  -H "Content-Type: application/json" \
  -d '{"participant_ids": [2], "name": "Project Chat"}'
```

---

## Testing

### `tests/conftest.py`

```python
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import patch, MagicMock, AsyncMock

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

    # Mock Redis operations
    with patch("app.cache.redis_client", MagicMock()), \
         patch("app.cache.publish_message", new_callable=AsyncMock), \
         patch("app.presence.redis_client", MagicMock()):
        yield TestClient(app)

    app.dependency_overrides.clear()


def create_test_user(client, username="testuser", phone="+1234567890"):
    """Helper to register a test user."""
    response = client.post("/api/auth/register", json={
        "phone": phone,
        "username": username,
        "display_name": username.title(),
        "password": "password123",
    })
    return response.json()
```

Tests use SQLite for speed and mock Redis with `MagicMock` since Pub/Sub, presence, and caching are not needed in unit tests. `AsyncMock` is used for `publish_message` because it is an async function. `create_test_user` is a helper that other tests call to quickly set up test users.

### `tests/test_messages.py`

```python
from tests.conftest import create_test_user


def test_create_conversation(client, db):
    """Test creating a direct conversation."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")

    response = client.post(
        "/api/conversations?user_id=1",
        json={"participant_ids": [2]},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["type"] == "direct"
    assert len(data["participants"]) == 2


def test_get_message_history(client, db):
    """Test retrieving message history."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")

    client.post("/api/conversations?user_id=1", json={"participant_ids": [2]})

    response = client.get("/api/conversations/1/messages?user_id=1")
    assert response.status_code == 200
    data = response.json()
    assert "messages" in data
    assert "next_cursor" in data
    assert "has_more" in data


def test_non_participant_blocked(client, db):
    """Test that non-participants cannot access messages."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")
    create_test_user(client, "eve", "+3333333333")

    client.post("/api/conversations?user_id=1", json={"participant_ids": [2]})

    response = client.get("/api/conversations/1/messages?user_id=3")
    assert response.status_code == 403


def test_delete_message_unauthorized(client, db):
    """Test that only the sender can delete a message."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")

    response = client.delete("/api/messages/999?user_id=1")
    assert response.status_code == 404
```

### `tests/test_receipts.py`

```python
from app.receipts import should_update_status


def test_status_advances_forward():
    """Test that status only moves forward."""
    assert should_update_status("sent", "delivered") is True
    assert should_update_status("sent", "read") is True
    assert should_update_status("delivered", "read") is True


def test_status_does_not_go_backward():
    """Test that status cannot revert."""
    assert should_update_status("delivered", "sent") is False
    assert should_update_status("read", "delivered") is False
    assert should_update_status("read", "sent") is False


def test_same_status_is_no_op():
    """Test that updating to the same status is rejected."""
    assert should_update_status("sent", "sent") is False
    assert should_update_status("delivered", "delivered") is False
    assert should_update_status("read", "read") is False
```

### `tests/test_groups.py`

```python
from tests.conftest import create_test_user


def test_create_group(client, db):
    """Test creating a group conversation."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")
    create_test_user(client, "charlie", "+3333333333")

    response = client.post(
        "/api/conversations?user_id=1",
        json={"participant_ids": [2, 3], "name": "Friends"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["type"] == "group"
    assert data["name"] == "Friends"
    assert len(data["participants"]) == 3


def test_group_requires_name(client, db):
    """Test that groups require a name."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")
    create_test_user(client, "charlie", "+3333333333")

    response = client.post(
        "/api/conversations?user_id=1",
        json={"participant_ids": [2, 3]},
    )
    assert response.status_code == 400


def test_leave_group(client, db):
    """Test leaving a group."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")

    client.post(
        "/api/conversations?user_id=1",
        json={"participant_ids": [2], "name": "Duo"},
    )

    response = client.post("/api/conversations/1/leave?user_id=2")
    assert response.status_code == 200


def test_cannot_leave_direct(client, db):
    """Test that users cannot leave a direct conversation."""
    create_test_user(client, "alice", "+1111111111")
    create_test_user(client, "bob", "+2222222222")

    client.post("/api/conversations?user_id=1", json={"participant_ids": [2]})

    response = client.post("/api/conversations/1/leave?user_id=2")
    assert response.status_code == 400
```

### `tests/test_websocket.py`

```python
from unittest.mock import patch, AsyncMock, MagicMock

from tests.conftest import create_test_user


def test_websocket_connect(client, db):
    """Test WebSocket connection and disconnection."""
    create_test_user(client, "alice", "+1111111111")

    with patch("app.routers.ws.set_user_online", new_callable=AsyncMock), \
         patch("app.routers.ws.set_user_offline", new_callable=AsyncMock), \
         patch("app.routers.ws.broadcast_presence", new_callable=AsyncMock), \
         patch("app.routers.ws.deliver_pending_messages", new_callable=AsyncMock), \
         patch("app.routers.ws.subscribe_to_user_channel", return_value=AsyncIterMock()):

        with client.websocket_connect("/ws/1") as websocket:
            websocket.send_json({"type": "ping"})


class AsyncIterMock:
    """Mock async iterator for Pub/Sub subscription."""
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration
```

### Run Tests

```bash
pytest -v                                     # Verbose output
pytest --tb=short                             # Shorter tracebacks
pytest -k "test_group"                        # Run only group tests
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
# Create a Cloud SQL instance with high availability
gcloud sql instances create whatsapp-db \
  --database-version=POSTGRES_16 \
  --tier=db-custom-4-15360 \
  --region=us-central1 \
  --storage-size=100GB \
  --storage-auto-increase \
  --availability-type=REGIONAL

# Create the database
gcloud sql databases create whatsapp \
  --instance=whatsapp-db

# Set the user password
gcloud sql users set-password postgres \
  --instance=whatsapp-db \
  --password=YOUR_SECURE_PASSWORD
```

`--tier=db-custom-4-15360` provisions 4 vCPUs and 15 GB RAM to handle the high write throughput of a messaging system. `--availability-type=REGIONAL` enables automatic failover for 99.95% SLA. `--storage-auto-increase` adds storage automatically as messages accumulate.

### Set Up Memorystore (Redis)

```bash
# Create a Redis instance for Pub/Sub and caching
gcloud redis instances create whatsapp-cache \
  --size=5 \
  --region=us-central1 \
  --redis-version=redis_7_0 \
  --network=default \
  --tier=STANDARD_HA

# Get the Redis host IP
gcloud redis instances describe whatsapp-cache \
  --region=us-central1 \
  --format="value(host)"
```

`--size=5` provisions 5 GB for Pub/Sub channels, presence data, session caches, and unread counts. `--tier=STANDARD_HA` provides automatic failover. Redis Pub/Sub channels do not persist data, but the presence and session caches benefit from HA.

### Set Up Cloud Storage (Media)

```bash
# Create a bucket for media files
gsutil mb -l us-central1 gs://YOUR_PROJECT_ID-whatsapp-media

# Set lifecycle policy to auto-delete old media (optional)
cat > lifecycle.json << 'EOF'
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 365}
    }
  ]
}
EOF
gsutil lifecycle set lifecycle.json gs://YOUR_PROJECT_ID-whatsapp-media
```

The lifecycle policy automatically deletes media older than 365 days, reducing storage costs.

### Build & Push the Docker Image

```bash
# Build and push to Artifact Registry
gcloud builds submit --tag us-central1-docker.pkg.dev/YOUR_PROJECT_ID/whatsapp/chat
```

`gcloud builds submit` uploads source code and builds the Docker image using Cloud Build. The image is pushed directly to Artifact Registry.

### Deploy to Cloud Run

```bash
gcloud run deploy whatsapp-chat \
  --image us-central1-docker.pkg.dev/YOUR_PROJECT_ID/whatsapp/chat \
  --platform managed \
  --region us-central1 \
  --port 8000 \
  --memory 1Gi \
  --cpu 2 \
  --min-instances 3 \
  --max-instances 50 \
  --timeout 3600 \
  --session-affinity \
  --allow-unauthenticated \
  --add-cloudsql-instances YOUR_PROJECT_ID:us-central1:whatsapp-db \
  --set-env-vars "\
DATABASE_URL=postgresql://postgres:YOUR_SECURE_PASSWORD@/whatsapp?host=/cloudsql/YOUR_PROJECT_ID:us-central1:whatsapp-db,\
REDIS_URL=redis://REDIS_HOST_IP:6379/0,\
GCS_BUCKET_NAME=YOUR_PROJECT_ID-whatsapp-media,\
CDN_BASE_URL=https://cdn.yourdomain.com,\
JWT_SECRET_KEY=USE_SECRET_MANAGER_IN_PRODUCTION,\
MAX_GROUP_SIZE=256,\
PRESENCE_TTL_SECONDS=120"
```

`--timeout 3600` sets the request timeout to 1 hour, necessary for long-lived WebSocket connections (the default 5-minute timeout would close them). `--session-affinity` enables sticky sessions, routing a user's subsequent requests to the same instance where their WebSocket is connected. `--min-instances 3` keeps three instances warm for low-latency WebSocket handshakes. `--max-instances 50` caps scaling to control costs.

### Set Up Custom Domain

```bash
gcloud run domain-mappings create \
  --service whatsapp-chat \
  --domain chat.yourdomain.com \
  --region us-central1
```

After running this command, GCP provides DNS records to add at your domain registrar. GCP automatically provisions and renews SSL certificates.

---

## Scaling & Production Considerations

### Database Scaling

| Strategy              | When to Use                                | How                                              |
| --------------------- | ------------------------------------------ | ------------------------------------------------ |
| Read Replicas         | Message history reads outpace writes       | Cloud SQL read replicas for GET endpoints         |
| Connection Pooling    | Many WebSocket servers × many DB conns     | PgBouncer sidecar or Cloud SQL Auth Proxy pooling |
| Table Partitioning    | `messages` table grows past 1B rows        | Partition by `created_at` month using range parts |
| Sharding              | Single DB can't handle write volume        | Shard by `conversation_id` across multiple DBs    |
| Archive Old Messages  | Storage costs grow unsustainably           | Move messages older than 1 year to cold storage   |

### WebSocket Scaling

| Strategy              | When to Use                                | How                                              |
| --------------------- | ------------------------------------------ | ------------------------------------------------ |
| Horizontal Scaling    | Single server can't hold all connections   | Multiple instances + sticky sessions + Pub/Sub    |
| Connection Limits     | Memory per connection limits capacity      | ~50K connections per server (tune OS limits)       |
| Kafka Replace Pub/Sub | Redis Pub/Sub drops messages under load    | Kafka for durable, replayable message routing     |
| Regional Deployment   | Global user base needs low latency         | Deploy chat servers in multiple GCP regions       |

### Message Delivery Guarantees

```
At-Most-Once   → Fire and forget. Messages may be lost.     ✗ Not acceptable
At-Least-Once  → Retry until ack. Messages may duplicate.   ✓ WhatsApp uses this
Exactly-Once   → Dedup on client. Best UX.                  ✓ Ideal, complex
```

WhatsApp uses **at-least-once delivery** with client-side deduplication. Each message has a unique ID; the client ignores messages it has already received. The server retries delivery until it receives an acknowledgement from the recipient.

### Architecture Improvements for Production

```
                     ┌────────────────┐
                     │  Cloud DNS     │
                     │ chat.app.com   │
                     └──────┬─────────┘
                            │
                     ┌──────▼─────────┐
                     │ Cloud Load     │
                     │ Balancer       │
                     │ (WebSocket     │
                     │  affinity)     │
                     └──────┬─────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
       ┌──────▼──────┐ ┌───▼────┐ ┌──────▼──────┐
       │ Chat Server │ │Chat    │ │ Chat Server │
       │ Instance 1  │ │Srv #2  │ │ Instance N  │
       │ (WebSocket) │ │        │ │ (WebSocket) │
       └──────┬──────┘ └───┬────┘ └──────┬──────┘
              │             │             │
         ┌────▼─────────────▼─────────────▼────┐
         │            VPC Network              │
         │                                      │
         │  ┌─────────────────────────────┐    │
         │  │  Kafka / Cloud Pub/Sub      │    │
         │  │  (Cross-server routing +    │    │
         │  │   durable message queue)    │    │
         │  └─────────────────────────────┘    │
         │                                      │
         │  ┌─────────┐    ┌────────────────┐  │
         │  │ Memory- │    │   Cloud SQL    │  │
         │  │ store   │    │ (Primary +     │  │
         │  │ Cluster │    │  Read Replica) │  │
         │  │ (Redis) │    │  Sharded by    │  │
         │  │ Presence│    │  convo hash    │  │
         │  │ +Session│    └────────────────┘  │
         │  └─────────┘                        │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Cloud Storage + CDN          │   │
         │  │  (Media: encrypted blobs)     │   │
         │  └───────────────────────────────┘   │
         │                                      │
         │  ┌───────────────────────────────┐   │
         │  │  Firebase Cloud Messaging     │   │
         │  │  (Push notifications for      │   │
         │  │   offline users)              │   │
         │  └───────────────────────────────┘   │
         └──────────────────────────────────────┘
```

**Key improvements over the basic architecture:**

- **Kafka / Cloud Pub/Sub** replaces Redis Pub/Sub for cross-server routing. Kafka provides durable, replayable message streams and handles millions of messages per second. Redis Pub/Sub is fire-and-forget; if no subscriber is listening, the message is lost.
- **Database Sharding** by `conversation_id` distributes write load across multiple Cloud SQL instances. All messages in a conversation go to the same shard, preserving ordering.
- **Redis Cluster** (Memorystore cluster mode) handles presence, sessions, and unread counts across multiple nodes, scaling beyond single-node memory.
- **Firebase Cloud Messaging (FCM)** sends push notifications to mobile devices when users are offline. The chat server publishes to FCM after a message is stored and delivery fails.
- **Cloud Storage + CDN** serves media globally. In production, media is end-to-end encrypted; the server stores opaque ciphertext that only the sender and recipient can decrypt.
- **Regional Deployment** places chat servers near users. A user in Asia connects to an Asia-Pacific instance; messages to a user in Europe are routed through Kafka across regions.

### Security Checklist

```
✓ End-to-end encrypt all messages (Signal Protocol)
✓ Authenticate WebSocket connections with JWT tokens
✓ Validate and sanitize all message content
✓ Rate limit by user ID (message frequency, connection attempts)
✓ Use TLS 1.3 for all connections (Cloud Run provides this)
✓ Store secrets in Secret Manager, not environment variables
✓ Encrypt media at rest in Cloud Storage (default GCS encryption)
✓ Implement account lockout after failed login attempts
✓ Enable Cloud Audit Logs for compliance (GDPR, HIPAA)
✓ Scan media for CSAM (Cloud Vision API SafeSearch)
✓ Implement message retention policies and right-to-delete
```

### Monitoring & Alerting

```bash
# Create an uptime check
gcloud monitoring uptime create whatsapp-health \
  --display-name="WhatsApp Health" \
  --uri="https://your-service-url.run.app/health" \
  --period=60

# Key metrics to alert on:
# - WebSocket connection count (per instance and total)
# - Message delivery latency p50, p95, p99
# - Delivery failure rate > 0.1%
# - Error rate > 1% over 5 minutes
# - Redis Pub/Sub channel count and message rate
# - Cloud SQL connections near pool limit
# - Redis memory usage > 80%
# - Kafka consumer lag > 10,000 messages
# - Instance count near max-instances
# - WebSocket handshake failure rate
```

Cloud Run exports metrics to Cloud Monitoring. Set up alert policies to notify via PagerDuty, Slack, or email when thresholds are breached. Use Cloud Trace for distributed tracing across the chat server → Redis Pub/Sub → database chain. Monitor WebSocket health by tracking connection duration, message throughput per connection, and abnormal disconnection rates.
