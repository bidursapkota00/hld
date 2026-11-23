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