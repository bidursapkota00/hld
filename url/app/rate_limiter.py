import time

from fastapi import Request
from fastapi.responses import JSONResponse
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
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded. Max {settings.rate_limit_per_minute} requests per minute."
                },
                headers={
                    "Retry-After": "60",
                    "X-RateLimit-Limit": str(settings.rate_limit_per_minute),
                    "X-RateLimit-Remaining": "0",
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(
            max(0, settings.rate_limit_per_minute - request_count)
        )
        return response