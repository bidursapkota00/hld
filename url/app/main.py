from fastapi import FastAPI

from app.routers import urls
from app.rate_limiter import RateLimiterMiddleware


app = FastAPI(
    title="URL Shortener API",
    description="A production-ready URL shortening service",
    version="1.0.0",
)

app.add_middleware(RateLimiterMiddleware)


@app.get("/health")
def health_check():
    return {"status": "healthy"}

app.include_router(urls.router)