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