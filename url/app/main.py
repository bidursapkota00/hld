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