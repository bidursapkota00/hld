from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_snowflake_code, is_valid_alias
from app.cache import get_cached_url, set_cached_url, increment_click_count

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
    """Redirect with Redis cache layer."""

    # 1. Check cache first
    cached = get_cached_url(short_code)
    if cached:
        # increment_click_count(short_code)
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

    # 4. Update count and redirect
    # url_record.click_count += 1
    db.commit()

    return RedirectResponse(url=url_record.original_url, status_code=301)