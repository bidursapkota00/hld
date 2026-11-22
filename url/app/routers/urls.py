import uuid
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_from_hash, generate_snowflake_code
from app.shortener import is_valid_alias


router = APIRouter()


@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
    """Create a shortened URL using Snowflake ID or custom alias."""
    original_url = str(payload.url)

    # Handle custom alias or generate code in-memory
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

    # Direct single-step insert (no temporary UUID or db.flush required)
    url_record = URL(
        short_code=short_code,
        original_url=original_url,
    )
    db.add(url_record)
    db.commit()
    db.refresh(url_record)

    return URLResponse(
        short_code=url_record.short_code,
        short_url=f"{settings.base_url}/{url_record.short_code}",
        original_url=url_record.original_url,
        created_at=url_record.created_at,
    )

@router.get("/{short_code}")
def redirect_to_url(short_code: str, request: Request, db: Session = Depends(get_db)):
    """Redirect a short code to its original URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()

    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    return RedirectResponse(
        url=url_record.original_url,
        status_code=301,                      # Permanent redirect (cacheable)
    )