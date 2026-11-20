import uuid
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import URL
from app.schemas import URLCreate, URLResponse
from app.shortener import generate_short_code

router = APIRouter()


@router.post("/shorten", response_model=URLResponse, status_code=201)
def shorten_url(payload: URLCreate, request: Request, db: Session = Depends(get_db)):
    """Create a shortened URL."""
    original_url = str(payload.url)

    # Create the URL record with a temporary short code (max 10 chars)
    url_record = URL(
        short_code=uuid.uuid4().hex[:8],      # Temporary unique code, replaced below
        original_url=original_url,
    )

    db.add(url_record)
    db.flush()                                # Get the auto-generated ID

    # Generate short code from ID
    url_record.short_code = generate_short_code(url_record.id)

    db.commit()
    db.refresh(url_record)

    return URLResponse(
        short_code=url_record.short_code,
        short_url=f"{settings.base_url}/{url_record.short_code}",
        original_url=url_record.original_url,
        created_at=url_record.created_at,
    )