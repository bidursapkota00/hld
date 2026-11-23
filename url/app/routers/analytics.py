from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import URL, ClickEvent
from app.schemas import URLStats, ClickEventResponse

router = APIRouter()


@router.get("/{short_code}", response_model=URLStats)
def get_url_stats(short_code: str, db: Session = Depends(get_db)):
    """Get statistics for a shortened URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    return url_record


@router.get("/{short_code}/clicks", response_model=list[ClickEventResponse])
def get_click_events(
    short_code: str,
    skip: int = Query(0, ge=0),               # Pagination offset
    limit: int = Query(50, ge=1, le=200),      # Page size (max 200)
    db: Session = Depends(get_db),
):
    """Get individual click events for a shortened URL."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    clicks = (
        db.query(ClickEvent)
        .filter(ClickEvent.url_id == url_record.id)
        .order_by(ClickEvent.clicked_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return clicks


@router.get("/{short_code}/summary")
def get_click_summary(short_code: str, db: Session = Depends(get_db)):
    """Get aggregated click analytics."""
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    if not url_record:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    # Clicks per day for the last 30 days
    daily_clicks = (
        db.query(
            func.date(ClickEvent.clicked_at).label("date"),
            func.count().label("count"),
        )
        .filter(ClickEvent.url_id == url_record.id)
        .group_by(func.date(ClickEvent.clicked_at))
        .order_by(func.date(ClickEvent.clicked_at).desc())
        .limit(30)
        .all()
    )

    # Top referrers
    top_referrers = (
        db.query(
            ClickEvent.referrer,
            func.count().label("count"),
        )
        .filter(ClickEvent.url_id == url_record.id)
        .filter(ClickEvent.referrer.isnot(None))
        .group_by(ClickEvent.referrer)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )

    # Top countries
    top_countries = (
        db.query(
            ClickEvent.country,
            func.count().label("count"),
        )
        .filter(ClickEvent.url_id == url_record.id)
        .filter(ClickEvent.country.isnot(None))
        .group_by(ClickEvent.country)
        .order_by(func.count().desc())
        .limit(10)
        .all()
    )

    return {
        "short_code": short_code,
        "total_clicks": url_record.click_count,
        "daily_clicks": [{"date": str(d.date), "count": d.count} for d in daily_clicks],
        "top_referrers": [{"referrer": r.referrer, "count": r.count} for r in top_referrers],
        "top_countries": [{"country": c.country, "count": c.count} for c in top_countries],
    }