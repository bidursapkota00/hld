from datetime import datetime, timezone

from sqlalchemy import update

from app.database import SessionLocal
from app.models import URL
from app.cache import delete_cached_url


def deactivate_expired_urls():
    """Mark expired URLs as inactive and clear their cache entries."""
    db = SessionLocal()
    try:
        expired = (
            db.query(URL)
            .filter(URL.expires_at < datetime.now(timezone.utc))
            .filter(URL.is_active == True)
            .all()
        )

        for url in expired:
            url.is_active = False
            delete_cached_url(url.short_code)

        db.commit()
        print(f"Deactivated {len(expired)} expired URLs.")
    finally:
        db.close()


if __name__ == "__main__":
    deactivate_expired_urls()