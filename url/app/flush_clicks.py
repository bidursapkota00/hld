
from app.database import SessionLocal
from app.models import URL
from app.cache import redis_client


def flush_click_counts():
    """Sync Redis click counters back to PostgreSQL's click_count column.

    Redis key pattern: clicks:<short_code>  (set by increment_click_count)
    """
    db = SessionLocal()
    try:
        # 1. Discover all pending counters
        keys = redis_client.keys("clicks:*")
        if not keys:
            print("No pending click counters to flush.")
            return

        flushed = 0
        for key in keys:
            short_code = key.removeprefix("clicks:")

            # 2. Atomically read-and-delete so we never double-count
            delta = redis_client.getdel(key)
            if not delta:
                continue  # another process beat us to it
            delta = int(delta)

            # 3. Apply delta to Postgres
            updated = (
                db.query(URL)
                .filter(URL.short_code == short_code)
                .update({"click_count": URL.click_count + delta})
            )
            if updated:
                flushed += 1
                print(f"  {short_code}: +{delta} clicks written to DB")
            else:
                # short_code was deleted from DB but key lingered in Redis — ignore
                print(f"  {short_code}: skipped (URL not found in DB)")

        db.commit()
        print(f"Flushed click counts for {flushed} URL(s).")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    flush_click_counts()