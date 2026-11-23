from datetime import datetime, timedelta, timezone
from app.models import URL


def test_redirect(client):
    """Test that valid short codes redirect to the original URL."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == "https://www.example.com/"


def test_redirect_not_found(client):
    """Test 404 for non-existent short codes."""
    response = client.get("/nonexistent", follow_redirects=False)
    assert response.status_code == 404


def test_redirect_expired_url_returns_410(client, db):
    """Test 410 Gone for expired links."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    # Manually backdate expiration in the database
    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    url_record.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
    db.commit()

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 410
    assert response.json()["detail"] == "This link has expired."


def test_redirect_deactivated_url_returns_410(client, db):
    """Test 410 Gone for deactivated links."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    url_record = db.query(URL).filter(URL.short_code == short_code).first()
    url_record.is_active = False
    db.commit()

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 410
    assert response.json()["detail"] == "This link has been deactivated."

def test_click_count_increments(client):
    """Test that clicks are tracked."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    client.get(f"/{short_code}", follow_redirects=False)
    client.get(f"/{short_code}", follow_redirects=False)

    stats = client.get(f"/api/analytics/{short_code}")
    assert stats.json()["click_count"] == 2
