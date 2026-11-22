def test_shorten_url(client):
    """Test creating a shortened URL with default expiration."""
    response = client.post("/shorten", json={"url": "https://www.example.com"})
    assert response.status_code == 201
    data = response.json()
    assert "short_code" in data
    assert "short_url" in data
    assert data["original_url"] == "https://www.example.com/"
    assert data["expires_at"] is not None


def test_shorten_with_custom_ttl(client):
    """Test creating a shortened URL with custom TTL in days."""
    response = client.post(
        "/shorten",
        json={"url": "https://www.example.com", "expires_in_days": 7},
    )
    assert response.status_code == 201
    assert response.json()["expires_at"] is not None


def test_shorten_with_custom_alias(client):
    """Test creating a URL with a custom alias."""
    response = client.post(
        "/shorten",
        json={"url": "https://www.example.com", "custom_alias": "mylink"},
    )
    assert response.status_code == 201
    assert response.json()["short_code"] == "mylink"


def test_duplicate_alias_rejected(client):
    """Test that duplicate custom aliases are rejected."""
    client.post(
        "/shorten",
        json={"url": "https://www.example.com", "custom_alias": "taken"},
    )
    response = client.post(
        "/shorten",
        json={"url": "https://www.other.com", "custom_alias": "taken"},
    )
    assert response.status_code == 409


def test_invalid_alias_rejected(client):
    """Test that invalid aliases are rejected."""
    response = client.post(
        "/shorten",
        json={"url": "https://www.example.com", "custom_alias": "a"},
    )
    assert response.status_code == 400


def test_invalid_url_rejected(client):
    """Test that invalid URLs are rejected."""
    response = client.post("/shorten", json={"url": "not-a-url"})
    assert response.status_code == 422