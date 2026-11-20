def test_shorten_url(client):
    """Test creating a shortened URL."""
    response = client.post("/shorten", json={"url": "https://www.example.com"})
    assert response.status_code == 201
    data = response.json()
    assert "short_code" in data
    assert "short_url" in data
    assert data["original_url"] == "https://www.example.com/"


def test_invalid_url_rejected(client):
    """Test that invalid URLs are rejected."""
    response = client.post("/shorten", json={"url": "not-a-url"})
    assert response.status_code == 422