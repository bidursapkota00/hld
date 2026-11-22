def test_redirect(client):
    """Test that short codes redirect to the original URL."""
    create = client.post("/shorten", json={"url": "https://www.example.com"})
    short_code = create.json()["short_code"]

    response = client.get(f"/{short_code}", follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == "https://www.example.com/"


def test_redirect_not_found(client):
    """Test 404 for non-existent short codes."""
    response = client.get("/nonexistent", follow_redirects=False)
    assert response.status_code == 404