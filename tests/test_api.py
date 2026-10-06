import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from src.api.main import app

client = TestClient(app)


def test_health_check():
    """Verify that the /health probe returns 200 OK and status ok."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "timestamp" in data


def test_place_order_validation_error():
    """Verify that malformed order requests are rejected with 422 Unprocessable Entity."""
    # Empty items list violates min_length=1
    bad_payload = {
        "outlet_id": "outlet-dxb-01",
        "customer_name": "Test Customer",
        "items": []
    }
    response = client.post("/orders", json=bad_payload)
    assert response.status_code == 422


def test_place_order_negative_quantity_error():
    """Verify that item quantity <= 0 is rejected with 422."""
    bad_payload = {
        "outlet_id": "outlet-dxb-01",
        "customer_name": "Test Customer",
        "items": [
            {"item_id": "burger-01", "name": "Zinger Burger", "quantity": 0}
        ]
    }
    response = client.post("/orders", json=bad_payload)
    assert response.status_code == 422


def test_get_menu_redis_cache_hit():
    """Verify that /menu returns Redis cache data immediately when key exists."""
    fake_menu = [{"item_id": "test-1", "name": "Test Item", "price": 1.0, "category": "Test"}]
    import json

    with patch("src.api.main.redis_client") as mock_redis:
        mock_redis.get.return_value = json.dumps(fake_menu)
        response = client.get("/menu")
        assert response.status_code == 200
        data = response.json()
        assert data["source"] == "redis_cache"
        assert len(data["items"]) == 1
        assert data["items"][0]["name"] == "Test Item"
