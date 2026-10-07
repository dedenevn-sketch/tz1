"""Создание и чтение платежа: ключ API, идемпотентность, валидация."""

from datetime import datetime
from uuid import uuid4

import pytest

from tests.conftest import auth_headers, payment_body


async def test_create_requires_api_key(client):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers={"Idempotency-Key": "idem-1"},
    )
    assert response.status_code == 401


async def test_create_rejects_wrong_api_key(client):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers=auth_headers(api_key="wrong"),
    )
    assert response.status_code == 401


async def test_get_requires_api_key(client):
    created = await client.post("/api/v1/payments", json=payment_body(), headers=auth_headers())
    response = await client.get(f"/api/v1/payments/{created.json()['payment_id']}")
    assert response.status_code == 401


async def test_create_requires_idempotency_key(client):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers={"X-API-Key": "test-api-key"},
    )
    assert response.status_code == 400


async def test_create_rejects_blank_idempotency_key(client):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(),
        headers=auth_headers("   "),
    )
    assert response.status_code == 400


async def test_create_payment_returns_accepted(client):
    response = await client.post("/api/v1/payments", json=payment_body(), headers=auth_headers())
    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    datetime.fromisoformat(body["created_at"])


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
async def test_supported_currencies(client, currency):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(currency=currency),
        headers=auth_headers(f"cur-{currency}"),
    )
    assert response.status_code == 202


@pytest.mark.parametrize(
    "overrides",
    [
        {"currency": "GBP"},
        {"amount": "0"},
        {"amount": "-1.00"},
        {"amount": "10.555"},
        {"metadata": ["order"]},
        {"webhook_url": "ftp://merchant.example/hook"},
        {"description": ""},
    ],
)
async def test_create_rejects_invalid_body(client, overrides):
    response = await client.post(
        "/api/v1/payments",
        json=payment_body(**overrides),
        headers=auth_headers(f"bad-{overrides}"),
    )
    assert response.status_code == 422


async def test_numeric_amount_is_stored_with_two_decimals(client):
    body = payment_body()
    body["amount"] = 10.5
    created = await client.post("/api/v1/payments", json=body, headers=auth_headers("amount-float"))
    assert created.status_code == 202
    fetched = await client.get(
        f"/api/v1/payments/{created.json()['payment_id']}",
        headers={"X-API-Key": "test-api-key"},
    )
    assert fetched.json()["amount"] == "10.50"


async def test_get_returns_payment_details(client):
    created = await client.post("/api/v1/payments", json=payment_body(), headers=auth_headers("get-1"))
    payment_id = created.json()["payment_id"]
    response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": "test-api-key"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "10.50"
    assert body["currency"] == "RUB"
    assert body["description"] == "Заказ 15"
    assert body["metadata"] == {"order_id": "15"}
    assert body["status"] == "pending"
    assert body["idempotency_key"] == "get-1"
    assert body["webhook_url"] == "https://merchant.example/hooks/payment"
    assert body["processed_at"] is None
    datetime.fromisoformat(body["created_at"])


async def test_metadata_defaults_to_empty_object(client):
    body = payment_body()
    del body["metadata"]
    created = await client.post("/api/v1/payments", json=body, headers=auth_headers("no-meta"))
    fetched = await client.get(
        f"/api/v1/payments/{created.json()['payment_id']}",
        headers={"X-API-Key": "test-api-key"},
    )
    assert fetched.json()["metadata"] == {}


async def test_get_unknown_payment_returns_404(client):
    response = await client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={"X-API-Key": "test-api-key"},
    )
    assert response.status_code == 404


async def test_get_invalid_id_returns_422(client):
    response = await client.get(
        "/api/v1/payments/not-a-uuid",
        headers={"X-API-Key": "test-api-key"},
    )
    assert response.status_code == 422


async def test_same_idempotency_key_returns_original_payment(client, session_factory):
    from sqlalchemy import func, select

    from app.models import Outbox, Payment

    headers = auth_headers("same-key")
    first = await client.post("/api/v1/payments", json=payment_body(), headers=headers)
    second = await client.post("/api/v1/payments", json=payment_body(), headers=headers)
    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert second.json()["created_at"] == first.json()["created_at"]

    async with session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = await session.scalar(select(func.count()).select_from(Outbox))
    assert payments == 1
    assert events == 1


async def test_same_key_with_reordered_metadata_is_a_replay(client):
    headers = auth_headers("reorder")
    first = await client.post(
        "/api/v1/payments",
        json=payment_body(metadata={"a": 1, "b": 2}),
        headers=headers,
    )
    second = await client.post(
        "/api/v1/payments",
        json=payment_body(metadata={"b": 2, "a": 1}),
        headers=headers,
    )
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]


async def test_same_key_with_different_body_conflicts(client):
    headers = auth_headers("conflict")
    created = await client.post("/api/v1/payments", json=payment_body(), headers=headers)
    conflict = await client.post(
        "/api/v1/payments",
        json=payment_body(amount="11.00"),
        headers=headers,
    )
    assert created.status_code == 202
    assert conflict.status_code == 409
