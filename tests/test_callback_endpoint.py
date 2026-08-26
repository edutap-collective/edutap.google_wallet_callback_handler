"""The whole path, once: HTTP request in, Kafka record out.

Every other test in this suite reaches into the package -- it calls
`KafkaCallbackHandler.handle` directly, or the health check function, or a
settings class. None of them goes through the app, and that is precisely why the
service could answer every callback with HTTP 500 for months with a green suite:
the route is the library's, the handler is ours, and nothing tested the seam
between them.

This test drives the app the way Google does. It is deliberately the only one
that does; it needs the package installed, and it duplicates coverage the
library already has. Its job is the seam, not the parts.

Signature verification is switched off here. Forging a valid ECv2SigningOnly
signature would mean reimplementing Google's signing scheme in the test, and
`edutap.wallet_google` already covers verification against its own fixtures.
What is left to check is that an *accepted* message reaches Kafka intact.
"""

from conftest import FakeSessionManager
from edutap.wallet_google.clientpool import client_pool
from edutap.wallet_google_callback_handler import kafka as kafka_module
from edutap.wallet_google_callback_handler.main import create_app
from edutap.wallet_google_callback_handler.settings import Settings
from fastapi.testclient import TestClient
from typing import TypedDict

import json
import pytest


class SignedMessagePayload(TypedDict):
    """Google's wire format for the signed message, as the library models it."""

    classId: str
    objectId: str
    eventType: str
    expTimeMillis: int
    count: int
    nonce: str


SIGNED_MESSAGE: SignedMessagePayload = {
    "classId": "3388000000022125777.test-class",
    "objectId": "3388000000022125777.abc-123",
    "eventType": "SAVE",
    # 2030-01-01, so the library's expiry check passes without freezing time.
    "expTimeMillis": 1893456000000,
    "count": 1,
    "nonce": "6f1c1e5a-0000-4000-8000-000000000000",
}

CALLBACK_BODY = {
    "signature": "not-checked-here",
    "intermediateSigningKey": {"signedKey": "{}", "signatures": []},
    "protocolVersion": "ECv2SigningOnly",
    "signedMessage": json.dumps(SIGNED_MESSAGE),
}


@pytest.fixture
def unverified(monkeypatch):
    """Accept the message without checking Google's signature.

    Patched on the live settings object rather than through an environment
    variable: `client_pool` is built when the library is imported, so a variable
    set from a test would arrive too late.
    """
    monkeypatch.setattr(client_pool.settings, "handler_callback_verify_signature", "0")


@pytest.fixture
def published(monkeypatch, session_manager: FakeSessionManager) -> FakeSessionManager:
    """Put a recording session manager where the registered plugin will find it.

    The library instantiates the handler itself, with no arguments, so the seam
    is the module attribute rather than a constructor argument here.
    """
    monkeypatch.setattr(kafka_module, "kafka_session_manager", session_manager)
    return session_manager


@pytest.fixture
def client(published, unverified):
    with TestClient(create_app(Settings())) as test_client:
        yield test_client


def test_a_callback_is_accepted(client):
    response = client.post("/v1/callback", json=CALLBACK_BODY)

    assert response.status_code == 200, response.text


def test_an_accepted_callback_reaches_kafka(client, published):
    """The one assertion that was missing.

    A green suite plus a healthy container still meant zero records in the
    topic, because no handler was registered for the route to call.
    """
    client.post("/v1/callback", json=CALLBACK_BODY)

    assert len(published.producer.sent) == 1


def test_the_record_carries_googles_field_names(client, published):
    """Consumers read these records, so the key names are part of the contract."""
    client.post("/v1/callback", json=CALLBACK_BODY)

    payload = json.loads(published.producer.sent[0]["value"].decode("utf-8"))
    assert payload == SIGNED_MESSAGE


def test_the_record_key_is_the_object_id(client, published):
    """The key decides the partition, and therefore the order events are read in."""
    client.post("/v1/callback", json=CALLBACK_BODY)

    assert published.producer.sent[0]["key"] == SIGNED_MESSAGE["objectId"].encode(
        "utf-8"
    )


def test_the_producer_is_closed_on_shutdown(published, unverified):
    """The lifespan owns the producer's life, since `atexit` cannot await."""
    with TestClient(create_app(Settings())) as test_client:
        test_client.post("/v1/callback", json=CALLBACK_BODY)

    assert published.closed == 1
