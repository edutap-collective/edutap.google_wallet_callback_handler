"""How the Kafka producer is built -- plaintext or mTLS.

This branch lives only in this package. It decides whether the service talks to
the broker authenticated or not, and it is driven purely by whether three
settings happen to be set. Worth pinning down.
"""

from edutap.wallet_google_callback_handler import kafka as kafka_module

import pytest


pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _no_leftover_producer(monkeypatch):
    """Each test starts without a cached producer.

    The manager caches in a thread-local, so without this a producer built by an
    earlier test would be handed out again.
    """
    monkeypatch.setattr(kafka_module, "_THREADLOCAL", type("TL", (), {})())


@pytest.fixture
def captured(monkeypatch):
    """Capture the AIOKafkaProducer kwargs instead of connecting."""
    calls = []

    class FakeProducer:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        async def start(self):
            pass

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", FakeProducer)
    return calls


async def test_without_certificates_the_producer_is_plaintext(captured, monkeypatch):
    monkeypatch.delenv("EDUTAP_KAFKA_CA_FILE", raising=False)
    monkeypatch.delenv("EDUTAP_KAFKA_CERT_FILE", raising=False)
    monkeypatch.delenv("EDUTAP_KAFKA_KEY_FILE", raising=False)

    await kafka_module.KafkaSessionManager()._establish_kafka_producer()

    assert "security_protocol" not in captured[0]
    assert "ssl_context" not in captured[0]


async def test_with_all_three_certificates_the_producer_uses_ssl(
    captured, monkeypatch, tmp_path
):
    for name in ("CA_FILE", "CERT_FILE", "KEY_FILE"):
        f = tmp_path / name.lower()
        f.write_text("")
        monkeypatch.setenv(f"EDUTAP_KAFKA_{name}", str(f))
    monkeypatch.setattr(
        kafka_module, "create_ssl_context", lambda **kwargs: "ssl-context"
    )

    await kafka_module.KafkaSessionManager()._establish_kafka_producer()

    assert captured[0]["security_protocol"] == "SSL"
    assert captured[0]["ssl_context"] == "ssl-context"


async def test_a_partial_certificate_set_falls_back_to_plaintext(
    captured, monkeypatch, tmp_path
):
    """Documents a sharp edge: two of three settings silently mean no TLS.

    A deployment that sets CA and CERT but forgets KEY connects unauthenticated
    rather than failing. Whether that should raise instead is a design question;
    this test makes sure the answer is not changed by accident.
    """
    for name in ("CA_FILE", "CERT_FILE"):
        f = tmp_path / name.lower()
        f.write_text("")
        monkeypatch.setenv(f"EDUTAP_KAFKA_{name}", str(f))
    monkeypatch.delenv("EDUTAP_KAFKA_KEY_FILE", raising=False)

    await kafka_module.KafkaSessionManager()._establish_kafka_producer()

    assert "security_protocol" not in captured[0]


async def test_the_producer_is_built_once_and_reused(captured, monkeypatch):
    """The manager caches per thread; a second call must not open a connection."""
    monkeypatch.delenv("EDUTAP_KAFKA_CA_FILE", raising=False)
    manager = kafka_module.KafkaSessionManager()

    first = await manager.kafka_producer()
    second = await manager.kafka_producer()

    assert first is second
    assert len(captured) == 1
