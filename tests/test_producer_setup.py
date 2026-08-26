"""How the Kafka producer is built -- plaintext or mTLS, once, and cleaned up.

This branch lives only in this package. It decides whether the service talks to
the broker authenticated or not, and it is driven purely by whether three
settings happen to be set.

Each test builds its own `KafkaSessionManager` with its own settings object.
That is what the refactor bought: the previous version of this file had to
replace the module's `threading.local` in an autouse fixture, because a producer
opened by an earlier test would otherwise be handed to a later one.
"""

from aiokafka.errors import KafkaConnectionError
from edutap.wallet_google_callback_handler import kafka as kafka_module
from edutap.wallet_google_callback_handler.kafka import KafkaSessionManager
from edutap.wallet_google_callback_handler.kafka import KafkaSettings

import anyio
import pytest


pytestmark = pytest.mark.anyio


@pytest.fixture
def captured(monkeypatch):
    """Capture the AIOKafkaProducer keyword arguments instead of connecting."""
    calls = []

    class RecordingProducer:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        async def start(self):
            pass

        async def stop(self):
            pass

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", RecordingProducer)
    return calls


def _with_certificates(tmp_path, *names) -> KafkaSettings:
    files = {}
    for name in names:
        path = tmp_path / name.lower()
        path.write_text("")
        files[name] = path
    return KafkaSettings(**files)


async def test_without_certificates_the_producer_is_plaintext(captured):
    manager = KafkaSessionManager(settings=KafkaSettings())

    await manager.kafka_producer()

    assert "security_protocol" not in captured[0]
    assert "ssl_context" not in captured[0]


async def test_with_all_three_certificates_the_producer_uses_ssl(
    captured, monkeypatch, tmp_path
):
    monkeypatch.setattr(
        kafka_module, "create_ssl_context", lambda **kwargs: "ssl-context"
    )
    settings = _with_certificates(tmp_path, "CA_FILE", "CERT_FILE", "KEY_FILE")

    await KafkaSessionManager(settings=settings).kafka_producer()

    assert captured[0]["security_protocol"] == "SSL"
    assert captured[0]["ssl_context"] == "ssl-context"


async def test_a_partial_certificate_set_falls_back_to_plaintext(captured, tmp_path):
    """Documents a sharp edge: two of three settings silently mean no TLS.

    A deployment that sets CA and CERT but forgets KEY connects unauthenticated
    rather than failing. Whether that should raise instead is a design question;
    this test makes sure the answer is not changed by accident.
    """
    settings = _with_certificates(tmp_path, "CA_FILE", "CERT_FILE")

    await KafkaSessionManager(settings=settings).kafka_producer()

    assert "security_protocol" not in captured[0]


async def test_the_producer_is_built_once_and_reused(captured):
    manager = KafkaSessionManager(settings=KafkaSettings())

    first = await manager.kafka_producer()
    second = await manager.kafka_producer()

    assert first is second
    assert len(captured) == 1


async def test_two_callbacks_on_a_cold_start_share_one_producer(monkeypatch):
    """The reason `kafka_producer` holds a lock.

    Two callbacks arriving together would otherwise each find no producer and
    each open one. The second overwrites the first, which is then never closed
    and never used -- a connection leaked per concurrent cold start.
    """
    opened = []

    class SlowProducer:
        def __init__(self, **kwargs):
            opened.append(self)

        async def start(self):
            # Long enough for the second caller to reach the same branch.
            await anyio.sleep(0.01)

        async def stop(self):
            pass

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", SlowProducer)
    manager = KafkaSessionManager(settings=KafkaSettings())

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(manager.kafka_producer)
        task_group.start_soon(manager.kafka_producer)

    assert len(opened) == 1


async def test_a_producer_that_cannot_connect_is_stopped(monkeypatch):
    """Otherwise it is dropped half-open, and asyncio says so at exit.

    "ERROR:asyncio:Unclosed AIOKafkaProducer" was printed on every unreachable
    broker, because the object was built, failed to start, and never stopped.
    """
    stopped = []

    class UnreachableProducer:
        def __init__(self, **kwargs):
            pass

        async def start(self):
            raise KafkaConnectionError("no broker")

        async def stop(self):
            stopped.append(True)

    monkeypatch.setattr(kafka_module, "AIOKafkaProducer", UnreachableProducer)
    manager = KafkaSessionManager(settings=KafkaSettings())

    with pytest.raises(KafkaConnectionError):
        await manager.kafka_producer()

    assert stopped == [True]


async def test_close_stops_the_producer_and_forgets_it(captured):
    manager = KafkaSessionManager(settings=KafkaSettings())
    await manager.kafka_producer()

    await manager.close()
    await manager.kafka_producer()

    assert len(captured) == 2, "a closed manager has to open a new producer"


async def test_close_without_a_producer_is_harmless(captured):
    """Shutdown runs whether or not a callback ever arrived."""
    await KafkaSessionManager(settings=KafkaSettings()).close()

    assert captured == []
