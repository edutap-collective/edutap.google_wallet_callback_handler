"""Fixtures and stand-ins shared by the whole suite.

The stand-ins are here rather than in each module because the refactor that
introduced them made the same two objects useful everywhere: a producer that
records instead of connecting, and a session manager that hands it out. Before
that, every module reached into `kafka.py`'s module state with its own
`monkeypatch.setattr` and its own idea of what the state looked like.
"""

from edutap.wallet_google_callback_handler.kafka import KafkaSettings
from typing import TypedDict

import pytest


@pytest.fixture(scope="session")
def anyio_backend():
    """Run the async tests on asyncio, and only on asyncio.

    anyio's plugin would otherwise offer trio as well. The service runs under
    uvicorn and `aiokafka`, both of which are asyncio; a trio run would exercise
    a combination that is never deployed and cannot be, and its failures would
    say nothing about this package.
    """
    return "asyncio"


class FakeProducer:
    """Records what would have been sent, instead of talking to a broker."""

    def __init__(
        self, closed: bool = False, fail_on_send: Exception | None = None
    ) -> None:
        self.sent: list[dict] = []
        self.flushed = 0
        self.stopped = 0
        self._closed = closed
        self._fail_on_send = fail_on_send

    async def send(self, topic, key, value, headers=None):
        if self._fail_on_send is not None:
            raise self._fail_on_send
        self.sent.append(
            {"topic": topic, "key": key, "value": value, "headers": headers}
        )

    async def flush(self):
        self.flushed += 1

    async def stop(self):
        self.stopped += 1
        self._closed = True


class FakeSessionManager:
    """A `KafkaSessionManager` that hands out a `FakeProducer`."""

    def __init__(
        self,
        producer: FakeProducer | None = None,
        settings: KafkaSettings | None = None,
    ) -> None:
        self.producer = producer if producer is not None else FakeProducer()
        self.settings = settings if settings is not None else KafkaSettings()
        self.closed = 0

    async def kafka_producer(self):
        return self.producer

    async def close(self):
        self.closed += 1


@pytest.fixture
def producer() -> FakeProducer:
    """Return a producer that records what the service tries to publish."""
    return FakeProducer()


@pytest.fixture
def session_manager(producer: FakeProducer) -> FakeSessionManager:
    """Return a session manager handing out the recording producer."""
    return FakeSessionManager(producer=producer)


@pytest.fixture(autouse=True)
def _clear_kafka_settings_cache():
    """Never let one test's Kafka environment leak into the next.

    `get_kafka_settings` caches for the lifetime of the process, which is what
    the service wants and what a test suite must not inherit.
    """
    from edutap.wallet_google_callback_handler.kafka import get_kafka_settings

    get_kafka_settings.cache_clear()
    yield
    get_kafka_settings.cache_clear()


class CallbackEvent(TypedDict):
    """The six arguments `CallbackHandler.handle` takes, as the protocol names them."""

    class_id: str
    object_id: str
    event_type: str
    exp_time_millis: int
    count: int
    nonce: str


# `SAVE` rather than `save`: the library passes `eventType.value`, and its enum
# values are upper case. `expTimeMillis` is 2030-01-01, so the library's expiry
# check passes without anyone having to freeze time.
EVENT: CallbackEvent = {
    "class_id": "3388000000022125777.test-class",
    "object_id": "3388000000022125777.abc-123",
    "event_type": "SAVE",
    "exp_time_millis": 1893456000000,
    "count": 1,
    "nonce": "6f1c1e5a-0000-4000-8000-000000000000",
}
