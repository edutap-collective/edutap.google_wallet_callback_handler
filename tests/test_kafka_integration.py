"""The one test that talks to a real broker.

Everything else in this suite substitutes the producer, which is right: those
tests are about this package's decisions, and a broker would only make them slow
and flaky. But a fake producer accepts anything. It cannot tell us whether the
key and value this service sends are bytes the way `aiokafka` wants them,
whether the topic name is one Kafka accepts, or whether a record written by this
handler is a record a consumer can read back.

Deselected by default -- `pytest` alone skips it. `make test-integration` starts
the broker in `compose.test.yml`, runs it, and tears the broker down again.
"""

from aiokafka import AIOKafkaConsumer
from conftest import CallbackEvent
from conftest import EVENT
from datetime import datetime
from edutap.data_models import messaging
from edutap.wallet_google_callback_handler.kafka import KafkaCallbackHandler
from edutap.wallet_google_callback_handler.kafka import KafkaSessionManager
from edutap.wallet_google_callback_handler.kafka import KafkaSettings
from uuid import uuid4

import anyio
import json
import os
import pytest


pytestmark = [pytest.mark.anyio, pytest.mark.integration]

BOOTSTRAP_SERVERS = os.environ.get("EDUTAP_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
async def topic() -> str:
    """Return a topic name nobody else is using, so runs cannot read each other's records."""
    return f"edutap.test.callback.{uuid4().hex[:12]}"


@pytest.fixture
async def manager(topic: str):
    """Yield a session manager pointed at the broker from `compose.test.yml`."""
    session = KafkaSessionManager(
        settings=KafkaSettings(
            BOOTSTRAP_SERVERS=BOOTSTRAP_SERVERS,
            GOOGLE_CALLBACK_TOPIC=topic,
        )
    )
    # Fail here rather than in the assertion: `handle` swallows every exception,
    # so an unreachable broker would otherwise look like an empty topic and the
    # test would time out instead of saying what is wrong.
    await session.kafka_producer()
    try:
        yield session
    finally:
        await session.close()


async def test_a_callback_becomes_a_record_a_consumer_can_read(manager, topic):
    await KafkaCallbackHandler(session_manager=manager).handle(**EVENT)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        # No consumer group: this reader is not part of anyone's rebalancing and
        # should not commit offsets a later run would inherit.
        group_id=None,
    )
    await consumer.start()
    try:
        with anyio.fail_after(30):
            record = await consumer.getone()
    finally:
        await consumer.stop()

    assert record.key == EVENT["object_id"].encode("utf-8")
    assert record.value is not None
    assert json.loads(record.value.decode("utf-8")) == {
        "classId": EVENT["class_id"],
        "objectId": EVENT["object_id"],
        "eventType": EVENT["event_type"],
        "expTimeMillis": EVENT["exp_time_millis"],
        "count": EVENT["count"],
        "nonce": EVENT["nonce"],
    }


async def test_the_producer_survives_more_than_one_event(manager, topic):
    """The manager hands out one producer; it has to stay usable."""
    handler = KafkaCallbackHandler(session_manager=manager)

    second = CallbackEvent(**EVENT)
    second["object_id"] = f"{EVENT['object_id']}-2"

    await handler.handle(**EVENT)
    await handler.handle(**second)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=None,
    )
    await consumer.start()
    try:
        with anyio.fail_after(30):
            keys = [(await consumer.getone()).key for _ in range(2)]
    finally:
        await consumer.stop()

    assert keys == [
        EVENT["object_id"].encode("utf-8"),
        f"{EVENT['object_id']}-2".encode(),
    ]


async def test_the_header_block_survives_the_broker(manager, topic):
    """A fake producer records whatever it is handed; a broker does not.

    `aiokafka` serialises the header block on the way out and the consumer gets it
    back as `(name, bytes)` pairs. Nothing in the unit tests would notice if that
    round trip lost or mangled it, and the consumer reads this block before it
    looks at the body -- so a record whose headers did not survive is a record that
    goes to the dead letter topic.
    """
    await KafkaCallbackHandler(session_manager=manager).handle(**EVENT)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id=None,
    )
    await consumer.start()
    try:
        with anyio.fail_after(30):
            record = await consumer.getone()
    finally:
        await consumer.stop()

    headers = record.headers
    assert (
        messaging.read_header(headers, messaging.HEADER_SCHEMA)
        == messaging.SCHEMA_PASS_STATE
    )
    assert messaging.read_header(headers, messaging.HEADER_EVENT_ID) == EVENT["nonce"]
    assert messaging.read_header(headers, messaging.HEADER_ACTION) is None
    raw_time = messaging.read_header(headers, messaging.HEADER_OCCURRED_AT)
    assert raw_time is not None
    occurred_at = datetime.fromisoformat(raw_time)
    assert occurred_at.tzinfo is not None
