"""What this service does with a callback once the library has accepted it.

`edutap.wallet_google` already tests the route, the signature check, expiry,
nonce handling and key caching. None of that is repeated here. What it cannot
test is this package's implementation of the CallbackHandler protocol: which
topic the event lands in, what the record key is, and what happens when Kafka
is unavailable.
"""

import json

import pytest

from edutap.wallet_google_callback_handler.kafka import KafkaCallbackHandler

EVENT = {
    "class_id": "3388000000022125777.test-class",
    "object_id": "3388000000022125777.abc-123",
    "event_type": "save",
    "exp_time_millis": 1893456000000,
    "count": 1,
    "nonce": "6f1c1e5a-0000-4000-8000-000000000000",
}


class FakeProducer:
    """Records what would have been sent, instead of talking to a broker."""

    def __init__(self, fail_on_send: Exception | None = None):
        self.sent: list[dict] = []
        self.flushed = 0
        self._fail_on_send = fail_on_send

    async def send(self, topic, key, value):
        if self._fail_on_send is not None:
            raise self._fail_on_send
        self.sent.append({"topic": topic, "key": key, "value": value})

    async def flush(self):
        self.flushed += 1


@pytest.fixture
def producer(monkeypatch):
    fake = FakeProducer()

    async def _producer():
        return fake

    monkeypatch.setattr(
        "edutap.wallet_google_callback_handler.kafka.kafka_session_manager.kafka_producer",
        _producer,
    )
    return fake


@pytest.mark.asyncio
async def test_event_is_published_to_the_configured_topic(producer):
    await KafkaCallbackHandler().handle(**EVENT)

    assert len(producer.sent) == 1
    assert producer.sent[0]["topic"] == "edutap.google_callback"


@pytest.mark.asyncio
async def test_record_key_is_the_object_id(producer):
    """The key decides partitioning, and therefore ordering.

    Events for one pass must stay in order relative to each other, so the key
    has to be the object id -- not the class id, which would put every pass of
    a class into one partition.
    """
    await KafkaCallbackHandler().handle(**EVENT)

    assert producer.sent[0]["key"] == EVENT["object_id"].encode("utf-8")


@pytest.mark.asyncio
async def test_payload_uses_googles_camel_case_field_names(producer):
    """SignedMessage mirrors Google's wire format -- camelCase, no aliases.

    This is where the handler used to fail: it passed snake_case, which raised a
    ValidationError for every callback. Consumers read these records, so the key
    names are part of the contract.
    """
    await KafkaCallbackHandler().handle(**EVENT)

    payload = json.loads(producer.sent[0]["value"].decode("utf-8"))
    assert payload == {
        "classId": EVENT["class_id"],
        "objectId": EVENT["object_id"],
        "eventType": EVENT["event_type"],
        "expTimeMillis": EVENT["exp_time_millis"],
        "count": EVENT["count"],
        "nonce": EVENT["nonce"],
    }


@pytest.mark.asyncio
async def test_send_is_flushed(producer):
    """Without the flush the event may sit in the client buffer.

    The process is short-lived per request; an unflushed batch can be lost on
    shutdown.
    """
    await KafkaCallbackHandler().handle(**EVENT)

    assert producer.flushed == 1


@pytest.mark.asyncio
async def test_a_broker_failure_does_not_reach_the_caller(monkeypatch):
    """Documents current behaviour, which is a deliberate trade-off -- and a risk.

    `handle` swallows every exception and only logs. Google therefore always
    receives a 200 and will not retry, even though the event never reached
    Kafka: the callback is lost silently.

    That is the right call for the response (a retry storm helps nobody), but the
    loss should be visible. This test pins the behaviour so that a change to it
    is a deliberate one.
    """
    fake = FakeProducer(fail_on_send=RuntimeError("broker down"))

    async def _producer():
        return fake

    monkeypatch.setattr(
        "edutap.wallet_google_callback_handler.kafka.kafka_session_manager.kafka_producer",
        _producer,
    )

    await KafkaCallbackHandler().handle(**EVENT)  # must not raise

    assert fake.sent == []
