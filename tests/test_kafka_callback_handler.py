"""What this service does with a callback once the library has accepted it.

`edutap.wallet_google` already tests the route, the signature check, expiry,
nonce handling and key caching. None of that is repeated here. What it cannot
test is this package's implementation of the CallbackHandler protocol: which
topic the event lands in, what the record key is, and what happens when Kafka is
unavailable.

The handler takes its session manager as an argument, so these tests hand it
one. The library instantiates plugins with no arguments, and
`test_plugin_registration.py` covers that path.
"""

from conftest import FakeProducer
from conftest import FakeSessionManager
from edutap.wallet_google_callback_handler.kafka import KafkaCallbackHandler
from edutap.wallet_google_callback_handler.kafka import KafkaSettings
from typing import TypedDict

import json
import pytest


pytestmark = pytest.mark.anyio


class CallbackEvent(TypedDict):
    """The six arguments `CallbackHandler.handle` takes, as the protocol names them."""

    class_id: str
    object_id: str
    event_type: str
    exp_time_millis: int
    count: int
    nonce: str


EVENT: CallbackEvent = {
    "class_id": "3388000000022125777.test-class",
    "object_id": "3388000000022125777.abc-123",
    "event_type": "save",
    "exp_time_millis": 1893456000000,
    "count": 1,
    "nonce": "6f1c1e5a-0000-4000-8000-000000000000",
}


async def test_event_is_published_to_the_configured_topic(session_manager, producer):
    await KafkaCallbackHandler(session_manager=session_manager).handle(**EVENT)

    assert len(producer.sent) == 1
    assert producer.sent[0]["topic"] == "edutap.google_callback"


async def test_the_topic_comes_from_the_managers_settings(producer):
    """Not from a module level `settings` read at import time, as it used to.

    The deployment configures a topic per instance
    (`edutap.<environment>.pass.state`), and the default here is not that name.
    A handler that had frozen the value at import would publish to the default
    for the rest of the process.
    """
    manager = FakeSessionManager(
        producer=producer,
        settings=KafkaSettings(GOOGLE_CALLBACK_TOPIC="edutap.production.pass.state"),
    )

    await KafkaCallbackHandler(session_manager=manager).handle(**EVENT)

    assert producer.sent[0]["topic"] == "edutap.production.pass.state"


async def test_record_key_is_the_object_id(session_manager, producer):
    """The key decides partitioning, and therefore ordering.

    Events for one pass must stay in order relative to each other, so the key
    has to be the object id -- not the class id, which would put every pass of a
    class into one partition.
    """
    await KafkaCallbackHandler(session_manager=session_manager).handle(**EVENT)

    assert producer.sent[0]["key"] == EVENT["object_id"].encode("utf-8")


async def test_payload_uses_googles_camel_case_field_names(session_manager, producer):
    """SignedMessage mirrors Google's wire format -- camelCase, no aliases.

    This is where the handler used to fail: it passed snake_case, which raised a
    ValidationError for every callback. Consumers read these records, so the key
    names are part of the contract.
    """
    await KafkaCallbackHandler(session_manager=session_manager).handle(**EVENT)

    payload = json.loads(producer.sent[0]["value"].decode("utf-8"))
    assert payload == {
        "classId": EVENT["class_id"],
        "objectId": EVENT["object_id"],
        "eventType": EVENT["event_type"],
        "expTimeMillis": EVENT["exp_time_millis"],
        "count": EVENT["count"],
        "nonce": EVENT["nonce"],
    }


async def test_send_is_flushed(session_manager, producer):
    """Without the flush the event may sit in the client buffer.

    The process is short-lived per request; an unflushed batch can be lost on
    shutdown.
    """
    await KafkaCallbackHandler(session_manager=session_manager).handle(**EVENT)

    assert producer.flushed == 1


async def test_a_broker_failure_does_not_reach_the_caller():
    """Documents current behaviour, which is a deliberate trade-off -- and a risk.

    `handle` swallows every exception and only logs. Google therefore always
    receives a 200 and will not retry, even though the event never reached
    Kafka: the callback is lost silently.

    That is the right call for the response (a retry storm helps nobody when the
    broker is down), but the loss should be visible. This test pins the
    behaviour so that a change to it is a deliberate one.
    """
    producer = FakeProducer(fail_on_send=RuntimeError("broker down"))
    manager = FakeSessionManager(producer=producer)

    await KafkaCallbackHandler(session_manager=manager).handle(
        **EVENT
    )  # must not raise

    assert producer.sent == []


async def test_without_an_argument_the_handler_uses_the_process_manager(monkeypatch):
    """The path the library takes: `plugin()`, no arguments.

    A default that resolved at import rather than at call time would make the
    entry point unusable in any process that configures Kafka after import --
    and untestable without reloading the module.
    """
    from edutap.wallet_google_callback_handler import kafka as kafka_module

    manager = FakeSessionManager()
    monkeypatch.setattr(kafka_module, "kafka_session_manager", manager)

    await KafkaCallbackHandler().handle(**EVENT)

    assert len(manager.producer.sent) == 1
