"""The header block `pass.state` requires, checked against the consumer's gate.

The acceptance criterion. Until this existed the service published a bare body --
no `edutap-*` headers at all -- while `lmu_edutap_worker` requires four of them and
refuses anything else. The broker accepts such records, the producer reports
success, and every one of them is parked in `pass.state.dlq`. The failure is
completely silent, which is why it needs a test rather than an inspection.

The consumer's rules are restated here as data rather than imported. Importing the
worker would make this suite depend on a service it has no business knowing, and
the two are deployed separately -- a test that reached across would go red for
somebody else's refactoring. Both sides read their header names from
`edutap.data_models.messaging`, so a change to the contract itself still breaks one
of the two suites.

Read out of the deployed worker image on 2026-08-26
(`lmu_edutap_worker/headers.py`, `read_envelope`).
"""

from conftest import EVENT
from conftest import FakeSessionManager
from datetime import datetime
from edutap.data_models import messaging
from edutap.wallet_google_callback_handler.kafka import KafkaCallbackHandler
from edutap.wallet_google_callback_handler.kafka import PRODUCER_NAME

import pytest


pytestmark = pytest.mark.anyio

#: Present and non-empty, or the message is refused.
MANDATORY = (
    messaging.HEADER_PRODUCER,
    messaging.HEADER_SCHEMA,
    messaging.HEADER_EVENT_ID,
    messaging.HEADER_OCCURRED_AT,
)


def _require(headers, name: str) -> str:
    """Return a mandatory header, the way the consumer insists on it."""
    value = messaging.read_header(headers, name)
    assert value is not None and value.strip(), f"mandatory header {name} is missing"
    return value


def _read_envelope(headers):
    """Apply what `lmu_edutap_worker.headers.read_envelope()` does on `pass.state`."""
    for name in MANDATORY:
        _require(headers, name)

    schema = _require(headers, messaging.HEADER_SCHEMA)
    assert schema == messaging.SCHEMA_PASS_STATE, f"unknown schema {schema!r}"

    occurred_at = datetime.fromisoformat(
        _require(headers, messaging.HEADER_OCCURRED_AT)
    )
    assert occurred_at.tzinfo is not None, "occurred_at has no time zone"

    # `pass.state` is the deliberate empty case: the callback services normalise at
    # the edge, so the topic carries one payload and has nothing to discriminate.
    action = messaging.read_header(headers, messaging.HEADER_ACTION)
    assert action is None, f"pass.state carries no action, got {action!r}"
    return occurred_at


async def _publish(**overrides):
    manager = FakeSessionManager()
    await KafkaCallbackHandler(session_manager=manager).handle(**{**EVENT, **overrides})
    assert manager.producer.sent, "nothing was published"
    return manager.producer.sent[0]


async def test_the_record_passes_the_consumers_gate():
    """The whole point, in one assertion: the worker would accept this."""
    record = await _publish()

    _read_envelope(record["headers"])


async def test_every_mandatory_header_is_present():
    record = await _publish()

    names = {name for name, _ in record["headers"]}
    assert set(MANDATORY) <= names


async def test_the_schema_is_pass_state_v1():
    """An unknown schema is not ignored, it is refused -- a `pass-state/v2` too."""
    record = await _publish()

    assert (
        messaging.read_header(record["headers"], messaging.HEADER_SCHEMA)
        == "pass-state/v1"
    )


async def test_no_action_header_is_sent():
    """A present action is rejected here, not merely unused.

    Google's `eventType` is exactly the kind of discriminator that would be an
    action on another topic. On `pass.state` it stays in the body.
    """
    record = await _publish()

    assert messaging.read_header(record["headers"], messaging.HEADER_ACTION) is None


async def test_occurred_at_carries_a_time_zone():
    """The consumer's watermark compares this value across services.

    A timestamp without an offset is a timestamp against an unknown clock, and the
    consumer refuses it rather than comparing it.
    """
    record = await _publish()

    occurred_at = datetime.fromisoformat(
        _require(record["headers"], messaging.HEADER_OCCURRED_AT)
    )
    assert occurred_at.tzinfo is not None
    assert occurred_at.utcoffset() is not None


async def test_the_event_id_is_googles_nonce():
    """So that a redelivered callback is one event, not two.

    The event id is the consumer's idempotency key. A fresh uuid per publish would
    make Google's retry of the same callback look like a second event -- and Google
    retries whenever it does not get a 200.
    """
    record = await _publish()

    assert (
        messaging.read_header(record["headers"], messaging.HEADER_EVENT_ID)
        == EVENT["nonce"]
    )


async def test_two_deliveries_of_one_callback_share_an_event_id():
    first = await _publish()
    second = await _publish()

    assert messaging.read_header(
        first["headers"], messaging.HEADER_EVENT_ID
    ) == messaging.read_header(second["headers"], messaging.HEADER_EVENT_ID)


async def test_two_different_callbacks_do_not():
    first = await _publish()
    second = await _publish(nonce="0f1c1e5a-1111-4000-8000-000000000000")

    assert messaging.read_header(
        first["headers"], messaging.HEADER_EVENT_ID
    ) != messaging.read_header(second["headers"], messaging.HEADER_EVENT_ID)


async def test_the_producer_names_this_service():
    """Name this service, so a dead-lettered record can be traced back.

    A claim rather than an authentication -- but several services feed
    `pass.state` and they share one `pass.state.dlq`, so it is what says which of
    them wrote a given record.
    """
    record = await _publish()

    assert messaging.read_header(record["headers"], messaging.HEADER_PRODUCER) == (
        PRODUCER_NAME
    )


async def test_headers_are_bytes_pairs():
    """The shape `aiokafka` expects; a str value raises at send time."""
    record = await _publish()

    for name, value in record["headers"]:
        assert isinstance(name, str)
        assert isinstance(value, bytes)
