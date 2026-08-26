"""The health check is this package's own, and it is what the orchestrator reads.

The library provides the callback route; the liveness signal is ours. It reports
unhealthy exactly when the Kafka producer is closed -- which is the condition
that makes the service useless, because a callback it cannot publish is lost.
"""

from edutap.wallet_google_callback_handler import main as main_module
from fastapi import HTTPException

import pytest


pytestmark = pytest.mark.anyio


class FakeProducer:
    def __init__(self, closed: bool):
        self._closed = closed


async def test_healthy_while_the_producer_is_open(monkeypatch):
    async def _producer():
        return FakeProducer(closed=False)

    monkeypatch.setattr(main_module.kafka_session_manager, "kafka_producer", _producer)

    response = await main_module.basic_health_check()

    assert response.status_code == 200


async def test_unhealthy_once_the_producer_is_closed(monkeypatch):
    """503, not 500: the orchestrator must take the instance out of rotation.

    A closed producer means published callbacks would be dropped, so the
    instance has to stop receiving traffic rather than keep answering 200.
    """

    async def _producer():
        return FakeProducer(closed=True)

    monkeypatch.setattr(main_module.kafka_session_manager, "kafka_producer", _producer)

    with pytest.raises(HTTPException) as excinfo:
        await main_module.basic_health_check()

    assert excinfo.value.status_code == 503
