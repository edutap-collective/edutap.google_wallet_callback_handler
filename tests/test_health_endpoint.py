"""The health check is this package's own, and it is what the orchestrator reads.

The library provides the callback route; the liveness signal is ours. It reports
unhealthy exactly when the Kafka producer is closed -- which is the condition
that makes the service useless, because a callback it cannot publish is lost.
"""

from conftest import FakeProducer
from conftest import FakeSessionManager
from edutap.wallet_google_callback_handler.main import basic_health_check
from fastapi import HTTPException

import pytest


pytestmark = pytest.mark.anyio


async def test_healthy_while_the_producer_is_open():
    manager = FakeSessionManager(producer=FakeProducer(closed=False))

    response = await basic_health_check(session_manager=manager)

    assert response.status_code == 200


async def test_unhealthy_once_the_producer_is_closed():
    """503, not 500: the orchestrator must take the instance out of rotation.

    A closed producer means published callbacks would be dropped, so the
    instance has to stop receiving traffic rather than keep answering 200.
    """
    manager = FakeSessionManager(producer=FakeProducer(closed=True))

    with pytest.raises(HTTPException) as excinfo:
        await basic_health_check(session_manager=manager)

    assert excinfo.value.status_code == 503
