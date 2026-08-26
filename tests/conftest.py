"""Fixtures shared by the whole suite."""

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
