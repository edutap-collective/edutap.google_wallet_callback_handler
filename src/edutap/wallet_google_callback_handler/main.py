"""The ASGI application: the callback route, the health check, and the wiring.

The callback route itself belongs to `edutap.wallet_google`; what this module
adds is the app it is mounted in, the Sentry and logging setup, and the health
check the orchestrator reads.
"""

from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from edutap.wallet_google.handlers.fastapi import router_callback
from edutap.wallet_google_callback_handler.env_guard import check_retired_env_vars
from edutap.wallet_google_callback_handler.kafka import kafka_session_manager
from edutap.wallet_google_callback_handler.log import logger
from edutap.wallet_google_callback_handler.settings import Settings
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Response
from importlib.metadata import version
from starlette.status import HTTP_200_OK
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

import os
import sentry_sdk
import uvicorn


__version__ = version("edutap.wallet_google_callback_handler")

# Before the settings are read: every field has a default, so a deployment still
# exporting the retired EDUTAP_* names would start with development defaults.
check_retired_env_vars(os.environ)

settings = Settings()
SERVICE_NAME = (
    "eduTAP Google Wallet Callback Service (edutap.wallet_google_callback_handler)"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start and stop the parts of the service that outlive a single request."""
    # Initializing
    logger.info("%s: Initializing Service Start", SERVICE_NAME)

    if settings.SENTRY_DSN:
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            # Add data like request headers and IP for users,
            # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
            send_default_pii=True,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for tracing.
            traces_sample_rate=1.0,
            debug=settings.ENVIRONMENT == "development",
            environment=settings.ENVIRONMENT,
        )

    logger.info("%s: Service Ready", SERVICE_NAME)
    yield

    logger.info("%s: Initializing Service Shutdown", SERVICE_NAME)
    # The Kafka producer is the one resource that outlives a request and has to
    # be closed by someone. It used to be handed to `atexit`, which cannot await
    # anything -- see the commit message.
    await kafka_session_manager.close()
    logger.info("%s: Service Shutdown completed", SERVICE_NAME)


app = FastAPI(
    title="eduTAP Google Wallet Callback Service",
    description="A fastAPI based eduTAP Google Wallet Callback Service, to handle register and unregister events of passes in the device wallets.",
    summary="",
    version=__version__,
    openapi_url="/openapi.json" if settings.ENVIRONMENT == "development" else None,
    lifespan=lifespan,
    root_path="/wallet/google",
)


# The route this service exists for. It belongs to `edutap.wallet_google`; all
# this package does is mount it and register the handler it calls.
app.include_router(router_callback, prefix="/v1")


@app.get("/")
async def read_root():
    """Name the service, for anyone who reaches it in a browser."""
    return {"Module": "eduTAP Google Wallet Callback Service"}


@app.head("/")
async def basic_health_check():
    """Report whether the service can still publish what it accepts.

    A callback it cannot write to Kafka is a callback that is lost, so a closed
    producer has to take the instance out of rotation rather than keep it
    answering 200.
    """
    # Check if Kafka is available
    producer: AIOKafkaProducer = await kafka_session_manager.kafka_producer()
    if producer._closed:
        raise HTTPException(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka Connection failed.",
        )
    return Response(status_code=HTTP_200_OK)


def main():
    """Run the service under uvicorn -- the console script entry point."""
    uvicorn.run(
        app=app,
        # The service only ever runs in a container, where binding to the
        # container-local loopback would make it unreachable from the outside.
        host="0.0.0.0",  # noqa: S104
        port=8085,
        log_level="debug",
        reload=False,
    )


if __name__ == "__main__":
    main()
