from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from edutap.wallet_google_callback_handler.env_guard import check_retired_env_vars
from edutap.wallet_google_callback_handler.kafka import kafka_session_manager
from edutap.wallet_google_callback_handler.log import logger
from edutap.wallet_google_callback_handler.settings import Settings
from edutap.wallet_google.handlers.fastapi import router_callback
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Response
from importlib.metadata import version
from starlette.status import HTTP_200_OK
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

import asyncio
import os
import sentry_sdk
import uvicorn
import uvloop


__version__ = version("edutap.wallet_google_callback_handler")

# Before the settings are read: every field has a default, so a deployment still
# exporting the retired EDUTAP_* names would start with development defaults.
check_retired_env_vars(os.environ)

settings = Settings()
SERVICE_NAME = "eduTAP Google Wallet Callback Service (edutap.wallet_google_callback_handler)"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initializing
    logger.info("%s: Initializing Service Start", SERVICE_NAME)

    # These five options each contradict the SDK's own default, and they are the
    # same set that `sentry_options()` in `edutap.observability_settings`
    # installs for the sibling services, where the reasoning behind each one is
    # written out against a measurement. This package initializes the SDK itself
    # rather than going through that one, so the set is repeated here. Keep the
    # two in step.
    #
    # Until 2026-08-24 this call did the opposite: `send_default_pii=True` and
    # local variables left on the SDK default, which is also True. It was
    # harmless only while `SENTRY_DSN` was empty. This service receives Google's
    # callbacks, so its request bodies carry the identifying datum, and with
    # locals on a bearer token sits in the ASGI scope and reappears in dozens of
    # frames of an event whose rendered `authorization` header reads
    # `[Filtered]` -- Sentry's scrubber matches key names, it does not walk a
    # list of byte tuples.
    #
    # `traces_sample_rate=0` because the receiving end is an error tracker and
    # not an APM: at 1.0 every request became a transaction event.
    if settings.SENTRY_DSN:
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            send_default_pii=False,
            include_local_variables=False,
            max_request_body_size="never",
            max_breadcrumbs=0,
            traces_sample_rate=0,
            debug=settings.ENVIRONMENT == "development",
            environment=settings.ENVIRONMENT,
        )

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    # Add fastAPI Router for Google Callback Service
    app.include_router(router_callback, prefix="/v1")
    # Enable or Disable docs and openapi.json paths
    if settings.ENVIRONMENT == "development":
        app.add_route("/openapi.json", route=app.openapi(), methods={"GET"})

    logger.info("%s: Service Ready", SERVICE_NAME)
    yield
    # Shutdown
    logger.info("%s: Initializing Service Shutdown", SERVICE_NAME)

    # task.cancel()

    logger.info("%s: Service Shutdown completed")


app = FastAPI(
    title="eduTAP Google Wallet Callback Service",
    description="A fastAPI based eduTAP Google Wallet Callback Service, to handle register and unregister events of passes in the device wallets.",
    summary="",
    version=__version__,
    openapi_url="/openapi.json" if settings.ENVIRONMENT == "development" else None,
    lifespan=lifespan,
    root_path="/wallet/google",
)


@app.get("/")
async def read_root():
    return {"Module": "eduTAP Google Wallet Callback Service"}


@app.head("/")
async def basic_health_check():
    # Check if Kafka is available
    producer: AIOKafkaProducer = await kafka_session_manager.kafka_producer()
    if producer._closed:
        raise HTTPException(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka Connection failed.",
        )
    return Response(status_code=HTTP_200_OK)


def main():
    uvicorn.run(
        app=app,
        host="0.0.0.0",
        port=8085,
        log_level="debug",
        reload=False,
    )


if __name__ == "__main__":
    main()
