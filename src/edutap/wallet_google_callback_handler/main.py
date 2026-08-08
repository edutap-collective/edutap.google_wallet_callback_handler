from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
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
import sentry_sdk
import uvicorn
import uvloop


__version__ = version("edutap.wallet_google_callback_handler")

settings = Settings()
SERVICE_NAME = "eduTAP Google Wallet Callback Service (edutap.wallet_google_callback_handler)"


@asynccontextmanager
async def lifespan(app: FastAPI):
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
