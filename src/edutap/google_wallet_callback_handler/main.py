from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from edutap.wallet_google.handlers.fastapi import router_callback
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Response
from importlib.metadata import version
from edutap.google_wallet_callback_handler.log import logger
from edutap.google_wallet_callback_handler.settings import BaseSettings
from starlette.status import HTTP_200_OK
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

import asyncio
import os
import uvicorn
import uvloop


__version__ = version("edutap.google_wallet_callback_handler")
base_settings = BaseSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initializing
    logger.info("Initializing Start of eduTAP Google Callback Service")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    # Add fastAPI Router for Google Callback Service
    app.include_router(router_callback, prefix="/v1")
    # Enable or Disable docs and openapi.json paths
    if base_settings.ENVIRONMENT == "development":
        app.add_route("/openapi.json", route=app.openapi(), methods={"GET"})

    # Start Kafka Health Messages
    # hostname = os.environ.get("HOSTNAME", "local")
    # task = asyncio.create_task(
    #     health_message(
    #         service_name="Google Callback Handler (lmu_edutap_google_callback_handler)",
    #         instance_name=hostname,
    #     )
    # )

    logger.info("eduTAP Google Callback Service ready")
    yield
    # Shutdown
    logger.info("Initializing Shutdown of eduTAP Google Callback Service")

    # task.cancel()

    logger.info("Shutdown of eduTAP Google Callback Service completed")


app = FastAPI(
    title="eduTAP Google Wallet Callback Service",
    description="A fastAPI based eduTAP Google Wallet Callback Service, to handle register and unregister events of passes in the device wallets.",
    summary="",
    version=__version__,
    openapi_url="/openapi.json" if base_settings.ENVIRONMENT == "development" else None,
    lifespan=lifespan,
    root_path="/wallet/google",
)


@app.get("/")
async def read_root():
    return {"Module": "eduTAP Google Wallet Callback Service"}


@app.head("/")
async def basic_health_check():
    # producer: AIOKafkaProducer = await kafka_session_manager.kafka_producer()
    # if producer._closed:
    #     raise HTTPException(
    #         status_code=HTTP_503_SERVICE_UNAVAILABLE,
    #         detail="Kafka Connection failed.",
    #     )
    return Response(status_code=HTTP_200_OK)


def main():
    uvicorn.run(
        app=app,
        host="0.0.0.0",
        port=9000,
        log_level="debug",
        reload=False,
    )


if __name__ == "__main__":
    main()
