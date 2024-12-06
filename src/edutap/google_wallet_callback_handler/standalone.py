from .config import GoogleWalletCallbackHandlerSettings
from .fastapi_kafka_callback_handler import kafka_producer, get_kafka_producer
from .fastapi_kafka_callback_handler import setup
from .kafka_stream import process_messages
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi import Request
from fastapi.logger import logger
from importlib.metadata import version

import asyncio
import uvicorn


logger.setLevel("DEBUG")

__version__ = version("edutap.google_wallet_callback_handler")


settings = GoogleWalletCallbackHandlerSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initializing
    await setup(app)

    logger.info("creating stream processor for google wallet notifications")
    asyncio.create_task(
        process_messages(
            settings.broker_url,
            settings.notification_topic,
            settings.target_topic,
        )
    )
    yield
    # Shutdown


app = FastAPI(
    title="eduTAP Google Wallet Callback Handler",
    description="A fastAPI based Callback-Handler for Google Wallet",
    # summary=""" """,
    version=__version__,
    lifespan=lifespan,
)


@app.get("/")
async def info():
    return {
        "package": "edutap.google_wallet_callback_handler",
        "version": __version__,
        "broker_url": settings.broker_url,
        "topic": settings.notification_topic,
    }


@app.get("/openapi.json")
async def openapi():
    return app.openapi()


@app.post("/test/message")
async def test_message(request: Request, msg: str):
    await get_kafka_producer().send_and_wait("test", msg.encode("utf-8"))


def main():
    uvicorn.run(
        "edutap.google_wallet_callback_handler.standalone:app",
        host="127.0.0.1",
        port=9000,
        log_level="debug",
        reload=True,
    )


if __name__ == "__main__":
    main()
