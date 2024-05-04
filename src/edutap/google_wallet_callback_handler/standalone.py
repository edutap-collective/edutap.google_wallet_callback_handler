import uvicorn


from .model import CallbackMessage
from .config import GoogleWalletCallbackHandlerSettings
from .stream import process_messages
from .callback_handler import setup
from .callback_handler import router
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.logger import logger
from importlib.metadata import version
from typing import Any

import asyncio
import os
import time


logger.setLevel("DEBUG")

__version__ = version("edutap.google_wallet_callback_handler")


settings = GoogleWalletCallbackHandlerSettings()

app = FastAPI(
    # title="Google Wallet Callback Handler",
    # description=""" """,
    # summary=""" """,
    version=__version__,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initializing
    setup(app, logger)

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


def main():
    uvicorn.run(
        "edutap.google_wallet_callback_handler.callback_handler:app",
        host="127.0.0.1",
        port=9000,
        log_level="debug",
        reload=True,
    )


if __name__ == "__main__":
    main()
