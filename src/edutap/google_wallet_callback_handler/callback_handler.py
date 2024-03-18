from .model import CallbackMessage
from .stream import process_messages
from aiokafka import AIOKafkaProducer
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

kafka_producer: AIOKafkaProducer | None = None

app = FastAPI(
    # title="Google Wallet Callback Handler",
    # description=""" """,
    # summary=""" """,
    version=__version__,
)

NOTIFICATION_TOPIC = "google-wallet-notification"
TARGET_TOPIC = "google-wallet-notification-decrypted"

BROKER_URL = os.environ.get("BROKER_URL", "kafka:19094")
logger.info(f"BROKER_URL: {BROKER_URL}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initializing
    print("BROKER_URL: ", BROKER_URL)
    global kafka_producer
    retries = 50
    for i in range(retries):
        try:
            kafka_producer = AIOKafkaProducer(bootstrap_servers=BROKER_URL)
            await kafka_producer.start()
            break
        except Exception as e:
            logger.error(e)
            logger.warn(
                f"Waiting for kafka at {BROKER_URL} to start, retry in 1 second"
            )
            time.sleep(1)

    logger.info("Kafka producer started")

    print("creating stream processor for google wallet notifications")
    asyncio.create_task(process_messages(BROKER_URL, NOTIFICATION_TOPIC, TARGET_TOPIC))

    yield
    # Shutdown


@app.get("/")
async def read_root():
    return {"Hello": "World!"}


@app.get("/info")
async def info():
    return {
        "package": "edutap.google_wallet_callback_handler",
        "version": __version__,
        "broker_url": BROKER_URL,
        "topic": NOTIFICATION_TOPIC,
    }


@app.post("/test/message")
async def test_message(request: Request, msg: str):
    await kafka_producer.send_and_wait("test", msg.encode("utf-8"))


@app.post("/google/callback")
async def handle_callback(request: Request, callback_message: CallbackMessage):
    try:
        print("Received signed message: ", callback_message)
        # callback_message.repair()
        msg_text = callback_message.json().encode("utf-8")
        print(f"sending message to {NOTIFICATION_TOPIC}, text: {msg_text}")
        await app.kafka_producer.send_and_wait(NOTIFICATION_TOPIC, msg_text)
        return {"status": "success"}
    except Exception as e:
        print("Error handling callback: ", e)
        await kafka_producer.send_and_wait(NOTIFICATION_TOPIC, str(e).encode("utf-8"))
        await kafka_producer.send_and_wait(
            NOTIFICATION_TOPIC, callback_message.json().encode("utf-8")
        )

        raise HTTPException(status_code=500, detail="Error handling callback")


@app.post("/google/update_request")
async def update_request(data: Any):
    try:
        print("Received signed message: ", data)
        return {"status": "success"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=500, detail="Error handling callback")
