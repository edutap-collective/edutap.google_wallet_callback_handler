from .config import GoogleWalletCallbackHandlerSettings
from .model import CallbackMessage
from aiokafka import AIOKafkaProducer
from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.logger import logger
from typing import Any

import time


settings = GoogleWalletCallbackHandlerSettings()
router = APIRouter(prefix=settings.api_prefix)
kafka_producer: AIOKafkaProducer = None

def get_kafka_producer():
    return kafka_producer


def set_kafka_producer(kp):
    global kafka_producer
    kafka_producer = kp


async def setup(app: FastAPI):
    """
    try to start the kafka producer until it reaches kafka
    """
    # global kafka_producer
    retries = 500
    for i in range(retries):
        try:
            kafka_producer = AIOKafkaProducer(bootstrap_servers=settings.broker_url)
            logger.warn(f"trying to start produer...")

            await kafka_producer.start()
            logger.warn(f"kafka producer started: {get_kafka_producer()}")
            set_kafka_producer(kafka_producer)
            break
        except Exception as e:
            logger.error(e)
            logger.warn(
                f"Waiting for kafka at {settings.broker_url} to start, retry in 1 second..."
            )
            time.sleep(1)
    logger.warn(f"Kafka Producer started:{get_kafka_producer()}")

    logger.warn("Register router")
    app.include_router(router)


@router.post("/callback")
async def handle_callback(request: Request, callback_message: CallbackMessage):
    try:
        print("Received signed message: ", callback_message)
        # callback_message.repair()
        msg_text = callback_message.model_dump_json().encode("utf-8")
        logger.debug(
            "sending message to %s, text: %s", settings.notification_topic, msg_text
        )
        await get_kafka_producer().send_and_wait(settings.notification_topic, msg_text)
        return {"status": "success"}
    except Exception as e:
        print("Error handling callback: ", e)
        await get_kafka_producer().send_and_wait(
            settings.notification_topic, str(e).encode("utf-8")
        )
        await get_kafka_producer().send_and_wait(
            settings.notification_topic,
            callback_message.model_dump_json().encode("utf-8"),
        )

        raise HTTPException(
            status_code=500,
            detail="Error handling callback",
        )


@router.post("/update_request")
async def update_request(data: Any):
    try:
        logger.debug("Received signed message: ", data)
        return {"status": "success"}
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=500,
            detail="Error handling callback",
        )
