from edutap.google_wallet_callback_handler.log import logger
from edutap.wallet_google.models.handlers import SignedMessage
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from aiokafka.helpers import create_ssl_context
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from typing import Literal

import pathlib
import threading
import atexit

_THREADLOCAL = threading.local()

class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EDUTAP_KAFKA_",
        case_sensitive=False,
        extra="ignore",
        # extra="allow",
        # extra="forbid",
    )

    # Kafka Setting:
    BOOTSTRAP_SERVERS: list[str] | str = ["kafka:9094"]

    # Kafka SSL-Settings for mTLS Auth
    CA_FILE: pathlib.Path | None = None
    CERT_FILE: pathlib.Path | None = None
    KEY_FILE: pathlib.Path | None = None
    PASSWORD: str = ""

    # Kafka Topics
    GOOGLE_CALLBACK_TOPIC: str = "edutap.google_callback"

settings = KafkaSettings()

class KafkaSessionManager:
    """ """

    async def _exit_event(self):
        if getattr(_THREADLOCAL, "kafka_producer", None) is not None:
            await _THREADLOCAL.kafka_producer.stop()

    async def _establish_kafka_producer(self):
        settings = KafkaSettings()
        try:
            logger.info(f"Create Kafka Producer and try to connect to: {settings.BOOTSTRAP_SERVERS}")
            producer: AIOKafkaProducer = None
            if settings.CA_FILE is not None and settings.CERT_FILE is not None and settings.KEY_FILE is not None:
                ssl_context = create_ssl_context(
                    cafile=settings.CA_FILE,
                    certfile=settings.CERT_FILE,
                    keyfile=settings.KEY_FILE,
                    password=settings.PASSWORD,
                )
                producer = AIOKafkaProducer(
                    bootstrap_servers=settings.BOOTSTRAP_SERVERS,
                    security_protocol="SSL",
                    ssl_context=ssl_context,
                )
            else:
                producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVERS)
            await producer.start()
            logger.info(
                "Kafka Producer created and connected to: %s",
                settings.BOOTSTRAP_SERVERS,
            )
        except KafkaConnectionError as e:
            logger.error(e)
            raise e
        atexit.register(self._exit_event)
        return producer

    async def kafka_producer(self) -> AIOKafkaProducer:
        if getattr(_THREADLOCAL, "kafka_producer", None) is None:
            _THREADLOCAL.kafka_producer = await self._establish_kafka_producer()
        # logger.debug(_THREADLOCAL.kafka_producer.client.ready())
        return _THREADLOCAL.kafka_producer


kafka_session_manager = KafkaSessionManager()


class KafkaCallbackHandler:
    """
    Implementation of edutap.wallet_google.protocols.CallbackHandler
    """

    async def handle(
        self,
        class_id: str,
        object_id: str,
        event_type: str,
        exp_time_millis: int,
        count: int,
        nonce: str,
    ) -> None:
        """Handle a Callback and write the Infos into a Kafka Topic."""
        try:
            # Produce message
            producer = await kafka_session_manager.kafka_producer()
            await producer.send(
                topic=settings.GOOGLE_CALLBACK_TOPIC,
                key=object_id.encode("utf-8"),
                value=SignedMessage(
                    class_id=class_id,
                    object_id=object_id,
                    event_type=event_type,
                    exp_time_millis=exp_time_millis,
                    count=count,
                    nonce=nonce,
                )
                .model_dump_json(indent=2)
                .encode("utf-8"),
            )
            await producer.flush()
            logger.debug(
                "Message produced for class-id: %s; object-id: %s", class_id, object_id
            )
        except Exception as e:
            logger.error(e)
