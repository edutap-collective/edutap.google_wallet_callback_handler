"""The Kafka side of the service: one producer, one topic, one handler.

This is where an accepted callback becomes a record. `KafkaCallbackHandler`
is what `pyproject.toml` registers as an `edutap.wallet_google.plugins` entry
point, and it is the only thing in this package the library ever calls.
"""

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from aiokafka.helpers import create_ssl_context
from edutap.wallet_google.models.handlers import SignedMessage
from edutap.wallet_google_callback_handler.log import logger
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

import atexit
import pathlib
import threading


_THREADLOCAL = threading.local()


class KafkaSettings(BaseSettings):
    """Kafka connection and topic configuration, read from `EDUTAP_KAFKA_*`.

    The prefix is deliberately not this package's own: the broker list and the
    client certificates are the same for every eduTAP service on a cluster, and
    duplicating them per package would mean configuring one fact in six places.
    """

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
    """Hands out the process-wide Kafka producer, opening it on first use."""

    async def _exit_event(self):
        if getattr(_THREADLOCAL, "kafka_producer", None) is not None:
            await _THREADLOCAL.kafka_producer.stop()

    async def _establish_kafka_producer(self):
        settings = KafkaSettings()
        try:
            logger.info(
                f"Create Kafka Producer and try to connect to: {settings.BOOTSTRAP_SERVERS}"
            )
            if (
                settings.CA_FILE is not None
                and settings.CERT_FILE is not None
                and settings.KEY_FILE is not None
            ):
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
                producer = AIOKafkaProducer(
                    bootstrap_servers=settings.BOOTSTRAP_SERVERS
                )
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
        """Return the producer, establishing the connection if there is none yet."""
        if getattr(_THREADLOCAL, "kafka_producer", None) is None:
            _THREADLOCAL.kafka_producer = await self._establish_kafka_producer()
        # logger.debug(_THREADLOCAL.kafka_producer.client.ready())
        return _THREADLOCAL.kafka_producer


kafka_session_manager = KafkaSessionManager()


class KafkaCallbackHandler:
    """Implementation of edutap.wallet_google.protocols.CallbackHandler."""

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
                # SignedMessage mirrors Google's wire format and therefore uses
                # camelCase field names, with no aliases and no populate_by_name.
                # Passing snake_case raised a ValidationError for every single
                # callback -- swallowed by the except below, so no event ever
                # reached Kafka and Google still got a 200.
                value=SignedMessage(
                    classId=class_id,
                    objectId=object_id,
                    eventType=event_type,
                    expTimeMillis=exp_time_millis,
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
