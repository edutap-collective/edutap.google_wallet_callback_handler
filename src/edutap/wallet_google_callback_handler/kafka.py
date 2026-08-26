"""The Kafka side of the service: one producer, one topic, one handler.

This is where an accepted callback becomes a record. `KafkaCallbackHandler` is
what `pyproject.toml` registers as an `edutap.wallet_google.plugins` entry point,
and it is the only thing in this package the library ever calls.

Nothing here reads the environment at import time, and nothing here is a module
level singleton except the one the entry point needs. Both used to be true, and
both made the module hard to test: the topic came from a `KafkaSettings()`
evaluated whenever the module happened to be imported first, and the producer
was cached in a `threading.local` that a test could only reach by replacing the
module attribute.
"""

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from aiokafka.helpers import create_ssl_context
from edutap.wallet_google.models.handlers import SignedMessage
from edutap.wallet_google_callback_handler.log import logger
from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from typing import Any
from typing import Protocol
from typing import runtime_checkable

import asyncio
import pathlib


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
    )

    # Kafka connection
    BOOTSTRAP_SERVERS: list[str] | str = ["kafka:9094"]

    # Kafka SSL settings for mTLS auth
    CA_FILE: pathlib.Path | None = None
    CERT_FILE: pathlib.Path | None = None
    KEY_FILE: pathlib.Path | None = None
    PASSWORD: str = ""

    # Kafka topics
    GOOGLE_CALLBACK_TOPIC: str = "edutap.google_callback"


@lru_cache(maxsize=1)
def get_kafka_settings() -> KafkaSettings:
    """Read the Kafka configuration, once per process.

    Cached rather than evaluated at import. A module level `KafkaSettings()`
    freezes the environment at the moment something first imports this module,
    which in a test run is whenever the first test happens to touch it -- and
    then no `monkeypatch.setenv` can reach it any more. `cache_clear()` is the
    way back, and the suite uses it.
    """
    return KafkaSettings()


@runtime_checkable
class KafkaSession(Protocol):
    """What the callback handler and the health check need from a Kafka session.

    Narrower than `KafkaSessionManager` on purpose. The handler does not care
    how the producer came about, only that there is one and which topic to write
    to, and stating that lets a test hand it something that records instead of
    connecting -- without inheriting from the real manager and inheriting its
    behaviour along with its shape.
    """

    @property
    def settings(self) -> KafkaSettings:
        """The Kafka configuration this session works from."""
        ...

    async def kafka_producer(self) -> Any:
        """Return a producer, opening the connection if there is none yet."""
        ...

    async def close(self) -> None:
        """Release whatever the session holds."""
        ...


class KafkaSessionManager:
    """Owns the process's Kafka producer: opens it on first use, closes it once.

    One instance is enough for a service, and `kafka_session_manager` below is
    that instance. It is a class rather than a pair of module level functions so
    that a test can have its own, with its own settings and its own producer,
    without reaching into module state.
    """

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None
        # Two callbacks arriving together on a cold start would otherwise each
        # find no producer and each open one; the second would overwrite the
        # first, which is then never closed and never used.
        self._lock = asyncio.Lock()

    @property
    def settings(self) -> KafkaSettings:
        """The settings this manager works from -- injected, or the process's."""
        if self._settings is not None:
            return self._settings
        return get_kafka_settings()

    async def kafka_producer(self) -> AIOKafkaProducer:
        """Return the producer, establishing the connection if there is none yet."""
        if self._producer is not None:
            return self._producer
        async with self._lock:
            if self._producer is None:
                self._producer = await self._establish_kafka_producer()
        return self._producer

    async def close(self) -> None:
        """Stop the producer, if one was ever opened."""
        producer, self._producer = self._producer, None
        if producer is None:
            return
        await producer.stop()

    async def _establish_kafka_producer(self) -> AIOKafkaProducer:
        settings = self.settings
        logger.info(
            "Create Kafka Producer and try to connect to: %s",
            settings.BOOTSTRAP_SERVERS,
        )
        # mTLS as soon as all three files are configured, plaintext otherwise.
        # Two of the three is not an error and not a partial upgrade -- it is
        # plaintext, silently. See test_producer_setup.py.
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
            producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVERS)

        try:
            await producer.start()
        except KafkaConnectionError:
            # A producer whose `start()` failed still holds a client, and
            # dropping it here is what printed "ERROR:asyncio:Unclosed
            # AIOKafkaProducer" every time the broker was unreachable.
            logger.exception(
                "Kafka Producer could not connect to: %s", settings.BOOTSTRAP_SERVERS
            )
            await producer.stop()
            raise

        logger.info(
            "Kafka Producer created and connected to: %s", settings.BOOTSTRAP_SERVERS
        )
        return producer


kafka_session_manager = KafkaSessionManager()


class KafkaCallbackHandler:
    """Implementation of edutap.wallet_google.protocols.CallbackHandler."""

    def __init__(self, session_manager: KafkaSession | None = None) -> None:
        # The library instantiates plugins with no arguments, so the default has
        # to be the process's manager. The argument exists for the tests, and
        # for anyone embedding this handler in another application.
        self._session_manager = session_manager

    @property
    def session_manager(self) -> KafkaSession:
        """The manager this handler publishes through."""
        if self._session_manager is not None:
            return self._session_manager
        return kafka_session_manager

    async def handle(
        self,
        class_id: str,
        object_id: str,
        event_type: str,
        exp_time_millis: int,
        count: int,
        nonce: str,
    ) -> None:
        """Handle a callback and write the event into a Kafka topic."""
        manager = self.session_manager
        try:
            producer = await manager.kafka_producer()
            await producer.send(
                topic=manager.settings.GOOGLE_CALLBACK_TOPIC,
                # The key decides the partition and therefore the order events
                # are read in. Per pass, not per class.
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
        except Exception:
            # Deliberate, and a known risk: raising here would make the library
            # answer Google with a 500 and Google would retry, which does not
            # help when the broker is down. The event is lost instead, so the
            # log line is the only trace there is -- hence `exception`, with the
            # ids, rather than the bare `logger.error(e)` this used to be.
            logger.exception(
                "Callback could not be published for class-id: %s; object-id: %s",
                class_id,
                object_id,
            )
