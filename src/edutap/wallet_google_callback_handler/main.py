"""The ASGI application: the callback route, the health check, and the wiring.

The callback route itself belongs to `edutap.wallet_google`; what this module
adds is the app it is mounted in, the Sentry and logging setup, and the health
check the orchestrator reads.

`create_app()` is the seam. Importing this module reads no environment and
builds nothing, so a test can construct an application with the settings it
wants instead of exporting variables before the import and hoping nothing
imported the module earlier. `app` below is one such application, built with the
process's own settings, because `uvicorn edutap...main:app` needs a name to
point at.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from edutap.wallet_google.handlers.fastapi import router_callback
from edutap.wallet_google_callback_handler import kafka as kafka_module
from edutap.wallet_google_callback_handler.kafka import KafkaSession
from edutap.wallet_google_callback_handler.log import configure_logging
from edutap.wallet_google_callback_handler.log import logger
from edutap.wallet_google_callback_handler.settings import Settings
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Response
from importlib.metadata import version
from starlette.status import HTTP_200_OK
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE
from typing import Annotated

import sentry_sdk
import uvicorn


__version__ = version("edutap.wallet_google_callback_handler")

SERVICE_NAME = (
    "eduTAP Google Wallet Callback Service (edutap.wallet_google_callback_handler)"
)


def get_session_manager() -> KafkaSession:
    """Return the Kafka session manager the routes publish through.

    Resolved through the module rather than imported by value, so that replacing
    `kafka.kafka_session_manager` reaches the routes too. As a FastAPI dependency
    it can also be overridden per application with `app.dependency_overrides`.
    """
    return kafka_module.kafka_session_manager


SessionManager = Annotated[KafkaSession, Depends(get_session_manager)]


async def read_root() -> dict[str, str]:
    """Name the service, for anyone who reaches it in a browser."""
    return {"Module": "eduTAP Google Wallet Callback Service"}


async def basic_health_check(session_manager: SessionManager) -> Response:
    """Report whether the service can still publish what it accepts.

    A callback it cannot write to Kafka is a callback that is lost, so a closed
    producer has to take the instance out of rotation rather than let it keep
    answering 200.
    """
    producer = await session_manager.kafka_producer()
    if producer._closed:
        raise HTTPException(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka Connection failed.",
        )
    return Response(status_code=HTTP_200_OK)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the application.

    :param settings: the configuration to build it from; read from the
        environment when omitted.
    """
    if settings is None:
        settings = Settings()

    is_development = settings.ENVIRONMENT == "development"

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Start and stop the parts of the service that outlive a request."""
        configure_logging()
        logger.info("%s: Initializing Service Start", SERVICE_NAME)

        # These five options each contradict the SDK's own default, and they are
        # the same set that `sentry_options()` in `edutap.observability_settings`
        # installs for the sibling services, where the reasoning behind each one
        # is written out against a measurement. This package initializes the SDK
        # itself rather than going through that one, so the set is repeated here.
        # Keep the two in step.
        #
        # Until 2026-08-24 this call did the opposite: `send_default_pii=True`
        # and local variables left on the SDK default, which is also True. It was
        # harmless only while `SENTRY_DSN` was empty. This service receives
        # Google's callbacks, so its request bodies carry the identifying datum,
        # and with locals on a bearer token sits in the ASGI scope and reappears
        # in dozens of frames of an event whose rendered `authorization` header
        # reads `[Filtered]` -- Sentry's scrubber matches key names, it does not
        # walk a list of byte tuples.
        #
        # `traces_sample_rate=0` because the receiving end is an error tracker
        # and not an APM: at 1.0 every request became a transaction event.
        if settings.SENTRY_DSN:
            sentry_sdk.init(
                dsn=settings.SENTRY_DSN,
                send_default_pii=False,
                include_local_variables=False,
                max_request_body_size="never",
                max_breadcrumbs=0,
                traces_sample_rate=0,
                debug=is_development,
                environment=settings.ENVIRONMENT,
            )

        logger.info("%s: Service Ready", SERVICE_NAME)
        yield

        logger.info("%s: Initializing Service Shutdown", SERVICE_NAME)
        # The Kafka producer is the one resource that outlives a request and has
        # to be closed by someone.
        await get_session_manager().close()
        logger.info("%s: Service Shutdown completed", SERVICE_NAME)

    app = FastAPI(
        title="eduTAP Google Wallet Callback Service",
        description=(
            "A fastAPI based eduTAP Google Wallet Callback Service, to handle "
            "register and unregister events of passes in the device wallets."
        ),
        summary="",
        version=__version__,
        openapi_url="/openapi.json" if is_development else None,
        lifespan=lifespan,
        root_path="/wallet/google",
    )

    # The route this service exists for. It belongs to `edutap.wallet_google`;
    # all this package does is mount it and register the handler it calls.
    app.include_router(router_callback, prefix="/v1")
    app.add_api_route("/", read_root, methods=["GET"])
    app.add_api_route("/", basic_health_check, methods=["HEAD"])
    return app


app = create_app()


def main() -> None:
    """Run the service under uvicorn -- the console script entry point."""
    uvicorn.run(
        app=app,
        # The service only ever runs in a container, where binding to the
        # container-local loopback would make it unreachable from the outside.
        host="0.0.0.0",  # noqa: S104
        port=8085,
        log_level="debug",
        reload=False,
    )


if __name__ == "__main__":
    main()
