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
from edutap.observability_settings import install_observability
from edutap.observability_settings import instrument_fastapi_safely
from edutap.observability_settings import ObservabilitySettings
from edutap.observability_settings import OTLP_ENDPOINT_VARIABLE
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

import os
import uvicorn


__version__ = version("edutap.wallet_google_callback_handler")

SERVICE_NAME = (
    "eduTAP Google Wallet Callback Service (edutap.wallet_google_callback_handler)"
)

#: The name telemetry travels under, which is NOT `SERVICE_NAME` above.
#:
#: That one is prose meant for a log line; this one becomes `service.name` on every
#: span and log record, and in Loki `service_name` is the only indexed label. A label
#: carrying spaces and parentheses is one nobody can select on, so the distribution
#: name is used instead -- the same spelling `pip show` prints, which is what the
#: sibling services do.
DISTRIBUTION_NAME = "edutap.wallet_google_callback_handler"


def exports_to_a_collector(observability: ObservabilitySettings) -> bool:
    """Whether an exporter will actually carry a span off this process.

    Both conditions are needed: `telemetry_enabled` is the deliberate off switch, and
    the endpoint decides whether anything is listening. The endpoint is read from the
    environment rather than from a field because `OTEL_EXPORTER_OTLP_ENDPOINT` is the
    variable every OpenTelemetry SDK reads by itself -- giving it a second name here
    would ask an operator to set the same address twice.

    At module level rather than inside `create_app` so it can be asserted on its own.
    A three-line gate hidden in a closure is a gate nobody notices going wrong, and
    what it guards is not free: instrumentation patches the application whether or not
    a receiver exists.
    """
    return observability.telemetry_enabled and bool(
        os.environ.get(OTLP_ENDPOINT_VARIABLE)
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

    # Error reporting, tracing and structured logging in one call, from the house
    # package. It replaces the `sentry_sdk.init()` that used to sit in `lifespan`
    # and repeat that package's option set by hand -- the comment there asked for
    # the two to be kept in step, and this is the version where they cannot drift.
    #
    # THE DSN AND THE ENVIRONMENT ARE PASSED IN, not read by `ObservabilitySettings`
    # itself, and that is the whole subtlety of this call. That class reads the bare
    # `EDUTAP_` prefix; this package deliberately moved off it (see `settings.py`)
    # because `EDUTAP_SENTRY_DSN` reads like an organisation-wide setting while it
    # only ever meant this one service. The deployment sets
    # `EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_SENTRY_DSN`, so letting the shared class
    # look the DSN up itself would find nothing and turn error reporting off in
    # production -- silently, because an absent DSN is a supported state.
    #
    # Everything the class is left to resolve on its own is genuinely stack-wide:
    # `telemetry_enabled`, `log_level`, and the pseudonym settings.
    #
    # LATER THAN IN THE SIBLING SERVICES, and knowingly. `edutap.image_api` installs
    # this at import, before anything resolves settings, so that a process refusing
    # to start is still reported. Here the DSN lives in this package's own settings,
    # so `Settings()` must come first; the unreported window is that one call.
    observability = ObservabilitySettings(
        sentry_dsn=settings.SENTRY_DSN,
        environment=settings.ENVIRONMENT,
    )
    install_observability(
        observability,
        service_name=DISTRIBUTION_NAME,
        service_version=__version__,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Start and stop the parts of the service that outlive a request."""
        configure_logging()
        logger.info("%s: Initializing Service Start", SERVICE_NAME)

        # THE HTTP SIDE OF OBSERVABILITY. The call in `create_app` configures the
        # *process* -- error reporting, the exporter, structured logging -- and knows
        # nothing about this application, so it produces no request spans by itself.
        # Instrumenting needs the finished route table, and the routes are registered
        # after `create_app` builds the application object.
        #
        # `instrument_fastapi_safely` rather than `logfire.instrument_fastapi`: the
        # bare instrumentation writes the raw request path into five span attributes.
        # This service receives Google's callbacks, whose paths carry the identifying
        # datum, so the house helper substitutes the route template and thereby
        # honours `person_uid_mode`.
        #
        # ONLY WHEN SOMETHING EXPORTS. Instrumentation patches the application whether
        # or not a receiver exists, and a span nobody collects is work done on every
        # request.
        if exports_to_a_collector(observability):
            instrument_fastapi_safely(app, observability)
            logger.info("%s: FastAPI instrumentation active", SERVICE_NAME)

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
