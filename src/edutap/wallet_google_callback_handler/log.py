"""Logging setup for the service.

`logger` is safe to import anywhere: `structlog.get_logger()` is lazy and binds
nothing until the first call. Configuring the standard library underneath it is
a process-wide side effect, so it happens once, from the application lifespan --
not at import, where it used to run for anything that so much as touched this
package, the test suite included.
"""

import http.client
import logging
import structlog


logger = structlog.get_logger()


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the standard library logging that structlog renders through.

    Called from the application lifespan rather than from `main()`: the container
    starts uvicorn against `main:app` directly and never calls `main()`, so
    anything configured only there would be missing in production and present
    locally.
    """
    logging.basicConfig(level=level)
    if level > logging.DEBUG:
        return
    # Log the HTTP conversation as well: request line, headers and body, and the
    # response headers. The response body is not covered by this.
    http.client.HTTPConnection.debuglevel = 1
    urllib3_logger = logging.getLogger("requests.packages.urllib3")
    urllib3_logger.setLevel(logging.DEBUG)
    urllib3_logger.propagate = True
