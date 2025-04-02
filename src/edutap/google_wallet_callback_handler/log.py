import http.client
import logging
import structlog


def config_logger(level=logging.INFO):
    # Initializing
    logger = structlog.get_logger(level=level)

    # You must initialize logging, otherwise you'll not see debug output.
    logging.basicConfig()
    if level == logging.DEBUG:
        # These two lines enable debugging at httplib level (requests->urllib3->http.client)
        # You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
        # The only thing missing will be the response.body which is not logged.
        http.client.HTTPConnection.debuglevel = 1
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        # logger.setLevel(logging.DEBUG)
    elif level == logging.INFO:
        # logger.setLevel(logging.INFO)
        pass

    return logger


logger = config_logger(level=logging.INFO)  # noqa: F841