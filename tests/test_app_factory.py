"""`create_app` is the seam this package was missing.

Before it, the application was built at import from an environment read at
import, so a test could only influence it by exporting variables before the
first import of the module -- and only once per process. Everything below is a
property that could not be asserted at all until the factory existed.
"""

from edutap.wallet_google_callback_handler.main import create_app
from edutap.wallet_google_callback_handler.settings import Settings

import pytest


def _schema_paths(app) -> set[str]:
    """Return what the application actually exposes.

    Read from the generated schema rather than from `app.routes`: an included
    router appears there as one opaque `_IncludedRouter` entry with no path of
    its own, so the route list cannot answer this question.
    """
    return set(app.openapi()["paths"])


def test_the_callback_route_is_mounted():
    """The route the service exists for, contributed by the library."""
    assert "/v1/callback" in _schema_paths(
        create_app(Settings(ENVIRONMENT="development"))
    )


def test_building_two_applications_does_not_mount_the_route_twice():
    """A regression guard with a name: "Duplicate Operation ID".

    `include_router` used to run inside the lifespan, so a second startup in the
    same process mounted the callback route a second time and generating the
    schema warned about the collision. This suite turns warnings into errors, so
    the second `openapi()` call below is the assertion.
    """
    first = create_app(Settings(ENVIRONMENT="development"))
    second = create_app(Settings(ENVIRONMENT="development"))

    assert "/v1/callback" in _schema_paths(first)
    assert "/v1/callback" in _schema_paths(second)


def test_injected_settings_win_over_the_environment(monkeypatch):
    """The point of passing settings in: the environment stops being an input."""
    monkeypatch.setenv(
        "EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_ENVIRONMENT", "development"
    )

    app = create_app(Settings(ENVIRONMENT="production"))

    assert app.openapi_url is None


@pytest.mark.parametrize(
    ("environment", "expected"),
    [("development", "/openapi.json"), ("production", None), ("testing", None)],
)
def test_the_schema_is_served_in_development_only(environment, expected):
    """The endpoint is public by construction; its schema does not have to be.

    Not a security boundary -- the signature check is -- but there is no reason
    to describe the callback contract to the open internet.
    """
    app = create_app(Settings(ENVIRONMENT=environment))

    assert app.openapi_url == expected


def test_the_service_is_mounted_under_its_deployment_prefix():
    """Traefik routes `/wallet/google/` here and does not strip the prefix.

    `root_path` is what makes the generated URLs match what the client asked
    for; it does not affect matching.
    """
    assert create_app(Settings()).root_path == "/wallet/google"
