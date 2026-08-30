"""`create_app` is the seam this package was missing.

Before it, the application was built at import from an environment read at
import, so a test could only influence it by exporting variables before the
first import of the module -- and only once per process. Everything below is a
property that could not be asserted at all until the factory existed.
"""

from edutap.wallet_google_callback_handler import main as main_module
from edutap.wallet_google_callback_handler.main import create_app
from edutap.wallet_google_callback_handler.settings import Settings

import pytest
import sentry_sdk


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


def test_sentry_is_initialised_without_personal_data(monkeypatch):
    """The six options of the 2026-08-24 fix, pinned -- now installed, not repeated.

    They no longer live in this package: `install_observability` applies the set that
    `edutap.observability_settings` chose against measurements, and the reasoning for
    each option is written out there. What stays worth asserting HERE is that the set
    still arrives, because the way to lose it is no longer to delete arguments but to
    stop routing through that call at all.

    Patched on the `sentry_sdk` module rather than on an attribute of `main`: the
    house package looks `init` up on the module when it calls it, and this package no
    longer imports the SDK.
    """
    captured: dict = {}
    monkeypatch.setattr(sentry_sdk, "init", lambda **kwargs: captured.update(kwargs))

    create_app(
        Settings(
            ENVIRONMENT="production",
            SENTRY_DSN="https://public@sentry.invalid/1",
        )
    )

    assert captured["send_default_pii"] is False
    assert captured["include_local_variables"] is False
    assert captured["max_request_body_size"] == "never"
    assert captured["max_breadcrumbs"] == 0
    assert captured["traces_sample_rate"] == 0
    assert captured["environment"] == "production"


def test_the_dsn_is_read_under_this_packages_own_prefix(monkeypatch):
    """The regression that would otherwise be silent.

    `ObservabilitySettings` reads the bare `EDUTAP_` prefix. This package moved off
    it on purpose (see `settings.py`): `EDUTAP_SENTRY_DSN` reads like an
    organisation-wide setting while it only ever meant this one service, and the
    deployment accordingly sets `EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_SENTRY_DSN`.

    So the DSN is handed to `install_observability` rather than looked up by it. Were
    that to change, error reporting would switch itself off in production and nothing
    would say so -- an absent DSN is a supported state, not a failure.
    """
    captured: dict = {}
    monkeypatch.setattr(sentry_sdk, "init", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setenv(
        "EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_SENTRY_DSN",
        "https://public@sentry.invalid/2",
    )
    monkeypatch.delenv("EDUTAP_SENTRY_DSN", raising=False)

    create_app()

    assert captured["dsn"] == "https://public@sentry.invalid/2"


def test_telemetry_travels_under_the_distribution_name():
    """`service.name` has to be selectable in Loki, where it is the only index label.

    `SERVICE_NAME` is prose for a log line -- spaces and parentheses -- and would make
    a label nobody can query. The distribution name is what `pip show` prints and what
    the sibling services use.
    """
    assert main_module.DISTRIBUTION_NAME == "edutap.wallet_google_callback_handler"


def test_without_a_dsn_sentry_is_not_initialised_at_all(monkeypatch):
    """An empty DSN is how the deployment switches the error tracker off.

    It is also what kept the 2026-08-24 leak harmless for as long as it existed, so
    the two belong in the same file.
    """
    calls = []
    monkeypatch.setattr(sentry_sdk, "init", lambda **kwargs: calls.append(kwargs))

    create_app(Settings(SENTRY_DSN=None))

    assert calls == []


def test_instrumentation_is_gated_on_there_being_a_receiver(monkeypatch):
    """Instrumentation costs something on every request; it is not installed blind.

    `logfire.instrument_fastapi` patches the application whether or not anything
    collects what it produces, so the gate is what keeps an unmonitored deployment
    from paying for spans nobody reads.
    """
    from edutap.observability_settings import ObservabilitySettings

    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    assert not main_module.exports_to_a_collector(ObservabilitySettings())

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector.invalid:4318")
    assert main_module.exports_to_a_collector(ObservabilitySettings())

    monkeypatch.setenv("EDUTAP_TELEMETRY_ENABLED", "false")
    assert not main_module.exports_to_a_collector(ObservabilitySettings())
