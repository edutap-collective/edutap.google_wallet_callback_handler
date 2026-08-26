"""Retired settings variables must stop the service, not be ignored."""

from edutap.wallet_google_callback_handler.env_guard import check_retired_env_vars
from edutap.wallet_google_callback_handler.env_guard import CURRENT_PREFIX
from edutap.wallet_google_callback_handler.env_guard import RETIRED_KEYS
from edutap.wallet_google_callback_handler.settings import Settings

import pytest


def test_clean_environment_passes():
    check_retired_env_vars({f"{CURRENT_PREFIX}ENVIRONMENT": "production"})


def test_empty_environment_passes():
    check_retired_env_vars({})


def test_retired_variable_raises():
    with pytest.raises(RuntimeError) as excinfo:
        check_retired_env_vars({"EDUTAP_SENTRY_DSN": "https://x@y/1"})
    message = str(excinfo.value)
    assert "EDUTAP_SENTRY_DSN" in message
    assert CURRENT_PREFIX in message


def test_detection_is_case_insensitive():
    with pytest.raises(RuntimeError):
        check_retired_env_vars({"edutap_sentry_dsn": "https://x@y/1"})


def test_message_names_every_offending_variable():
    with pytest.raises(RuntimeError) as excinfo:
        check_retired_env_vars(
            {
                "EDUTAP_SENTRY_DSN": "https://x@y/1",
                "EDUTAP_ENVIRONMENT": "production",
                "PATH": "/usr/bin",
            }
        )
    message = str(excinfo.value)
    assert "EDUTAP_SENTRY_DSN" in message
    assert "EDUTAP_ENVIRONMENT" in message
    assert "PATH" not in message
    assert "2 retired" in message


def test_shared_edutap_variables_are_left_alone():
    """The decisive difference to the prefix-based guards in the sibling services.

    The retired prefix here was the bare ``EDUTAP_``. These variables belong to
    other packages and must survive -- a prefix check would flag them and the
    service would never start again.
    """
    check_retired_env_vars(
        {
            "EDUTAP_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "EDUTAP_KAFKA_GOOGLE_CALLBACK_TOPIC": "google_callback",
            "EDUTAP_WALLET_GOOGLE_GOOGLE_ENVIRONMENT": "production",
        }
    )


def test_retired_keys_cover_every_settings_field():
    """Guard against a field being added without its retired name.

    The old prefix was ``EDUTAP_``, so the retired spelling of any field is
    ``EDUTAP_`` plus the field name. If this fails, extend RETIRED_KEYS.
    """
    expected = {f"EDUTAP_{name}" for name in Settings.model_fields}
    assert expected == RETIRED_KEYS
