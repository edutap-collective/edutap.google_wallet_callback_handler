"""Three settings namespaces meet in this service and must not bleed into each other.

- ``EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_`` -- this package
- ``EDUTAP_KAFKA_``                          -- cross-cutting Kafka configuration
- ``EDUTAP_WALLET_GOOGLE_``                  -- the wallet library

The package prefix was the bare ``EDUTAP_`` until 2026-08-08, which reached into
the shared namespace. These tests keep the separation from silently regressing --
both settings classes use ``extra="ignore"``, so a mix-up produces defaults
rather than an error.
"""

from edutap.wallet_google_callback_handler.kafka import KafkaSettings
from edutap.wallet_google_callback_handler.settings import Settings


def test_package_settings_read_their_own_prefix(monkeypatch):
    monkeypatch.setenv(
        "EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_ENVIRONMENT", "production"
    )

    assert Settings().ENVIRONMENT == "production"


def test_package_settings_ignore_the_retired_bare_prefix(monkeypatch):
    """``EDUTAP_ENVIRONMENT`` looked organisation-wide but was only ever ours."""
    monkeypatch.delenv(
        "EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_ENVIRONMENT", raising=False
    )
    monkeypatch.setenv("EDUTAP_ENVIRONMENT", "production")

    assert Settings().ENVIRONMENT == "development"


def test_kafka_settings_keep_the_cross_cutting_prefix(monkeypatch):
    """Kafka configuration is shared across services and stays where it is.

    Pulling it under the package prefix would duplicate the same broker list into
    every service.
    """
    monkeypatch.setenv("EDUTAP_KAFKA_BOOTSTRAP_SERVERS", "broker:9092")

    assert KafkaSettings().BOOTSTRAP_SERVERS == "broker:9092"


def test_the_two_namespaces_do_not_capture_each_other(monkeypatch):
    monkeypatch.setenv("EDUTAP_KAFKA_GOOGLE_CALLBACK_TOPIC", "some.topic")
    monkeypatch.setenv("EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_ENVIRONMENT", "testing")

    assert KafkaSettings().GOOGLE_CALLBACK_TOPIC == "some.topic"
    assert Settings().ENVIRONMENT == "testing"


def test_bootstrap_servers_accepts_a_comma_separated_list(monkeypatch):
    """Ansible renders one string for several brokers; the field allows both."""
    monkeypatch.setenv("EDUTAP_KAFKA_BOOTSTRAP_SERVERS", '["a:9092","b:9092"]')

    assert KafkaSettings().BOOTSTRAP_SERVERS == ["a:9092", "b:9092"]
