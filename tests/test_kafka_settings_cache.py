"""Kafka configuration is read once per process, and a test can say when.

`kafka.py` used to evaluate `KafkaSettings()` at module level. Whatever the
environment held when something first imported the module became the topic and
the broker list for the rest of the process -- including for every later test,
whose `monkeypatch.setenv` then had no effect and no error.
"""

from edutap.wallet_google_callback_handler.kafka import get_kafka_settings


def test_the_settings_are_read_once():
    assert get_kafka_settings() is get_kafka_settings()


def test_clearing_the_cache_picks_up_a_changed_environment(monkeypatch):
    before = get_kafka_settings().GOOGLE_CALLBACK_TOPIC

    monkeypatch.setenv("EDUTAP_KAFKA_GOOGLE_CALLBACK_TOPIC", "edutap.test.pass.state")
    get_kafka_settings.cache_clear()

    assert before == "edutap.google_callback"
    assert get_kafka_settings().GOOGLE_CALLBACK_TOPIC == "edutap.test.pass.state"


def test_a_stale_cache_would_be_visible(monkeypatch):
    """Without the clear, the environment is simply ignored -- silently.

    Pinned so that anyone who removes the autouse fixture in `conftest.py` sees
    why it is there.
    """
    get_kafka_settings()
    monkeypatch.setenv("EDUTAP_KAFKA_GOOGLE_CALLBACK_TOPIC", "edutap.ignored")

    assert get_kafka_settings().GOOGLE_CALLBACK_TOPIC == "edutap.google_callback"
