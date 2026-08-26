"""The service is only a service because the library can find its handler.

`edutap.wallet_google` discovers callback handlers through the entry point group
``edutap.wallet_google.plugins``; nothing imports `KafkaCallbackHandler`
directly. A missing entry point therefore does not fail at import or at startup
-- it fails on the first real callback, with HTTP 500 and no record in Kafka,
while the health check keeps reporting healthy.

These tests read the *installed distribution metadata*, not the source tree.
That is the point: the entry point only exists once the package is installed,
and a test that inspected `pyproject.toml` would pass on a build where it is not.
"""

from edutap.wallet_google.plugins import get_callback_handlers
from edutap.wallet_google.protocols import CallbackHandler
from edutap.wallet_google_callback_handler.kafka import KafkaCallbackHandler
from importlib.metadata import distribution


PLUGIN_GROUP = "edutap.wallet_google.plugins"
DISTRIBUTION = "edutap.wallet_google_callback_handler"


def _plugin_entry_points():
    return [
        entry_point
        for entry_point in distribution(DISTRIBUTION).entry_points
        if entry_point.group == PLUGIN_GROUP
    ]


def test_the_distribution_declares_a_callback_handler_plugin():
    names = [entry_point.name for entry_point in _plugin_entry_points()]

    assert names, (
        f"{DISTRIBUTION} declares no {PLUGIN_GROUP} entry point. "
        "Every callback will be answered with HTTP 500."
    )
    assert any(name.startswith("CallbackHandler") for name in names), (
        f"An entry point in {PLUGIN_GROUP} is only picked up when its name "
        f"starts with the protocol name; got {names}."
    )


def test_the_declared_plugin_is_this_packages_kafka_handler():
    loaded = [entry_point.load() for entry_point in _plugin_entry_points()]

    assert KafkaCallbackHandler in loaded


def test_the_library_hands_out_our_handler():
    """The path the route actually takes, not the metadata one level below it."""
    handlers = get_callback_handlers()

    assert any(isinstance(handler, KafkaCallbackHandler) for handler in handlers)


def test_the_handler_satisfies_the_protocol():
    """`get_plugins` rejects a class that does not -- with ValueError, at runtime.

    The protocol is `runtime_checkable`, so this only checks the presence and
    name of `handle`, not its signature. It still catches a rename.
    """
    assert isinstance(KafkaCallbackHandler(), CallbackHandler)

