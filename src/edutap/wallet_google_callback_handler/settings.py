"""This package's own configuration."""

from pydantic.networks import HttpUrl
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from typing import Literal


class Settings(BaseSettings):
    """Settings read from `EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_*`."""

    # The prefix is EDUTAP_ plus the module path, like every other eduTAP
    # package. It used to be the bare EDUTAP_, which reached into the shared
    # namespace: EDUTAP_ENVIRONMENT and EDUTAP_SENTRY_DSN read like
    # organisation-wide settings but were only ever this one service's.
    #
    # Note what is NOT covered by this prefix and must not be: EDUTAP_KAFKA_*
    # is cross-cutting (see kafka.py) and EDUTAP_WALLET_GOOGLE_* belongs to the
    # wallet library.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EDUTAP_WALLET_GOOGLE_CALLBACK_HANDLER_",
        case_sensitive=False,
        extra="ignore",
    )

    ENVIRONMENT: Literal["production", "development", "testing"] = "development"

    GOOGLE_CALLBACK_URL: HttpUrl = HttpUrl(
        "https://localhost/wallet/google/v1/callback"
    )

    SENTRY_DSN: str | None = None
