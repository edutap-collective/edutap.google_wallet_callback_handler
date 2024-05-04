from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from pydantic import HttpUrl


class GoogleWalletCallbackHandlerSettings(BaseSettings):
    """Settings for Google Wallet Preferences."""

    model_config = SettingsConfigDict(
        env_prefix="EDUTAP_WALLET_GOOGLE_CALLBACK_",
        case_sensitive=False,
        extra="ignore",
        # extra="allow",
    )

    api_prefix: str | None = "/google_callback_api/v1/"

    url: HttpUrl | None = None
    update_url: HttpUrl | None = None

    notification_topic: str | None = "google-wallet-callback-notifications"
    target_topic: str | None = "google-wallet-callback-notifications-decrypted"

    broker_url: str | None = "kafka:19094"
