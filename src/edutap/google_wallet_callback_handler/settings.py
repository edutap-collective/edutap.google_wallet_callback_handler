from pydantic.networks import HttpUrl
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from typing import Literal


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="EDUTAP_",
        case_sensitive=False,
        extra="ignore",
        # extra="allow",
        # extra="forbid",
    )

    ENVIRONMENT: Literal["production", "development", "testing"] = "development"

    GOOGLE_CALLBACK_URL: HttpUrl = "https://localhost/wallet/google/v1/callback"
