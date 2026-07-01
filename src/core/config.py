"""
Centralized application configuration.

Single source of truth for every environment variable the platform reads.
Previously each module called `os.getenv(...)` directly (often with a
different default, or none at all), so a missing/misspelled var would only
surface as a cryptic failure deep inside whichever ingestion job happened to
run first. Settings validates everything once, at import time, with one
clear error listing exactly what's missing.

Usage:
    from src.core.config import settings
    settings.database_url
    settings.bucket_id
"""

from __future__ import annotations

import os

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Only psycopg2 is installed (see requirements.txt) -- restricting to the
# schemes it actually supports so a typo'd "+psycopg" (v3) URL fails here
# with a clear message instead of an ImportError deep inside SQLAlchemy.
_ALLOWED_DATABASE_SCHEMES = ("postgresql://", "postgresql+psycopg2://")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = Field(default="local", validation_alias=AliasChoices("ENV", "ENVIRONMENT"))

    database_url: str = Field(validation_alias=AliasChoices("DATABASE_URL"))

    bucket_id: str = Field(validation_alias=AliasChoices("BUCKET_ID"))

    aws_access_key_id: str | None = Field(
        default=None, validation_alias=AliasChoices("AWS_ACCESS_KEY_ID", "ACCESS_KEY_ID")
    )
    aws_secret_access_key: str | None = Field(
        default=None, validation_alias=AliasChoices("AWS_SECRET_ACCESS_KEY", "SECRET_ACCESS_KEY")
    )
    aws_region: str = Field(
        default="ca-central-1", validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION")
    )

    fred_api_key: str | None = Field(default=None, validation_alias=AliasChoices("FRED_API_KEY"))
    openfigi_api_key: str | None = Field(default=None, validation_alias=AliasChoices("OPENFIGI_API_KEY"))

    redis_url: str | None = Field(default=None, validation_alias=AliasChoices("REDIS_URL"))

    log_level: str = Field(default="INFO", validation_alias=AliasChoices("LOG_LEVEL"))

    @field_validator("database_url")
    @classmethod
    def _validate_database_url(cls, v: str) -> str:
        if not v.startswith(_ALLOWED_DATABASE_SCHEMES):
            raise ValueError(
                f"DATABASE_URL must start with one of {_ALLOWED_DATABASE_SCHEMES}, got: {v[:20]}..."
            )
        return v

    @field_validator("bucket_id")
    @classmethod
    def _validate_bucket_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("BUCKET_ID must not be empty")
        return v.strip()

    @model_validator(mode="after")
    def _export_aws_env(self) -> Settings:
        """
        boto3 and polars's S3 reader both read AWS credentials straight from
        the *process* environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
        AWS_DEFAULT_REGION) -- they have no idea this Settings object exists.
        If the .env file only defines the legacy names (ACCESS_KEY_ID, ...),
        those two libraries would silently fail to authenticate. We normalize
        once here so every S3 call in the codebase works regardless of which
        naming convention is in .env.
        """
        if self.aws_access_key_id and not os.environ.get("AWS_ACCESS_KEY_ID"):
            os.environ["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
        if self.aws_secret_access_key and not os.environ.get("AWS_SECRET_ACCESS_KEY"):
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
        if not os.environ.get("AWS_DEFAULT_REGION"):
            os.environ["AWS_DEFAULT_REGION"] = self.aws_region
        return self


settings = Settings()
