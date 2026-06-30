import os

import pytest

from src.core.config import Settings


def _base_env(**overrides):
    env = {
        "DATABASE_URL": "postgresql://user:pw@host:5432/db",
        "BUCKET_ID": "my-bucket",
    }
    env.update(overrides)
    return env


def test_settings_loads_required_fields(monkeypatch):
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)

    s = Settings(_env_file=None)

    assert s.database_url == "postgresql://user:pw@host:5432/db"
    assert s.bucket_id == "my-bucket"
    assert s.aws_region == "ca-central-1"  # default


def test_settings_rejects_non_postgres_database_url(monkeypatch):
    for k, v in _base_env(DATABASE_URL="mysql://user:pw@host/db").items():
        monkeypatch.setenv(k, v)

    with pytest.raises(ValueError):
        Settings(_env_file=None)


def test_settings_rejects_empty_bucket_id(monkeypatch):
    for k, v in _base_env(BUCKET_ID="   ").items():
        monkeypatch.setenv(k, v)

    with pytest.raises(ValueError):
        Settings(_env_file=None)


def test_settings_accepts_legacy_aws_var_names(monkeypatch):
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("ACCESS_KEY_ID", "legacy-key")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "legacy-secret")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    s = Settings(_env_file=None)

    assert s.aws_access_key_id == "legacy-key"
    assert s.aws_secret_access_key == "legacy-secret"


def test_settings_exports_aws_env_vars_for_boto3_and_polars(monkeypatch):
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("ACCESS_KEY_ID", "legacy-key")
    monkeypatch.setenv("SECRET_ACCESS_KEY", "legacy-secret")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

    Settings(_env_file=None)

    # boto3/polars read straight from os.environ, not from the Settings object
    assert os.environ["AWS_ACCESS_KEY_ID"] == "legacy-key"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "legacy-secret"
    assert os.environ["AWS_DEFAULT_REGION"] == "ca-central-1"
