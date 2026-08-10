"""M0.9 credential refuse matrix + threads + catalog_type validation."""

from __future__ import annotations

import pytest
from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.repark.connections import DEFAULT_MATERIALIZATION
from dbt.adapters.repark.credentials import ReparkCredentials


def test_default_materialization_is_table() -> None:
    assert DEFAULT_MATERIALIZATION == "table"


def test_memory_credentials_ok() -> None:
    creds = ReparkCredentials.from_dict(
        {
            "catalog_type": "memory",
            "schema": "default",
            "database": "spark_catalog",
            "threads": 1,
        }
    )
    assert creds.type == "repark"
    assert creds.catalog_type == "memory"


@pytest.mark.parametrize(
    "bad_key",
    [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "access_key",
        "secret_key",
        "session_token",
        "token",
        "client_secret",
        "password",
        "secret",
        "secret_access_key",
        "access_key_id",
    ],
)
def test_static_credential_fields_refused(bad_key: str) -> None:
    data = {
        "catalog_type": "memory",
        "schema": "default",
        "database": "spark_catalog",
        "threads": 1,
        bad_key: "should-not-be-accepted",
    }
    with pytest.raises(FailedToConnectError, match="refuses static credential"):
        ReparkCredentials.from_dict(data)


@pytest.mark.parametrize("nested", ["aws", "credentials", "auth"])
def test_nested_credential_blocks_refused(nested: str) -> None:
    data = {
        "catalog_type": "memory",
        "schema": "default",
        "database": "spark_catalog",
        "threads": 1,
        nested: {"aws_access_key_id": "x", "aws_secret_access_key": "y"},
    }
    with pytest.raises(FailedToConnectError, match="refuses static credential"):
        ReparkCredentials.from_dict(data)


def test_threads_not_one_refused() -> None:
    with pytest.raises(FailedToConnectError, match="threads"):
        ReparkCredentials.from_dict(
            {
                "catalog_type": "memory",
                "schema": "default",
                "database": "spark_catalog",
                "threads": 4,
            }
        )


def test_glue_requires_warehouse() -> None:
    with pytest.raises(FailedToConnectError, match="warehouse"):
        ReparkCredentials.from_dict(
            {
                "catalog_type": "glue",
                "schema": "default",
                "database": "spark_catalog",
                "threads": 1,
            }
        )


def test_s3tables_requires_arn() -> None:
    with pytest.raises(FailedToConnectError, match="table_bucket_arn"):
        ReparkCredentials.from_dict(
            {
                "catalog_type": "s3tables",
                "schema": "default",
                "database": "spark_catalog",
                "threads": 1,
            }
        )
