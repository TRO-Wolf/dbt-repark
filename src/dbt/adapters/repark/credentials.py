"""Profile credentials for the repark adapter (type: ``repark``)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.exceptions import FailedToConnectError

# Static secret material that must never appear in profiles.yml (M0.9 / plan §1.7).
# Non-exhaustive denylist: any match → loud refuse before session open.
_STATIC_CREDENTIAL_KEYS = frozenset(
    {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "access_key",
        "secret_key",
        "session_token",
        "token",
        "secret_access_key",
        "access_key_id",
        "client_secret",
        "password",
        "secret",
    }
)

# Nested blocks that commonly smuggle static keys (e.g. aws: {…}).
_STATIC_CREDENTIAL_NESTED = frozenset({"aws", "credentials", "auth"})


def _find_static_credential_keys(data: dict[str, Any], *, prefix: str = "") -> list[str]:
    """Return dotted paths of forbidden static-credential fields in *data*."""
    hits: list[str] = []
    for key, value in data.items():
        path = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        lower = key.lower()
        if lower in _STATIC_CREDENTIAL_KEYS:
            hits.append(path)
            continue
        if isinstance(value, dict):
            # Any non-empty nested credential block is refused (even if keys are unknown).
            if lower in _STATIC_CREDENTIAL_NESTED and value:
                hits.append(path)
            hits.extend(_find_static_credential_keys(value, prefix=path))
    return hits


@dataclass
class ReparkCredentials(Credentials):
    """Connection settings for an embedded :class:`repark.ReparkSession`.

    Auth is ambient AWS SDK chain only for Glue/S3 Tables. Memory catalog is for
    single-process unit tests (process-ephemeral — never assert multi-process persistence).
    """

    # Catalog kind for M0: memory (unit) | glue | s3tables (AWS functional — M0b).
    catalog_type: str = "memory"
    # Iceberg warehouse path (memory + glue) or S3 Tables table_bucket_arn (s3tables).
    warehouse: str = ""
    table_bucket_arn: str = ""
    # Iceberg catalog name registered on the session (Spark door).
    catalog_name: str = "spark_catalog"
    # dbt schema maps to Iceberg namespace under the catalog.
    schema: str = "default"
    database: str = "spark_catalog"
    threads: int = 1
    # Optional named shared profile for the ambient AWS chain (never secret material).
    aws_profile_name: str | None = None

    @property
    def type(self) -> str:
        return "repark"

    @property
    def unique_field(self) -> str:
        return f"{self.catalog_type}:{self.catalog_name}:{self.warehouse or self.table_bucket_arn}"

    def _connection_keys(self) -> tuple[str, ...]:
        # Never list secret-like fields — there are none allowed.
        return (
            "catalog_type",
            "catalog_name",
            "warehouse",
            "table_bucket_arn",
            "schema",
            "database",
            "threads",
            "aws_profile_name",
        )

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        """Refuse static credential material before mashumaro builds the object (M0.9)."""
        if isinstance(data, dict):
            hits = _find_static_credential_keys(data)
            if hits:
                raise FailedToConnectError(
                    "dbt-repark refuses static credential fields in profiles.yml "
                    f"(found: {', '.join(hits)}). Use the ambient AWS SDK default chain "
                    "(env / named shared profile / instance or task role). "
                    "See adapter docs § credentials."
                )
        return data

    def __post_init__(self) -> None:
        # threads hard-refuse (OQ-6 / M0).
        if int(self.threads) != 1:
            raise FailedToConnectError(
                f"dbt-repark refuses threads={self.threads}: only threads=1 is supported "
                "until engine concurrency (G3-E6) is proven. Set threads: 1 in profiles.yml."
            )
        kind = (self.catalog_type or "").strip().lower()
        if kind not in {"memory", "glue", "s3tables"}:
            raise FailedToConnectError(
                f"dbt-repark catalog_type={self.catalog_type!r} is not supported "
                "(expected memory | glue | s3tables)."
            )
        if kind == "memory" and not self.warehouse:
            # Allow empty warehouse — connection open will create a temp dir.
            pass
        if kind == "glue" and not self.warehouse:
            raise FailedToConnectError("dbt-repark catalog_type=glue requires warehouse (s3://…).")
        if kind == "s3tables" and not self.table_bucket_arn:
            raise FailedToConnectError(
                "dbt-repark catalog_type=s3tables requires table_bucket_arn."
            )
