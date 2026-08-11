"""Connection manager: embedded ReparkSession, documented non-atomic begin/commit."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.handle import ReparkConnectionHandle

logger = AdapterLogger("repark")

# Adapter-level default materialization for operator docs / M0.3 (dbt NodeConfig still defaults
# to view; projects must set +materialized: table — sample_project does; view mat refuses).
DEFAULT_MATERIALIZATION = "table"


class ReparkConnectionManager(SQLConnectionManager):
    TYPE = "repark"

    def __init__(self, profile: Any, mp_context: Any) -> None:
        # OQ-6: dbt strips `threads` from credentials before Credentials is built;
        # refuse on the profile/target config (the live path).
        threads = int(getattr(profile, "threads", 1) or 1)
        if threads != 1:
            raise FailedToConnectError(
                f"dbt-repark refuses threads={threads}: only threads=1 is supported "
                "until engine concurrency (G3-E6) is proven. Set threads: 1 in profiles.yml."
            )
        super().__init__(profile, mp_context)

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection

        credentials = cls.get_credentials(connection.credentials)
        try:
            session = cls._open_session(credentials)
            connection.handle = ReparkConnectionHandle(session)
            connection.state = ConnectionState.OPEN
        except FailedToConnectError:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise
        except Exception as exc:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            logger.debug("repark open failed: %s", exc)
            raise FailedToConnectError(str(exc)) from exc
        return connection

    @classmethod
    def _open_session(cls, credentials: ReparkCredentials) -> Any:
        try:
            from repark import ReparkSession
        except ImportError as exc:
            raise FailedToConnectError(
                "dbt-repark requires the repark package (path/editable/git@SHA install from a "
                "known rev — do not pip install repark from PyPI while 0.0.1 is only a "
                f"name-holding placeholder). Import error: {exc}"
            ) from exc

        kind = credentials.catalog_type.strip().lower()
        catalog = credentials.catalog_name
        builder = ReparkSession.builder.appName("dbt-repark")

        catalog_impl = "org.apache.iceberg.spark.SparkCatalog"
        if kind == "memory":
            warehouse = credentials.warehouse or tempfile.mkdtemp(prefix="dbt-repark-wh-")
            Path(warehouse).mkdir(parents=True, exist_ok=True)
            # Config-driven memory catalog registration at getOrCreate.
            builder = (
                builder.config(f"spark.sql.catalog.{catalog}", catalog_impl)
                .config(f"spark.sql.catalog.{catalog}.type", "memory")
                .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
                .config("spark.sql.defaultCatalog", catalog)
            )
            session = builder.getOrCreate()
        elif kind == "glue":
            builder = (
                builder.config(f"spark.sql.catalog.{catalog}", catalog_impl)
                .config(f"spark.sql.catalog.{catalog}.type", "glue")
                .config(f"spark.sql.catalog.{catalog}.warehouse", credentials.warehouse)
                .config("spark.sql.defaultCatalog", catalog)
            )
            if credentials.aws_profile_name:
                # Named profile is resolved by the ambient SDK chain (not static keys).
                import os

                os.environ.setdefault("AWS_PROFILE", credentials.aws_profile_name)
            session = builder.getOrCreate()
        elif kind == "s3tables":
            arn = credentials.table_bucket_arn
            builder = (
                builder.config(f"spark.sql.catalog.{catalog}", catalog_impl)
                .config(f"spark.sql.catalog.{catalog}.type", "s3tables")
                .config(f"spark.sql.catalog.{catalog}.table_bucket_arn", arn)
                .config("spark.sql.defaultCatalog", catalog)
            )
            if credentials.aws_profile_name:
                import os

                os.environ.setdefault("AWS_PROFILE", credentials.aws_profile_name)
            session = builder.getOrCreate()
        else:
            raise FailedToConnectError(f"unsupported catalog_type={credentials.catalog_type!r}")

        # Ensure the dbt schema (Iceberg namespace) exists for memory/dev convenience.
        schema = credentials.schema
        try:
            session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")
        except Exception as exc:
            # IF NOT EXISTS may vary; retry without IF NOT EXISTS when already present is fine.
            logger.debug("CREATE NAMESPACE note: %s", exc)
        return session

    @classmethod
    def get_credentials(cls, credentials: Any) -> ReparkCredentials:
        if not isinstance(credentials, ReparkCredentials):
            # Re-parse enforces static-cred refuse if something slipped through.
            credentials = ReparkCredentials.from_dict(credentials.to_dict(omit_none=True))  # type: ignore[union-attr]
        return credentials

    def cancel(self, connection: Connection) -> None:
        # Embedded engine: no remote cancel API in M0.
        logger.debug("repark cancel: no-op (embedded session)")

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        rows = getattr(cursor, "rowcount", -1)
        return AdapterResponse(_message="OK", rows_affected=rows if rows is not None else -1)

    # -------------------------------------------------------------------------
    # Transaction honesty (M0.8 / plan §1.5): no-ops that do NOT claim atomicity.
    # Never emit BEGIN/COMMIT/ROLLBACK SQL — the engine has no transaction surface.
    # -------------------------------------------------------------------------

    def begin(self) -> None:
        connection = self.get_thread_connection()
        if connection.transaction_open:
            raise DbtRuntimeError(
                f'Tried to begin a transaction on connection "{connection.name}", '
                "but one was already open (dbt-repark has no real transactions)."
            )
        connection.transaction_open = True
        logger.debug(
            "repark begin: documented no-op (each execute is one eager engine statement; "
            "materializations are not multi-statement atomic)"
        )

    def commit(self) -> None:
        connection = self.get_thread_connection()
        if not connection.transaction_open:
            raise DbtRuntimeError(
                f'Tried to commit on connection "{connection.name}", but no transaction was open.'
            )
        connection.transaction_open = False
        logger.debug("repark commit: documented no-op (non-atomic)")

    def clear_transaction(self) -> None:
        conn = self.get_if_exists()
        if conn is not None:
            conn.transaction_open = False
        logger.debug("repark clear_transaction: documented no-op (cannot roll back prior executes)")

    def add_begin_query(self) -> None:
        # Parent would run BEGIN SQL — never do that on repark.
        return None

    def add_commit_query(self) -> None:
        return None

    @classmethod
    def _rollback_handle(cls, connection: Connection) -> None:
        """Documented no-op rollback — prior executes already committed eagerly."""
        logger.debug(
            "repark rollback: documented no-op (cannot undo eager engine statements)"
        )
        handle = connection.handle
        if handle is not None and hasattr(handle, "rollback"):
            handle.rollback()

    @contextmanager
    def exception_handler(self, sql: str) -> Generator[None, None, None]:
        try:
            yield
        except FailedToConnectError:
            raise
        except Exception as exc:
            logger.debug("repark query error on sql=%s: %s", sql[:200], exc)
            raise DbtRuntimeError(str(exc)) from exc
