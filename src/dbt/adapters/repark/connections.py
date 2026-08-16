"""Connection manager: embedded ReparkSession, documented non-atomic begin/commit."""

from __future__ import annotations

import atexit
import tempfile
import threading
import warnings
from collections.abc import Generator
from contextlib import contextmanager, suppress
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

# -----------------------------------------------------------------------------------------
# U-1 session registry — one ReparkSession per credentials key, per process.
#
# The engine session is process-scoped: on ``catalog_type: memory`` the Iceberg catalog lives
# inside it, and ``ReparkSession.builder…getOrCreate()`` is a process-global singleton whose
# catalog registrations are fixed at build time. dbt closes a connection between nodes, so a
# per-handle session (the pre-U-1 behaviour) made every relation connection-ephemeral.
#
# Precedent: dbt-duckdb keeps one ``Environment`` on the connection manager and tears it down
# with ``DuckDBConnectionManager.close_all_connections`` registered on ``atexit`` — handle
# close never touches the database. This mirrors that shape.
# -----------------------------------------------------------------------------------------
_SESSION_LOCK = threading.RLock()
_SESSIONS: dict[tuple[str, str, str, str, str], Any] = {}

# Identity fields of a profile that decide which engine session it needs. ``schema`` is
# deliberately absent: namespaces are created per connection under one shared catalog.
_SESSION_KEY_FIELDS = (
    "catalog_type",
    "catalog_name",
    "warehouse",
    "table_bucket_arn",
    "aws_profile_name",
)

# Engine warning emitted when ``getOrCreate`` hands back a session someone else built and
# this builder's configuration was therefore NOT applied (repark facade, session_core).
_REUSE_WARNING_NEEDLE = "using an existing reparksession"


def _session_key(credentials: ReparkCredentials) -> tuple[str, str, str, str, str]:
    """Identity of the engine session a profile needs (never includes secret material)."""
    return (
        (credentials.catalog_type or "").strip().lower(),
        credentials.catalog_name or "",
        str(credentials.warehouse or ""),
        str(credentials.table_bucket_arn or ""),
        str(credentials.aws_profile_name or ""),
    )


def _describe_key(key: tuple[str, str, str, str, str]) -> str:
    return " ".join(
        f"{name}={value!r}" for name, value in zip(_SESSION_KEY_FIELDS, key, strict=True)
    )


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
            # U-1: one process session per credentials key; handles are cheap references.
            session = cls.get_or_create_session(credentials)
            cls._ensure_namespace(session, credentials)
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

    # -------------------------------------------------------------------------
    # U-1 session lifetime: registry, teardown, and the reuse/mismatch refuses.
    # -------------------------------------------------------------------------

    @classmethod
    def get_or_create_session(cls, credentials: ReparkCredentials) -> Any:
        """Return the process session for *credentials*, building it on first use.

        Refuses loud rather than handing back a session bound to a different catalog:
        the embedded ``ReparkSession`` is process-global and its catalog registration is
        fixed at build time, so a second profile in one process would silently read and
        write the *first* profile's warehouse.
        """
        key = _session_key(credentials)
        with _SESSION_LOCK:
            cached = _SESSIONS.get(key)
            if cached is not None:
                return cached
            if _SESSIONS:
                live = next(iter(_SESSIONS))
                raise FailedToConnectError(
                    "dbt-repark refuses a second repark target in one process: the embedded "
                    "ReparkSession is process-global and its catalog registration is fixed at "
                    "session build, so this connection would silently read and write the "
                    "already-open warehouse.\n"
                    f"  already open: {_describe_key(live)}\n"
                    f"  requested:    {_describe_key(key)}\n"
                    "Run one target per dbt process, or call "
                    "ReparkConnectionManager.close_all() before switching targets."
                )
            session = cls._open_session(credentials)
            _SESSIONS[key] = session
            return session

    @classmethod
    def live_sessions(cls) -> dict[tuple[str, str, str, str, str], Any]:
        """Live view of the process session registry (introspection / tests)."""
        return _SESSIONS

    @classmethod
    def close_all(cls) -> None:
        """Stop every cached engine session. The adapter teardown hook (U-1).

        Registered on ``atexit`` (dbt-duckdb precedent:
        ``DuckDBConnectionManager.close_all_connections``). Never called per node and never
        called from handle close — those are exactly the paths that destroyed the memory
        catalog mid-run.
        """
        with _SESSION_LOCK:
            sessions = list(_SESSIONS.values())
            _SESSIONS.clear()
        for session in sessions:
            stop = getattr(session, "stop", None)
            if callable(stop):
                with suppress(Exception):
                    stop()

    def cleanup_all(self) -> None:
        """Close dbt's connections; the engine session deliberately survives (U-1).

        dbt calls this from the ``finally`` of every task invocation
        (``dbt/task/runnable.py``), i.e. once per ``dbt run`` / ``dbt test`` / ``dbt build``
        — **not** once per process. Stopping the session here would put the catalog back
        on a per-invocation lifetime and break ``dbt run`` followed by ``dbt test`` inside
        one process. Session teardown is :meth:`close_all` on ``atexit``; memory catalogs
        stay honestly *process*-ephemeral (G3-E1), never connection-ephemeral.
        """
        super().cleanup_all()

    @classmethod
    def _ensure_namespace(cls, session: Any, credentials: ReparkCredentials) -> None:
        """Create the connection's Iceberg namespace under the shared catalog."""
        catalog = credentials.catalog_name
        schema = credentials.schema
        try:
            session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")
        except Exception as exc:
            # IF NOT EXISTS may vary; retry without IF NOT EXISTS when already present is fine.
            logger.debug("CREATE NAMESPACE note: %s", exc)

    @classmethod
    def _get_or_create_engine_session(cls, builder: Any, credentials: ReparkCredentials) -> Any:
        """``builder.getOrCreate()`` that refuses a session it does not own (§5.4).

        The engine warns — and then carries on — when ``getOrCreate`` hands back an existing
        session whose already-registered catalogs keep their own configuration. Silently
        accepting that means writing to a warehouse other than the one in ``profiles.yml``.
        Read the warning instead of ignoring it.
        """
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            session = builder.getOrCreate()

        for record in caught:
            text = str(record.message)
            if _REUSE_WARNING_NEEDLE in text.lower():
                raise FailedToConnectError(
                    "dbt-repark refuses to reuse a ReparkSession it does not own: engine knobs "
                    "and catalog registrations are fixed at session build, so this profile's "
                    "configuration was not applied and the connection would target whatever "
                    "warehouse the live session was built with.\n"
                    f"  profiles.yml asked for: {_describe_key(_session_key(credentials))}\n"
                    f"  engine warning: {text}\n"
                    "Build the repark session through dbt only, or call "
                    "ReparkConnectionManager.close_all() before reconnecting."
                )
            # Not ours to swallow — re-emit anything else the engine had to say.
            warnings.warn_explicit(
                record.message, record.category, record.filename, record.lineno
            )
        return session

    @classmethod
    def _verify_catalog_binding(
        cls, session: Any, *, catalog: str, suffix: str, expected: str
    ) -> None:
        """Refuse loud when the live session's catalog config is not what the profile asked for."""
        key = f"spark.sql.catalog.{catalog}.{suffix}"
        conf = getattr(session, "conf", None)
        if conf is None or not hasattr(conf, "get"):
            return
        try:
            live = conf.get(key)
        except Exception as exc:  # engine may not expose the key; not a mismatch signal
            logger.debug("repark conf.get(%s) unavailable: %s", key, exc)
            return
        if live is None or str(live) == str(expected):
            return
        raise FailedToConnectError(
            f"dbt-repark session catalog mismatch on {key}: the live engine session is bound to "
            f"{live!r} but profiles.yml asked for {expected!r}. The embedded ReparkSession fixes "
            "catalog registration at build time, so continuing would read and write the wrong "
            "warehouse. Run one target per dbt process, or call "
            "ReparkConnectionManager.close_all() before switching targets."
        )

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
            session = cls._get_or_create_engine_session(builder, credentials)
            cls._verify_catalog_binding(
                session, catalog=catalog, suffix="warehouse", expected=str(warehouse)
            )
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
            session = cls._get_or_create_engine_session(builder, credentials)
            cls._verify_catalog_binding(
                session, catalog=catalog, suffix="warehouse", expected=str(credentials.warehouse)
            )
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
            session = cls._get_or_create_engine_session(builder, credentials)
            cls._verify_catalog_binding(
                session, catalog=catalog, suffix="table_bucket_arn", expected=str(arn)
            )
        else:
            raise FailedToConnectError(f"unsupported catalog_type={credentials.catalog_type!r}")

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
        logger.debug("repark rollback: documented no-op (cannot undo eager engine statements)")
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


# U-1 teardown hook: the process — not a handle, not a dbt task — owns session lifetime.
# dbt-duckdb precedent: atexit.register(DuckDBConnectionManager.close_all_connections).
atexit.register(ReparkConnectionManager.close_all)
