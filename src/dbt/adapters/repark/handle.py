"""DB-API-ish handle over an embedded ReparkSession (one statement per execute)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pyarrow as pa


class ReparkCursor:
    """Minimal cursor: ``execute`` → ``session.sql`` → Arrow; fetch for SELECT-shaped results."""

    def __init__(self, session: Any) -> None:
        self._session = session
        self._table: pa.Table | None = None
        self._row_index = 0
        self.description: list[tuple[Any, ...]] | None = None
        self.rowcount: int = -1

    def execute(self, sql: str, bindings: Sequence[Any] | None = None) -> ReparkCursor:
        if bindings is not None:
            raise RuntimeError(
                "dbt-repark does not support bound parameters; expand values in Jinja macros"
            )
        # Strip trailing semicolons / whitespace — multi-statement still refused by the engine.
        text = sql.strip().rstrip(";").strip()
        if not text:
            self._table = None
            self.description = None
            self.rowcount = 0
            return self

        df = self._session.sql(text)
        # DDL/DML is eager at sql(); collect only when a result set is meaningful.
        # Always materialize via Arrow for a uniform description/fetch path.
        table = df.to_arrow()
        self._table = table
        self._row_index = 0
        self.rowcount = table.num_rows
        self.description = [
            (name, str(field.type), None, None, None, None, None)
            for name, field in zip(table.column_names, table.schema, strict=True)
        ]
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._table is None:
            return []
        rows: list[tuple[Any, ...]] = []
        n_cols = self._table.num_columns
        for i in range(self._table.num_rows):
            rows.append(tuple(self._table.column(j)[i].as_py() for j in range(n_cols)))
        self._row_index = self._table.num_rows
        return rows

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if self._table is None:
            return []
        end = min(self._row_index + size, self._table.num_rows)
        rows: list[tuple[Any, ...]] = []
        n_cols = self._table.num_columns
        for i in range(self._row_index, end):
            rows.append(tuple(self._table.column(j)[i].as_py() for j in range(n_cols)))
        self._row_index = end
        return rows

    def fetchone(self) -> tuple[Any, ...] | None:
        batch = self.fetchmany(1)
        return batch[0] if batch else None

    def close(self) -> None:
        self._table = None


class ReparkConnectionHandle:
    """Connection handle exposed as ``connection.handle`` to dbt's SQLConnectionManager."""

    def __init__(self, session: Any) -> None:
        self._session = session
        self._closed = False

    def cursor(self) -> ReparkCursor:
        if self._closed:
            raise RuntimeError("repark connection handle is closed")
        return ReparkCursor(self._session)

    def close(self) -> None:
        """Release this handle. **Never** stops the engine session (U-1).

        The engine session is process-scoped and owned by
        :class:`~dbt.adapters.repark.connections.ReparkConnectionManager`'s session
        registry, not by any one handle. dbt closes a connection between nodes; on
        ``catalog_type: memory`` the Iceberg catalog *lives in the session*, so stopping
        it here made every relation a prior node created disappear (``dbt build`` on a
        two-node project failed, ``dbt run`` then ``dbt test`` failed, snapshots could not
        resolve ``ref()``). Session teardown is
        ``ReparkConnectionManager.close_all()``, wired to ``atexit``.
        """
        if self._closed:
            return
        self._closed = True

    def rollback(self) -> None:
        """Documented no-op (M0.8). Eager ``sql()`` commits cannot be undone."""
        return None

    @property
    def session(self) -> Any:
        return self._session
