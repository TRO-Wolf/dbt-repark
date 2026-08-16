"""Cursor/handle unit tests without a live repark engine (mock session)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from dbt.adapters.repark.handle import ReparkConnectionHandle, ReparkCursor


class _FakeDF:
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_arrow(self) -> pa.Table:
        return self._table


class _FakeSession:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.stopped = False

    def sql(self, query: str) -> _FakeDF:
        self.queries.append(query)
        return _FakeDF(pa.table({"a": [1, 2], "b": ["x", "y"]}))

    def stop(self) -> None:
        self.stopped = True


def test_cursor_execute_and_fetchall() -> None:
    session = _FakeSession()
    cur = ReparkCursor(session)
    cur.execute("select 1 as a")
    assert session.queries == ["select 1 as a"]
    rows = cur.fetchall()
    assert rows == [(1, "x"), (2, "y")]
    assert cur.description is not None
    assert cur.description[0][0] == "a"


def test_cursor_rejects_bindings() -> None:
    cur = ReparkCursor(_FakeSession())
    with pytest.raises(RuntimeError, match="bound parameters"):
        cur.execute("select 1", bindings=(1,))


def test_handle_close_releases_without_stopping_session() -> None:
    """U-1 inverts the pre-fix pin: close() must NOT stop the process session.

    The memory catalog lives in the session and dbt closes a connection between nodes, so
    stopping here made every relation connection-ephemeral. Teardown is
    ``ReparkConnectionManager.close_all()`` (atexit), never handle close.
    """
    session = _FakeSession()
    handle = ReparkConnectionHandle(session)
    handle.cursor().execute("select 1")
    handle.close()
    assert session.stopped is False
    # The session stays usable for the next connection dbt opens.
    assert handle.session is session
    handle.session.sql("select 1")
    with pytest.raises(RuntimeError, match="closed"):
        handle.cursor()


def test_handle_rollback_is_noop() -> None:
    handle = ReparkConnectionHandle(_FakeSession())
    handle.rollback()  # must not raise
