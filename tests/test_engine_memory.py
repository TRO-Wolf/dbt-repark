"""M0.6 memory integration — requires live repark (skip if not importable)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("repark")

from dbt.adapters.contracts.connection import Connection, ConnectionState

from dbt.adapters.repark.connections import ReparkConnectionManager
from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.handle import ReparkConnectionHandle


def test_memory_session_sql_round_trip() -> None:
    wh = tempfile.mkdtemp(prefix="dbt-repark-test-")
    creds = ReparkCredentials.from_dict(
        {
            "catalog_type": "memory",
            "catalog_name": "spark_catalog",
            "warehouse": wh,
            "schema": "default",
            "database": "spark_catalog",
            "threads": 1,
        }
    )
    conn = Connection(type="repark", name="test", credentials=creds)
    opened = ReparkConnectionManager.open(conn)
    assert opened.state == ConnectionState.OPEN
    assert isinstance(opened.handle, ReparkConnectionHandle)

    cur = opened.handle.cursor()
    cur.execute("select 1 as id, 'm0a' as label")
    rows = cur.fetchall()
    assert rows[0][0] == 1
    assert rows[0][1] == "m0a"

    # CTAS-shaped write (single statement).
    cur.execute(
        "create or replace table spark_catalog.default.m0a_demo "
        "using iceberg as select 1 as id, 'ok' as msg"
    )
    cur.execute("select id, msg from spark_catalog.default.m0a_demo")
    out = cur.fetchall()
    assert out == [(1, "ok")]

    opened.handle.close()
    # Anti-goal: do not assert cross-process persistence on memory.
    assert Path(wh).exists()
