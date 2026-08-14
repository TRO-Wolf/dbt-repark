"""S-4: honest engine DELETE spellings + loud residual refusals (memory catalog)."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest
from dbt.adapters.contracts.connection import Connection, ConnectionState

from dbt.adapters.repark.connections import ReparkConnectionManager
from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.handle import ReparkConnectionHandle

# Engine G3-E8 needle (both doors). Assert this token; never swallow a generic raise.
G3E8_NEEDLE = "subquery predicates are silently mis-executed"


def _open_memory() -> tuple[Connection, Path]:
    wh = Path(tempfile.mkdtemp(prefix="dbt-repark-s4-dml-"))
    creds = ReparkCredentials.from_dict(
        {
            "catalog_type": "memory",
            "catalog_name": "spark_catalog",
            "warehouse": str(wh),
            "schema": "default",
            "database": "spark_catalog",
            "threads": 1,
        }
    )
    conn = Connection(type="repark", name="s4-dml", credentials=creds)
    opened = ReparkConnectionManager.open(conn)
    assert opened.state == ConnectionState.OPEN
    assert isinstance(opened.handle, ReparkConnectionHandle)
    return opened, wh


def _rows(session: Any, sql: str) -> list[tuple[object, ...]]:
    table = session.sql(sql).to_arrow()
    out: list[tuple[object, ...]] = []
    for i in range(table.num_rows):
        out.append(tuple(table.column(j)[i].as_py() for j in range(table.num_columns)))
    return out


def _exec(session: Any, sql: str) -> None:
    session.sql(sql)


def test_s4_engine_delete_in_single_key_memory() -> None:
    """Uncorrelated DELETE … WHERE key IN (SELECT key FROM staging) — #78 path."""
    opened, _wh = _open_memory()
    session = opened.handle.session
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_in_tgt using iceberg as "
        "select 1 as id, 'a' as v union all select 2 as id, 'b' as v "
        "union all select 3 as id, 'c' as v",
    )
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_in_keys using iceberg as select 2 as id",
    )
    _exec(
        session,
        "delete from spark_catalog.default.s4_in_tgt "
        "where id in (select id from spark_catalog.default.s4_in_keys)",
    )
    out = _rows(session, "select id, v from spark_catalog.default.s4_in_tgt order by id")
    # Must keep non-matching rows (the pre-#78 bug emptied the table).
    assert out == [(1, "a"), (3, "c")], out
    opened.handle.close()


def test_s4_engine_delete_exists_composite_memory() -> None:
    """Correlated DELETE … WHERE EXISTS (composite keys) — #89 path."""
    opened, _wh = _open_memory()
    session = opened.handle.session
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_ex_tgt using iceberg as "
        "select 1 as a, 'x' as b, 10 as v union all "
        "select 2 as a, 'y' as b, 20 as v",
    )
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_ex_keys using iceberg as "
        "select 1 as a, 'x' as b",
    )
    _exec(
        session,
        "delete from spark_catalog.default.s4_ex_tgt as t "
        "where exists ("
        "  select 1 from spark_catalog.default.s4_ex_keys as s "
        "  where s.a = t.a and s.b = t.b"
        ")",
    )
    out = _rows(session, "select a, b, v from spark_catalog.default.s4_ex_tgt order by a")
    assert out == [(2, "y", 20)], out
    opened.handle.close()


def _refuse_and_unchanged(session: Any, dml: str, read_sql: str) -> str:
    before = _rows(session, read_sql)
    with pytest.raises(Exception) as ei:
        result = session.sql(dml)
        if hasattr(result, "to_arrow"):
            result.to_arrow()
    msg = str(ei.value)
    assert G3E8_NEEDLE in msg, msg
    after = _rows(session, read_sql)
    assert after == before, (before, after, msg)
    return msg


def test_s4_engine_refuses_update_in_loudly() -> None:
    """UPDATE … IN (SELECT …) stays refused; engine needle, no swallow, no mutation."""
    opened, _wh = _open_memory()
    session = opened.handle.session
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_up_tgt using iceberg as "
        "select 1 as id, 'a' as v union all select 2 as id, 'b' as v",
    )
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_up_keys using iceberg as select 2 as id",
    )
    msg = _refuse_and_unchanged(
        session,
        "update spark_catalog.default.s4_up_tgt set v = 'z' "
        "where id in (select id from spark_catalog.default.s4_up_keys)",
        "select id, v from spark_catalog.default.s4_up_tgt order by id",
    )
    assert "G3-E8" in msg
    opened.handle.close()


def test_s4_engine_refuses_correlated_in_loudly() -> None:
    """Correlated DELETE … IN (SELECT … WHERE outer.col) stays refused."""
    opened, _wh = _open_memory()
    session = opened.handle.session
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_ci_tgt using iceberg as "
        "select 1 as id, 'a' as v union all select 2 as id, 'b' as v",
    )
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_ci_keys using iceberg as select 2 as id",
    )
    msg = _refuse_and_unchanged(
        session,
        "delete from spark_catalog.default.s4_ci_tgt "
        "where id in ("
        "  select k.id from spark_catalog.default.s4_ci_keys k "
        "  where k.id = spark_catalog.default.s4_ci_tgt.id"
        ")",
        "select id, v from spark_catalog.default.s4_ci_tgt order by id",
    )
    assert "G3-E8" in msg
    opened.handle.close()


@pytest.mark.parametrize(
    "predicate",
    [
        "id = any (select id from spark_catalog.default.s4_aa_keys)",
        "id = all (select id from spark_catalog.default.s4_aa_keys)",
    ],
)
def test_s4_engine_refuses_any_all_loudly(predicate: str) -> None:
    """ANY / ALL subquery predicates stay refused with the engine needle."""
    opened, _wh = _open_memory()
    session = opened.handle.session
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_aa_tgt using iceberg as "
        "select 1 as id, 'a' as v union all select 2 as id, 'b' as v",
    )
    _exec(
        session,
        "create or replace table spark_catalog.default.s4_aa_keys using iceberg as select 2 as id",
    )
    msg = _refuse_and_unchanged(
        session,
        f"delete from spark_catalog.default.s4_aa_tgt where {predicate}",
        "select id, v from spark_catalog.default.s4_aa_tgt order by id",
    )
    assert "G3-E8" in msg
    opened.handle.close()
