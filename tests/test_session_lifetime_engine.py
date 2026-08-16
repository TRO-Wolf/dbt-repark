"""U-1 live gates: the recon §3.3 reproduction and multi-node dbt on one process session.

G1.1 ``dbt build`` (source ref + 2 chained models + 3 data tests) green in one process.
G1.2 ``dbt run`` then ``dbt test`` as two invocations in one process; memory stays
     honestly *process*-ephemeral (G3-E1) rather than connection-ephemeral.
G1.3 connection A writes, closes; connection B reads — the exact reproduction.
G1.4 two warehouses in one process refuse loud, naming both.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("repark")
pytest.importorskip("dbt.cli.main")

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.exceptions import FailedToConnectError
from dbt.cli.main import dbtRunner

from dbt.adapters.repark.connections import ReparkConnectionManager
from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.handle import ReparkConnectionHandle

ROOT = Path(__file__).resolve().parents[1]


def _memory_credentials(warehouse: Path) -> ReparkCredentials:
    return ReparkCredentials.from_dict(
        {
            "catalog_type": "memory",
            "catalog_name": "spark_catalog",
            "warehouse": str(warehouse),
            "schema": "default",
            "database": "spark_catalog",
            "threads": 1,
        }
    )


def _connect(warehouse: Path, name: str) -> Connection:
    conn = Connection(type="repark", name=name, credentials=_memory_credentials(warehouse))
    opened = ReparkConnectionManager.open(conn)
    assert opened.state == ConnectionState.OPEN
    assert isinstance(opened.handle, ReparkConnectionHandle)
    return opened


def _rows(session: Any, sql: str) -> list[tuple[Any, ...]]:
    table = session.sql(sql).to_arrow()
    return [
        tuple(table.column(j)[i].as_py() for j in range(table.num_columns))
        for i in range(table.num_rows)
    ]


# ---------------------------------------------------------------------------
# G1.3 — the recon §3.3 reproduction
# ---------------------------------------------------------------------------


def test_g1_3_connection_b_reads_what_connection_a_wrote(tmp_path: Path) -> None:
    """Write in A, close A, open B, read.

    Pre-U-1 this raised ``table 'spark_catalog.default.u1_t1' not found``: close() stopped
    the session and the memory catalog went with it.
    """
    warehouse = tmp_path / "wh"

    conn_a = _connect(warehouse, "a")
    session_a = conn_a.handle.session
    cur_a = conn_a.handle.cursor()
    cur_a.execute(
        "create or replace table spark_catalog.default.u1_t1 using iceberg as select 1 as id"
    )
    cur_a.execute("select id from spark_catalog.default.u1_t1")
    assert cur_a.fetchall() == [(1,)]

    conn_a.handle.close()

    conn_b = _connect(warehouse, "b")
    session_b = conn_b.handle.session
    # Same live session object — not a fresh one with an empty catalog.
    assert session_b is session_a, (id(session_a), id(session_b))

    cur_b = conn_b.handle.cursor()
    cur_b.execute("select id from spark_catalog.default.u1_t1")
    assert cur_b.fetchall() == [(1,)], "connection B must see connection A's relation"

    # …and B's own write survives for a third connection.
    cur_b.execute(
        "create or replace table spark_catalog.default.u1_t2 using iceberg as select 2 as id"
    )
    conn_b.handle.close()

    conn_c = _connect(warehouse, "c")
    cur_c = conn_c.handle.cursor()
    cur_c.execute("select id from spark_catalog.default.u1_t2")
    assert cur_c.fetchall() == [(2,)]
    conn_c.handle.close()


def test_u1_close_all_stops_the_live_session_and_clears_the_registry(tmp_path: Path) -> None:
    warehouse = tmp_path / "wh"
    conn = _connect(warehouse, "teardown")
    assert len(ReparkConnectionManager.live_sessions()) == 1

    conn.handle.close()
    # Handle close alone leaves the session registered and usable.
    assert len(ReparkConnectionManager.live_sessions()) == 1

    ReparkConnectionManager.close_all()
    assert ReparkConnectionManager.live_sessions() == {}

    # A fresh connection after teardown builds a genuinely new session.
    conn2 = _connect(warehouse, "after-teardown")
    assert len(ReparkConnectionManager.live_sessions()) == 1
    assert conn2.handle.session is not conn.handle.session
    conn2.handle.close()


def test_u1_reopening_the_same_target_emits_no_engine_reuse_warning(tmp_path: Path) -> None:
    """The registry hit means ``getOrCreate`` is never asked a second time."""
    import warnings

    warehouse = tmp_path / "wh"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        first = _connect(warehouse, "one")
        first.handle.close()
        second = _connect(warehouse, "two")
        second.handle.close()
    reuse = [str(w.message) for w in caught if "existing ReparkSession" in str(w.message)]
    assert reuse == [], reuse
    assert first.handle.session is second.handle.session


# ---------------------------------------------------------------------------
# G1.4 — warehouse mismatch refuses loud
# ---------------------------------------------------------------------------


def test_g1_4_second_warehouse_in_one_process_refuses_loud(tmp_path: Path) -> None:
    wh_a = tmp_path / "wh_a"
    wh_b = tmp_path / "wh_b"
    conn_a = _connect(wh_a, "target-a")

    conn_b = Connection(type="repark", name="target-b", credentials=_memory_credentials(wh_b))
    with pytest.raises(FailedToConnectError) as ei:
        ReparkConnectionManager.open(conn_b)
    msg = str(ei.value)
    assert str(wh_a) in msg, msg
    assert str(wh_b) in msg, msg
    assert "close_all" in msg
    assert conn_b.state == ConnectionState.FAIL
    assert conn_b.handle is None

    # The live target is untouched by the refusal.
    cur = conn_a.handle.cursor()
    cur.execute("select 1 as ok")
    assert cur.fetchall() == [(1,)]
    conn_a.handle.close()


# ---------------------------------------------------------------------------
# G1.1 / G1.2 — functional multi-node dbt
# ---------------------------------------------------------------------------


def _write_project(project: Path, warehouse: Path) -> Path:
    """A source + two chained models + data tests — the shape that used to fail."""
    models = project / "models"
    models.mkdir(parents=True, exist_ok=True)

    (models / "stg_orders.sql").write_text(
        "select id, amount from {{ source('u1_raw', 'u1_raw_orders') }}\n", encoding="utf-8"
    )
    (models / "mart_orders.sql").write_text(
        "select id, amount * 2 as doubled from {{ ref('stg_orders') }}\n", encoding="utf-8"
    )
    (models / "sources.yml").write_text(
        textwrap.dedent(
            """
            version: 2
            sources:
              - name: u1_raw
                database: spark_catalog
                schema: default
                tables:
                  - name: u1_raw_orders
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (models / "schema.yml").write_text(
        textwrap.dedent(
            """
            version: 2
            models:
              - name: stg_orders
                columns:
                  - name: id
                    data_tests:
                      - not_null
                      - unique
              - name: mart_orders
                columns:
                  - name: id
                    data_tests:
                      - not_null
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (project / "dbt_project.yml").write_text(
        textwrap.dedent(
            """
            name: u1_session_lifetime
            version: 1.0.0
            config-version: 2
            profile: repark_mem
            model-paths: ["models"]
            models:
              +materialized: table
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    profiles = project / "profiles.yml"
    profiles.write_text(
        textwrap.dedent(
            f"""
            repark_mem:
              target: dev
              outputs:
                dev:
                  type: repark
                  catalog_type: memory
                  catalog_name: spark_catalog
                  warehouse: {warehouse}
                  schema: default
                  threads: 1
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return profiles


def _invoke(project: Path, profiles: Path, *args: str) -> Any:
    src = str(ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    return dbtRunner().invoke(
        [*args, "--project-dir", str(project), "--profiles-dir", str(profiles.parent)]
    )


def _statuses(result: Any) -> dict[str, str]:
    return {r.node.name: str(r.status) for r in result.result}


def _assert_no_node_failed(statuses: dict[str, str]) -> None:
    bad = {k: v for k, v in statuses.items() if "error" in v.lower() or "fail" in v.lower()}
    assert bad == {}, bad


def _seed_source_table(warehouse: Path) -> Any:
    """Create the source relation through the adapter, before dbt ever runs."""
    conn = _connect(warehouse, "source-seed")
    cur = conn.handle.cursor()
    cur.execute(
        "create or replace table spark_catalog.default.u1_raw_orders using iceberg as "
        "select 1 as id, 10 as amount union all select 2 as id, 20 as amount"
    )
    session = conn.handle.session
    conn.handle.close()  # exactly what dbt does between nodes
    return session


def test_g1_1_dbt_build_multi_node_one_process(tmp_path: Path) -> None:
    """``dbt build``: source ref + 2 models + 3 data tests, green in one process.

    Pre-U-1 the first model passed and every downstream node ERRORed with
    ``table … not found`` because that node's connection close destroyed the catalog.
    """
    project = tmp_path / "proj_build"
    warehouse = tmp_path / "wh"
    profiles = _write_project(project, warehouse)
    session = _seed_source_table(warehouse)

    result = _invoke(project, profiles, "build")
    statuses = _statuses(result) if result.result else {}
    assert result.success, (result.exception, statuses)

    assert "stg_orders" in statuses, statuses
    assert "mart_orders" in statuses, statuses
    _assert_no_node_failed(statuses)
    # Two models plus the three data tests, all in one invocation.
    assert len(statuses) >= 5, statuses

    assert _rows(
        session, "select id, doubled from spark_catalog.default.mart_orders order by id"
    ) == [(1, 20), (2, 40)]


def test_g1_2_dbt_run_then_dbt_test_two_invocations_one_process(tmp_path: Path) -> None:
    """``dbt run`` then ``dbt test`` as separate invocations inside one process.

    dbt calls ``cleanup_all`` in the ``finally`` of *every* task invocation; the engine
    session must survive it, so the tests find the relations the run built.
    """
    project = tmp_path / "proj_run_test"
    warehouse = tmp_path / "wh"
    profiles = _write_project(project, warehouse)
    session = _seed_source_table(warehouse)

    run_result = _invoke(project, profiles, "run")
    assert run_result.success, run_result.exception

    test_result = _invoke(project, profiles, "test")
    assert test_result.success, test_result.exception
    statuses = _statuses(test_result)
    assert statuses, "dbt test selected no nodes"
    _assert_no_node_failed(statuses)

    assert _rows(session, "select count(*) as n from spark_catalog.default.stg_orders") == [(2,)]


def test_g1_2_memory_catalog_stays_process_ephemeral(tmp_path: Path) -> None:
    """The honest boundary (G3-E1): teardown — a new process — still starts empty.

    Asserts the distinction rather than papering over it: U-1 makes the catalog
    *process*-ephemeral, which is what G3-E1 always claimed. It does not make memory
    durable across processes, and nothing here should imply that it does.
    """
    project = tmp_path / "proj_ephemeral"
    warehouse = tmp_path / "wh"
    profiles = _write_project(project, warehouse)
    _seed_source_table(warehouse)

    assert _invoke(project, profiles, "run").success

    # Model the process boundary: stop the session, then reconnect.
    ReparkConnectionManager.close_all()
    conn = _connect(warehouse, "next-process")
    cur = conn.handle.cursor()
    with pytest.raises(Exception, match=r"(?i)not found|does not exist|no table"):
        cur.execute("select id from spark_catalog.default.stg_orders")
    conn.handle.close()
