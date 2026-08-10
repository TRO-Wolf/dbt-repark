"""G3-M1a: append + delete+insert strategy plumbing (memory catalog; no persistence claim)."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("repark")
pytest.importorskip("dbt.cli.main")

from dbt.adapters.contracts.connection import Connection, ConnectionState

from dbt.adapters.repark.connections import ReparkConnectionManager
from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.handle import ReparkConnectionHandle
from dbt.adapters.repark.impl import ReparkAdapter

ROOT = Path(__file__).resolve().parents[1]


def _open_memory() -> tuple[Connection, Path]:
    wh = Path(tempfile.mkdtemp(prefix="dbt-repark-m1a-"))
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
    conn = Connection(type="repark", name="m1a", credentials=creds)
    opened = ReparkConnectionManager.open(conn)
    assert opened.state == ConnectionState.OPEN
    assert isinstance(opened.handle, ReparkConnectionHandle)
    return opened, wh


def _rows(handle: ReparkConnectionHandle, sql: str) -> list[tuple[object, ...]]:
    cur = handle.cursor()
    cur.execute(sql)
    return cur.fetchall()


def test_valid_incremental_strategies_pin() -> None:
    # Unbound call — method does not use instance state.
    strategies = ReparkAdapter.valid_incremental_strategies(None)  # type: ignore[arg-type]
    assert strategies == ["append", "delete+insert"]


def test_m1_1_append_second_run_arrow() -> None:
    """Append: first CTAS, second INSERT; row counts + keys via Arrow (memory only)."""
    conn, _wh = _open_memory()
    h = conn.handle
    assert isinstance(h, ReparkConnectionHandle)

    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_append "
        "using iceberg as select 1 as id, 'a' as v"
    )
    assert _rows(h, "select id, v from spark_catalog.default.m1_append order by id") == [
        (1, "a")
    ]

    # Second incremental batch staging + append insert (strategy plumbing).
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_append__dbt_tmp "
        "using iceberg as select 2 as id, 'b' as v union all select 3 as id, 'c' as v"
    )
    h.cursor().execute(
        "insert into spark_catalog.default.m1_append (id, v) "
        "select id, v from spark_catalog.default.m1_append__dbt_tmp"
    )
    out = _rows(h, "select id, v from spark_catalog.default.m1_append order by id")
    assert out == [(1, "a"), (2, "b"), (3, "c")]
    assert len(out) == 3
    h.close()


def test_m1_1_delete_insert_second_run_arrow() -> None:
    """delete+insert: delete matching keys then insert batch; key-level Arrow checks."""
    conn, _wh = _open_memory()
    h = conn.handle
    assert isinstance(h, ReparkConnectionHandle)

    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_di "
        "using iceberg as "
        "select 1 as id, 'keep' as v union all select 2 as id, 'old' as v"
    )
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_di__dbt_tmp "
        "using iceberg as "
        "select 2 as id, 'new' as v union all select 3 as id, 'added' as v"
    )
    # Execute 1: delete vehicle (MERGE WHEN MATCHED THEN DELETE) — not M2 upsert.
    h.cursor().execute(
        "merge into spark_catalog.default.m1_di as DBT_INTERNAL_DEST "
        "using ("
        "  select distinct id from spark_catalog.default.m1_di__dbt_tmp"
        ") as DBT_INTERNAL_SOURCE "
        "on DBT_INTERNAL_DEST.id = DBT_INTERNAL_SOURCE.id "
        "when matched then delete"
    )
    mid = _rows(h, "select id, v from spark_catalog.default.m1_di order by id")
    assert mid == [(1, "keep")]

    # Execute 2: insert batch
    h.cursor().execute(
        "insert into spark_catalog.default.m1_di (id, v) "
        "select id, v from spark_catalog.default.m1_di__dbt_tmp"
    )
    out = _rows(h, "select id, v from spark_catalog.default.m1_di order by id")
    assert out == [(1, "keep"), (2, "new"), (3, "added")]
    h.close()


def test_m1_3_delete_insert_residual_after_delete_failure() -> None:
    """M1.3: fail after delete → residual = keys deleted, nothing inserted (§1.5)."""
    conn, _wh = _open_memory()
    h = conn.handle
    assert isinstance(h, ReparkConnectionHandle)

    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_residual "
        "using iceberg as "
        "select 1 as id, 'keep' as v union all select 2 as id, 'old' as v "
        "union all select 4 as id, 'also_keep' as v"
    )
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_residual__dbt_tmp "
        "using iceberg as "
        "select 2 as id, 'new' as v union all select 3 as id, 'added' as v"
    )
    # Macro shape: DISTINCT keys source (avoids MERGE_CARDINALITY_VIOLATION).
    h.cursor().execute(
        "merge into spark_catalog.default.m1_residual as DBT_INTERNAL_DEST "
        "using ("
        "  select distinct id from spark_catalog.default.m1_residual__dbt_tmp"
        ") as DBT_INTERNAL_SOURCE "
        "on DBT_INTERNAL_DEST.id = DBT_INTERNAL_SOURCE.id "
        "when matched then delete"
    )
    # Injected failure: do not run insert. Residual is real and non-atomic.
    residual = _rows(
        h, "select id, v from spark_catalog.default.m1_residual order by id"
    )
    assert residual == [(1, "keep"), (4, "also_keep")], residual
    ids = {r[0] for r in residual}
    assert 2 not in ids  # deleted
    assert 3 not in ids  # never inserted
    h.close()


def test_m1_1_delete_insert_multi_key_arrow() -> None:
    """Composite unique_key delete+insert via DISTINCT key projection."""
    conn, _wh = _open_memory()
    h = conn.handle
    assert isinstance(h, ReparkConnectionHandle)
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_mk "
        "using iceberg as "
        "select 1 as a, 'x' as b, 10 as v union all select 2 as a, 'y' as b, 20 as v"
    )
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_mk__dbt_tmp "
        "using iceberg as select 1 as a, 'x' as b, 11 as v"
    )
    h.cursor().execute(
        "merge into spark_catalog.default.m1_mk as DBT_INTERNAL_DEST "
        "using ("
        "  select distinct a, b from spark_catalog.default.m1_mk__dbt_tmp"
        ") as DBT_INTERNAL_SOURCE "
        "on DBT_INTERNAL_DEST.a = DBT_INTERNAL_SOURCE.a "
        "and DBT_INTERNAL_DEST.b = DBT_INTERNAL_SOURCE.b "
        "when matched then delete"
    )
    h.cursor().execute(
        "insert into spark_catalog.default.m1_mk (a, b, v) "
        "select a, b, v from spark_catalog.default.m1_mk__dbt_tmp"
    )
    out = _rows(h, "select a, b, v from spark_catalog.default.m1_mk order by a")
    assert out == [(1, "x", 11), (2, "y", 20)], out
    h.close()


def test_m1_1_delete_insert_duplicate_batch_keys() -> None:
    """Duplicate unique_key rows in the batch must not fail the delete half."""
    conn, _wh = _open_memory()
    h = conn.handle
    assert isinstance(h, ReparkConnectionHandle)
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_dup "
        "using iceberg as select 1 as id, 'a' as v union all select 2 as id, 'old' as v"
    )
    h.cursor().execute(
        "create or replace table spark_catalog.default.m1_dup__dbt_tmp "
        "using iceberg as "
        "select 2 as id, 'n1' as v union all select 2 as id, 'n2' as v "
        "union all select 3 as id, 'c' as v"
    )
    h.cursor().execute(
        "merge into spark_catalog.default.m1_dup as DBT_INTERNAL_DEST "
        "using ("
        "  select distinct id from spark_catalog.default.m1_dup__dbt_tmp"
        ") as DBT_INTERNAL_SOURCE "
        "on DBT_INTERNAL_DEST.id = DBT_INTERNAL_SOURCE.id "
        "when matched then delete"
    )
    h.cursor().execute(
        "insert into spark_catalog.default.m1_dup (id, v) "
        "select id, v from spark_catalog.default.m1_dup__dbt_tmp"
    )
    out = _rows(h, "select id, v from spark_catalog.default.m1_dup order by id, v")
    # Both batch rows for id=2 are appended after delete (append semantics of insert half).
    assert out == [(1, "a"), (2, "n1"), (2, "n2"), (3, "c")], out
    h.close()


def test_m1_2_strategy_macros_refuse_pins() -> None:
    """Macro source pins for loud refuse messages (merge / insert_overwrite / other)."""
    path = ROOT / "src/dbt/include/repark/macros/materializations/incremental_strategies.sql"
    text = path.read_text(encoding="utf-8")
    assert "repark_validate_incremental_strategy" in text
    assert "G3-M2" in text
    assert "OQ-5" in text
    assert "insert_overwrite" in text
    assert "microbatch" in text
    # Delete vehicle must DISTINCT keys (MERGE cardinality) and not claim M2 upsert.
    assert "select distinct" in text.lower()
    assert "when matched then delete" in text.lower()
    assert "when not matched" not in text.lower()
    # Fallback macro must refuse single-string multi-statement (engine one-execute rule).
    assert "cannot run as a single SQL string" in text
    mat = ROOT / "src/dbt/include/repark/macros/materializations/incremental.sql"
    mtext = mat.read_text(encoding="utf-8")
    assert "{% materialization incremental, adapter='repark' %}" in mtext
    assert "repark_fail_after_delete" in mtext
    assert "delete+insert" in mtext
    assert "§1.5" in mtext or "1.5" in mtext or "not atomic" in mtext.lower() or "no-op" in mtext


def _write_mini_project(
    project: Path,
    *,
    model_sql: str,
    model_name: str = "inc_model",
    vars_yaml: str | None = None,
) -> Path:
    project.mkdir(parents=True, exist_ok=True)
    (project / "models").mkdir(exist_ok=True)
    (project / "models" / f"{model_name}.sql").write_text(
        model_sql.strip() + "\n", encoding="utf-8"
    )
    dbt_project = textwrap.dedent(
        f"""
        name: m1a_inc_test
        version: 1.0.0
        config-version: 2
        profile: repark_mem
        model-paths: ["models"]
        models:
          +materialized: table
        """
    )
    if vars_yaml:
        dbt_project += "\n" + vars_yaml
    (project / "dbt_project.yml").write_text(dbt_project.strip() + "\n", encoding="utf-8")
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
                  warehouse: {project / "wh"}
                  schema: default
                  threads: 1
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return profiles


def _run_dbt(project: Path, profiles: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(profiles.parent)
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    dbt_bin = Path(sys.executable).parent / "dbt"
    cmd = [
        str(dbt_bin if dbt_bin.exists() else "dbt"),
        "run",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles.parent),
        *extra,
    ]
    return subprocess.run(cmd, cwd=str(project), env=env, capture_output=True, text=True, check=False)


@pytest.mark.parametrize(
    ("strategy", "needle"),
    [
        ("merge", "G3-M2"),
        ("insert_overwrite", "OQ-5"),
        ("microbatch", "microbatch"),
        ("weird_strategy", "not supported"),
    ],
)
def test_m1_2_dbt_run_refuses_unsupported_strategy(tmp_path: Path, strategy: str, needle: str) -> None:
    project = tmp_path / "proj"
    model = textwrap.dedent(
        f"""
        {{{{ config(
            materialized='incremental',
            incremental_strategy='{strategy}',
            unique_key='id',
        ) }}}}
        select 1 as id, 'x' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model)
    proc = _run_dbt(project, profiles)
    combined = proc.stdout + proc.stderr
    assert proc.returncode != 0, combined
    assert needle.lower() in combined.lower() or strategy in combined, combined


def test_m1_1_dbt_run_incremental_first_run_smoke(tmp_path: Path) -> None:
    """dbt run smoke: incremental mat registers for append + delete+insert (first run CTAS).

    Second-run correctness is single-process Arrow strategy plumbing (M1.1 unit tests above).
    Memory catalog is process-local — subprocess dbt cannot see prior-session tables.
    """
    for strategy, name in (("append", "inc_append"), ("delete+insert", "inc_di")):
        project = tmp_path / f"proj_{name}"
        model = textwrap.dedent(
            f"""
            {{{{ config(
                materialized='incremental',
                incremental_strategy='{strategy}',
                unique_key='id',
            ) }}}}
            select 1 as id, 'a' as v
            """
        )
        profiles = _write_mini_project(project, model_sql=model, model_name=name)
        r1 = _run_dbt(project, profiles)
        assert r1.returncode == 0, r1.stdout + r1.stderr


def test_m1_3_fail_after_delete_hook_in_materialization() -> None:
    """M1.3 hook: materialization raises when repark_fail_after_delete is set (between executes)."""
    mat = (
        ROOT / "src/dbt/include/repark/macros/materializations/incremental.sql"
    ).read_text(encoding="utf-8")
    assert "repark_fail_after_delete" in mat
    assert "injected failure after delete" in mat
    # Residual language must stay honest (§1.5).
    assert "cannot be rolled back" in mat or "no-op" in mat
    # Order pin: delete statement before the inject, inject before insert (main).
    delete_pos = mat.find("statement('delete')")
    inject_pos = mat.find("repark_fail_after_delete")
    main_pos = mat.find("statement('main')", inject_pos)
    assert 0 <= delete_pos < inject_pos < main_pos, (delete_pos, inject_pos, main_pos)
