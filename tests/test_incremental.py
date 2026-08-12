"""G3-M1a/M1b: append + delete+insert + insert_overwrite materialization tests (memory)."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("repark")
pytest.importorskip("dbt.cli.main")

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.cli.main import dbtRunner

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


def _session_rows(session: Any, sql: str) -> list[tuple[object, ...]]:
    table = session.sql(sql).to_arrow()
    out: list[tuple[object, ...]] = []
    for i in range(table.num_rows):
        out.append(tuple(table.column(j)[i].as_py() for j in range(table.num_columns)))
    return out


def _session_table_names(session: Any) -> set[str]:
    catalog = getattr(session, "catalog", None)
    if catalog is not None and hasattr(catalog, "listTables"):
        names: set[str] = set()
        for item in catalog.listTables("default"):
            if isinstance(item, str):
                names.add(item)
            else:
                name = getattr(item, "name", None)
                if name is not None:
                    names.add(str(name))
        return names
    return set()


@contextmanager
def _shared_memory_session() -> Iterator[dict[str, Any]]:
    """Reuse one memory-catalog session across dbtRunner invokes (process-local catalog).

    Memory catalogs are empty per new ReparkSession. Tests that need a real second
    incremental run share one session and soft-close handles so dbt does not stop it.
    """
    shared: dict[str, Any] = {}
    orig_open = ReparkConnectionManager._open_session.__func__  # type: ignore[attr-defined]
    orig_close = ReparkConnectionHandle.close

    def shared_open(cls: type, credentials: ReparkCredentials) -> Any:
        key = str(credentials.warehouse or "default")
        if key not in shared:
            shared[key] = orig_open(cls, credentials)
        return shared[key]

    def soft_close(self: ReparkConnectionHandle) -> None:
        # Mark closed for dbt lifecycle without stopping the shared process session.
        self._closed = True

    ReparkConnectionManager._open_session = classmethod(shared_open)  # type: ignore[method-assign]
    ReparkConnectionHandle.close = soft_close  # type: ignore[method-assign]
    try:
        yield shared
    finally:
        ReparkConnectionManager._open_session = classmethod(orig_open)  # type: ignore[method-assign]
        ReparkConnectionHandle.close = orig_close  # type: ignore[method-assign]
        for sess in shared.values():
            stop = getattr(sess, "stop", None)
            if callable(stop):
                with suppress(Exception):
                    stop()


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
        """
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


def _run_dbt_subprocess(
    project: Path, profiles: Path, *extra: str
) -> subprocess.CompletedProcess[str]:
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
    return subprocess.run(
        cmd, cwd=str(project), env=env, capture_output=True, text=True, check=False
    )


def _dbt_invoke(
    project: Path,
    profiles: Path,
    *extra: str,
    callbacks: list[Any] | None = None,
) -> tuple[Any, list[str]]:
    """In-process dbtRunner.invoke; returns (result, captured event messages)."""
    msgs: list[str] = []

    def _cb(event: Any) -> None:
        try:
            msgs.append(str(event.info.msg))
        except Exception:
            msgs.append(str(event))

    cbs = list(callbacks or [])
    cbs.append(_cb)
    # Ensure adapter package is importable for the runner process (same interpreter).
    src = str(ROOT / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    runner = dbtRunner(callbacks=cbs)
    result = runner.invoke(
        [
            "run",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(profiles.parent),
            *extra,
        ]
    )
    return result, msgs


def test_valid_incremental_strategies_pin() -> None:
    # Unbound call — method does not use instance state (discovery surface only).
    strategies = ReparkAdapter.valid_incremental_strategies(None)  # type: ignore[arg-type]
    assert strategies == ["append", "delete+insert", "insert_overwrite"]
    doc = ReparkAdapter.valid_incremental_strategies.__doc__ or ""
    assert "Dead-surface" in doc or "sole strategy gate" in doc


# ---------------------------------------------------------------------------
# M1.1 — real materialization, dbtRunner twice, Arrow row/key checks
# ---------------------------------------------------------------------------


def test_m1_1_append_dbt_runner_twice_arrow(tmp_path: Path) -> None:
    """Append: two dbtRunner invokes through the real incremental mat; Arrow keys."""
    project = tmp_path / "proj_append"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='append',
        ) }}
        select 1 as id, 'a' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_append")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_append.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='append',
                ) }}
                select 2 as id, 'b' as v
                union all
                select 3 as id, 'c' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, v from spark_catalog.default.inc_append order by id",
        )
        assert out == [(1, "a"), (2, "b"), (3, "c")], out
        assert len(out) == 3
        names = _session_table_names(session)
        assert "inc_append" in names
        assert not any(n.endswith("__dbt_tmp") for n in names), names


def test_m1_1_delete_insert_dbt_runner_twice_arrow(tmp_path: Path) -> None:
    """delete+insert: second run through real mat; keep / replace / add key checks."""
    project = tmp_path / "proj_di"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            unique_key='id',
        ) }}
        select 1 as id, 'keep' as v
        union all
        select 2 as id, 'old' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_di")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_di.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='delete+insert',
                    unique_key='id',
                ) }}
                select 2 as id, 'new' as v
                union all
                select 3 as id, 'added' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, v from spark_catalog.default.inc_di order by id",
        )
        assert out == [(1, "keep"), (2, "new"), (3, "added")], out


def test_m1_1_delete_insert_multi_key_dbt_runner(tmp_path: Path) -> None:
    """Composite unique_key delete+insert through the real materialization."""
    project = tmp_path / "proj_mk"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            unique_key=['a', 'b'],
        ) }}
        select 1 as a, 'x' as b, 10 as v
        union all
        select 2 as a, 'y' as b, 20 as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_mk")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_mk.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='delete+insert',
                    unique_key=['a', 'b'],
                ) }}
                select 1 as a, 'x' as b, 11 as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select a, b, v from spark_catalog.default.inc_mk order by a",
        )
        assert out == [(1, "x", 11), (2, "y", 20)], out


def test_m1_1_delete_insert_duplicate_batch_keys_dbt_runner(tmp_path: Path) -> None:
    """Duplicate unique_key rows in the batch must not fail the delete half (real mat)."""
    project = tmp_path / "proj_dup"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            unique_key='id',
        ) }}
        select 1 as id, 'a' as v
        union all
        select 2 as id, 'old' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_dup")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_dup.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='delete+insert',
                    unique_key='id',
                ) }}
                select 2 as id, 'n1' as v
                union all
                select 2 as id, 'n2' as v
                union all
                select 3 as id, 'c' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, v from spark_catalog.default.inc_dup order by id, v",
        )
        assert out == [(1, "a"), (2, "n1"), (2, "n2"), (3, "c")], out


def test_m1_1_full_refresh_rename_fqn(tmp_path: Path) -> None:
    """Full-refresh swap uses fully-qualified rename targets (A3 small FQN fix)."""
    project = tmp_path / "proj_fr"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='append',
        ) }}
        select 1 as id, 'a' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_fr")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_fr.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='append',
                ) }}
                select 9 as id, 'full' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles, "--full-refresh")
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, v from spark_catalog.default.inc_fr order by id",
        )
        assert out == [(9, "full")], out
        names = _session_table_names(session)
        assert "inc_fr" in names
        assert not any("__dbt_tmp" in n or "__dbt_backup" in n for n in names), names


# ---------------------------------------------------------------------------
# M1b — insert_overwrite (dynamic partition overwrite via Spark-door)
# ---------------------------------------------------------------------------


def test_m1b_insert_overwrite_single_partition_untouched(tmp_path: Path) -> None:
    """Second batch overwrites exactly its partition; other partitions intact (Arrow)."""
    project = tmp_path / "proj_io_single"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='insert_overwrite',
            partition_by='p',
        ) }}
        select 1 as id, 'a' as p, 'keep_a' as v
        union all
        select 2 as id, 'b' as p, 'old_b' as v
        union all
        select 3 as id, 'c' as p, 'keep_c' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_io")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_io.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='insert_overwrite',
                    partition_by='p',
                ) }}
                select 20 as id, 'b' as p, 'new_b1' as v
                union all
                select 21 as id, 'b' as p, 'new_b2' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, p, v from spark_catalog.default.inc_io order by id",
        )
        assert out == [
            (1, "a", "keep_a"),
            (3, "c", "keep_c"),
            (20, "b", "new_b1"),
            (21, "b", "new_b2"),
        ], out
        # Old partition-b row gone; a and c untouched.
        ids = {r[0] for r in out}
        assert 2 not in ids
        names = _session_table_names(session)
        assert "inc_io" in names
        assert not any(n.endswith("__dbt_tmp") for n in names), names


def test_m1b_insert_overwrite_multi_partition_batch(tmp_path: Path) -> None:
    """Multi-partition batch overwrites only those partitions; others intact."""
    project = tmp_path / "proj_io_multi"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='insert_overwrite',
            partition_by='p',
        ) }}
        select 1 as id, 'a' as p, 'old_a' as v
        union all
        select 2 as id, 'b' as p, 'keep_b' as v
        union all
        select 3 as id, 'c' as p, 'old_c' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_io_m")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_io_m.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='insert_overwrite',
                    partition_by='p',
                ) }}
                select 10 as id, 'a' as p, 'new_a' as v
                union all
                select 30 as id, 'c' as p, 'new_c' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, p, v from spark_catalog.default.inc_io_m order by id",
        )
        assert out == [
            (2, "b", "keep_b"),
            (10, "a", "new_a"),
            (30, "c", "new_c"),
        ], out


def test_m1b_insert_overwrite_composite_partition_by(tmp_path: Path) -> None:
    """Composite partition_by=['p','q'] dynamic overwrite through real mat."""
    project = tmp_path / "proj_io_comp"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='insert_overwrite',
            partition_by=['p', 'q'],
        ) }}
        select 1 as id, 'a' as p, 1 as q, 'keep' as v
        union all
        select 2 as id, 'a' as p, 2 as q, 'old' as v
        union all
        select 3 as id, 'b' as p, 1 as q, 'keep_b' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_io_c")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_io_c.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='insert_overwrite',
                    partition_by=['p', 'q'],
                ) }}
                select 20 as id, 'a' as p, 2 as q, 'new' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles)
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, p, q, v from spark_catalog.default.inc_io_c order by id",
        )
        assert out == [
            (1, "a", 1, "keep"),
            (3, "b", 1, "keep_b"),
            (20, "a", 2, "new"),
        ], out


def test_m1b_insert_overwrite_full_refresh(tmp_path: Path) -> None:
    """Full-refresh replaces the whole partitioned table (CTAS + rename path)."""
    project = tmp_path / "proj_io_fr"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='insert_overwrite',
            partition_by='p',
        ) }}
        select 1 as id, 'a' as p, 'old' as v
        union all
        select 2 as id, 'b' as p, 'old' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_io_fr")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_io_fr.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='insert_overwrite',
                    partition_by='p',
                ) }}
                select 9 as id, 'z' as p, 'full' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, _ = _dbt_invoke(project, profiles, "--full-refresh")
        assert r2.success, r2.exception

        session = next(iter(shared.values()))
        out = _session_rows(
            session,
            "select id, p, v from spark_catalog.default.inc_io_fr order by id",
        )
        assert out == [(9, "z", "full")], out
        names = _session_table_names(session)
        assert "inc_io_fr" in names
        assert not any("__dbt_tmp" in n or "__dbt_backup" in n for n in names), names


def test_m1b_insert_overwrite_refuses_without_partition_by(tmp_path: Path) -> None:
    """Non-partitioned insert_overwrite is a loud refuse (overwrite-all footgun / A10)."""
    project = tmp_path / "proj_io_refuse"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='insert_overwrite',
        ) }}
        select 1 as id, 'x' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_io_np")
    result, msgs = _dbt_invoke(project, profiles)
    assert not result.success
    joined = "\n".join(msgs)
    assert "insert_overwrite" in joined.lower(), joined
    assert "partition_by" in joined.lower(), joined
    # Guidance toward alternatives (charter refuse-loud fence).
    assert "delete+insert" in joined.lower() or "full-refresh" in joined.lower(), joined
    assert (
        "footgun" in joined.lower()
        or "overwrite-all" in joined.lower()
        or ("whole-table" in joined.lower())
    ), joined


# ---------------------------------------------------------------------------
# M1.3 — executed failure injection + residual assertion
# ---------------------------------------------------------------------------


def test_m1_3_fail_after_delete_run_residual(tmp_path: Path) -> None:
    """Run fails after delete; residual = keys deleted, nothing inserted; __dbt_tmp left."""
    project = tmp_path / "proj_residual"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            unique_key='id',
        ) }}
        select 1 as id, 'keep' as v
        union all
        select 2 as id, 'old' as v
        union all
        select 4 as id, 'also_keep' as v
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="inc_res")

    with _shared_memory_session() as shared:
        r1, _ = _dbt_invoke(project, profiles)
        assert r1.success, r1.exception

        (project / "models" / "inc_res.sql").write_text(
            textwrap.dedent(
                """
                {{ config(
                    materialized='incremental',
                    incremental_strategy='delete+insert',
                    unique_key='id',
                ) }}
                select 2 as id, 'new' as v
                union all
                select 3 as id, 'added' as v
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        r2, msgs = _dbt_invoke(
            project,
            profiles,
            "--vars",
            "{repark_fail_after_delete: true}",
        )
        assert not r2.success, "expected injected failure after delete"
        joined = "\n".join(msgs)
        assert "repark_fail_after_delete" in joined or "injected failure after delete" in joined, (
            joined
        )

        session = next(iter(shared.values()))
        residual = _session_rows(
            session,
            "select id, v from spark_catalog.default.inc_res order by id",
        )
        assert residual == [(1, "keep"), (4, "also_keep")], residual
        ids = {r[0] for r in residual}
        assert 2 not in ids  # deleted
        assert 3 not in ids  # never inserted

        names = _session_table_names(session)
        # Durable staging left behind after injected failure (documented residual).
        assert any(n.endswith("__dbt_tmp") for n in names), names


def test_m1_3_fail_after_delete_hook_order_pin() -> None:
    """Materialization source order: delete statement → inject → insert (main)."""
    mat = (ROOT / "src/dbt/include/repark/macros/materializations/incremental.sql").read_text(
        encoding="utf-8"
    )
    assert "repark_fail_after_delete" in mat
    assert "injected failure after delete" in mat
    assert "cannot be rolled back" in mat or "no-op" in mat
    assert "__dbt_tmp" in mat  # staging residual documented in mat comment
    delete_pos = mat.find("statement('delete')")
    # var()/config get of the inject flag (not the header comment mention).
    inject_pos = mat.find("var('repark_fail_after_delete'")
    if inject_pos < 0:
        inject_pos = mat.find("repark_fail_after_delete", delete_pos)
    main_pos = mat.find("statement('main')", inject_pos)
    assert 0 <= delete_pos < inject_pos < main_pos, (delete_pos, inject_pos, main_pos)


# ---------------------------------------------------------------------------
# M1.2 — refuse pins (executed)
# ---------------------------------------------------------------------------


def test_m1_2_strategy_macros_source_pins() -> None:
    """Macro source pins for loud refuse messages + delete vehicle + insert_overwrite."""
    path = ROOT / "src/dbt/include/repark/macros/materializations/incremental_strategies.sql"
    text = path.read_text(encoding="utf-8")
    assert "repark_validate_incremental_strategy" in text
    assert "G3-M2" in text
    assert "insert_overwrite" in text
    assert "repark_get_incremental_insert_overwrite_sql" in text
    assert "insert overwrite" in text.lower()
    assert "not exists" in text.lower()
    assert "partition_by" in text
    assert "microbatch" in text
    assert "select distinct" in text.lower()
    assert "when matched then delete" in text.lower()
    assert "when not matched" not in text.lower()
    assert "cannot run as a single SQL string" in text
    # incremental_predicates plumbing present (not e2e-gated M1a — G3-M2 note in macro).
    assert "incremental_predicates" in text
    # Dynamic composition residual documented (engine static OW).
    assert "static whole-table" in text.lower() or "dynamic partition" in text.lower()
    mat = ROOT / "src/dbt/include/repark/macros/materializations/incremental.sql"
    mtext = mat.read_text(encoding="utf-8")
    assert "{% materialization incremental, adapter='repark' %}" in mtext
    assert "repark_fail_after_delete" in mtext
    assert "delete+insert" in mtext
    assert "insert_overwrite" in mtext
    adapters = (ROOT / "src/dbt/include/repark/macros/adapters.sql").read_text(encoding="utf-8")
    assert "repark_partitioned_by_clause" in adapters


def test_m1_2_dbt_run_refuses_delete_insert_without_unique_key(tmp_path: Path) -> None:
    project = tmp_path / "proj_uk"
    model = textwrap.dedent(
        """
        {{ config(
            materialized='incremental',
            incremental_strategy='delete+insert',
        ) }}
        select 1 as id
        """
    )
    profiles = _write_mini_project(project, model_sql=model)
    proc = _run_dbt_subprocess(project, profiles)
    combined = proc.stdout + proc.stderr
    assert proc.returncode != 0, combined
    assert "unique_key" in combined.lower(), combined


@pytest.mark.parametrize(
    ("strategy", "needle"),
    [
        ("merge", "G3-M2"),
        ("weird_strategy", "not supported"),
    ],
)
def test_m1_2_dbt_run_refuses_unsupported_strategy(
    tmp_path: Path, strategy: str, needle: str
) -> None:
    project = tmp_path / f"proj_{strategy}"
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
    proc = _run_dbt_subprocess(project, profiles)
    combined = proc.stdout + proc.stderr
    assert proc.returncode != 0, combined
    assert needle.lower() in combined.lower() or strategy in combined, combined


def test_m1_2_microbatch_refuse_non_vacuous(tmp_path: Path) -> None:
    """Microbatch with dbt-core required configs; assert adapter's own refuse message."""
    begin = (date.today() - timedelta(days=1)).isoformat()
    project = tmp_path / "proj_mb"
    model = textwrap.dedent(
        f"""
        {{{{ config(
            materialized='incremental',
            incremental_strategy='microbatch',
            event_time='ts',
            batch_size='day',
            begin='{begin}',
        ) }}}}
        select 1 as id, date '{begin}' as ts
        """
    )
    profiles = _write_mini_project(project, model_sql=model, model_name="mb_model")
    result, msgs = _dbt_invoke(project, profiles)
    assert not result.success
    joined = "\n".join(msgs)
    # Must be the adapter macro refuse — not only dbt-core's missing-config parse error.
    assert "dbt-repark refuses incremental_strategy='microbatch'" in joined, joined
    assert "Supported strategies: append, delete+insert, insert_overwrite" in joined, joined


# ---------------------------------------------------------------------------
# NITs — temporary-table refuse restored; rename FQN source pin
# ---------------------------------------------------------------------------


def test_temporary_table_create_table_as_refuses() -> None:
    """M0 temporary-table refuse pin: create_table_as(temporary=True) is loud refuse."""
    text = (ROOT / "src/dbt/include/repark/macros/adapters.sql").read_text(encoding="utf-8")
    assert "does not support temporary tables" in text
    assert "temporary" in text
    # Rename must use fully-qualified to_relation (not bare identifier).
    assert "rename to {{ to_relation }}" in text
    assert "to_relation.identifier" not in text
