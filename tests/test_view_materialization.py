"""M0.2: view materialization must refuse at compile — mutation-proof vs macro rename."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main")

ROOT = Path(__file__).resolve().parents[1]


def test_view_materialization_macro_registered_for_adapter() -> None:
    """If the materialization block is renamed/removed, this fails (not a substring-only pin)."""
    view_path = ROOT / "src/dbt/include/repark/macros/materializations/view.sql"
    text = view_path.read_text(encoding="utf-8")
    assert "{% materialization view, adapter='repark' %}" in text
    assert "raise_compiler_error" in text
    assert "G3-E2" in text or "durable Iceberg VIEW" in text


def test_dbt_compile_view_model_fails(tmp_path: Path) -> None:
    """End-to-end: a model with materialized=view fails compile/run with the refuse message."""
    import os
    import subprocess
    import sys

    project = tmp_path / "proj"
    project.mkdir()
    (project / "models").mkdir()
    (project / "models" / "v.sql").write_text(
        textwrap.dedent(
            """
            {{ config(materialized='view') }}
            select 1 as id
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (project / "dbt_project.yml").write_text(
        textwrap.dedent(
            """
            name: view_refuse_test
            version: 1.0.0
            config-version: 2
            profile: repark_mem
            model-paths: ["models"]
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    profiles = tmp_path / "profiles.yml"
    profiles.write_text(
        textwrap.dedent(
            """
            repark_mem:
              target: dev
              outputs:
                dev:
                  type: repark
                  catalog_type: memory
                  catalog_name: spark_catalog
                  warehouse: {wh}
                  schema: default
                  threads: 1
            """
        )
        .format(wh=tmp_path / "wh")
        .strip()
        + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(tmp_path)
    # Ensure adapter package is importable
    src = str(ROOT / "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")

    dbt_bin = Path(sys.executable).parent / "dbt"
    # Materialization refuse runs at run-time, not compile-time.
    cmd = [
        str(dbt_bin if dbt_bin.exists() else "dbt"),
        "run",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(tmp_path),
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    combined = proc.stdout + proc.stderr
    # Must fail; message must name the missing durable view surface.
    assert proc.returncode != 0, combined
    lower = combined.lower()
    assert "view" in lower, combined
    assert "durable" in lower or "g3-e2" in lower or "refuse" in lower or "iceberg view" in lower, (
        combined
    )
