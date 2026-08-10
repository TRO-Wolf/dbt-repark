"""M0.4 hygiene: no private repark imports; sample defaults to table; no PyPI repark dep."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"


def _python_files() -> list[Path]:
    return list(SRC.rglob("*.py"))


def test_no_private_repark_imports() -> None:
    """Adapter must not import repark private/underscore modules or reach into crates."""
    offenders: list[str] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("repark._") or alias.name.startswith("repark.session._"):
                        offenders.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                if mod.startswith("repark._") or "._" in mod:
                    offenders.append(f"{path}:{mod}")
                for alias in node.names:
                    if alias.name.startswith("_") and mod == "repark":
                        offenders.append(f"{path}:from repark import {alias.name}")
    assert offenders == [], f"private repark imports: {offenders}"


def test_pyproject_does_not_depend_on_pypi_repark() -> None:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    # Must not list repark as an installable dependency (placeholder 0.0.1 trap).
    deps_section = text.split("dependencies = [")[1].split("]")[0]
    # Strip comments so the anti-goal note does not false-positive.
    dep_lines = [
        line
        for line in deps_section.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    dep_body = "\n".join(dep_lines).lower()
    assert "repark" not in dep_body


def test_sample_project_defaults_to_table() -> None:
    yml = (ROOT / "sample_project" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "+materialized: table" in yml or "materialized: table" in yml


def test_adapter_documents_default_materialization_table() -> None:
    from dbt.adapters.repark.connections import DEFAULT_MATERIALIZATION
    from dbt.adapters.repark.impl import ReparkAdapter

    assert DEFAULT_MATERIALIZATION == "table"
    assert ReparkAdapter.default_materialization == "table"
