"""Engine-presence canary (dbt#4 hollow-venv lesson).

These tests must FAIL — not skip — when repark is missing or is a hollow stub.
They do not use pytest.importorskip.
"""

from __future__ import annotations

from pathlib import Path


def test_repark_import_required_not_skipped() -> None:
    """ImportError here is a red suite: never skip the engine pin."""
    import repark
    from repark import ReparkSession

    assert ReparkSession is not None
    assert hasattr(ReparkSession, "builder")
    path = Path(repark.__file__).resolve()
    # Wheel install lands in site-packages; editable/orchestrator trees are not the pin.
    assert "site-packages" in path.parts, path
    assert path.name == "__init__.py"


def test_repark_session_is_top_level_export() -> None:
    """Adapter spelling stays ``from repark import ReparkSession`` (not repark.spark)."""
    import repark
    from repark import ReparkSession

    assert repark.ReparkSession is ReparkSession
