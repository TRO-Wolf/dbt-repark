"""Plugin registration and adapter surface smoke tests."""

from __future__ import annotations

from dbt.adapters.repark import Plugin
from dbt.adapters.repark.connections import ReparkConnectionManager
from dbt.adapters.repark.impl import ReparkAdapter


def test_plugin_type_repark() -> None:
    assert Plugin.credentials.type == "repark" or Plugin.credentials().type == "repark"  # type: ignore[misc]


def test_adapter_uses_connection_manager() -> None:
    assert ReparkAdapter.ConnectionManager is ReparkConnectionManager
    assert ReparkConnectionManager.TYPE == "repark"


def test_begin_commit_are_noop_methods() -> None:
    # Documented no-ops exist (M0.8) — source-level pin.
    import inspect

    src = inspect.getsource(ReparkConnectionManager.begin)
    assert "transaction_open" in src
    assert "BEGIN" not in src or "no-op" in src.lower()
    src_c = inspect.getsource(ReparkConnectionManager.commit)
    assert "transaction_open" in src_c
