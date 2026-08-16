"""U-1 engine-free pins: handle close must never stop the session; loud reuse refuses.

Pre-U-1, ``ReparkConnectionHandle.close()`` called ``session.stop()``. On
``catalog_type: memory`` the Iceberg catalog lives *in* the session, and dbt closes a
connection between nodes — so every node after the first saw an empty catalog:
``dbt build`` on a two-node project failed, ``dbt run`` then ``dbt test`` failed, and
snapshots could not resolve ``ref()``.

These tests need no engine, so (like the canary) they never skip. The live reproduction
and the functional dbt gates are in ``test_session_lifetime_engine.py``.
"""

from __future__ import annotations

import inspect
import warnings
from typing import Any

import pytest
from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.repark.connections import ReparkConnectionManager, _session_key
from dbt.adapters.repark.credentials import ReparkCredentials
from dbt.adapters.repark.handle import ReparkConnectionHandle


class _FakeSession:
    """Records whether anyone stopped it."""

    def __init__(self) -> None:
        self.stopped = 0

    def stop(self) -> None:
        self.stopped += 1

    def sql(self, text: str) -> Any:
        raise AssertionError(f"unexpected sql on fake session: {text}")


def _credentials(warehouse: str, *, schema: str = "default") -> ReparkCredentials:
    return ReparkCredentials.from_dict(
        {
            "catalog_type": "memory",
            "catalog_name": "spark_catalog",
            "warehouse": warehouse,
            "schema": schema,
            "database": "spark_catalog",
            "threads": 1,
        }
    )


# ---------------------------------------------------------------------------
# The P0 mechanism, pinned at the smallest possible scope
# ---------------------------------------------------------------------------


def test_u1_handle_close_does_not_stop_the_session() -> None:
    session = _FakeSession()
    handle = ReparkConnectionHandle(session)
    handle.close()
    assert session.stopped == 0, "handle close must never stop the engine session"
    handle.close()  # idempotent
    assert session.stopped == 0
    with pytest.raises(RuntimeError, match="closed"):
        handle.cursor()


def test_u1_handle_close_source_has_no_stop_call() -> None:
    """Mutation-proof: re-introducing ``session.stop()`` in close() fails here."""
    src = inspect.getsource(ReparkConnectionHandle.close)
    assert "stop()" not in src, src
    assert "close_all" in src, "close() must point operators at the real teardown hook"


def test_u1_cleanup_all_is_overridden_and_does_not_stop_sessions() -> None:
    """dbt calls cleanup_all once per task invocation, not per process — it must not stop."""
    src = inspect.getsource(ReparkConnectionManager.cleanup_all)
    assert "super().cleanup_all()" in src
    assert "close_all" in src
    assert ".stop()" not in src


def test_u1_atexit_teardown_is_registered() -> None:
    """Session teardown is the process boundary (dbt-duckdb precedent)."""
    from dbt.adapters.repark import connections as conn_mod

    src = inspect.getsource(conn_mod)
    assert "atexit.register(ReparkConnectionManager.close_all)" in src


def test_u1_close_all_stops_every_cached_session() -> None:
    registry = ReparkConnectionManager.live_sessions()
    a, b = _FakeSession(), _FakeSession()
    registry[("memory", "spark_catalog", "/wh/a", "", "")] = a
    registry[("memory", "spark_catalog", "/wh/b", "", "")] = b

    ReparkConnectionManager.close_all()

    assert a.stopped == 1
    assert b.stopped == 1
    assert ReparkConnectionManager.live_sessions() == {}
    ReparkConnectionManager.close_all()  # idempotent
    assert a.stopped == 1


# ---------------------------------------------------------------------------
# Session key identity
# ---------------------------------------------------------------------------


def test_u1_session_key_ignores_dbt_schema() -> None:
    """Two connections differing only by dbt schema share one catalog session."""
    assert _session_key(_credentials("/tmp/u1-key", schema="default")) == _session_key(
        _credentials("/tmp/u1-key", schema="other")
    )


def test_u1_session_key_separates_warehouses() -> None:
    assert _session_key(_credentials("/tmp/u1-a")) != _session_key(_credentials("/tmp/u1-b"))


def test_u1_session_key_carries_no_secret_material() -> None:
    key = _session_key(_credentials("/tmp/u1-key"))
    assert key == ("memory", "spark_catalog", "/tmp/u1-key", "", "")


def test_u1_second_target_in_one_process_refuses_naming_both() -> None:
    """G1.4 at the registry level, with no engine involved."""
    registry = ReparkConnectionManager.live_sessions()
    registry[("memory", "spark_catalog", "/wh/already-open", "", "")] = _FakeSession()

    with pytest.raises(FailedToConnectError) as ei:
        ReparkConnectionManager.get_or_create_session(_credentials("/wh/requested"))
    msg = str(ei.value)
    assert "/wh/already-open" in msg, msg
    assert "/wh/requested" in msg, msg
    assert "close_all" in msg


def test_u1_same_key_returns_the_cached_session_without_building() -> None:
    registry = ReparkConnectionManager.live_sessions()
    session = _FakeSession()
    key = _session_key(_credentials("/wh/cached"))
    registry[key] = session

    def _explode(cls: type, credentials: ReparkCredentials) -> Any:
        raise AssertionError("cache miss: a cached credentials key must not rebuild a session")

    original = ReparkConnectionManager._open_session
    ReparkConnectionManager._open_session = classmethod(_explode)  # type: ignore[method-assign]
    try:
        assert ReparkConnectionManager.get_or_create_session(_credentials("/wh/cached")) is session
    finally:
        ReparkConnectionManager._open_session = original  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# §5.4 — the getOrCreate reuse warning the adapter used to ignore
# ---------------------------------------------------------------------------

_ENGINE_REUSE_WARNING = (
    "Using an existing ReparkSession; some configuration may not apply "
    "(engine knobs are fixed at session build; already-registered catalogs keep their "
    "configuration: ['spark_catalog']; unapplied keys: "
    "['spark.sql.catalog.spark_catalog.warehouse'])."
)


def test_u1_getorcreate_reuse_warning_refuses_loud() -> None:
    class _ReusingBuilder:
        def getOrCreate(self) -> Any:  # noqa: N802 — engine (PySpark) spelling
            warnings.warn(_ENGINE_REUSE_WARNING, UserWarning, stacklevel=2)
            return _FakeSession()

    with pytest.raises(FailedToConnectError) as ei:
        ReparkConnectionManager._get_or_create_engine_session(
            _ReusingBuilder(), _credentials("/tmp/u1-wanted")
        )
    msg = str(ei.value)
    assert "refuses to reuse a ReparkSession it does not own" in msg
    assert "/tmp/u1-wanted" in msg, msg
    assert "unapplied keys" in msg, msg


def test_u1_unrelated_engine_warnings_are_re_emitted_not_swallowed() -> None:
    class _ChattyBuilder:
        def getOrCreate(self) -> Any:  # noqa: N802 — engine (PySpark) spelling
            warnings.warn("some other engine note", UserWarning, stacklevel=2)
            return _FakeSession()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ReparkConnectionManager._get_or_create_engine_session(
            _ChattyBuilder(), _credentials("/tmp/u1-chatty")
        )
    assert any("some other engine note" in str(w.message) for w in caught), caught


def test_u1_quiet_getorcreate_is_accepted() -> None:
    class _QuietBuilder:
        def getOrCreate(self) -> Any:  # noqa: N802 — engine (PySpark) spelling
            return _FakeSession()

    session = ReparkConnectionManager._get_or_create_engine_session(
        _QuietBuilder(), _credentials("/tmp/u1-quiet")
    )
    assert isinstance(session, _FakeSession)


def test_u1_catalog_binding_mismatch_refuses_naming_both_values() -> None:
    class _Conf:
        def get(self, key: str) -> str:
            return "/warehouse/that/is/live"

    class _BoundSession(_FakeSession):
        conf = _Conf()

    with pytest.raises(FailedToConnectError) as ei:
        ReparkConnectionManager._verify_catalog_binding(
            _BoundSession(),
            catalog="spark_catalog",
            suffix="warehouse",
            expected="/warehouse/that/profiles/asked/for",
        )
    msg = str(ei.value)
    assert "/warehouse/that/is/live" in msg
    assert "/warehouse/that/profiles/asked/for" in msg


def test_u1_catalog_binding_match_is_silent() -> None:
    class _Conf:
        def get(self, key: str) -> str:
            return "/warehouse/agreed"

    class _BoundSession(_FakeSession):
        conf = _Conf()

    ReparkConnectionManager._verify_catalog_binding(
        _BoundSession(), catalog="spark_catalog", suffix="warehouse", expected="/warehouse/agreed"
    )
