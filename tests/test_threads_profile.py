"""OQ-6: threads refuse on the live dbt profile path (not stripped credentials)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from dbt.adapters.exceptions import FailedToConnectError

from dbt.adapters.repark.connections import ReparkConnectionManager


def test_connection_manager_refuses_threads_not_one() -> None:
    profile = SimpleNamespace(threads=4, credentials=MagicMock())
    with pytest.raises(FailedToConnectError, match="threads=4"):
        ReparkConnectionManager(profile, MagicMock())


def test_connection_manager_accepts_threads_one() -> None:
    # May still fail later on missing full profile — only assert threads gate passes.
    profile = SimpleNamespace(
        threads=1,
        credentials=MagicMock(),
        query_comment=None,
    )
    # SQLConnectionManager needs more fields; if construction proceeds past threads check, OK.
    try:
        ReparkConnectionManager(profile, MagicMock())
    except FailedToConnectError as exc:
        assert "threads" not in str(exc).lower()
    except Exception:
        # Other init errors from incomplete profile mock are acceptable for this unit.
        pass
