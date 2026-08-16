"""Suite-wide fixtures.

U-1 makes the engine session *process*-scoped: it is no longer stopped when a dbt
connection closes. Tests each build their own temp warehouse, so the process session must
be torn down between them or the second test would (correctly) hit the one-target-per-
process refuse. This fixture is the test-harness equivalent of the ``atexit`` hook.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from dbt.adapters.repark.connections import ReparkConnectionManager


@pytest.fixture(autouse=True)
def _fresh_repark_session() -> Iterator[None]:
    ReparkConnectionManager.close_all()
    try:
        yield
    finally:
        ReparkConnectionManager.close_all()
