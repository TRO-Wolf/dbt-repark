# Unit ledger — U-1 session lifetime

**Unit:** U-1 (hardening recon `DBT-REPARK-RECON-2026-08.md` §3.3 / §6, P0)
**Date:** 2026-08-15 · **Lane:** hardening · **Branch:** `hardening/u1-session-lifetime`
**dbt base freeze:** `9455a01cf0edb3f4161fb9dbf39a9c789bf4e47d` (S-4, dbt#5)

## 1. The defect

`ReparkConnectionHandle.close()` called `self._session.stop()`. On `catalog_type: memory`
the Iceberg catalog **lives in the session**, and dbt closes a connection between nodes, so
every node after the first met a fresh session with an empty catalog.

| Command | Pre-U-1 |
|---|---|
| `dbt run` (1 model) | PASS |
| `dbt build` (model + tests, one process) | model PASS, **tests ERROR** `table … not found` |
| `dbt run` then `dbt test` | tests ERROR |
| `dbt snapshot` | ERROR — cannot resolve `ref()` |

Every committed test passed only because each held a single handle or ran a single-node dbt
invocation: **no test opened two connections in sequence.** G3-E1 says the memory catalog is
*process*-ephemeral; the adapter had made it ***connection*-ephemeral**.

## 2. Fix shape (adapter-only; zero engine files)

| Surface | Change |
|---|---|
| `handle.py` | `close()` releases the handle; **never** calls `session.stop()` |
| `connections.py` | module-level `_SESSIONS` registry + `_SESSION_LOCK`, keyed by profile identity |
| `connections.py` | `open()` → `get_or_create_session()`; namespace creation moved per connection |
| `connections.py` | `close_all()` teardown, registered on `atexit` |
| `connections.py` | `cleanup_all()` override — documents why dbt's per-invocation hook must *not* stop |
| `connections.py` | `_get_or_create_engine_session()` reads the `getOrCreate` reuse warning and refuses |
| `connections.py` | `_verify_catalog_binding()` compares the live session's catalog conf to the profile |

**Session key:** `(catalog_type, catalog_name, warehouse, table_bucket_arn, aws_profile_name)`.
`schema` is deliberately excluded — two connections differing only by dbt schema share one
catalog session and each ensures its own namespace. No secret material can enter the key
(the credentials denylist refuses it before `Credentials` is even built).

**Precedent honored.** dbt-duckdb keeps one `Environment` on the connection manager and tears
it down with `DuckDBConnectionManager.close_all_connections` on `atexit`; its handle close
never touches the database. `ReparkConnectionManager.close_all` is the same shape.

### D-U1-1 — teardown is `atexit`, not `cleanup_all`

The recon named `cleanup_all` / `close_all` as the teardown candidates. `cleanup_all` was
**rejected as the stop point**: dbt calls it from the `finally` of *every* task invocation
(`dbt/task/runnable.py`), not once per process, so stopping there would put the catalog back
on a per-invocation lifetime and re-break `dbt run` followed by `dbt test` in one process.
`cleanup_all` is overridden anyway, to carry that reasoning as a source-level pin
(`test_u1_cleanup_all_is_overridden_and_does_not_stop_sessions`).

### D-U1-2 — one target per process, loud

The embedded `ReparkSession` is a process-global singleton whose catalog registration is
fixed at build time. Two refuses now guard §5.4, which the adapter previously ignored:

1. **Registry mismatch** — a second credentials key while one session is live refuses,
   naming both configurations and pointing at `close_all()`.
2. **Foreign session** — `getOrCreate` emitting
   `Using an existing ReparkSession; some configuration may not apply … unapplied keys: […]`
   refuses, quoting the engine warning and what `profiles.yml` asked for. Unrelated engine
   warnings are re-emitted, never swallowed.
   `_verify_catalog_binding` is the belt-and-braces check on the live session's own conf.

This warning was firing **inside the committed suite** before this unit (see §3.2 of the
recon): the adapter was silently writing to a warehouse other than the one in the profile.

### D-U1-3 — memory stays process-ephemeral, and is asserted as such

U-1 does **not** make memory catalogs durable. `test_g1_2_memory_catalog_stays_process_ephemeral`
pins the boundary explicitly rather than papering over it.

## 3. Gates

| Gate | Test | Result |
|---|---|---|
| G1.1 `dbt build` ≥2 models + ≥2 data tests + 1 source ref, one process | `test_g1_1_dbt_build_multi_node_one_process` | **pass** |
| G1.2 `dbt run` then `dbt test`, two invocations | `test_g1_2_dbt_run_then_dbt_test_two_invocations_one_process` | **pass** (one process) |
| G1.2 process boundary still empty (G3-E1) | `test_g1_2_memory_catalog_stays_process_ephemeral` | **pass** |
| G1.3 connection A writes → close → connection B reads (recon §3.3) | `test_g1_3_connection_b_reads_what_connection_a_wrote` | **pass** |
| G1.4 warehouse mismatch refuses loud, naming both | `test_g1_4_second_warehouse_in_one_process_refuses_loud` | **pass** |
| G1.5 full suite green | see §5 | **pass**, modulo two pre-existing (§6) |

**Non-vacuity (executed).** With `close()` restored to `session.stop()` and the registry
write removed, **all 7 live gates in `test_session_lifetime_engine.py` fail** — including
G1.1 and G1.3. Gates were then re-verified green against the fix.

## 4. Test surfaces

| File | Contents |
|---|---|
| `tests/test_session_lifetime.py` | engine-free pins: close-does-not-stop, mutation-proof source pins, key identity, reuse/mismatch refuses. **No `importorskip`** — these never skip |
| `tests/test_session_lifetime_engine.py` | live gates G1.1–G1.4 + teardown/registry behaviour |
| `tests/conftest.py` | autouse `close_all()` around every test — the harness equivalent of the `atexit` hook, now that sessions are process-scoped |
| `tests/test_handle.py` | `test_handle_close_stops_session` **inverted** → `test_handle_close_releases_without_stopping_session`. That test pinned the P0 defect |
| `tests/test_incremental.py` | `_shared_memory_session` no longer monkeypatches `_open_session` / `ReparkConnectionHandle.close`; it is now a scope guard over the adapter's own registry. The workaround existed only because of this defect |

Suite: **71 → 93 tests** (+22).

## 5. Engine provenance (A7)

| Item | Value |
|---|---|
| Engine | `repark==0.2.0` **from PyPI** (real release; sdist/wheel, not the 0.0.1 name-holder) |
| Import path (canary) | `<venv>/lib/python3.12/site-packages/repark/__init__.py` |
| `ReparkSession.__module__` | `repark.spark.session.session_core` (#95 re-home; adapter spelling unaffected) |
| Adapter import spelling | `from repark import ReparkSession` |
| Install | `uv pip install "repark==0.2.0"` into a rebuilt `.venv` |

**Dev-loop outage repaired.** The pre-existing `.venv` installed repark **editable**
(`repark.pth` → a local worktree) against a `_native.abi3.so` dated 2026-08-10 — older than
`#78`/`#89`, the DELETE fixes S-4 depends on. It could not execute this adapter correctly and
had not been able to since S-4 landed. `test_engine_canary.py` caught it exactly as designed;
its `site-packages` assertion was **not** relaxed.

## 6. Suite results — honest attribution

| Run | Result |
|---|---|
| `origin/main`, old editable venv (recon §3.2) | 60 passed, **11 failed** |
| `origin/main`, rebuilt venv on repark 0.2.0 | 69 passed, **2 failed** |
| this branch, rebuilt venv on repark 0.2.0 | **91 passed, 2 failed** |

The **2 remaining failures are pre-existing and not caused by this unit** — identical on
`origin/main` with the same venv, and untouched by any file this branch changes:

```
FAILED tests/test_engine_dml.py::test_s4_engine_refuses_update_in_loudly
FAILED tests/test_engine_dml.py::test_s4_engine_refuses_correlated_in_loudly
```

Both assert that the engine still **refuses** `UPDATE … WHERE col IN (SELECT …)` and
correlated `DELETE … IN (SELECT … WHERE outer.col)` with the G3-E8 needle. repark **#98**
("identity UPDATE IN + correlated IN + proven ANY/ALL — family close") implemented them, so
they now execute correctly and the tests fail with `DID NOT RAISE`. This is precisely the
recon's **G2.5** prediction under U-2: *"#98 closed part of the UPDATE-IN/correlated-IN
family, so some of those tests may now be pinning the wrong thing. Re-derive from the current
allow-list; do not assume."* Re-deriving the S-4 residual table is **U-2's work**, not this
unit's, and it is good news — a narrower refuse surface. The two `ANY`/`ALL` residual tests
still pass, so the family is not wholly closed.

## 7. Rider for U-2 (not done here)

The engine used for this run is **repark 0.2.0 from PyPI**, but `README.md` § Install and
`task/s4-dbt-dml-ledger.md` still pin `d9a7391` and still carry the
*"NEVER `pip install repark` from PyPI while 0.0.1 is only a name-holding placeholder"*
warning. That warning's premise has expired: 0.2.0 is a real published release. **Neither
file was edited here** — the pin bump, the wheel-sha ledger row, the G2.5 re-derivation, and
the CI floor (M0.7) are U-2's scope and must land together. Flagging the contradiction so it
is not read as a silent re-pin.

## 8. Out of scope (honored)

- U-2 re-pin / dev-install docs / `.github/workflows` CI floor (M0.7)
- AWS / Glue / S3 Tables gates (M0b / M1b / M2b — owner daytime)
- `threads > 1` (G3-E6)
- seeds, `get_catalog`, column-type fidelity (U-3)
- `memory_limit_gb` profile key (U-4 §2 — lands cleanly on this registry)
- repark repo edits; engine files (zero touched)
