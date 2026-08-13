# Unit ledger — S-4 dbt-repark engine-DML upgrade

**Unit:** S-4 (`BRIEF-s4-dbt-upgrade.md` + conductor-8 A7/A8)  
**Date:** 2026-08-14 · **Lane:** S-4 · **Branch:** `grok/s4-dbt-dml-upgrade`  
**dbt base freeze:** `b6470f63ca3e494d9012a8023ecfefae404b67d4` (dbt#4 M2a)

## 1. Engine wheel provenance (A7)

| Item | Value |
|---|---|
| Freeze SHA | `d9a739123be8b00bc1fc1e6d4bbad875ba6caa76` |
| Wheel source tree | `/tmp/grok-s4-repark-wheel` (detached; no branch / no commits / no PR) |
| Build | `make build-wheel` in that tree |
| Wheel path | `/tmp/grok-s4-repark-wheel/target/wheels/repark-0.0.0-cp312-abi3-manylinux_2_39_x86_64.whl` |
| Wheel sha256 | `a46c51d8067b4ad1720ec1fcda5b8c495c8c1b8850783d87e35faf3f8586efb8` |
| Install | absolute path into `/tmp/grok-s4/.venv` |
| Import path (canary) | `/tmp/grok-s4/.venv/lib/python3.12/site-packages/repark/__init__.py` |
| Adapter import spelling | `from repark import ReparkSession` (not `repark.spark`) |

Never built from `/tmp/grok-s1`…`s5` or the orchestrator clone.

## 2. A8 spelling table

| unique_key | Delete SQL | Engine hole | Test |
|---|---|---|---|
| single-column, no extras | `DELETE FROM target WHERE key IN (SELECT key FROM staging)` | uncorrelated IN (#78) | `test_s4_engine_delete_in_single_key_memory` + `test_m1_1_delete_insert_dbt_runner_twice_arrow` |
| composite | `DELETE FROM target AS t WHERE EXISTS (SELECT 1 FROM staging AS s WHERE s.k = t.k AND …)` | correlated EXISTS (#89) | `test_s4_engine_delete_exists_composite_memory` + `test_m1_1_delete_insert_multi_key_dbt_runner` |
| single-column + `incremental_predicates` | same EXISTS inner-WHERE (mixed outer AND/OR still valved) | EXISTS (#89) | source pin in `repark_get_incremental_delete_sql` |
| tuple IN | **not emitted** | not a proven hole | `test_s4_delete_sql_spelling_source_pins` |

MERGE is **not** the delete+insert vehicle. `repark_get_incremental_merge_sql` is untouched
(M2a upsert). Pin: `test_s4_merge_strategy_macro_untouched`.

## 3. Residual refusals (loud; engine needle)

Needle: `subquery predicates are silently mis-executed` (G3-E8). Never swallowed.

| Spelling | Test |
|---|---|
| `UPDATE … WHERE col IN (SELECT …)` | `test_s4_engine_refuses_update_in_loudly` |
| correlated `DELETE … IN (SELECT … WHERE outer.col)` | `test_s4_engine_refuses_correlated_in_loudly` |
| `ANY` / `ALL` subquery | `test_s4_engine_refuses_any_all_loudly` |

Rows are unchanged after the refuse (valve, not execute-and-lie).

## 4. Hollow-venv canary (dbt#4 lesson)

Committed tests (`tests/test_engine_canary.py`) **do not** `importorskip("repark")`. Missing
engine → `ImportError` → red suite.

| Probe | Result |
|---|---|
| S-4 venv import path | `/tmp/grok-s4/.venv/lib/python3.12/site-packages/repark/__init__.py` |
| S-4 venv `from repark import ReparkSession` | ok (`repark.session.session_core.ReparkSession`) |
| Hollow venv `/tmp/grok-s4-hollow` (adapter + `[dev]`, **no** wheel) | `from repark import ReparkSession` → `ModuleNotFoundError` (exit 1) |
| Hollow `pytest tests/test_engine_canary.py` | **2 failed** (`ModuleNotFoundError`) — not skipped |
| Hollow `pytest tests/test_engine_memory.py` | **1 skipped** (`importorskip`) |

## 5. Suite count

**71 passed**, exit 0 — `/tmp/grok-s4/.venv/bin/python -m pytest -q` against the pinned wheel.

## 6. Out of scope (honored)

- AWS / Glue / S3 Tables (M\*b daytime)
- `threads > 1`
- repark repo edits / branch / PR
- adapter import move to `repark.spark`
- publish / PyPI / publish tooling
- JVM lock (never taken)

## 7. Decisions

**D-S4-1 — Honest DELETE, two spellings.** Single-key IN; composite EXISTS. No tuple IN.

**D-S4-2 — Predicates inside EXISTS.** Mixed AND/OR around IN/EXISTS is still valved. Extras
cannot ride on the outer WHERE.

**D-S4-3 — No DISTINCT in IN subquery.** Engine allow-list refuses `SELECT DISTINCT` inside
the IN hole. DELETE is idempotent on the key set.

**D-S4-4 — Merge strategy byte-stable.** M2a upsert macro not edited.
