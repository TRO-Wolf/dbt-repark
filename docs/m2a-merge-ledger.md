# Unit ledger — G3-M2a merge incremental strategy (dbt-repark)

**Unit:** G3-M2a (`planning/grok/G3-LANE-PLAN.md` M2.1 / M2.2 / M2.4)  
**Date:** 2026-08-11 · **Lane:** X-6 overnight conductor #3 · **Branch:** `g3-m2a-merge`  
**Base freeze:** `e26e730` (A11) · **dbt#3 insert_overwrite:** READ-ONLY reference only (no stack)

## 1. What landed

| Artifact | Role |
|---|---|
| `repark_get_incremental_merge_sql` | Emits Spark-door `MERGE INTO` upsert (update + insert arms) |
| `materialization incremental` merge branch | One `statement('main')` execute after durable staging CTAS |
| `unique_key` required for merge | Loud refuse before staging (no stock `ON FALSE` insert-only footgun) |
| `valid_incremental_strategies` | `append`, `delete+insert`, `merge` |
| M2.1 pytest gates | update-only, insert-only, mixed, multi-key, dup-source-key, no-uk refuse, full-refresh |
| README strategy table + M2.2 residual table | Operator docs in same PR |
| This ledger | In-repo dbt-lane convention (`docs/`) |

## 2. Decisions

**D-M2a-1 — Require `unique_key` for merge.** Stock dbt may emit `ON FALSE` without a key
(insert-only). This adapter refuses loud with an actionable message so `merge` always means
keyed upsert. Mirrors `delete+insert` pre-staging validation.

**D-M2a-2 — Cite engine MERGE corpora; do not re-pin.** Semantics for cardinality, insert-only
dups, and basic upsert come from repark N-2 / N-2b (`test_merge_differential_parity`,
`tests::merge::*`). Adapter tests assert the surface to dbt users (Arrow + error token), not
re-derive engine goldens.

**D-M2a-3 — Explicit column UPDATE/INSERT lists.** Quoted dest columns via `adapter.quote` /
`get_quoted_csv` (consistent with append/delete paths). `UPDATE SET *` / `INSERT *` also work on
the door; explicit lists keep parity with stock dbt column control and future
`merge_update_columns` without relying on star expansion.

**D-M2a-4 — Optional Spark extras residual (M2.2).** `insert_overwrite` stays OQ-5 refuse;
`microbatch` refuse; `merge_update_columns` / `merge_exclude_columns` / matched-delete-only /
`NOT MATCHED BY SOURCE` not productized. Documented residual table in README.

**D-M2a-5 — Never claim dbt snapshot materialization.** M2 = MERGE upsert (+ optional public
time-travel SQL stretch, skipped). SCD snapshot is out of M0–M2.

**D-M2a-6 — Zero engine files.** Adapter repo only (M2.4).

## 3. Gates

| ID | Gate | Evidence |
|---|---|---|
| M2.1 | update-only / insert-only / mixed Arrow | `test_m2_1_merge_*_dbt_runner` |
| M2.1 | duplicate source key matched | `test_m2_1_merge_duplicate_source_key_matched_raises` → `MERGE_CARDINALITY_VIOLATION` |
| M2.1 | no `unique_key` refuse | `test_m2_1_merge_refuses_without_unique_key` |
| M2.2 | optional extras off / loud-gated | residual table + refuse pins (`insert_overwrite`, `microbatch`) |
| M2.4 | no repark engine files | PR diff scoped to dbt-repark |

## 4. Out of scope (honored)

- M2.3 AWS merge gate (G3-M2b, daytime)
- Engine repo changes
- dbt `snapshot` materialization
- Stacking on dbt#3 `insert_overwrite` (reference only)
- Time-travel stretch (skipped; not done-gate)

## 5. Engine pin (test runtime)

Record at PR time in the complete report / PR body (not re-pinned here).
