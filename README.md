# dbt-repark

dbt adapter for the [repark](https://github.com/TRO-Wolf/repark) pure-Rust Iceberg engine
(**embedded**, no JVM). Adapter type in `profiles.yml`: **`repark`**.

**Status:** M1b — connect + table + incremental (`append`, `delete+insert`,
`insert_overwrite`) + memory unit tests. Not a public release. No AWS gate in this unit.

## Install (pre-PyPI)

```bash
# 1) Build/install repark from a known git rev (path/editable). NEVER: pip install repark
#    from PyPI while 0.0.1 is only a name-holding placeholder.
cd /path/to/repark
uv sync && make develop   # or: maturin develop / make build-wheel

# 2) Install this adapter editable into the same venv (or a dedicated one that can import repark)
cd /path/to/dbt-repark
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
uv pip install -e /path/to/repark/python/repark   # if not already on PYTHONPATH via make develop
```

Record the repark git SHA used for each test run (start + PR time).

## profiles.yml (type: repark)

```yaml
repark_mem:
  target: dev
  outputs:
    dev:
      type: repark
      catalog_type: memory          # memory | glue | s3tables
      catalog_name: spark_catalog
      warehouse: /tmp/dbt-repark-wh # required for glue; optional for memory (temp dir)
      schema: default
      threads: 1                    # only 1 until G3-E6
      # aws_profile_name: my-profile  # optional named shared profile (ambient SDK)
```

### Credentials (non-negotiable)

- **Ambient AWS SDK default chain only** for Glue / S3 Tables (env → shared profile → instance/task role).
- **Loud refuse** any static key/secret/session-token fields in `profiles.yml` (and nested `aws:` blocks).
- Do **not** equate engine tier-2 CI OIDC with operator profiles.
- Manual/AWS acceptance (M0b): scratch/non-prod namespace + warehouse; verify LocationUri binding.

### Transactions (§1.5 honesty)

Each `execute` is **one** engine `sql()` with **eager** commit. `begin`/`commit`/`rollback` are
**documented no-ops** — multi-step materializations are **not** atomic. Do not advertise
“transactional dbt-repark.”

**`delete+insert` residual:** that strategy is **two** executes (delete matching keys, then
insert the batch). If the process fails **after** delete and **before** insert, the residual
state is real and **cannot be rolled back**: rows whose `unique_key` appeared in the batch are
already gone; nothing from that batch was inserted. The durable `__dbt_tmp` staging table from
that run is also left behind (materialization cleanup only runs after a successful path). Unit
tests pin this (M1.3); ops must re-run or repair manually. Prefer single-statement strategies
(future `merge` in G3-M2) when atomicity of the upsert matters.

**`insert_overwrite` residual / composition:** the engine Spark-door `INSERT OVERWRITE` is
**static whole-table replace** (`overwrite_by_row_filter(AlwaysTrue)`). Hive-style
`INSERT OVERWRITE … PARTITION (…)` and `partitionOverwriteMode=DYNAMIC` are **not** implemented
engine-side. This adapter still offers **dbt-spark DYNAMIC semantics** for partitioned models by
composing a single `INSERT OVERWRITE` whose source is:

`(batch rows) ∪ (existing target rows whose partition keys are absent from the batch)`.

That is one eager execute on the incremental path (atomic for the strategy itself). It is **not**
engine-native dynamic partition overwrite — kept partitions are re-materialized through the
overwrite source (cost scales with kept data, not only the batch). Staging is still a durable
`__dbt_tmp` Iceberg table (engine has no `TEMP TABLE` / `TEMP VIEW`); successful runs drop it.
**Non-partitioned models refuse loud** (overwrite-all footgun) — use `delete+insert` or
full-refresh instead; the adapter will not silently whole-table overwrite.

## Materializations (M0–M1b)

| Mat | Behavior |
|---|---|
| **table** | `CREATE OR REPLACE TABLE … USING iceberg AS …` (default **recommendation**); optional `partition_by` → `PARTITIONED BY` |
| **incremental** | Strategies: **`append`**, **`delete+insert`**, **`insert_overwrite`** (see below) |
| **view** | **Loud refuse** — no durable Iceberg VIEW (G3-E2) |
| **ephemeral** | Stock dbt CTE path |

### Incremental strategies (M1b)

| Strategy | Behavior |
|---|---|
| **`append`** (default) | Staging CTAS + `INSERT INTO … SELECT` (one insert execute on incremental runs) |
| **`delete+insert`** | Staging CTAS + **delete** matching `unique_key` rows + **insert** batch — **two** eager executes; not atomic (see residual above). Requires `unique_key`. |
| **`insert_overwrite`** | Staging CTAS + one Spark-door `INSERT OVERWRITE` composed for **dynamic partition overwrite** (partitions present in the batch replaced; others left alone). Requires `partition_by` (identity column name or list). **Loud refuse** without `partition_by` (no overwrite-whole-table). See composition residual above. |
| **`merge`** | **Loud refuse** — scheduled **G3-M2** |
| **other / microbatch** | **Loud refuse** — not supported |

`delete+insert` delete half uses Spark-door `MERGE … WHEN MATCHED THEN DELETE` as a
**delete vehicle only** (engine `DELETE … WHERE key IN (SELECT …)` is incorrect today). That is
**not** the G3-M2 merge incremental strategy (no combined upsert in one statement).

Staging relations are **durable** Iceberg tables (`__dbt_tmp` suffix); the engine has no
`TEMP TABLE` / `TEMP VIEW`. Materializations call `create_table_as(False, …)` for staging and
drop temps after a successful run. **`create_table_as(temporary=True)` is refused loud** (M0
pin restored) — there is no session-scoped temp table path. Optional `incremental_predicates`
/`predicates` are plumbed into the delete+insert ON clause but not e2e-gated in M1a (G3-M2).

**`partition_by`:** identity partition column name or list (e.g. `partition_by='ds'` or
`partition_by=['region', 'ds']`). Applied on CTAS via Spark-door `PARTITIONED BY (…)` for
first-run and full-refresh. Transform partitions (`days(ts)`, `bucket(n, col)`, …) are **not**
accepted as `partition_by` config values in M1b — use identity columns in the model select list.

**Required project config** (dbt core `NodeConfig` still hard-defaults models to `view`;
this adapter cannot change that core default, so projects **must** set a non-view default):

```yaml
# dbt_project.yml
models:
  +materialized: table
```

Without that, models resolve to `view` and **fail loud** (refuse-now). See `sample_project/`.
Incremental models set `+materialized: incremental` (and strategy) on the model or folder.

## Threat model (year one)

Single-user embedded engine in the dbt process; ambient AWS credentials apply to configured
catalogs; dbt project SQL/hooks/packages are trusted callers. Multi-tenant untrusted SQL is out of scope.

## License

Apache-2.0 (matches repark).
