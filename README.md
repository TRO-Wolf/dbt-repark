# dbt-repark

dbt adapter for the [repark](https://github.com/TRO-Wolf/repark) pure-Rust Iceberg engine
(**embedded**, no JVM). Adapter type in `profiles.yml`: **`repark`**.

**Status:** M0a skeleton — connect + table materialization + memory unit tests. Not a public release.

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

### Transactions

Each `execute` is **one** engine `sql()` with **eager** commit. `begin`/`commit`/`rollback` are
**documented no-ops** — multi-step materializations are **not** atomic. Do not advertise
“transactional dbt-repark.”

## Materializations (M0)

| Mat | Behavior |
|---|---|
| **table** | `CREATE OR REPLACE TABLE … USING iceberg AS …` (default **recommendation**) |
| **view** | **Loud refuse** — no durable Iceberg VIEW (G3-E2) |
| **ephemeral** | Stock dbt CTE path |

**Required project config** (dbt core `NodeConfig` still hard-defaults models to `view`;
this adapter cannot change that core default, so projects **must** set):

```yaml
# dbt_project.yml
models:
  +materialized: table
```

Without that, models resolve to `view` and **fail loud** (refuse-now). See `sample_project/`.

## Threat model (year one)

Single-user embedded engine in the dbt process; ambient AWS credentials apply to configured
catalogs; dbt project SQL/hooks/packages are trusted callers. Multi-tenant untrusted SQL is out of scope.

## License

Apache-2.0 (matches repark).
