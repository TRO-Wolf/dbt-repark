# map — dbt-repark

## Purpose

dbt adapter for the [repark](https://github.com/TRO-Wolf/repark) embedded Iceberg engine
(adapter type `repark`). Not a public release.

## Contents

- `src/dbt/adapters/repark/` — Python adapter (connections, credentials, handle, impl).
- `src/dbt/include/repark/` — macros: adapters + table/view/incremental materializations.
- `tests/` — memory-catalog unit suite (the S-4 gate).
- `sample_project/` — operator sample (`+materialized: table`).
- `docs/` — historical unit ledgers (M2a). See [docs/map.md](docs/map.md).
- `task/` — in-flight unit ledgers. See [task/map.md](task/map.md).
- `README.md` — operator contract (M2a+DML status; engine wheel pin).
- `pyproject.toml` — package metadata. `repark` is **not** a PyPI dependency.

## Session lifetime (U-1)

One `ReparkSession` per credentials key, per process, cached in a registry on
`ReparkConnectionManager`. Handle close **releases**; `close_all()` on `atexit` stops. A
second target in one process refuses loud (the engine session is a process singleton whose
catalog registration is fixed at build time). Memory catalogs stay *process*-ephemeral.

## Incremental DML (S-4)

`delete+insert` emits honest engine `DELETE` (`IN` / `EXISTS`). The M2a `merge` strategy is a
separate `MERGE INTO` upsert. Residual refusals (UPDATE IN, correlated IN, ANY/ALL) stay
engine-loud.
