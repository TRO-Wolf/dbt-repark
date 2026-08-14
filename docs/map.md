# map — docs/

## Purpose

In-repo ledgers for closed dbt-repark units that landed under `docs/` (pre-S-4 convention).

## Contents

- [m2a-merge-ledger.md](m2a-merge-ledger.md) — G3-M2a merge incremental strategy (historical).
  `delete+insert` in that ledger still describes the pre-S-4 MERGE-as-delete vehicle; current
  truth is [../task/s4-dbt-dml-ledger.md](../task/s4-dbt-dml-ledger.md) + README.
