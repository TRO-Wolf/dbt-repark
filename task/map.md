# map — task/

## Purpose

In-flight / current-unit ledgers for dbt-repark (S-4 convention). Historical M2a ledger remains
under [../docs/map.md](../docs/map.md).

## Contents

- [s4-dbt-dml-ledger.md](s4-dbt-dml-ledger.md) — S-4 honest engine DELETE for `delete+insert`
  (A7 wheel pin + A8 spelling table + hollow-venv canary + suite count).
- [u1-session-lifetime-ledger.md](u1-session-lifetime-ledger.md) — U-1 P0: one process
  `ReparkSession` per credentials key; handle close no longer stops it; `atexit` teardown;
  one-target-per-process refuse. Carries the U-2 re-pin rider.
