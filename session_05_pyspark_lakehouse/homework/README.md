# Session 5 Homework — Time-Travel Reproducibility

A short take-home on the lakehouse stack you used in the lab, on the property that
separates a Delta table from session 4's single-file warehouse: **every version is kept,
and any earlier state is reproducible**. One required task, one optional bonus.

You run it on the session 5 stack (this folder's `docker-compose.yml`). If you add code,
put it under `homework/submissions/` — that folder is mounted into Airflow at
`/opt/airflow/dags/session_05_homework` and into Spark at `/opt/jobs/homework`.

> Delta time travel (`VERSION AS OF`) runs through **Spark** — this Trino build's Delta
> connector does not expose versioned tables. Use Trino for current-state queries and a
> small `spark-sql` helper for time travel (jars + Delta/S3A config come from
> spark-master's `spark-defaults.conf`, so no `--jars`/`--conf` flags are needed):
>
> ```bash
> ttq(){ docker exec spark-master /opt/spark/bin/spark-sql -e "$1"; }
> ```

---

## Required — reproduce the past after a correction

Run the reference pipeline for `2026-06-26` first, so `fact_sales` is loaded. Then:

1. **Record the current state** and the current Delta version:
   ```bash
   docker compose exec -T trino trino --server localhost:8080 --execute \
     "SELECT COUNT(*) FROM delta.lakehouse.fact_sales WHERE order_date = DATE '2026-06-26'"
   ttq "SELECT MAX(version) FROM (SELECT version FROM json.\`s3a://warehouse/fact_sales/_delta_log\`)" 2>/dev/null || \
     docker compose exec -T trino trino --server localhost:8080 --execute \
       "SELECT MAX(version) FROM delta.lakehouse.\"fact_sales\$history\""
   ```
2. **Simulate a late-arriving correction** — regenerate a *larger* file for the **same
   date** (same `ds` ⇒ the first 1000 rows are identical, plus more), re-upload, and rerun
   the pipeline:
   ```bash
   python3 scripts/generate_data.py --ds 2026-06-26 --rows 1200 --output-dir data/raw
   # re-upload to MinIO (see ../README.md), then:
   docker compose exec airflow-scheduler airflow tasks clear session_05_lakehouse_daily_pipeline -s 2026-06-26 -e 2026-06-26 -y
   ```
3. **Show both states coexist** — the fact now reflects the correction, *and* the original
   load is still reproducible with time travel:
   ```bash
   # current (corrected) — a higher count than before
   docker compose exec -T trino trino --server localhost:8080 --execute \
     "SELECT COUNT(*) FROM delta.lakehouse.fact_sales WHERE order_date = DATE '2026-06-26'"
   # the very first load, reproduced exactly (version 0 was the empty table; use the
   # version you recorded in step 1 for the first real load)
   ttq "SELECT COUNT(*) FROM lakehouse.fact_sales VERSION AS OF 1 WHERE order_date = DATE '2026-06-26'"
   ```

Include both counts and the version numbers in your PR description.

### Short answer (include in your PR)

In 2–3 sentences: session 4's DuckDB warehouse was a single file with one writer and no
history. **What do Delta's table format and time travel give you here that it couldn't** —
think about reproducing a prior state, and many engines (Spark *and* Trino) reading the
same table at once.

---

## Optional bonus — a daily revenue rollup

Build a **Gold-on-Gold** aggregate `lakehouse.fact_sales_daily`: one row per
`(customer_id, order_date)` with `order_count` and `total_amount`, rolled up from
`lakehouse.fact_sales` (not the raw CSV). Write it as a `SparkSubmitOperator` job that
loads the date **idempotently** with `replaceWhere`, and reconcile through Trino that
`SUM(total_amount)` ties back to `SUM(amount)` in `fact_sales` (within 1%) and
`SUM(order_count)` equals `COUNT(*)`. Same conventions as the lab: credentials from env
vars, `on_failure_callback=notify_on_failure`, no hardcoded keys.

---

## Submit — open a Pull Request

1. Create a branch: `git checkout -b homework/session-05/<your-name>`.
2. Add any code under `session_05_pyspark_lakehouse/homework/submissions/`.
3. Commit and push, then **open a Pull Request** against `main`.
4. In the PR description include: the two counts + version numbers (required), your short
   answer, and — if you did the bonus — the `fact_sales_daily` reconcile result.

The PR is for review only — it will not be merged, so don't worry about conflicts.

## Acceptance criteria

- [ ] Required: both counts shown (current corrected vs the first load via `VERSION AS
  OF`), proving the earlier state is still reproducible after a rerun; short answer
  included.
- [ ] Bonus (if attempted): `fact_sales_daily` loaded with `replaceWhere`, rerunning a
  date does not change the count, and the reconcile ties back to `fact_sales`.
