# Session 5 Lab — Setup & Exercises

**Master Class DataOps for Modern Data Platforms · Session 5/11**

You complete the daily pipeline the reference demo runs: an **idempotent** Delta fact
load, a **MERGE** dimension upsert, and a **three-layer reconcile** — each a small,
focused piece, wired into one Airflow DAG.

The stack lives at the **session root**, not here — bring it up first following
[../README.md](../README.md#bring-up-the-stack). This guide assumes it is running.

## 1. Prerequisites

- The session 5 stack is up (`docker compose ps` all healthy) and the four jars are in
  `jars/` — see [../README.md](../README.md#bring-up-the-stack).
- Airflow UI reachable at `http://localhost:8888` (`airflow` / `airflow`).
- Sample data generated + uploaded and `init_schema.py` run once — see
  [../README.md](../README.md#load-sample-data-and-create-the-tables). Use date
  **2026-06-26** for this lab.

## 2. What you complete

The DAG `lab/dags/session_05_lakehouse_daily_starter.py` already wires four tasks:
`validate_raw_file → transform_fact_sales → upsert_dim_customer → reconcile`. You fill in
three jobs/tasks plus one DAG line. The reference `demo/` versions are the same shape —
read them if you get stuck, but write your own first.

| # | File | What to do |
|---|---|---|
| Lab 1 | `lab/jobs/transform_fact_sales.py` | Build `df_fact` and load it idempotently with `replaceWhere`. |
| Lab 2 | `lab/jobs/upsert_dim_customer.py` | `MERGE` the deterministic dimension into `dim_customer`. |
| Lab 3 | `reconcile` task in the DAG | Three reconcile layers via Trino (count, SUM, business rule). |
| DAG | the DAG's `default_args` | Add `on_failure_callback=notify_on_failure`. |

Edits under `lab/` are picked up automatically — the scheduler rescans DAGs every ~30s,
and each Spark job is re-read on every run. No rebuild or restart.

## 3. Lab 1 — the idempotent fact load

`lab/jobs/transform_fact_sales.py`. The Spark session and the CSV extract are given.

- **TODO 1 — `df_fact`:** keep `order_status IN ('completed','cancelled')` and
  `amount > 0`; cast `amount` to `DecimalType(18, 2)`; add `order_date =
  to_date(created_at)` and `processed_at = lit(ds).cast("date")`; select the eight output
  columns. Use `lit(ds)`, **never** `current_timestamp()` — `processed_at` must be
  reproducible on a rerun.
- **TODO 2 — idempotent load:** write `df_fact` as Delta with
  `mode("overwrite").option("replaceWhere", f"order_date = '{ds}'").save("s3a://warehouse/fact_sales")`.
  `init_schema.py` already registered the table, so writing to its path keeps the logs
  clean (no per-run Hive alter).

## 4. Lab 2 — the MERGE dimension

`lab/jobs/upsert_dim_customer.py`. `df_dim` (distinct customers, deterministic
`segment = hash % 3`, `updated_at = lit(ds)`) is given.

- **TODO:** register `df_dim` as a temp view and `MERGE INTO lakehouse.dim_customer` on
  `customer_id` — `WHEN MATCHED THEN UPDATE SET *`, `WHEN NOT MATCHED THEN INSERT *`. A
  rerun of the same date must not create duplicate customers.

## 5. Lab 3 — the three-layer reconcile

The `reconcile` task in the DAG. Two things are given: the expected values from the raw
file (`staging_count`, `staging_sum`, filtered the same way the fact job filters), and
the query `lab/dags/sql/reconcile.sql`, which the task runs to get `(fact_count,
fact_sum, negatives)` in one scan. You write the three checks, each raising on failure:

1. **Row count** — `reconcile_counts(staging_count, fact_count, label)` (reused from
   `dataops_common.storage`).
2. **Aggregate** — the fact `SUM(amount)` within 1% of `staging_sum`.
3. **Business rule** — `negatives` must be 0.

Then, in the DAG's `default_args`, add `on_failure_callback=notify_on_failure` so a
failed run alerts via the shared `dataops_common` helper (logs locally if no Slack
webhook is set).

## 6. Run and check it

Trigger the lab DAG for `2026-06-26` (all `docker compose` commands run from the session
root):

```bash
docker compose exec airflow-scheduler airflow dags trigger session_05_lakehouse_daily_starter -e 2026-06-26
```

Watch the four tasks in the UI, then query the result from Trino (or DBeaver — see
[../README.md](../README.md#querying-from-trino-or-dbeaver-on-your-host)):

```bash
docker compose exec trino trino --server localhost:8080 --execute \
  "SELECT COUNT(*) AS rows, SUM(amount) AS revenue FROM delta.lakehouse.fact_sales WHERE order_date = DATE '2026-06-26'"
```

## 7. Acceptance criteria

- Before you finish, the Spark tasks fail (`NotImplementedError`) and `reconcile` fails —
  the "not done yet" signals.
- Once complete, the DAG runs green; `fact_sales` for `2026-06-26` has **664 rows** (of
  1000 raw — pending orders and non-positive amounts dropped), `dim_customer` has **199**
  customers, and `reconcile` passes.
- **Idempotency:** clear the run and let it rerun — `fact_sales` stays 664 and
  `dim_customer` stays 199, not doubled. (`airflow tasks clear
  session_05_lakehouse_daily_starter -s 2026-06-26 -e 2026-06-26 -y`.)
- If you swap `replaceWhere`/overwrite for `mode("append")` in Lab 1, a second run doubles
  the rows and `reconcile` fails — put it back.

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A Spark task fails, `NotImplementedError` | TODO not done | Complete the relevant `lab/jobs/*.py`. |
| `Py4JJavaError ... NoClassDefFoundError: DeltaLog` | Jars missing | Confirm all four jars in `jars/` (see [../README.md](../README.md#bring-up-the-stack)). |
| `reconcile` fails on row count | `replaceWhere` missing or wrong filter | Overwrite must target `order_date = '{ds}'` only. |
| DAG missing / import error in UI | Syntax error in a DAG edit | `airflow dags list-import-errors`. |
| Executors can't reach the driver | Client-mode networking | The DAG sets `spark.driver.host=airflow-worker`; keep the `airflow-worker` hostname in compose. |
