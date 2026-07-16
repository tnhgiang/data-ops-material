# Session 4 Homework — Deduplication & a Data-Quality Gate

Two take-home tasks on the same DuckDB warehouse you used in the lab. Both are
patterns interviewers reach for constantly: making an append-only source
idempotent by **deduplicating to the latest version**, and guarding a table with a
**data-quality suite** that fails the pipeline instead of shipping bad data.

You run them on the same Airflow stack from session 2. Put your DAGs (and their
`.sql` files) in `session_04_sql_dwh/homework/submissions/` — that folder is mounted
into Airflow at `/opt/airflow/dags/session_04_homework`, so the scheduler picks them
up within ~30s.

> To inspect your results, you don't need `docker exec` at all — the warehouse is a
> plain file on your **host** (`session_04_sql_dwh/db/dwh.duckdb`). Open it directly
> in DBeaver or any DuckDB client; see
> [../README.md](../README.md#querying-the-warehouse-from-dbeaver-or-any-duckdb-client-on-your-host).

Build your DAGs the same way as the lab: **operators + external `.sql` files**, not
raw Python connections. Reach the warehouse through the baked-in `duckdb_default`
connection, keep every DAG on the `duckdb_pool` pool (DuckDB is single-writer), and
put the SQL in a sibling `sql/` folder pointed to by `template_searchpath`:

```python
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator, SQLCheckOperator,
    SQLColumnCheckOperator, SQLTableCheckOperator,
)

default_args = {"conn_id": "duckdb_default", "pool": "duckdb_pool", ...}
# with DAG(..., template_searchpath="/opt/airflow/dags/session_04_homework/sql"):
#   SQLExecuteQueryOperator(task_id="...", sql="my_step.sql",
#                           split_statements=True, autocommit=True)
```

Wrap any DELETE+INSERT in `BEGIN … COMMIT` inside the `.sql` file for atomicity, and
template the run date with `{{ ds }}`.

---

## Homework 1 — Turn a raw change stream into a clean table (required)

This is the shape of real change-data-capture (CDC) ingestion: a source system emits
a row for every **change** — an insert, an update, or a delete — and those events
arrive duplicated (retries) and out of order (late). Each event carries an operation
flag `op` ∈ {`I`, `U`, `D`}. Your job is to collapse that noisy `bronze.events_raw`
into a clean `silver.events` holding **the current state**: exactly one row per
`event_id` — its newest version — and **no row at all for a key whose latest event is
a delete** (a "tombstone").

### The source (generate it deterministically in your DAG)

Seed `bronze.events_raw` per run date with this exact shape, so your run is
reproducible and the duplicates, late update, and delete are real:

```python
def raw_events_for(ds: str) -> list[tuple]:
    # (event_id, user_id, amount, op, updated_at)
    return [
        (1, 10, 100.0, "I", f"{ds} 09:00:00"),
        (2, 11, 200.0, "I", f"{ds} 09:05:00"),
        (1, 10, 100.0, "I", f"{ds} 09:10:00"),   # duplicate insert of event 1 (a retry)
        (2, 11, 250.0, "U", f"{ds} 11:00:00"),   # late UPDATE of event 2 (amount changed)
        (3, 12, 300.0, "I", f"{ds} 12:00:00"),
        (3, 12, 300.0, "D", f"{ds} 12:30:00"),   # event 3 later DELETED (tombstone)
    ]
```

Columns: `bronze.events_raw(event_id BIGINT, user_id INTEGER, amount DOUBLE, op VARCHAR, updated_at TIMESTAMP, event_date DATE)`.
Set `event_date = ds` when you load, and load it idempotently (replace the date's rows).

### Your task

Write a DAG `session_04_events_dedup` that, for each run date, loads the raw events, then builds
`silver.events` as the current state: latest row per `event_id`, tombstones removed.

| Principle | What your pipeline must do |
|---|---|
| **Determinism** | Derive everything from `ds`; never `now()` / `random()`. |
| **Deduplication** | Reduce to the latest event per key **first**, with a window function: `QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY updated_at DESC) = 1`. (Dedup happens before the write, not inside it.) |
| **Tombstones** | After picking the latest event, drop the ones where `op = 'D'` — a deleted key must not appear in `silver.events`. |
| **Idempotency** | `silver.events` load = DELETE the date + INSERT, inside one `.sql` file run by a `SQLExecuteQueryOperator`. Re-running a date must not change the result. |
| **Atomicity** | Wrap the DELETE+INSERT in `BEGIN … COMMIT` inside the `.sql` file. |
| **Reconciliation** | A `SQLCheckOperator` runs a `reconcile.sql` that returns TRUE only when `COUNT(*) in silver.events == number of keys whose latest event is not a delete` **and** `silver.events` has no duplicate `event_id`. |
| **Fault tolerance / alerting** | `retries`, `retry_delay`, and `on_failure_callback=notify_on_failure` in `default_args`. |

### Test it yourself

1. **Runs clean:** trigger `2024-03-01`. `silver.events` has **2 rows** (event_ids 1 and 2). Event 2's amount is **250.0** (the update won). Event 3 is **absent** (its latest event was a delete).
2. **Idempotent:** trigger `2024-03-01` again — still 2 rows, not 4.
3. **Reconcile catches dupes:** temporarily change your dedup to a plain `INSERT ... SELECT` (no `QUALIFY`, no tombstone filter), rerun, and confirm the reconcile task **fails** — `silver.events` now has duplicate `event_id`s and the deleted key. Then put the logic back.

Inspect results:

```bash
cd session_02_airflow_intro/lab
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True)
print(c.execute('SELECT event_id, amount FROM silver.events ORDER BY event_id').fetchall())
"
```

> Inspect with plain `duckdb` and the file path. `dataops_common` only imports
> inside an Airflow task, not from a bare `python -c`.

---

## Homework 2 — A data-quality gate on the fact table (required)

The reference `reconcile` task checks three things. Real pipelines wrap a **suite**
of independent checks around a table and refuse to publish if any fail — a
data-quality gate. Build a small version of that.

Run the reference pipeline (`session_04_orders_medallion_etl`) for a date first so
`gold.fact_sales` and `gold.dim_customer` exist. Then write a DAG `session_04_fact_sales_dq`
that runs **all** of these checks against `gold.fact_sales` for the date — using the
common-sql **check operators**, not a Python task that raises:

| Check | Rule | Operator |
|---|---|---|
| Primary key unique | no duplicate `order_id` | `SQLColumnCheckOperator` (`unique_check`) or `SQLTableCheckOperator` |
| Not null | `customer_id` is never NULL | `SQLColumnCheckOperator` (`null_check`) |
| Non-negative amounts | `amount >= 0` for all rows | `SQLColumnCheckOperator` (`min` ≥ 0) |
| Referential integrity | every `fact_sales.customer_id` exists in `gold.dim_customer` | `SQLCheckOperator` with a `.sql` returning a boolean |
| No future orders | `order_date <= ds` for all rows | `SQLCheckOperator` with a `.sql` returning a boolean |

`SQLColumnCheckOperator`/`SQLTableCheckOperator` cover the single-column rules
declaratively (`column_mapping` / `checks`); the two cross-row/cross-table rules go
in small `SQLCheckOperator` queries. Any failing check fails the DAG.

Wire `on_failure_callback=notify_on_failure` so a failure alerts, and keep the tasks
on `duckdb_pool`.

### Test it yourself

1. Against a clean date, every check task is green.
2. Break one rule on purpose and confirm the matching check task fails. For example,
   insert an orphan row and rerun:
   ```bash
   cd session_02_airflow_intro/lab
   docker compose exec -T airflow-scheduler python -c "
   import duckdb
   c = duckdb.connect('/opt/airflow/db/dwh.duckdb')
   c.execute(\"INSERT INTO gold.fact_sales VALUES (999999, 999, 5.0, DATE '2024-03-01')\")
   "
   ```
   Then delete the orphan row again so the table is clean.

### Short answer (include in your PR description)

DuckDB gave you MERGE, transactions, and these checks on a single file. In session 5
we move to a lakehouse (Spark + Delta/Iceberg). In 2–3 sentences: **what does the
single-file warehouse stop you from doing** that a lakehouse table format is built
to handle? (Hint: think about the single-writer limit you hit in this session, and
many jobs/engines reading and writing the same tables at once.)

---

## Submit — open a Pull Request

1. Create a branch: `git checkout -b homework/session-04/<your-name>`.
2. Add your DAGs under `session_04_sql_dwh/homework/submissions/`.
3. Commit and push, then **open a Pull Request** against `main`.
4. In the PR description include: what you did, the `silver.events` output after a
   rerun (HW1), a note on which DQ check you broke to prove the gate works (HW2),
   and your short answer above.

The PR is for review only — it will not be merged, so don't worry about conflicts
with other submissions.

## Acceptance criteria

- [ ] HW1: `silver.events` keeps one row per `event_id` (latest by `updated_at`), tombstones removed; rerunning a date does not duplicate.
- [ ] HW1: reconcile (`SQLCheckOperator`) fails if duplicates slip through; the dedup uses a window function, not a manual `GROUP BY` guess.
- [ ] HW1: load is idempotent (DELETE-by-date + INSERT) and atomic (`BEGIN … COMMIT` in the `.sql`).
- [ ] HW2: all five rules run as check operators; a failing check fails the DAG.
- [ ] HW2: `on_failure_callback` wired; a deliberately broken rule makes the matching check task fail.
- [ ] Both use operators + external `.sql` (no raw Python connections), reach the warehouse via `conn_id="duckdb_default"`, and run on `duckdb_pool`.
