# Session 4 Lab — Setup & Exercises

**Master Class DataOps for Modern Data Platforms · Session 4/11**

Session 4 reuses the Airflow stack from session 2 — there is no separate stack to
start here. This guide adds the DuckDB package and the session 4 mounts, runs the
reference pipeline, then walks through the two exercises.

> Every `python -c "import duckdb..."` snippet in this guide is copy-paste
> terminal inspection — nothing requires `docker exec`. The warehouse is a plain
> file on your **host** (`db/dwh.duckdb`); open it directly in DBeaver or any
> DuckDB client instead if you prefer. See
> [../README.md](../README.md#querying-the-warehouse-from-dbeaver-or-any-duckdb-client-on-your-host).

## 1. Prerequisites

- The session 2 stack works (`docker compose ps` all healthy). See
  `session_02_airflow_intro/lab/README.md`.
- Port `8080` (Airflow) reachable.

## 2. Enable session 4 on the stack

The session 2 `docker-compose.yaml` already carries the session 4 changes: it builds
a custom image (base Airflow + DuckDB, via the `Dockerfile` next to the compose),
sets `DATAOPS_DWH_PATH`, and mounts:

| Host path | Container path | Purpose |
|---|---|---|
| `plugins/` (repo root) | `/opt/airflow/plugins` | shared `dataops_common` (notifications) |
| `session_04_sql_dwh/demo/dags` | `/opt/airflow/dags/session_04` | both demo DAGs (`session_04_demo_write_patterns`, `session_04_orders_medallion_etl`) + their `sql/` |
| `session_04_sql_dwh/lab/dags` | `/opt/airflow/dags/session_04_lab` | the two lab DAGs + their `sql/` |
| `session_04_sql_dwh/db` | `/opt/airflow/db` | the DuckDB warehouse file |

It also builds a custom image (base Airflow + `duckdb` + `airflow-provider-duckdb`),
bakes the `duckdb_default` connection (an env var), and creates the 1-slot
`duckdb_pool` (in `airflow-init`) — so there is **no manual connection or pool
setup**. Build the image once, then bring the stack up:

```bash
cd session_02_airflow_intro/lab
docker compose build      # one-time: bakes DuckDB into the image
docker compose up -d

# confirm DuckDB is available and the DAGs are mounted
docker compose exec airflow-scheduler python -c "import duckdb; print(duckdb.__version__)"
docker compose exec airflow-scheduler ls /opt/airflow/dags/session_04 /opt/airflow/dags/session_04_lab
```

`docker compose build` runs only once (rerun it only if the `Dockerfile` changes);
DuckDB is baked in, so container starts stay normal speed. Editing a DAG under
`session_04_sql_dwh/**/dags/` afterwards is picked up automatically (the scheduler
rescans every ~30s) — no rebuild or restart needed.

## 3. Run the reference pipeline first

The labs read `silver.orders`, which the reference pipeline produces. Run it for a
date before starting the exercises. All `docker compose` commands run from
`session_02_airflow_intro/lab`.

1. In the UI (`http://localhost:8080`, `airflow` / `airflow`), unpause
   `session_04_orders_medallion_etl` and trigger a run for **2024-03-01**, or from the CLI:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger session_04_orders_medallion_etl -e 2024-03-01
   ```
2. Watch `create_schema → load_bronze → validate_to_silver → build_dim_customer →
   build_fact_sales → reconcile` go green.
3. Inspect the warehouse:
   ```bash
   docker compose exec -T airflow-scheduler python -c "
   import duckdb
   c = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True)
   print('silver rows :', c.execute(\"SELECT COUNT(*) FROM silver.orders WHERE order_date='2024-03-01'\").fetchone()[0])
   print('fact rows   :', c.execute(\"SELECT COUNT(*) FROM gold.fact_sales WHERE order_date='2024-03-01'\").fetchone()[0])
   "
   ```
   Both are **400,000** (of 400,002 raw rows, two dirty ones were dropped at Silver).

   > Inspect the warehouse with plain `duckdb` and the file path, as above. The
   > `dataops_common` helper only imports **inside** an Airflow task, not from a
   > bare `python -c`.

> DuckDB is single-writer, but you don't have to babysit it: every session-4 task
> runs on the 1-slot `duckdb_pool`, so Airflow serializes warehouse access even if
> two DAGs are triggered at once.

## 4. Lab 1 — an idempotent daily rollup fact

DAG: `lab/dags/session_04_fact_customer_daily_starter.py`. Build a Gold table with
one row per customer per day — `order_count` and `total_amount` — aggregated from
`silver.orders`. **You complete the SQL, not the Python**: the DAG already wires
three operators to three `.sql` files in `lab/dags/sql/fact_customer_daily_starter/`.
The reference pipeline's `build_fact_sales.sql` + `reconcile.sql` are the same
shape — read them first.

1. **DAG, TODO #1** — fill `default_args`: `retries=2`, `retry_delay=timedelta(minutes=1)`, `on_failure_callback=notify_on_failure`.
2. **`sql/fact_customer_daily_starter/build_fact_customer_daily.sql`** — the idempotent + atomic rollup: `BEGIN;` `DELETE` the date; `INSERT … SELECT customer_id, order_date, COUNT(*), SUM(amount) … GROUP BY customer_id, order_date;` `COMMIT;`. Use `DATE '{{ ds }}'` for the run date.
3. **`sql/fact_customer_daily_starter/reconcile_fact_customer_daily.sql`** — return one row of boolean columns (`SQLCheckOperator` fails on any falsy): `SUM(silver.amount) == SUM(fact.total_amount)` and `COUNT(silver rows) == SUM(fact.order_count)`.

> **Exact vs tolerance.** Here both sides derive from the same `silver.orders`
> rows with no lossy step, so an exact `ROUND(…, 2)` equality is the right check.
> Real-world reconciliation usually allows a small tolerance (e.g. abs diff
> `< 0.01%`) because floats, currency conversion, or a lossy source make an exact
> match too brittle — a check that fails on a one-cent rounding drift just trains
> people to ignore it.

`sql/fact_customer_daily_starter/create_fact_customer_daily.sql` (the target table) is given.

Test it (date `2024-03-01`, after the reference pipeline ran that date):

```bash
docker compose exec airflow-scheduler airflow dags trigger session_04_fact_customer_daily_starter -e 2024-03-01
```

Acceptance criteria:
- Before you start, `reconcile` fails (its placeholder returns `false`) — that's the "not done yet" signal.
- Once finished, the DAG runs green; `gold.fact_customer_daily` has 5,000 rows for the date (one per customer, ids 101–5100), and `SUM(total_amount)` equals `SUM(amount)` in `silver.orders`.
- Triggering the same date twice leaves exactly 5,000 rows for that date (idempotent).
- If you drop the `DELETE` (plain `INSERT`), a second run makes `reconcile` fail — put it back.

## 5. Lab 2 — a Slowly Changing Dimension (Type 2)

DAG: `lab/dags/session_04_scd2_dim_customer_starter.py`. The reference `dim_customer`
is SCD Type 1 — it overwrites, so history is lost. Here you keep every version with a
validity window (`effective_from`, `effective_to`, `is_current`) plus a **surrogate
key** `customer_key` (one per version — Kimball), so you can ask "what tier was this
customer on that day?". This DAG seeds its own source; you write one `.sql` file.

`sql/scd2_dim_customer_starter/create_scd2.sql` (tables + the sequence for the
surrogate key) and `sql/scd2_dim_customer_starter/load_snapshot.sql` (the
deterministic daily source) are given.

1. **DAG, TODO #1** — fill `default_args` (same as Lab 1).
2. **`sql/scd2_dim_customer_starter/apply_scd2.sql`** — inside one transaction (`BEGIN … COMMIT`):
   - **Close** current rows whose `tier` differs from today's snapshot: set `effective_to = DATE '{{ ds }}'`, `is_current = false`.
   - **Open** a new current row (`effective_from = DATE '{{ ds }}'`, `effective_to = NULL`, `is_current = true`) for every snapshot customer with no current row of that tier — new customers and the ones you just closed. Omit `customer_key` so the sequence fills it.

Reference: DuckDB's [MERGE for SCD Type 2](https://duckdb.org/docs/current/guides/sql_features/merge) guide.

Test the history is built correctly by running three days **in order**:

```bash
for d in 2024-01-01 2024-01-02 2024-01-03; do
  docker compose exec airflow-scheduler airflow dags trigger session_04_scd2_dim_customer_starter -e $d
  sleep 20
done
```

Then inspect:

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True)
for r in c.execute('SELECT customer_key, customer_id, tier, effective_from, effective_to, is_current FROM gold.dim_customer_scd2 ORDER BY customer_id, effective_from').fetchall():
    print(r)
"
```

Acceptance criteria:
- Customer 201 has **two** versions with **different `customer_key`s**: `gold` valid
  `[2024-01-01, 2024-01-03)` with `is_current=false`, and `platinum` valid from
  `2024-01-03` with `is_current=true`.
- Customer 202 has **one** current `silver` version.
- Re-running any of those dates does **not** add a duplicate version (idempotent) —
  the check is that a same-tier snapshot opens no new row.

## 6. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: duckdb` | Image not built with DuckDB | `docker compose build` then `docker compose up -d` in `session_02_airflow_intro/lab` |
| `ModuleNotFoundError: dataops_common` | Plugins mount not applied | Confirm `../../plugins` is mounted; restart the stack |
| `session_04*` DAGs missing in UI | Mounts not applied, or an import error | Restart the stack; check the DAG's import-error banner in the UI |
| `IO Error: Could not set lock on file` | A task ran off the pool (concurrent writer) | Confirm every session-4 task has `pool="duckdb_pool"` and the pool exists (`airflow pools list`) |
| Task stuck `queued`, never runs | `duckdb_pool` missing (airflow-init didn't create it) | `docker compose up -d` to re-run init, or `airflow pools set duckdb_pool 1 x` |
| Lab DAG fails at `silver.orders` does not exist | Reference pipeline hasn't run for the date | Trigger `session_04_orders_medallion_etl` for the date first (section 3) |
