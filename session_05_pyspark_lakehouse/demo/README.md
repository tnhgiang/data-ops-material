# Session 5 Demo — Run-Book

Two demos, in order. Demo 1 isolates the lakehouse's core write semantics on a tiny
table with plain `spark-submit`; Demo 2 is the full reference pipeline in Airflow where
everything converges. Every command and every output below has been run end to end.

All commands run from `session_05_pyspark_lakehouse/`. Bring the stack up, generate +
upload data for `2026-06-26`, and run `init_schema.py` first — see
[../README.md](../README.md#bring-up-the-stack). The Delta jars and S3A config are
baked into spark-master's `spark-defaults.conf`, so the `spark-submit` / `spark-sql`
commands below need no `--jars` or `--conf`.

## Why these two demos, and in this order

Session 5 is about running the daily pipeline on a **lakehouse** instead of a single
file. The two properties that make that worth it — **idempotent partition overwrite**
and **time-travel reproducibility** — are easiest to see in isolation first, then in a
real pipeline.

1. **Demo 1** does one thing: write one date's partition to a Delta table, and show that
   a rerun replaces instead of appends, and that every version is still reachable. One
   Spark job, no Airflow.
2. **Demo 2 (the reference pipeline)** is the full Airflow DAG — validate → fact → dim →
   reconcile — where idempotency, determinism, and reconciliation come together, and
   `reconcile` catches what "the task succeeded" alone cannot.

---

## Demo 1 — Idempotent write + time travel

`demo/jobs/write_patterns.py` writes a fixed three-row batch for a date into a small
Delta table, `lakehouse.demo_sales`.

### Step 1 — write once

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  /opt/jobs/demo/write_patterns.py --ds 2026-06-26
# ... demo_sales (idempotent) rows for 2026-06-26: 3
```

### Step 2 — Idempotency: rerun the same date
Using [Delta Overwrite Option](https://docs.delta.io/delta-batch/#overwrite), we can run a task multiple times with the same result 😁.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  /opt/jobs/demo/write_patterns.py --ds 2026-06-26
# ... demo_sales (idempotent) rows for 2026-06-26: 3
```

Still **3**, not 6. ✅ `mode("overwrite").option("replaceWhere", "order_date = '{ds}'")`
replaced the date's partition instead of adding to it — a rerun equals one run.

### Step 3 — The anti-pattern: `append`

The job's `--mode append` swaps `replaceWhere` for `mode("append")` — what you must
*not* do:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  /opt/jobs/demo/write_patterns.py --ds 2026-06-26 --mode append
# ... demo_sales (append) rows for 2026-06-26: 6
```

🔴 **Symptom:** now **6** rows — the same batch landed twice. A pipeline that appends is
not safe to rerun or backfill. This is exactly the duplication `replaceWhere` prevents.

### Step 4 — Time travel: every write is a version

```bash
docker compose exec -T trino trino --server localhost:8080 \
  --execute "SELECT version, operation FROM delta.lakehouse.\"demo_sales\$history\" ORDER BY version"
# 0  CREATE TABLE
# 1  WRITE       (step 1)
# 2  WRITE       (step 2, idempotent)
# 3  WRITE       (step 3, append)
```

Delta keeps each version. Read an earlier one with `VERSION AS OF` (via Spark — this
Trino build's Delta connector does not expose versioned tables):

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) FROM lakehouse.demo_sales VERSION AS OF 1"
# 3   — the state right after the first load, reproduced exactly
```

**Narrate:** a single-file DuckDB warehouse (session 4) had none of this — one writer,
no history. The lakehouse table format gives idempotent partition writes *and* a
reproducible past. Drop the scratch table between classes if you like:
`docker exec spark-master /opt/spark/bin/spark-sql -e "DROP TABLE lakehouse.demo_sales"`.

---

## Demo 2 — The reference pipeline, and what `reconcile` is

The full Medallion-style pipeline as an Airflow DAG:
`validate_raw_file → transform_fact_sales → upsert_dim_customer → reconcile`. The two
Spark steps are `SparkSubmitOperator`s submitting to the standalone cluster; `reconcile`
is a Python task that checks the Delta fact against the raw source through Trino.

### 2a. Run it clean

```bash
docker compose exec airflow-scheduler airflow dags unpause session_05_lakehouse_daily_pipeline
docker compose exec airflow-scheduler airflow dags trigger session_05_lakehouse_daily_pipeline -e 2026-06-26
```

In the Grid view (`http://localhost:8888`, `airflow` / `airflow`) all four tasks go
green. Show the fact and the dimension:

```bash
docker compose exec -T trino trino --server localhost:8080 --execute \
  "SELECT COUNT(*) AS rows, SUM(amount) AS revenue FROM delta.lakehouse.fact_sales WHERE order_date = DATE '2026-06-26'"
# 664 | 166738.34     (of 1000 raw rows — pending orders and non-positive amounts dropped)

docker compose exec -T trino trino --server localhost:8080 --execute \
  "SELECT segment, COUNT(*) FROM delta.lakehouse.dim_customer GROUP BY segment ORDER BY segment"
# enterprise | 69
# retail     | 57
# wholesale  | 73     (199 customers, segment = hash(customer_id) % 3 — deterministic)
```

### 2b. Idempotency: rerun the whole date

Clear the run and let it rerun; the fact still has **664** rows, not 1328:

```bash
docker compose exec airflow-scheduler airflow tasks clear session_05_lakehouse_daily_pipeline -s 2026-06-26 -e 2026-06-26 -y
# after it finishes, COUNT(*) for 2026-06-26 is still 664
```

`transform_fact_sales` uses `replaceWhere`; `upsert_dim_customer` uses `MERGE` — both
replace the date's slice rather than adding to it.

### 2c. Determinism: `processed_at` is the run date

```bash
docker compose exec -T trino trino --server localhost:8080 --execute \
  "SELECT DISTINCT processed_at FROM delta.lakehouse.fact_sales WHERE order_date = DATE '2026-06-26'"
# 2026-06-26     — every row, because the job uses lit(ds), not current_timestamp()
```

That single value is why a backfill reproduces a date exactly instead of stamping it
with whenever the rerun happened.

### 2d. Reproducibility: time travel to before the first load

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) AS current FROM lakehouse.fact_sales;
      SELECT COUNT(*) AS at_v0 FROM lakehouse.fact_sales VERSION AS OF 0"
# current : 664
# at_v0   : 0      — version 0 is the empty table init_schema created, still reachable
```

### 2e. Fault tolerance: clear one task, only it reruns

```bash
docker compose exec airflow-scheduler airflow tasks clear session_05_lakehouse_daily_pipeline -t transform_fact_sales -s 2026-06-26 -e 2026-06-26 -y
```

Only `transform_fact_sales` (and its downstream `upsert_dim_customer`, `reconcile`)
rerun; `validate_raw_file` stays SUCCESS. The fact ends at 664 again — recovering from a
bad state is just "rerun it".

> **Bonus (atomicity):** while `transform_fact_sales` is running, kill a Spark worker
> (`docker compose kill spark-worker && docker compose up -d spark-worker`). The Delta
> write is atomic — either the new partition commits or it doesn't; a half-written
> partition never appears. The task fails, retries, and the next run completes cleanly
> with the count intact.

---

## Cleanup (optional)

Remove the demo rows so a later class starts fresh; the schema stays:

```bash
docker compose exec -T trino trino --server localhost:8080 --execute \
  "DELETE FROM delta.lakehouse.fact_sales WHERE order_date = DATE '2026-06-26'"
```

(`demo_sales` from Demo 1 can be dropped with the `spark-sql ... DROP TABLE` shown above.)
