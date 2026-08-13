# Session 10 — Incident RCA: Tracing a Silent Data Bug with Marquez

Reproduce a real-world silent data failure — a schema change that passes every row-count check — then trace the root cause end-to-end using OpenLineage and Marquez.

## Two Ways to Track Lineage

Lineage is not just a tool feature — it is the ability to answer: **"where did this data come from, and what transformed it?"**

There are two complementary approaches:

**1. Tool-based lineage** — use OpenLineage + Marquez to automatically record every dataset read and write at runtime. You get a live graph of how data flows across jobs, what schema each dataset had at each run, and exactly which job introduced a change. This is what we build in this session.

**2. Convention-based lineage** — embed lineage directly into your source code through naming discipline. A job named `bronze2silver__orders.py` tells you the source layer, the target layer, and the table — without opening the file. A SQL file named `reconcile__revenue__fact_daily_revenue.sql` tells you what it checks and on which table. When every file, task, and query follows this convention, your repository itself becomes a lineage document that any engineer can read.

Both approaches answer the same question. Tool-based lineage answers it at runtime; convention-based lineage answers it at design time. In a mature DataOps team you use both — the tool catches what the code cannot express, and the code makes the tool's graph easier to interpret.

This session demonstrates the combination: the incident is caught by Marquez, and the fix path is obvious because the codebase already tells you where each layer lives.

## The Incident

An upstream partner renames `amount` → `revenue` in their daily order CSV without notice.
The pipeline runs end-to-end: no task failures, normal row counts throughout.
The only signal is `total_revenue = 0` in the gold table, caught by a value-level reconciliation check.

Open Marquez, trace `gold.fact_daily_revenue` upstream — the schema divergence is visible in under two minutes.

## Stack

| Tool | Role |
|---|---|
| Airflow 2.9 (CeleryExecutor) | Orchestrate bronze → silver → gold pipeline + reconciliation checks |
| Apache Spark 3.5 + Delta Lake | Process CSV into medallion Delta tables |
| OpenLineage (Spark listener + Airflow provider) | Emit lineage events automatically on every job run |
| Marquez 0.49 | Collect lineage events, visualise the dataset graph, surface schema changes |
| Trino 445 | Query Delta tables for reconciliation checks |
| Hive Metastore | Delta table registry shared by Spark and Trino |
| MinIO | S3-compatible object store for bronze / silver / gold buckets |

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8888 | airflow / airflow |
| Marquez UI | http://localhost:3000 | — |
| Marquez API | http://localhost:5050 | — |
| Spark UI | http://localhost:8080 | — |
| Trino UI | http://localhost:8090 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

## Structure

```
session_10_incident/
├── dags/
│   ├── session_10_order_pipeline.py   # DAG: bronze → silver → gold → 3 reconciliation checks
│   └── sql/session_10_order_pipeline/ # SQLCheckOperator queries (silver empty, revenue > 0, row count match)
├── jobs/
│   ├── init_schema.py                 # register Delta tables in Hive Metastore (run once)
│   ├── raw2bronze__marketplace_orders.py   # CSV → bronze Delta (mergeSchema; missing columns → WARNING, not error)
│   ├── bronze2silver__orders.py            # bronze → silver (dedup on order_id, mask PII, status filter)
│   └── silver2gold__fact_daily_revenue.py  # silver → gold (daily revenue per partner)
├── runbooks/
│   └── schema_change.md               # step-by-step RCA: symptom → Marquez trace → fix
├── scripts/
│   ├── generate_data.py               # generate and upload 3 days of order CSVs to MinIO
│   └── inject_incident.py             # rename 'amount' → 'revenue' in 2026-06-15.csv (or restore)
├── conf/                              # spark-defaults.conf, hive-site.xml, trino/, marquez.yml
├── jars/                              # Delta + Hadoop JARs (git-ignored — copy from session_05)
├── Dockerfile                         # extends session_05 image; adds apache-airflow-providers-trino
└── docker-compose.yml
```

## Quick Start

**1. Copy JARs from session_05**

```bash
cp ../session_05_pyspark_lakehouse/jars/*.jar jars/
```

**2. Start the stack**

```bash
cd session_10_incident
docker compose up --build -d
docker compose ps
```

Wait ~3 minutes for all services to become healthy. Trino takes the longest — confirm it is ready:

```bash
curl -s http://localhost:8090/v1/info | grep '"starting":false'
```

**3. Upload sample data**

```bash
pip install minio
python scripts/generate_data.py
```

Uploads three days of order CSVs (`2026-06-13`, `2026-06-14`, `2026-06-15`) to `bronze/orders/` on MinIO.

**4. Register Delta tables in Hive Metastore** (run once)

```bash
docker exec session_10_incident-airflow-worker-1 spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=airflow-worker \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf "spark.extraListeners=" \
  --py-files /opt/airflow/plugins/dataops_common/lakehouse_common.py \
  /opt/airflow/jobs/init_schema.py
```

Expected output:
```
INFO init_schema.py:25 Registered bronze.marketplace_orders
INFO init_schema.py:31 Registered silver.orders
INFO init_schema.py:37 Registered gold.fact_daily_revenue
```

**5. Run a clean baseline**

Trigger the DAG in Airflow UI (http://localhost:8888) for execution dates `2026-06-13T00:00:00+00:00` and `2026-06-14T00:00:00+00:00`. For a `@daily` DAG, execution date = `data_interval_end`, so these runs process `2026-06-13.csv` and `2026-06-14.csv` respectively. All six tasks should turn green for each run.

Verify via Trino:

```sql
-- Row counts per layer per date
SELECT order_date, count(*) AS rows FROM delta.silver.orders GROUP BY 1 ORDER BY 1;
SELECT order_date, partner, order_count, total_revenue FROM delta.gold.fact_daily_revenue ORDER BY 1, 2;
```

Expected: two dates (`2026-06-13`, `2026-06-14`), `total_revenue > 0` for all rows.

## Querying from Trino (or DBeaver) on your host

Trino is the interactive query engine over the Delta tables. Connect **DBeaver** on your host
to `localhost:8090` (catalog `delta`, schemas `bronze` / `silver` / `gold`), or use the CLI:

```bash
# Row counts per layer
docker exec session_10_incident-trino-1 trino --server http://localhost:8080 --user admin --execute \
  "SELECT order_date, count(*) AS rows FROM delta.silver.orders GROUP BY 1 ORDER BY 1"

# Daily revenue summary
docker exec session_10_incident-trino-1 trino --server http://localhost:8080 --user admin --execute \
  "SELECT order_date, partner, order_count, total_revenue FROM delta.gold.fact_daily_revenue ORDER BY 1, 2"
```

> **Read-after-write.** `conf/trino/catalog/delta.properties` sets `delta.metadata.cache-ttl=0s`
> so Trino sees Spark's latest commit immediately without stale metadata.

## Demo Walkthrough

This walkthrough follows the standard RCA lifecycle: **Detect → Contain → Investigate → Fix → Post-mortem**. Each step maps to a real phase so you can see the framework in action, not just as theory.

**Step 1 — Inject the incident**

```bash
python scripts/inject_incident.py
```

Renames `amount` → `revenue` in `bronze/orders/2026-06-15.csv` on MinIO.

---

### ① Detect

**Step 2 — Trigger the incident run**

Trigger the DAG for execution date `2026-06-15T00:00:00+00:00` in Airflow UI (`data_interval_end = 2026-06-15` → reads the tampered `2026-06-15.csv`).

Watch `raw2bronze__marketplace_orders`, `bronze2silver__orders`, and `silver2gold__fact_daily_revenue` go green — then `reconcile__revenue__fact_daily_revenue` fails. The task log shows the Trino query returned `False`: revenue summed to zero.

The reconciliation check is your monitoring. Without it, this incident produces no alert — the pipeline finishes green and bad data lands silently in gold.

Confirm the silent failure via Trino:

```sql
-- gold shows total_revenue = 0 for 2026-06-15
SELECT order_date, partner, order_count, total_revenue
FROM delta.gold.fact_daily_revenue
WHERE order_date = DATE '2026-06-15'
ORDER BY partner;

-- bronze shows amount = NULL for 2026-06-15 (column was renamed upstream)
SELECT order_date, count(*) AS rows, count(amount) AS with_amount, count(revenue) AS with_revenue
FROM delta.bronze.marketplace_orders
WHERE order_date = DATE '2026-06-15'
GROUP BY 1;
```

---

### ② Contain

**Before investigating or fixing**, contain the blast radius. Bad data is already in gold for `2026-06-15` — any downstream consumer reading that table now will see `total_revenue = 0` and may act on it.

Two actions to take immediately:

1. **Notify stakeholders** — alert the team that `fact_daily_revenue` for `2026-06-15` is unreliable. Do not wait until the fix is ready.
2. **Block downstream** — if any job or report reads from `gold.fact_daily_revenue` on a schedule, pause it now. In this stack there are no automated downstream jobs, but in production this step prevents cascading bad data.

Contain ≠ Fix. The goal is to stop the damage from spreading while you investigate the root cause.

---

### ③ Investigate

**Step 3 — Trace the root cause**

Three Spark tasks are green, row counts are normal, but `total_revenue = 0`. Something changed in the data itself — not the pipeline. The question is: at which layer did the value go missing?

**Option A — Convention-based lineage (no tools needed)**

Read the job names in `jobs/`:

```
raw2bronze__marketplace_orders.py      CSV → bronze.marketplace_orders
bronze2silver__orders.py               bronze.marketplace_orders → silver.orders
silver2gold__fact_daily_revenue.py     silver.orders → gold.fact_daily_revenue
reconcile__revenue__fact_daily_revenue.sql  checks gold.fact_daily_revenue
```

The naming convention encodes the full lineage path. Revenue is summed in `silver2gold`, which reads `amount` from silver. Silver selects `amount` from bronze. Bronze ingests from the CSV. Start at the bottom: check whether `amount` exists in the source CSV for `2026-06-15`.

Confirm by searching the Airflow task log for `raw2bronze__marketplace_orders` (incident run). The job warns when a column is missing — search by keyword `missing expected columns` in the task log. Airflow wraps all Spark output as `INFO`, so filtering by log level will not find it:

```
WARNING raw2bronze__marketplace_orders.py:28 Source CSV is missing expected columns: {'amount'} — they will be null in bronze
```

This confirms the column disappeared at the bronze ingestion step — the source CSV is the root cause.

**Option B — Tool-based lineage via Marquez**

Revenue = 0 but all pipeline tasks are green — the data passed through without error, but the value is wrong. The question is: at which layer did `amount` go missing? Marquez recorded the schema of every dataset at every run, so you can trace the answer directly from the lineage graph.

Open http://localhost:3000, navigate from `gold.fact_daily_revenue` upstream layer by layer, and inspect the field list on each dataset node. When you reach `bronze.marketplace_orders`, the field list at the incident run shows `revenue` instead of `amount` — the column was renamed in the source CSV without notice. Root cause confirmed in under two minutes.

Follow `runbooks/schema_change.md` for the exact navigation steps.

---

### ④ Fix

**Step 4 — Fix and rerun**

Root cause confirmed: upstream renamed `amount` → `revenue` in the source CSV. Fix the source, then rerun the failed date.

```bash
# Restore the correct column name in the source file
python scripts/inject_incident.py --restore

# Clear and rerun the failed date
docker exec session_10_incident-airflow-worker-1 \
  airflow tasks clear session_10_order_pipeline -s 2026-06-15 -e 2026-06-16 -y
```

All tasks should turn green and all three reconciliation checks should pass.

Confirm via Trino:

```sql
SELECT order_date, partner, order_count, total_revenue
FROM delta.gold.fact_daily_revenue
WHERE order_date = DATE '2026-06-15'
ORDER BY partner;
```

Expected: `total_revenue > 0` for all rows.

---

### ⑤ Post-mortem

The runbook at `runbooks/schema_change.md` is the post-mortem artifact for this incident class. It documents the symptom, the investigation path, the fix, and the escalation criteria — so the next engineer who sees `total_revenue = 0` does not start from zero.

Two things to update after every real incident of this type:

- **Runbook** — add any new signal or navigation step you discovered during the investigation
- **Schema contract** — consider adding an explicit column check in `raw2bronze__marketplace_orders` that fails fast instead of silently accepting a renamed column

Post-mortem is the step that turns a one-time fix into institutional knowledge. Skip it and the same incident takes just as long to resolve the second time.

## Common Errors

| Symptom | Cause | Fix |
|---|---|---|
| `Jar /opt/jars/delta-spark_2.12-3.2.0.jar not found` | JARs not copied before `docker compose up` | `cp ../session_05_pyspark_lakehouse/jars/*.jar jars/` then restart worker |
| `Unknown hook type "trino"` | Image built without `apache-airflow-providers-trino` | `docker compose up --build -d` to rebuild from the session_10 Dockerfile |
| `PATH_NOT_FOUND: s3a://bronze/orders/<date>.csv` | Wrong execution date used to trigger; CSV for that date does not exist | Trigger with an execution date whose `data_interval_end` matches an uploaded CSV (see Step 5 above) |
| `total_revenue = 0` — all three Spark tasks green | Expected — this is the incident. The `reconcile__revenue__fact_daily_revenue` check catches it | Follow Step 3: trace in Marquez |
| `ClassNotFoundException: DeltaSparkSessionExtension` | `spark.jars` in `spark-defaults.conf` points to missing JAR | Confirm `jars/` is populated and the volume mount in `docker-compose.yml` is correct |
| Marquez graph empty after DAG runs | OpenLineage Spark listener jar missing from `$SPARK_HOME/jars/` | Check Dockerfile `RUN wget … openlineage-spark_2.12-1.22.0.jar` ran successfully: `docker exec session_10_incident-airflow-worker-1 ls $SPARK_HOME/jars/ \| grep openlineage` |
| `duckdb` errors | Session 10 uses Delta Lake + Trino, not DuckDB | Ensure you are in `session_10_incident/` and not mixing with another session's stack |

## Shutdown

```bash
docker compose down -v
```
