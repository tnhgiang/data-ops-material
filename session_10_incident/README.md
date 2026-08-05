# Session 10 — Incident RCA: Tracing a Silent Data Bug with Marquez

## What You Will Learn

- How to use Marquez to trace a data lineage graph from symptom to root cause
- Why row-count reconciliation alone does not catch all data quality failures
- How to write a runbook that references lineage nodes — not just Airflow tasks

## The Incident

An upstream partner renames the `amount` column to `revenue` in their daily order CSV
without notice. The pipeline runs end-to-end with no task failures. Row counts are normal.
The only signal is `total_revenue = 0` in the gold table — caught by the reconciliation check.

Without Marquez, you would manually inspect each job's output to find where `amount`
turned null. With Marquez, you open the lineage graph, trace from `gold.fact_daily_revenue`
upstream, and find the schema break in under two minutes.

## Services

| Service       | URL                        |
|---------------|----------------------------|
| Airflow UI    | http://localhost:8888       |
| Marquez UI    | http://localhost:3000       |
| Marquez API   | http://localhost:5050       |
| Spark UI      | http://localhost:8080       |
| MinIO Console | http://localhost:9001       |

## Setup

### 1. Copy JARs from session_05

```bash
cp ../session_05_pyspark_lakehouse/jars/*.jar jars/
```

### 2. Start the stack

```bash
docker compose up --build -d
```

Wait until all services are healthy (~3 minutes). Trino takes the longest — verify with:

```bash
curl -s http://localhost:8090/v1/info | grep '"starting":false'
```

### 3. Upload sample data

```bash
pip install minio
python scripts/generate_data.py
```

This uploads 3 days of order CSVs (`2026-06-13`, `2026-06-14`, `2026-06-15`) to MinIO.

### 4. Register tables in Hive Metastore

Run once before triggering the DAG for the first time:

```bash
docker exec session_10_incident-airflow-worker-1 spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=airflow-worker \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --jars /opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/hadoop-aws-3.3.4.jar,/opt/jars/aws-java-sdk-bundle-1.12.262.jar \
  --py-files /opt/airflow/plugins/dataops_common/lakehouse_common.py \
  /opt/airflow/jobs/init_schema.py
```

You should see:
```
INFO init_schema.py:25 Registered bronze.marketplace_orders
INFO init_schema.py:31 Registered silver.orders
INFO init_schema.py:37 Registered gold.fact_daily_revenue
```

### 5. Run a clean baseline

Trigger the DAG for `2026-06-13` and `2026-06-14` in Airflow UI (http://localhost:8888).
All 4 tasks should turn green for each run.

## Demo: Reproducing and Tracing the Incident

### Step 1 — Inject the incident

```bash
python scripts/inject_incident.py
```

This renames `amount` → `revenue` in `bronze/orders/2026-06-15.csv` on MinIO.

### Step 2 — Trigger the pipeline

Trigger the DAG for `2026-06-15` in Airflow UI. Watch all Spark tasks go green — then
see `reconciliation` fail with:

```
total_revenue is 0.0 for 2026-06-15 — expected > 0.
Check silver_transform: 'amount' column may be null due to upstream schema change.
```

### Step 3 — Trace in Marquez

Open `http://localhost:3000` and follow the steps in `runbooks/schema_change.md`.

### Step 4 — Fix and verify

```bash
# Restore the correct column name
python scripts/inject_incident.py --restore

# Rerun the full pipeline for the incident date
docker exec session_10_incident-airflow-worker-1 \
  airflow tasks clear session_10_order_pipeline -s 2026-06-15 -e 2026-06-16 -y
```

All 4 tasks should turn green and reconciliation should pass.

## Folder Structure

```
session_10_incident/
├── dags/
│   └── session_10_order_pipeline.py   # Airflow DAG
├── jobs/
│   ├── init_schema.py                 # register Delta tables in Hive Metastore (run once)
│   ├── bronze_load.py                 # CSV → bronze Delta
│   ├── silver_transform.py            # bronze → silver (dedup, mask PII)
│   └── gold_aggregate.py              # silver → gold (daily revenue)
├── runbooks/
│   └── schema_change.md               # step-by-step RCA guide
├── scripts/
│   ├── generate_data.py               # upload sample CSVs to MinIO
│   └── inject_incident.py             # inject / restore the incident
├── conf/                              # Spark, Hive, Trino, Marquez config
├── jars/                              # Delta JARs (git-ignored, copy from session_05)
└── docker-compose.yml
```
