# Session 5 — The Lakehouse: PySpark + Delta Lake

**Master Class DataOps for Modern Data Platforms · Session 5/11**

Session 4 built a warehouse in a single DuckDB file: one writer at a time, no history.
This session removes both limits. We build the **same daily sales pipeline** on a
**lakehouse** — a Delta Lake table on object storage — so many engines can read and
write the same tables at once, and every version of a table is kept and reproducible.

The pipeline reads a raw CSV, transforms it with **PySpark** into a Delta **fact table**,
registers it in the **Hive Metastore**, and is queried through **Trino** — all
orchestrated by **Airflow**, which submits the Spark job with a `SparkSubmitOperator`.

## What you will learn

- **Lakehouse architecture** — a table format (Delta) over object storage (MinIO/S3),
  a catalog (Hive Metastore) many engines share, and separate query engines (Spark for
  compute, Trino for interactive SQL) reading the same tables.
- **Idempotent partition overwrite** — `replaceWhere` overwrites only the run date's
  partition, so a rerun equals a single run instead of appending duplicates. This is
  the session's centrepiece.
- **Determinism** — `processed_at = lit(ds)` and a `hash % 3` customer segment, never
  `current_timestamp()` / `random()`, so a rerun of a date reproduces the same rows.
- **Reproducibility / time travel** — Delta keeps every version; `VERSION AS OF`
  reconstructs a table's earlier state (the property the single-file warehouse lacked).
- **Upserts on a lakehouse** — `MERGE INTO` for the customer dimension.
- **Reconciliation** — checking the Delta fact against its raw source on row count, a
  `SUM` aggregate, and a business rule, through Trino, and failing the run on drift.
- **Orchestrating Spark from Airflow** — `SparkSubmitOperator` submitting to a
  standalone Spark cluster in client mode, with `on_failure_callback` alerting reused
  from the shared `dataops_common` package.

## Stack

| Component | Tool | Version | Port |
|---|---|---|---|
| Object Storage | MinIO | latest | 9000 (API) · 9001 (Console) |
| Table Format | Delta Lake | 3.2.0 | — |
| Processing | PySpark | 3.5.x | — |
| Catalog | Hive Metastore | 3.0.0 (`bitsondatadev/hive-metastore`) | 9083 (Thrift) |
| Catalog Backend | MariaDB | 10.11 | 3306 |
| Query Engine | Trino | 435 | 8090 |
| Orchestration | Apache Airflow | 2.9.0 (CeleryExecutor) | 8888 |
| Airflow Backend | PostgreSQL | 15 | 5432 |
| Message Broker | Redis | 7.2 | 6379 |
| Spark UI | Spark Master | 3.5.0 (`apache/spark`) | 8080 |

> The Hive Metastore backend is **MariaDB**, not PostgreSQL — the
> `bitsondatadev/hive-metastore` image is hardwired to MySQL/MariaDB. PostgreSQL 15 is
> the **Airflow** metadata backend. The Airflow image is built from the `Dockerfile`
> here: base Airflow + Java 11 + PySpark 3.5.0 (its Python 3.8 and Java 11 match the
> Spark image, so the Spark driver runs inside the Airflow worker without version
> clashes) + the Spark provider and MinIO/Trino clients.

## Contents

| Path | What it is |
|---|---|
| `demo/` | Mentor-run material: **Demo 1** — `replaceWhere` idempotency + time travel on a tiny table (`write_patterns.py`); **Demo 2** — the reference pipeline `session_05_lakehouse_daily_pipeline` (fact + dim jobs). See [demo/README.md](demo/README.md). |
| `lab/` | Three exercises you complete — the idempotent fact load, the `MERGE` dimension, and a three-layer reconcile. See [lab/README.md](lab/README.md). |
| `homework/` | Take-home, submitted by Pull Request: a time-travel reproducibility task (+ optional daily rollup). See [homework/README.md](homework/README.md). |

## Bring up the stack

All commands run from `session_05_pyspark_lakehouse/`.

**1. Download the four Spark jars** (≈275 MB, not committed — see `.gitignore`):

```bash
wget -P jars/ \
  https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

> `delta-storage` is a separate runtime dependency of `delta-spark` (it holds
> `io.delta.storage.LogStore`); omitting it causes `NoClassDefFoundError` at write time.

**2. Build the Airflow image and start everything:**

```bash
docker compose build      # one-time: bakes Java + PySpark + the Spark provider in
docker compose up -d
docker compose ps         # wait until services are healthy
```

`docker compose build` runs only once (rerun it only if the `Dockerfile` or
`requirements_local.txt` changes). Editing a DAG under `demo/` or `lab/` afterwards is
picked up automatically (the scheduler rescans every ~30s); the Spark jobs are re-read
on each run.

> **Local dev (optional).** To edit the Spark jobs with IDE support or run one outside
> Docker, install the same libraries into a virtualenv:
> `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements_local.txt`.

> Compose adds the project prefix (e.g. `session_05_pyspark_lakehouse-trino-1`) to
> container names. The exception is `spark-master`, which sets an explicit
> `container_name`. Use `docker compose ps` to see the real names.

## Load sample data and create the tables

**1. Generate a deterministic CSV and upload it to MinIO** (the generator is seeded by
the date, so the same `ds` always yields the same file):

```bash
python3 scripts/generate_data.py --ds 2026-06-26 --rows 1000 --output-dir data/raw

NET=$(docker inspect session_05_pyspark_lakehouse-minio-1 -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run --rm --network "$NET" -v "$PWD/data/raw:/data/raw" --entrypoint sh minio/mc -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc cp /data/raw/2026-06-26.csv local/warehouse/raw/2026-06-26.csv &&
  mc ls local/warehouse/raw/
"
```

**2. Create the Delta tables once** (empty `fact_sales` partitioned by `order_date`,
and `dim_customer`):

```bash
docker exec spark-master /opt/spark/bin/spark-submit /jobs/init_schema.py
```

> The jars and Delta/S3A config live in spark-master's `conf/spark-defaults.conf`, so
> `docker exec spark-master spark-submit|spark-sql` commands need no `--jars` or `--conf`.

Now run the reference pipeline (see [demo/README.md](demo/README.md)) or the lab
exercise (see [lab/README.md](lab/README.md)).

## Querying from Trino (or DBeaver) on your host

Trino is the interactive query engine over the Delta tables. Query it from a one-off
CLI, or connect **DBeaver** (or any Trino-compatible client) on your host to
`localhost:8090` (catalog `delta`, schema `lakehouse`) — no `docker exec` needed.

```bash
# One-off query
docker exec session_05_pyspark_lakehouse-trino-1 trino --server localhost:8080 \
  --execute "SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
             FROM delta.lakehouse.fact_sales GROUP BY order_date ORDER BY order_date"

# Delta version history is visible from Trino
docker exec session_05_pyspark_lakehouse-trino-1 trino --server localhost:8080 \
  --execute "SELECT version, operation FROM delta.lakehouse.\"fact_sales\$history\" ORDER BY version"
```

**Time travel** (`VERSION AS OF`) runs through **Spark** — this Trino build's Delta
connector does not expose versioned tables, so reproducibility checks use `spark-sql`:

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) FROM lakehouse.fact_sales VERSION AS OF 0"   # 0 — the empty table, before the first load
```

Inspect the raw Delta files in MinIO the same way as the raw CSV, pointing `mc` at
`local/warehouse/fact_sales/`, or via the MinIO console at `http://localhost:9001`
(`minioadmin` / `minioadmin`).

## Directory structure

```
session_05_pyspark_lakehouse/
├── README.md                 # this file — session overview
├── docker-compose.yml        # the lakehouse stack
├── Dockerfile                # Airflow + Java 11 + PySpark + Spark provider
├── requirements_local.txt    # Python libs (also installed into the image)
├── conf/                     # hive-site.xml, trino/ config, spark-defaults.conf
├── jars/                     # the four Spark jars (downloaded, gitignored)
├── scripts/generate_data.py  # deterministic raw-data generator
├── jobs/init_schema.py       # one-off: create the empty Delta tables
├── demo/
│   ├── README.md             # mentor run-book (Demo 1 + Demo 2)
│   ├── jobs/                  # write_patterns.py, transform_fact_sales.py, upsert_dim_customer.py
│   └── dags/
│       ├── session_05_lakehouse_daily_pipeline.py
│       └── sql/reconcile.sql  # reconcile query (read by the reconcile task)
├── lab/
│   ├── README.md             # the three exercises
│   ├── jobs/                  # transform_fact_sales.py + upsert_dim_customer.py (TODO starters)
│   └── dags/
│       ├── session_05_lakehouse_daily_starter.py
│       └── sql/reconcile.sql  # given (you write the reconcile task's checks)
└── homework/
    ├── README.md
    └── submissions/          # you add code here (PR is review-only)

plugins/                      # repo-level, shared across sessions
└── dataops_common/           # notifications.py, storage.py, ...
```

## Position in the course

| | |
|---|---|
| Previous | Session 4 — SQL in the warehouse: Medallion layers, idempotent/atomic loads, MERGE, reconciliation on DuckDB |
| This session | The lakehouse: PySpark + Delta, `replaceWhere` idempotency, time-travel reproducibility, Spark-from-Airflow, Trino |
| Next | Session 6 — Streaming & incremental processing |

## Where these patterns come from

Tool-neutral lakehouse practice, not any one framework:

- **Delta Lake** — [table format, `replaceWhere`, and time travel](https://docs.delta.io/latest/delta-batch.html)
  (selective partition overwrite and `versionAsOf`).
- **Lakehouse architecture** — Databricks,
  [What is a Data Lakehouse?](https://www.databricks.com/glossary/data-lakehouse).
- **Trino over Delta** — [Trino Delta Lake connector](https://trino.io/docs/current/connector/delta-lake.html).
- **Orchestrating Spark from Airflow** —
  [`SparkSubmitOperator`](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html).
