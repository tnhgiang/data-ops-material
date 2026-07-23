# Session 07 — Security & Sensitive Data

Stack: Airflow (Celery) + Spark + MinIO (Delta Lake) + Hive Metastore + Trino + HashiCorp Vault + Postgres.

## Quick start

```bash
cd session_07_security
mkdir -p logs/audit
docker compose up -d
```

Wait for all services to go healthy (`docker compose ps`), then initialize Delta schemas once:

```bash
JARS=/opt/jars/delta-spark_2.12-3.2.0.jar,/opt/jars/delta-storage-3.2.0.jar,/opt/jars/hadoop-aws-3.3.4.jar,/opt/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/jars/postgresql-42.6.0.jar
docker exec s07-spark-master /opt/spark/bin/spark-submit --jars $JARS /jobs/init_schema.py
```

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8888 | airflow / airflow |
| Vault UI | http://localhost:8200 | token: `root-demo-token` |
| Trino (HTTP, healthcheck only) | http://localhost:8092 | — |
| Trino (HTTPS, queries) | https://localhost:8443 | admin / AdminDemo_2026! |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |

## Structure

```
demo/
  dags/         DAG: session_07_customer_pii_pipeline
    sql/        SQL files for DAG operators
  jobs/         Spark jobs (ingest_bronze, transform_silver, transform_gold, init_schema)
                security_utils.py — shared Vault + audit helpers
  README.md     Full demo walkthrough (7-beat script)
labs/           3 hands-on labs
  dags/         Lab 1 DAG
  jobs/         Lab 2 & 3 Spark jobs
  sql/          Lab 3 SQL scripts
conf/trino/     Trino config, TLS certs, access-control rules.json
vault-init/     seed_secrets.sh — seeds all demo secrets into Vault on first boot
scripts/        generate_customers.py, query_trino.py
sql/            init_source.sql — creates raw.customers in postgres-source
logs/audit/     pii_access.log — written by log_pii_access() in every Spark job
```

## Shutdown

```bash
docker compose down -v
```
