from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dataops_common.notifications import notify_on_failure
from dataops_common.storage import reconcile_counts

log = logging.getLogger(__name__)

JARS = ",".join([
    "/opt/jars/delta-spark_2.12-3.2.0.jar",
    "/opt/jars/delta-storage-3.2.0.jar",
    "/opt/jars/hadoop-aws-3.3.4.jar",
    "/opt/jars/aws-java-sdk-bundle-1.12.262.jar",
])

ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

# The transform keeps only completed/cancelled orders with a positive amount, so
# the reconcile expectation from the raw file must apply the same filter.
KEPT_STATUSES = {"completed", "cancelled"}

SQL_DIR = Path(__file__).parent / "sql"


def _read_raw_rows(ds: str) -> list[dict]:
    from minio import Minio

    client = Minio(
        "minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )
    response = client.get_object("warehouse", f"raw/{ds}.csv")
    try:
        text = response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()
    return list(csv.DictReader(io.StringIO(text)))


def _trino_row(sql_file: str, ds: str):
    import trino

    sql = (SQL_DIR / sql_file).read_text().format(ds=ds)
    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user="airflow", catalog="delta", schema="lakehouse"
    )
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone()


@dag(
    dag_id="session_05_lakehouse_daily_pipeline",
    description="Reference lakehouse pipeline: raw CSV -> Delta fact_sales (Spark) + Trino reconcile",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_on_failure,
    },
    tags=["session-05", "lakehouse", "spark", "delta"],
)
def session_05_lakehouse_daily_pipeline():

    @task
    def validate_raw_file(ds=None):
        rows = _read_raw_rows(ds)
        if not rows:
            raise ValueError(f"s3a://warehouse/raw/{ds}.csv has no data rows")
        log.info("Validated s3a://warehouse/raw/%s.csv: %d rows", ds, len(rows))

    # Client mode: the driver runs in the airflow-worker container, so executors
    # must reach it back at this address.
    spark_conf = {
        "spark.driver.host": "airflow-worker",
        "spark.driver.bindAddress": "0.0.0.0",
    }

    transform_fact_sales = SparkSubmitOperator(
        task_id="transform_fact_sales",
        application="/opt/airflow/jobs/demo/transform_fact_sales.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ ds }}"],
        jars=JARS,
        conf=spark_conf,
        verbose=False,
    )

    upsert_dim_customer = SparkSubmitOperator(
        task_id="upsert_dim_customer",
        application="/opt/airflow/jobs/demo/upsert_dim_customer.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ ds }}"],
        jars=JARS,
        conf=spark_conf,
        verbose=False,
    )

    @task
    def reconcile(ds=None):
        rows = _read_raw_rows(ds)
        kept = [
            r for r in rows
            if r["order_status"] in KEPT_STATUSES and float(r["amount"]) > 0
        ]
        staging_count = len(kept)
        staging_sum = round(sum(float(r["amount"]) for r in kept), 2)

        # One scan returns all three fact metrics (see sql/reconcile.sql).
        fact_count, fact_sum, negatives = _trino_row("reconcile.sql", ds)
        fact_sum = float(fact_sum)

        # Layer 1 — row count: raw (post-filter) must equal the fact partition.
        reconcile_counts(staging_count, fact_count, f"fact_sales rows for {ds}")

        # Layer 2 — aggregate: SUM(amount) within a 1% tolerance (float vs decimal).
        diff_pct = abs(staging_sum - fact_sum) / staging_sum if staging_sum else 0.0
        if diff_pct >= 0.01:
            raise ValueError(
                f"SUM(amount) mismatch for {ds}: raw={staging_sum} fact={fact_sum} ({diff_pct:.2%})"
            )

        # Layer 3 — business rule: no negative amounts in the fact.
        if negatives:
            raise ValueError(f"Found {negatives} rows with negative amount for {ds}")

        log.info(
            "Reconcile passed for %s: %d rows, SUM=%.2f (raw %.2f), 0 negatives",
            ds, fact_count, fact_sum, staging_sum,
        )

    validate_raw_file() >> transform_fact_sales >> upsert_dim_customer >> reconcile()


session_05_lakehouse_daily_pipeline()
