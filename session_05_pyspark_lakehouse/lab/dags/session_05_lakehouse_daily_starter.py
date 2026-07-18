from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dataops_common.notifications import notify_on_failure  # noqa: F401 (for DAG TODO)
from dataops_common.storage import reconcile_counts  # noqa: F401 (for Lab 3)

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

KEPT_STATUSES = {"completed", "cancelled"}

SQL_DIR = Path(__file__).parent / "sql"


def _read_raw_rows(ds: str) -> list[dict]:
    from minio import Minio

    client = Minio("minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
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
    dag_id="session_05_lakehouse_daily_starter",
    description="LAB — complete the fact/dim jobs and the reconcile task, then run this pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        # DAG TODO: add on_failure_callback=notify_on_failure so a failed run alerts.
    },
    tags=["session-05", "lab", "exercise"],
)
def session_05_lakehouse_daily_starter():

    @task
    def validate_raw_file(ds=None):
        rows = _read_raw_rows(ds)
        if not rows:
            raise ValueError(f"s3a://warehouse/raw/{ds}.csv has no data rows")
        log.info("Validated s3a://warehouse/raw/%s.csv: %d rows", ds, len(rows))

    spark_conf = {"spark.driver.host": "airflow-worker", "spark.driver.bindAddress": "0.0.0.0"}

    # Lab 1: you write jobs/lab/transform_fact_sales.py
    transform_fact_sales = SparkSubmitOperator(
        task_id="transform_fact_sales",
        application="/opt/airflow/jobs/lab/transform_fact_sales.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ ds }}"],
        jars=JARS,
        conf=spark_conf,
        verbose=False,
    )

    # Lab 2: you write jobs/lab/upsert_dim_customer.py
    upsert_dim_customer = SparkSubmitOperator(
        task_id="upsert_dim_customer",
        application="/opt/airflow/jobs/lab/upsert_dim_customer.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ ds }}"],
        jars=JARS,
        conf=spark_conf,
        verbose=False,
    )

    @task
    def reconcile(ds=None):
        # Given: the expected values from the raw file, applying the same filter the
        # fact job uses (completed/cancelled + amount > 0).
        rows = _read_raw_rows(ds)
        kept = [r for r in rows if r["order_status"] in KEPT_STATUSES and float(r["amount"]) > 0]
        staging_count = len(kept)
        staging_sum = round(sum(float(r["amount"]) for r in kept), 2)

        # Given: sql/reconcile.sql returns (fact_count, fact_sum, negatives) for the date.
        fact_count, fact_sum, negatives = _trino_row("reconcile.sql", ds)
        fact_sum = float(fact_sum)

        # Lab 3 — write the three reconcile layers. Raise (fail the task) when one
        # does not hold:
        #   Layer 1 — row count: reconcile_counts(staging_count, fact_count, label)
        #   Layer 2 — aggregate: abs(staging_sum - fact_sum) / staging_sum < 0.01
        #   Layer 3 — business rule: negatives must be 0
        raise NotImplementedError("Lab 3: implement the three reconcile layers")

    validate_raw_file() >> transform_fact_sales >> upsert_dim_customer >> reconcile()


session_05_lakehouse_daily_starter()
