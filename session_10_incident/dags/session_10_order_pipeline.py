from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import trino
from airflow.decorators import dag, task
from airflow.lineage.entities import File
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dataops_common.notifications import notify_on_failure

log = logging.getLogger(__name__)

JARS = (
    "/opt/jars/delta-spark_2.12-3.2.0.jar"
    ",/opt/jars/delta-storage-3.2.0.jar"
    ",/opt/jars/hadoop-aws-3.3.4.jar"
    ",/opt/jars/aws-java-sdk-bundle-1.12.262.jar"
)
PY_FILES = "/opt/airflow/plugins/dataops_common/lakehouse_common.py"
SPARK_CONF = {
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",
}

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


@dag(
    dag_id="session_10_order_pipeline",
    description="Order pipeline: bronze CSV → silver clean → gold daily revenue",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_on_failure,
    },
    tags=["session-10", "lakehouse", "spark", "delta", "orders"],
)
def session_10_order_pipeline():
    bronze_load = SparkSubmitOperator(
        task_id="bronze_load",
        application="/opt/airflow/jobs/bronze_load.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ data_interval_end | ds }}"],
        jars=JARS,
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
        inlets=[File(url="s3a://bronze/orders/{{ ds }}.csv")],
        outlets=[File(url="s3a://bronze/marketplace_orders")],
    )

    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        application="/opt/airflow/jobs/silver_transform.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ data_interval_end | ds }}"],
        jars=JARS,
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
        inlets=[File(url="s3a://bronze/marketplace_orders")],
        outlets=[File(url="s3a://silver/orders")],
    )

    gold_aggregate = SparkSubmitOperator(
        task_id="gold_aggregate",
        application="/opt/airflow/jobs/gold_aggregate.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ data_interval_end | ds }}"],
        jars=JARS,
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
        inlets=[File(url="s3a://silver/orders")],
        outlets=[File(url="s3a://gold/fact_daily_revenue")],
    )

    @task(task_id="reconciliation")
    def reconciliation(data_interval_end=None):
        run_date = data_interval_end.strftime("%Y-%m-%d")
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user="airflow", catalog="delta"
        )
        cur = conn.cursor()

        cur.execute(
            f"SELECT count(*) FROM delta.silver.orders WHERE order_date = DATE '{run_date}'"
        )
        silver_rows = cur.fetchone()[0]

        cur.execute(
            f"SELECT coalesce(sum(total_revenue), 0) FROM delta.gold.fact_daily_revenue WHERE order_date = DATE '{run_date}'"
        )
        total_revenue = float(cur.fetchone()[0])

        cur.execute(
            f"SELECT coalesce(sum(order_count), 0) FROM delta.gold.fact_daily_revenue WHERE order_date = DATE '{run_date}'"
        )
        gold_order_count = int(cur.fetchone()[0])

        log.info(
            "Reconcile %s: silver_rows=%d gold_order_count=%d total_revenue=%.2f",
            run_date, silver_rows, gold_order_count, total_revenue,
        )

        if silver_rows == 0:
            raise ValueError(f"silver.orders is empty for {run_date}")

        if silver_rows != gold_order_count:
            raise ValueError(
                f"Row count mismatch for {run_date}: silver={silver_rows} gold={gold_order_count}"
            )

        if total_revenue <= 0:
            raise ValueError(
                f"total_revenue is {total_revenue} for {run_date} — expected > 0. "
                "Check silver_transform: 'amount' column may be null due to upstream schema change."
            )

        log.info("Reconcile passed for %s", run_date)

    bronze_load >> silver_transform >> gold_aggregate >> reconciliation()


session_10_order_pipeline()
