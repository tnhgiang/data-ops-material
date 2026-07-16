from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "dataops-class",
    "conn_id": "duckdb_default",
    # DuckDB is single-writer: this 1-slot pool serializes warehouse access across
    # all session-04 DAGs so two runs never fight over the file lock.
    "pool": "duckdb_pool",
}

with DAG(
    dag_id="session_04_demo_write_patterns",
    description="Idempotency + atomicity on one load task",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/session_04/sql/demo_write_patterns",
    tags=["session-04", "demo"],
) as dag:

    SQLExecuteQueryOperator(
        task_id="load_day",
        sql="load_day.sql",
        split_statements=True,
        autocommit=True,
    )
