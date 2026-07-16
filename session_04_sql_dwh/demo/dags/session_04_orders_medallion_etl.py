from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)

from dataops_common.notifications import notify_on_failure, notify_on_sla_miss

default_args = {
    "owner": "dataops-class",
    "conn_id": "duckdb_default",
    # DuckDB is single-writer: this 1-slot pool serializes warehouse access across
    # all session-04 DAGs so two runs never fight over the file lock.
    "pool": "duckdb_pool",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
}

with DAG(
    dag_id="session_04_orders_medallion_etl",
    description="Reference Medallion pipeline on DuckDB (operators + external .sql)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=notify_on_sla_miss,
    template_searchpath="/opt/airflow/dags/session_04/sql/orders_medallion_etl",
    tags=["session-04", "sql", "duckdb", "medallion"],
) as dag:

    # split_statements: run each ; statement in the file (incl. BEGIN/COMMIT).
    load_opts = {"split_statements": True, "autocommit": True}

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema", 
        sql="create_schema.sql", 
        **load_opts
    )
    load_bronze = SQLExecuteQueryOperator(
        task_id="load_bronze", 
        sql="load_bronze.sql", 
        **load_opts
    )
    validate_to_silver = SQLExecuteQueryOperator(
        task_id="validate_to_silver", 
        sql="validate_to_silver.sql",
        sla=timedelta(hours=1), 
        **load_opts,
    )
    build_dim_customer = SQLExecuteQueryOperator(
        task_id="build_dim_customer", 
        sql="build_dim_customer.sql", 
        **load_opts
    )
    build_fact_sales = SQLExecuteQueryOperator(
        task_id="build_fact_sales", 
        sql="build_fact_sales.sql", 
        **load_opts
    )
    # SQLCheckOperator fails the task if any column in reconcile.sql's row is falsy.
    reconcile = SQLCheckOperator(
        task_id="reconcile", 
        sql="reconcile.sql")

    (
        create_schema
        >> load_bronze
        >> validate_to_silver
        >> build_dim_customer
        >> build_fact_sales
        >> reconcile
    )
