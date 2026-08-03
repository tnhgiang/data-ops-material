import logging
from datetime import datetime
from pathlib import Path

import duckdb
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from dataops_common.ge_checkpoint import run_ge_checkpoint

logger = logging.getLogger(__name__)

DEMO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = DEMO_DIR / "data"
GE_DIR = DEMO_DIR / "great_expectations"
WAREHOUSE = DATA_DIR / "session_09.duckdb"


@dag(
    dag_id="session_09_order_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["session-09", "monitoring", "data-quality"],
)
def order_pipeline():
    @task
    def load_bronze() -> str:
        dataset = Variable.get("orders_dataset", default_var="orders_clean.parquet")
        source = DATA_DIR / dataset
        con = duckdb.connect(str(WAREHOUSE))
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute(
            "CREATE OR REPLACE TABLE bronze.orders AS SELECT * FROM read_parquet(?)",
            [str(source)],
        )
        con.close()
        logger.info("Loaded bronze.orders from %s", source)
        return dataset

    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_silver",
        conn_id="session_09_duckdb",
        sql="sql/session_09_order_pipeline/bronze2silver__orders.sql",
    )

    @task
    def ge_checkpoint_silver() -> None:
        run_ge_checkpoint(
            warehouse=WAREHOUSE,
            ge_dir=GE_DIR,
            suite_name="orders_silver_suite",
            sql="SELECT * FROM silver.orders",
        )

    load_gold = SQLExecuteQueryOperator(
        task_id="load_gold",
        conn_id="session_09_duckdb",
        sql="sql/session_09_order_pipeline/silver2gold__orders.sql",
    )

    recon_row_count = SQLCheckOperator(
        task_id="recon_row_count",
        conn_id="session_09_duckdb",
        sql="sql/session_09_order_pipeline/recon_row_count.sql",
    )

    bronze = load_bronze()
    bronze >> transform_silver >> ge_checkpoint_silver() >> load_gold >> recon_row_count


order_pipeline()
