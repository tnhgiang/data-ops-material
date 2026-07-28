import logging
import os
from datetime import datetime

import great_expectations as gx
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from demo.jobs.notify import send_slack_alert

logger = logging.getLogger(__name__)
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK_URL"]


@dag(
    dag_id="session_09_order_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
)
def order_pipeline():
    @task
    def load_bronze(ds: str) -> str:
        bronze_path = f"s3a://bronze/orders/date={ds}/orders.parquet"
        logger.info("Loaded bronze: %s", bronze_path)
        return bronze_path

    @task
    def ge_checkpoint_bronze(bronze_path: str) -> None:
        context = gx.get_context()
        results = context.run_checkpoint(
            checkpoint_name="orders_bronze_checkpoint",
            batch_request={"path": bronze_path},
        )
        if not results["success"]:
            failed = [
                r["expectation_config"]["expectation_type"]
                for r in results.list_validation_results()[0]["results"]
                if not r["success"]
            ]
            msg = f"GE checkpoint FAILED on `{bronze_path}`\nFailed: {', '.join(failed)}"
            send_slack_alert(SLACK_WEBHOOK, msg, level="critical")
            raise ValueError(msg)

        logger.info("GE checkpoint passed: %s", bronze_path)

    recon_row_count = SQLCheckOperator(
        task_id="recon_row_count",
        conn_id="duckdb_default",
        sql="sql/session_09_order_pipeline/recon_row_count.sql",
    )

    bronze_path = load_bronze()
    ge_checkpoint_bronze(bronze_path) >> recon_row_count


order_pipeline()
