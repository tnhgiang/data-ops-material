import json
import logging
from datetime import datetime
from pathlib import Path

import duckdb
import great_expectations as gx
from airflow.decorators import dag, task
from airflow.models import Variable
from dataops_common.notifications import send_slack_alert
from great_expectations.core.expectation_suite import ExpectationSuite

logger = logging.getLogger(__name__)

DEMO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = DEMO_DIR / "data"
GE_DIR = DEMO_DIR / "great_expectations"
WAREHOUSE = DATA_DIR / "session_09.duckdb"
RECON_SQL = Path(__file__).resolve().parent / "sql/session_09_order_pipeline/recon_row_count.sql"


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

    @task
    def ge_checkpoint_bronze(dataset: str) -> str:
        con = duckdb.connect(str(WAREHOUSE), read_only=True)
        df = con.execute("SELECT * FROM bronze.orders").df()
        con.close()

        context = gx.get_context(mode="ephemeral")
        datasource = context.sources.add_pandas("runtime")
        asset = datasource.add_dataframe_asset("orders_bronze")
        batch_request = asset.build_batch_request(dataframe=df)
        suite_dict = json.loads((GE_DIR / "expectations" / "orders_bronze_suite.json").read_text())
        suite = context.add_expectation_suite(expectation_suite=ExpectationSuite(**suite_dict))
        validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
        result = validator.validate()
        if not result.success:
            failed = [
                r["expectation_config"]["expectation_type"]
                for r in result["results"]
                if not r["success"]
            ]
            msg = f"GE checkpoint FAILED on bronze.orders ({dataset})\nFailed: {', '.join(failed)}"
            send_slack_alert(msg, level="critical")
            raise ValueError(msg)

        logger.info("GE checkpoint passed on bronze.orders (%s)", dataset)
        return dataset

    @task
    def load_gold(dataset: str) -> None:
        con = duckdb.connect(str(WAREHOUSE))
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("CREATE OR REPLACE TABLE gold.fact_orders AS SELECT * FROM bronze.orders")
        con.close()
        logger.info("Loaded gold.fact_orders (%s)", dataset)

    @task
    def recon_row_count() -> None:
        con = duckdb.connect(str(WAREHOUSE), read_only=True)
        ok = con.execute(RECON_SQL.read_text()).fetchone()[0]
        con.close()
        if not ok:
            raise ValueError("Reconciliation failed: bronze and gold row counts differ")
        logger.info("Reconciliation passed: bronze and gold row counts match")

    validated = ge_checkpoint_bronze(load_bronze())
    load_gold(validated) >> recon_row_count()


order_pipeline()
