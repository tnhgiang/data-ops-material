"""Daily order ETL — idempotent partition overwrite, safe to rerun and backfill."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from common.idempotent_store import read_records, reconcile_counts, write_records
from common.sample_data import enrich_orders, generate_orders

DEFAULT_ORDER_COUNT = int(Variable.get("order_daily_count", default_var=5))


default_args = {
    "owner": "dataops-class",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="order_daily_etl",
    description="Idempotent daily order pipeline — safe to rerun and backfill",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["session-03", "idempotent", "etl"],
)
def order_daily_etl():

    @task(execution_timeout=timedelta(minutes=5))
    def extract_orders(logical_date: datetime | None = None) -> list[dict]:
        orders = generate_orders(logical_date, count=DEFAULT_ORDER_COUNT)
        print(f"Extracted {len(orders)} orders for {logical_date.date()}")
        return orders

    @task(execution_timeout=timedelta(minutes=5))
    def load_raw(orders: list[dict], logical_date: datetime | None = None) -> int:
        path = write_records("raw", "orders", logical_date, orders)
        print(f"Wrote raw partition: {path}")
        return len(orders)

    @task(execution_timeout=timedelta(minutes=5))
    def transform_and_curate(_raw_count: int, logical_date: datetime | None = None) -> int:
        raw_orders = read_records("raw", "orders", logical_date)
        curated = enrich_orders(raw_orders)
        path = write_records("curated", "orders", logical_date, curated)
        print(f"Wrote curated partition: {path}")
        return len(curated)

    @task(execution_timeout=timedelta(minutes=5))
    def reconcile(_curated_count: int, logical_date: datetime | None = None) -> None:
        raw_count = len(read_records("raw", "orders", logical_date))
        curated_count = len(read_records("curated", "orders", logical_date))
        reconcile_counts(raw_count, curated_count, label=f"orders/{logical_date.date()}")
        print(f"Reconciliation OK: {raw_count} raw == {curated_count} curated")

    orders = extract_orders()
    raw_count = load_raw(orders)
    curated_count = transform_and_curate(raw_count)
    reconcile(curated_count)


order_daily_etl()
