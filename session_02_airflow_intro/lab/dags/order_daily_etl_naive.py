"""
Naive order pipeline — each rerun APPENDS to the same file, so backfills
duplicate data. Lab exercise: refactor it toward idempotency.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from common.idempotent_store import DATA_ROOT
from common.sample_data import generate_orders

NAIVE_FILE = DATA_ROOT / "naive" / "orders.jsonl"


default_args = {
    "owner": "dataops-class",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="order_daily_etl_naive",
    description="NOT-Optimized pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["session-03", "exercise", "anti-pattern"],
)
def order_daily_etl_naive():

    @task
    def extract_and_append(logical_date: datetime | None = None) -> int:
        orders = generate_orders(logical_date)
        NAIVE_FILE.parent.mkdir(parents=True, exist_ok=True)

        with NAIVE_FILE.open("a", encoding="utf-8") as handle:
            for order in orders:
                handle.write(json.dumps(order) + "\n")

        print(f"Appended {len(orders)} orders to {NAIVE_FILE}")
        return len(orders)

    @task
    def count_total_rows() -> int:
        if not NAIVE_FILE.exists():
            return 0
        with NAIVE_FILE.open(encoding="utf-8") as handle:
            total = sum(1 for _ in handle)
        print(f"Total rows in naive file: {total}")
        return total

    extract_and_append() >> count_total_rows()


order_daily_etl_naive()
