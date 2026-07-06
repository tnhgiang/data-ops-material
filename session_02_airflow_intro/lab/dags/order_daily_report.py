"""Downstream report — waits for order_daily_etl via ExternalTaskSensor."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from common.idempotent_store import read_records
from common.notifications import notify_on_failure

default_args = {
    "owner": "dataops-class",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
}


@dag(
    dag_id="order_daily_report",
    description="Downstream report — runs after order_daily_etl succeeds",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["session-03", "dependency"],
)
def order_daily_report():

    wait_for_etl = ExternalTaskSensor(
        task_id="wait_for_order_etl",
        external_dag_id="order_daily_etl",
        external_task_id="reconcile",
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    @task
    def build_daily_summary(logical_date: datetime | None = None) -> dict:
        orders = read_records("curated", "orders", logical_date)
        total_amount = sum(row.get("amount_usd", 0) for row in orders)
        high_value = sum(1 for row in orders if row.get("is_high_value"))
        summary = {
            "date": logical_date.strftime("%Y-%m-%d"),
            "order_count": len(orders),
            "total_amount_usd": round(total_amount, 2),
            "high_value_orders": high_value,
        }
        print(f"Daily summary: {summary}")
        return summary

    wait_for_etl >> build_daily_summary()


order_daily_report()
