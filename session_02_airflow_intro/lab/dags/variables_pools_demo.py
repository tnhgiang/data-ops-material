"""
Demo DAG for Variables, Connections, Pools, and XCom.

Set these in Airflow UI before running:
- Admin → Variables: notification_email = team@example.com
- Admin → Pools: create pool "reporting_pool" with 1 slot
- Admin → Connections (optional): postgres_default for demo logging
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from common.notifications import notify_on_failure, notify_on_sla_miss

default_args = {
    "owner": "dataops-class",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
    "sla_miss_callback": notify_on_sla_miss
}


@dag(
    dag_id="variables_pools_demo",
    description="Explore Variable, Connection, Pool, and XCom patterns",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["session-03", "demo"],
)
def variables_pools_demo():

    @task
    def read_runtime_config() -> dict:
        email = Variable.get("notification_email", default_var="ops@example.com")
        env_name = Variable.get("environment", default_var="dev")
        config = {"notification_email": email, "environment": env_name}
        print(f"Runtime config: {config}")
        return config

    @task
    def log_connection_host(config: dict) -> str:
        raise ValueError(
            "Just for testing the Slack Alert."
        )
        # try:
        #     conn = BaseHook.get_connection("postgres_default")
        #     host = conn.host or "not-configured"
        # except Exception:
        #     host = "connection-not-found"
        # message = (
        #     f"Would notify {config['notification_email']} "
        #     f"from {config['environment']} using DB host {host}"
        # )
        # print(message)
        # return message

    ping = BashOperator(
        task_id="ping",
        bash_command='echo "Pool demo starting"',
        pool="reporting_pool",
    )

    config = read_runtime_config()
    message = log_connection_host(config)
    ping >> config >> message


variables_pools_demo()
