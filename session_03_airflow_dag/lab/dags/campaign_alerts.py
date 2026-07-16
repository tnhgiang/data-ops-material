"""
STARTER DAG — you complete the body. See the "Exercise" section in lab/README.md.

Flags campaigns whose CTR is below a threshold, downstream of
campaign_daily_metrics. The pattern mirrors campaign_spend_report — read that
DAG first, then fill in the TODOs in flag_low_ctr.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from dataops_common.notifications import notify_on_failure
from dataops_common.storage import read_partition, write_partition

log = logging.getLogger(__name__)

default_args = {
    "owner": "growth-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
}


@dag(
    dag_id="session_03_campaign_alerts",
    description="STARTER — flag low-CTR campaigns (complete the TODOs)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["session-03", "ads", "exercise"],
)
def campaign_alerts():

    wait_for_metrics = ExternalTaskSensor(
        task_id="wait_for_metrics",
        external_dag_id="session_03_campaign_daily_metrics",
        external_task_id="reconcile",
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    @task
    def flag_low_ctr(logical_date: datetime | None = None) -> dict:
        # TODO 1: read the Variable "min_ctr" (default 0.15) as a float
        # TODO 2: read the published "campaign_metrics" partition for logical_date
        # TODO 3: keep campaigns with ctr < min_ctr; sum their spend_usd (wasted spend)
        # TODO 4: build a report dict: date, min_ctr, low_ctr_campaigns, wasted_spend_usd
        # TODO 5: write it to the "reports" / "campaign_low_ctr" partition (idempotent)
        # TODO 6: log the report with log.info(...) and return it
        raise NotImplementedError("Complete flag_low_ctr — see the TODOs and lab/README.md")

    alert = flag_low_ctr()

    # TODO 7: make `alert` run only after `wait_for_metrics` (hint: use >>)


campaign_alerts()
