"""
Slack alert callbacks for Airflow.

Set the Airflow Variable 'slack_webhook_url' to your Incoming Webhook URL.
If the variable is missing, callbacks log a warning and do nothing — safe for
local development without a real Slack workspace.
"""

from __future__ import annotations

import requests
from airflow.models import Variable


def _post_slack(text: str) -> None:
    url = Variable.get("slack_webhook_url", default_var=None)
    if not url:
        print(f"[notify] slack_webhook_url not set — skipping. Message would be: {text}")
        return
    try:
        resp = requests.post(url, json={"text": text}, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[notify] Slack post failed (non-fatal): {exc}")


def notify_on_failure(context: dict) -> None:
    """on_failure_callback: posts a red-circle alert to Slack."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    logical_date = context.get("logical_date", "unknown")
    _post_slack(
        f":red_circle: *{dag_id}* › `{task_id}` failed\n"
        f"Run: `{run_id}` | Date: `{logical_date}`"
    )


def notify_on_sla_miss(dag, _task_list, _blocking_task_list, slas, _blocking_tis) -> None:
    """sla_miss_callback: posts a warning to Slack when a task exceeds its SLA."""
    missed = ", ".join(f"`{sla.task_id}`" for sla in slas)
    _post_slack(f":warning: *{dag.dag_id}* SLA missed — tasks: {missed}")
