# Session 3 Lab — Setup Guide

**Master Class DataOps for Modern Data Platforms · Session 3/11**

Session 3 reuses the Airflow stack from session 2 — there is no separate stack to start here. This guide wires the session 3 DAGs into that stack and does the one-time setup they need.

## 1. Prerequisites

- The session 2 stack works (`docker compose ps` all healthy). See `session_02_airflow_intro/lab/README.md`.
- Ports `8080` (Airflow) and `5555` (Flower) reachable.

## 2. How the DAGs reach Airflow

The session 2 `docker-compose.yaml` already mounts session 3 into the running containers:

| Host path | Container path | Purpose |
|---|---|---|
| `plugins/` (repo root) | `/opt/airflow/plugins` | shared `dataops_common` package |
| `session_03_airflow_dag/lab/dags` | `/opt/airflow/dags/session_03` | the 3 session 3 DAGs |
| `session_03_airflow_dag/lab/data` | `/opt/airflow/data` | partition output |

Restart the stack once so the new mounts take effect:

```bash
cd session_02_airflow_intro/lab
docker compose --profile flower up -d

# confirm the DAGs are mounted
docker compose exec airflow-scheduler ls /opt/airflow/dags/session_03
# → campaign_alerts.py  campaign_daily_metrics.py  campaign_spend_report.py
```

Editing a DAG under `session_03_airflow_dag/lab/dags/` is picked up automatically (the scheduler rescans every ~30s) — no restart needed.

## 3. One-time setup

`campaign_daily_metrics` uses a Pool, and optionally a Connection and Variables. Create them once in the Airflow UI (`http://localhost:8080`, `airflow` / `airflow`).

| Type | Name | Value | Required? |
|---|---|---|---|
| Pool | `campaign_db_pool` | 2 slots | **Yes** — the `publish` task won't schedule without it |
| Variable | `daily_event_volume` | e.g. `30` | No — defaults to 30 |
| Variable | `daily_budget_usd` | e.g. `5.0` | No — used by `campaign_spend_report`, defaults to 5.0 |
| Variable | `min_ctr` | e.g. `0.15` | No — used by `campaign_alerts` (the exercise), defaults to 0.15 |
| Connection | `ad_warehouse` | any host (e.g. Conn Type `Postgres`, host `dwh`) | No — the task logs a fallback if missing |
| Variable | `slack_webhook_url` | your Slack Incoming Webhook URL | No — alerts just log if missing |

CLI alternative:

```bash
cd session_02_airflow_intro/lab
docker compose exec airflow-scheduler airflow pools set campaign_db_pool 2 "Session 3 campaign publish"
docker compose exec airflow-scheduler airflow variables set daily_event_volume 30
```

## 4. Run & verify

1. In the UI, unpause `campaign_daily_metrics` and trigger a run (or pick a logical date).
2. Watch `ingest → rollup → publish → reconcile` go green in Graph view.
3. Check the partition output on the host:

```bash
cat session_03_airflow_dag/lab/data/published/campaign_metrics/dt=*/data.json
# → one row per campaign: impressions, clicks, spend_usd, ctr
```

4. Enable `campaign_spend_report` — its `wait_for_metrics` sensor stays in `up_for_reschedule` until `campaign_daily_metrics` finishes that date, then `flag_over_budget` runs.

## 5. DAGs in this lab

| DAG | What it shows |
|---|---|
| `campaign_daily_metrics` | Idempotent partition writes, deterministic data, retries + timeouts, SLA + callbacks, Pool, Variable, Connection |
| `campaign_spend_report` | Cross-DAG dependency via `ExternalTaskSensor` (reschedule mode) |
| `campaign_alerts` | **Starter for you to finish** — see the Exercise below |

## 6. Exercise — finish `campaign_alerts`

`campaign_alerts.py` is a starter DAG with the wiring done and the logic left as
TODOs. Your job: complete it so it flags campaigns whose CTR is below a
threshold, downstream of `campaign_daily_metrics`. Read `campaign_spend_report.py`
first — it is the same pattern with a different metric.

Steps (each maps to a `# TODO` in the file):

1. Read the Variable `min_ctr` (default `0.15`) as a float.
2. Read the published `campaign_metrics` partition for `logical_date` with `read_partition("published", "campaign_metrics", logical_date)`.
3. Keep the campaigns whose `ctr` is below `min_ctr`, and sum their `spend_usd` (the "wasted" spend).
4. Build a report dict: `date`, `min_ctr`, `low_ctr_campaigns` (list of names), `wasted_spend_usd`.
5. Write it to a partition with `write_partition("reports", "campaign_low_ctr", logical_date, [report])` — one row, overwrites on rerun (idempotent).
6. Log the report with `log.info(...)` and return it.
7. Wire the dependency so `flag_low_ctr` runs only after `wait_for_metrics` (`wait_for_metrics >> alert`).

Test it:

1. Run `campaign_daily_metrics` for a date first (it produces the metrics you read).
2. Unpause `campaign_alerts`, trigger the same date.
3. Check the output on the host:
   ```bash
   cat session_03_airflow_dag/lab/data/reports/campaign_low_ctr/dt=*/data.json
   ```

Acceptance criteria:
- The DAG runs green (no `NotImplementedError`).
- `flag_low_ctr` runs only after `wait_for_metrics` succeeds.
- With the default sample data, `brand_awareness` (CTR 0.10) is flagged; `prospecting` (0.30) and `retargeting` (0.50) are not.
- Triggering the same date twice leaves exactly one row in the partition (idempotent).

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `campaign_*` DAGs missing in UI | Stack not restarted after mounts were added | `docker compose --profile flower up -d` in `session_02_airflow_intro/lab` |
| `ModuleNotFoundError: dataops_common` | Plugins mount not applied | Confirm `../../plugins` is mounted; restart the stack |
| `publish` task stuck, never runs | Pool `campaign_db_pool` doesn't exist | Create it (section 3) |
| Import error on a DAG | Editing while the scheduler rescans | Wait ~30s; check the DAG's import-error banner in the UI |
| No data under `lab/data/` | Data volume not mounted | Confirm `session_03_airflow_dag/lab/data` maps to `/opt/airflow/data` |
