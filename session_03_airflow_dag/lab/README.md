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
| Connection | `ad_warehouse` | Conn Type `Postgres`, host `dwh` (anything works) | No — the task logs a fallback if missing |
| Variable | `slack_webhook_url` | your Slack Incoming Webhook URL | No — alerts just log if missing |

CLI alternative:

```bash
cd session_02_airflow_intro/lab
docker compose exec airflow-scheduler airflow pools set campaign_db_pool 2 "Session 3 campaign publish"
docker compose exec airflow-scheduler airflow variables set daily_event_volume 30
docker compose exec airflow-scheduler airflow connections add ad_warehouse \
  --conn-type postgres --conn-host dwh
```

To create the `ad_warehouse` Connection from the UI instead: **Admin → Connections → +** → `Conn Id` = `ad_warehouse`, `Conn Type` = `Postgres`, `Host` = `dwh` (or anything) → **Save**.

Note what this Connection is actually for: `publish` only calls `BaseHook.get_connection("ad_warehouse")` and reads `.host` for a log line — it never opens a real database connection. You don't need a working Postgres behind it. The point being demonstrated is the pattern itself: look up credentials by `conn_id`, never hardcode them in the DAG.

### Setting up `slack_webhook_url` (optional — enables real Slack alerts)

Without this Variable, `on_failure_callback` and `sla_miss_callback` just log the message they would have sent — the DAGs run the same either way. To see a real alert land in Slack:

1. In Slack: **create an app** (or reuse one) at [api.slack.com/apps](https://api.slack.com/apps) → **Incoming Webhooks** → toggle **Activate Incoming Webhooks** on → **Add New Webhook to Workspace** → pick a channel → **Allow**.
2. Copy the generated URL (`https://hooks.slack.com/services/...`).
3. Set it as an Airflow Variable — **Admin → Variables → +** → `Key` = `slack_webhook_url`, `Val` = the URL you copied → **Save**. Or via CLI:

```bash
docker compose exec airflow-scheduler airflow variables set slack_webhook_url 'https://hooks.slack.com/services/...'
```

The URL only lives in the Airflow Variable — never put it in DAG code or commit it to the repo.

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

## 7. Demo — a real alert from a real failure

Fixed, single-path script, run against the real reference pipeline — not a throwaway DAG. It doubles as the answer to "why does `reconcile` even exist?": you corrupt the exact thing it guards, then watch it catch the corruption, retry, fail, and alert Slack — for real. Every step below has been run end to end.

> **Use `airflow dags trigger`, NOT `airflow dags backfill`.** Clearing a task inside a *backfill* run is **not** picked up by the scheduler — the task sits stuck with no status and the demo dies. A triggered (manual) run is scheduler-managed, so a cleared task reruns normally. This is the one thing that will break the demo if you get it wrong.

**Before class:** `slack_webhook_url` Variable set (section 3) with a **real** webhook, and the Slack channel open next to the Airflow UI (Grid view of `campaign_daily_metrics`). The demo uses date `2025-06-01` — one no exercise touches. Run the `docker compose exec` commands from `session_02_airflow_intro/lab`.

1. **Run the pipeline clean** for the date:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger campaign_daily_metrics -e 2025-06-01
   ```
   In Grid view the new run goes all green (`ingest → rollup → publish → reconcile`) in ~40s.

2. **Corrupt the published partition** — simulating a downstream process dropping one impression *after* `publish` ran. From the repo root:
   ```bash
   python3 - <<'PY'
   import json, pathlib
   p = pathlib.Path("session_03_airflow_dag/lab/data/published/campaign_metrics/dt=2025-06-01/data.json")
   rows = json.loads(p.read_text())
   rows[0]["impressions"] -= 1
   p.write_text(json.dumps(rows, indent=2, sort_keys=True))
   print("corrupted:", rows[0]["campaign"], "→ impressions now", rows[0]["impressions"])
   PY
   ```
   Only `published` changes; `raw` and `curated` stay correct — `published` is the layer `reconcile` checks.

3. **Clear only `reconcile`.** In the UI: Grid view → the `2025-06-01` run → click the `reconcile` box → **Clear** → confirm. (This is also the live "clear one task, only that task reruns" example.) CLI equivalent:
   ```bash
   docker compose exec airflow-scheduler airflow tasks clear campaign_daily_metrics -t reconcile -s 2025-06-01 -e 2025-06-01 -y
   ```

4. `reconcile` reruns, recomputes from the corrupted partition, the counts don't match → it raises, and the task log shows the real error:
   ```
   ValueError: Reconciliation failed for ad_events/2025-06-01: source=30, target=29
   ```

5. **Narrate the retry window — don't wait in silence.** `reconcile` inherits `retries=2, retry_delay=1min`: it fails (state `up_for_retry`), waits ~1 min, retries, fails, retries once more, fails — **~2 minutes total** — then gives up (`failed`). Point at the attempt history in the UI; this is the retry behavior every task in the DAG has.

6. On the **final** failure `on_failure_callback` fires and posts to Slack:
   ```
   🔴 *campaign_daily_metrics* › `reconcile` failed
   Run: `manual__2025-06-01T00:00:00+00:00` | Date: `2025-06-01T00:00:00+00:00`
   ```
   You can confirm it fired in the `reconcile` task log — look for a `{notifications.py}` line (a successful post logs nothing; a failed post logs `Slack post failed (non-fatal): ...`).

7. **If no Slack message arrives**, read the `reconcile` task log (not the DAG health):
   - `slack_webhook_url not set — skipping` → Variable missing/empty; set it (section 3), redo step 3.
   - `Slack post failed (non-fatal): ...` → webhook URL is wrong, revoked, or the app/channel was removed; recreate it (section 3), redo step 3.

8. **Fix the data** — clear `publish` (it reruns `reconcile` downstream too):
   ```bash
   docker compose exec airflow-scheduler airflow tasks clear campaign_daily_metrics -t "publish|reconcile" -s 2025-06-01 -e 2025-06-01 -y
   ```
   `publish` rewrites the partition from `curated` (idempotent overwrite), `reconcile` passes, the run is green again. Say it out loud: recovering from a bad state here is just "rerun it" — that is the payoff of idempotency, not luck.

**SLA alerts (`sla_miss_callback`) are a separate mechanism and not part of this demo**: Airflow only evaluates SLA misses on *scheduled* runs, never on manual triggers or backfills — there's no way to force one on demand. Walk through the `sla=timedelta(...)` config and `notify_on_sla_miss` code instead of trying to trigger it live.

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `campaign_*` DAGs missing in UI | Stack not restarted after mounts were added | `docker compose --profile flower up -d` in `session_02_airflow_intro/lab` |
| `ModuleNotFoundError: dataops_common` | Plugins mount not applied | Confirm `../../plugins` is mounted; restart the stack |
| `publish` task stuck, never runs | Pool `campaign_db_pool` doesn't exist | Create it (section 3) |
| Import error on a DAG | Editing while the scheduler rescans | Wait ~30s; check the DAG's import-error banner in the UI |
| No data under `lab/data/` | Data volume not mounted | Confirm `session_03_airflow_dag/lab/data` maps to `/opt/airflow/data` |
