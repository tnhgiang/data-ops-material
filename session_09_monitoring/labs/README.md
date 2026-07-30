# Session 09 — Labs

Two hands-on labs on the monitoring stack.

| Lab | Topic | Time |
|---|---|---|
| Lab 1 | Grafana Alert + Slack Webhook | 20 min |
| Lab 2 | Great Expectations Data Quality | 20 min |

## Prerequisites

```bash
pip install -r session_09_monitoring/requirements.txt

# Lab 1 needs BOTH stacks: session_07 runs Airflow (and the pipeline DAG), session_09 runs monitoring.
cd session_07_security   && docker-compose up -d --build
cd session_09_monitoring && docker-compose up -d
docker-compose ps          # session_09 services must be "Up"
```

- Lab 1 needs both stacks running and the `session_09_order_pipeline` DAG (deployed into the
  session_07 Airflow) to have run at least once, so `af_agg_*` metrics exist in Prometheus.
- Lab 2 runs entirely on your laptop with the committed parquet files — no Docker needed.

---

## Lab 1 — Grafana Alert + Slack Webhook

**Ticket (OPS-231):** Ops has no early warning when the orders pipeline slows down. We only find out
when downstream reports are late. Add an alert that pings our Slack channel when the pipeline task
runtime crosses SLA, so on-call can react before reports break.

Demo §4 in [`../demo/README.md`](../demo/README.md) shows the exact click-path for creating a Grafana
alert and wiring a Slack contact point — follow that flow, with these values:

| Field | Value |
|---|---|
| Alert name | `DAG Duration High — orders` |
| Query (Prometheus) | p95 of `af_agg_dag_task_duration` for `dag_id="session_09_order_pipeline"` |
| Condition | p95 > `300` (seconds) |
| FOR | `2m` |
| Label | `severity = critical` |
| Annotation | `runbook = <any URL>` |
| Contact point | Slack webhook (mentor provides a test URL) |

**Steps:**

1. Grafana (`http://localhost:3000`, `admin`/`admin`) → **Alerting → Contact points → Add** — type
   `Slack`, paste the test webhook URL, **Test** it (message must arrive in the channel).
2. **Alerting → Alert rules → New alert rule** — set the query, condition, and FOR from the table above.
3. Save into a folder named `DataOps Alerts`.

**Done when:**

- [ ] `docker-compose ps` shows every service `Up`, and Prometheus target `airflow` is **UP** at
      `http://localhost:9090/targets`.
- [ ] The Slack **Test** from the contact point arrived in the channel.
- [ ] The alert rule exists with condition `p95 > 300` and `FOR = 2m`.

---

## Lab 2 — Great Expectations Data Quality

**Ticket (DQ-118):** Finance reported orders with **negative amounts** and **missing order IDs**
reaching their dashboards last week. Before we load anything to gold, we need a data-quality gate that
rejects a batch like that. Build the expectation suite that would have caught it.

You are given a starter suite with two weak expectations that pass on *any* data, and a runner that
validates a parquet file against that suite and prints, per expectation, the % of rows that failed.

**Files:**

| File | Purpose |
|---|---|
| `labs/jobs/orders_lab_suite.json` | The expectation suite — **you edit this** |
| `labs/jobs/run_checkpoint.py` | The runner — do **not** edit; run it to check your suite |

**The broken batch** is `demo/data/orders_broken.parquet`. Its columns are:

| Column | Type | Known issue in this batch |
|---|---|---|
| `order_id` | string | some rows null; some duplicated |
| `customer_id` | string | — |
| `amount` | float | some rows negative |
| `status` | string | should only be pending / paid / cancelled / refunded |
| `order_date` | date | — |

**What to do (run everything from `session_09_monitoring/`):**

1. Run the starter suite against the broken batch and watch it **pass** — proof it is too weak:
   ```bash
   python labs/jobs/run_checkpoint.py
   ```
2. Open `labs/jobs/orders_lab_suite.json` and add expectations that catch the issues in the table
   above. Add them to the `expectations` list, each as an object with `expectation_type`, `kwargs`,
   and `meta: {}` (copy the shape of the two starter entries).
3. Re-run the runner against the broken batch — it must now report failures:
   ```bash
   python labs/jobs/run_checkpoint.py
   ```
4. Confirm the same suite passes on a clean batch:
   ```bash
   python labs/jobs/run_checkpoint.py demo/data/orders_clean.parquet
   ```

**Done when:**

- [ ] `orders_lab_suite.json` has **at least 5** expectations.
- [ ] Against `orders_broken.parquet` the runner reports **at least 3** failed expectations, and the
      log shows a non-zero "% rows failed" for each.
- [ ] Against `orders_clean.parquet` the runner reports **0** failed (exit code 0).

**Stretch — wire it into the DAG:** the demo DAG task `ge_checkpoint_bronze` in
[`../demo/dags/session_09_order_pipeline.py`](../demo/dags/session_09_order_pipeline.py) shows the
exact pattern for failing a task and firing a Slack alert when a suite fails. Add an equivalent task
that runs your `orders_lab_suite` after `load_bronze`.
