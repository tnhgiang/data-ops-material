# Session 09 — Labs

Two hands-on labs on the monitoring stack (`session_09_monitoring/docker-compose.yml` must be running and `session_09_order_pipeline` must have completed at least once).

| Lab | Topic | Time |
|---|---|---|
| Lab 1 | Grafana Alert + Slack Webhook | 20 min |
| Lab 2 | GE Data Quality Checkpoint | 20 min |

## Rules

1. Each finding requires: **(a)** exact location, **(b)** specific risk or business impact, **(c)** working fix, **(d)** evidence of retest.
2. Do not modify demo infrastructure unless the lab explicitly requires it.

## Running lab code

```bash
pip install -r session_09_monitoring/requirements.txt

# Lab 1 needs BOTH stacks: session_07 runs Airflow (and the pipeline DAG), session_09 runs monitoring.
cd session_07_security   && docker compose up -d --build
cd session_09_monitoring && docker compose up -d
docker compose ps          # session_09 services must be "Up"
```

- Lab 1 needs both stacks running and the `session_09_order_pipeline` DAG (deployed into the
  session_07 Airflow) to have run at least once, so `af_agg_*` metrics exist in Prometheus.
- Lab 2 runs entirely on your laptop with the committed parquet files — no Docker needed.

---

## Lab 1 — Grafana Alert + Slack Webhook

The Ops team needs to be notified on Slack when the `session_09_order_pipeline` task duration exceeds SLA. Set up a Grafana alert rule that fires into a Slack channel.

Demo #4 in [`../demo/README.md`](../demo/README.md) shows the exact click-path for creating a Grafana
alert and wiring a Slack contact point — follow that flow, with these values:

| Field | Value |
|---|---|
| Alert name | `DAG Duration High — orders` |
| Condition | Task duration p95 > 300s |
| FOR | 2m |
| Severity label | `critical` |
| Annotation `runbook` | Any URL |
| Contact point | Slack webhook (mentor provides test URL) |

**Acceptance criteria:**

- [ ] `docker compose ps` shows every service `Up`, and Prometheus target `airflow` is **UP** at
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
| `order_id` | string |
| `customer_id` | string |
| `amount` | float |
| `status` | string |
| `order_date` | date |

**Acceptance criteria:**

- [ ] `expectations/orders_lab_suite.json` has at least 5 expectations
- [ ] Running checkpoint against `orders_broken.parquet` fails at least 3 expectations
- [ ] Data Docs shows correct % rows failed per expectation
- [ ] Task `ge_checkpoint_orders` is in the DAG graph, after `load_bronze`
- [ ] When data is broken: task fails and Slack receives alert
- [ ] When data is clean: task passes and pipeline continues
