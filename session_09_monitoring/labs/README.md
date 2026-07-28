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
# Start the stack
cd session_09_monitoring
docker-compose up -d

# Verify
docker-compose ps
```

---

## Lab 1 — Grafana Alert + Slack Webhook

The Ops team needs to be notified on Slack when the `session_09_order_pipeline` task duration exceeds SLA. Set up a Grafana alert rule that fires into a Slack channel.

**Requirements:**

| Field | Value |
|---|---|
| Alert name | `DAG Duration High — orders` |
| Condition | Task duration p95 > 300s |
| FOR | 2m |
| Severity label | `critical` |
| Annotation `runbook` | Any URL |
| Contact point | Slack webhook (mentor provides test URL) |

**Acceptance criteria:**

- [ ] `docker-compose ps` — all services Up
- [ ] Prometheus target `airflow` is UP at `http://localhost:9090/targets`
- [ ] Grafana dashboard imported, at least 2 panels show data
- [ ] Alert rule exists with correct condition and FOR duration
- [ ] Slack test message received in channel

---

## Lab 2 — GE Data Quality Checkpoint

The data engineering team received a complaint that some orders in the pipeline had invalid amounts and missing IDs. Write a Great Expectations suite that catches data quality issues before they reach silver, and wire it into the DAG.

**Dataset:** `demo/data/orders_broken.parquet`

| Column | Type |
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
