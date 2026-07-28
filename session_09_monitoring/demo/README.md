# Session 09 Demo — Monitoring, Alerting & Data Quality

Stack: Prometheus + StatsD Exporter + Grafana + Great Expectations + Slack Webhook + Airflow.

## 1. Start the stack

```bash
cd session_09_monitoring
docker-compose up -d
docker-compose ps
```

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)
- StatsD Exporter metrics: `http://localhost:9102/metrics`

## 2. Configure Airflow to emit metrics via StatsD

Add to Airflow environment (docker-compose or airflow.cfg):

```yaml
AIRFLOW__METRICS__STATSD_ON: "True"
AIRFLOW__METRICS__STATSD_HOST: "statsd-exporter"
AIRFLOW__METRICS__STATSD_PORT: "8125"
AIRFLOW__METRICS__STATSD_PREFIX: "airflow"
```

Verify at `http://localhost:9090/targets` — the `airflow` job must be UP.

## 3. Import Airflow dashboard into Grafana

1. Connections → Data Sources → Add Prometheus (`http://prometheus:9090`)
2. Dashboards → Import → upload `grafana/dashboards/airflow_overview.json`

Key panels:

| Panel | Metric | What to look for |
|---|---|---|
| DAG Success Rate | `rate(airflow_dagrun_duration_success[5m])` | Drop → recent failures |
| Task Duration p95 | `histogram_quantile(0.95, rate(airflow_task_duration_bucket[10m]))` | Spike → slow tasks |
| Scheduler Heartbeat | `airflow_scheduler_heartbeat` | Must stay > 0 |
| Executor Queue | `airflow_executor_queued_tasks` | Rising queue → executor bottleneck |

> Flow: Airflow → UDP → StatsD Exporter → HTTP → Prometheus scrape → Grafana pull. Debug from right to left when a metric is missing.

## 4. Run the pipeline — Demo 2

```bash
# Trigger with clean data
airflow dags trigger session_09_order_pipeline

# Trigger with broken data to show GE failure + Slack alert
cp demo/data/orders_broken.parquet /tmp/orders_test.parquet
airflow dags trigger session_09_order_pipeline
```

Observe:
- `ge_checkpoint_bronze` fails
- Slack receives alert with list of failed expectations
- Data Docs at `demo/great_expectations/uncommitted/data_docs/`

## 5. GE Expectation Suite

Suite defined in `demo/great_expectations/expectations/orders_bronze_suite.json`.

To regenerate from clean data:

```python
import great_expectations as gx

context = gx.get_context()
suite = context.add_expectation_suite("orders_bronze_suite")
validator = context.sources.pandas_default.read_parquet(
    path="demo/data/orders_clean.parquet",
    asset_name="orders_bronze",
    expectation_suite=suite,
)

validator.expect_column_values_to_not_be_null(column="order_id")
validator.expect_column_values_to_be_unique(column="order_id")
validator.expect_column_values_to_be_between(column="amount", min_value=0, max_value=1_000_000)
validator.expect_column_values_to_not_be_null(column="order_date")
validator.expect_column_values_to_be_in_set(
    column="status", value_set=["pending", "paid", "cancelled", "refunded"]
)
validator.expect_table_row_count_to_be_between(min_value=1)

validator.save_expectation_suite()
```

## Demo simplifications

- Slack webhook is a test URL — real deployment uses a proper channel with on-call rotation.
- GE context uses local filesystem — production uses a shared backend (S3 + shared Data Docs site).
- StatsD exporter runs as a sidecar — production typically uses the Prometheus push gateway or a managed metrics service.
