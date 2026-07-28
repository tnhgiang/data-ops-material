# Session 09 Demo — Monitoring, Alerting & Data Quality

Stack: Prometheus + StatsD Exporter + Grafana + Great Expectations + Slack Webhook + Airflow.

## Architecture

Session 09 runs as a second Docker Compose stack alongside Session 07:

```
session_07_security/    ← Airflow (webserver + scheduler + worker)
session_09_monitoring/  ← Prometheus + StatsD Exporter + Grafana
```

The two stacks are on separate Docker networks. Airflow emits metrics via UDP to
`host.docker.internal:8125`, which forwards to the StatsD Exporter on the host's port 8125.

```
Airflow → UDP → host.docker.internal:8125 → StatsD Exporter → Prometheus scrape → Grafana
```

Both stacks must be running before the demo:

```bash
cd session_07_security && docker-compose up -d
cd session_09_monitoring && docker-compose up -d
```

## 1. Start the monitoring stack

```bash
cd session_09_monitoring
docker-compose up -d
docker-compose ps
```

| Service | URL | Account | Password |
|---|---|---|---|
| Prometheus | `http://localhost:9090` | — | — |
| Grafana | `http://localhost:3000` | `admin` | `admin` |
| StatsD Exporter metrics | `http://localhost:9102/metrics` | — | — |
| Airflow | `http://localhost:8888` | `airflow` | `airflow` |

## 2. Airflow StatsD config

Already configured in `session_07_security/docker-compose.yml` under `x-airflow-common`:

```yaml
AIRFLOW__METRICS__STATSD_ON: 'true'
AIRFLOW__METRICS__STATSD_HOST: 'host.docker.internal'
AIRFLOW__METRICS__STATSD_PORT: '8125'
AIRFLOW__METRICS__STATSD_PREFIX: 'airflow'
```

> Note: StatsD Exporter listens on UDP port **9125** inside the container, mapped to host port **8125**
> (`8125:9125/udp`). Airflow sends to `host.docker.internal:8125` which routes to the exporter.

Verify Prometheus is scraping: `http://localhost:9090/targets` — `airflow` job must be **UP**.

## 3. Grafana dashboards

Dashboards are auto-provisioned from `grafana/dashboards/` on startup — no manual import needed.

| Dashboard | What it shows |
|---|---|
| Airflow cluster dashboard | Scheduler heartbeat, dagbag size, executor queue, task success/fail |
| Airflow DAG dashboard | Per-DAG duration, schedule delay, task breakdown |

Metrics use the `af_agg_*` prefix (mapped by `statsd/statsd.yaml`). Key ones:

| Metric | What to look for |
|---|---|
| `af_agg_scheduler_heartbeat` | Must stay > 0; drop → scheduler dead |
| `af_agg_ti_failures` | Any increment → task failed |
| `af_agg_executor_queued_tasks` | Rising → executor bottleneck |
| `af_agg_dagrun_duration_success` | Spike → pipeline slowing down |

> Flow: Airflow → UDP → StatsD Exporter → HTTP → Prometheus scrape (15s) → Grafana pull.
> Debug right to left when a metric is missing.

## 4. Grafana alert — DAG task failure

Alert rule `DAG Task Failure` is configured in folder `DataOps Alerts`:

- **Condition:** `increase(af_agg_ti_failures{airflow_id="airflow"}[5m]) > 0`
- **For:** 0s — fires immediately on first failure
- **Contact point:** Slack webhook
- **Repeat:** every 1 hour while still failing

When a GE checkpoint fails, the DAG task also fails → `af_agg_ti_failures` increments →
Grafana alert fires within 1 minute. The DAG additionally sends a direct Slack alert with the
list of failed expectations before raising the exception.

## 5. Run the GE demo

Generate sample data:

```bash
cd session_09_monitoring/demo
python data/generate_data.py
# Creates:
#   data/orders_clean.parquet  — 10,000 valid rows
#   data/orders_broken.parquet — 200 null order_id, 150 negative amount, 10 duplicate order_id
```

Run validation manually:

```python
import great_expectations as gx
import pandas as pd

context = gx.get_context(context_root_dir="great_expectations")

# Clean data — all 6 expectations pass
df_clean = pd.read_parquet("data/orders_clean.parquet")

# Broken data — 3 expectations fail:
# expect_column_values_to_not_be_null, expect_column_values_to_be_unique,
# expect_column_values_to_be_between
df_broken = pd.read_parquet("data/orders_broken.parquet")
```

View HTML report after a checkpoint run:

```bash
open great_expectations/uncommitted/data_docs/local_site/index.html
```

## 6. GE Expectation Suite

Suite: `great_expectations/expectations/orders_bronze_suite.json` — 6 expectations:

| Expectation | Column | Rule |
|---|---|---|
| not_be_null | order_id | No null PKs |
| be_unique | order_id | No duplicates |
| be_between | amount | 0 – 1,000,000 |
| not_be_null | order_date | Date required |
| be_in_set | status | pending / paid / cancelled / refunded |
| row_count_between | — | min 1 row |

## Demo simplifications

- Slack alert goes to a test webhook — real setup uses a proper on-call channel.
- GE uses local filesystem context — production uses S3 backend with a shared Data Docs site.
- StatsD exporter runs as a separate container — production typically co-locates it as a sidecar.
