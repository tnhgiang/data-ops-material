# Session 09 Demo — Monitoring, Alerting & Data Quality

Stack: Prometheus + StatsD Exporter + Grafana + Great Expectations + Slack Webhook + Airflow.

## Architecture

Session 09 runs as **two Docker Compose stacks** that talk over `host.docker.internal`:

![Session 09 flow](../images/session_09.png)


```
session_07_security/    ← Airflow (webserver + scheduler + worker)  ─┐  emits StatsD
session_09_monitoring/  ← Prometheus + StatsD Exporter + Grafana    ←┘  host.docker.internal:8125
```

Airflow (in the session_07 stack) emits metrics via UDP to `host.docker.internal:8125`, which the
session_09 StatsD Exporter receives on its `8125:9125/udp` mapping:

```
Airflow → UDP → host.docker.internal:8125 → StatsD Exporter → Prometheus scrape (15s) → Grafana
```

The session_09 order-pipeline DAG is **deployed into the session_07 Airflow** (see
[Deploy the pipeline](#deploy-the-pipeline-into-session_07)), so its task runs emit the metrics the
dashboards visualize and its Great Expectations check drives the data-quality alert.

Both stacks must be up:

```bash
cd session_07_security   && docker compose up -d --build   # --build picks up GE + duckdb
cd session_09_monitoring && docker compose up -d
```

## 1. Start the monitoring stack

```bash
cd session_09_monitoring
docker compose up -d
docker compose ps
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

## Deploy the pipeline into session_07

The session_07 Airflow mounts this session's DAG and installs its runtime deps (already wired):

- `session_07_security/docker-compose.yml` mounts `../session_09_monitoring/demo` →
  `/opt/airflow/dags/session_09` and `../plugins` → `/opt/airflow/plugins`.
- `session_07_security/Dockerfile` installs `great-expectations` + `duckdb` (rebuild with
  `docker compose up -d --build`).
- An `.airflowignore` in `demo/` keeps `data/` (which holds `generate_data.py`) out of DAG parsing.

After `--build`, the `session_09_order_pipeline` DAG appears in the session_07 Airflow UI.

## DuckDB warehouse — host access

The pipeline builds a local DuckDB warehouse at `session_09_monitoring/demo/data/session_09.duckdb`
(bind-mounted into the session_07 Airflow at `/opt/airflow/dags/session_09/data`). It is gitignored.
To inspect `bronze.orders` / `gold.fact_orders` from your laptop, point **DBeaver** (or the `duckdb`
CLI) at that file while the DAG is **not** running (DuckDB is single-writer):

```bash
python3 -c "
import duckdb
con = duckdb.connect('session_09_monitoring/demo/data/session_09.duckdb', read_only=True)
print(con.execute('SELECT COUNT(*) FROM gold.fact_orders').fetchone())
con.close()
"
```

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

The pipeline (`session_09_order_pipeline`) loads a parquet file into a local DuckDB warehouse
(`bronze.orders`), validates it against the `orders_bronze_suite`, then builds `gold.fact_orders`
and reconciles row counts. Which dataset it loads is controlled by the Airflow Variable
`orders_dataset` (default `orders_clean.parquet`).

The two sample files are committed under `demo/data/`. To regenerate them:

```bash
# pyarrow is required — install once if not already present
pip install -r session_09_monitoring/requirements.txt

cd session_09_monitoring/demo/data
python generate_data.py
# Creates (in this folder):
#   orders_clean.parquet  — 10,000 valid rows
#   orders_broken.parquet — 200 null order_id, 150 negative amount, 10 duplicate order_id
```

**Happy path** — trigger the DAG with the default clean dataset; all 6 expectations pass and the
pipeline runs through to `recon_row_count`.

**Failure path** — in Airflow, **Admin → Variables → +**, add `orders_dataset` = `orders_broken.parquet`,
then trigger the DAG again. The `ge_checkpoint_bronze`
task fails on exactly 3 expectations (`expect_column_values_to_not_be_null`,
`expect_column_values_to_be_unique`, `expect_column_values_to_be_between`), sends the Slack alert
(when the Airflow Variable `slack_webhook_url` is set — otherwise it logs and no-ops), and raises —
so `load_gold` and `recon_row_count` never run.

The task validates through an ephemeral GE context, so no files under `great_expectations/` are
modified by a run.

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

- Slack webhook is a test URL — real deployment uses a proper channel with on-call rotation.
- GE context uses local filesystem — production uses a shared backend (S3 + shared Data Docs site).
- StatsD exporter runs as a sidecar — production typically uses the Prometheus push gateway or a managed metrics service.
