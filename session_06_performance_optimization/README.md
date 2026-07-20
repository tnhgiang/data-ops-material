# Session 06 — Performance & Cost Optimization

Diagnose and fix real Spark/SQL bottlenecks with the **Measure → Find → Fix → Re-measure**
loop. The demos teach *how to read the signals* (Spark UI / EXPLAIN); the labs and
homework hand you **broken pipelines** and ask you to find the problem yourself, then
prove the fix with before/after numbers.

---

## 🏗️ Stack (Spark + object storage only)

| Component | Tool | Port |
|---|---|---|
| Processing | PySpark 3.5 (`apache/spark:3.5.0`) | — |
| Storage | Delta Lake 3.2 + MinIO | 9000 (API) · 9001 (Console) |
| Spark Master UI | standalone master | 8080 |
| Spark App UI | driver (only while a job runs) | 4040 |
| **Spark History Server** | replays completed jobs | **18080** |
| DWH (homework 3) | DuckDB | — (host, file-based) |

> The Delta/S3A jars are **reused from session_05** — the compose mounts
> `../session_05_pyspark_lakehouse/jars`. Download them in session_05 first.

> Port note: **8080 is the Spark Master UI**, **4040 is the live app UI** — not Airflow.
> There is no Airflow in this session. If 8080 is taken, stop other sessions' stacks first.

---

## 🚀 How to Run

All commands run from `session_06_performance_optimization/`. Every `spark-submit`
ships the shared helper with `--py-files /scripts/spark_common.py`.

### 0. Prerequisites
- Docker Desktop (≥ 6 GB RAM allocated — the worker uses 4 GB).
- The session_05 jars must already exist.
- Host Python only for homework 3: `pip install -r requirements.txt` (just `duckdb`).

### 1. Start the stack
```bash
docker compose up -d
docker compose ps           # wait until services are healthy
```
UIs: Spark Master http://localhost:8080 · History http://localhost:18080 ·
MinIO http://localhost:9001 (minioadmin/minioadmin)

### 2. Generate the data (Spark)
```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --py-files /scripts/spark_common.py /scripts/generate_data.py --scale large
```
Writes Parquet to `./data/` (mounted at `/data`). `--scale large` (default, ~15M orders /
40M items) makes the broken jobs run for **several minutes** so the signal is real. Use
`--scale small` for a quick dry run while iterating.

### 3. Load the Delta tables on MinIO
```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --py-files /scripts/spark_common.py /scripts/setup_delta_tables.py
```

### 4. Analyze in the History Server
After any job, open **http://localhost:18080**, click the app, and inspect
**Stages → Summary Metrics** (percentiles for Duration, GC Time, Spill), the
**SQL/DataFrame** plan, and per-task Shuffle Read. Runs are persisted, so you can
re-open a run and compare before/after your fix without keeping the driver alive.

### 5. Tear down
```bash
docker compose down          # keep data
docker compose down -v       # also wipe the MinIO volume
```

---

## 🎬 Demos (mentor-led)

```bash
# Demo 1
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo1_pipeline.py

# Demo 2 — run twice and compare the same stage
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo2_pipeline.py
docker exec -e AQE=true s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo2_pipeline.py
```

| Demo | File | What to read |
|---|---|---|
| Demo 1 | `demo/demo1_pipeline.py` | Stages → Summary Metrics: Max vs Median task duration on the final join stage |
| Demo 2 | `demo/demo2_pipeline.py` | Stages: task count and per-task size; how it changes with `AQE=true` |

---

## 🧪 Labs (in class — you diagnose, then fix)

Each lab ships **broken**. Open the Spark UI, read the symptom, name the bottleneck,
apply the fix, and record before/after. Run a lab:
```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /labs/lab1.py
```

| Lab | File | Symptom you're told | Where to look |
|---|---|---|---|
| Lab 1 | `labs/lab1.py` | "The first-week-of-July report reads far more data than a week." | SQL tab → Scan node → `PartitionFilters` vs `PushedFilters` |
| Lab 2 | `labs/lab2.py` | "A month of daily loads and now reads crawl." | `DESCRIBE DETAIL` → `numFiles` vs `sizeInBytes` |
| Lab 3 | `labs/lab3.py` | "One task in the join runs for minutes; the rest finish fast." | Stages → the customers-join stage → Shuffle Read per task |

Helpers: `scripts/get_num_files.py <delta_path>` prints `numFiles`;
`scripts/operations__compact_files.py <delta_path>` runs `OPTIMIZE` and reports before/after.
(Lab 2 writes ~40 daily batches — it takes a couple of minutes to build.)

---

## 📝 Homework (self-study — harder, not demoed)

```bash
# HW 1 / HW 2 (Spark)
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /homework/hw1.py
# HW 3 (host DuckDB)
python3 homework/hw3.py
```

| HW | File | Symptom you're told | Where to look |
|---|---|---|---|
| HW 1 | `homework/hw1.py` | "This aggregation is slow and thrashing." | Stages → Summary Metrics: Spill (Memory/Disk), GC Time |
| HW 2 | `homework/hw2.py` | "Small logic, but the job pins CPU for a long time." | SQL/DataFrame tab → how often the same computation repeats |
| HW 3 | `homework/hw3.py` | "This SQL report is slow and its result changes by the day." | DuckDB `EXPLAIN ANALYZE` → `TABLE_SCAN` + filter |

HW 3 writes `data/hw3.duckdb`. Inspect it from the host — **DBeaver**: New Connection →
DuckDB → Path `data/hw3.duckdb` (open **read-only** while nothing else holds the lock), or:
```bash
duckdb data/hw3.duckdb "SELECT COUNT(*) FROM orders"
```

---

## 📊 Dataset

`scripts/generate_data.py` (Spark) writes four Parquet datasets to `./data/`. Purchase
timestamps span **2026-06-01 → 2026-07-10**.

**orders** — `order_id` (PK), `customer_id`, `order_status`, `order_purchase_timestamp`,
`order_approved_at`, `order_delivered_timestamp`, `order_estimated_delivery_date`.
Loaded to Delta partitioned by `order_date` (= date of purchase).

**order_items** — `order_id`, `order_item_id`, `product_id`, `seller_id`, `product_category`,
`price`, `shipping_charges`.

**customers** — `customer_id` (PK), `customer_name`, `customer_zip_code_prefix`,
`customer_city`, `customer_state`.

**payments** — `order_id`, `payment_sequential`, `payment_type`, `payment_installments`,
`payment_value`.

Scales: `--scale small | medium | large` (default `large`: orders 15M, items 40M).

---

## 📋 Submission format (per lab / homework)

Submit one short markdown or notebook file with:

1. **Symptom** — screenshot or pasted numbers from Spark UI / EXPLAIN (not prose).
2. **Bottleneck** — which one, and the signal that told you.
3. **Fix** — the code/config you changed.
4. **Before / After** — a table (duration, shuffle GB, spill, numFiles, GC % — as fits).
5. **Mechanism** — 2-3 sentences on *why* the fix addresses the root cause.

Grading: identify **30%** · fix **30%** · before/after numbers **20%** · mechanism **20%**.
A correct fix with no explanation does not score full marks.

---

## 📁 Project structure

```
session_06_performance_optimization/
├── README.md
├── docker-compose.yml           ← MinIO + Spark master/worker/history
├── requirements.txt             ← host libs (homework 3 only: duckdb)
├── conf/spark-defaults.conf     ← event logging + spark.jars (no --jars needed)
├── scripts/
│   ├── spark_common.py          ← shared build_spark() (shipped via --py-files)
│   ├── generate_data.py         ← Spark data generator (Parquet)
│   ├── setup_delta_tables.py    ← load Parquet → Delta on MinIO
│   ├── get_num_files.py         ← print numFiles for a Delta table
│   └── operations__compact_files.py  ← OPTIMIZE a Delta table, report before/after
├── demo/
│   ├── demo1_pipeline.py
│   └── demo2_pipeline.py
├── labs/
│   ├── lab1.py
│   ├── lab2.py
│   └── lab3.py
├── homework/
│   ├── hw1.py
│   ├── hw2.py
│   └── hw3.py
├── data/                        ← generated Parquet + hw3.duckdb (gitignored)
└── spark-events/                ← Spark event logs (read by History Server)
```
