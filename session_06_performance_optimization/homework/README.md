# Session 6 Homework — Performance Diagnosis

## Run

**HW 1 / HW 2 (Spark):**
```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /homework/hw1.py
```
Swap `hw1.py` for `hw2.py`. Analyze completed runs at **http://localhost:18080**.

**HW 3 (host DuckDB — no Spark):**
```bash
# from session_06_performance_optimization/
pip install -r requirements.txt   # one-time: just duckdb
python homework/hw3.py
```

---

## The tickets

| HW | Reported problem |
|---|---|
| HW 1 | "The seller/product stats job takes forever and sometimes crashes the workers." |
| HW 2 | "The risk-score report reads modest data but pins the CPU for a very long time." |
| HW 3 | "This SQL report is slow, and yesterday's numbers don't match today's re-run." |

---

## HW 1 — Seller/product stats

**Business context:** a daily job aggregates `silver/order_items` (50M rows) into a seller × product × category stats table written to `s3a://gold/hw1_seller_product_stats`.

**Where to look:**
- History Server → the hw1 app → **Executors** tab.
- History Server → **Stages** tab: task count, task duration spread, any failed tasks.

**Evidence to capture (before fix):**
- Executor tab: memory and cores per executor
- Stage metrics: failed tasks or error messages in logs

**Acceptance criteria:**
- The fix changes only the Spark session config in `hw1.py` — not the aggregation logic or output path.
- Re-run completes without task failures.
- Before/after numbers recorded.

---

## HW 2 — Risk-score report

**Business context:** a risk-score report joins `order_items`, `orders`, and `payments`, computes a score per row, and aggregates by `order_status`. It runs correctly but takes far longer than the data size suggests.

**Where to look:**
- History Server → the hw2 app → **Stages** tab: elapsed time and input row count of the scoring stage.
- History Server → **SQL/DataFrame** tab: look at the physical plan for the scoring step.

**Evidence to capture (before fix):**
- Scoring stage elapsed time
- SQL physical plan (screenshot or paste)

**Acceptance criteria:**
- The output (`total_risk_score` by `order_status`) is numerically identical before and after.
- Scoring stage elapsed time drops by at least 1.5×.
- Before/after numbers recorded.

---

## HW 3 — DuckDB daily report

**Business context:** a daily report queries an `orders` table in DuckDB (loaded from bronze MinIO parquet). The report is slow, and re-running it on a different day produces different numbers for the same time window.

> HW 3 reads bronze parquet through MinIO's S3 API — the MinIO stack must be up (`docker compose ps`).
> Inspect `data/hw3.duckdb` from your host with DBeaver (New Connection → DuckDB → Path: `data/hw3.duckdb`, open read-only while the script is not running) or the CLI:
> ```bash
> duckdb data/hw3.duckdb "SELECT COUNT(*) FROM orders"
> ```

**Where to look:**
- `EXPLAIN ANALYZE` output printed by the script: `TABLE_SCAN` projected columns, row count, `ORDER BY` elapsed time.
- Re-run on a different date — do the result numbers change?

**Evidence to capture (before fix):**
- `TABLE_SCAN` projected column count from `EXPLAIN ANALYZE`
- Elapsed time of the `ORDER BY` node

**Acceptance criteria:**
- The fixed query returns the same numbers on any re-run for the same reporting period.
- `EXPLAIN ANALYZE` shows fewer columns projected in the scan.
- Before/after elapsed time recorded.

---

## Completion checklist (per homework)

- [ ] Ran the broken pipeline and captured evidence (screenshot or pasted metrics — not prose)
- [ ] Named the real root cause backed by that evidence
- [ ] Applied the fix and re-ran the pipeline
- [ ] Recorded before/after numbers in a table
- [ ] Explained in 2–3 sentences why the fix addresses the root cause
