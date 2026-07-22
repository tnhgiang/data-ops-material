# Session 6 Labs — Performance Diagnosis (In Class)

Each lab is a pipeline that already runs — but with a real performance problem. Your job follows the same loop every time: **reproduce → read the signal → name the root cause → fix → prove with numbers**.

Prerequisites: data generated and silver Delta tables loaded (see the [session README](../README.md)).

## Run

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /labs/lab1.py
```

Swap `lab1.py` for `lab2.py` or `lab3.py`. Every completed run is saved at **http://localhost:18080** — you can re-open and compare before/after runs without keeping the driver alive.

> Cold-cache matters for Lab 1. Restart MinIO before each timed run to flush the OS page cache:
> `docker compose restart minio` (takes ~10s; re-run immediately after).

---

## The tickets

| Lab | Reported problem |
|---|---|
| Lab 1 | "The first-week-of-July revenue report takes far longer than a one-week report should." |
| Lab 2 | "This table is loaded every day. After a few weeks, reading it has slowed to a crawl." |
| Lab 3 | "The top-amount-per-customer report produces wrong numbers and Stage 2 takes several minutes." |

---

## Lab 1 — Revenue report on a partitioned Delta table

**Business context:** a scheduled daily job reports item revenue for orders approved on a specific date. The job reads `silver/orders` (partitioned by `order_date`) and joins to `silver/order_items`.

**Where to look:**
- History Server → Stages: note the input sizes and elapsed time of each stage.
- History Server → SQL/DataFrame tab: what join strategy does Spark choose?
- The script prints `EXPLAIN` output to the logs — read it.

**Evidence to capture (before fix):**
- Stage metrics: input size of the shuffle stage
- Join strategy from the SQL plan or EXPLAIN output

**Acceptance criteria:**
- The fix is a code change inside `lab1.py` only — no cluster config changes.
- Before/after numbers recorded in a table.
- Elapsed time drops measurably on a cold-cache run.

---

## Lab 2 — Daily-appended Delta table

**Business context:** `silver/lab2_orders` is loaded once per day by the production pipeline. After weeks of daily ingestion, a simple daily operations report (orders and unique customers per day per status) has slowed to a crawl. The table itself is not large — the problem is in the physical layout.

**Setup note:** run `scripts/setup_lab2_table.py` once before starting this lab — it replays 21 days of the daily ingest job so the file layout matches what the production table looks like after weeks of running:

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py \
  /scripts/setup_lab2_table.py
```

**Where to look:**
- File count and average file size:
  ```bash
  docker exec s06-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 --py-files /scripts/spark_common.py \
    /scripts/get_num_files.py s3a://silver/lab2_orders
  ```
- History Server → Stages: how many tasks does the scan stage spawn?

**Evidence to capture (before fix):**
- Output of `get_num_files.py` (file count, avg size)
- Number of tasks in the scan stage

**Acceptance criteria:**
- After the fix, `get_num_files.py` shows dramatically fewer files and larger average size.
- Re-running lab2.py shows fewer scan tasks and faster elapsed time.
- The fix is an operation on the table, not a code change inside lab2.py.

---

## Lab 3 — Customer dimension join

**Business context:** a pipeline joins order data to a customer dimension and ranks each customer's highest-value order. The dimension is stored as SCD2 — multiple history rows per customer. The job produces wrong row counts and Stage 2 is slow.

**Data shape (self-contained — no MinIO needed):**

| Table | Rows | Notes |
|---|---|---|
| `orders` | 5,000,000 (default) | one row per order |
| `customers_scd2` | 2,000,000 | 100k customers × 20 history versions |

**Where to look:**
- `result.count()` from the broken run — is the number plausible?
- History Server → Stage 2: input row count and elapsed time.
- History Server → SQL/DataFrame: what is the shuffle size?

**Evidence to capture (before fix):**
- `result.count()` value
- Stage 2 input rows and elapsed time

**Acceptance criteria:**
- `result.count()` after the fix returns exactly 100,000.
- Stage 2 input rows drop by at least 10×; elapsed time drops by at least 5×.
- The fix is a one-line change inside `lab3.py`.

---

## Completion checklist (per lab)

- [ ] Ran the broken pipeline and captured evidence (screenshot or pasted metrics — not prose)
- [ ] Named the real root cause backed by that evidence
- [ ] Applied the fix and re-ran the pipeline
- [ ] Recorded before/after numbers in a table
- [ ] Explained in 2–3 sentences why the fix addresses the root cause
