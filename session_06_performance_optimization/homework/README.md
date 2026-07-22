# Homework (self-study)

Same format as the labs: each pipeline works but has a performance problem. Find the
real cause with evidence, fix it, prove it with before/after numbers. These are harder
than the labs — the cause was not shown in any demo.

## Run

HW 1 / HW 2 (Spark):
```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /homework/hw1.py
```

HW 3 (host DuckDB, no Spark):
```bash
pip install -r ../requirements.txt   # from this folder; just duckdb
python3 homework/hw3.py              # run from the session root
```

## The tickets

| HW | Reported problem |
|---|---|
| HW 1 | "The seller/product stats job takes forever and sometimes dies." |
| HW 2 | "The risk-score job reads modest data but pins the CPU for a very long time." |
| HW 3 | "This SQL report is slow, and yesterday's numbers don't match today's re-run." |

Notes:
- HW 1 / HW 2: analyze completed runs at http://localhost:18080.
- HW 3 creates `data/hw3.duckdb`. Inspect it from the host with DBeaver
  (New Connection → DuckDB → Path `data/hw3.duckdb`, open read-only while the script
  is not running) or the CLI: `duckdb data/hw3.duckdb "SELECT COUNT(*) FROM orders"`.
- HW 3 needs the MinIO stack up: it reads the bronze parquet through MinIO's S3 API
  (`localhost:9000`).

## Completion checklist (per homework)

- [ ] Ran the broken pipeline and captured the evidence of the problem
      (screenshot or pasted metrics — not a description)
- [ ] Identified the real root cause, backed by that evidence
- [ ] Applied a fix and re-ran the pipeline
- [ ] Recorded before/after numbers in a table
- [ ] Explained in 2-3 sentences why the fix removes the root cause
