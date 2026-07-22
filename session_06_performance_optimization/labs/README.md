# Labs (in class)

Each lab is a working pipeline with a performance problem reported by "the business".
Your job: reproduce it, find the real cause with evidence from the Spark UI / query plan,
fix it, and prove the fix with numbers.

Prerequisites: data generated and silver tables loaded (see the session README).

## Run

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /labs/lab1.py
```
Swap `lab1.py` for `lab2.py` or `lab3.py`. Every completed run is kept at
http://localhost:18080 for analysis.

## The tickets

| Lab | Reported problem |
|---|---|
| Lab 1 | "The first-week-of-July revenue report takes far longer than a one-week report should." |
| Lab 2 | "This table is loaded every day. After a few weeks, reading it has slowed to a crawl." |
| Lab 3 | "The state-level payment report stalls at the end — it sits at 199/200 tasks for minutes." |

Notes:
- Lab 2 reads a table built during environment prep (`scripts/setup_lab2_table.py`,
  see the session README) — the same table the daily pipeline has been loading in
  production.
- Fixes are config/code changes inside the lab file (or a follow-up operation on the
  table) — the business logic and its output must stay the same.

## Completion checklist (per lab)

- [ ] Ran the broken pipeline and captured the evidence of the problem
      (screenshot or pasted metrics — not a description)
- [ ] Identified the real root cause, backed by that evidence
- [ ] Applied a fix and re-ran the pipeline
- [ ] Recorded before/after numbers in a table
- [ ] Explained in 2-3 sentences why the fix removes the root cause
