# Demos Session

Both demos run against the silver Delta tables (run `scripts/generate_data.py` and
`scripts/setup_delta_tables.py` first — see the session README).

## Demo 1 — one slow task holds the whole job

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo1_pipeline.py
```

Walkthrough on http://localhost:18080:
1. Open the completed app → **Stages** → the final join stage.
2. **Summary Metrics**: compare Max vs Median task duration, and the Shuffle Read
   spread across tasks.
3. Re-run, open both apps side by side in the History Server, compare stage duration.

Note: the History Server persists every run — the before/after comparison works even
after the driver exits.

## Demo 2 — 200 tasks for a tiny batch

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo2_pipeline.py

docker exec -e AQE=true s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo2_pipeline.py
```
