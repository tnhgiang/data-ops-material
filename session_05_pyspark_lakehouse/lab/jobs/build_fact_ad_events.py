from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql import Window  # noqa: F401 (for the TODO)
from pyspark.sql.functions import col, row_number, to_date  # noqa: F401

log = get_logger("build_fact_ad_events")

ID_COLUMNS = ["event_id", "ad_id", "campaign_id", "user_id"]
KNOWN_COLUMNS = ID_COLUMNS + ["event_type", "event_ts", "device_type", "cost_micros"]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
ds = parser.parse_args().ds

spark = build_spark("ad-build-fact")

raw_path = f"s3a://bronze/ad_events/{ds}.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
if df.count() == 0:
    raise ValueError(f"No data found at {raw_path}")

# ---- TODO 1: clean + dedup ----
# - Cast each column in ID_COLUMNS to "string".
# - Keep event_type in ('impression', 'click'); add event_date = to_date(event_ts).
# - Dedup by event_id, keeping the earliest event_ts (a retry sends the same event twice):
#       w = Window.partitionBy("event_id").orderBy(col("event_ts").asc())
#       df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")
raise NotImplementedError("TODO 1: clean + dedup the events")

# ---- TODO 2: select known columns, then idempotent load into silver.fact_ad_events ----
# - Keep only columns the fact expects (so an unexpected source column isn't written):
#       df = df.select([c for c in KNOWN_COLUMNS if c in df.columns] + ["event_date"])
# - Write Delta, mode("overwrite"), .option("replaceWhere", f"event_date = '{ds}'"),
#   .save("s3a://silver/fact_ad_events"). (Lab 3 revisits this when the source adds a column.)
# log.info("silver.fact_ad_events loaded for %s: %d rows", ds, df.count())

spark.stop()
