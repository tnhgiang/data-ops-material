from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql.functions import (  # noqa: F401 (for the TODO)
    col,
    count,
    countDistinct,
    lit,
    when,  # noqa: F401
)
from pyspark.sql.functions import round as sround  # noqa: F401

log = get_logger("aggregate_ad_daily")

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
ds = parser.parse_args().ds

spark = build_spark("ad-aggregate-daily")

fact = spark.table("silver.fact_ad_events").filter(col("event_date") == lit(ds))

# ---- TODO 1: build the daily per-ad aggregate ----
# Group by event_date, ad_id, campaign_id and compute:
#   impressions  = count of rows where event_type == 'impression'
#   clicks       = count of rows where event_type == 'click'
#   unique_users = countDistinct("user_id")
#   ctr          = round(clicks / impressions, 4) when impressions > 0 else 0.0
# agg = fact.groupBy(...).agg(...).withColumn("ctr", ...)
raise NotImplementedError("TODO 1: build the daily per-ad aggregate")

# ---- TODO 2: idempotent load into gold.agg_ad_daily ----
# (
#     agg.repartition("event_date").write.format("delta").mode("overwrite")
#        .option("replaceWhere", f"event_date = '{ds}'")
#        .save("s3a://gold/agg_ad_daily")
# )
# log.info("gold.agg_ad_daily loaded for %s: %d ads", ds, agg.count())

spark.stop()
