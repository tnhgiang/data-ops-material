from __future__ import annotations

import argparse

from lakehouse_common import build_spark, get_logger
from pyspark.sql.functions import col, count, lit, round as sround, sum as ssum

log = get_logger("gold_aggregate")

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
ds = parser.parse_args().ds

spark = build_spark("orders-gold-aggregate")

df = (
    spark.read.format("delta")
    .load("s3a://silver/orders")
    .filter(col("order_date") == lit(ds))
)

if df.count() == 0:
    raise ValueError(f"silver.orders is empty for {ds}")

agg = (
    df.groupBy("order_date", "partner")
    .agg(
        count("*").alias("order_count"),
        sround(ssum("amount"), 2).alias("total_revenue"),
    )
)

(
    agg.repartition("order_date")
    .write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_date = '{ds}'")
    .save("s3a://gold/fact_daily_revenue")
)
log.info("gold.fact_daily_revenue loaded for %s: %d partner rows", ds, agg.count())

spark.stop()
