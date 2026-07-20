from pyspark.sql import functions as F
from spark_common import build_spark

spark = build_spark("lab1", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://warehouse/orders")
order_items = spark.read.format("delta").load("s3a://warehouse/order_items")

# Report: item revenue for orders approved in the first week of July 2026.
result = (
    orders.filter(
        (F.col("order_approved_at") >= "2026-07-01") & (F.col("order_approved_at") < "2026-07-08")
    )
    .join(order_items, "order_id")
    .groupBy(F.to_date("order_purchase_timestamp").alias("day"))
    .agg(F.sum("price").alias("revenue"))
)

result.explain("formatted")
print(f"lab1 result rows: {result.count()}")
print("Read the Scan node on orders above: which predicates land where?")

spark.stop()
