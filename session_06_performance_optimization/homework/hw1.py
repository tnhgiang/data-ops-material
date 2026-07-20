from pyspark.sql import functions as F
from spark_common import build_spark

spark = build_spark(
    "hw1",
    {
        "spark.executor.memory": "512m",
        "spark.executor.cores": "1",
        "spark.sql.shuffle.partitions": "5",
        "spark.sql.adaptive.enabled": "false",
    },
)

order_items = spark.read.format("delta").load("s3a://warehouse/order_items")

agg = order_items.groupBy("seller_id", "product_category", "product_id").agg(
    F.sum("price").alias("sum_price"),
    F.count("*").alias("n"),
    F.avg("price").alias("avg_price"),
    F.max("shipping_charges").alias("max_ship"),
    F.min("shipping_charges").alias("min_ship"),
)

agg.write.format("delta").mode("overwrite").save("s3a://warehouse/hw1_out")
print(f"hw1 done: wrote {agg.count():,} rows")
print("Open :18080 -> Stages -> Summary Metrics: read the per-task percentile columns.")

spark.stop()
