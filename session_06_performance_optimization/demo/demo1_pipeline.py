from pyspark.sql import functions as F
from spark_common import build_spark

spark = build_spark("demo1", {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.shuffle.partitions": "200",
})

orders = spark.read.format("delta").load("s3a://warehouse/orders")
order_items = spark.read.format("delta").load("s3a://warehouse/order_items")
customers = spark.read.format("delta").load("s3a://warehouse/customers")

enriched = (
    orders.join(order_items, "order_id")
    .join(customers, "customer_id")
)
result = enriched.groupBy("customer_state").agg(F.sum("price").alias("gmv"))

result.write.format("delta").mode("overwrite").save("s3a://warehouse/demo1_out")
print(f"demo1 done: {result.count()} state rows")
print("Open http://localhost:18080 -> the last join stage -> Summary Metrics (Max vs Median duration).")

spark.stop()
