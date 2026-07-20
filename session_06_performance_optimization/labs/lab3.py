from pyspark.sql import functions as F
from spark_common import build_spark

spark = build_spark(
    "lab3",
    {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": "200",
    },
)

orders = spark.read.format("delta").load("s3a://warehouse/orders")
payments = spark.read.format("delta").load("s3a://warehouse/payments")
customers = spark.read.format("delta").load("s3a://warehouse/customers")

enriched = orders.join(payments, "order_id").join(customers, "customer_id")
result = enriched.groupBy("customer_state").agg(F.sum("payment_value").alias("paid"))

result.write.format("delta").mode("overwrite").save("s3a://warehouse/lab3_out")
print(f"lab3 done: {result.count()} state rows")
print("Open :18080 -> the customers join stage -> Shuffle Read per task. Even, or not?")

spark.stop()
