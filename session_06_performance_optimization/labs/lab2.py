import datetime as dt

from pyspark.sql import functions as F
from spark_common import build_spark

spark = build_spark("lab2", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://warehouse/orders")
TABLE = "s3a://warehouse/lab2_orders"
spark.sql(f"DROP TABLE IF EXISTS delta.`{TABLE}`")

day = dt.date(2026, 6, 1)
end = dt.date(2026, 7, 10)
while day <= end:
    batch = orders.filter(F.to_date("order_purchase_timestamp") == F.lit(str(day)))
    (
        batch.repartition(6)
        .write.format("delta").mode("append").partitionBy("order_date").save(TABLE)
    )
    day += dt.timedelta(days=1)
print(f"lab2: finished daily ingestion up to {end}")

df = spark.read.format("delta").load(TABLE)
result = df.groupBy("order_status").agg(F.count("*").alias("n"))
print(f"lab2 result rows: {result.count()}")

spark.sql(f"DESCRIBE DETAIL delta.`{TABLE}`").select("numFiles", "sizeInBytes").show(truncate=False)
print("Compare numFiles against sizeInBytes: what is the average file size?")

spark.stop()
