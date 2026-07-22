import datetime as dt

from pyspark.sql import functions as F
from spark_common import build_spark, get_logger

log = get_logger(__name__)
spark = build_spark("lab2", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://silver/orders")
TABLE = "s3a://silver/lab2_orders"
spark.sql(f"DROP TABLE IF EXISTS delta.`{TABLE}`")

day = dt.date(2026, 6, 1)
end = dt.date(2026, 7, 10)
while day <= end:
    batch = orders.filter(F.col("order_date") == F.lit(str(day)))
    (
        batch.repartition(24)
        .write.format("delta")
        .mode("append")
        .partitionBy("order_date")
        .save(TABLE)
    )
    day += dt.timedelta(days=1)
log.info("lab2: finished daily ingestion up to %s -> %s", end, TABLE)

df = spark.read.format("delta").load(TABLE)
result = df.groupBy("order_status").agg(F.count("*").alias("n"))
log.info("lab2 done: %d order-status rows", result.count())

spark.stop()
