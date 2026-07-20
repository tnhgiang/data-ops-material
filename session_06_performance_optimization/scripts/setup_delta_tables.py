from pyspark.sql import functions as F
from spark_common import build_spark

spark = build_spark("setup-delta-tables")

DATA = "/data"
WAREHOUSE = "s3a://warehouse"


def load(name, partition_by=None, derive=None):
    df = spark.read.parquet(f"{DATA}/{name}")
    if derive:
        df = derive(df)
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(f"{WAREHOUSE}/{name}")
    print(f"Loaded {name}: {df.count():,} rows -> {WAREHOUSE}/{name}")


load(
    "orders",
    partition_by="order_date",
    derive=lambda df: df.withColumn("order_date", F.to_date("order_purchase_timestamp")),
)
load("order_items")
load("customers")
load("payments")

spark.stop()
