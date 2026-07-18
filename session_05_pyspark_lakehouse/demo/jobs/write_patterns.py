from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("write_patterns")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
parser.add_argument("--mode", default="idempotent", choices=["idempotent", "append"],
                    help="idempotent = overwrite+replaceWhere; append = the anti-pattern")
args = parser.parse_args()
ds = args.ds

spark = (
    SparkSession.builder
    .appName("demo-write-patterns")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://warehouse/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse LOCATION 's3a://warehouse/'")

# Create the table once as an empty, partitioned Delta table (same shape as
# init_schema does for fact_sales), so later saveAsTable writes never try to alter
# a Hive definition that was declared differently.
if not spark.catalog.tableExists("lakehouse.demo_sales"):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("order_date", DateType(), True),
    ])
    (
        spark.createDataFrame([], schema).write
        .format("delta")
        .partitionBy("order_date")
        .option("path", "s3a://warehouse/demo_sales")
        .saveAsTable("lakehouse.demo_sales")
    )

# A fixed three-row batch for this date — deterministic, so a rerun is comparable.
df = (
    spark.createDataFrame(
        [("A", 10.0), ("B", 20.0), ("C", 30.0)],
        ["order_id", "amount"],
    )
    .withColumn("order_date", to_date(lit(ds)))
)

# Write to the table's path (already registered above). Writing by path instead of
# saveAsTable avoids a redundant Hive alter-table sync on every run.
target = "s3a://warehouse/demo_sales"
writer = df.write.format("delta")
if args.mode == "append":
    # The anti-pattern: every run adds the batch again -> duplicates on rerun.
    writer.mode("append").save(target)
else:
    # Idempotent: overwrite only this date's partition, so a rerun == one run.
    writer.mode("overwrite").option("replaceWhere", f"order_date = '{ds}'").save(target)


count = spark.sql(f"SELECT COUNT(*) FROM lakehouse.demo_sales WHERE order_date = DATE '{ds}'").collect()[0][0]
log.info("demo_sales (%s) rows for %s: %d", args.mode, ds, count)

spark.stop()
