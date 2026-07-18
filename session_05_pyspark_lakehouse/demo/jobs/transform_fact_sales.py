from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import DecimalType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("transform_fact_sales")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
ds = parser.parse_args().ds

spark = (
    SparkSession.builder
    .appName("lakehouse-transform-fact")
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

raw_path = f"s3a://warehouse/raw/{ds}.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
if df_raw.count() == 0:
    raise ValueError(f"No data found at {raw_path}")

df_fact = (
    df_raw
    .filter(col("order_status").isin("completed", "cancelled"))
    .filter(col("amount") > 0)
    .withColumn("amount", col("amount").cast(DecimalType(18, 2)))
    .withColumn("order_date", to_date(col("created_at")))
    # processed_at is lit(ds), never current_timestamp(), so a rerun of a date
    # reproduces the same rows — the property this session is about.
    .withColumn("processed_at", lit(ds).cast("date"))
    .select("order_id", "customer_id", "product_id", "amount", "quantity",
            "order_status", "order_date", "processed_at")
)

# replaceWhere overwrites only this date's partition, so re-running ds is
# idempotent instead of appending duplicates. init_schema.py already registered
# the table, so we write to its path (avoids a redundant Hive alter per run).
(
    df_fact.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"order_date = '{ds}'")
    .save("s3a://warehouse/fact_sales")
)
log.info("fact_sales loaded for %s: %d rows", ds, df_fact.count())

spark.stop()
