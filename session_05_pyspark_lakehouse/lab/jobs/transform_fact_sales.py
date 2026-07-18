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

# ---- Given: Spark session wired to Hive Metastore + MinIO (do not change) ----
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

# ---- Given: extract the raw CSV for this date ----
raw_path = f"s3a://warehouse/raw/{ds}.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
if df_raw.count() == 0:
    raise ValueError(f"No data found at {raw_path}")

# ---- TODO 1: build df_fact ----
# From df_raw, keep only order_status in ('completed', 'cancelled') AND amount > 0,
# then:
#   - cast amount to DecimalType(18, 2)
#   - add order_date = to_date(created_at)
#   - add processed_at = lit(ds).cast("date")   (NOT current_timestamp — it must
#     be reproducible on a rerun)
#   - select: order_id, customer_id, product_id, amount, quantity,
#             order_status, order_date, processed_at
# df_fact = ...
raise NotImplementedError("TODO 1: build df_fact")

# ---- TODO 2: idempotent load into lakehouse.fact_sales ----
# Write df_fact as Delta with mode("overwrite") AND
# .option("replaceWhere", f"order_date = '{ds}'") so re-running ds overwrites only
# that partition instead of appending. init_schema.py already registered the table,
# so write to its path: .save("s3a://warehouse/fact_sales").
# (
#     df_fact.write.format("delta").mode("overwrite")
#            .option("replaceWhere", f"order_date = '{ds}'")
#            .save("s3a://warehouse/fact_sales")
# )
# log.info("fact_sales loaded for %s: %d rows", ds, df_fact.count())

spark.stop()
