from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col, element_at, lit, pmod
from pyspark.sql.functions import hash as spark_hash

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("upsert_dim_customer")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

SEGMENTS = ["retail", "wholesale", "enterprise"]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True, help="Execution date YYYY-MM-DD")
ds = parser.parse_args().ds

# ---- Given: Spark session (same wiring as the fact job) ----
spark = (
    SparkSession.builder
    .appName("lakehouse-upsert-dim")
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

# ---- Given: build df_dim (deterministic segment from customer_id) ----
segments = array(*[lit(s) for s in SEGMENTS])
df_dim = (
    df_raw
    .select("customer_id")
    .distinct()
    .withColumn("customer_name", col("customer_id"))
    .withColumn("segment", element_at(segments, pmod(spark_hash(col("customer_id")), lit(len(SEGMENTS))) + 1))
    .withColumn("updated_at", lit(ds).cast("date"))
)

# ---- TODO: upsert df_dim into lakehouse.dim_customer with MERGE ----
# Register df_dim as a temp view, then run a MERGE INTO lakehouse.dim_customer
# matching on customer_id: WHEN MATCHED UPDATE SET *, WHEN NOT MATCHED INSERT *.
# A rerun of the same date must not create duplicate customers (idempotent).
# df_dim.createOrReplaceTempView("dim_customer_updates")
# spark.sql(""" MERGE INTO lakehouse.dim_customer AS target ... """)
raise NotImplementedError("TODO: MERGE df_dim into lakehouse.dim_customer")

log.info("dim_customer upserted for %s: %d customers", ds, df_dim.count())

spark.stop()
