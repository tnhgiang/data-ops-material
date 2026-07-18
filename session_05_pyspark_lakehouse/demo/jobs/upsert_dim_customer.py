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

# segment is derived deterministically from customer_id (hash % 3), so a rerun
# assigns the same segment — a random assignment would break reproducibility.
segments = array(*[lit(s) for s in SEGMENTS])
df_dim = (
    df_raw
    .select("customer_id")
    .distinct()
    .withColumn("customer_name", col("customer_id"))
    .withColumn("segment", element_at(segments, pmod(spark_hash(col("customer_id")), lit(len(SEGMENTS))) + 1))
    .withColumn("updated_at", lit(ds).cast("date"))
)

# Upsert by customer_id: existing customers are updated, new ones inserted. A
# rerun of the same date is a no-op change, so the dimension stays idempotent.
df_dim.createOrReplaceTempView("dim_customer_updates")
spark.sql("""
    MERGE INTO lakehouse.dim_customer AS target
    USING dim_customer_updates AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
log.info("dim_customer upserted for %s: %d customers", ds, df_dim.count())

spark.stop()
