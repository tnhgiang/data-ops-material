from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("init_schema")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

spark = (
    SparkSession.builder
    .appName("lakehouse-init")
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

fact_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("quantity", IntegerType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("processed_at", DateType(), False),
])

dim_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("updated_at", DateType(), False),
])

# fact_sales is partitioned by order_date so replaceWhere can overwrite one day's
# partition atomically; dim_customer is a full table upserted by MERGE.
(
    spark.createDataFrame([], fact_schema).write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .option("path", "s3a://warehouse/fact_sales")
    .saveAsTable("lakehouse.fact_sales")
)
log.info("Created lakehouse.fact_sales")

(
    spark.createDataFrame([], dim_schema).write
    .format("delta")
    .mode("overwrite")
    .option("path", "s3a://warehouse/dim_customer")
    .saveAsTable("lakehouse.dim_customer")
)
log.info("Created lakehouse.dim_customer")

spark.stop()
