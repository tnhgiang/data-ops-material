from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from security_utils import get_logger, get_vault_secret

logger = get_logger("init_schema")
minio_secret = get_vault_secret("dataops/minio")

spark = (
    SparkSession.builder.appName("security-demo-init")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", minio_secret["endpoint"])
    .config("spark.hadoop.fs.s3a.access.key", minio_secret["access_key"])
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret["secret_key"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://silver/")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://bronze/'")
spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://silver/'")
spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 's3a://gold/'")

bronze_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cccd", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("account_balance", DecimalType(18, 2), True),
        StructField("credit_score", IntegerType(), True),
        StructField("ingested_at", DateType(), False),
    ]
)

silver_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("full_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cccd", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("account_balance", DecimalType(18, 2), True),
        StructField("credit_score", IntegerType(), True),
        StructField("updated_at", DateType(), False),
    ]
)

gold_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registered_at", TimestampType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cccd", StringType(), True),
        StructField("balance_tier", StringType(), True),
        StructField("credit_tier", StringType(), True),
        StructField("updated_at", DateType(), False),
    ]
)

(
    spark.createDataFrame([], bronze_schema)
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("ingested_at")
    .option("path", "s3a://bronze/customers")
    .saveAsTable("bronze.customers")
)
logger.info("Created bronze.customers")

(
    spark.createDataFrame([], silver_schema)
    .write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://silver/customers")
    .saveAsTable("silver.customers")
)
logger.info("Created silver.customers")

(
    spark.createDataFrame([], gold_schema)
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("updated_at")
    .option("path", "s3a://gold/customer_analytics")
    .saveAsTable("gold.customer_analytics")
)
logger.info("Created gold.customer_analytics")

spark.stop()
