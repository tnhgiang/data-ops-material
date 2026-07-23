import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from security_utils import get_logger, get_vault_secret, log_pii_access

logger = get_logger("ingest_bronze")

PII_COLUMNS = [
    "full_name",
    "phone",
    "email",
    "cccd",
    "date_of_birth",
    "address",
    "account_balance",
    "credit_score",
]

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

pg_secret = get_vault_secret("dataops/postgres_source")
minio_secret = get_vault_secret("dataops/minio")

spark = (
    SparkSession.builder.appName("ingest-bronze-customers")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.hadoop.fs.s3a.endpoint", minio_secret["endpoint"])
    .config("spark.hadoop.fs.s3a.access.key", minio_secret["access_key"])
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret["secret_key"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://warehouse/")
    .enableHiveSupport()
    .getOrCreate()
)
# WARN, not DEBUG: DEBUG would dump DataFrame schema + sample rows (PII) to logs.
spark.sparkContext.setLogLevel("WARN")

log_pii_access(
    job_name="ingest_bronze",
    table="raw.customers",
    columns=PII_COLUMNS,
    record_count=0,
    ds=ds,
)

jdbc_url = f"jdbc:postgresql://{pg_secret['host']}:{pg_secret['port']}/{pg_secret['database']}"
df_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "raw.customers")
    .option("user", pg_secret["username"])
    .option("password", pg_secret["password"])
    .option("driver", "org.postgresql.Driver")
    .load()
)

df_bronze = df_raw.withColumn("ingested_at", lit(ds).cast("date"))

(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"ingested_at = '{ds}'")
    .option("path", "s3a://warehouse/bronze/customers")
    .saveAsTable("bronze.customers")
)

# .count() only -- never .show() on a DataFrame that carries PII.
row_count = df_bronze.count()
logger.info(f"bronze.customers loaded for {ds}: {row_count} rows")

log_pii_access(
    job_name="ingest_bronze",
    table="raw.customers",
    columns=PII_COLUMNS,
    record_count=row_count,
    ds=ds,
)

spark.stop()
