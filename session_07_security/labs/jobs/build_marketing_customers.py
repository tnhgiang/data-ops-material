import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from security_utils import get_vault_secret

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

minio_secret = get_vault_secret("dataops/minio")

spark = (
    SparkSession.builder.appName("build-marketing-customers")
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
spark.sparkContext.setLogLevel("WARN")

df = spark.table("silver.customers")

df_out = df.withColumn("updated_at", lit(ds).cast("date"))

(
    df_out.write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://warehouse/gold/marketing_customers")
    .saveAsTable("gold.marketing_customers")
)

print(f"marketing_customers built for {ds}: {df_out.count()} rows")

spark.stop()
