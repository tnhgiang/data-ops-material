import argparse

from pyspark.sql.functions import lit
from security_utils import get_logger, get_spark_session, get_vault_secret

logger = get_logger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument("--ds", required=True)
args = parser.parse_args()
ds = args.ds

minio_secret = get_vault_secret("dataops/minio")

spark = get_spark_session("build-marketing-customers", minio_secret)
spark.sparkContext.setLogLevel("WARN")

df = spark.table("silver.customers")

df_out = df.withColumn("updated_at", lit(ds).cast("date"))

(
    df_out.write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://gold/marketing_customers")
    .saveAsTable("gold.marketing_customers")
)

logger.info("marketing_customers built for %s: %d rows", ds, df_out.count())

spark.stop()
