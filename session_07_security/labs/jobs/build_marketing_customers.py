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

# silver.customers columns:
#   customer_id, full_name, phone, email, cccd, date_of_birth, address,
#   segment, region, registered_at, account_balance, credit_score, updated_at

# TODO (Finding 1): this output exposes PII columns not needed by Marketing.
# The DPO confirmed business requirements:
#   - Cohort analysis: segment, region, registered_at
#   - Financial distribution: tier only (not raw account_balance or credit_score)
#   - Contactability: partially masked phone/email only
# Remove all columns that don't serve these requirements.
df_out = df.select(
    "customer_id",
    "full_name",  # not required
    "phone",  # Finding 2: needs partial masking, e.g. keep last 3 digits only
    "email",  # Finding 2: needs partial masking, e.g. mask local part
    "cccd",  # not required
    "date_of_birth",  # not required
    "address",  # not required
    "segment",
    "region",
    "registered_at",
    "account_balance",  # not required — use tier instead
    "credit_score",  # not required — use tier instead
).withColumn("updated_at", lit(ds).cast("date"))

# TODO (Finding 2): add partial masking for phone and email.
# Example:
#   phone: regexp_replace(col("phone"), r"^\d+(\d{3})$", "******$1")
#   email: regexp_replace(col("email"), r"^[^@]+", "***")

# TODO (Finding 3): check labs/README.md — the trino_rules_patch.json also needs fixing.

(
    df_out.write.format("delta")
    .mode("overwrite")
    .option("path", "s3a://gold/marketing_customers")
    .saveAsTable("gold.marketing_customers")
)

logger.info("marketing_customers built for %s: %d rows", ds, df_out.count())

spark.stop()
