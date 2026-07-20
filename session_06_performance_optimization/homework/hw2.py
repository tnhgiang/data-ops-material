from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from spark_common import build_spark

spark = build_spark("hw2", {"spark.sql.adaptive.enabled": "false"})

orders = spark.read.format("delta").load("s3a://warehouse/orders")
order_items = spark.read.format("delta").load("s3a://warehouse/order_items")
payments = spark.read.format("delta").load("s3a://warehouse/payments")


@udf(returnType=DoubleType())
def risk_score(payment_type, installments, value):
    base = {"credit_card": 30.0, "debit_card": 10.0, "voucher": 5.0, "boleto": 15.0}.get(
        payment_type, 0.0
    )
    return float(base + installments * 2.0 + (value or 0.0) * 0.01)


payments_scored = payments.withColumn(
    "score",
    risk_score(F.col("payment_type"), F.col("payment_installments"), F.col("payment_value")),
)

r1 = orders.join(payments_scored, "order_id").groupBy("order_status").agg(F.sum("score").alias("s"))
r2 = (
    order_items.join(payments_scored, "order_id")
    .groupBy("product_category")
    .agg(F.sum("score").alias("s"))
)
r3 = payments_scored.groupBy("payment_type").agg(F.avg("score").alias("s"))

print("by status:")
r1.show()
print("by category:")
r2.show()
print("by type:")
r3.show()
print(
    "Open :18080 -> SQL/DataFrame tab: how often does the payments computation appear in the plan?"
)

spark.stop()
