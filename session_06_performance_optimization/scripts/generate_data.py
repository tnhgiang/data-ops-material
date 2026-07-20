import argparse

from pyspark.sql import functions as F
from spark_common import build_spark

SEED = 42
START = "2026-06-01 00:00:00"
SPAN_SECONDS = 39 * 24 * 3600   # 2026-06-01 .. 2026-07-10

SCALES = {
    "small":  dict(customers=100_000,   orders=500_000,    items=1_300_000,  payments=600_000),
    "medium": dict(customers=1_000_000, orders=5_000_000,  items=13_000_000, payments=6_000_000),
    "large":  dict(customers=4_000_000, orders=15_000_000, items=40_000_000, payments=18_000_000),
}

FIRST_NAMES = ["Ana", "Bruno", "Carla", "Diego", "Elena", "Felipe", "Gabi", "Hugo", "Igor", "Julia"]
LAST_NAMES = ["Silva", "Souza", "Costa", "Santos", "Oliveira", "Pereira", "Lima", "Gomes"]
CITIES = ["sao paulo", "rio de janeiro", "belo horizonte", "curitiba", "porto alegre", "salvador"]
STATES = ["SP", "RJ", "MG", "PR", "RS", "BA"]
CATEGORIES = ["electronics", "furniture", "toys", "health_beauty", "sports", "auto", "books", "garden"]
PAYMENT_TYPES = ["credit_card", "debit_card", "voucher", "boleto"]


def _pick(rand_col, values):
    expr = F.lit(values[-1])
    step = 1.0 / len(values)
    for i in range(len(values) - 2, -1, -1):
        expr = F.when(rand_col < step * (i + 1), F.lit(values[i])).otherwise(expr)
    return expr


def gen_customers(spark, n):
    r = F.rand(SEED)
    return (
        spark.range(1, n + 1).withColumnRenamed("id", "cid")
        .withColumn("customer_id", F.concat(F.lit("cust_"), F.lpad(F.col("cid").cast("string"), 7, "0")))
        .withColumn("customer_name", F.concat(
            _pick(F.rand(SEED + 1), FIRST_NAMES), F.lit(" "), _pick(F.rand(SEED + 2), LAST_NAMES)))
        .withColumn("customer_zip_code_prefix", F.lpad((F.rand(SEED + 3) * 90000 + 1000).cast("int").cast("string"), 5, "0"))
        .withColumn("customer_city", _pick(r, CITIES))
        .withColumn("customer_state", _pick(F.rand(SEED + 4), STATES))
        .drop("cid")
    )


def gen_orders(spark, n, n_customers):
    r = F.rand(SEED + 10)
    purchase = F.to_timestamp(F.from_unixtime(
        F.unix_timestamp(F.lit(START)) + (F.rand(SEED + 11) * SPAN_SECONDS).cast("long")))
    hot = F.rand(SEED + 12) < 0.30
    cust = F.when(hot, F.lit("cust_0000001")).otherwise(
        F.concat(F.lit("cust_"), F.lpad((F.rand(SEED + 13) * n_customers + 1).cast("long").cast("string"), 7, "0")))
    status = _pick(r, ["delivered", "delivered", "delivered", "shipped", "processing", "cancelled", "unavailable"])
    return (
        spark.range(1, n + 1).withColumnRenamed("id", "oid")
        .withColumn("order_id", F.concat(F.lit("ord_"), F.lpad(F.col("oid").cast("string"), 8, "0")))
        .withColumn("customer_id", cust)
        .withColumn("order_status", status)
        .withColumn("order_purchase_timestamp", purchase)
        .withColumn("order_approved_at",
                    (F.col("order_purchase_timestamp").cast("long") + (F.rand(SEED + 14) * 172800).cast("long")).cast("timestamp"))
        .withColumn("order_delivered_timestamp",
                    F.when(F.col("order_status") == "delivered",
                           (F.col("order_purchase_timestamp").cast("long") + (F.rand(SEED + 15) * 1123200 + 172800).cast("long")).cast("timestamp")))
        .withColumn("order_estimated_delivery_date",
                    F.to_date((F.col("order_purchase_timestamp").cast("long") + 864000).cast("timestamp")))
        .drop("oid")
    )


def gen_order_items(spark, n, n_orders):
    order_ref = F.concat(F.lit("ord_"), F.lpad((F.rand(SEED + 21) * n_orders + 1).cast("long").cast("string"), 8, "0"))
    # 35% of items are sold by one seller.
    hot = F.rand(SEED + 22) < 0.35
    seller = F.when(hot, F.lit("seller_00001")).otherwise(
        F.concat(F.lit("seller_"), F.lpad((F.rand(SEED + 23) * 50000 + 1).cast("int").cast("string"), 5, "0")))
    return (
        spark.range(1, n + 1).withColumnRenamed("id", "iid")
        .withColumn("order_id", order_ref)
        .withColumn("order_item_id", (F.pmod(F.col("iid"), F.lit(5)) + 1).cast("int"))
        .withColumn("product_id", F.concat(F.lit("prod_"), F.lpad((F.rand(SEED + 24) * 30000 + 1).cast("int").cast("string"), 5, "0")))
        .withColumn("seller_id", seller)
        .withColumn("product_category", _pick(F.rand(SEED + 25), CATEGORIES))
        .withColumn("price", F.round(F.rand(SEED + 26) * 995 + 5, 2))
        .withColumn("shipping_charges", F.round(F.rand(SEED + 27) * 48 + 2, 2))
        .drop("iid")
    )


def gen_payments(spark, n, n_orders):
    order_ref = F.concat(F.lit("ord_"), F.lpad((F.rand(SEED + 31) * n_orders + 1).cast("long").cast("string"), 8, "0"))
    ptype = _pick(F.rand(SEED + 32), PAYMENT_TYPES)
    return (
        spark.range(1, n + 1).withColumnRenamed("id", "pid")
        .withColumn("order_id", order_ref)
        .withColumn("payment_sequential", (F.pmod(F.col("pid"), F.lit(3)) + 1).cast("int"))
        .withColumn("payment_type", ptype)
        .withColumn("payment_installments",
                    F.when(F.col("payment_type") == "credit_card", (F.rand(SEED + 33) * 12 + 1).cast("int")).otherwise(F.lit(1)))
        .withColumn("payment_value", F.round(F.rand(SEED + 34) * 490 + 10, 2))
        .drop("pid")
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", default="large", choices=list(SCALES))
    p.add_argument("--output-dir", default="/data")
    args = p.parse_args()
    s = SCALES[args.scale]

    spark = build_spark("generate-data")

    def write(df, name):
        path = f"{args.output_dir}/{name}"
        df.write.mode("overwrite").parquet(path)
        print(f"  {name:14s} -> {path}")

    print(f"Generating '{args.scale}' scale...")
    write(gen_customers(spark, s["customers"]), "customers")
    write(gen_orders(spark, s["orders"], s["customers"]), "orders")
    write(gen_order_items(spark, s["items"], s["orders"]), "order_items")
    write(gen_payments(spark, s["payments"], s["orders"]), "payments")
    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
