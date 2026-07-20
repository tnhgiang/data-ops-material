import sys

from spark_common import build_spark

path = sys.argv[1] if len(sys.argv) > 1 else "s3a://warehouse/lab2_orders"

spark = build_spark("compact-files")
before = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles").first()[0]
spark.sql(f"OPTIMIZE delta.`{path}`")
after = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles").first()[0]
print(f"numFiles: {before} -> {after}")
spark.stop()
