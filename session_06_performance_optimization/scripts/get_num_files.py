import sys

from spark_common import build_spark

path = sys.argv[1] if len(sys.argv) > 1 else "s3a://warehouse/lab2_orders"

spark = build_spark("get-num-files")
spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").show(truncate=False)
df = spark.read.format("delta").load(path)
print("inputFiles() count:", len(df.inputFiles()))
print("rows:", df.count())
spark.stop()
