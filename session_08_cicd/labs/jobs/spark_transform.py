from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(df: DataFrame, ds: str) -> DataFrame:
    return (
        df.filter(
            F.col("amount") > 0
        ).withColumn(  # TODO Bug3: NULL customer_id rows are not filtered
            "processed_at", F.current_timestamp()
        )  # TODO Bug1: non-deterministic — retrying produces different processed_at values
    )
