import logging
import os

import duckdb

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s"
)
log = logging.getLogger(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "data")
db_path = os.path.join(DATA_DIR, "hw3.duckdb")

con = duckdb.connect(db_path)
# Bronze parquet lives on MinIO; DuckDB reads it over the exposed S3 API.
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='localhost:9000'")
con.execute("SET s3_access_key_id='minioadmin'")
con.execute("SET s3_secret_access_key='minioadmin'")
con.execute("SET s3_use_ssl=false")
con.execute("SET s3_url_style='path'")

con.execute("DROP TABLE IF EXISTS orders")
con.execute("CREATE TABLE orders AS SELECT * FROM read_parquet('s3://bronze/orders/*.parquet')")
n = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
log.info("Loaded orders into %s: %s rows", db_path, f"{n:,}")

query = """
EXPLAIN ANALYZE
SELECT *
FROM orders
WHERE order_approved_at >= NOW() - INTERVAL '30 days'
  AND order_status = 'delivered'
ORDER BY order_approved_at DESC;
"""

plan = con.execute(query).fetchall()
log.info("EXPLAIN ANALYZE:\n%s", "\n".join(row[1] for row in plan))
con.close()
