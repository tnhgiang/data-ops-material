import os

import duckdb

DATA_DIR = os.environ.get("DATA_DIR", "data")
orders_glob = os.path.join(DATA_DIR, "orders", "*.parquet")
db_path = os.path.join(DATA_DIR, "hw3.duckdb")

con = duckdb.connect(db_path)
con.execute("DROP TABLE IF EXISTS orders")
con.execute(f"CREATE TABLE orders AS SELECT * FROM read_parquet('{orders_glob}')")
n = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
print(f"Loaded orders into {db_path}: {n:,} rows\n")

query = """
EXPLAIN ANALYZE
SELECT *
FROM orders
WHERE order_approved_at >= NOW() - INTERVAL '30 days'
  AND order_status = 'delivered'
ORDER BY order_approved_at DESC;
"""

print("=== EXPLAIN ANALYZE ===")
plan = con.execute(query).fetchall()
print("\n".join(row[1] for row in plan))
print("\nHow many rows survive the filter, and does the result depend on when you run it?")
con.close()
