import pandas as pd
import numpy as np

rng = np.random.default_rng(42)
n = 10000

statuses = ["pending", "paid", "cancelled", "refunded"]

clean = pd.DataFrame({
    "order_id": [f"ORD-{i:06d}" for i in range(n)],
    "customer_id": [f"CUST-{rng.integers(1, 1000):04d}" for _ in range(n)],
    "amount": rng.uniform(10_000, 500_000, n).round(2),
    "status": rng.choice(statuses, n),
    "order_date": pd.date_range("2026-01-01", periods=n, freq="1min").date,
})
clean.to_parquet("orders_clean.parquet", index=False)

broken = clean.copy()
# null order_id
broken.loc[rng.choice(n, 200, replace=False), "order_id"] = None
# negative amount
broken.loc[rng.choice(n, 150, replace=False), "amount"] = rng.uniform(-50_000, -1, 150).round(2)
# duplicate order_id
broken.loc[100:109, "order_id"] = "ORD-000001"

broken.to_parquet("orders_broken.parquet", index=False)
