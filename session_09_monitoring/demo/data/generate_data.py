import pandas as pd
import numpy as np
from datetime import date

rng = np.random.default_rng(42)
ORDER_DATE = date(2026, 8, 1)
n = 10000

clean = pd.DataFrame({
    "order_id":      np.arange(1, n + 1, dtype="int64"),
    "customer_id":   (101 + rng.integers(0, 5000, n)).astype("int32"),
    "customer_name": ["Customer " + str(101 + i % 5000) for i in range(n)],
    "amount":        rng.uniform(20.0, 3762.5, n).round(2),
    "order_date":    ORDER_DATE,
})
clean.to_parquet("orders_clean.parquet", index=False)

broken = clean.copy()
broken.loc[rng.choice(n, 200, replace=False), "customer_id"] = None
broken.loc[rng.choice(n, 150, replace=False), "amount"] = rng.uniform(-500, -1, 150).round(2)
broken.loc[rng.choice(n, 100, replace=False), "customer_name"] = None
broken.loc[100:109, "order_id"] = 1

broken.to_parquet("orders_broken.parquet", index=False)
