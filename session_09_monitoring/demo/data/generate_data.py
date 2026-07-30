import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent
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
clean.to_parquet(OUT_DIR / "orders_clean.parquet", index=False)

broken = clean.copy()
broken.loc[rng.choice(n, 200, replace=False), "order_id"] = None
broken.loc[rng.choice(n, 150, replace=False), "amount"] = rng.uniform(-50_000, -1, 150).round(2)
broken.loc[100:109, "order_id"] = "ORD-000001"
broken.to_parquet(OUT_DIR / "orders_broken.parquet", index=False)

logger.info("Wrote %s and %s", OUT_DIR / "orders_clean.parquet", OUT_DIR / "orders_broken.parquet")
