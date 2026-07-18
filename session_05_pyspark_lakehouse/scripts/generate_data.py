from __future__ import annotations

import argparse
import csv
import logging
import os
import random
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generate_data")

STATUSES = ["completed", "cancelled", "pending"]
PRODUCTS = [f"PROD-{i:03d}" for i in range(1, 21)]


def _rng(ds: str) -> random.Random:
    # Seed from the run date so the same ds always produces the same file — the
    # generator has to be reproducible for a session about reproducibility.
    return random.Random(int(ds.replace("-", "")))


def _timestamp(rng: random.Random, ds: str) -> str:
    base = datetime.strptime(ds, "%Y-%m-%d")
    return (base + timedelta(seconds=rng.randint(0, 86399))).strftime("%Y-%m-%d %H:%M:%S")


def generate_row(rng: random.Random, ds: str, i: int) -> dict:
    return {
        "order_id": f"ORD-{ds}-{i:05d}",
        "customer_id": f"CUST-{rng.randint(1, 200):04d}",
        "product_id": rng.choice(PRODUCTS),
        # A slice of negative amounts and pending orders on purpose, so the
        # transform's "amount > 0" and status filters have something to drop.
        "amount": round(rng.uniform(-10, 500), 2),
        "quantity": rng.randint(1, 20),
        "order_status": rng.choice(STATUSES),
        "created_at": _timestamp(rng, ds),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Date YYYY-MM-DD")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--output-dir", default="data/raw")
    args = parser.parse_args()

    rng = _rng(args.ds)
    os.makedirs(args.output_dir, exist_ok=True)
    output_path = os.path.join(args.output_dir, f"{args.ds}.csv")

    fields = ["order_id", "customer_id", "product_id", "amount", "quantity", "order_status", "created_at"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for i in range(args.rows):
            writer.writerow(generate_row(rng, args.ds, i))

    log.info("Generated %d rows -> %s", args.rows, output_path)


if __name__ == "__main__":
    main()
