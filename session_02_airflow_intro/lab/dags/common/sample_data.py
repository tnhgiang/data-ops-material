"""Deterministic sample order data for lab exercises."""

from __future__ import annotations

from datetime import datetime, timedelta


def generate_orders(logical_date: datetime, count: int = 5) -> list[dict]:
    """Same logical_date always yields the same records — reruns stay deterministic."""
    base_day = int(logical_date.strftime("%Y%m%d"))
    orders = []
    for index in range(count):
        order_id = base_day * 1000 + index
        orders.append(
            {
                "order_id": order_id,
                "customer_id": 100 + (index % 3),
                "amount": round(10.0 + (index * 3.5), 2),
                "currency": "USD",
                "order_ts": (logical_date + timedelta(hours=index)).isoformat(),
                "status": "completed",
            }
        )
    return orders


def enrich_orders(orders: list[dict]) -> list[dict]:
    """Simple transform step used by the curated layer."""
    enriched = []
    for order in orders:
        row = dict(order)
        row["amount_usd"] = row["amount"]
        row["is_high_value"] = row["amount"] >= 15.0
        enriched.append(row)
    return enriched
