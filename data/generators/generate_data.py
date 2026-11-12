#!/usr/bin/env python3
"""Generate deterministic e-commerce CSV/JSON fixtures for the learning lab."""
import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write dictionaries as a headered CSV, including empty datasets safely."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def generate(output: Path, customers_count: int = 100, orders_count: int = 500, seed: int = 42) -> None:
    """Create small, intentionally imperfect source files with stable contents."""
    rng = random.Random(seed)
    output.mkdir(parents=True, exist_ok=True)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customers = [
        {"customer_id": f"C{i:04d}", "name": f"Customer {i}", "country": rng.choice(["EG", "US", "DE", "GB"]), "signup_date": (start - timedelta(days=rng.randrange(365))).date().isoformat()}
        for i in range(1, customers_count + 1)
    ]
    products = [{"product_id": f"P{i:03d}", "product_name": f"Product {i}", "category": rng.choice(["books", "electronics", "home"]), "list_price": round(rng.uniform(8, 150), 2)} for i in range(1, 31)]
    orders, items, payments, events = [], [], [], []
    for i in range(1, orders_count + 1):
        order_id = f"O{i:06d}"
        # C0001 receives disproportionate activity to make skew inspectable.
        customer_id = "C0001" if i % 7 == 0 else f"C{rng.randint(1, customers_count):04d}"
        timestamp = start + timedelta(hours=rng.randrange(24 * 90))
        selected = rng.choice(products)
        quantity = rng.randint(1, 4)
        amount = round(selected["list_price"] * quantity, 2)
        raw_timestamp = "not-a-timestamp" if i % 113 == 0 else timestamp.isoformat()
        orders.append({"order_id": order_id, "customer_id": "" if i % 97 == 0 else customer_id, "order_ts": raw_timestamp, "amount": amount if i % 89 else "bad", "status": rng.choice(["paid", "shipped", "completed"]), "ingested_at": (timestamp + timedelta(hours=1)).isoformat()})
        items.append({"order_id": order_id, "product_id": selected["product_id"], "quantity": quantity if i % 101 else -1, "unit_price": selected["list_price"] if i % 83 else ""})
        payments.append({"payment_id": f"PAY{i:06d}", "order_id": order_id, "method": rng.choice(["card", "wallet", "cash"]), "paid_amount": amount})
        events.append({"event_id": f"E{i:06d}", "customer_id": customer_id, "event_ts": (timestamp + timedelta(days=2 if i % 107 == 0 else 0)).isoformat(), "event_type": rng.choice(["view", "cart", "purchase"]), "attributes": {"source": rng.choice(["email", "search", "ad"])}})
    # A later duplicate tests deterministic deduplication.
    if orders:
        duplicate = dict(orders[0]); duplicate["ingested_at"] = (start + timedelta(days=120)).isoformat(); orders.append(duplicate)
    write_csv(output / "customers.csv", customers)
    write_csv(output / "products.csv", products)
    write_csv(output / "orders.csv", orders)
    write_csv(output / "order_items.csv", items)
    write_csv(output / "payments.csv", payments)
    with (output / "events.json").open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate(args.output, args.customers, args.orders, args.seed)
