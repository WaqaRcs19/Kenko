"""Simple ETL pipeline: extract from CSV, transform, load into SQLite."""

import csv
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("etl")

SOURCE_CSV = Path("data/sales.csv")
TARGET_DB = Path("data/warehouse.db")

SAMPLE_ROWS = [
    ["order_id", "customer", "product", "quantity", "unit_price", "order_date"],
    ["1", " alice ", "Widget", "3", "9.99", "2026-10-01"],
    ["2", "BOB", "Gadget", "1", "24.50", "2026-10-02"],
    ["3", "carol", "widget", "", "9.99", "2026-10-02"],
    ["4", "dave", "Gizmo", "2", "abc", "2026-10-03"],
    ["5", "Eve", "gadget", "4", "24.50", "2026-10-04"],
    ["6", "waqar hassan", "Widget", "2", "9.99", "2026-10-05"],
]


def ensure_sample_data(path: Path) -> None:
    """Create a small sample CSV so the script runs out of the box."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(SAMPLE_ROWS)
    log.info("Created sample data at %s", path)


def extract(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    log.info("Extracted %d rows from %s", len(rows), path)
    return rows


def transform(rows: list[dict]) -> list[dict]:
    clean = []
    for row in rows:
        try:
            quantity = int(row["quantity"])
            unit_price = float(row["unit_price"])
        except (ValueError, KeyError):
            log.warning("Skipping invalid row: %s", row)
            continue

        clean.append({
            "order_id": int(row["order_id"]),
            "customer": row["customer"].strip().title(),
            "product": row["product"].strip().title(),
            "quantity": quantity,
            "unit_price": unit_price,
            "total": round(quantity * unit_price, 2),
            "order_date": row["order_date"],
        })
    log.info("Transformed %d rows (%d dropped)", len(clean), len(rows) - len(clean))
    return clean


def load(rows: list[dict], db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                order_id   INTEGER PRIMARY KEY,
                customer   TEXT,
                product    TEXT,
                quantity   INTEGER,
                unit_price REAL,
                total      REAL,
                order_date TEXT
            )
        """)
        conn.executemany("""
            INSERT OR REPLACE INTO sales
            VALUES (:order_id, :customer, :product, :quantity, :unit_price, :total, :order_date)
        """, rows)
    log.info("Loaded %d rows into %s", len(rows), db_path)


def run() -> None:
    ensure_sample_data(SOURCE_CSV)
    rows = extract(SOURCE_CSV)
    clean = transform(rows)
    load(clean, TARGET_DB)

    with sqlite3.connect(TARGET_DB) as conn:
        for product, revenue in conn.execute(
            "SELECT product, SUM(total) FROM sales GROUP BY product ORDER BY 2 DESC"
        ):
            print(f"{product:<10} {revenue:>8.2f}")


if __name__ == "__main__":
    run()
