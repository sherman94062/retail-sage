"""
Phase 1, Step 1: Generate TPC-DS data using DuckDB's built-in tpcds extension.

Generates SF1 by default (fast, for validation). Pass --scale-factor 100 for the full dataset.
The data is written directly into a DuckDB database file.

Usage:
    python scripts/01_generate_data.py              # SF1 (~1GB, seconds)
    python scripts/01_generate_data.py --sf 10       # SF10 (~10GB, minutes)
    python scripts/01_generate_data.py --sf 100      # SF100 (~100GB, 20-40 min)
"""

import argparse
import sys
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

# All 24 TPC-DS tables
TPCDS_TABLES = [
    # Fact tables
    "store_sales", "store_returns",
    "catalog_sales", "catalog_returns",
    "web_sales", "web_returns",
    "inventory",
    # Dimension tables
    "customer", "customer_address", "customer_demographics",
    "date_dim", "time_dim",
    "item", "promotion", "store", "warehouse",
    "call_center", "catalog_page", "web_site", "web_page",
    "household_demographics", "income_band", "ship_mode", "reason",
]


def generate_data(sf: int, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-DS SF{sf} into {db_path}")
    print(f"This will create ~{sf}GB of data. Please be patient...\n")

    conn = duckdb.connect(str(db_path))

    # Install and load the tpcds extension
    conn.execute("INSTALL tpcds")
    conn.execute("LOAD tpcds")

    start = time.time()
    conn.execute(f"CALL dsdgen(sf={sf})")
    elapsed = time.time() - start
    print(f"\nData generation completed in {elapsed:.1f}s")

    # Verify: print row counts for all tables
    print("\nTable row counts:")
    print("-" * 45)
    total_rows = 0
    for table in TPCDS_TABLES:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            total_rows += count
            print(f"  {table:<30s} {count:>12,}")
        except duckdb.CatalogException:
            print(f"  {table:<30s} {'MISSING':>12s}")

    print("-" * 45)
    print(f"  {'TOTAL':<30s} {total_rows:>12,}")

    conn.close()
    print(f"\nDatabase saved to: {db_path}")
    print(f"Database size: {db_path.stat().st_size / (1024**3):.2f} GB")


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-DS data with DuckDB")
    parser.add_argument("--sf", type=int, default=1, help="Scale factor (default: 1)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    db_path = project_root / "data" / "duckdb" / "retail_sage.duckdb"

    if db_path.exists():
        print(f"WARNING: {db_path} already exists.")
        resp = input("Overwrite? [y/N] ").strip().lower()
        if resp != "y":
            print("Aborted.")
            sys.exit(0)
        db_path.unlink()

    generate_data(args.sf, db_path)


if __name__ == "__main__":
    main()
