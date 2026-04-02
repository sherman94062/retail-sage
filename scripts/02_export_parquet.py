"""
Phase 1, Step 2: Export DuckDB tables to Parquet files in data/raw/.

Large fact tables are exported as partitioned Parquet (by date key).
Dimension tables are exported as single Parquet files.

Usage:
    python scripts/02_export_parquet.py
"""

import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

# Fact tables to partition by their date surrogate key
PARTITIONED_TABLES = {
    "store_sales": "ss_sold_date_sk",
    "store_returns": "sr_returned_date_sk",
    "catalog_sales": "cs_sold_date_sk",
    "catalog_returns": "cr_returned_date_sk",
    "web_sales": "ws_sold_date_sk",
    "web_returns": "wr_returned_date_sk",
    "inventory": "inv_date_sk",
}

# Dimension tables — single file each
DIMENSION_TABLES = [
    "customer", "customer_address", "customer_demographics",
    "date_dim", "time_dim",
    "item", "promotion", "store", "warehouse",
    "call_center", "catalog_page", "web_site", "web_page",
    "household_demographics", "income_band", "ship_mode", "reason",
]


def export_table(conn: duckdb.DuckDBPyConnection, table: str, raw_path: Path,
                 partition_col: str | None = None) -> None:
    dest = raw_path / table
    dest.mkdir(parents=True, exist_ok=True)

    start = time.time()
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    if partition_col:
        # Partition by year bucket (date_sk / 365) to get manageable partition sizes
        conn.execute(f"""
            COPY (
                SELECT *, ({partition_col} / 365) AS _year_bucket
                FROM {table}
            ) TO '{dest}/'
            (FORMAT PARQUET, PARTITION_BY (_year_bucket), OVERWRITE_OR_IGNORE)
        """)
    else:
        conn.execute(f"""
            COPY {table} TO '{dest}/{table}.parquet'
            (FORMAT PARQUET)
        """)

    elapsed = time.time() - start
    print(f"  {table:<30s} {count:>12,} rows  ({elapsed:.1f}s)")


def main():
    project_root = Path(__file__).resolve().parent.parent
    db_path = project_root / "data" / "duckdb" / "retail_sage.duckdb"
    raw_path = project_root / "data" / "raw"

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        print("Run 01_generate_data.py first.")
        return

    conn = duckdb.connect(str(db_path), read_only=True)

    print("Exporting fact tables (partitioned by date key):")
    print("-" * 60)
    for table, col in PARTITIONED_TABLES.items():
        export_table(conn, table, raw_path, partition_col=col)

    print("\nExporting dimension tables:")
    print("-" * 60)
    for table in DIMENSION_TABLES:
        export_table(conn, table, raw_path)

    conn.close()

    # Report total size
    total_bytes = sum(f.stat().st_size for f in raw_path.rglob("*.parquet"))
    print(f"\nTotal Parquet size: {total_bytes / (1024**3):.2f} GB")
    print(f"Files written to: {raw_path}")


if __name__ == "__main__":
    main()
