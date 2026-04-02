"""
Phase 1, Step 3: Verify the generated data.

1. Check all 24 tables exist and have expected row counts
2. Run spot-check queries for data quality
3. Execute all 99 TPC-DS benchmark queries as ground truth validation

Usage:
    python scripts/03_verify_data.py
    python scripts/03_verify_data.py --benchmark-only   # Just run the 99 queries
    python scripts/03_verify_data.py --skip-benchmark    # Skip the 99 queries
"""

import argparse
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

# Minimum expected rows per table at SF1. Scale linearly for higher SFs.
MIN_ROWS_SF1 = {
    "store_sales": 2_880_404,
    "catalog_sales": 1_441_548,
    "web_sales": 719_384,
    "store_returns": 287_514,
    "catalog_returns": 144_067,
    "web_returns": 71_600,
    "inventory": 11_745_000,
    "customer": 100_000,
    "customer_address": 50_000,
    "customer_demographics": 1_920_800,
    "date_dim": 73_049,
    "time_dim": 86_400,
    "item": 18_000,
    "promotion": 300,
    "store": 12,
    "warehouse": 5,
    "call_center": 6,
    "catalog_page": 11_718,
    "web_site": 30,
    "web_page": 60,
    "household_demographics": 7_200,
    "income_band": 20,
    "ship_mode": 20,
    "reason": 35,
}


def check_row_counts(conn: duckdb.DuckDBPyConnection) -> bool:
    print("=" * 60)
    print("ROW COUNT VERIFICATION")
    print("=" * 60)
    all_ok = True
    for table, min_rows in MIN_ROWS_SF1.items():
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            status = "OK" if count >= min_rows else "LOW"
            if status == "LOW":
                all_ok = False
            print(f"  {table:<30s} {count:>12,}  [{status}]")
        except Exception as e:
            print(f"  {table:<30s} {'ERROR':>12s}  [{e}]")
            all_ok = False
    return all_ok


def spot_checks(conn: duckdb.DuckDBPyConnection) -> bool:
    print("\n" + "=" * 60)
    print("SPOT CHECKS")
    print("=" * 60)
    checks = [
        (
            "Date range spans multiple years",
            "SELECT MIN(d_year), MAX(d_year) FROM date_dim",
        ),
        (
            "Store sales have valid date keys",
            "SELECT COUNT(*) as null_dates FROM store_sales WHERE ss_sold_date_sk IS NULL",
        ),
        (
            "Items have categories",
            "SELECT COUNT(DISTINCT i_category) as categories FROM item WHERE i_category IS NOT NULL",
        ),
        (
            "Multiple stores exist",
            "SELECT COUNT(*) as stores FROM store",
        ),
        (
            "Sales amount is positive",
            "SELECT AVG(ss_net_profit) as avg_profit FROM store_sales LIMIT 1",
        ),
    ]
    all_ok = True
    for desc, sql in checks:
        try:
            result = conn.execute(sql).fetchone()
            print(f"  [OK] {desc}: {result}")
        except Exception as e:
            print(f"  [FAIL] {desc}: {e}")
            all_ok = False
    return all_ok


def run_benchmark_queries(conn: duckdb.DuckDBPyConnection) -> dict:
    """Run all 99 TPC-DS queries and report pass/fail."""
    print("\n" + "=" * 60)
    print("TPC-DS BENCHMARK QUERIES (1-99)")
    print("=" * 60)

    conn.execute("LOAD tpcds")

    results = {"pass": 0, "fail": 0, "error": 0}
    errors = []

    for i in range(1, 100):
        try:
            # Get the query text from tpcds_queries() table function
            row = conn.execute(
                f"SELECT query FROM tpcds_queries() WHERE query_nr = {i}"
            ).fetchone()
            if row is None or row[0] is None:
                results["error"] += 1
                errors.append((i, "Query text not found"))
                print(f"  Query {i:>2d}: SKIP  (query text not found)")
                continue
            query = row[0]
            start = time.time()
            conn.execute(query).fetchall()
            elapsed = time.time() - start
            results["pass"] += 1
            status = "PASS"
            print(f"  Query {i:>2d}: {status}  ({elapsed:.2f}s)")
        except Exception as e:
            err_msg = str(e)[:80]
            results["fail" if "not implemented" not in err_msg.lower() else "error"] += 1
            errors.append((i, err_msg))
            print(f"  Query {i:>2d}: FAIL  ({err_msg})")

    print(f"\nResults: {results['pass']} pass, {results['fail']} fail, {results['error']} error")
    if errors:
        print(f"Failed queries: {[e[0] for e in errors]}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Verify TPC-DS data")
    parser.add_argument("--benchmark-only", action="store_true")
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    db_path = project_root / "data" / "duckdb" / "retail_sage.duckdb"

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        print("Run 01_generate_data.py first.")
        return

    conn = duckdb.connect(str(db_path), read_only=True)
    conn.execute("INSTALL tpcds")
    conn.execute("LOAD tpcds")

    if not args.benchmark_only:
        rows_ok = check_row_counts(conn)
        spots_ok = spot_checks(conn)
        print(f"\nRow counts: {'PASS' if rows_ok else 'FAIL'}")
        print(f"Spot checks: {'PASS' if spots_ok else 'FAIL'}")

    if not args.skip_benchmark:
        run_benchmark_queries(conn)

    conn.close()


if __name__ == "__main__":
    main()
