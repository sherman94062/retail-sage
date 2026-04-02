"""
Phase 4: Seed ChromaDB with table descriptions, column glossary,
and TPC-DS benchmark queries paraphrased as natural language questions.

Usage:
    python scripts/04_seed_memory.py
"""

from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.memory import MemoryStore

# Table descriptions for semantic search
TABLE_DESCRIPTIONS = {
    "store_sales": "In-store point-of-sale transactions including quantity, prices, discounts, tax, and profit for physical retail locations",
    "catalog_sales": "Catalog and mail-order sales transactions with shipping costs, warehouse info, and customer billing/shipping addresses",
    "web_sales": "E-commerce web sales transactions with web site, page, and shipping information",
    "store_returns": "Merchandise returns at physical store locations with return amounts, reasons, and customer info",
    "catalog_returns": "Catalog/mail-order returns with refund amounts, return reasons, and shipping details",
    "web_returns": "E-commerce web returns with refund amounts, return reasons, and original order references",
    "inventory": "Daily inventory snapshots showing quantity on hand by item and warehouse, used for stock analysis",
    "customer": "Customer master data with demographic keys, addresses, birth info, and preferred customer flags",
    "customer_address": "Customer mailing addresses with city, state, zip, country, and GMT offset",
    "customer_demographics": "Customer demographic attributes including gender, marital status, education, credit rating, dependents",
    "date_dim": "Date dimension with calendar year, quarter, month, day of week, holiday flags, and fiscal period attributes",
    "time_dim": "Time-of-day dimension with hour, minute, second, and time shift (morning/afternoon/evening)",
    "item": "Product/item master data with category, class, brand, manufacturer, size, color, and pricing",
    "promotion": "Promotional campaigns with channel flags (email, catalog, TV, radio), discount type, and date ranges",
    "store": "Physical retail store locations with address, manager, market info, floor space, and employee count",
    "warehouse": "Warehouse/distribution center locations with address, square footage, and name",
    "call_center": "Call center locations for catalog orders with employee count, hours, and manager info",
    "catalog_page": "Catalog page metadata with page number, department, catalog type, and description",
    "web_site": "E-commerce website metadata with URL, open/close dates, and market info",
    "web_page": "Individual web page metadata with page type, access date range, and customer count",
    "household_demographics": "Household demographic segments with buy potential, dependents, and vehicle count",
    "income_band": "Income bracket reference data with lower and upper bound amounts",
    "ship_mode": "Shipping method reference data with carrier, mode type, and delivery speed code",
    "reason": "Return reason codes and descriptions",
    "fct_sales": "Unified sales fact table combining store, catalog, and web channels with date dimension fields (year, month, quarter, holiday). Use this for cross-channel revenue analysis.",
    "fct_returns": "Unified returns fact table combining all three return channels with date fields. Use for return rate analysis.",
    "dim_customer": "Enriched customer dimension with demographics, address, household info, and income bands joined together",
    "dim_item": "Product/item dimension for joining with sales facts by item_sk",
    "dim_store": "Store dimension for geographic and store-level analysis",
    "dim_date": "Date dimension for time-based filtering and grouping",
    "daily_channel_summary": "Pre-aggregated daily metrics by channel: transaction count, gross/net sales, profit, unique customers",
    "customer_ltv": "Customer lifetime value metrics: first/last purchase, total transactions, lifetime sales, channels used",
    "int_sales_unified": "Intermediate unified sales across all three channels with standardized column names",
    "int_returns_unified": "Intermediate unified returns across all three channels with standardized column names",
    "int_customer_profile": "Intermediate enriched customer profile joining demographics, address, household, and income",
    "int_item_performance": "Intermediate item-level sales and return aggregations by channel with return rates",
}

# Sample TPC-DS queries paraphrased as natural language questions
SAMPLE_QUERIES = [
    {
        "question": "What are the top revenue-generating product categories by channel?",
        "sql": "SELECT i.i_category, s.channel, SUM(s.ext_sales_price) AS revenue FROM fct_sales s JOIN item i ON s.item_sk = i.i_item_sk GROUP BY i.i_category, s.channel ORDER BY revenue DESC LIMIT 20",
        "summary": "Shows product category revenue breakdown across store, catalog, and web channels",
    },
    {
        "question": "Which states have the highest customer concentration?",
        "sql": "SELECT ca_state, COUNT(*) AS customer_count FROM dim_customer WHERE ca_state IS NOT NULL GROUP BY ca_state ORDER BY customer_count DESC LIMIT 20",
        "summary": "Geographic distribution of customers by state",
    },
    {
        "question": "What is the monthly revenue trend for the last 3 years?",
        "sql": "SELECT d_year, d_month, SUM(gross_sales) AS total_sales FROM daily_channel_summary GROUP BY d_year, d_month ORDER BY d_year, d_month",
        "summary": "Monthly revenue trend across all channels",
    },
    {
        "question": "What is the return rate by product category and channel?",
        "sql": "SELECT i_category, channel, SUM(total_quantity_returned)::FLOAT / NULLIF(SUM(total_quantity_sold), 0) * 100 AS return_rate FROM int_item_performance GROUP BY i_category, channel ORDER BY return_rate DESC",
        "summary": "Return rates showing which categories and channels have the most returns",
    },
    {
        "question": "Who are our highest lifetime value customers?",
        "sql": "SELECT customer_sk, c_first_name, c_last_name, lifetime_gross_sales, total_transactions, channels_used FROM customer_ltv ORDER BY lifetime_gross_sales DESC LIMIT 20",
        "summary": "Top customers ranked by lifetime gross sales value",
    },
    {
        "question": "How does weekend vs weekday sales performance compare?",
        "sql": "SELECT CASE WHEN is_weekend = 'Y' THEN 'Weekend' ELSE 'Weekday' END AS day_type, channel, AVG(ext_sales_price) AS avg_sale, COUNT(*) AS txn_count FROM fct_sales WHERE d_date IS NOT NULL GROUP BY day_type, channel",
        "summary": "Comparison of average sale amounts and transaction volumes on weekends vs weekdays",
    },
    {
        "question": "What is the average order value by channel over time?",
        "sql": "SELECT d_year, d_month, channel, AVG(ext_sales_price) AS avg_order_value FROM fct_sales WHERE d_date IS NOT NULL GROUP BY d_year, d_month, channel ORDER BY d_year, d_month",
        "summary": "Average order value trends by month and sales channel",
    },
    {
        "question": "Which promotions drove the most incremental revenue?",
        "sql": "SELECT p.p_promo_name, p.p_channel_email, p.p_channel_catalog, p.p_channel_tv, SUM(s.ext_sales_price) AS promo_revenue, COUNT(*) AS promo_txns FROM fct_sales s JOIN promotion p ON s.promo_sk = p.p_promo_sk WHERE s.promo_sk IS NOT NULL GROUP BY p.p_promo_name, p.p_channel_email, p.p_channel_catalog, p.p_channel_tv ORDER BY promo_revenue DESC LIMIT 20",
        "summary": "Top promotions by revenue generated with channel breakdown",
    },
]


def main():
    project_root = Path(__file__).resolve().parent.parent
    chroma_path = project_root / "data" / "chroma"
    db_path = project_root / "data" / "duckdb" / "retail_sage.duckdb"

    print("Initializing ChromaDB memory store...")
    memory = MemoryStore(chroma_path)

    # Seed table descriptions
    print(f"\nSeeding {len(TABLE_DESCRIPTIONS)} table descriptions...")
    for table, desc in TABLE_DESCRIPTIONS.items():
        memory.add_table_description(table, desc)
    print(f"  Done. {memory.table_descriptions.count()} table descriptions indexed.")

    # Seed column glossary from the database schema
    if db_path.exists():
        print("\nSeeding column glossary from database schema...")
        conn = duckdb.connect(str(db_path), read_only=True)
        cols = conn.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            ORDER BY table_name, ordinal_position
        """).fetchall()
        for table, col, dtype in cols:
            # Generate a basic definition from the column name
            readable = col.replace("_", " ").replace("sk", "surrogate key")
            memory.add_column_definition(col, table, f"{readable} ({dtype}) in {table}")
        conn.close()
        print(f"  Done. {memory.column_glossary.count()} column definitions indexed.")
    else:
        print(f"\nWARNING: Database not found at {db_path}. Skipping column glossary seeding.")
        print("Run 01_generate_data.py first, then re-run this script.")

    # Seed sample queries
    print(f"\nSeeding {len(SAMPLE_QUERIES)} sample queries...")
    for i, q in enumerate(SAMPLE_QUERIES):
        memory.add_query(q["question"], q["sql"], q["summary"], query_id=f"seed_{i:03d}")
    print(f"  Done. {memory.query_history.count()} queries indexed.")

    # Summary
    print("\n" + "=" * 50)
    print("Memory seeding complete!")
    stats = memory.stats()
    for collection, count in stats.items():
        print(f"  {collection}: {count} entries")


if __name__ == "__main__":
    main()
