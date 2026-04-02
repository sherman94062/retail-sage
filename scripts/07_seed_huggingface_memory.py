"""
Seed ChromaDB memory for the Hugging Face data source.

Usage:
    python scripts/07_seed_huggingface_memory.py
"""

import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.datasources import HUGGINGFACE
from agent.memory import MemoryStore


def main():
    ds = HUGGINGFACE
    db_path = Path(ds.db_path)

    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        print("Run 06_generate_huggingface.py first.")
        return

    print(f"Seeding memory for: {ds.name}")
    memory = MemoryStore(ds.chroma_path)

    # Table descriptions
    print(f"\nSeeding {len(ds.table_descriptions)} table descriptions...")
    for table, desc in ds.table_descriptions.items():
        memory.add_table_description(table, desc)

    # Column glossary from schema
    print("Seeding column glossary...")
    conn = duckdb.connect(str(db_path), read_only=True)
    cols = conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        ORDER BY table_name, ordinal_position
    """).fetchall()
    for table, col, dtype in cols:
        readable = col.replace("_", " ")
        memory.add_column_definition(col, table, f"{readable} ({dtype}) in {table}")
    conn.close()

    # Sample queries
    print(f"Seeding {len(ds.sample_queries)} sample queries...")
    for i, q in enumerate(ds.sample_queries):
        memory.add_query(q["question"], q["sql"], q["summary"], query_id=f"hf_seed_{i:03d}")

    stats = memory.stats()
    print(f"\nDone! {stats['query_history']} queries, {stats['table_descriptions']} tables, {stats['column_glossary']} columns")


if __name__ == "__main__":
    main()
