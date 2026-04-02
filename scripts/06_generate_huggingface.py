"""
Fetch Hugging Face Model Hub data and load into DuckDB.

Pulls model metadata via the huggingface_hub API (no API key required)
and creates a DuckDB database with models and model_tags tables,
plus enriched mart tables.

Usage:
    python scripts/06_generate_huggingface.py                 # Default: 50K models
    python scripts/06_generate_huggingface.py --limit 10000   # Fewer for testing
    python scripts/06_generate_huggingface.py --limit 0       # All models (slow)
"""

import argparse
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
from huggingface_hub import list_models


def fetch_models(limit: int) -> pd.DataFrame:
    """Fetch model metadata from Hugging Face Hub."""
    print(f"Fetching models from Hugging Face Hub (limit={limit or 'ALL'})...")
    start = time.time()

    records = []
    for i, model in enumerate(list_models(sort="downloads", limit=limit or None)):
        if i % 5000 == 0 and i > 0:
            print(f"  ...fetched {i:,} models")

        # Extract author from model_id (format: "author/model-name")
        author = model.id.split("/")[0] if "/" in model.id else None

        records.append({
            "model_id": model.id,
            "author": author,
            "sha": model.sha,
            "created_at": model.created_at,
            "last_modified": model.last_modified,
            "private": model.private,
            "downloads": model.downloads,
            "likes": model.likes,
            "pipeline_tag": model.pipeline_tag,
            "library_name": model.library_name,
            "tags": model.tags,
        })

    elapsed = time.time() - start
    print(f"Fetched {len(records):,} models in {elapsed:.1f}s")
    return pd.DataFrame(records)


def build_tags_table(models_df: pd.DataFrame) -> pd.DataFrame:
    """Explode tags into a separate table."""
    rows = []
    for _, row in models_df.iterrows():
        if row["tags"]:
            for tag in row["tags"]:
                # Categorize tags
                tag_type = "other"
                if tag.startswith("license:"):
                    tag_type = "license"
                    tag = tag.replace("license:", "")
                elif tag.startswith("language:") or tag.startswith("lang:"):
                    tag_type = "language"
                    tag = tag.replace("language:", "").replace("lang:", "")
                elif tag.startswith("dataset:"):
                    tag_type = "dataset"
                    tag = tag.replace("dataset:", "")
                elif tag in ("pytorch", "tensorflow", "jax", "onnx", "safetensors",
                             "transformers", "diffusers", "gguf", "rust"):
                    tag_type = "framework"
                elif tag in ("bert", "gpt2", "gpt_neo", "gpt_neox", "llama", "mistral",
                             "falcon", "phi", "gemma", "qwen", "qwen2", "t5", "bart",
                             "roberta", "distilbert", "albert", "xlnet", "electra",
                             "clip", "vit", "stable-diffusion", "whisper", "wav2vec2",
                             "mbart", "xlm-roberta", "deberta", "bloom", "opt",
                             "codellama", "starcoder", "mixtral", "command-r"):
                    tag_type = "architecture"

                rows.append({
                    "model_id": row["model_id"],
                    "tag": tag,
                    "tag_type": tag_type,
                })
    return pd.DataFrame(rows)


def build_mart_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create enriched mart tables from raw models + tags."""

    # fct_models: main enriched fact table
    conn.execute("""
        CREATE OR REPLACE TABLE fct_models AS
        WITH arch AS (
            SELECT model_id, tag AS architecture
            FROM model_tags WHERE tag_type = 'architecture'
        ),
        lic AS (
            SELECT model_id, tag AS license
            FROM model_tags WHERE tag_type = 'license'
        ),
        lang AS (
            SELECT model_id, MIN(tag) AS primary_language
            FROM model_tags WHERE tag_type = 'language'
            GROUP BY model_id
        )
        SELECT
            m.*,
            a.architecture,
            l.license,
            la.primary_language
        FROM models m
        LEFT JOIN (SELECT model_id, MIN(architecture) AS architecture FROM arch GROUP BY model_id) a USING (model_id)
        LEFT JOIN (SELECT model_id, MIN(license) AS license FROM lic GROUP BY model_id) l USING (model_id)
        LEFT JOIN lang la USING (model_id)
    """)

    # dim_authors
    conn.execute("""
        CREATE OR REPLACE TABLE dim_authors AS
        SELECT
            author,
            COUNT(*) AS model_count,
            SUM(downloads) AS total_downloads,
            SUM(likes) AS total_likes,
            MIN(created_at) AS first_model_date,
            MAX(created_at) AS last_model_date,
            MODE(pipeline_tag) AS top_task
        FROM fct_models
        WHERE author IS NOT NULL
        GROUP BY author
    """)

    # model_task_summary
    conn.execute("""
        CREATE OR REPLACE TABLE model_task_summary AS
        SELECT
            pipeline_tag,
            COUNT(*) AS model_count,
            SUM(downloads) AS total_downloads,
            AVG(downloads) AS avg_downloads,
            SUM(likes) AS total_likes,
            AVG(likes) AS avg_likes
        FROM fct_models
        WHERE pipeline_tag IS NOT NULL
        GROUP BY pipeline_tag
    """)

    # architecture_trends
    conn.execute("""
        CREATE OR REPLACE TABLE architecture_trends AS
        SELECT
            architecture,
            EXTRACT(YEAR FROM created_at) AS year,
            COUNT(*) AS model_count,
            SUM(downloads) AS total_downloads,
            AVG(downloads) AS avg_downloads
        FROM fct_models
        WHERE architecture IS NOT NULL AND created_at IS NOT NULL
        GROUP BY architecture, year
    """)


def main():
    parser = argparse.ArgumentParser(description="Fetch Hugging Face model data")
    parser.add_argument("--limit", type=int, default=50000,
                        help="Max models to fetch (0 = all, default 50000)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    db_path = project_root / "data" / "duckdb" / "huggingface.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        print(f"Removing existing database at {db_path}")
        db_path.unlink()

    # Fetch data
    models_df = fetch_models(args.limit)

    # Build tags
    print("Building tags table...")
    tags_df = build_tags_table(models_df)
    print(f"  {len(tags_df):,} tag rows")

    # Drop the raw tags list column before loading
    models_clean = models_df.drop(columns=["tags"])

    # Load into DuckDB
    print(f"\nLoading into DuckDB at {db_path}...")
    conn = duckdb.connect(str(db_path))

    conn.execute("CREATE TABLE models AS SELECT * FROM models_clean")
    print(f"  models: {conn.execute('SELECT COUNT(*) FROM models').fetchone()[0]:,} rows")

    conn.execute("CREATE TABLE model_tags AS SELECT * FROM tags_df")
    print(f"  model_tags: {conn.execute('SELECT COUNT(*) FROM model_tags').fetchone()[0]:,} rows")

    # Build marts
    print("\nBuilding mart tables...")
    build_mart_tables(conn)

    for table in ["fct_models", "dim_authors", "model_task_summary", "architecture_trends"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    conn.close()
    print(f"\nDatabase size: {db_path.stat().st_size / (1024**2):.1f} MB")
    print("Done!")


if __name__ == "__main__":
    main()
