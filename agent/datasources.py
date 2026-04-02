"""
Data source configurations for Retail-SAGE.

Each data source defines its own database path, system prompt, few-shot examples,
table descriptions, and sample queries for memory seeding.
"""

from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class DataSource:
    """Configuration for a data source."""
    key: str                    # unique identifier
    name: str                   # display name
    description: str            # one-line description
    db_path: str                # path to DuckDB file
    chroma_path: str            # path to ChromaDB directory
    system_prompt: str          # domain-specific system prompt
    few_shot_examples: list[dict] = field(default_factory=list)
    table_descriptions: dict[str, str] = field(default_factory=dict)
    sample_queries: list[dict] = field(default_factory=list)
    example_questions: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Retail (TPC-DS)
# ---------------------------------------------------------------------------

RETAIL = DataSource(
    key="retail",
    name="Retail (TPC-DS)",
    description="Multi-channel retail analytics — store, catalog, and web sales",
    db_path=str(PROJECT_ROOT / "data" / "duckdb" / "retail_sage.duckdb"),
    chroma_path=str(PROJECT_ROOT / "data" / "chroma" / "retail"),
    system_prompt="""You are a retail analytics agent with access to a TPC-DS retail data lake \
covering store, catalog, and web sales channels spanning multiple years. You autonomously analyze \
data to answer business questions, diagnose metric changes, and identify root causes.

You have deep retail domain knowledge: multi-channel dynamics, seasonal patterns, \
return rate benchmarks, customer segmentation (RFM), and inventory turn expectations.

## Key Tables
- **Fact tables**: store_sales, catalog_sales, web_sales, store_returns, catalog_returns, web_returns, inventory
- **Dimension tables**: customer, item, store, date_dim, promotion, warehouse
- **Marts**: fct_sales (unified sales + dates), fct_returns, dim_customer (enriched), daily_channel_summary, customer_ltv
- **Channel codes**: 'store', 'catalog', 'web'
""",
    few_shot_examples=[
        {
            "question": "What was our total revenue last year by channel?",
            "reasoning": "Query fct_sales grouped by channel for the most recent complete year.",
            "sql": "SELECT channel, SUM(ext_sales_price) AS gross_revenue, SUM(net_paid) AS net_revenue FROM fct_sales WHERE d_year = (SELECT MAX(d_year) - 1 FROM fct_sales WHERE d_date IS NOT NULL) GROUP BY channel ORDER BY gross_revenue DESC",
            "why": "Used **fct_sales** mart because it unifies all three channels with date fields pre-joined. Lineage: stg_tpcds__*_sales → int_sales_unified → fct_sales.",
        },
    ],
    example_questions=[
        "Total revenue last year by channel?",
        "Top product categories by return rate?",
        "Monthly customer count trend",
        "Top 10 stores by net profit",
        "Weekend vs weekday performance",
    ],
)


# ---------------------------------------------------------------------------
# Hugging Face Model Hub
# ---------------------------------------------------------------------------

HUGGINGFACE = DataSource(
    key="huggingface",
    name="AI/ML Models (Hugging Face)",
    description="Hugging Face Model Hub — downloads, tasks, architectures, licenses, trends",
    db_path=str(PROJECT_ROOT / "data" / "duckdb" / "huggingface.duckdb"),
    chroma_path=str(PROJECT_ROOT / "data" / "chroma" / "huggingface"),
    system_prompt="""You are an AI/ML analytics agent with access to a comprehensive dataset of \
models from the Hugging Face Model Hub. You analyze trends in model adoption, architecture \
popularity, task coverage, licensing patterns, and community engagement.

You have deep knowledge of the ML ecosystem: transformer architectures (encoder, decoder, \
encoder-decoder), model families (BERT, GPT, LLaMA, Mistral, T5, CLIP, Whisper, Stable Diffusion), \
task types (text-generation, text-classification, image-classification, etc.), and the open-source \
AI landscape.

## Key Tables
- **models** — Core model metadata: id, author, downloads, likes, pipeline_tag, library, created_at, last_modified
- **model_tags** — Tags associated with each model (architecture, language, dataset, license)
- **daily_model_stats** — Download/like snapshots over time (if available)
- **Marts**: fct_models (enriched model facts), dim_authors (author profiles), model_task_summary, architecture_trends
""",
    few_shot_examples=[
        {
            "question": "Which model architectures have the most downloads?",
            "reasoning": "Group fct_models by architecture tag, sum downloads, rank by total.",
            "sql": "SELECT architecture, COUNT(*) AS model_count, SUM(downloads) AS total_downloads, AVG(downloads) AS avg_downloads FROM fct_models WHERE architecture IS NOT NULL GROUP BY architecture ORDER BY total_downloads DESC LIMIT 20",
            "why": "Used **fct_models** mart which joins model metadata with extracted architecture tags. Lineage: models + model_tags → fct_models.",
        },
        {
            "question": "How has the mix of model tasks changed over time?",
            "reasoning": "Group models by creation year/quarter and pipeline_tag, count models per bucket.",
            "sql": "SELECT EXTRACT(YEAR FROM created_at) AS year, EXTRACT(QUARTER FROM created_at) AS quarter, pipeline_tag, COUNT(*) AS model_count FROM fct_models WHERE created_at IS NOT NULL AND pipeline_tag IS NOT NULL GROUP BY year, quarter, pipeline_tag ORDER BY year, quarter, model_count DESC",
            "why": "Used **fct_models** with created_at for time series. Pipeline_tag is the HF-assigned task category (text-generation, image-classification, etc.).",
        },
    ],
    table_descriptions={
        "models": "Core Hugging Face model metadata: model ID, author, download counts, likes, pipeline task, ML library, creation and modification dates",
        "model_tags": "Tags for each model including architecture, language, dataset, license, and other community-assigned labels",
        "fct_models": "Enriched model fact table with architecture, license, language, and task extracted from tags. Main table for analysis.",
        "dim_authors": "Author/organization profiles with model counts, total downloads, and top tasks",
        "model_task_summary": "Pre-aggregated summary by pipeline task: model counts, total downloads, avg likes",
        "architecture_trends": "Model counts and downloads by architecture family and creation year",
    },
    sample_queries=[
        {"question": "Which model architectures have the most downloads?", "sql": "SELECT architecture, SUM(downloads) AS total_downloads FROM fct_models WHERE architecture IS NOT NULL GROUP BY architecture ORDER BY total_downloads DESC LIMIT 20", "summary": "Top architectures ranked by total downloads"},
        {"question": "What are the most popular model tasks?", "sql": "SELECT pipeline_tag, COUNT(*) AS model_count, SUM(downloads) AS total_downloads FROM fct_models WHERE pipeline_tag IS NOT NULL GROUP BY pipeline_tag ORDER BY total_downloads DESC", "summary": "Task types ranked by download volume"},
        {"question": "Who are the top model publishers?", "sql": "SELECT author, COUNT(*) AS models, SUM(downloads) AS total_downloads FROM fct_models GROUP BY author ORDER BY total_downloads DESC LIMIT 20", "summary": "Top authors/orgs by total downloads across all their models"},
        {"question": "How has model creation volume trended over time?", "sql": "SELECT EXTRACT(YEAR FROM created_at) AS year, EXTRACT(MONTH FROM created_at) AS month, COUNT(*) AS new_models FROM fct_models WHERE created_at IS NOT NULL GROUP BY year, month ORDER BY year, month", "summary": "Monthly count of new models published on HuggingFace"},
        {"question": "What licenses are most common for popular models?", "sql": "SELECT license, COUNT(*) AS model_count, SUM(downloads) AS total_downloads FROM fct_models WHERE license IS NOT NULL GROUP BY license ORDER BY total_downloads DESC LIMIT 15", "summary": "License distribution weighted by downloads"},
        {"question": "Open vs proprietary model adoption trends", "sql": "SELECT EXTRACT(YEAR FROM created_at) AS year, CASE WHEN license IN ('mit', 'apache-2.0', 'gpl-3.0', 'bsd-3-clause', 'cc-by-4.0', 'openrail') THEN 'open' ELSE 'other' END AS license_type, COUNT(*) AS models, SUM(downloads) AS downloads FROM fct_models WHERE created_at IS NOT NULL AND license IS NOT NULL GROUP BY year, license_type ORDER BY year", "summary": "Year-over-year open vs other license model counts and downloads"},
    ],
    example_questions=[
        "Top model architectures by downloads?",
        "How has text-generation grown vs other tasks?",
        "Who are the top 10 model publishers?",
        "Open vs proprietary license trends",
        "Most downloaded models this year",
    ],
)


# Registry
ALL_SOURCES = {
    "retail": RETAIL,
    "huggingface": HUGGINGFACE,
}
DEFAULT_SOURCE = "retail"
