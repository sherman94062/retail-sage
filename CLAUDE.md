# CLAUDE.md — Retail-SAGE Project
## Semantic Analytics & Governed Execution — AI-Powered Retail Analytics Agent

---

## Project Overview

Retail-SAGE is a local, end-to-end AI analytics agent inspired by Meta's internal Analytics Agent
(described in their March 2026 Medium post). The system demonstrates that enterprise-grade
autonomous data analysis — SQL generation, multi-step reasoning, metric diagnosis, root cause
analysis — can be built on commodity hardware without cloud infrastructure.

**Core thesis**: A constrained, well-seeded domain (retail analytics) + a powerful reasoning model
(Claude) + a fast local query engine (DuckDB) + a semantic layer (dbt + MetricFlow) = an analytics
agent that rivals what Fortune 100 companies run in the cloud.

**Hardware**: MacBook M3 Pro, 36GB unified RAM, ~400GB SSD  
**Primary model**: Claude Sonnet (via Claude Max plan / MCP)  
**Owner**: Mike Sherman (sherman78641@gmail.com)

---

## Goals

1. **Portfolio piece** for Staff Data Engineer, AI Success Architect, and Solutions Engineer roles
2. **Technical demonstration** of dbt + MetricFlow + AI agent integration at scale
3. **Benchmarkable** — agent SQL accuracy measured against TPC-DS ground truth (99 queries)
4. **Blogworthy** — publishable writeup positioning this as the "build your own Meta Analytics Agent"

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Data Lake | TPC-DS SF100 (~100GB) as Parquet files | 24 tables, 3 retail channels, 5+ yrs synthetic data |
| Query Engine | DuckDB (dbt-duckdb adapter) | Embedded, zero-config, M3-optimized |
| Transformation | dbt Core + dbt-duckdb | Staging → Intermediate → Marts → Metrics |
| Semantic Layer | MetricFlow (via dbt) | Metric definitions, dimensions, measures |
| Memory Layer | ChromaDB | Query history embeddings, schema embeddings |
| Agent Orchestration | Python (asyncio) | Multi-step reasoning loop |
| AI Reasoning | Claude via Anthropic SDK | Tool use for SQL execution, result interpretation |
| UI | Streamlit | Chat interface + visualization |
| Language | Python 3.11+ | |

---

## Repository Structure

```
retail-sage/
├── CLAUDE.md                    # This file
├── README.md
├── .env                         # ANTHROPIC_API_KEY, paths (git-ignored)
├── .gitignore
├── pyproject.toml               # uv-managed dependencies
│
├── data/                        # TPC-DS Parquet data lake (git-ignored, ~100GB)
│   ├── raw/                     # Generated TPC-DS SF100 Parquet files
│   │   ├── store_sales/
│   │   ├── catalog_sales/
│   │   ├── web_sales/
│   │   ├── store_returns/
│   │   ├── catalog_returns/
│   │   ├── web_returns/
│   │   ├── inventory/
│   │   ├── customer/
│   │   ├── customer_demographics/
│   │   ├── customer_address/
│   │   ├── item/
│   │   ├── promotion/
│   │   ├── store/
│   │   ├── warehouse/
│   │   ├── date_dim/
│   │   ├── time_dim/
│   │   └── ... (all 24 TPC-DS tables)
│   └── duckdb/
│       └── retail_sage.duckdb   # DuckDB database file (views over Parquet)
│
├── dbt/                         # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml             # DuckDB connection profile
│   ├── packages.yml
│   ├── semantic_models/         # MetricFlow semantic model definitions
│   │   ├── sales_metrics.yml
│   │   ├── customer_metrics.yml
│   │   ├── inventory_metrics.yml
│   │   └── channel_metrics.yml
│   └── models/
│       ├── staging/             # 1:1 with raw TPC-DS tables, light cleaning
│       │   ├── _stg_tpcds.yml
│       │   ├── stg_store_sales.sql
│       │   ├── stg_catalog_sales.sql
│       │   ├── stg_web_sales.sql
│       │   └── ... (one per raw table)
│       ├── intermediate/        # Joins, denormalization, business logic
│       │   ├── int_sales_unified.sql       # All 3 channels combined
│       │   ├── int_customer_profile.sql
│       │   └── int_item_performance.sql
│       └── marts/               # Wide, analytics-ready tables
│           ├── retail/
│           │   ├── fct_sales.sql
│           │   ├── fct_returns.sql
│           │   ├── dim_customer.sql
│           │   ├── dim_item.sql
│           │   ├── dim_store.sql
│           │   └── dim_date.sql
│           └── metrics/
│               ├── daily_channel_summary.sql
│               └── customer_ltv.sql
│
├── agent/                       # AI agent core
│   ├── __init__.py
│   ├── agent.py                 # Main agent loop (Claude tool use)
│   ├── tools.py                 # MCP-style tool definitions (execute_sql, get_schema, etc.)
│   ├── memory.py                # ChromaDB query history + schema embedding logic
│   ├── context.py               # Table context builder (seeds agent with relevant tables)
│   ├── prompts.py               # System prompt, few-shot examples
│   └── benchmark.py            # TPC-DS ground truth comparison framework
│
├── scripts/
│   ├── 01_generate_data.py      # TPC-DS SF100 generation via DuckDB
│   ├── 02_export_parquet.py     # Export DuckDB tables to Parquet files
│   ├── 03_verify_data.py        # Row counts, spot checks, query all 99 TPC-DS queries
│   ├── 04_seed_memory.py        # Pre-populate ChromaDB with schema + sample query embeddings
│   └── 05_run_benchmark.py      # Run agent against all 99 TPC-DS queries, score accuracy
│
├── ui/
│   └── app.py                   # Streamlit chat interface
│
└── tests/
    ├── test_tools.py
    ├── test_agent.py
    └── test_memory.py
```

---

## Data Lake Details

### TPC-DS Scale Factor 100

TPC-DS (Transaction Processing Council Decision Support) is the industry-standard retail analytics
benchmark. SF100 generates approximately 100GB of data across 24 tables representing a
multi-channel retailer with store, catalog, and web sales channels.

**Key fact tables:**

| Table | Description | Approx rows at SF100 |
|---|---|---|
| store_sales | In-store transactions | ~2.88 billion |
| catalog_sales | Catalog/mail-order | ~1.44 billion |
| web_sales | E-commerce transactions | ~720 million |
| store_returns | In-store returns | ~287 million |
| catalog_returns | Catalog returns | ~144 million |
| web_returns | Web returns | ~72 million |
| inventory | Store inventory snapshots | ~1.3 billion |

**Key dimension tables:** customer, customer_demographics, customer_address, item, promotion,
store, warehouse, call_center, catalog_page, web_site, web_page, date_dim, time_dim, household_demographics, income_band, ship_mode, reason

**Data generation command:**
```python
import duckdb
conn = duckdb.connect('data/duckdb/retail_sage.duckdb')
conn.execute("CALL dsdgen(sf=100)")  # Takes 20-40 min on M3 Pro
```

> **Note**: DuckDB's tpcds extension generates data in-memory then writes to the DB file.
> With 36GB RAM, SF100 should complete but will be memory-intensive. Monitor with Activity Monitor.
> If OOM occurs, try generating SF100 table-by-table using the `tables` parameter.

### Parquet Export Strategy

After generation, export each table to partitioned Parquet for the "data lake" feel and for dbt
to query directly without loading the full DuckDB file:

```python
# Large fact tables: partition by year (ss_sold_date_sk range)
# Dimension tables: single Parquet file each
conn.execute("COPY store_sales TO 'data/raw/store_sales/' (FORMAT PARQUET, PARTITION_BY (ss_sold_date_sk // 365), OVERWRITE_OR_IGNORE)")
```

---

## dbt Conventions

- **Adapter**: `dbt-duckdb` (connects to `data/duckdb/retail_sage.duckdb`)
- **Target**: `dev` (local DuckDB), no cloud targets needed
- **Materialization defaults**:
  - Staging: `view` (zero storage cost, always fresh)
  - Intermediate: `table` (expensive joins, cache them)
  - Marts: `table` with incremental option for fact tables
- **Naming**: `stg_<source>__<table>`, `int_<description>`, `fct_<entity>`, `dim_<entity>`
- **All models must have**: description, column-level docs, at least `not_null` + `unique` tests on PKs

### Key MetricFlow Metrics to Define

```
# Revenue metrics
- gross_sales_amount       (store + catalog + web)
- net_sales_amount         (after returns)
- average_order_value      (by channel)
- revenue_per_customer

# Customer metrics  
- active_customers_30d
- new_customers
- customer_lifetime_value
- repeat_purchase_rate

# Operations metrics
- return_rate              (by channel, by item category)
- inventory_turn_rate
- days_of_supply

# Channel metrics
- channel_mix_pct          (store vs catalog vs web)
- cross_channel_customers
```

---

## Agent Architecture

### Tool Definitions

The agent has access to these tools (defined in `agent/tools.py`):

```python
tools = [
    {
        "name": "execute_sql",
        "description": "Execute a SQL query against the retail DuckDB database and return results",
        "input_schema": {
            "query": "string",       # SQL to execute
            "limit": "integer",      # Max rows to return (default 100)
            "explain": "boolean"     # If true, return EXPLAIN ANALYZE output
        }
    },
    {
        "name": "get_schema",
        "description": "Get schema (columns, types, sample values) for one or more tables",
        "input_schema": {
            "tables": "list[string]" # Table names to inspect
        }
    },
    {
        "name": "search_tables",
        "description": "Semantic search over table/column descriptions to find relevant tables for a question",
        "input_schema": {
            "query": "string",       # Natural language description of data needed
            "top_k": "integer"       # Number of results (default 5)
        }
    },
    {
        "name": "get_metric",
        "description": "Query a defined MetricFlow metric by name with optional filters and groupings",
        "input_schema": {
            "metric_name": "string",
            "group_by": "list[string]",
            "where": "string",
            "time_grain": "string"   # day, week, month, quarter, year
        }
    },
    {
        "name": "get_query_history",
        "description": "Retrieve past queries similar to the current question from memory",
        "input_schema": {
            "question": "string",
            "top_k": "integer"
        }
    }
]
```

### Agent System Prompt (core)

```
You are a retail analytics agent with access to a 100GB TPC-DS retail data lake covering
store, catalog, and web sales channels spanning multiple years. You autonomously analyze
data to answer business questions, diagnose metric changes, and identify root causes.

Your workflow:
1. Search for relevant tables using search_tables
2. Check query history for similar past analyses
3. Inspect schemas of candidate tables
4. Write and execute SQL iteratively — start simple, refine based on results
5. Synthesize findings in plain business language with specific numbers

You have deep retail domain knowledge. You understand:
- Multi-channel retail dynamics (store vs catalog vs web cannibalization)
- Seasonal patterns (Q4 holiday, back-to-school, etc.)
- Return rate benchmarks by category
- Customer segmentation (RFM analysis)
- Inventory turn expectations by department

Always show your reasoning. When diagnosing a metric drop, systematically eliminate
hypotheses: date range issues, data pipeline gaps, seasonal effects, category-specific
changes, geographic effects, promotional calendar shifts.
```

### Memory Layer

ChromaDB collection structure:
- `query_history` — past user questions + generated SQL + results summary, embedded for semantic retrieval
- `table_descriptions` — AI-generated descriptions of each table based on schema + sample data
- `column_glossary` — business definitions for key columns (retail domain context)

Seed `query_history` with the 99 TPC-DS benchmark queries (paraphrased as natural language questions)
before first use. This gives the agent a strong prior on what analytical patterns exist.

---

## Benchmark Framework

The 99 TPC-DS queries provide ground truth. The benchmark (`scripts/05_run_benchmark.py`):

1. Takes each TPC-DS query's intent (expressed as a natural language question)
2. Feeds it to the agent
3. Executes both the agent's SQL and the ground-truth SQL
4. Compares results (exact match on aggregated values, within 1% tolerance for floating point)
5. Scores: exact match / partial match / wrong / error
6. Produces a report card by query category

This is the differentiator — most portfolio projects have no evaluation framework.

---

## Key Commands

```bash
# Environment setup
uv venv && source .venv/bin/activate
uv pip install duckdb dbt-duckdb chromadb anthropic streamlit pandas pyarrow

# Data generation (Phase 1)
python scripts/01_generate_data.py        # ~20-40 min, monitor RAM
python scripts/02_export_parquet.py       # Export to data/raw/
python scripts/03_verify_data.py          # Verify all 24 tables, run 99 benchmark queries

# dbt (Phase 2-3)
cd dbt
dbt deps
dbt build --select staging              # Build all staging models
dbt build --select intermediate         # Build intermediate models
dbt build --select marts               # Build mart models
dbt test                               # Run all tests
dbt docs generate && dbt docs serve    # Browse lineage

# Memory seeding (Phase 4)
python scripts/04_seed_memory.py

# Run agent interactively
python -m agent.agent

# Run benchmark (Phase 6)
python scripts/05_run_benchmark.py

# UI
streamlit run ui/app.py
```

---

## Environment Variables (.env)

```
ANTHROPIC_API_KEY=your_key_here
DUCKDB_PATH=/Users/arthursherman/retail-sage/data/duckdb/retail_sage.duckdb
DATA_RAW_PATH=/Users/arthursherman/retail-sage/data/raw
CHROMA_PATH=/Users/arthursherman/retail-sage/data/chroma
DBT_PROJECT_DIR=/Users/arthursherman/retail-sage/dbt
```

---

## Development Phases

| Phase | Focus | Status |
|---|---|---|
| 1 | Data generation — TPC-DS SF100, Parquet export, 99-query verification | 🔲 Not started |
| 2 | dbt staging models — all 24 TPC-DS tables | 🔲 Not started |
| 3 | dbt marts + MetricFlow metric definitions | 🔲 Not started |
| 4 | Memory layer — ChromaDB setup, schema embeddings, query history seeding | 🔲 Not started |
| 5 | Agent loop — Claude tool use, multi-step reasoning, result synthesis | 🔲 Not started |
| 6 | Benchmark framework — 99-query evaluation, scoring, report card | 🔲 Not started |
| 7 | Streamlit UI + blog post draft | 🔲 Not started |

---

## Important Notes for Claude Code

- **Always use `uv`** for package management, not pip directly
- **DuckDB memory**: SF100 generation is RAM-intensive. If DuckDB OOMs, try `sf=10` first to
  validate the pipeline, then attempt SF100 with `memory_limit` pragmas set
- **dbt profiles.yml** should live at `~/.dbt/profiles.yml` (not in the repo), pointing to the
  local DuckDB file
- **Never commit** the `data/` directory — it's ~100GB and git-ignored
- **Never commit** `.env`
- **Parquet over CSV** — always export/import as Parquet for performance
- **DuckDB threading**: DuckDB automatically uses all M3 Pro cores. Don't fight it with thread limits.
- **Test incrementally** — validate each script on SF1 before running SF100. SF1 generates in seconds.
- **The 99 TPC-DS queries** are available via `PRAGMA tpcds(N)` in DuckDB (N = 1 to 99).
  Use these as ground truth, not as agent inputs directly.

---

## Reference Links

- TPC-DS spec: https://www.tpc.org/tpcds/
- DuckDB TPC-DS extension: https://duckdb.org/docs/stable/core_extensions/tpcds
- dbt-duckdb adapter: https://github.com/duckdb/dbt-duckdb
- MetricFlow docs: https://docs.getdbt.com/docs/build/about-metricflow
- ChromaDB docs: https://docs.trychroma.com
- Anthropic tool use: https://docs.anthropic.com/en/docs/build-with-claude/tool-use
- Inspiration: https://medium.com/@AnalyticsAtMeta/inside-metas-home-grown-ai-analytics-agent-4ea6779acfb3

---

*Last updated: April 1, 2026*
*Project: Retail-SAGE v0.1*
