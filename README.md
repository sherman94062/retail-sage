# Retail-SAGE

**Semantic Analytics & Governed Execution** — an AI-powered retail analytics agent that autonomously analyzes a TPC-DS data lake using natural language.

Built on Claude + DuckDB + dbt + ChromaDB. Runs entirely on local hardware (MacBook M3 Pro, 36 GB RAM).

---

## What It Does

Ask a business question in plain English. The agent searches for relevant tables, writes SQL, executes it against DuckDB, iterates if needed, and returns an answer with specific numbers — all autonomously.

```
You: Which product categories have the highest return rates?

Agent: [searches tables → inspects schemas → writes SQL → executes → refines → synthesizes]

The top return-rate categories are:
  1. Electronics — 14.2% return rate (store), 18.7% (web)
  2. Shoes — 12.8% return rate across all channels
  ...
```

The agent has access to 5 tools and uses multi-turn reasoning with Claude's tool use API.

## Architecture

```
┌───────────────────────────────────────────────────────┐
│                   Streamlit Chat UI                    │
│        (token/cost metrics, SQL viewer, progress)     │
├───────────────────────────────────────────────────────┤
│                  Agent Orchestrator                    │
│           (multi-turn Claude tool use loop)            │
├───────────┬──────────┬────────────┬─────────┬─────────┤
│execute_sql│get_schema│search_tables│get_history│list_tables│
├───────────┴──────────┴────────────┴─────────┴─────────┤
│  DuckDB (TPC-DS data)  │  ChromaDB (semantic memory)  │
├─────────────────────────┴─────────────────────────────┤
│          dbt (staging → intermediate → marts)         │
│          MetricFlow (semantic metric definitions)      │
└───────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Data | TPC-DS SF1 (24 tables, ~17M rows) as DuckDB database |
| Query Engine | DuckDB (embedded, zero-config) |
| Transformation | dbt-core + dbt-duckdb (24 staging, 4 intermediate, 8 mart models) |
| Semantic Layer | MetricFlow (4 semantic models, 15+ metrics) |
| Memory | ChromaDB (query history, table descriptions, column glossary) |
| AI | Claude Sonnet via Anthropic SDK (tool use) |
| UI | Streamlit (chat + SQL viewer + token tracking) |

## Project Structure

```
retail-sage/
├── agent/                  # AI agent core
│   ├── agent.py            # Multi-turn reasoning loop + AgentResult
│   ├── tools.py            # Tool definitions and execution
│   ├── memory.py           # ChromaDB semantic memory
│   ├── context.py          # Schema context builder
│   ├── prompts.py          # System prompt + few-shot examples
│   └── benchmark.py        # TPC-DS 99-query evaluation framework
├── dbt/                    # dbt project
│   ├── models/
│   │   ├── staging/        # 24 source-aligned views
│   │   ├── intermediate/   # Unified sales, returns, customer profile, item perf
│   │   └── marts/          # fct_sales, fct_returns, dim_*, daily_channel_summary, customer_ltv
│   └── semantic_models/    # MetricFlow metric definitions
├── scripts/                # Data pipeline
│   ├── 01_generate_data.py # TPC-DS generation via DuckDB
│   ├── 02_export_parquet.py
│   ├── 03_verify_data.py   # 99/99 TPC-DS benchmark queries passing
│   ├── 04_seed_memory.py   # Populate ChromaDB
│   └── 05_run_benchmark.py # Agent accuracy evaluation
├── ui/
│   └── app.py              # Streamlit chat interface
└── tests/                  # 17 tests (tools, memory, prompts)
```

## Quick Start

```bash
# Clone
git clone https://github.com/sherman94062/retail-sage.git
cd retail-sage

# Setup
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# Configure
cp .env.example .env
# Edit .env with your ANTHROPIC_API_KEY

# Generate data (SF1 takes ~10 seconds)
python scripts/01_generate_data.py --sf 1

# Verify data (99/99 TPC-DS queries)
python scripts/03_verify_data.py

# Build dbt models (85/85 pass)
cd dbt && dbt deps && dbt build && cd ..

# Seed memory
python scripts/04_seed_memory.py

# Run the agent (CLI)
python -m agent.agent

# Or launch the web UI
streamlit run ui/app.py
```

## UI Features

- **Session metrics** — total tokens and cost displayed at the top
- **SQL viewer** — expandable panel showing every SQL query the agent executed
- **Live progress** — diagnostic messages during analysis (searching memory, executing SQL, inspecting schemas...)
- **Token breakdown** — per-query input/output tokens, cost, and turn count
- **Example questions** — sidebar with one-click example queries

## Benchmark

The project includes a benchmark framework that evaluates the agent against all 99 TPC-DS queries:

```bash
python scripts/05_run_benchmark.py --verbose
```

Each query is paraphrased as a natural language question, sent to the agent, and the agent's SQL results are compared against ground truth.

## Tests

```bash
python -m pytest tests/ -v
```

17 tests covering tools (SQL execution, schema inspection, error handling), memory (ChromaDB CRUD + semantic search), and prompt construction.

## Key Design Decisions

- **DuckDB over Postgres/SQLite** — columnar engine handles analytical queries on millions of rows without infrastructure
- **dbt semantic layer** — the agent queries mart tables with clean names rather than raw TPC-DS schemas
- **ChromaDB memory** — semantic search finds relevant tables and past queries, reducing hallucination and cold-start issues
- **Multi-turn tool use** — the agent iterates (search → schema → SQL → refine) rather than generating SQL in one shot
- **Structured AgentResult** — every query returns tokens, cost, SQL history, and diagnostics for full observability

## Scaling to SF100

The pipeline supports TPC-DS SF100 (~100 GB, ~5 billion rows). Generate with:

```bash
python scripts/01_generate_data.py --sf 100   # ~20-40 min on M3 Pro
python scripts/02_export_parquet.py            # Export to partitioned Parquet
```

Monitor RAM during generation — SF100 is memory-intensive on 36 GB.

---

*Built as a portfolio demonstration of dbt + MetricFlow + AI agent integration for Staff Data Engineer and AI Solutions Architect roles.*
