"""
Tool definitions and execution for the retail analytics agent.
These tools are exposed to Claude via the Anthropic tool use API.
"""

import json
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "duckdb" / "retail_sage.duckdb"

# Tool schemas for the Anthropic API
TOOL_DEFINITIONS = [
    {
        "name": "execute_sql",
        "description": (
            "Execute a SQL query against the retail DuckDB database and return results. "
            "The database contains TPC-DS retail data with store, catalog, and web sales channels. "
            "Use DuckDB SQL syntax. Results are returned as JSON rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The SQL query to execute",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return (default 100)",
                    "default": 100,
                },
                "explain": {
                    "type": "boolean",
                    "description": "If true, return EXPLAIN ANALYZE output instead of results",
                    "default": False,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_schema",
        "description": (
            "Get the schema (column names, types, row count, and sample values) "
            "for one or more database tables."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Table names to inspect",
                },
            },
            "required": ["tables"],
        },
    },
    {
        "name": "search_tables",
        "description": (
            "Semantic search over table and column descriptions to find relevant tables "
            "for answering a question. Returns the most relevant tables ranked by similarity."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language description of the data you need",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return (default 5)",
                    "default": 5,
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_query_history",
        "description": (
            "Retrieve past queries similar to the current question from memory. "
            "Returns the most similar past questions with their SQL and result summaries."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The current question to find similar past queries for",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return (default 5)",
                    "default": 5,
                },
            },
            "required": ["question"],
        },
    },
    {
        "name": "list_tables",
        "description": "List all available tables and views in the database.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
]


class ToolExecutor:
    """Executes agent tools against the DuckDB database and memory store."""

    def __init__(self, db_path: str | Path | None = None, memory_store=None):
        self.db_path = str(db_path or DEFAULT_DB_PATH)
        self.memory = memory_store

    def _get_conn(self, read_only: bool = True) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path, read_only=read_only)

    def execute_tool(self, tool_name: str, tool_input: dict) -> str:
        """Route a tool call to the appropriate handler. Returns JSON string."""
        handlers = {
            "execute_sql": self._execute_sql,
            "get_schema": self._get_schema,
            "search_tables": self._search_tables,
            "get_query_history": self._get_query_history,
            "list_tables": self._list_tables,
        }
        handler = handlers.get(tool_name)
        if not handler:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        try:
            return handler(**tool_input)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def _execute_sql(self, query: str, limit: int = 100, explain: bool = False) -> str:
        conn = self._get_conn()
        try:
            if explain:
                result = conn.execute(f"EXPLAIN ANALYZE {query}").fetchdf()
                return result.to_string()

            # Add LIMIT if not present and not an explain
            q = query.strip().rstrip(";")
            if limit and "limit" not in q.lower().split("--")[0].split("/*")[0].rsplit(")", 1)[-1]:
                q = f"SELECT * FROM ({q}) _sub LIMIT {limit}"

            df = conn.execute(q).fetchdf()
            records = df.to_dict(orient="records")

            # Truncate large values for readability
            for row in records:
                for k, v in row.items():
                    if isinstance(v, str) and len(v) > 200:
                        row[k] = v[:200] + "..."

            return json.dumps({
                "row_count": len(records),
                "columns": list(df.columns),
                "data": records,
            }, default=str)
        finally:
            conn.close()

    def _get_schema(self, tables: list[str]) -> str:
        conn = self._get_conn()
        try:
            schemas = []
            for table in tables:
                cols = conn.execute(
                    f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                    """
                ).fetchall()
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchdf()

                schemas.append({
                    "table": table,
                    "row_count": row_count,
                    "columns": [{"name": c[0], "type": c[1]} for c in cols],
                    "sample_rows": sample.to_dict(orient="records"),
                })
            return json.dumps(schemas, default=str)
        finally:
            conn.close()

    def _search_tables(self, query: str, top_k: int = 5) -> str:
        if not self.memory:
            return json.dumps({"error": "Memory store not initialized. Run seed_memory.py first."})
        results = self.memory.search_tables(query, top_k=top_k)
        return json.dumps(results, default=str)

    def _get_query_history(self, question: str, top_k: int = 5) -> str:
        if not self.memory:
            return json.dumps({"error": "Memory store not initialized. Run seed_memory.py first."})
        results = self.memory.search_queries(question, top_k=top_k)
        return json.dumps(results, default=str)

    def _list_tables(self) -> str:
        conn = self._get_conn()
        try:
            tables = conn.execute(
                "SELECT table_name, table_type FROM information_schema.tables ORDER BY table_name"
            ).fetchall()
            return json.dumps([{"name": t[0], "type": t[1]} for t in tables])
        finally:
            conn.close()
