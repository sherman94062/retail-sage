"""
Context builder: prepares relevant table schemas and metadata
for the agent's system prompt based on the user's question.
"""

from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "duckdb" / "retail_sage.duckdb"


class ContextBuilder:
    """Builds schema context for the agent based on a user question."""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = str(db_path or DEFAULT_DB_PATH)

    def get_all_tables(self) -> list[str]:
        """List all tables and views in the database."""
        conn = duckdb.connect(self.db_path, read_only=True)
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables ORDER BY table_name"
            ).fetchall()
        ]
        conn.close()
        return tables

    def get_table_schema(self, table_name: str) -> dict:
        """Get column names, types, and sample values for a table."""
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            columns = conn.execute(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
                """
            ).fetchall()

            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            # Get sample values (first 3 rows)
            sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()

            return {
                "table_name": table_name,
                "row_count": row_count,
                "columns": [{"name": c[0], "type": c[1]} for c in columns],
                "sample_rows": sample.to_dict(orient="records") if not sample.empty else [],
            }
        finally:
            conn.close()

    def get_schema_summary(self, table_names: list[str] | None = None) -> str:
        """Build a text summary of table schemas for the agent prompt."""
        if table_names is None:
            table_names = self.get_all_tables()

        parts = []
        for table in table_names:
            try:
                schema = self.get_table_schema(table)
                cols = ", ".join(
                    f"{c['name']} ({c['type']})" for c in schema["columns"]
                )
                parts.append(
                    f"### {table} ({schema['row_count']:,} rows)\n"
                    f"Columns: {cols}"
                )
            except Exception as e:
                parts.append(f"### {table}\nError reading schema: {e}")

        return "\n\n".join(parts)

    def build_context(self, question: str, memory_store=None,
                      max_tables: int = 10) -> str:
        """Build full context for a question using memory search + schema."""
        context_parts = []

        # If memory is available, search for relevant tables
        relevant_tables = None
        if memory_store:
            table_results = memory_store.search_tables(question, top_k=max_tables)
            if table_results:
                relevant_tables = [t["table_name"] for t in table_results]
                context_parts.append("## Relevant Tables (by semantic search)")
                for t in table_results:
                    context_parts.append(f"- **{t['table_name']}**: {t['description']}")

            # Search for similar past queries
            past_queries = memory_store.search_queries(question, top_k=3)
            if past_queries:
                context_parts.append("\n## Similar Past Queries")
                for q in past_queries:
                    context_parts.append(
                        f"- Q: {q['question']}\n"
                        f"  SQL: ```{q['sql']}```\n"
                        f"  Result: {q['result_summary']}"
                    )

        # Add schema details for relevant tables
        context_parts.append("\n## Table Schemas")
        context_parts.append(self.get_schema_summary(relevant_tables))

        return "\n".join(context_parts)
