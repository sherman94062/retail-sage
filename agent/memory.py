"""
Memory layer using ChromaDB for semantic search over:
- query_history: past questions + SQL + result summaries
- table_descriptions: AI-generated descriptions of each table
- column_glossary: business definitions for key columns
"""

import json
from pathlib import Path

import chromadb
from dotenv import load_dotenv

load_dotenv()

DEFAULT_CHROMA_PATH = Path(__file__).resolve().parent.parent / "data" / "chroma"


class MemoryStore:
    """ChromaDB-backed semantic memory for the retail analytics agent."""

    def __init__(self, persist_dir: str | Path | None = None):
        persist_dir = Path(persist_dir or DEFAULT_CHROMA_PATH)
        persist_dir.mkdir(parents=True, exist_ok=True)

        self.client = chromadb.PersistentClient(path=str(persist_dir))

        # Collections
        self.query_history = self.client.get_or_create_collection(
            name="query_history",
            metadata={"description": "Past user questions, generated SQL, and result summaries"},
        )
        self.table_descriptions = self.client.get_or_create_collection(
            name="table_descriptions",
            metadata={"description": "AI-generated descriptions of database tables"},
        )
        self.column_glossary = self.client.get_or_create_collection(
            name="column_glossary",
            metadata={"description": "Business definitions for key columns"},
        )

    def add_query(self, question: str, sql: str, result_summary: str,
                  query_id: str | None = None) -> str:
        """Store a question-SQL-result triple in query history."""
        import hashlib
        qid = query_id or hashlib.md5(question.encode()).hexdigest()[:12]
        self.query_history.upsert(
            ids=[qid],
            documents=[question],
            metadatas=[{"sql": sql, "result_summary": result_summary}],
        )
        return qid

    def search_queries(self, question: str, top_k: int = 5) -> list[dict]:
        """Find similar past queries by semantic search."""
        results = self.query_history.query(
            query_texts=[question],
            n_results=min(top_k, self.query_history.count() or 1),
        )
        if not results["ids"][0]:
            return []
        return [
            {
                "id": results["ids"][0][i],
                "question": results["documents"][0][i],
                "sql": results["metadatas"][0][i].get("sql", ""),
                "result_summary": results["metadatas"][0][i].get("result_summary", ""),
                "distance": results["distances"][0][i] if results.get("distances") else None,
            }
            for i in range(len(results["ids"][0]))
        ]

    def add_table_description(self, table_name: str, description: str,
                              columns: list[dict] | None = None) -> None:
        """Store a table description with optional column metadata."""
        metadata = {"table_name": table_name}
        if columns:
            metadata["columns_json"] = json.dumps(columns)
        self.table_descriptions.upsert(
            ids=[table_name],
            documents=[description],
            metadatas=[metadata],
        )

    def search_tables(self, query: str, top_k: int = 5) -> list[dict]:
        """Semantic search over table descriptions."""
        count = self.table_descriptions.count()
        if count == 0:
            return []
        results = self.table_descriptions.query(
            query_texts=[query],
            n_results=min(top_k, count),
        )
        return [
            {
                "table_name": results["metadatas"][0][i].get("table_name", ""),
                "description": results["documents"][0][i],
                "distance": results["distances"][0][i] if results.get("distances") else None,
            }
            for i in range(len(results["ids"][0]))
        ]

    def add_column_definition(self, column_name: str, table_name: str,
                              definition: str) -> None:
        """Store a business definition for a column."""
        col_id = f"{table_name}.{column_name}"
        self.column_glossary.upsert(
            ids=[col_id],
            documents=[definition],
            metadatas=[{"column_name": column_name, "table_name": table_name}],
        )

    def search_columns(self, query: str, top_k: int = 5) -> list[dict]:
        """Semantic search over column definitions."""
        count = self.column_glossary.count()
        if count == 0:
            return []
        results = self.column_glossary.query(
            query_texts=[query],
            n_results=min(top_k, count),
        )
        return [
            {
                "column": results["metadatas"][0][i].get("column_name", ""),
                "table": results["metadatas"][0][i].get("table_name", ""),
                "definition": results["documents"][0][i],
            }
            for i in range(len(results["ids"][0]))
        ]

    def stats(self) -> dict:
        """Return collection sizes."""
        return {
            "query_history": self.query_history.count(),
            "table_descriptions": self.table_descriptions.count(),
            "column_glossary": self.column_glossary.count(),
        }
