"""Tests for agent tools."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agent.tools import ToolExecutor


@pytest.fixture
def db_path(tmp_path):
    """Create a small test DuckDB database."""
    import duckdb
    db = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db))
    conn.execute("INSTALL tpcds")
    conn.execute("LOAD tpcds")
    conn.execute("CALL dsdgen(sf=0.01)")
    conn.close()
    return db


@pytest.fixture
def executor(db_path):
    return ToolExecutor(db_path=db_path)


class TestExecuteSQL:
    def test_simple_query(self, executor):
        result = json.loads(executor.execute_tool("execute_sql", {"query": "SELECT 1 AS x"}))
        assert result["row_count"] == 1
        assert result["data"][0]["x"] == 1

    def test_table_query(self, executor):
        result = json.loads(executor.execute_tool("execute_sql", {
            "query": "SELECT COUNT(*) AS cnt FROM store_sales",
        }))
        assert result["data"][0]["cnt"] > 0

    def test_limit_applied(self, executor):
        result = json.loads(executor.execute_tool("execute_sql", {
            "query": "SELECT * FROM store_sales",
            "limit": 5,
        }))
        assert result["row_count"] <= 5

    def test_invalid_sql(self, executor):
        result = json.loads(executor.execute_tool("execute_sql", {
            "query": "SELECT * FROM nonexistent_table",
        }))
        assert "error" in result


class TestGetSchema:
    def test_get_schema(self, executor):
        result = json.loads(executor.execute_tool("get_schema", {"tables": ["store_sales"]}))
        assert len(result) == 1
        assert result[0]["table"] == "store_sales"
        assert result[0]["row_count"] > 0
        assert len(result[0]["columns"]) > 0


class TestListTables:
    def test_list_tables(self, executor):
        result = json.loads(executor.execute_tool("list_tables", {}))
        table_names = [t["name"] for t in result]
        assert "store_sales" in table_names
        assert "customer" in table_names


class TestUnknownTool:
    def test_unknown_tool(self, executor):
        result = json.loads(executor.execute_tool("fake_tool", {}))
        assert "error" in result
