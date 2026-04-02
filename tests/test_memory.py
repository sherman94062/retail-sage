"""Tests for the memory layer."""

import pytest

from agent.memory import MemoryStore


@pytest.fixture
def memory(tmp_path):
    return MemoryStore(tmp_path / "test_chroma")


class TestQueryHistory:
    def test_add_and_search(self, memory):
        memory.add_query("total revenue by channel", "SELECT channel, SUM(sales) FROM ...", "Store: $1M, Web: $500K")
        results = memory.search_queries("revenue per channel")
        assert len(results) == 1
        assert "revenue" in results[0]["question"]

    def test_empty_search(self, memory):
        results = memory.search_queries("anything")
        assert results == []


class TestTableDescriptions:
    def test_add_and_search(self, memory):
        memory.add_table_description("store_sales", "In-store transactions with quantity and prices")
        results = memory.search_tables("point of sale transactions")
        assert len(results) == 1
        assert results[0]["table_name"] == "store_sales"


class TestColumnGlossary:
    def test_add_and_search(self, memory):
        memory.add_column_definition("ss_net_profit", "store_sales", "Net profit after all costs and returns")
        results = memory.search_columns("profit margin")
        assert len(results) == 1
        assert results[0]["column"] == "ss_net_profit"


class TestStats:
    def test_stats(self, memory):
        stats = memory.stats()
        assert stats["query_history"] == 0
        assert stats["table_descriptions"] == 0
        assert stats["column_glossary"] == 0

        memory.add_query("test", "SELECT 1", "result")
        stats = memory.stats()
        assert stats["query_history"] == 1
