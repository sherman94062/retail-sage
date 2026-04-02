"""Tests for the agent (unit-level, no API calls)."""

from agent.prompts import SYSTEM_PROMPT, build_system_prompt, format_few_shot_examples
from agent.tools import TOOL_DEFINITIONS


class TestPrompts:
    def test_system_prompt_not_empty(self):
        assert len(SYSTEM_PROMPT) > 100

    def test_build_system_prompt_includes_context(self):
        prompt = build_system_prompt("Here is some context")
        assert "Here is some context" in prompt
        assert "retail analytics agent" in prompt

    def test_few_shot_examples_format(self):
        examples = format_few_shot_examples()
        assert "Example 1" in examples
        assert "SELECT" in examples


class TestToolDefinitions:
    def test_all_tools_have_required_fields(self):
        for tool in TOOL_DEFINITIONS:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool
            assert tool["input_schema"]["type"] == "object"

    def test_expected_tools_present(self):
        names = {t["name"] for t in TOOL_DEFINITIONS}
        assert "execute_sql" in names
        assert "get_schema" in names
        assert "search_tables" in names
        assert "get_query_history" in names
        assert "list_tables" in names
