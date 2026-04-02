"""
Main agent loop: multi-step reasoning with Claude tool use.

Usage:
    python -m agent.agent                    # Interactive REPL
    python -m agent.agent "your question"    # Single question
"""

import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import anthropic
from dotenv import load_dotenv

load_dotenv()

from agent.context import ContextBuilder
from agent.memory import MemoryStore
from agent.prompts import build_system_prompt
from agent.tools import TOOL_DEFINITIONS, ToolExecutor

MAX_TURNS = 15  # Safety limit on tool-use turns

MODELS = {
    "haiku": {
        "id": "claude-haiku-4-5-20251001",
        "input_cost": 0.80,   # per million tokens
        "output_cost": 4.00,
    },
    "sonnet": {
        "id": "claude-sonnet-4-20250514",
        "input_cost": 3.00,
        "output_cost": 15.00,
    },
}
DEFAULT_MODEL = "haiku"

# Known tables/models for extraction from SQL
_KNOWN_TABLES = {
    "fct_sales", "fct_returns", "dim_customer", "dim_item", "dim_store", "dim_date",
    "daily_channel_summary", "customer_ltv",
    "int_sales_unified", "int_returns_unified", "int_customer_profile", "int_item_performance",
    "store_sales", "catalog_sales", "web_sales", "store_returns", "catalog_returns", "web_returns",
    "inventory", "customer", "customer_address", "customer_demographics", "date_dim", "time_dim",
    "item", "promotion", "store", "warehouse", "call_center", "catalog_page",
    "web_site", "web_page", "household_demographics", "income_band", "ship_mode", "reason",
}


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract known table names referenced in a SQL query."""
    sql_lower = sql.lower()
    found = []
    for table in _KNOWN_TABLES:
        # Match table name as a whole word (after FROM, JOIN, or comma)
        if re.search(rf'\b{re.escape(table)}\b', sql_lower):
            found.append(table)
    return found


@dataclass
class ToolCall:
    """A single tool invocation within an API call."""
    tool_use_id: str
    name: str
    input: dict
    result: str  # raw JSON string returned by tool

    @property
    def input_preview(self) -> str:
        if self.name == "execute_sql":
            return self.input.get("query", "")[:200]
        return json.dumps(self.input)[:200]

    @property
    def result_preview(self) -> str:
        return self.result[:300]


@dataclass
class ApiCall:
    """Full record of one Anthropic API request/response round-trip."""
    turn: int
    # Request
    model: str = ""
    system_prompt_length: int = 0
    messages_sent: int = 0
    tools_provided: int = 0
    max_tokens: int = 0
    # Response
    stop_reason: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost: float = 0.0
    # Content
    assistant_text: str = ""          # text blocks from the response
    tool_calls: list[ToolCall] = field(default_factory=list)

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def request_summary(self) -> dict:
        """Serializable summary of the request payload."""
        return {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system_prompt_chars": self.system_prompt_length,
            "messages_count": self.messages_sent,
            "tools_count": self.tools_provided,
        }

    def response_summary(self) -> dict:
        """Serializable summary of the response."""
        summary = {
            "stop_reason": self.stop_reason,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "cost": f"${self.cost:.6f}",
        }
        if self.assistant_text:
            summary["text_preview"] = self.assistant_text[:300]
        if self.tool_calls:
            summary["tool_calls"] = [
                {"name": tc.name, "input_preview": tc.input_preview}
                for tc in self.tool_calls
            ]
        return summary


@dataclass
class AgentResult:
    """Structured result from an agent query."""
    answer: str = ""
    sql_queries: list[str] = field(default_factory=list)
    tables_queried: list[str] = field(default_factory=list)
    diagnostics: list[str] = field(default_factory=list)
    api_calls: list[ApiCall] = field(default_factory=list)
    dataframes: list[Any] = field(default_factory=list)  # list of (sql, pd.DataFrame) tuples
    input_tokens: int = 0
    output_tokens: int = 0
    total_turns: int = 0
    model: str = ""
    input_cost_per_m: float = 0.0
    output_cost_per_m: float = 0.0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def cost(self) -> float:
        return (self.input_tokens * self.input_cost_per_m / 1_000_000
                + self.output_tokens * self.output_cost_per_m / 1_000_000)


class RetailSageAgent:
    """AI-powered retail analytics agent using Claude with tool use."""

    def __init__(self, db_path: str | None = None, chroma_path: str | None = None,
                 model: str = DEFAULT_MODEL):
        self.client = anthropic.Anthropic()
        self.set_model(model)
        self.db_path = db_path or os.getenv(
            "DUCKDB_PATH",
            str(Path(__file__).resolve().parent.parent / "data" / "duckdb" / "retail_sage.duckdb"),
        )
        chroma = chroma_path or os.getenv(
            "CHROMA_PATH",
            str(Path(__file__).resolve().parent.parent / "data" / "chroma"),
        )
        self.memory = MemoryStore(chroma)
        self.context_builder = ContextBuilder(self.db_path)
        self.tool_executor = ToolExecutor(self.db_path, self.memory)
        self.session_input_tokens = 0
        self.session_output_tokens = 0

    def set_model(self, model: str) -> None:
        """Switch the active model. Accepts 'haiku' or 'sonnet'."""
        if model not in MODELS:
            raise ValueError(f"Unknown model '{model}'. Choose from: {list(MODELS.keys())}")
        self.model_key = model
        self.model_config = MODELS[model]
        self.model_id = self.model_config["id"]

    @property
    def session_total_tokens(self) -> int:
        return self.session_input_tokens + self.session_output_tokens

    @property
    def session_cost(self) -> float:
        return (self.session_input_tokens * self.model_config["input_cost"] / 1_000_000
                + self.session_output_tokens * self.model_config["output_cost"] / 1_000_000)

    def ask(self, question: str, verbose: bool = False,
            on_progress: Callable[[str], None] | None = None) -> AgentResult:
        """
        Send a question to the agent and get a fully-reasoned answer.
        The agent may call tools multiple times before responding.

        Args:
            question: The user's question.
            verbose: Print debug output to stdout.
            on_progress: Optional callback for live progress updates (used by UI).
        """
        result = AgentResult(
            model=self.model_key,
            input_cost_per_m=self.model_config["input_cost"],
            output_cost_per_m=self.model_config["output_cost"],
        )

        def _progress(msg: str):
            result.diagnostics.append(msg)
            if on_progress:
                on_progress(msg)
            if verbose:
                print(f"  {msg}")

        # Build context from memory
        # Clear DataFrame buffer for this query
        self.tool_executor.last_dataframes = []

        _progress("Searching memory for relevant context...")
        context = self.context_builder.build_context(question, self.memory)
        system_prompt = build_system_prompt(context)

        messages = [{"role": "user", "content": question}]

        for turn in range(MAX_TURNS):
            result.total_turns = turn + 1
            _progress(f"Agent turn {turn + 1}: calling Claude ({self.model_key})...")

            response = self.client.messages.create(
                model=self.model_id,
                max_tokens=4096,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            # Track tokens
            turn_input = response.usage.input_tokens
            turn_output = response.usage.output_tokens
            turn_cost = (turn_input * self.model_config["input_cost"] / 1_000_000
                         + turn_output * self.model_config["output_cost"] / 1_000_000)
            result.input_tokens += turn_input
            result.output_tokens += turn_output
            self.session_input_tokens += turn_input
            self.session_output_tokens += turn_output

            # Build API call record
            api_call = ApiCall(
                turn=turn + 1,
                model=self.model_id,
                system_prompt_length=len(system_prompt),
                messages_sent=len(messages),
                tools_provided=len(TOOL_DEFINITIONS),
                max_tokens=4096,
                stop_reason=response.stop_reason,
                input_tokens=turn_input,
                output_tokens=turn_output,
                cost=turn_cost,
            )

            # Extract text from response
            for block in response.content:
                if hasattr(block, "text"):
                    api_call.assistant_text += block.text

            # Check if the model wants to use tools
            if response.stop_reason == "tool_use":
                tool_results = []
                assistant_content = response.content

                for block in response.content:
                    if block.type == "tool_use":
                        tool_name = block.name
                        tool_input = block.input

                        # Capture SQL queries and tables
                        if tool_name == "execute_sql":
                            sql = tool_input.get("query", "")
                            result.sql_queries.append(sql)
                            result.tables_queried.extend(_extract_tables_from_sql(sql))
                            _progress(f"Executing SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")
                        elif tool_name == "get_schema":
                            tables = tool_input.get("tables", [])
                            result.tables_queried.extend(tables)
                            _progress(f"Inspecting schema: {', '.join(tables)}")
                        elif tool_name == "search_tables":
                            _progress(f"Searching tables: {tool_input.get('query', '')[:80]}")
                        elif tool_name == "get_query_history":
                            _progress("Checking query history...")
                        elif tool_name == "list_tables":
                            _progress("Listing available tables...")
                        else:
                            _progress(f"Tool: {tool_name}")

                        tool_result_str = self.tool_executor.execute_tool(tool_name, tool_input)

                        # Record in API call log
                        api_call.tool_calls.append(ToolCall(
                            tool_use_id=block.id,
                            name=tool_name,
                            input=tool_input,
                            result=tool_result_str,
                        ))

                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": tool_result_str,
                        })

                result.api_calls.append(api_call)
                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})

            else:
                # Model is done — extract the text response
                result.api_calls.append(api_call)
                result.answer = api_call.assistant_text

                # Deduplicate tables, preserving order
                seen = set()
                result.tables_queried = [t for t in result.tables_queried if not (t in seen or seen.add(t))]
                # Collect DataFrames from tool executor
                result.dataframes = list(self.tool_executor.last_dataframes)
                _progress(f"Done ({self.model_key}). {result.total_turns} turns, {result.total_tokens:,} tokens, ${result.cost:.4f}")

                # Store this Q&A in memory
                if result.sql_queries:
                    self.memory.add_query(
                        question=question,
                        sql=result.sql_queries[-1],
                        result_summary=result.answer[:500],
                    )

                return result

        result.answer = "I reached the maximum number of reasoning steps. Please try a more specific question."
        return result


def interactive_repl():
    """Run the agent in interactive REPL mode."""
    print("=" * 60)
    print("  Retail-SAGE: AI-Powered Retail Analytics Agent")
    print("  Type 'quit' or 'exit' to leave, 'verbose' to toggle debug")
    print("=" * 60)

    agent = RetailSageAgent()
    verbose = False

    # Show memory stats
    stats = agent.memory.stats()
    print(f"\nMemory: {stats['query_history']} queries, "
          f"{stats['table_descriptions']} tables, "
          f"{stats['column_glossary']} columns indexed")
    print()

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue
        if question.lower() in ("quit", "exit"):
            print("Goodbye!")
            break
        if question.lower() == "verbose":
            verbose = not verbose
            print(f"Verbose mode: {'ON' if verbose else 'OFF'}")
            continue

        print("\nAnalyzing...\n")
        try:
            result = agent.ask(question, verbose=verbose)
            print(f"Agent: {result.answer}")
            if result.sql_queries:
                print(f"\nSQL executed ({len(result.sql_queries)} queries):")
                for i, sql in enumerate(result.sql_queries, 1):
                    print(f"  [{i}] {sql}")
            print(f"\nTokens: {result.total_tokens:,} (in: {result.input_tokens:,}, out: {result.output_tokens:,})")
            print(f"Cost: ${result.cost:.4f} | Session: ${agent.session_cost:.4f} ({agent.session_total_tokens:,} tokens)")
            print()
        except Exception as e:
            print(f"Error: {e}\n")


def main():
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        agent = RetailSageAgent()
        result = agent.ask(question, verbose=True)
        print(f"\n{result.answer}")
    else:
        interactive_repl()


if __name__ == "__main__":
    main()
