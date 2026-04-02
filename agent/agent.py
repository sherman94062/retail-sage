"""
Main agent loop: multi-step reasoning with Claude tool use.

Usage:
    python -m agent.agent                    # Interactive REPL
    python -m agent.agent "your question"    # Single question
"""

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

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


@dataclass
class AgentResult:
    """Structured result from an agent query."""
    answer: str = ""
    sql_queries: list[str] = field(default_factory=list)
    diagnostics: list[str] = field(default_factory=list)
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
        _progress("Searching memory for relevant context...")
        context = self.context_builder.build_context(question, self.memory)
        system_prompt = build_system_prompt(context)

        messages = [{"role": "user", "content": question}]

        for turn in range(MAX_TURNS):
            result.total_turns = turn + 1
            _progress(f"Agent turn {turn + 1}: calling Claude...")

            response = self.client.messages.create(
                model=self.model_id,
                max_tokens=4096,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            # Track tokens
            result.input_tokens += response.usage.input_tokens
            result.output_tokens += response.usage.output_tokens
            self.session_input_tokens += response.usage.input_tokens
            self.session_output_tokens += response.usage.output_tokens

            # Check if the model wants to use tools
            if response.stop_reason == "tool_use":
                tool_results = []
                assistant_content = response.content

                for block in response.content:
                    if block.type == "tool_use":
                        tool_name = block.name
                        tool_input = block.input

                        # Capture SQL queries
                        if tool_name == "execute_sql":
                            sql = tool_input.get("query", "")
                            result.sql_queries.append(sql)
                            _progress(f"Executing SQL: {sql[:120]}{'...' if len(sql) > 120 else ''}")
                        elif tool_name == "get_schema":
                            tables = tool_input.get("tables", [])
                            _progress(f"Inspecting schema: {', '.join(tables)}")
                        elif tool_name == "search_tables":
                            _progress(f"Searching tables: {tool_input.get('query', '')[:80]}")
                        elif tool_name == "get_query_history":
                            _progress(f"Checking query history...")
                        elif tool_name == "list_tables":
                            _progress(f"Listing available tables...")
                        else:
                            _progress(f"Tool: {tool_name}")

                        tool_result = self.tool_executor.execute_tool(tool_name, tool_input)

                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": tool_result,
                        })

                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})

            else:
                # Model is done — extract the text response
                answer = ""
                for block in response.content:
                    if hasattr(block, "text"):
                        answer += block.text

                result.answer = answer
                _progress(f"Done ({self.model_key}). {result.total_turns} turns, {result.total_tokens:,} tokens, ${result.cost:.4f}")

                # Store this Q&A in memory
                if result.sql_queries:
                    self.memory.add_query(
                        question=question,
                        sql=result.sql_queries[-1],
                        result_summary=answer[:500],
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
