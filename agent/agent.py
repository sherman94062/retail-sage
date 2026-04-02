"""
Main agent loop: multi-step reasoning with Claude tool use.

Usage:
    python -m agent.agent                    # Interactive REPL
    python -m agent.agent "your question"    # Single question
"""

import json
import os
import sys
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv()

from agent.context import ContextBuilder
from agent.memory import MemoryStore
from agent.prompts import build_system_prompt
from agent.tools import TOOL_DEFINITIONS, ToolExecutor

MODEL = "claude-sonnet-4-20250514"
MAX_TURNS = 15  # Safety limit on tool-use turns


class RetailSageAgent:
    """AI-powered retail analytics agent using Claude with tool use."""

    def __init__(self, db_path: str | None = None, chroma_path: str | None = None):
        self.client = anthropic.Anthropic()
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

    def ask(self, question: str, verbose: bool = False) -> str:
        """
        Send a question to the agent and get a fully-reasoned answer.
        The agent may call tools multiple times before responding.
        """
        # Build context from memory
        context = self.context_builder.build_context(question, self.memory)
        system_prompt = build_system_prompt(context)

        messages = [{"role": "user", "content": question}]

        for turn in range(MAX_TURNS):
            if verbose:
                print(f"\n--- Agent turn {turn + 1} ---")

            response = self.client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=messages,
            )

            # Check if the model wants to use tools
            if response.stop_reason == "tool_use":
                # Process all tool calls in this response
                tool_results = []
                assistant_content = response.content

                for block in response.content:
                    if block.type == "tool_use":
                        tool_name = block.name
                        tool_input = block.input

                        if verbose:
                            print(f"  Tool: {tool_name}({json.dumps(tool_input)[:200]})")

                        result = self.tool_executor.execute_tool(tool_name, tool_input)

                        if verbose:
                            print(f"  Result: {result[:200]}...")

                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })

                # Add the assistant's response and tool results to messages
                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})

            else:
                # Model is done — extract the text response
                answer = ""
                for block in response.content:
                    if hasattr(block, "text"):
                        answer += block.text

                # Store this Q&A in memory for future reference
                # Extract any SQL from the conversation for the memory
                sql_used = self._extract_sql_from_messages(messages)
                if sql_used:
                    self.memory.add_query(
                        question=question,
                        sql=sql_used,
                        result_summary=answer[:500],
                    )

                return answer

        return "I reached the maximum number of reasoning steps. Please try a more specific question."

    def _extract_sql_from_messages(self, messages: list) -> str:
        """Extract the last SQL query used from the conversation."""
        for msg in reversed(messages):
            if isinstance(msg.get("content"), list):
                for item in msg["content"]:
                    if isinstance(item, dict) and item.get("type") == "tool_result":
                        try:
                            data = json.loads(item["content"])
                            if isinstance(data, dict) and "columns" in data:
                                # This was a SQL result — find the corresponding tool call
                                break
                        except (json.JSONDecodeError, TypeError):
                            pass
            # Check assistant messages for execute_sql tool calls
            if isinstance(msg.get("content"), list):
                for item in msg["content"]:
                    if hasattr(item, "type") and item.type == "tool_use" and item.name == "execute_sql":
                        return item.input.get("query", "")
        return ""


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
            answer = agent.ask(question, verbose=verbose)
            print(f"Agent: {answer}\n")
        except Exception as e:
            print(f"Error: {e}\n")


def main():
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        agent = RetailSageAgent()
        answer = agent.ask(question, verbose=True)
        print(f"\n{answer}")
    else:
        interactive_repl()


if __name__ == "__main__":
    main()
