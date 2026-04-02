"""
Streamlit chat interface for Retail-SAGE.

Usage:
    streamlit run ui/app.py
"""

import sys
from pathlib import Path

import streamlit as st

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.agent import MODELS, RetailSageAgent

st.set_page_config(
    page_title="Retail-SAGE",
    page_icon="🏪",
    layout="wide",
)

# --- Session state init ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "query_results" not in st.session_state:
    st.session_state.query_results = []  # parallel list: AgentResult per assistant message


@st.cache_resource
def get_agent():
    return RetailSageAgent()


agent = get_agent()

# --- Top bar: session token/cost metrics ---
top_cols = st.columns([3, 1, 1, 1])
with top_cols[0]:
    st.title("Retail-SAGE")
with top_cols[1]:
    st.metric("Session Tokens", f"{agent.session_total_tokens:,}")
with top_cols[2]:
    st.metric("Session Cost", f"${agent.session_cost:.4f}")
with top_cols[3]:
    st.metric("Queries", len([m for m in st.session_state.messages if m["role"] == "user"]))

st.caption("Semantic Analytics & Governed Execution — AI-Powered Retail Analytics Agent")

# --- Sidebar ---
with st.sidebar:
    st.header("Model")
    model_choice = st.selectbox(
        "AI Model",
        options=list(MODELS.keys()),
        format_func=lambda m: f"{m.title()} (${MODELS[m]['input_cost']:.2f}/${MODELS[m]['output_cost']:.2f} per M tokens)",
        index=list(MODELS.keys()).index(agent.model_key),
    )
    if model_choice != agent.model_key:
        agent.set_model(model_choice)

    st.divider()

    st.header("About")
    st.markdown("""
    **Retail-SAGE** is an AI analytics agent powered by Claude
    that autonomously analyzes a TPC-DS retail data lake.

    **Capabilities:**
    - Natural language to SQL generation
    - Multi-step data analysis
    - Root cause diagnosis
    - Cross-channel retail analytics

    **Data:** TPC-DS benchmark (store, catalog, web channels)
    """)

    st.divider()

    stats = agent.memory.stats()
    st.metric("Indexed Queries", stats["query_history"])
    st.metric("Indexed Tables", stats["table_descriptions"])
    st.metric("Indexed Columns", stats["column_glossary"])

    st.divider()

    st.markdown("**Example Questions:**")
    examples = [
        "What was our total revenue last year by channel?",
        "Which product categories have the highest return rates?",
        "Show me the monthly customer count trend",
        "What are the top 10 stores by net profit?",
        "How does weekend vs weekday performance compare?",
    ]
    for ex in examples:
        if st.button(ex, key=ex, use_container_width=True):
            st.session_state.example_question = ex

def _render_result_details(agent_result):
    """Render SQL, diagnostics, LLM calls, and token info below an assistant message."""
    import json as _json

    detail_cols = st.columns(4)
    with detail_cols[0]:
        st.caption(f"Model: {agent_result.model}")
    with detail_cols[1]:
        st.caption(f"Tokens: {agent_result.total_tokens:,} (in: {agent_result.input_tokens:,} / out: {agent_result.output_tokens:,})")
    with detail_cols[2]:
        st.caption(f"Cost: ${agent_result.cost:.4f}")
    with detail_cols[3]:
        st.caption(f"Turns: {agent_result.total_turns}")

    if agent_result.tables_queried:
        marts = [t for t in agent_result.tables_queried if t.startswith(("fct_", "dim_", "daily_", "customer_ltv"))]
        intermediates = [t for t in agent_result.tables_queried if t.startswith("int_")]
        raw = [t for t in agent_result.tables_queried if t not in marts and t not in intermediates]
        lineage_parts = []
        if raw:
            lineage_parts.append(f"Raw: `{'`, `'.join(raw)}`")
        if intermediates:
            lineage_parts.append(f"Intermediate: `{'`, `'.join(intermediates)}`")
        if marts:
            lineage_parts.append(f"Marts: `{'`, `'.join(marts)}`")
        with st.expander(f"dbt Models Used ({len(agent_result.tables_queried)})", expanded=False):
            st.markdown(" → ".join(lineage_parts) if lineage_parts else "None")

    if agent_result.sql_queries:
        with st.expander(f"SQL Queries ({len(agent_result.sql_queries)})", expanded=False):
            for j, sql in enumerate(agent_result.sql_queries, 1):
                st.code(sql, language="sql")

    # LLM API Call Details
    if agent_result.api_calls:
        with st.expander(f"LLM API Calls ({len(agent_result.api_calls)} turns)", expanded=False):
            for call in agent_result.api_calls:
                st.markdown(f"---\n#### Turn {call.turn}")

                # Request payload
                req_col, resp_col = st.columns(2)
                with req_col:
                    st.markdown("**Request**")
                    st.code(_json.dumps({
                        "model": call.model,
                        "max_tokens": call.max_tokens,
                        "system": f"<{call.system_prompt_length:,} chars>",
                        "tools": f"<{call.tools_provided} tool definitions>",
                        "messages": f"<{call.messages_sent} messages>",
                    }, indent=2), language="json")

                with resp_col:
                    st.markdown("**Response**")
                    resp_data = {
                        "stop_reason": call.stop_reason,
                        "usage": {
                            "input_tokens": call.input_tokens,
                            "output_tokens": call.output_tokens,
                            "total_tokens": call.total_tokens,
                            "cost": f"${call.cost:.6f}",
                        },
                    }
                    st.code(_json.dumps(resp_data, indent=2), language="json")

                # Assistant reasoning text (if any)
                if call.assistant_text and call.stop_reason == "tool_use":
                    st.markdown("**Assistant reasoning** (before tool calls):")
                    st.markdown(f"> {call.assistant_text[:500]}{'...' if len(call.assistant_text) > 500 else ''}")

                # Tool calls with full payloads
                if call.tool_calls:
                    for tc in call.tool_calls:
                        st.markdown(f"**Tool call:** `{tc.name}`")
                        in_col, out_col = st.columns(2)
                        with in_col:
                            st.markdown("*Input payload:*")
                            st.code(_json.dumps(tc.input, indent=2, default=str), language="json")
                        with out_col:
                            st.markdown("*Result (truncated):*")
                            try:
                                parsed = _json.loads(tc.result)
                                preview = _json.dumps(parsed, indent=2, default=str)
                                if len(preview) > 1500:
                                    preview = preview[:1500] + "\n... (truncated)"
                                st.code(preview, language="json")
                            except (_json.JSONDecodeError, TypeError):
                                st.code(tc.result[:1500], language="text")

                # Final response text (last turn only)
                if call.stop_reason == "end_turn" and call.assistant_text:
                    st.markdown("**Final response:** *(see answer above)*")

            # Summary table
            st.markdown("---\n**Turn-by-turn token summary:**")
            summary_data = []
            for call in agent_result.api_calls:
                tool_names = ", ".join(tc.name for tc in call.tool_calls) if call.tool_calls else "—"
                summary_data.append({
                    "Turn": call.turn,
                    "Stop Reason": call.stop_reason,
                    "Tools Called": tool_names,
                    "Input Tokens": f"{call.input_tokens:,}",
                    "Output Tokens": f"{call.output_tokens:,}",
                    "Cost": f"${call.cost:.6f}",
                })
            st.table(summary_data)

    if agent_result.diagnostics:
        with st.expander("Diagnostics", expanded=False):
            for diag in agent_result.diagnostics:
                st.text(diag)


# --- Chat history ---
for i, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

        # Show SQL and diagnostics for assistant messages
        if message["role"] == "assistant":
            result_idx = i // 2
            if result_idx < len(st.session_state.query_results):
                agent_result = st.session_state.query_results[result_idx]
                _render_result_details(agent_result)

# --- Handle input ---
if "example_question" in st.session_state:
    prompt = st.session_state.pop("example_question")
else:
    prompt = st.chat_input("Ask a question about your retail data...")

if prompt:
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get agent response with live progress
    with st.chat_message("assistant"):
        progress_container = st.empty()
        progress_lines = []

        def on_progress(msg: str):
            progress_lines.append(msg)
            progress_container.info("\n".join(progress_lines))

        try:
            agent_result = agent.ask(prompt, on_progress=on_progress)
            progress_container.empty()

            st.markdown(agent_result.answer)
            st.session_state.messages.append({"role": "assistant", "content": agent_result.answer})
            st.session_state.query_results.append(agent_result)

            _render_result_details(agent_result)

        except Exception as e:
            progress_container.empty()
            error_msg = f"Error: {e}"
            st.error(error_msg)
            st.session_state.messages.append({"role": "assistant", "content": error_msg})
            from agent.agent import AgentResult
            st.session_state.query_results.append(AgentResult(answer=error_msg))

    # Rerun to update top metrics
    st.rerun()
