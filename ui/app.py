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
    """Render SQL, diagnostics, and token info below an assistant message."""
    detail_cols = st.columns(4)
    with detail_cols[0]:
        st.caption(f"Model: {agent_result.model}")
    with detail_cols[1]:
        st.caption(f"Tokens: {agent_result.total_tokens:,} (in: {agent_result.input_tokens:,} / out: {agent_result.output_tokens:,})")
    with detail_cols[2]:
        st.caption(f"Cost: ${agent_result.cost:.4f}")
    with detail_cols[3]:
        st.caption(f"Turns: {agent_result.total_turns}")

    if agent_result.sql_queries:
        with st.expander(f"SQL Queries ({len(agent_result.sql_queries)})", expanded=False):
            for j, sql in enumerate(agent_result.sql_queries, 1):
                st.code(sql, language="sql")

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
