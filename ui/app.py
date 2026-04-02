"""
Streamlit chat interface for Retail-SAGE.

Usage:
    streamlit run ui/app.py
"""

import json as _json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.agent import MODELS, AgentResult, RetailSageAgent

st.set_page_config(
    page_title="Retail-SAGE",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Compact styling
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 0.5rem; }
    h1 { font-size: 1.6rem !important; margin-bottom: 0 !important; }
    .stMetric label { font-size: 0.75rem !important; }
    .stMetric [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 0.5rem; }
    .stTabs [data-baseweb="tab"] { font-size: 0.85rem; padding: 0.4rem 0.8rem; }
    div[data-testid="stExpander"] summary { font-size: 0.85rem; }
    .stDataFrame { font-size: 0.8rem; }
</style>
""", unsafe_allow_html=True)

def _render_auto_chart(df: pd.DataFrame, idx: int) -> None:
    """Auto-detect and render the best chart for a DataFrame."""
    numeric_cols = list(df.select_dtypes(include="number").columns)
    non_numeric_cols = list(df.select_dtypes(exclude="number").columns)

    if not numeric_cols:
        return

    metric_cols = [c for c in numeric_cols if not c.endswith("_sk")]
    if not metric_cols:
        return

    time_cols = [c for c in df.columns if any(t in c.lower() for t in ["year", "month", "date", "quarter", "week"])]
    label_cols = [c for c in non_numeric_cols if not c.endswith("_sk")]

    if time_cols and len(df) > 2:
        time_col = time_cols[0]
        if "d_year" in df.columns and "d_month" in df.columns:
            chart_df = df.copy()
            chart_df["period"] = chart_df["d_year"].astype(str) + "-" + chart_df["d_month"].astype(str).str.zfill(2)
            x_col = "period"
        else:
            chart_df = df.copy()
            x_col = time_col

        color_col = None
        for c in label_cols:
            if c != x_col and df[c].nunique() <= 10:
                color_col = c
                break

        y_col = metric_cols[0]
        try:
            if color_col:
                st.line_chart(chart_df, x=x_col, y=y_col, color=color_col)
            else:
                st.line_chart(chart_df, x=x_col, y=y_col)
        except Exception:
            st.bar_chart(chart_df.set_index(x_col)[metric_cols[:3]])

    elif label_cols and len(df) <= 50:
        label_col = label_cols[0]
        y_col = metric_cols[0]
        try:
            st.bar_chart(df, x=label_col, y=y_col)
        except Exception:
            st.bar_chart(df.set_index(label_col)[metric_cols[:3]])

    elif len(metric_cols) >= 2 and len(df) > 2:
        if label_cols:
            st.bar_chart(df.set_index(label_cols[0])[metric_cols[:4]])
        else:
            st.bar_chart(df[metric_cols[:4]])


# --- Session state ---
if "current_result" not in st.session_state:
    st.session_state.current_result = None  # AgentResult for current query
if "current_question" not in st.session_state:
    st.session_state.current_question = None
if "history" not in st.session_state:
    st.session_state.history = []  # list of (question, AgentResult)


@st.cache_resource
def get_agent():
    return RetailSageAgent()


agent = get_agent()

# --- Sidebar ---
with st.sidebar:
    st.header("Model")
    model_choice = st.selectbox(
        "AI Model",
        options=list(MODELS.keys()),
        format_func=lambda m: f"{m.title()} (${MODELS[m]['input_cost']:.2f}/${MODELS[m]['output_cost']:.2f} per M)",
        index=list(MODELS.keys()).index(agent.model_key),
    )
    if model_choice != agent.model_key:
        agent.set_model(model_choice)

    st.divider()

    # Session metrics
    m1, m2 = st.columns(2)
    m1.metric("Tokens", f"{agent.session_total_tokens:,}")
    m2.metric("Cost", f"${agent.session_cost:.4f}")

    st.divider()

    st.markdown("**Quick questions:**")
    examples = [
        "Total revenue last year by channel?",
        "Top product categories by return rate?",
        "Monthly customer count trend",
        "Top 10 stores by net profit",
        "Weekend vs weekday performance",
    ]
    for ex in examples:
        if st.button(ex, key=ex, use_container_width=True):
            st.session_state.pending_question = ex

    st.divider()

    stats = agent.memory.stats()
    st.caption(f"Memory: {stats['query_history']} queries · {stats['table_descriptions']} tables · {stats['column_glossary']} columns")

    # History
    if st.session_state.history:
        st.divider()
        st.markdown("**History:**")
        for i, (q, _r) in enumerate(reversed(st.session_state.history)):
            if st.button(f"{q[:50]}{'...' if len(q) > 50 else ''}", key=f"hist_{i}", use_container_width=True):
                st.session_state.current_question = q
                st.session_state.current_result = _r

# --- Top bar ---
header_cols = st.columns([4, 1, 1, 1])
with header_cols[0]:
    st.title("Retail-SAGE")
with header_cols[1]:
    st.metric("Model", agent.model_key.title())
with header_cols[2]:
    st.metric("Session", f"${agent.session_cost:.4f}")
with header_cols[3]:
    st.metric("Queries", len(st.session_state.history))

# --- Input ---
if "pending_question" in st.session_state:
    prompt = st.session_state.pop("pending_question")
else:
    prompt = st.chat_input("Ask a question about your retail data...")

# --- Process new question ---
if prompt:
    st.session_state.current_question = prompt
    st.session_state.current_result = None  # clear while processing

    progress_area = st.empty()
    progress_lines = []

    def on_progress(msg: str):
        progress_lines.append(msg)
        progress_area.info("\n".join(progress_lines))

    try:
        result = agent.ask(prompt, on_progress=on_progress)
        progress_area.empty()
        st.session_state.current_result = result
        st.session_state.history.append((prompt, result))
    except Exception as e:
        progress_area.empty()
        st.session_state.current_result = AgentResult(answer=f"Error: {e}")
        st.session_state.history.append((prompt, st.session_state.current_result))
    st.rerun()

# --- Main display: current Q&A ---
question = st.session_state.current_question
result = st.session_state.current_result

if question and result:
    st.markdown(f"**Q:** {question}")
    st.divider()

    # Tabs: Results | Data & Charts | SQL | LLM Calls | Diagnostics
    tab_names = ["Results"]
    if result.dataframes:
        tab_names.append("Data & Charts")
    tab_names.extend(["SQL", "LLM Calls", "Diagnostics"])
    tabs = st.tabs(tab_names)
    tab_idx = 0

    # --- Results tab ---
    with tabs[tab_idx]:
        # Compact metrics row
        mc = st.columns(5)
        mc[0].caption(f"Model: **{result.model}**")
        mc[1].caption(f"Tokens: **{result.total_tokens:,}**")
        mc[2].caption(f"Cost: **${result.cost:.4f}**")
        mc[3].caption(f"Turns: **{result.total_turns}**")
        if result.tables_queried:
            mc[4].caption(f"Tables: **{len(result.tables_queried)}**")

        st.markdown(result.answer)

        # dbt lineage (compact, inline)
        if result.tables_queried:
            marts = [t for t in result.tables_queried if t.startswith(("fct_", "dim_", "daily_", "customer_ltv"))]
            intermediates = [t for t in result.tables_queried if t.startswith("int_")]
            raw = [t for t in result.tables_queried if t not in marts and t not in intermediates]
            parts = []
            if raw:
                parts.append(f"**Raw:** `{'`, `'.join(raw)}`")
            if intermediates:
                parts.append(f"**Intermediate:** `{'`, `'.join(intermediates)}`")
            if marts:
                parts.append(f"**Marts:** `{'`, `'.join(marts)}`")
            st.caption("dbt lineage: " + " → ".join(parts))
    tab_idx += 1

    # --- Data & Charts tab ---
    if result.dataframes:
        with tabs[tab_idx]:
            for i, (sql, df) in enumerate(result.dataframes):
                if len(df) == 0:
                    continue
                st.caption(f"Result set {i + 1} — {len(df)} rows, {len(df.columns)} columns")
                _render_auto_chart(df, i)
                st.dataframe(df, use_container_width=True, height=min(35 * len(df) + 38, 400))
                if i < len(result.dataframes) - 1:
                    st.divider()
        tab_idx += 1

    # --- SQL tab ---
    with tabs[tab_idx]:
        if result.sql_queries:
            for i, sql in enumerate(result.sql_queries, 1):
                st.caption(f"Query {i}")
                st.code(sql, language="sql")
        else:
            st.caption("No SQL executed.")
    tab_idx += 1

    # --- LLM Calls tab ---
    with tabs[tab_idx]:
        if result.api_calls:
            for call in result.api_calls:
                st.markdown(f"**Turn {call.turn}** — stop: `{call.stop_reason}` · tokens: {call.total_tokens:,} · cost: ${call.cost:.6f}")

                req_col, resp_col = st.columns(2)
                with req_col:
                    st.caption("Request")
                    st.code(_json.dumps({
                        "model": call.model,
                        "max_tokens": call.max_tokens,
                        "system": f"<{call.system_prompt_length:,} chars>",
                        "tools": f"<{call.tools_provided} tools>",
                        "messages": f"<{call.messages_sent} msgs>",
                    }, indent=2), language="json")
                with resp_col:
                    st.caption("Response")
                    st.code(_json.dumps({
                        "stop_reason": call.stop_reason,
                        "input_tokens": call.input_tokens,
                        "output_tokens": call.output_tokens,
                        "cost": f"${call.cost:.6f}",
                    }, indent=2), language="json")

                if call.assistant_text and call.stop_reason == "tool_use":
                    with st.expander("Assistant reasoning", expanded=False):
                        st.markdown(call.assistant_text[:800])

                for tc in call.tool_calls:
                    with st.expander(f"Tool: `{tc.name}`", expanded=False):
                        c1, c2 = st.columns(2)
                        with c1:
                            st.caption("Input")
                            st.code(_json.dumps(tc.input, indent=2, default=str), language="json")
                        with c2:
                            st.caption("Result")
                            try:
                                parsed = _json.loads(tc.result)
                                preview = _json.dumps(parsed, indent=2, default=str)
                                if len(preview) > 2000:
                                    preview = preview[:2000] + "\n..."
                                st.code(preview, language="json")
                            except (_json.JSONDecodeError, TypeError):
                                st.code(tc.result[:2000], language="text")

                st.divider()

            # Summary table
            st.caption("Turn-by-turn summary")
            st.dataframe(pd.DataFrame([
                {
                    "Turn": c.turn,
                    "Stop": c.stop_reason,
                    "Tools": ", ".join(tc.name for tc in c.tool_calls) or "—",
                    "In": c.input_tokens,
                    "Out": c.output_tokens,
                    "Cost": f"${c.cost:.6f}",
                }
                for c in result.api_calls
            ]), use_container_width=True, hide_index=True)
        else:
            st.caption("No API calls recorded.")
    tab_idx += 1

    # --- Diagnostics tab ---
    with tabs[tab_idx]:
        if result.diagnostics:
            for diag in result.diagnostics:
                st.text(diag)
        else:
            st.caption("No diagnostics.")

elif not question:
    st.info("Ask a question about your retail data to get started. Try one of the quick questions in the sidebar.")


# NOTE: _render_auto_chart is defined above, before first use
