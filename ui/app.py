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

from agent.agent import RetailSageAgent

st.set_page_config(
    page_title="Retail-SAGE",
    page_icon="🏪",
    layout="wide",
)

st.title("Retail-SAGE")
st.caption("Semantic Analytics & Governed Execution — AI-Powered Retail Analytics Agent")


@st.cache_resource
def get_agent():
    return RetailSageAgent()


agent = get_agent()

# Sidebar with info
with st.sidebar:
    st.header("About")
    st.markdown("""
    **Retail-SAGE** is an AI analytics agent powered by Claude
    that autonomously analyzes a 100GB TPC-DS retail data lake.

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

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Handle example question click
if "example_question" in st.session_state:
    prompt = st.session_state.pop("example_question")
else:
    prompt = st.chat_input("Ask a question about your retail data...")

if prompt:
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get agent response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            try:
                response = agent.ask(prompt)
                st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})
            except Exception as e:
                error_msg = f"Error: {e}"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})
