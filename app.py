import streamlit as st
from sqlalchemy import text
from database.db_utils import get_duckdb_conn, sqlite_engine
from database.schema_duckdb import create_duckdb_schema
from database.populate_duckdb import populate_duckdb
from database.schema_sqlite import create_sqlite_schema
import pandas as pd
from setup_db import populate_all

populate_all()

st.set_page_config(page_title="Agentic ERP System", layout="wide")

st.markdown("<h1 style='text-align: center;'>Agentic ERP System</h1>", unsafe_allow_html=True)

# Horizontal tabs
tab1, tab2, tab3 = st.tabs(["🗨️ Conversation", "🦆 DuckDB", "🗃️ SQLite"])

# --- Tab 1: Conversation ---
with tab1:
    st.markdown("""
    <style>
        .chat-area {
            height: 65vh;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            padding: 1rem;
            margin-bottom: 1rem;
        }
        .chat-bubble {
            padding: 0.8rem 1.2rem;
            border-radius: 20px;
            margin-bottom: 0.5rem;
            max-width: 75%;
            word-wrap: break-word;
            font-size: 1rem;
            line-height: 1.5;
        }

        .user {
            align-self: flex-end;
            background: linear-gradient(135deg, #0575E6, #00F260);  /* Instagram blue-green */
            color: white;
            text-align: right;
            border: none;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }

        .bot {
            align-self: flex-start;
            background-color: #e4e6eb;  /* Light grey for bot */
            color: #111;
            text-align: left;
            border: none;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }

        pre {
            background-color: #f4f4f4;
            padding: 0.5rem;
            border-radius: 5px;
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
            font-size: 0.95rem;
        }
        table, th, td {
            border: 1px solid #ccc;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
""", unsafe_allow_html=True)


    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    chat_box = st.empty()

    def render_chat():
        chat_html = "<div class='chat-area'>"
        for msg in st.session_state.chat_history:
            role_class = "user" if msg["role"] == "user" else "bot"
            chat_html += f"<div class='chat-bubble {role_class}'>{msg['content']}</div>"
        chat_html += "</div>"
        chat_box.markdown(chat_html, unsafe_allow_html=True)

    render_chat()

    user_input = st.chat_input("Ask something like 'update quantity for latest order' or 'list top 5 sold products'")

    if user_input:
        # 1. Add user message
        st.session_state.chat_history.append({"role": "user", "content": user_input})

        # 2. Add placeholder with spinner
        thinking_index = len(st.session_state.chat_history)
        st.session_state.chat_history.append({"role": "assistant", "content": "<div class='spinner'></div>Processing..."})
        render_chat()

        # 3. Run agent in blocking mode
        from orchestrator_openai import unified_agent
        agent_response = unified_agent(user_input)

        if isinstance(agent_response, dict) and agent_response.get("type") == "insight":
            summary = agent_response.get("summary", "Insight Result")
            sqls = agent_response.get("executed_sql", {})
            result_df = pd.DataFrame(agent_response.get("result_table", []))

            # 4. Build response HTML (summary + SQL + Table)
            bot_response_html = f"<strong>📊 {summary}</strong><br><br>"

            if sqls:
                bot_response_html += "<strong>📄 Executed SQL:</strong><br>"
                for db, sql in sqls.items():
                    bot_response_html += f"<em>{db}</em>:<br><pre><code>{sql}</code></pre>"

            if not result_df.empty:
                bot_response_html += "<strong>📈 Insight Result:</strong><br>"
                bot_response_html += result_df.to_html(index=False, escape=False, border=0)
            else:
                bot_response_html += "<em>No results found.</em>"

            # Replace spinner with formatted HTML
            st.session_state.chat_history[thinking_index] = {"role": "assistant", "content": bot_response_html}
            render_chat()
        else:
            # Normal response
            st.session_state.chat_history[thinking_index] = {"role": "assistant", "content": str(agent_response)}
            render_chat()

# --- Tab 2: DuckDB ---
with tab2:
    col1, col2 = st.columns([5, 1])
    with col1:
        st.title("🦆 DuckDB Tables")
    with col2:
        if st.button("🔄 Reset DuckDB", use_container_width=True):
            from database.schema_duckdb import create_duckdb_schema
            from database.populate_duckdb import populate_duckdb
            create_duckdb_schema()
            populate_duckdb()
            st.success("✅ DuckDB reset and repopulated.")

    try:
        with get_duckdb_conn() as duck_conn:
            tables = duck_conn.execute("SHOW TABLES").fetchall()
            for table in tables:
                st.subheader(table[0])
                df = duck_conn.execute(f"SELECT * FROM {table[0]}").fetchdf()
                st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"DuckDB Error: {str(e)}")

# --- Tab 3: SQLite ---
with tab3:
    col1, col2 = st.columns([5, 1])
    with col1:
        st.title("🗃️ SQLite Tables")
    with col2:
        if st.button("🔄 Reset SQLite", use_container_width=True):
            from database.schema_sqlite import create_sqlite_schema
            create_sqlite_schema()
            st.success("✅ SQLite reset and repopulated.")

    try:
        with sqlite_engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = result.fetchall()
            for (table_name,) in tables:
                if table_name != "sqlite_sequence":
                    st.subheader(table_name)
                    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                    st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"SQLite Error: {str(e)}")

