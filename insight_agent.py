# llm_agents/handle_insight_query.py
import json
import pandas as pd
from llm.llm_utils import client
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # your decorator

def generate_sql_from_nl_agent(query: str) -> dict:
    prompt = f"""
You are a SQL assistant. Translate the user's request into a SQL query that may use both the DuckDB and SQLite schemas.

DuckDB Tables:
- inventory (product_id (int), total_qty (int), committed_qty (int), available_qty (int), backorder_qty (int), scheduled_qty (int))
- product (product_id (int), name (string), category (string), status (string), price (float)) 

SQLite Tables:
- sales (sale_id (int), product_id (int), quantity (int), sale_date (date), revenue (float), order_status (string))

For complex queries, show how you'd join them or simulate if not directly joinable.
If the data is only available in one DB (like sales in SQLite), do not query the other. Only include a second query if it's truly needed (e.g., getting backorder from DuckDB after fetching top product from SQLite).

User Query: {query}

Respond with JSON like:
{{
  "dbs": ["duckdb", "sqlite"],
  "sqls": {{
    "sqlite": "...",
    "duckdb": "..."
  }}
}}
"""

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "system", "content": "You generate SQL from user questions."},
                  {"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=400,
    )
    output = response.choices[0].message.content.strip()
    return json.loads(output)

@debug_log
def handle_insight_query(user_query: str) -> dict:
    try:
        sql_obj = generate_sql_from_nl_agent(user_query)
        dbs = sql_obj.get("dbs", [])
        sqls = sql_obj.get("sqls", {})

        if not dbs or not sqls:
            return {
                "type": "error",
                "action": "handle_insight_query",
                "status": "failed",
                "message": "No database or SQL queries generated from the input."
            }

        # Pick one DB to query, priority SQLite then DuckDB
        if "sqlite" in dbs and sqls.get("sqlite"):
            sql_to_run = sqls["sqlite"]
            executed_sql = {"sqlite": sql_to_run}
            with sqlite_engine.connect() as conn:
                df = pd.read_sql_query(sql_to_run, conn)

        elif "duckdb" in dbs and sqls.get("duckdb"):
            sql_to_run = sqls["duckdb"]
            executed_sql = {"duckdb": sql_to_run}
            with get_duckdb_conn() as conn:
                df = conn.execute(sql_to_run).fetchdf()

        else:
            return {
                "type": "error",
                "action": "handle_insight_query",
                "status": "failed",
                "message": "No valid SQL query found for either SQLite or DuckDB."
            }

        if df.empty:
            return {
                "type": "insight",
                "executed_sql": executed_sql,
                "result_table": [],
                "summary": "No data found for the requested query."
            }

        return {
            "type": "insight",
            "executed_sql": executed_sql,
            "result_table": df.to_dict(orient="records"),
            "summary": f"Insight: {user_query.capitalize()}"
        }

    except Exception as e:
        return {
            "type": "error",
            "action": "handle_insight_query",
            "status": "failed",
            "message": f"Insight query failed: {str(e)}"
        }
