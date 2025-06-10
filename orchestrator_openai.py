import os
from llm.llm_utils import client
from llm.erp_tool_agent import call_tool_agent
from llm.insight_agent import handle_insight_query
from llm.format_response import format_response_with_gpt
from llm.fallback_gpt_chat import fallback_gpt_chat
# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def classify_query_type(query: str) -> str:
    system_msg = (
        "You're a classifier for an ERP assistant. Respond with only 'action' if the query "
        "involves an order operation like create, cancel, update, return etc. Respond with 'insight' "
        "if the query asks for data analysis, summary or SQL-based information. Respond with 'other'"
        "if the user greets you or asks non-technical questions"
    )

    user_msg = f"Query:\n{query}\n\nOnly respond with 'action' or 'insight' or 'other'"

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg}
        ],
        temperature=0,
    )

    return response.choices[0].message.content.strip().lower()


def unified_agent(query: str) -> dict:
    try:
        task_type = classify_query_type(query)
    except Exception as e:
        return {
            "type": "error",
            "status": "failed",
            "message": f"❌ Failed to classify query: {str(e)}"
        }

    if task_type == "action":
        raw_response = call_tool_agent(query)
        # raw_response should be a dict with keys: type, status, message, etc.
        if not isinstance(raw_response, dict):
            # fallback if unexpected format
            return fallback_gpt_chat(query)

        if raw_response.get("type") == "error" or raw_response.get("status") == "failed":
            error_msg = raw_response.get("message", "")
            # fallback for unknown tool or None tool error keywords
            if "unknown tool" in error_msg.lower() or "no valid json" in error_msg.lower():
                return fallback_gpt_chat(query)
            return {
                "type": "error",
                "status": "failed",
                "message": f"❌ Tool agent error: {error_msg}"
            }

        # Successful tool response
        # Format response with GPT for nice output if possible
        content = raw_response.get("message") or raw_response.get("result") or raw_response
        formatted = format_response_with_gpt(content)
        # return {
        #     "type": "action_response",
        #     "status": "success",
        #     "formatted_response": formatted,
        #     "raw_response": raw_response
        # }
        return formatted

    elif task_type == "insight":
        # Handle insight queries; returns dict or string depending on your implementation
        insight_response = handle_insight_query(query)
        # If insight_response is a dict, pass through; else fallback
        if isinstance(insight_response, dict):
            return insight_response
        else:
            # fallback to GPT chat if unexpected
            return fallback_gpt_chat(query)
    else:
        # Unknown task type - fallback
        return fallback_gpt_chat(query)
