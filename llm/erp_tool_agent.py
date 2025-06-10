import json
from llm.llm_utils import client
from tools.create_order import create_order
from tools.schedule_order import schedule_order
from tools.complete_order import complete_order
from tools.cancel_order import cancel_order
from tools.return_order import return_order
from tools.change_order import change_order

TOOL_FUNCTIONS = {
    "create_order": create_order,
    "schedule_order": schedule_order,
    "complete_order": complete_order,
    "cancel_order": cancel_order,
    "return_order": return_order,
    "modify_order": change_order,
}

def extract_json_block(text: str) -> str | None:
    """Extract first valid JSON object from text, or None if not found."""
    start = text.find('{')
    if start == -1:
        return None
    brace_count = 0
    for i in range(start, len(text)):
        if text[i] == '{':
            brace_count += 1
        elif text[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                return text[start:i+1]
    return None

def call_tool_agent(user_query: str) -> dict:
    prompt = f"""
You are an ERP assistant with the following tools:

- create_order(product_id: int, quantity: int)
- schedule_order(sale_id: int)
- complete_order(sale_id: int)
- cancel_order(sale_id: int)
- return_order(sale_id: int)
- modify_order(sale_id: int, new_quantity: int)

Given a user's query, respond only with JSON (no explanation):

{{
  "tool": "<tool_name>",
  "parameters": {{
    "param1": "value1"
  }}
}}
User Query: {user_query}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful ERP assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4,
            max_tokens=300,
        )
        output = response.choices[0].message.content.strip()
        json_block = extract_json_block(output)
        if not json_block:
            return {
                "type": "error",
                "status": "failed",
                "message": "❌ No valid JSON found in model response."
            }

        parsed = json.loads(json_block)
        tool = parsed.get("tool")
        params = parsed.get("parameters", {})

        if not tool:
            return {
                "type": "error",
                "status": "failed",
                "message": "❌ 'tool' not specified in response JSON."
            }

        if tool not in TOOL_FUNCTIONS:
            return {
                "type": "error",
                "status": "failed",
                "message": f"❌ Unknown tool requested: {tool}"
            }

        # Optional: convert parameter types if you want here, e.g. int()
        # Just pass params as is for now
        result = TOOL_FUNCTIONS[tool](**params)
        
        # Normalize result: if result is string, wrap in dict for consistency
        if isinstance(result, str):
            return {
                "type": "tool_response",
                "tool": tool,
                "status": "success",
                "message": result
            }
        elif isinstance(result, dict):
            # Assume tool returns a dict with its own status/message structure
            result.setdefault("type", "tool_response")
            result.setdefault("tool", tool)
            result.setdefault("status", "success")
            return result
        else:
            return {
                "type": "tool_response",
                "tool": tool,
                "status": "success",
                "message": str(result)
            }

    except Exception as e:
        return {
            "type": "error",
            "status": "failed",
            "message": f"❌ Tool agent execution error: {str(e)}"
        }
