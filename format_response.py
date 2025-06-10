# format_response.py
from openai import OpenAI
from typing import Union
from llm.llm_utils import client


def format_response_with_gpt(raw_response: Union[str, dict]) -> str:
    content = str(raw_response) if isinstance(raw_response, dict) else raw_response

    FORMAT_RESPONSE_PROMPT = """
You are an ERP assistant. Convert the tool output into a short, human-friendly 1-liner to show in the UI.

Rules:
- Keep it concise and readable.
- Use this format: "<Action> <sale_id> <status>. Details: ..."
- Don't add greetings or explanations.
- If revenue, quantity, or other values are included, display them clearly.
- Round monetary values to 2 decimal places.

Examples:

Input: ✅ Order #5 created → Product 101, Qty: 1, Revenue: 1000.0, Committed Qty: 3, Backorder Qty: 0  
Output: Order 5 created successfully. Details: 101 (Qty: 1) — Revenue: $1000.00, Committed: 3, Backorder: 0

Input: ✅ Sale 6 scheduled successfully. Scheduled Qty: 1, Remaining schedulable: 3  
Output: Order 6 scheduled successfully. Details: Qty: 1, Remaining: 3

Input: ✅ Sale 6 completed. Invoiced Revenue: $1000.0, Quantity: 1  
Output: Order 6 completed. Details: Qty: 1, Revenue: $1000.00

Input: ✅ Sale 6 cancelled. Reason: customer request  
Output: Order 6 cancelled. Reason: customer request

Input: ✅ Return processed for Sale 6. Refunded: $1000.0  
Output: Order 6 returned successfully. Refunded: $1000.00

Input: ✅ Sale 6 modified → New Quantity: 2, Revenue: 2000.0, Backorder: 0  
Output: Order 6 modified. Details: Qty: 2, Revenue: $2000.00, Backorder: 0

Now format this:
{tool_output}
"""

    prompt = FORMAT_RESPONSE_PROMPT.format(tool_output=raw_response)

    completion = client.chat.completions.create(
        model="gpt-4",  # Or "gpt-3.5-turbo"
        messages=[
            {"role": "system", "content": "You are a concise ERP assistant."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.5
    )

    return completion.choices[0].message.content.strip()
