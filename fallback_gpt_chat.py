from llm.llm_utils import client

def fallback_gpt_chat(user_input: str) -> str:
    system_prompt = (
        "You are a friendly ERP assistant. If the user greets you or asks non-technical questions, "
        "respond in a helpful and conversational manner."
    )

    completion = client.chat.completions.create(
        model="gpt-4",  # Or gpt-3.5-turbo if using that
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_input}
        ],
        temperature=0.7,
    )
    return completion.choices[0].message.content.strip()

