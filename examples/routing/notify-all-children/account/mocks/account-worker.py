def handle(input):
    if input.get("tool_results"):
        return {
            "text": "Account notification completed.",
            "usage": {"input_tokens": 8, "output_tokens": 3},
        }
    return {
        "calls": [{
            "name": "emit_account_notification_completed",
            "arguments": {},
        }],
        "usage": {"input_tokens": 8, "output_tokens": 3},
    }
