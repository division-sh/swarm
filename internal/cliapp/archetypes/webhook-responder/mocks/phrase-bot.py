def handle(input):
    if input.get("tool_results"):
        return {
            "text": "Telegram reply requested.",
            "usage": {"input_tokens": 12, "output_tokens": 4},
        }
    event = input.get("event") or {}
    payload = event.get("payload") or {}
    return {
        "calls": [{
            "name": "emit_telegram_reply_requested",
            "arguments": {
                "chat_id": str(payload.get("conversation_reference", "")),
                "text": "Swarm mock heard: " + str(payload.get("text", "")),
            },
        }],
        "usage": {"input_tokens": 12, "output_tokens": 4},
    }
