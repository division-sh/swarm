import json


def _events(messages):
    result = []
    for message in messages:
        if message.get("role") != "user":
            continue
        try:
            frame = json.loads(message.get("content") or "{}")
        except (TypeError, ValueError):
            continue
        event = frame.get("event")
        if isinstance(event, dict):
            result.append(event)
    return result


def handle(input):
    messages = input.get("messages") or []
    if messages and messages[-1].get("role") == "tool":
        return {
            "text": "Lifecycle reply requested.",
            "usage": {"input_tokens": 8, "output_tokens": 3},
        }

    events = _events(messages)
    payload = (events[-1] if events else {}).get("payload") or {}
    return {
        "calls": [{
            "name": "emit_telegram_reply_requested",
            "arguments": {
                "chat_id": str(payload.get("conversation_reference", "")),
                "text": "Lifecycle mock: " + str(payload.get("text", "")),
            },
        }],
        "usage": {"input_tokens": 8, "output_tokens": 3},
    }
