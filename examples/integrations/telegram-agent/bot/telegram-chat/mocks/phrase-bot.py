import json


def _event_messages(messages):
    events = []
    for message in messages:
        if message.get("role") != "user":
            continue
        try:
            frame = json.loads(message.get("content") or "{}")
        except (TypeError, ValueError):
            continue
        event = frame.get("event")
        if isinstance(event, dict):
            events.append(event)
    return events


def handle(input):
    messages = input.get("messages") or []
    if messages and messages[-1].get("role") == "tool":
        return {
            "text": "Telegram reply requested.",
            "usage": {"input_tokens": 12, "output_tokens": 4},
        }

    events = _event_messages(messages)
    event = events[-1] if events else {}
    payload = event.get("payload") or {}
    turn = len(events)
    return {
        "calls": [{
            "name": "emit_telegram_reply_requested",
            "arguments": {
                "chat_id": str(payload.get("conversation_reference", "")),
                "text": "Mock turn " + str(turn) + ": " + str(payload.get("text", "")),
            },
        }],
        "usage": {"input_tokens": 12, "output_tokens": 4},
    }
