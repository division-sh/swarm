import json


def _event_payloads(messages):
    payloads = []
    for message in messages:
        if message.get("role") != "user":
            continue
        try:
            payload = json.loads(message.get("content") or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        # Retained turns keep the rendering originally sent.
        if payload.get("kind") == "initial" and isinstance(payload.get("event"), dict):
            payload = payload["event"].get("payload")
        if isinstance(payload, dict) and "text" in payload:
            payloads.append(payload)
    return payloads


def handle(input):
    messages = input.get("messages") or []
    if messages and messages[-1].get("role") == "tool":
        return {
            "text": "Telegram reply requested.",
            "usage": {"input_tokens": 12, "output_tokens": 4},
        }

    payloads = _event_payloads(messages)
    payload = payloads[-1] if payloads else {}
    turn = len(payloads)
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
