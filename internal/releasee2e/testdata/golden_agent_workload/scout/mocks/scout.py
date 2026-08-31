import json


def handle(input):
    if input.get("tool_results"):
        return {
            "text": "Scout completed.",
            "usage": {"input_tokens": 8, "output_tokens": 3},
        }
    payload = json.loads(input["messages"][-1]["content"])["event"]["payload"]
    return {
        "calls": [{
            "name": "emit_scout_completed",
            "arguments": {
                "batch_id": "golden-batch",
                "candidate_ids": payload["candidate_ids"],
            },
        }],
        "usage": {"input_tokens": 8, "output_tokens": 3},
    }
