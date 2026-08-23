def handle(input):
    if input.get("tool_results"):
        return {
            "text": "Scout completed.",
            "usage": {"input_tokens": 8, "output_tokens": 3},
        }
    return {
        "calls": [{
            "name": "emit_scout_completed",
            "arguments": {
                "batch_id": "golden-batch",
                "candidate_ids": ["alpha", "beta"],
            },
        }],
        "usage": {"input_tokens": 8, "output_tokens": 3},
    }
